from __future__ import annotations

import asyncio
import logging
import threading
from dataclasses import dataclass
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Generic
from typing import TypeVar
from typing import cast

T = TypeVar("T")

_log = logging.getLogger(__name__)


class Resource(Generic[T]):
    """
    A single-use async context manager for resource acquisition.

    This class can only be used once as an async context manager. After
    acquisition, it cannot be reacquired, and after release, it cannot be
    released again.

    :param pool:
        The `ResourcePool` this resource belongs to.
    :param key:
        The cache key for this resource.
    """

    def __init__(self, pool: ResourcePool[T], key):
        self._pool = pool
        self._key = key
        self._resource = None
        self._acquired = False
        self._released = False

    async def __aenter__(self) -> T:
        """
        Context manager entry - acquire resource.

        :returns:
            The cached resource object.
        :raises RuntimeError:
            If called on a resource that was previously acquired.
        """
        if self._acquired:
            raise RuntimeError(
                "Cannot re-acquire a resource that has already been acquired"
            )

        self._acquired = True
        try:
            self._resource = await self._pool.acquire(self._key)
            return cast(T, self._resource)
        except Exception:
            self._acquired = False
            raise

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit - release resource.

        :param exc_type:
            Exception type if an exception occurred, None otherwise.
        :param exc_val:
            Exception value if an exception occurred, None otherwise.
        :param exc_tb:
            Exception traceback if an exception occurred, None otherwise.
        """
        await self._release()

    async def _release(self):
        """
        Release the resource.

        :raises RuntimeError:
            If attempting to release a resource that was not acquired or
            already released.
        """
        if not self._acquired:
            raise RuntimeError("Cannot release a resource that was not acquired")
        if self._released:
            raise RuntimeError(
                "Cannot release a resource that has already been released"
            )

        self._released = True
        if self._resource is not None:
            await self._pool.release(self._key)


class ResourcePool(Generic[T]):
    """
    An asynchronous reference-counted cache with TTL-based cleanup.

    Objects are created on-demand via a factory function (sync or async) and
    automatically cleaned up after all references are released and the TTL
    expires.

    **Loop partitioning.** A pool serves any number of running event
    loops at once, each through a private partition only that loop can
    reach: entries, reference counts, and TTL timers are never shared
    across loops, because a pooled resource can only be used and
    finalized on the loop that created it. `acquire`, `release`,
    `expire`, `clear`, `stats`, and `pending_cleanup` therefore act on
    the calling loop's partition alone, and a process-wide clear does not
    exist. A partition is finalized by clearing it from its own loop
    before that loop stops. A partition whose loop has stopped without
    doing so is swept the next time a new loop reaches the pool: its
    entries are dropped without running their finalizers and the drop is
    reported at warning level, since a resource stranded that way is a
    leak whether or not anything still referenced it.

    :param factory:
        Function to create new objects (sync or async).
    :param finalizer:
        Optional cleanup function (sync or async). It runs while the
        partition holds its lock, so it must not await `acquire`,
        `release`, `expire`, `expire_all` or `clear` — those take the
        same lock and the call deadlocks; the read-only members are
        lock-free and safe. An `Exception` it raises is suppressed, a
        `BaseException` propagates to whichever operation ran the
        finalizer, and the entry is evicted either way.
    :param ttl:
        Time-to-live in seconds after last reference is released.
    """

    @dataclass
    class CacheEntry:
        """
        Internal cache entry tracking an object and its metadata.

        :param obj:
            The cached object.
        :param reference_count:
            Number of active references to this object.
        :param timer:
            Optional TTL timer scheduled when the reference count
            reaches zero; spawns the cleanup task once the TTL
            elapses.
        :param cleanup:
            Optional cleanup task created when the TTL timer fires.
        :param doomed:
            Whether `ResourcePool.expire` marked this entry for
            finalization as soon as its reference count reaches zero.
            Cleared when the entry is re-acquired first — re-access
            resurrects, matching the pool's timer-cancellation
            semantics.
        """

        obj: Any
        reference_count: int
        timer: asyncio.TimerHandle | None = None
        cleanup: asyncio.Task | None = None
        doomed: bool = False

    @dataclass
    class Stats:
        """
        Statistics about the current state of the resource pool.

        :param total_entries:
            Total number of cached entries.
        :param referenced_entries:
            Number of entries currently being referenced (reference_count > 0).
        :param pending_cleanup:
            Number of keys in `ResourcePool.pending_cleanup`.
        """

        total_entries: int
        referenced_entries: int
        pending_cleanup: int

    def __init__(
        self,
        factory: Callable[[Any], T | Awaitable[T]],
        *,
        finalizer: Callable[[T], None | Awaitable[None]] | None = None,
        ttl: float = 0,
    ):
        self._factory = factory
        self._finalizer = finalizer
        self._ttl = ttl
        self._partitions: dict[asyncio.AbstractEventLoop, _Partition[T]] = {}
        self._registry_lock = threading.Lock()

    async def __aenter__(self):
        """Async context manager entry.

        :returns:
            The ResourcePool instance itself.
        """
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - cleanup all resources.

        :param exc_type:
            Exception type if an exception occurred, None otherwise.
        :param exc_val:
            Exception value if an exception occurred, None otherwise.
        :param exc_tb:
            Exception traceback if an exception occurred, None otherwise.
        """
        await self.clear()

    @property
    def stats(self) -> Stats:
        """
        Return cache statistics for the calling loop's partition.

        Reading resolves the calling loop's partition like every other
        operation: it registers the partition on first use, which also
        sweeps the partitions of loops that have stopped.

        .. note::
            This is synchronous for convenience, but should only be called
            when not concurrently modifying the cache.

        :returns:
            `ResourcePool.Stats` containing current statistics.
        :raises RuntimeError:
            If there is no running event loop.
        """
        return self._partition().stats

    @property
    def pending_cleanup(self):
        """
        Map the calling loop's cache keys to their pending cleanup work.

        A pending entry holds either an unfired TTL timer or a
        cleanup task that has not finished.

        :returns:
            Dictionary mapping each such key to its pending TTL timer
            or cleanup task.
        :raises RuntimeError:
            If there is no running event loop; see `stats`.
        """
        return self._partition().pending_cleanup

    def get(self, key: Any) -> Resource[T]:
        """
        Get a resource acquisition that can be awaited or used as context
        manager.

        :param key:
            The cache key.
        :returns:
            `Resource` that can be awaited or used with 'async with'.
        """
        return Resource(self, key)

    async def acquire(self, key: Any) -> T:
        """
        Internal acquire method - acquires a reference to the cached object.

        Creates a new object via the factory if not cached. Increments
        reference count and cancels any pending cleanup.

        :param key:
            The cache key.
        :returns:
            The cached or newly created object.
        """
        return await self._partition().acquire(key)

    async def release(self, key: Any) -> None:
        """
        Release a reference to the cached object.

        Decrements reference count. If count reaches 0, schedules cleanup
        after TTL expires (if TTL > 0); an entry retired by `expire` or
        `expire_all` is finalized here rather than deferred — see `expire`
        for the retirement contract. Releasing a key that is not cached is
        a silent no-op.

        :param key:
            The cache key.
        :raises ValueError:
            If the key's reference count is already 0.

        .. rubric:: Implementation notes

        Finalizing inline rather than in a spawned task is what lets a
        release that lands while its loop is shutting down still close
        the resource: a task spawned there would be orphaned by the
        closing loop, and the resource abandoned with it.
        """
        await self._partition().release(key)

    async def expire(self, key: Any) -> None:
        """Treat *key* as TTL-expired now, finalizing it once unreferenced.

        Drops the pool's own retention of an entry — the retention that
        keeps it cached for reuse until its TTL fires — without touching
        the reference count callers hold through `acquire` and `release`.
        An unreferenced entry, including one already idling out its TTL,
        is finalized immediately; a referenced entry is marked and
        finalized by the release that drops its last reference, before
        that release returns, so in-flight users always drain first.
        Re-acquiring a marked entry before that release clears the mark —
        re-access resurrects, matching the
        pool's timer-cancellation semantics. Unlike `clear`, which tears
        the whole partition down regardless of reference count, this
        never finalizes a resource out from under an active reference.
        Expiring a key that is not cached is a silent no-op.

        :param key:
            The cache key to expire.
        """
        await self._partition().expire(key)

    async def expire_all(self) -> None:
        """Treat every key the calling loop cached as TTL-expired now.

        `expire` applied to every key in the calling loop's partition at
        once (see `expire` for the per-key drain-first and resurrection
        semantics): the retirement primitive for a loop that stays
        running. Unlike `clear`, it does not wait for the finalizers of
        referenced entries; each runs in the release that drops the
        entry's last reference (see `release`).

        Retirement is all-or-nothing in reach, not in outcome: every
        cached key is retired even if finalizing one of them fails. An
        `Exception` raised by a finalizer is contained per entry (see
        ``finalizer``); any other `BaseException`, e.g., a cancelled
        teardown's `asyncio.CancelledError`, propagates once the sweep
        is over, not in place of it.

        :raises BaseException:
            The first non-`Exception` failure a finalizer raised,
            re-raised after every remaining key has been retired.

        .. rubric:: Implementation notes

        The sweep deliberately outlives its own failures. This is the
        primitive a loop retires its resources through, and it is
        reached on teardown paths that are themselves cancellable, so
        abandoning the loop at the first `BaseException` would strand
        every key it had not reached yet, i.e., the leak the primitive
        exists to close, reappearing under cancellation.
        """
        await self._partition().expire_all()

    async def clear(self) -> None:
        """Finalize every entry the calling loop cached and cancel its cleanups.

        The teardown primitive: it force-finalizes regardless of reference
        count, which is correct when the loop's use of the pool is over
        and there is nothing left to drain for. It is loop-scoped: another
        loop's partition is untouched and must be cleared from that loop,
        the only place its finalizers can run. To retire keys while the
        loop stays in use, use `expire` or `expire_all`, which drain
        first: clearing with references outstanding lets a later release
        land on whatever entry has since been rebuilt under the same key.
        """
        await self._partition().clear()

    def _partition(self) -> _Partition[T]:
        """Return the running loop's partition, creating it on first use.

        Creating a partition also sweeps every partition whose loop is no
        longer running — see the class docstring for what a sweep drops
        and how it is reported.

        .. rubric:: Implementation notes

        The registry is the only state shared across loops, so its lookup
        is the only cross-thread critical section: synchronous, O(1) on a
        hit, and never held across an ``await``. It takes an explicit
        `threading.Lock` rather than relying on dict atomicity under the
        GIL, which free-threaded Python does not preserve. Sweeping on the
        miss path keeps the hit path constant-time and matches when a
        stale partition first becomes observable: a loop that stopped
        without clearing leaves nothing to react to until another loop
        arrives. Liveness is `is_running`, not `is_closed`: a partition
        holds an `asyncio.Lock` and timers that only a running loop can
        drive, and `is_running` reads one attribute, so it is safe to
        call from another thread.
        """
        loop = asyncio.get_running_loop()
        with self._registry_lock:
            partition = self._partitions.get(loop)
            if partition is None:
                stale = [owner for owner in self._partitions if not owner.is_running()]
                for owner in stale:
                    self._partitions.pop(owner).discard(self._name)
                partition = _Partition(self._factory, self._finalizer, self._ttl)
                self._partitions[loop] = partition
        return partition

    @property
    def _name(self) -> str:
        """Identify this pool in log records by its factory."""
        return getattr(self._factory, "__qualname__", None) or repr(self._factory)


class _Partition(Generic[T]):
    """One event loop's share of a `ResourcePool`.

    Holds the cache, the `asyncio.Lock` serializing it, and the TTL
    timers for a single loop; the owning pool routes every operation
    here from that loop alone. The caching semantics implemented here
    are documented on `ResourcePool`, which owns the contract.
    """

    def __init__(
        self,
        factory: Callable[[Any], T | Awaitable[T]],
        finalizer: Callable[[T], None | Awaitable[None]] | None,
        ttl: float,
    ):
        self._factory = factory
        self._finalizer = finalizer
        self._ttl = ttl
        self._cache: dict[Any, ResourcePool.CacheEntry] = {}
        self._lock = asyncio.Lock()

    @property
    def stats(self) -> ResourcePool.Stats:
        return ResourcePool.Stats(
            total_entries=len(self._cache),
            referenced_entries=sum(
                1 for e in self._cache.values() if e.reference_count > 0
            ),
            pending_cleanup=len(self.pending_cleanup),
        )

    @property
    def pending_cleanup(self):
        return {
            k: v.timer if v.timer is not None else v.cleanup
            for k, v in self._cache.items()
            if v.timer is not None or (v.cleanup is not None and not v.cleanup.done())
        }

    async def acquire(self, key: Any) -> T:
        async with self._lock:
            if key in self._cache:
                entry = self._cache[key]
                entry.reference_count += 1
                entry.doomed = False
                self._cancel_timer(entry)
                await self._cancel_cleanup(entry)
                return entry.obj
            else:
                # Cache miss - create new object
                obj = await _call(self._factory, key)
                self._cache[key] = ResourcePool.CacheEntry(obj=obj, reference_count=1)
                return obj

    async def release(self, key: Any) -> None:
        async with self._lock:
            if key not in self._cache:
                return
            entry = self._cache[key]

            if entry.reference_count <= 0:
                raise ValueError(f"Reference count for key '{key}' is already 0")

            entry.reference_count -= 1

            if entry.reference_count <= 0:
                if entry.doomed or self._ttl <= 0:
                    # Inline rather than a spawned task: with nothing to
                    # defer to, a task spawned here may never be run —
                    # see `ResourcePool.release`.
                    await self._cleanup(key)
                else:
                    # Defer cleanup with a plain timer rather than a
                    # task parked on a TTL sleep: an unfired
                    # TimerHandle is discarded silently at loop close,
                    # whereas a parked task is destroyed pending —
                    # and, if never started, its coroutine emits a
                    # "never awaited" RuntimeWarning.
                    loop = asyncio.get_running_loop()
                    entry.timer = loop.call_later(self._ttl, self._expire, key)

    async def expire(self, key: Any) -> None:
        async with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return
            await self._retire(key, entry)

    async def expire_all(self) -> None:
        async with self._lock:
            failure: BaseException | None = None
            for key, entry in list(self._cache.items()):
                try:
                    await self._retire(key, entry)
                except BaseException as error:
                    failure = failure or error
            if failure is not None:
                raise failure

    async def clear(self) -> None:
        async with self._lock:
            for key in list(self._cache.keys()):
                await self._cleanup(key)

    def discard(self, name: str) -> None:
        """Drop every entry without finalizing it, reporting what was lost.

        The path a partition takes when its loop is found to have
        stopped: the finalizers cannot run without that loop, so the
        entries are abandoned and the drop is logged at warning level
        with the referenced and idle counts. Timers are left to be
        discarded with their loop.

        :param name:
            How the owning pool identifies itself in the log record.
        """
        if not self._cache:
            return
        referenced = sum(1 for e in self._cache.values() if e.reference_count > 0)
        _log.warning(
            "ResourcePool(%s): dropping %d referenced and %d idle entries "
            "stranded by an event loop that stopped without clearing its "
            "partition (finalizers not run)",
            name,
            referenced,
            len(self._cache) - referenced,
        )
        self._cache.clear()

    def _cancel_timer(self, entry: ResourcePool.CacheEntry) -> None:
        """
        Cancel an entry's pending TTL timer, if any.

        The timer always belongs to this partition's loop, so a plain
        cancel suffices.

        :param entry:
            The cache entry whose timer to cancel.
        """
        if entry.timer is None:
            return
        timer, entry.timer = entry.timer, None
        timer.cancel()

    async def _cancel_cleanup(self, entry: ResourcePool.CacheEntry) -> None:
        """
        Cancel an entry's in-flight cleanup task, if any.

        The task is cancelled and awaited. The current task is left
        alone: on the expiry path this runs *inside* the entry's own
        cleanup task (`_finalize`), which must not cancel itself.

        :param entry:
            The cache entry whose cleanup task to cancel.
        """
        cleanup = entry.cleanup
        entry.cleanup = None
        if cleanup is None or cleanup.done() or cleanup is asyncio.current_task():
            return
        cleanup.cancel()
        try:
            await cleanup
        except asyncio.CancelledError:
            pass

    def _expire(self, key: Any) -> None:
        """
        Spawn the cleanup task for an expired entry.

        Runs synchronously, as a timer callback, on this partition's
        loop; see `_finalize` for how the spawned task tolerates a
        concurrent re-acquire. A timer that outlived its partition —
        one swept while its loop was stopped, then resumed — finds
        nothing cached and is ignored.

        :param key:
            The cache key whose TTL elapsed.
        """
        entry = self._cache.get(key)
        if entry is None:
            return
        entry.timer = None
        entry.cleanup = asyncio.get_running_loop().create_task(self._finalize(key))

    async def _retire(self, key: Any, entry: ResourcePool.CacheEntry) -> None:
        """
        Retire one entry: finalize it now if unreferenced, else mark it.

        .. warning::
            Must be called while holding the lock.

        :param key:
            The cache key to retire.
        :param entry:
            The entry cached under ``key``.
        """
        if entry.reference_count <= 0:
            # Also cancels any pending TTL timer or cleanup task.
            await self._cleanup(key)
        else:
            entry.doomed = True

    async def _finalize(self, key: Any) -> None:
        """
        Clean up an expired entry if it is still unreferenced.

        Re-checks the reference count under the lock, so an entry
        re-acquired between TTL expiry and lock acquisition is left
        untouched; cancellation by a concurrent re-acquire is
        likewise tolerated as an expected outcome.

        :param key:
            The cache key to clean up.
        """
        try:
            async with self._lock:
                if key in self._cache:
                    entry = self._cache[key]
                    if entry.reference_count == 0:
                        await self._cleanup(key)

        except asyncio.CancelledError:
            pass

    async def _cleanup(self, key: Any) -> None:
        """
        Remove entry from cache and call finalizer.

        .. warning::
            Must be called while holding the lock.

        :param key:
            The cache key to cleanup.
        """
        entry = self._cache[key]
        try:
            self._cancel_timer(entry)
            await self._cancel_cleanup(entry)
        finally:
            # Evict from the cache *unconditionally*, before and
            # regardless of how the finalizer exits. A finalized
            # resource must never remain cached: if the finalizer
            # raises — including ``CancelledError`` when cleanup runs
            # under a cancelled teardown, which is a ``BaseException``
            # and so escapes ``except Exception`` — the entry must
            # still be removed, or a later ``acquire`` hands back a
            # torn-down resource (e.g., a closed event loop). The
            # inner ``try`` lets the finalizer run for its side
            # effects while the outer ``finally`` guarantees eviction
            # and lets any cancellation propagate.
            try:
                if self._finalizer:
                    try:
                        await _call(self._finalizer, entry.obj)
                    except Exception:
                        pass
            finally:
                del self._cache[key]


async def _call(func: Callable, *args) -> Any:
    """
    Call a function that might be sync or async.

    If the function is a coroutine function, await it. Otherwise, call it
    synchronously. If the result is a coroutine, await that as well.

    :param func:
        The function to call.
    :param args:
        Arguments to pass to the function.
    :returns:
        The result of the function call.
    """
    if asyncio.iscoroutinefunction(func):
        return await func(*args)
    else:
        result = func(*args)
        # Check if the result is a coroutine and await it if so
        if asyncio.iscoroutine(result):
            return await result
        return result
