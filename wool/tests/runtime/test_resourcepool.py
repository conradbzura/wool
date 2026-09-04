import asyncio
import gc
import logging
import threading
import time
import warnings
from contextlib import nullcontext
from types import SimpleNamespace

import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies

from wool.runtime.resourcepool import Resource
from wool.runtime.resourcepool import ResourcePool


@strategies.composite
def factory_functions(draw):
    """Generate various factory function types with consistent interfaces."""
    factory_type = draw(
        strategies.sampled_from(
            [
                "sync_simple",
                "async_simple",
                "sync_lambda",
                "async_lambda",
                "callable",
                "awaitable",
            ]
        )
    )

    if factory_type == "sync_simple":

        def sync_factory(key):
            return SimpleNamespace(name=f"sync-{key}", created_by="sync_simple")

        return sync_factory

    elif factory_type == "async_simple":

        async def async_factory(key):
            return SimpleNamespace(name=f"async-{key}", created_by="async_simple")

        return async_factory

    elif factory_type == "sync_lambda":

        def sync_lambda_factory(key):
            return SimpleNamespace(name=f"lambda-{key}", created_by="sync_lambda")

        return lambda key: sync_lambda_factory(key)

    elif factory_type == "async_lambda":

        async def async_lambda_factory(key):
            return SimpleNamespace(name=f"async-lambda-{key}", created_by="async_lambda")

        return lambda key: async_lambda_factory(key)

    elif factory_type == "callable":

        class CallableLike:
            def __call__(self, key):
                return self.sync_factory(key)

            def sync_factory(self, key):
                return SimpleNamespace(name=f"callable-{key}", created_by="callable")

        return CallableLike()

    elif factory_type == "awaitable":

        class AwaitableLike:
            def __init__(self, key) -> None:
                self.key = key

            def __await__(self):
                return self.async_factory().__await__()

            async def async_factory(self):
                return SimpleNamespace(
                    name=f"awaitable-{self.key}", created_by="awaitable"
                )

        return AwaitableLike


@strategies.composite
def finalizer_functions(draw):
    """Generate various finalizer function types."""
    finalizer_type = draw(
        strategies.sampled_from(
            [
                None,
                "sync_simple",
                "async_simple",
                "sync_lambda",
                "async_lambda",
            ]
        )
    )

    if finalizer_type is None:
        return None

    elif finalizer_type == "sync_simple":

        def simple_sync_finalizer(obj):
            assert obj is not None

        return simple_sync_finalizer

    elif finalizer_type == "async_simple":

        async def simple_async_finalizer(obj):
            assert obj is not None

        return simple_async_finalizer

    elif finalizer_type == "sync_lambda":

        def sync_lambda_finalizer(obj):
            assert obj is not None

        return lambda obj: sync_lambda_finalizer(obj)

    elif finalizer_type == "async_lambda":

        async def async_lambda_finalizer(obj):
            assert obj is not None

        return lambda obj: async_lambda_finalizer(obj)


@pytest.fixture
def mock_resource_factory(mocker):
    """Create a mock factory with consistent behavior."""
    factory = mocker.Mock()
    factory.return_value = mocker.Mock(name="test-resource")
    return factory


@pytest.fixture
def resource_pool_immediate_cleanup(mocker, mock_resource_factory):
    """Build a ``ttl=0`` pool that finalizes on the last release.

    Returns the pool, its factory mock, and its awaitable finalizer
    mock.
    """
    finalizer = mocker.AsyncMock()
    pool = ResourcePool(factory=mock_resource_factory, finalizer=finalizer, ttl=0)
    yield pool, mock_resource_factory, finalizer


@pytest.fixture
def ttl_pool(mocker):
    """Build a long-TTL pool whose factory and finalizer are mocks.

    Returns the pool, its factory mock (which yields the string
    ``"resource"``), and its awaitable finalizer mock. The 60 second TTL
    keeps a released entry cached for the whole of a test, so nothing
    expires underneath an assertion.
    """
    factory = mocker.Mock(return_value="resource")
    finalizer = mocker.AsyncMock()
    yield ResourcePool(factory=factory, finalizer=finalizer, ttl=60), factory, finalizer


@pytest.fixture
def retired_entry_pool(mocker):
    """Build a long-TTL pool holding one entry retired while referenced.

    Returns the pool, its finalizer mock and its factory mock. The
    factory yields ``"first"`` then ``"second"``, so a test can prove
    eviction by acquiring again and getting the second object.
    """
    factory = mocker.Mock(side_effect=["first", "second"])
    finalizer = mocker.AsyncMock()
    pool = ResourcePool(factory=factory, finalizer=finalizer, ttl=60)
    return pool, finalizer, factory


@pytest.fixture
def background_loops():
    """Run event loops on daemon threads for cross-loop tests.

    Yields a ``spawn()`` returning a handle onto a freshly started
    loop: ``handle.loop`` is the loop itself, ``handle.submit(coro)``
    schedules a coroutine on it and returns the concurrent future,
    ``handle.run(coro)`` submits and waits for the result, and
    ``handle.close()`` stops, joins, and closes it. Every spawned loop
    is closed at teardown, so a test only calls ``close`` when it needs
    the loop to stop mid-test.
    """

    class BackgroundLoop:
        def __init__(self):
            self.loop = asyncio.new_event_loop()
            self._thread = threading.Thread(target=self.loop.run_forever, daemon=True)
            self._thread.start()

        def submit(self, coro):
            return asyncio.run_coroutine_threadsafe(coro, self.loop)

        def run(self, coro, timeout=5):
            return self.submit(coro).result(timeout=timeout)

        def close(self, timeout=5):
            if self.loop.is_closed():
                return
            self.loop.call_soon_threadsafe(self.loop.stop)
            self._thread.join(timeout=timeout)
            self.loop.close()

    handles = []

    def spawn():
        handle = BackgroundLoop()
        handles.append(handle)
        return handle

    yield spawn

    for handle in handles:
        handle.close()


@pytest.fixture
def stranded_loop():
    """Leave pool entries behind on a loop that has stopped running.

    Yields a ``strand(coro, *, close=True)`` that drives the coroutine
    to completion on a fresh event loop and returns
    ``(loop, result)``. The loop is closed on the way out by default,
    or merely stopped when ``close=False``, so a test can distinguish
    a closed loop from one that stopped without closing, or resume it
    to let a stale timer fire. Every loop is closed at teardown.
    """
    loops = []

    def strand(coro, *, close=True):
        loop = asyncio.new_event_loop()
        loops.append(loop)
        try:
            return loop, loop.run_until_complete(coro)
        finally:
            if close:
                loop.close()

    yield strand

    for loop in loops:
        loop.close()


@pytest.fixture
def expiry_race_pool(mocker):
    """Build a short-TTL pool whose lock can be parked via a blocker key.

    Returns the pool, its finalizer mock, the list of factory calls,
    and the event that releases the parked ``blocker`` acquire.
    """
    release_blocker = asyncio.Event()
    factory_calls = []

    async def factory(key):
        factory_calls.append(key)
        if key == "blocker":
            await release_blocker.wait()
        return f"obj-{key}"

    finalizer = mocker.AsyncMock()
    pool = ResourcePool(factory=factory, finalizer=finalizer, ttl=0.05)
    return pool, finalizer, factory_calls, release_blocker


async def _queue_behind_fired_cleanup(pool, factory_calls, queued_coroutine):
    """Race a fired TTL cleanup against an operation queued on the pool lock.

    Caches and releases ``expired`` so its TTL timer arms, parks an
    acquire of ``blocker`` inside its factory — the factory runs under
    the pool lock, so the lock stays held — then queues the given
    operation on the (FIFO) lock and waits for the timer to fire so
    its cleanup task queues behind that operation. Returns the blocker
    and queued-operation tasks.
    """
    async with pool.get("expired"):
        pass

    blocker_task = asyncio.create_task(pool.acquire("blocker"))

    async def blocker_parked():
        while "blocker" not in factory_calls:
            await asyncio.sleep(0)

    await asyncio.wait_for(blocker_parked(), timeout=2.0)

    queued_task = asyncio.create_task(queued_coroutine)

    async def cleanup_task_spawned():
        # The armed timer already counts as pending; wait until the
        # pending work is the fired timer's cleanup *task*.
        while not isinstance(pool.pending_cleanup.get("expired"), asyncio.Task):
            await asyncio.sleep(0.01)

    await asyncio.wait_for(cleanup_task_spawned(), timeout=2.0)
    return blocker_task, queued_task


@pytest.fixture
def counting_factory():
    """Create a factory that counts how many times it's called."""

    class CountingFactory:
        def __init__(self):
            self.call_count = 0

        def __call__(self, _key):
            self.call_count += 1
            return f"resource-{self.call_count}"

    return CountingFactory()


class TestResourcePool:
    @staticmethod
    @strategies.composite
    def setup(draw, *, max_key_count=5):
        """Generate a ResourcePool with varied initial resource states.

        Creates a pool with 0-max_key_count resources using the public API
        to create realistic pool states for property-based testing.

        :param draw:
            The Hypothesis draw function for generating test data.
        :param max_key_count:
            Maximum number of keys to create resources for.
        :returns:
            An async function that when called returns a tuple of
            (ResourcePool, factory, list of resources, list of keys).
        """
        factory = draw(factory_functions())
        finalizer = draw(finalizer_functions())
        pool = ResourcePool(factory=factory, finalizer=finalizer, ttl=0)
        created_resources = []
        keys = []

        async def setup():
            for i in range(draw(strategies.integers(0, max_key_count))):
                key = f"resource-{i}"
                keys.append(key)

                # Create the initial resource using public API and track it
                async with pool.get(key) as resource:
                    created_resources.append(resource)

                # The resource is now in the pool with TTL=0, so it should be immediately
                # cleaned up. We verify pool behavior through public interface

            return pool, factory, created_resources, keys

        return setup

    @pytest.mark.asyncio
    @given(setup=setup())
    async def test_get_should_return_resource_instance(self, setup):
        """Test that get returns a Resource instance.

        Given:
            A pool with various initial resource states
        When:
            get() is called with a test key
        Then:
            Should return a Resource instance
        """
        # Arrange
        pool, _, _, _ = await setup()

        # Act
        resource_acquisition = pool.get("test-key")

        # Assert
        assert isinstance(resource_acquisition, Resource)

    @pytest.mark.asyncio
    async def test_get_should_handle_none_key(self, mocker):
        """Test resource pool handles None key appropriately.

        Given:
            A resource pool
        When:
            get() is called with None key
        Then:
            Should handle None key as a valid cache key
        """
        # Arrange
        mock_resource = mocker.Mock()
        mock_factory = mocker.Mock(return_value=mock_resource)
        pool = ResourcePool(factory=mock_factory, ttl=0)

        # Act & assert
        # None should be treated as a valid key
        async with pool.get(None) as resource:
            assert resource is mock_resource

        # Resource should be cleaned up after use
        assert pool.stats.total_entries == 0

    @pytest.mark.asyncio
    async def test_acquire_should_share_one_resource_when_contended_on_same_key(
        self, counting_factory
    ):
        """Test concurrent operations on same key maintain consistency.

        Given:
            A resource pool with TTL
        When:
            Multiple coroutines acquire and release the same key concurrently
        Then:
            Resource pool should maintain consistency and not leak resources
        """
        # Arrange
        pool = ResourcePool(factory=counting_factory, ttl=0.1)

        # Act
        async def acquire_release_worker():
            async with pool.get("shared-key") as resource:
                await asyncio.sleep(0.01)  # Small delay to increase contention
                return resource

        # Run multiple concurrent workers
        tasks = [acquire_release_worker() for _ in range(10)]
        results = await asyncio.gather(*tasks)

        # Assert
        # All workers should get the same resource instance (cached)
        assert len(set(results)) == 1  # All got the same resource
        # Factory should only be called once despite concurrent access
        assert counting_factory.call_count == 1
        # Pool should be consistent after all operations
        assert pool.stats.total_entries <= 1  # 0 or 1 depending on TTL timing

    @pytest.mark.asyncio
    async def test_acquire_should_cancel_pending_cleanup_when_reacquired_within_ttl(
        self, mocker
    ):
        """Test TTL cleanup is cancelled when resource is reacquired.

        Given:
            A pool with TTL > 0 and a scheduled cleanup
        When:
            The resource is reacquired before TTL expires
        Then:
            Cleanup should be cancelled and resource kept
        """
        # Arrange
        mock_resource = mocker.Mock()
        mock_factory = mocker.Mock(return_value=mock_resource)
        mock_finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=mock_factory, finalizer=mock_finalizer, ttl=0.1)

        key = "ttl-cancel-test"

        # Act
        # Acquire and release to schedule cleanup
        async with pool.get(key):
            pass

        # Should be scheduled for cleanup
        assert pool.stats.total_entries == 1
        assert pool.stats.referenced_entries == 0
        assert pool.stats.pending_cleanup == 1

        # Reacquire the resource while cleanup is still waiting
        async with pool.get(key) as resource:
            # Assert - cleanup should be cancelled and resource reused
            assert resource is mock_resource
            assert pool.stats.referenced_entries == 1

        # After reacquisition and release, verify finalizer wasn't called
        # (which would indicate the original resource was preserved)
        mock_finalizer.assert_not_called()

        # Resource should still exist due to TTL
        assert pool.stats.total_entries == 1
        assert pool.stats.referenced_entries == 0

    @pytest.mark.asyncio
    async def test_acquire_should_cancel_in_flight_cleanup_when_reacquired_after_expiry(
        self, expiry_race_pool
    ):
        """Test acquire cancels a fired cleanup racing on the pool lock.

        Given:
            A pool whose expired key's TTL timer has fired while the
            pool lock is held by another key's acquire, so the
            spawned cleanup task and a queued re-acquire of the
            expired key both wait on the lock with the re-acquire
            first
        When:
            The lock holder completes and the queued re-acquire runs
        Then:
            It should cancel the in-flight cleanup, return the cached
            object without re-invoking the factory or finalizer, and
            leave the key without pending cleanup
        """
        # Arrange
        pool, finalizer, factory_calls, release_blocker = expiry_race_pool
        blocker_task, reacquire_task = await _queue_behind_fired_cleanup(
            pool, factory_calls, pool.acquire("expired")
        )

        # Act
        release_blocker.set()
        acquired = await reacquire_task
        await blocker_task

        # Assert
        assert acquired == "obj-expired"
        assert factory_calls.count("expired") == 1
        finalizer.assert_not_awaited()
        assert "expired" not in pool.pending_cleanup
        assert pool.stats.total_entries == 2

    @pytest.mark.asyncio
    async def test_release_should_decrement_reference_counts(self, mocker):
        """Test releasing resources decrements reference counts properly.

        Given:
            A pool with resources that have active references
        When:
            Resources are released via pool.release()
        Then:
            Should properly decrement ref counts or cleanup and remove resources
        """
        # Arrange - Create pool with TTL to keep resources after context exit
        mock_factory = mocker.Mock()
        pool = ResourcePool(factory=mock_factory, ttl=60)

        # Create some test resources
        test_keys = ["key1", "key2", "key3"]
        for i, key in enumerate(test_keys):
            mock_factory.return_value = f"resource-{i}"
            async with pool.get(key):
                pass  # Creates and caches the resource

        # Verify initial state
        assert pool.stats.total_entries == len(test_keys)
        assert pool.stats.referenced_entries == 0  # All released from context

        # Now manually acquire some resources to test release
        await pool.acquire("key1")
        await pool.acquire("key2")

        assert pool.stats.referenced_entries == 2

        # Act & assert
        await pool.release("key1")
        assert pool.stats.referenced_entries == 1

        await pool.release("key2")
        assert pool.stats.referenced_entries == 0

    @pytest.mark.asyncio
    async def test_release_should_not_affect_existing_resources_when_key_nonexistent(
        self, counting_factory
    ):
        """Test releasing a nonexistent key is a silent no-op.

        Given:
            A pool with some existing resources
        When:
            Release is called with a nonexistent key
        Then:
            Should exit without affecting existing resources
        """
        # Arrange
        pool = ResourcePool(factory=counting_factory, ttl=1.0)

        # Create some resources to establish initial state
        keys = ["key1", "key2"]
        for key in keys:
            async with pool.get(key):
                pass  # Just acquire and release to populate cache

        initial_cache_size = pool.stats.total_entries

        # Act & assert
        # Try to release a nonexistent key
        await pool.release("nonexistent")

        # Should not affect existing resources
        assert pool.stats.total_entries == initial_cache_size
        # All keys should have zero references (since they were released)
        assert pool.stats.referenced_entries == 0

    @pytest.mark.asyncio
    async def test_release_should_raise_value_error_when_zero_reference_count(
        self, ttl_pool
    ):
        """Test releasing key with zero ref count raises ValueError.

        Given:
            A pool with a resource that has zero reference count
        When:
            Release is called on that key
        Then:
            Should raise ValueError indicating reference count is already
            zero
        """
        # Arrange
        # Acquire and release once to get the reference count to 0; the
        # long TTL keeps the entry cached afterwards.
        pool, _, _ = ttl_pool
        unique_key = "test-zero-ref-count"
        async with pool.get(unique_key):
            pass

        # Act & assert
        with pytest.raises(
            ValueError,
            match=f"Reference count for key '{unique_key}' is already 0",
        ):
            await pool.release(unique_key)

    @pytest.mark.asyncio
    async def test_release_should_finalize_immediately_when_ttl_zero(
        self, resource_pool_immediate_cleanup
    ):
        """Test TTL=0 performs immediate cleanup as expected.

        Given:
            A resource pool with TTL=0
        When:
            A resource is acquired and released
        Then:
            Should perform immediate cleanup without scheduling
        """
        # Arrange
        pool, factory, finalizer = resource_pool_immediate_cleanup
        mock_resource = factory.return_value

        # Act
        async with pool.get("test-key") as resource:
            # While in context, resource should exist
            assert resource is mock_resource
            assert pool.stats.total_entries == 1
            assert pool.stats.referenced_entries == 1

        # Assert
        # After context exit with TTL=0, should be immediately cleaned up
        assert pool.stats.total_entries == 0
        assert pool.stats.referenced_entries == 0
        assert pool.stats.pending_cleanup == 0  # No pending cleanup tasks
        finalizer.assert_awaited_once_with(mock_resource)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("ttl", [0, 0.1, 1, 1.1, 10, 10.1])
    async def test_release_should_schedule_cleanup_according_to_ttl(self, mocker, ttl):
        """Test specific TTL values defer or run cleanup accordingly.

        Given:
            A pool with specific TTL value
        When:
            A resource is acquired and released
        Then:
            It should finalize immediately for TTL 0 and defer
            cleanup for positive TTLs
        """
        # Arrange
        mock_factory = mocker.Mock(return_value=mocker.Mock(name="test-obj"))
        mock_finalizer = mocker.Mock()
        pool = ResourcePool(factory=mock_factory, finalizer=mock_finalizer, ttl=ttl)

        # Act
        async with pool.get("test-key"):
            pass

        # Assert
        if ttl == 0:
            mock_finalizer.assert_called_once()
            assert pool.stats.total_entries == 0
            assert pool.stats.pending_cleanup == 0
        else:
            mock_finalizer.assert_not_called()
            assert pool.stats.total_entries == 1
            assert pool.stats.pending_cleanup == 1

    @pytest.mark.asyncio
    async def test_release_should_finalize_after_ttl_elapses(self, mocker):
        """Test TTL-based cleanup schedules and executes properly.

        Given:
            A pool with TTL > 0
        When:
            A resource reference count reaches 0
        Then:
            Should schedule cleanup after TTL expires
        """
        # Arrange
        mock_resource = mocker.Mock()
        mock_factory = mocker.Mock(return_value=mock_resource)
        mock_finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=mock_factory, finalizer=mock_finalizer, ttl=0.1)

        key = "ttl-test"

        # Act
        # Acquire and immediately release
        async with pool.get(key) as resource:
            assert resource is mock_resource
            assert pool.stats.total_entries == 1
            assert pool.stats.referenced_entries == 1

        # Resource should still be in cache with cleanup deferred
        assert pool.stats.total_entries == 1
        assert pool.stats.referenced_entries == 0
        assert pool.stats.pending_cleanup == 1
        mock_finalizer.assert_not_called()

        # Assert
        # Wait for cleanup to complete using polling with timeout
        start_time = time.time()
        while (key in pool.pending_cleanup) and (time.time() - start_time < 2.0):
            await asyncio.sleep(0.01)

        # Resource should now be cleaned up
        assert pool.stats.total_entries == 0
        mock_finalizer.assert_called_once_with(mock_resource)

    @pytest.mark.asyncio
    async def test_release_should_evict_entry_when_finalizer_raises(self, mocker):
        """Test finalizer exceptions are caught and logged.

        Given:
            A finalizer that raises an exception
        When:
            Resource cleanup occurs
        Then:
            Exception should be caught and resource still removed
        """
        # Arrange
        mock_resource = mocker.Mock()
        mock_factory = mocker.Mock(return_value=mock_resource)

        async def failing_finalizer(_):
            raise ValueError("Finalizer failed")

        pool = ResourcePool(factory=mock_factory, finalizer=failing_finalizer)

        key = "test"

        # Act & assert
        # This should not raise despite finalizer failing
        async with pool.get(key):
            pass

        # Resource should still be cleaned up
        assert pool.stats.total_entries == 0

    @pytest.mark.asyncio
    async def test_release_should_evict_entry_when_finalizer_raises_base_exception(
        self, mocker
    ):
        """Test a cancelled finalizer still evicts the cache entry.

        Given:
            A ``ttl=0`` pool whose finalizer raises
            ``CancelledError`` — a ``BaseException``, not an
            ``Exception`` — on its first call, modelling cleanup that
            runs under a cancelled teardown
        When:
            A resource is acquired and released, driving immediate
            cleanup whose finalizer raises
        Then:
            The ``CancelledError`` propagates, but the torn-down entry
            is still evicted, so the next acquire is a cache miss that
            builds a fresh resource via the factory rather than handing
            back the finalized one
        """

        # Arrange
        finalizer_calls = {"count": 0}

        async def finalizer(obj):
            finalizer_calls["count"] += 1
            if finalizer_calls["count"] == 1:
                # First cleanup runs under cancellation.
                raise asyncio.CancelledError()

        factory = mocker.Mock(
            side_effect=[
                SimpleNamespace(name="first"),
                SimpleNamespace(name="second"),
            ]
        )
        pool = ResourcePool(factory=factory, finalizer=finalizer, ttl=0)

        # Act
        # Acquire then release: rc -> 0 drives immediate cleanup, whose
        # finalizer raises CancelledError out of the release.
        with pytest.raises(asyncio.CancelledError):
            async with pool.get("key"):
                pass

        # Assert
        # The finalized resource must not survive in the cache.
        assert pool.stats.total_entries == 0
        # The next acquire is therefore a miss that builds a fresh
        # resource, never the torn-down one.
        async with pool.get("key") as resource:
            assert resource.name == "second"
        assert factory.call_count == 2

    @pytest.mark.asyncio
    async def test_release_should_finalize_before_returning_when_entry_retired(
        self, ttl_pool
    ):
        """Test the release of a retired entry finalizes it inline.

        Given:
            A long-TTL pool holding a referenced entry that expire_all
            has retired.
        When:
            The last reference is released.
        Then:
            It should have awaited the finalizer and emptied the
            partition by the time release returns, spawning no task to
            do it later — a task would be orphaned by a loop that stops
            straight after the release.
        """
        # Arrange
        pool, _, finalizer = ttl_pool
        await pool.acquire("key")
        await pool.expire_all()
        tasks_before = asyncio.all_tasks()

        # Act
        await pool.release("key")

        # Assert
        finalizer.assert_awaited_once_with("resource")
        assert pool.stats.total_entries == 0
        assert not pool.pending_cleanup
        assert asyncio.all_tasks() == tasks_before

    @pytest.mark.asyncio
    async def test_release_should_finalize_before_returning_when_entry_expired(
        self, mocker
    ):
        """Test the release of an expired entry finalizes it inline.

        Given:
            A long-TTL pool holding a referenced entry that expire() has
            marked for its key.
        When:
            The last reference is released.
        Then:
            It should have awaited the finalizer and emptied the pool by
            the time release returns, spawning no task to do it later,
            the same way a whole-pool retirement is settled.
        """
        # Arrange
        mock_finalizer = mocker.AsyncMock()
        pool = ResourcePool(
            factory=mocker.Mock(return_value="resource"),
            finalizer=mock_finalizer,
            ttl=60,
        )
        await pool.acquire("key")
        await pool.expire("key")
        tasks_before = asyncio.all_tasks()

        # Act
        await pool.release("key")

        # Assert
        mock_finalizer.assert_awaited_once_with("resource")
        assert pool.stats.total_entries == 0
        assert not pool.pending_cleanup
        assert asyncio.all_tasks() == tasks_before

    @pytest.mark.asyncio
    async def test_release_should_evict_retired_entry_when_cancelled_mid_finalizer(
        self, mocker
    ):
        """Test cancelling a release mid-finalize still evicts the entry.

        Given:
            A long-TTL pool holding an entry retired by ``expire`` while
            still referenced, whose finalizer parks on an event so the
            release is suspended inside it.
        When:
            The releasing task is cancelled while the finalizer is
            parked.
        Then:
            It should raise ``CancelledError`` and still evict the entry,
            so no torn-down resource is handed back to a later acquire.
        """
        # Arrange
        parked = asyncio.Event()
        factory = mocker.Mock(side_effect=["first", "second"])

        async def finalizer(_):
            parked.set()
            await asyncio.Event().wait()

        pool = ResourcePool(factory=factory, finalizer=finalizer, ttl=60)
        await pool.acquire("key")
        await pool.expire("key")
        release = asyncio.ensure_future(pool.release("key"))
        # Bounded: a regression that never enters the finalizer must
        # fail here rather than idle out the pool's own TTL.
        await asyncio.wait_for(parked.wait(), timeout=2.0)

        # Act & assert
        release.cancel()
        with pytest.raises(asyncio.CancelledError):
            await release

        assert pool.stats.total_entries == 0
        assert await pool.acquire("key") == "second"

    def test_release_should_finalize_retired_entry_when_loop_ends_immediately(
        self, mocker, caplog, stranded_loop
    ):
        """Test a release during shutdown closes the resource before returning.

        Given:
            A long-TTL pool holding an entry retired by ``expire`` while
            still referenced, on a loop that closes as soon as the last
            reference is released.
        When:
            That release is awaited and the loop is closed with no
            further iterations.
        Then:
            It should have run the finalizer to completion before
            returning, leaving no pending task on the loop and no
            destroyed-while-pending report from asyncio.
        """
        # Arrange
        closed = []

        # The suspension point is load-bearing: a finalizer that never
        # awaits would finish inside a single loop step, so this test
        # could not tell an inline finalize from deferred work the loop
        # happens to run before closing.
        async def finalizer(obj):
            await asyncio.sleep(0)
            closed.append(obj)

        pool = ResourcePool(
            factory=mocker.Mock(return_value="obj"), finalizer=finalizer, ttl=60
        )

        async def acquire_retire_release():
            await pool.acquire("key")
            await pool.expire("key")
            await pool.release("key")

        # Act
        with caplog.at_level(logging.ERROR, logger="asyncio"):
            loop, _ = stranded_loop(acquire_retire_release(), close=False)
            pending = asyncio.all_tasks(loop)
            loop.close()
            gc.collect()

        # Assert
        assert closed == ["obj"]
        assert pending == set()
        assert not [
            record
            for record in caplog.records
            if "Task was destroyed" in record.getMessage()
        ]

    @pytest.mark.asyncio
    @given(
        operations=strategies.lists(
            strategies.tuples(
                strategies.sampled_from(["acquire", "release"]),
                strategies.sampled_from(["a", "b", "c"]),
            ),
            max_size=30,
        )
    )
    async def test_release_should_maintain_bookkeeping_invariants(self, operations):
        """Test acquire and release keep TTL bookkeeping consistent.

        Given:
            Any interleaved sequence of acquire and release
            operations over a small key domain, where releases are
            applied only while a reference is held
        When:
            The sequence is applied step by step to a long-TTL pool
        Then:
            It should keep total entries equal to the keys ever
            acquired, referenced entries equal to the keys with live
            references, and pending cleanup on exactly the keys whose
            references all released
        """
        # Arrange
        pool = ResourcePool(factory=lambda key: f"obj-{key}", ttl=60)
        model_refcount = {}

        # Act & assert
        for operation, key in operations:
            if operation == "acquire":
                await pool.acquire(key)
                model_refcount[key] = model_refcount.get(key, 0) + 1
            elif model_refcount.get(key, 0) > 0:
                await pool.release(key)
                model_refcount[key] -= 1

            stats = pool.stats
            assert stats.total_entries == len(model_refcount)
            assert stats.referenced_entries == sum(
                1 for count in model_refcount.values() if count > 0
            )
            assert set(pool.pending_cleanup) == {
                key for key, count in model_refcount.items() if count == 0
            }

    @pytest.mark.asyncio
    async def test_expire_should_leave_other_entries_when_key_expired(self):
        """Test expiring one key retires only that key.

        Given:
            A pool holding two unreferenced entries under a long TTL.
        When:
            One of them is expired.
        Then:
            It should finalize that entry alone, leaving the other cached
            and its resource untouched.
        """
        # Arrange
        finalized = []

        async def factory(key):
            return f"obj-{key}"

        async def finalizer(resource):
            finalized.append(resource)

        pool = ResourcePool(factory, finalizer=finalizer, ttl=3600)
        async with pool:
            await pool.acquire("key1")
            await pool.acquire("key2")
            await pool.release("key1")
            await pool.release("key2")
            assert pool.stats.total_entries == 2

            # Act
            await pool.expire("key1")

            # Assert
            assert finalized == ["obj-key1"]
            assert pool.stats.total_entries == 1
            assert "key2" in pool.pending_cleanup

    @pytest.mark.asyncio
    async def test_expire_should_finalize_immediately_when_unreferenced(self, ttl_pool):
        """Test expiring an unreferenced entry skips its remaining TTL.

        Given:
            A long-TTL pool holding an unreferenced entry whose TTL timer is
            pending.
        When:
            expire() is called with that entry's key.
        Then:
            It should run the finalizer immediately, remove the entry, and
            leave no pending cleanup — no TTL wait.
        """
        # Arrange
        pool, _, finalizer = ttl_pool
        async with pool.get("key"):
            pass  # Released: the TTL timer is now armed.
        assert "key" in pool.pending_cleanup

        # Act
        await pool.expire("key")

        # Assert
        finalizer.assert_awaited_once_with("resource")
        assert pool.stats.total_entries == 0
        assert not pool.pending_cleanup

    @pytest.mark.asyncio
    async def test_expire_should_not_finalize_while_referenced(self, ttl_pool):
        """Test expiring a referenced entry leaves the in-flight user alone.

        Given:
            A long-TTL pool holding an entry with an active reference.
        When:
            expire() is called.
        Then:
            It should leave the resource unfinalized and the entry cached —
            an in-flight user is never torn out from under.
        """
        # Arrange
        pool, _, finalizer = ttl_pool
        await pool.acquire("key")

        # Act
        await pool.expire("key")

        # Assert
        finalizer.assert_not_awaited()
        assert pool.stats.total_entries == 1

    @pytest.mark.asyncio
    async def test_expire_should_finalize_at_last_release_when_expired(self, ttl_pool):
        """Test an expired entry is finalized as soon as its users drain.

        Given:
            A long-TTL pool holding an entry that has been expired while
            still referenced.
        When:
            The last reference is released.
        Then:
            It should finalize the resource promptly, without waiting out
            the TTL.
        """
        # Arrange
        pool, _, finalizer = ttl_pool
        await pool.acquire("key")
        await pool.expire("key")

        # Act
        await pool.release("key")
        await asyncio.sleep(0)

        # Assert
        finalizer.assert_awaited_once_with("resource")
        assert pool.stats.total_entries == 0
        assert not pool.pending_cleanup

    @pytest.mark.asyncio
    async def test_expire_should_resurrect_entry_when_reacquired(self, ttl_pool):
        """Test re-acquiring an expired entry cancels its doom.

        Given:
            A long-TTL pool holding a referenced entry that has been
            expired.
        When:
            The key is acquired again before the references drain and both
            references are then released.
        Then:
            It should keep the entry cached on the normal TTL schedule — the
            re-acquire resurrects it — with the finalizer never called.
        """
        # Arrange
        pool, _, finalizer = ttl_pool
        async with pool:
            await pool.acquire("key")
            await pool.expire("key")

            # Act
            await pool.acquire("key")  # Resurrection: clears the mark.
            await pool.release("key")
            await pool.release("key")

            # Assert
            finalizer.assert_not_awaited()
            assert pool.stats.total_entries == 1
            assert "key" in pool.pending_cleanup  # Normal TTL schedule.

    @pytest.mark.asyncio
    async def test_expire_should_not_raise_when_key_unknown(self, ttl_pool):
        """Test expiring an uncached key is a silent no-op.

        Given:
            A pool that has never cached the given key.
        When:
            expire() is called with that key.
        Then:
            It should neither raise nor invoke the finalizer.
        """
        # Arrange
        pool, _, finalizer = ttl_pool

        # Act
        await pool.expire("missing")

        # Assert
        finalizer.assert_not_awaited()
        assert pool.stats.total_entries == 0

    @pytest.mark.asyncio
    async def test_expire_should_cancel_in_flight_cleanup_when_expired_after_ttl(
        self, expiry_race_pool
    ):
        """Test expire cancels a fired cleanup racing on the pool lock.

        Given:
            A pool whose expired key's TTL timer has fired while the
            pool lock is held by another key's acquire, so the
            spawned cleanup task and a queued expiry of the expired
            key both wait on the lock with the expiry first
        When:
            The lock holder completes and the queued expiry runs
        Then:
            It should cancel the in-flight cleanup, still run the
            finalizer exactly once, and evict the entry
        """
        # Arrange
        pool, finalizer, factory_calls, release_blocker = expiry_race_pool
        blocker_task, clear_task = await _queue_behind_fired_cleanup(
            pool, factory_calls, pool.expire("expired")
        )

        # Act
        release_blocker.set()
        await clear_task
        await blocker_task

        # Assert
        finalizer.assert_awaited_once_with("obj-expired")
        assert "expired" not in pool.pending_cleanup
        assert pool.stats.total_entries == 1

    @pytest.mark.asyncio
    async def test_expire_all_should_finalize_idle_entries_immediately(self, mocker):
        """Test expire_all retires idle entries without closing one in use.

        Given:
            A pool holding one idle entry awaiting its TTL and one entry
            still referenced.
        When:
            expire_all is awaited.
        Then:
            It should finalize the idle entry immediately, leaving the
            referenced one cached with no pending cleanup of its own.
        """
        # Arrange
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=lambda key: key, finalizer=finalizer, ttl=60)
        async with pool.get("idle"):
            pass
        await pool.acquire("held")

        # Act
        await pool.expire_all()

        # Assert
        assert list(finalizer.await_args_list) == [mocker.call("idle")]
        assert pool.stats.total_entries == 1
        assert not pool.pending_cleanup

    @pytest.mark.asyncio
    async def test_expire_all_should_finalize_referenced_entry_on_last_release(
        self, mocker
    ):
        """Test a retired entry is finalized once its users drain.

        Given:
            A pool whose only entry is referenced and has been retired
            by expire_all.
        When:
            The last reference is released.
        Then:
            It should finalize that entry then, ending with an empty
            partition and no pending cleanup.
        """
        # Arrange
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=lambda key: key, finalizer=finalizer, ttl=60)
        await pool.acquire("held")
        await pool.expire_all()

        # Act
        await pool.release("held")

        # Assert
        finalizer.assert_awaited_once_with("held")
        assert pool.stats.total_entries == 0
        assert not pool.pending_cleanup

    @pytest.mark.asyncio
    async def test_expire_all_should_keep_entry_cached_when_reacquired_before_release(
        self, mocker
    ):
        """Test re-acquiring a retired entry before its release keeps it.

        Given:
            A pool whose only entry is referenced and has been retired
            by expire_all.
        When:
            The same key is acquired again and both references are then
            released.
        Then:
            It should hand back the cached object without rebuilding it
            and, with the retirement cleared by the re-acquire, keep the
            entry cached under its TTL rather than finalizing it.
        """
        # Arrange
        factory = mocker.Mock(return_value="obj")
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=factory, finalizer=finalizer, ttl=60)
        await pool.acquire("key")
        await pool.expire_all()

        # Act
        acquired = await pool.acquire("key")
        await pool.release("key")
        await pool.release("key")
        for _ in range(5):
            await asyncio.sleep(0)

        # Assert
        assert acquired == "obj"
        assert factory.call_count == 1
        finalizer.assert_not_awaited()
        assert pool.stats.total_entries == 1
        assert "key" in pool.pending_cleanup

    @pytest.mark.asyncio
    async def test_expire_all_should_not_raise_when_partition_empty(self, mocker):
        """Test retiring a partition that holds nothing is a no-op.

        Given:
            A pool the calling loop has never cached anything in.
        When:
            expire_all is awaited.
        Then:
            It should return without raising, invoke no finalizer, and
            leave the partition empty — the parity of expire with an
            unknown key.
        """
        # Arrange
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(
            factory=mocker.Mock(return_value="obj"), finalizer=finalizer, ttl=60
        )

        # Act
        await pool.expire_all()

        # Assert
        finalizer.assert_not_awaited()
        assert pool.stats.total_entries == 0
        assert not pool.pending_cleanup

    @pytest.mark.asyncio
    async def test_expire_all_should_finalize_retired_entry_when_release_is_late(
        self, mocker
    ):
        """Test a late release finalizes the entry it was taken against.

        Given:
            A pool whose only entry is referenced and has been retired
            by expire_all.
        When:
            The outstanding reference is released and the same key is
            acquired again afterwards.
        Then:
            It should finalize the retired object exactly once on that
            release and build a fresh object for the new acquire, so a
            release landing after retirement can never finalize a
            resource handed out since.
        """
        # Arrange
        factory = mocker.Mock(side_effect=["first", "second"])
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=factory, finalizer=finalizer, ttl=60)
        await pool.acquire("key")
        await pool.expire_all()

        # Act
        await pool.release("key")
        reacquired = await pool.acquire("key")

        # Assert
        finalizer.assert_awaited_once_with("first")
        assert factory.call_count == 2
        assert reacquired == "second"
        assert pool.stats.referenced_entries == 1

    @pytest.mark.asyncio
    @given(reference_counts=strategies.lists(strategies.integers(0, 3), max_size=5))
    @settings(max_examples=25, deadline=None)
    async def test_expire_all_should_finalize_every_entry_exactly_once(
        self, reference_counts
    ):
        """Test retirement drains a whole partition without double finalizing.

        Given:
            Any partition of up to five distinct keys whose entries
            carry reference counts between zero and three.
        When:
            expire_all is awaited and every outstanding reference is
            then released.
        Then:
            It should finalize each cached object exactly once and end
            with an empty partition holding no pending cleanup.
        """
        # Arrange
        finalized = []

        async def finalizer(obj):
            finalized.append(obj)

        pool = ResourcePool(
            factory=lambda key: f"obj-{key}", finalizer=finalizer, ttl=60
        )
        keys = [f"key-{index}" for index in range(len(reference_counts))]
        for key, count in zip(keys, reference_counts):
            # One seeding reference caches the entry; the extra
            # acquires and the single release leave `count` behind.
            await pool.acquire(key)
            for _ in range(count):
                await pool.acquire(key)
            await pool.release(key)

        # Act
        await pool.expire_all()
        for key, count in zip(keys, reference_counts):
            for _ in range(count):
                await pool.release(key)

        # Assert
        assert sorted(finalized) == sorted(f"obj-{key}" for key in keys)
        assert pool.stats.total_entries == 0
        assert not pool.pending_cleanup

    @pytest.mark.asyncio
    async def test_expire_all_should_finalize_idle_entry_when_ttl_zero(self, mocker):
        """Test retirement holds for a pool with no idle grace at all.

        Given:
            A pool with no TTL holding one idle entry and one still
            referenced.
        When:
            expire_all() is awaited and the outstanding reference is
            then released.
        Then:
            It should finalize both without ever scheduling pending
            cleanup, since a zero TTL leaves nothing to defer to.
        """
        # Arrange
        mock_finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=lambda key: key, finalizer=mock_finalizer, ttl=0)
        async with pool.get("idle"):
            pass
        await pool.acquire("held")

        # Act
        await pool.expire_all()
        await pool.release("held")

        # Assert
        assert sorted(c.args[0] for c in mock_finalizer.await_args_list) == [
            "held",
            "idle",
        ]
        assert pool.stats.total_entries == 0
        assert not pool.pending_cleanup

    @pytest.mark.asyncio
    async def test_expire_all_should_evict_every_entry_when_finalizer_raises(self):
        """Test one failing finalizer does not spare the entries after it.

        Given:
            A long-TTL pool of three idle entries whose finalizer raises
            for the middle key.
        When:
            expire_all() is awaited.
        Then:
            It should attempt every finalizer, swallow the error, and
            leave the pool empty, so a failed teardown cannot leave a
            torn-down resource cached.
        """
        # Arrange
        attempted = []

        async def finalizer(obj):
            attempted.append(obj)
            if obj == "obj-b":
                raise RuntimeError("teardown failed")

        pool = ResourcePool(
            factory=lambda key: f"obj-{key}", finalizer=finalizer, ttl=60
        )
        for key in ("a", "b", "c"):
            async with pool.get(key):
                pass

        # Act
        await pool.expire_all()

        # Assert
        assert attempted == ["obj-a", "obj-b", "obj-c"]
        assert pool.stats.total_entries == 0
        assert not pool.pending_cleanup

    @pytest.mark.asyncio
    async def test_expire_all_should_finish_sweep_when_finalizer_cancelled(self):
        """Test a cancelled finalizer does not strand the keys behind it.

        Given:
            A long-TTL pool of three idle entries whose finalizer raises
            CancelledError for the first key.
        When:
            expire_all() is awaited.
        Then:
            It should retire the remaining two before propagating that
            CancelledError, so retirement under a cancelled teardown
            still reaches every key rather than leaking the tail.
        """
        # Arrange
        attempted = []

        async def finalizer(obj):
            attempted.append(obj)
            if obj == "obj-a":
                raise asyncio.CancelledError

        pool = ResourcePool(
            factory=lambda key: f"obj-{key}", finalizer=finalizer, ttl=60
        )
        for key in ("a", "b", "c"):
            async with pool.get(key):
                pass

        # Act
        with pytest.raises(asyncio.CancelledError):
            await pool.expire_all()

        # Assert
        assert attempted == ["obj-a", "obj-b", "obj-c"]
        assert pool.stats.total_entries == 0
        assert not pool.pending_cleanup

    @pytest.mark.asyncio
    async def test_expire_all_should_reraise_first_failure_when_finalizers_cancelled(
        self,
    ):
        """Test the failure propagated after the sweep is the first one raised.

        Given:
            A long-TTL pool of three idle entries whose finalizer raises
            a distinct CancelledError instance for each of the first two
            keys in cache order.
        When:
            expire_all() is awaited.
        Then:
            It should attempt every finalizer, leave the pool empty with
            no pending cleanup, and propagate the first key's instance
            rather than the last.
        """
        # Arrange
        attempted = []
        failures = {"obj-a": asyncio.CancelledError(), "obj-b": asyncio.CancelledError()}

        async def finalizer(obj):
            attempted.append(obj)
            if obj in failures:
                raise failures[obj]

        pool = ResourcePool(
            factory=lambda key: f"obj-{key}", finalizer=finalizer, ttl=60
        )
        for key in ("a", "b", "c"):
            async with pool.get(key):
                pass

        # Act
        with pytest.raises(asyncio.CancelledError) as raised:
            await pool.expire_all()

        # Assert
        assert raised.value is failures["obj-a"]
        assert attempted == ["obj-a", "obj-b", "obj-c"]
        assert pool.stats.total_entries == 0
        assert not pool.pending_cleanup

    @pytest.mark.asyncio
    async def test_expire_all_should_cancel_in_flight_cleanup_when_retired_after_ttl(
        self, expiry_race_pool
    ):
        """Test retirement cancels a fired cleanup racing on the pool lock.

        Given:
            A pool whose expired key's TTL timer has fired while the
            pool lock is held by another key's acquire, so the spawned
            cleanup task and a queued expire_all both wait on the lock
            with the retirement first.
        When:
            The lock holder completes and the queued retirement runs.
        Then:
            It should cancel the in-flight cleanup, still run the
            finalizer exactly once, and evict the entry, i.e., the parity
            of expire for the whole-pool primitive.
        """
        # Arrange
        pool, finalizer, factory_calls, release_blocker = expiry_race_pool
        blocker_task, retire_task = await _queue_behind_fired_cleanup(
            pool, factory_calls, pool.expire_all()
        )

        # Act
        release_blocker.set()
        await retire_task
        await blocker_task

        # Assert
        finalizer.assert_awaited_once_with("obj-expired")
        assert "expired" not in pool.pending_cleanup
        assert pool.stats.total_entries == 1

    @pytest.mark.asyncio
    async def test_clear_should_finalize_all_resources(self, mocker):
        """Test clearing the pool calls finalizer on all resources.

        Given:
            A pool with resources
        When:
            Clear is called without specific key
        Then:
            All resources should be finalized and cache cleared
        """
        # Arrange - Create pool with TTL to keep resources after context exit
        mock_factory = mocker.Mock()
        mock_finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=mock_factory, finalizer=mock_finalizer, ttl=60)

        # Create some resources
        for i in range(3):
            mock_factory.return_value = mocker.Mock(name=f"resource-{i}")
            async with pool.get(f"key-{i}"):
                pass  # Creates and caches the resource

        # Verify initial state
        assert pool.stats.total_entries == 3

        # Act
        await pool.clear()

        # Assert
        # All resources should be cleaned up and cache cleared
        assert pool.stats.total_entries == 0

        # Finalizer should have been called for all resources
        assert mock_finalizer.call_count == 3

    @pytest.mark.asyncio
    async def test_stats_should_return_accurate_counts(self, mocker):
        """Test stats method returns accurate cache statistics.

        Given:
            A pool with various resource states
        When:
            Stats property is accessed
        Then:
            Should return accurate counts for entries, references, and pending
            timers or tasks
        """
        # Arrange
        mock_factory = mocker.Mock()
        mock_finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=mock_factory, finalizer=mock_finalizer, ttl=0.1)

        # Guard: a fresh pool reports zero across all stats.
        stats = pool.stats
        assert stats.total_entries == 0
        assert stats.referenced_entries == 0
        assert stats.pending_cleanup == 0

        # Act
        # Add some resources
        mock_factory.side_effect = [mocker.Mock() for _ in range(3)]

        async with pool.get("key1"):  # ref_count = 1 while in context
            async with pool.get("key2"):  # ref_count = 1 while in context
                async with pool.get("key3"):  # will be released immediately
                    # Assert while all resources are active
                    stats = pool.stats
                    assert stats.total_entries == 3
                    assert stats.referenced_entries == 3  # All active
                    assert stats.pending_cleanup == 0  # None scheduled yet

    def test_stats_should_raise_when_no_running_loop(self, mocker):
        """Test reading stats outside a running loop is an error.

        Given:
            A pool read from synchronous code, with no event loop
            running.
        When:
            The stats property is accessed.
        Then:
            It should raise RuntimeError, since a partition — and so
            the statistics describing one — only exists relative to a
            running loop.
        """
        # Arrange
        pool = ResourcePool(factory=mocker.Mock(return_value="obj"), ttl=60)

        # Act & assert
        with pytest.raises(RuntimeError, match="no running event loop"):
            pool.stats

    def test_pending_cleanup_should_raise_when_no_running_loop(self, mocker):
        """Test reading pending cleanup outside a running loop is an error.

        Given:
            A pool read from synchronous code, with no event loop
            running.
        When:
            The pending_cleanup property is accessed.
        Then:
            It should raise RuntimeError, for the same reason stats
            does: the pending work belongs to a loop's partition.
        """
        # Arrange
        pool = ResourcePool(factory=mocker.Mock(return_value="obj"), ttl=60)

        # Act & assert
        with pytest.raises(RuntimeError, match="no running event loop"):
            pool.pending_cleanup

    @pytest.mark.asyncio
    async def test___aexit___should_clear_resources(self, mocker):
        """Test ResourcePool as async context manager clears all on exit.

        Given:
            A ResourcePool with resources
        When:
            Used as async context manager and then exited
        Then:
            Should clear all resources on exit
        """
        # Arrange
        mock_resource = mocker.Mock()
        mock_factory = mocker.Mock(return_value=mock_resource)
        mock_finalizer = mocker.AsyncMock()

        # Act & assert
        async with ResourcePool(factory=mock_factory, finalizer=mock_finalizer) as pool:
            async with pool.get("test-key"):
                assert pool.stats.total_entries == 1

        # After context exit, cache should be cleared
        assert pool.stats.total_entries == 0
        mock_finalizer.assert_called_once_with(mock_resource)

    def test_acquire_should_isolate_entries_when_two_loops_run_concurrently(
        self, background_loops
    ):
        """Test one pool serves two live loops through separate partitions.

        Given:
            One pool and two event loops running concurrently on their
            own threads, with a factory slow enough for acquires to
            overlap.
        When:
            Each loop acquires the same key twice concurrently.
        Then:
            It should invoke the factory exactly once per loop, hand each
            loop its own object, and report one entry per loop, so
            entries never cross loops while acquires on one loop still
            serialize.
        """
        # Arrange
        objects = []

        async def factory(key):
            await asyncio.sleep(0.05)
            obj = object()
            objects.append(obj)
            return obj

        pool = ResourcePool(factory, ttl=60)
        handles = [background_loops() for _ in range(2)]

        async def acquire_twice():
            first, second = await asyncio.gather(
                pool.acquire("key"), pool.acquire("key")
            )
            entries = pool.stats.total_entries
            await pool.clear()
            return first, second, entries

        # Act
        futures = [handle.submit(acquire_twice()) for handle in handles]
        results = [future.result(timeout=5) for future in futures]

        # Assert
        assert len(objects) == 2
        assert all(first is second for first, second, _ in results)
        assert results[0][0] is not results[1][0]
        assert [entries for _, _, entries in results] == [1, 1]

    @given(loop_count=strategies.integers(2, 5))
    @settings(
        max_examples=10,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_acquire_should_isolate_partitions_when_many_loops_run_concurrently(
        self, background_loops, loop_count
    ):
        """Test partitions stay private however many loops share a pool.

        Given:
            One pool and any number of event loops, between two and
            five, running concurrently on their own threads.
        When:
            Every loop acquires the same key twice concurrently.
        Then:
            It should invoke the factory exactly once per loop, hand
            each loop a distinct object shared by both of that loop's
            acquires, and report exactly one entry to each loop.
        """
        # Arrange
        objects = []

        async def factory(key):
            await asyncio.sleep(0.01)
            obj = object()
            objects.append(obj)
            return obj

        pool = ResourcePool(factory, ttl=60)
        handles = [background_loops() for _ in range(loop_count)]

        async def acquire_twice():
            first, second = await asyncio.gather(
                pool.acquire("key"), pool.acquire("key")
            )
            entries = pool.stats.total_entries
            await pool.clear()
            return first, second, entries

        # Act
        try:
            futures = [handle.submit(acquire_twice()) for handle in handles]
            results = [future.result(timeout=10) for future in futures]
        finally:
            # Retire each example's loops eagerly; the fixture would
            # otherwise hold every loop of every example open.
            for handle in handles:
                handle.close()

        # Assert
        assert len(objects) == loop_count
        assert all(first is second for first, second, _ in results)
        assert len({id(first) for first, _, _ in results}) == loop_count
        assert [entries for _, _, entries in results] == [1] * loop_count

    def test_release_should_ignore_key_cached_only_on_another_loop(
        self, mocker, background_loops
    ):
        """Test releasing another loop's key touches nothing.

        Given:
            A pool holding one referenced entry on an event loop still
            running on another thread, and a second loop that has
            cached nothing.
        When:
            That key is released and then expired from the second loop.
        Then:
            It should treat both as silent no-ops against an empty
            partition, leaving the first loop's entry cached, still
            referenced, and never finalized.
        """
        # Arrange
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=lambda key: key, finalizer=finalizer, ttl=60)
        live = background_loops()
        live.run(pool.acquire("key"))

        async def release_and_expire_elsewhere():
            await pool.release("key")
            await pool.expire("key")
            return pool.stats.total_entries

        async def live_stats():
            return pool.stats

        # Act
        entries = asyncio.run(release_and_expire_elsewhere())

        # Assert
        live_snapshot = live.run(live_stats())
        assert entries == 0
        finalizer.assert_not_awaited()
        assert live_snapshot.total_entries == 1
        assert live_snapshot.referenced_entries == 1

    def test_expire_all_should_finalize_only_current_loop_partition(
        self, mocker, background_loops
    ):
        """Test retirement stops at the calling loop's partition.

        Given:
            A pool holding one idle entry on an event loop still
            running on another thread and one idle entry on a second
            loop.
        When:
            expire_all is awaited on the second loop.
        Then:
            It should finalize that loop's entry alone, leaving the
            first loop's entry cached with its finalizer never run.
        """
        # Arrange
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=lambda key: key, finalizer=finalizer, ttl=60)
        live = background_loops()

        async def cache_idle_entry(key):
            async with pool.get(key):
                pass

        live.run(cache_idle_entry("live"))

        async def retire_own_partition():
            await cache_idle_entry("own")
            await pool.expire_all()
            return pool.stats.total_entries

        async def live_stats():
            return pool.stats

        # Act
        remaining = asyncio.run(retire_own_partition())

        # Assert
        live_snapshot = live.run(live_stats())
        assert remaining == 0
        finalizer.assert_awaited_once_with("own")
        assert live_snapshot.total_entries == 1

    def test_clear_should_finalize_only_current_loop_partition(
        self, mocker, background_loops
    ):
        """Test clear leaves another running loop's entries alone.

        Given:
            A pool holding one referenced entry on an event loop that is
            still running on another thread, and one entry on a second
            loop.
        When:
            The pool is cleared from the second loop.
        Then:
            It should finalize the second loop's entry only: the first
            loop's entry stays cached and referenced with its finalizer
            never run, until that loop clears it.
        """
        # Arrange
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=lambda key: key, finalizer=finalizer, ttl=60)
        live = background_loops()

        async def clear_own_entry():
            await pool.acquire("second")
            await pool.clear()
            return pool.stats.total_entries

        async def live_stats():
            return pool.stats

        live.run(pool.acquire("first"))

        # Act
        remaining = asyncio.run(clear_own_entry())

        # Assert
        live_snapshot = live.run(live_stats())
        assert remaining == 0
        finalizer.assert_awaited_once_with("second")
        assert live_snapshot.total_entries == 1
        assert live_snapshot.referenced_entries == 1

    def test_stats_should_count_only_current_loop_entries(self, background_loops):
        """Test stats describe the calling loop's partition alone.

        Given:
            A pool holding two entries on an event loop running on
            another thread and one entry on a second loop.
        When:
            stats is read on each loop.
        Then:
            It should report two entries to the first loop and one to
            the second.
        """
        # Arrange
        pool = ResourcePool(factory=lambda key: key, ttl=60)
        live = background_loops()

        async def acquire_many(*keys):
            for key in keys:
                await pool.acquire(key)
            return pool.stats.total_entries

        # Act
        first = live.run(acquire_many("a", "b"))
        second = asyncio.run(acquire_many("c"))

        # Assert
        live.run(pool.clear())
        assert first == 2
        assert second == 1

    def test_acquire_should_sweep_partition_when_its_loop_closed(
        self, mocker, stranded_loop
    ):
        """Test a fresh loop starts from an empty partition.

        Given:
            A pool that cached and released an entry on an event loop
            that has since closed, leaving that entry's TTL timer behind.
        When:
            The same key is acquired from a fresh event loop.
        Then:
            It should sweep the closed loop's partition -- the entry and
            its timer gone, its finalizer never run -- invoke the factory
            again, and hold only the new entry with no pending cleanup.
        """
        # Arrange
        factory = mocker.Mock(return_value="obj")
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=factory, finalizer=finalizer, ttl=60)

        async def acquire_and_release():
            async with pool.get("key"):
                pass
            return pool.pending_cleanup

        _, pending_before = stranded_loop(acquire_and_release())

        async def acquire_again():
            acquired = await pool.acquire("key")
            return acquired, pool.stats.total_entries, pool.pending_cleanup

        # Act
        acquired, entries, pending = asyncio.run(acquire_again())

        # Assert
        assert "key" in pending_before
        assert acquired == "obj"
        assert factory.call_count == 2
        finalizer.assert_not_awaited()
        assert entries == 1
        assert pending == {}

    def test_acquire_should_sweep_partition_when_its_loop_stopped_but_not_closed(
        self, mocker, stranded_loop
    ):
        """Test liveness is whether a loop runs, not whether it closed.

        Given:
            A pool holding an entry on an event loop that has stopped
            running but has not been closed.
        When:
            The same key is acquired from a second event loop.
        Then:
            It should sweep the stopped loop's partition and rebuild the
            resource on the new loop, since a loop that is not running
            cannot finalize what it cached.
        """
        # Arrange
        factory = mocker.Mock(return_value="obj")
        pool = ResourcePool(factory=factory, ttl=60)
        stranded_loop(pool.acquire("key"), close=False)

        # Act
        acquired = asyncio.run(pool.acquire("key"))

        # Assert
        assert acquired == "obj"
        assert factory.call_count == 2

    def test_acquire_should_not_warn_when_partition_cleared_before_loop_stopped(
        self, caplog, stranded_loop
    ):
        """Test a partition cleared by its own loop is swept silently.

        Given:
            A pool whose entry was cleared on its own event loop before
            that loop closed, leaving an empty partition in the
            registry.
        When:
            The pool is used from a fresh event loop.
        Then:
            It should sweep the empty partition without logging
            anything, because nothing was stranded and there is no leak
            to report.
        """

        # Arrange
        def make_resource(key):
            return "obj"

        pool = ResourcePool(factory=make_resource, ttl=60)

        async def acquire_and_clear():
            async with pool.get("key"):
                pass
            await pool.clear()

        stranded_loop(acquire_and_clear())

        # Act
        with caplog.at_level(logging.DEBUG, logger="wool.runtime.resourcepool"):
            asyncio.run(pool.acquire("other"))

        # Assert
        assert [r for r in caplog.records if r.name == "wool.runtime.resourcepool"] == []

    def test_acquire_should_warn_when_sweeping_stranded_referenced_entry(
        self, caplog, stranded_loop
    ):
        """Test a still-referenced stranded entry is reported as a leak.

        Given:
            A pool whose loop closed while an entry was still referenced.
        When:
            The pool is used from a fresh event loop.
        Then:
            It should log one WARNING from wool.runtime.resourcepool
            naming the pool by its factory and the referenced entry it
            dropped without finalizing.
        """

        # Arrange
        def make_resource(key):
            return "obj"

        pool = ResourcePool(factory=make_resource, ttl=60)
        stranded_loop(pool.acquire("key"))

        # Act
        with caplog.at_level(logging.DEBUG, logger="wool.runtime.resourcepool"):
            asyncio.run(pool.acquire("other"))

        # Assert
        records = [r for r in caplog.records if r.name == "wool.runtime.resourcepool"]
        assert [r.levelno for r in records] == [logging.WARNING]
        assert "make_resource" in records[0].getMessage()
        assert "1 referenced and 0 idle" in records[0].getMessage()

    def test_acquire_should_warn_when_sweeping_stranded_idle_entry(
        self, caplog, stranded_loop
    ):
        """Test an idle stranded entry is reported as a leak too.

        Given:
            A pool whose loop closed with only an idle entry cached
            (released, awaiting its TTL).
        When:
            The pool is used from a fresh event loop.
        Then:
            It should log one WARNING from wool.runtime.resourcepool
            reporting the idle entry, since an entry the loop never
            finalized is a leak whether or not it was in use.
        """

        # Arrange
        def make_resource(key):
            return "obj"

        pool = ResourcePool(factory=make_resource, ttl=60)

        async def acquire_and_release():
            async with pool.get("key"):
                pass

        stranded_loop(acquire_and_release())

        # Act
        with caplog.at_level(logging.DEBUG, logger="wool.runtime.resourcepool"):
            asyncio.run(pool.acquire("other"))

        # Assert
        records = [r for r in caplog.records if r.name == "wool.runtime.resourcepool"]
        assert [r.levelno for r in records] == [logging.WARNING]
        assert "0 referenced and 1 idle" in records[0].getMessage()

    def test_acquire_should_warn_once_per_partition_when_several_loops_stranded(
        self, caplog, background_loops
    ):
        """Test the sweep drains every stale partition, reporting each.

        Given:
            A pool that cached one entry on each of three event loops
            running concurrently on their own threads, all of which
            then stopped without clearing their partitions.
        When:
            The pool is used from a fresh event loop.
        Then:
            It should log exactly one WARNING per stranded partition,
            so the whole registry is drained on the first touch rather
            than one partition at a time.
        """

        # Arrange
        def make_resource(key):
            return "obj"

        pool = ResourcePool(factory=make_resource, ttl=60)
        handles = [background_loops() for _ in range(3)]
        for index, handle in enumerate(handles):
            handle.run(pool.acquire(f"key-{index}"))
        for handle in handles:
            handle.close()

        # Act
        with caplog.at_level(logging.DEBUG, logger="wool.runtime.resourcepool"):
            asyncio.run(pool.acquire("other"))

        # Assert
        records = [r for r in caplog.records if r.name == "wool.runtime.resourcepool"]
        assert [r.levelno for r in records] == [logging.WARNING] * 3
        assert all("1 referenced and 0 idle" in r.getMessage() for r in records)

    def test_release_should_ignore_stale_timer_from_a_swept_partition(
        self, mocker, stranded_loop
    ):
        """Test a TTL timer left on a swept partition cannot touch a later loop's.

        Given:
            A pool that released an entry on one event loop, scheduling
            its TTL timer there, whose partition was then swept when a
            second loop acquired the same key and still holds it.
        When:
            The first loop resumes long enough for that stale timer to
            fire.
        Then:
            It should leave the second loop's entry untouched -- still
            cached and still referenced, its finalizer never run.
        """
        # Arrange
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(
            factory=mocker.Mock(return_value="obj"), finalizer=finalizer, ttl=0.05
        )

        async def acquire_and_release():
            async with pool.get("key"):
                pass

        async def stats():
            return pool.stats

        first_loop, _ = stranded_loop(acquire_and_release(), close=False)
        second_loop, _ = stranded_loop(pool.acquire("key"), close=False)

        # Act
        first_loop.run_until_complete(asyncio.sleep(0.1))
        second = second_loop.run_until_complete(stats())

        # Assert
        assert second.total_entries == 1
        assert second.referenced_entries == 1
        finalizer.assert_not_awaited()

    def test_release_should_leave_no_pending_task_when_loop_closes_before_ttl(
        self, mocker, stranded_loop
    ):
        """Test release defers cleanup without parking a task on the loop.

        Given:
            A pool with a positive TTL whose resource is released on
            a dedicated event loop.
        When:
            The loop is closed and garbage-collected before the TTL
            elapses.
        Then:
            It should leave no pending task on the loop and emit no
            RuntimeWarning when the deferred cleanup is collected.
        """

        # Arrange
        def create_release_and_drop():
            pool = ResourcePool(factory=mocker.Mock(return_value="obj"), ttl=60)

            async def acquire_release():
                async with pool.get("key"):
                    pass

            loop, _ = stranded_loop(acquire_release(), close=False)
            return loop

        # Act
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            loop = create_release_and_drop()
            pending = asyncio.all_tasks(loop)
            loop.close()
            gc.collect()

        # Assert
        assert pending == set()
        assert not [w for w in caught if issubclass(w.category, RuntimeWarning)]


class TestResource:
    """Test suite for the Resource class."""

    @pytest.mark.asyncio
    async def test___aenter___should_raise_when_acquired_twice(self, mocker):
        """Test that re-acquiring the same Resource instance raises error.

        Given:
            A Resource that has been used as context manager once
        When:
            Attempting to use it as context manager again
        Then:
            Should raise RuntimeError
        """
        # Arrange
        mock_resource = mocker.Mock()
        mock_factory = mocker.Mock(return_value=mock_resource)
        pool = ResourcePool(factory=mock_factory, ttl=0)
        resource_acquisition = pool.get("test-key")

        # First use as context manager
        async with resource_acquisition as resource:
            assert resource is mock_resource

        # Act & assert
        # Second use as context manager should fail
        with pytest.raises(RuntimeError, match="Cannot re-acquire a resource"):
            async with resource_acquisition:
                pass

    @pytest.mark.asyncio
    async def test___aenter___should_propagate_exception_when_acquire_fails(
        self, mocker
    ):
        """Test a failed acquisition leaves the Resource usable again.

        Given:
            A Resource whose pool fails the first acquisition of its
            key and succeeds on the next.
        When:
            The Resource is entered, raising, and then entered again.
        Then:
            It should propagate the first failure and still admit the
            second entry, because a failed acquisition consumes nothing.
        """
        # Arrange
        factory = mocker.Mock(side_effect=[RuntimeError("Acquire failed"), "resource"])
        pool = ResourcePool(factory=factory, ttl=0)
        resource_acquisition = pool.get("test-key")

        # Act & assert
        with pytest.raises(RuntimeError, match="Acquire failed"):
            async with resource_acquisition:
                pass

        async with resource_acquisition as resource:
            assert resource == "resource"

    @pytest.mark.asyncio
    async def test___aexit___should_release_resource_when_context_exits(self, mocker):
        """Test Resource as async context manager.

        Given:
            A Resource instance from a pool
        When:
            Used as async context manager
        Then:
            Should auto-acquire on enter and auto-release on exit
        """
        # Arrange
        mock_resource = mocker.Mock()
        mock_resource.name = "context-resource"
        mock_factory = mocker.Mock(return_value=mock_resource)

        pool = ResourcePool(factory=mock_factory, ttl=0)

        # Act & assert
        # Use Resource as context manager
        async with pool.get("test-key") as resource:
            assert resource is mock_resource
            assert pool.stats.total_entries == 1
            assert pool.stats.referenced_entries == 1

        # Should be automatically cleaned up after context exit
        assert pool.stats.total_entries == 0

    @pytest.mark.asyncio
    async def test___aexit___should_keep_entry_cached_when_ttl_positive(self, ttl_pool):
        """Test Resource lifecycle with TTL keeps resource in cache.

        Given:
            A Resource instance with TTL pool
        When:
            Used as context manager
        Then:
            Should handle lifecycle correctly and resource stays cached due to TTL
        """
        # Arrange
        pool, factory, _ = ttl_pool
        resource_acquisition = pool.get("test-key")

        # Act
        # Use as context manager
        async with resource_acquisition as resource:
            assert resource is factory.return_value
            assert pool.stats.referenced_entries == 1

        # Assert
        # Resource should still exist due to TTL but no longer referenced
        assert pool.stats.total_entries == 1
        assert pool.stats.referenced_entries == 0

    @pytest.mark.asyncio
    @given(resource=strategies.sampled_from([0, 0.0, False, "", b"", (), [], {}, set()]))
    @settings(max_examples=10, deadline=None)
    async def test___aexit___should_release_when_resource_is_falsy(self, resource):
        """Test a falsy resource is still released on context exit.

        Given:
            A zero-TTL pool whose factory yields any falsy object that
            is not None — zero, False, or an empty string, bytes,
            tuple, list, dict, or set. (Whether a factory returning
            None should pin its entry is an open question, so None is
            excluded here.)
        When:
            The Resource is used as an async context manager and exits.
        Then:
            It should drop the reference and finalize the entry, so
            falsiness never suppresses the release.
        """
        # Arrange
        finalized = []
        pool = ResourcePool(
            factory=lambda key: resource,
            finalizer=lambda obj: finalized.append(obj),
            ttl=0,
        )

        # Act
        async with pool.get("key") as acquired:
            referenced = pool.stats.referenced_entries

        # Assert
        assert acquired is resource
        assert referenced == 1
        assert pool.stats.total_entries == 0
        assert len(finalized) == 1
        assert finalized[0] is resource

    @pytest.mark.asyncio
    async def test___aexit___should_raise_when_not_acquired(self, mocker):
        """Test Resource release when not acquired raises RuntimeError.

        Given:
            A Resource instance that was never acquired
        When:
            Attempting to exit context without entering properly
        Then:
            Should raise RuntimeError indicating resource was not acquired
        """
        # Arrange
        pool = ResourcePool(factory=mocker.Mock(return_value="resource"), ttl=0)
        resource = pool.get("test-key")

        # Act & assert - exit without ever entering the context
        with pytest.raises(
            RuntimeError, match="Cannot release a resource that was not acquired"
        ):
            await resource.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test___aexit___should_raise_when_already_released(self, mocker):
        """Test Resource release when already released raises RuntimeError.

        Given:
            A Resource instance that was already released
        When:
            Attempting to exit context again after normal usage
        Then:
            Should raise RuntimeError indicating resource was already released
        """
        # Arrange
        pool = ResourcePool(factory=mocker.Mock(return_value="resource"), ttl=0)
        resource = pool.get("test-key")

        # Use normally once (which sets the resource released)
        async with resource:
            pass

        # Act & assert - exit the already-released context a second time
        with pytest.raises(
            RuntimeError,
            match="Cannot release a resource that has already been released",
        ):
            await resource.__aexit__(None, None, None)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "body_error",
        [None, KeyError("boom")],
        ids=["clean-exit", "body-raises"],
    )
    async def test___aexit___should_finalize_when_retired_in_body(
        self, retired_entry_pool, body_error
    ):
        """Test exiting the context finalizes a resource retired inside it.

        Given:
            A long-TTL pool whose key is retired by ``expire`` from
            inside an ``async with pool.get(key)`` body while still
            referenced, where the body then either returns or raises.
        When:
            The context manager exits, normally or on the exceptional
            unwind.
        Then:
            It should have awaited the finalizer before the statement
            following the block runs, leaving the entry evicted with no
            cleanup pending and any original exception propagating
            unchanged.
        """
        # Arrange
        pool, finalizer, _ = retired_entry_pool
        guard = pytest.raises(KeyError, match="boom") if body_error else nullcontext()

        # Act & assert
        with guard:
            async with pool.get("key"):
                await pool.expire("key")
                # Guard: still referenced, so nothing is finalized yet.
                finalizer.assert_not_awaited()
                if body_error:
                    raise body_error

        finalizer.assert_awaited_once_with("first")
        assert pool.stats.total_entries == 0
        assert not pool.pending_cleanup
