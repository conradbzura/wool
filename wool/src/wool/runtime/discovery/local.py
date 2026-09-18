from __future__ import annotations

import asyncio
import atexit
import errno
import fcntl
import io
import logging
import os
import stat
import struct
import sys
import tempfile
import warnings
from contextlib import AsyncExitStack
from contextlib import ExitStack
from contextlib import asynccontextmanager
from contextlib import closing
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING
from typing import AsyncGenerator
from typing import AsyncIterator
from typing import Callable
from typing import Final
from typing import Iterator
from typing import NamedTuple
from typing import Self
from uuid import UUID
from uuid import uuid4

from watchdog.events import FileSystemEvent
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from wool import protocol as wire
from wool.runtime.discovery.base import Discovery
from wool.runtime.discovery.base import DiscoveryEvent
from wool.runtime.discovery.base import DiscoveryEventType
from wool.runtime.discovery.base import DiscoveryPublisherLike
from wool.runtime.discovery.base import DiscoverySubscriberLike
from wool.runtime.discovery.base import PredicateFunction
from wool.runtime.discovery.exceptions import DiscoveryBlockExhausted
from wool.runtime.discovery.exceptions import DiscoveryCapacityExhausted
from wool.runtime.discovery.exceptions import DiscoveryNamespaceInUse
from wool.runtime.discovery.exceptions import DiscoveryNamespaceNotFound
from wool.runtime.discovery.exceptions import DiscoveryWorkerNotFound
from wool.runtime.discovery.pool import SubscriberMeta
from wool.runtime.resourcepool import ResourcePool
from wool.runtime.worker.metadata import WorkerMetadata
from wool.utilities.afilter import afilter
from wool.utilities.noreentry import noreentry

#: Worker slots a namespace's registry holds when its owner does not
#: choose otherwise.
DEFAULT_CAPACITY: Final = 128
#: Seconds a publisher waits for the registry lock when its caller does
#: not choose otherwise.
DEFAULT_LOCK_TIMEOUT: Final[float] = 30.0
_REF_WIDTH: Final = 16
_NULL_REF: Final = b"\x00" * _REF_WIDTH
_HEADER_MAGIC: Final = b"WLD1"
_HEADER_FORMAT: Final = "<4sII"
_HEADER_SIZE: Final = _REF_WIDTH
_REGISTRY: Final = "registry"
_STAGING: Final = "registry.tmp"
_NOTIFY: Final = "notify"
_ROOT_DIRNAME: Final = "wool"
_CURRENT: Final = "current"
_BLOCKS: Final = "blocks"
_NAME_MAX: Final = 255
_CARRY_FORWARD_LIMIT: Final = 3
#: How long a parked subscription waits before re-checking that its
#: owner still holds the generation it bound. Bounds how long a borrower
#: can serve a snapshot of a namespace that no longer has a live owner,
#: independently of how often it rescans: a subscription with no poll
#: interval would otherwise park on a notification that can no longer
#: arrive, and one with a long poll interval would wait that interval
#: out. A liveness wake is not a rescan; see `LocalDiscovery.Subscriber`.
_LIVENESS_INTERVAL: Final[float] = 5.0

#: Bumped in the child by an `os.register_at_fork` handler, so an owner
#: entered before a fork can tell that it is running in the fork. See
#: `LocalDiscovery` for the ownership contract this enforces.
_fork_generation = 0

logger = logging.getLogger(__name__)


def _bump_fork_generation() -> None:
    """Disown every namespace this process inherited by forking."""
    global _fork_generation
    _fork_generation += 1


os.register_at_fork(after_in_child=_bump_fork_generation)


class _Watchdog(FileSystemEventHandler):
    """Filesystem event handler for worker discovery notifications.

    Monitors the notification file for modifications and sets an asyncio
    Event to wake subscribers when publishers write to the registry. Runs
    on watchdog's observer thread, so the set is handed to the loop the
    notification belongs to.

    Setting takes no lock. A scan clears the notification before it reads
    the registry, so a set arriving at any point during that scan is
    preserved and wakes the next one. Coalescing in the event itself also
    keeps a burst of publishes from queueing one task per notification
    behind the scan, where each would only set an event already set.

    :param notification:
        asyncio.Event to set when the notification file is modified.
    :param watchdog:
        Path to the notification file to monitor.
    :param loop:
        Event loop where the notification lives.
    """

    def __init__(
        self,
        notification: asyncio.Event,
        watchdog: Path,
        loop: asyncio.AbstractEventLoop,
    ):
        self._notification = notification
        self._watchdog = watchdog
        self._loop = loop

    def on_modified(self, event: FileSystemEvent):
        """Handle file modification events.

        :param event:
            The filesystem event containing the modified file path.
        """
        event_path = Path(str(event.src_path))
        if event_path == self._watchdog:
            # Called on the observer thread; the set belongs to the loop.
            self._loop.call_soon_threadsafe(self._notification.set)


class _WorkerReference:
    """Reference to a worker using its UUID.

    Provides both byte and unicode string representations of a worker's UUID.

    :param uid:
        The worker's UID to reference.
    """

    __slots__ = ("_uuid",)

    def __init__(self, uid: UUID):
        self._uuid = uid

    def __str__(self) -> str:
        """Return the file name of this worker's metadata block.

        :returns:
            The UUID as 32 hexadecimal digits.
        """
        return self._uuid.hex

    def __hash__(self) -> int:
        return hash(self._uuid)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, _WorkerReference):
            return self._uuid == other._uuid
        return NotImplemented  # pragma: no cover

    def __repr__(self) -> str:
        return f"_WorkerReference({self._uuid})"  # pragma: no cover

    @classmethod
    def from_bytes(cls, data: bytes) -> _WorkerReference:
        """Create a reference from its bytes representation.

        :param data:
            The 16-byte UUID representation.
        :returns:
            A new reference instance.
        :raises ValueError:
            If data is not 16 bytes. A NULL slot yields the nil UUID and
            is the caller's to skip; see `LocalDiscovery.Subscriber`.
        """
        ref = object.__new__(cls)
        ref._uuid = UUID(bytes=data)
        return ref

    @property
    def bytes(self) -> bytes:
        """The 16-byte representation stored in a registry slot.

        :returns:
            The UUID as 16 bytes.
        """
        return self._uuid.bytes


class _File:
    """An open file in a namespace's directory, read and written at offsets.

    Reads and writes address the file directly, so a write is visible to
    every process holding the same file, and a handle stays usable after
    the file it names is removed or replaced.

    :param path:
        The path the file was opened by.
    :param file:
        The file, open for reading and writing. The handle takes
        ownership of it and closes it on `close`.
    """

    __slots__ = ("file", "path")

    file: io.FileIO
    path: Path

    def __init__(self, path: Path, file: io.FileIO):
        self.path = path
        self.file = file

    @property
    def size(self) -> int:
        """The file's size in bytes.

        :returns:
            The size of the open file, whatever its path now names.
        """
        return os.fstat(self.file.fileno()).st_size

    @classmethod
    def create(cls, path: Path, size: int) -> _File:
        """Create a zero-filled file of ``size`` bytes and open it.

        :param path:
            The file to create.
        :param size:
            The file's size in bytes.
        :returns:
            A handle on the new file.
        :raises FileExistsError:
            If ``path`` already exists.
        """
        descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
        # Bound to the raw descriptor before anything else can raise, so
        # a failure inside the wrapper's construction closes it rather
        # than losing it.
        try:
            os.ftruncate(descriptor, size)
            file = io.FileIO(descriptor, "r+", closefd=True)
        except BaseException:  # pragma: no cover
            os.close(descriptor)
            _unlink_quietly(path.parent, path.name)
            raise
        return cls(path, file)

    @classmethod
    def open(cls, path: Path, *, write: bool = True) -> _File:
        """Open an existing file.

        :param path:
            The file to open.
        :param write:
            Whether the handle may write. A reader passes ``False``: on
            APFS the write mode is not free, and the subscriber opens
            every registered worker's block once per scan.
        :returns:
            A handle on the file.
        :raises FileNotFoundError:
            If ``path`` does not exist.
        """
        return cls(path, io.FileIO(path, "r+" if write else "r"))

    def current(self) -> bool:
        """Report whether this handle's path still names this file.

        A handle whose path was removed, or replaced by a successor
        owner's file, is stale: the file it holds is no longer the one
        that path names, so nothing reads what it writes.

        :returns:
            True while the path resolves to the open file.
        """
        try:
            return _same_file(self.file.fileno(), self.path)
        except OSError:
            # Any answer the filesystem cannot give is stale enough: a
            # path that no longer resolves, or resolves through something
            # that is not a directory, does not name this file.
            return False

    def read(self, size: int, offset: int) -> bytes:
        """Return ``size`` bytes read from ``offset``.

        :param size:
            The number of bytes to read.
        :param offset:
            The offset to read from.
        :returns:
            The bytes read, fewer than ``size`` at end of file.
        """
        return os.pread(self.file.fileno(), size, offset)

    def write(self, data: bytes, offset: int) -> None:
        """Write ``data`` at ``offset``.

        :param data:
            The bytes to write.
        :param offset:
            The offset to write at.
        """
        os.pwrite(self.file.fileno(), data, offset)

    def close(self) -> None:
        """Close the file, leaving it in place."""
        self.file.close()


class _Header(NamedTuple):
    """The sizes a namespace's owner stamps into its registry.

    Both bind a borrower: the owner fixes them when it stages the
    generation, and a borrower reads rather than supplies them, so two
    publishers cannot disagree about the size of an artifact the owner
    owns. See `LocalDiscovery` for the ownership contract.

    :param capacity:
        The maximum number of workers registered at once.
    :param block_size:
        The size in bytes of each worker's metadata block.
    """

    capacity: int
    block_size: int


class _Binding:
    """A borrower's hold on the generation that was live when it bound.

    Resolving the pointer, proving the registry is there and pinning a
    descriptor on the generation are one step with one outcome: either
    this borrower is bound to an owner's incarnation, or the namespace
    has no live owner to borrow from. Both borrower kinds bind through
    here, so a publisher and a subscriber cannot disagree about what a
    missing generation, a replaced directory or a dead owner means.

    A binding ends with the owner it was made against; see
    `LocalDiscovery` for the borrowing contract and `_owner_alive` for
    how a dead owner is detected.

    :param generation:
        The generation directory this binding holds.
    :param liveness:
        A descriptor on that directory, pinned for the binding's life.
    """

    __slots__ = ("generation", "header", "_liveness")

    def __init__(self, generation: Path, header: _Header, liveness: int) -> None:
        self.generation = generation
        self.header = header
        self._liveness = liveness

    @classmethod
    def bind(cls, namespace: str) -> Self:
        """Resolve a namespace's live generation and hold it.

        :param namespace:
            The namespace to borrow.
        :returns:
            A binding on the generation that was live at this moment.
        :raises DiscoveryNamespaceNotFound:
            If the namespace has no live owner to borrow from.
        """
        generation = _resolve_generation(namespace)
        # Read and then released: a publisher opens its own handle per
        # publish, so the descriptor it locks is the one it writes, and a
        # subscriber keeps a handle of its own. The sizes come with the
        # bind because they are the owner's to fix, not a borrower's.
        with closing(_open_registry(namespace, generation)) as registry:
            # A registry the pointer reaches was stamped before the
            # pointer existed, so an unstamped one was planted rather
            # than staged and there is nothing here to borrow. This is
            # the only place the header is validated; every later read
            # happens against a binding that already passed.
            try:
                header = _read_header(registry)
            except ValueError as error:
                raise DiscoveryNamespaceNotFound(namespace) from error
        try:
            liveness = os.open(generation, os.O_RDONLY)
        except (FileNotFoundError, NotADirectoryError) as error:
            raise DiscoveryNamespaceNotFound(namespace) from error
        binding = cls(generation, header, liveness)
        # A killed owner leaves its generation readable, so binding it
        # would otherwise succeed against a namespace no process owns.
        if not binding.alive():
            binding.close()
            raise DiscoveryNamespaceNotFound(namespace)
        return binding

    def alive(self) -> bool:
        """Report whether the owner this binding was made against lives.

        :returns:
            True while that owner's process holds the generation.
        """
        return _owner_alive(self._liveness)

    def close(self) -> None:
        """Release the descriptor this binding pins.

        Closing is idempotent, and a closed binding reports its owner as
        gone rather than guessing from a descriptor it no longer holds.
        """
        liveness, self._liveness = self._liveness, -1
        _close_quietly(liveness)


# public
class LocalDiscovery(Discovery):
    """File-backed discovery for single-machine worker pools.

    The default discovery protocol of a
    `~wool.runtime.worker.pool.WorkerPool` created without one. Processes
    on one host share a registry of workers identified by a namespace
    string, and a cross-process file lock serializes writes to it.

    **Ownership.** A namespace's registry has exactly one owner: the
    entered instance holding the namespace's claim. Entering claims the
    namespace and creates its registry, and exiting reclaims both. The
    namespace is in use — and a further entry raises
    `DiscoveryNamespaceInUse` — while *any* process holding the claim
    lives. That is the owner's process, and anything forked from it
    after entry; see **Forks**. An owner that never exits, e.g., one
    abandoned when the interpreter shuts down, reclaims the registry at
    shutdown.

    A killed owner that was never forked from holds the claim in no
    surviving process, so the kernel drops it and the next entry on the
    namespace succeeds, replacing any registry the killed owner left
    behind. What that entry does *not* reclaim is the rest of the
    directory; see the note on non-graceful exits in the implementation
    notes below.

    **Forks.** A process forked from an entered owner inherits both the
    claim and the owner's teardown, because the claim is a descriptor
    the fork keeps and the teardown is interpreter state the fork
    copies. Two consequences follow, and neither is what a forking
    caller usually wants: the fork's own ordinary exit reclaims the live
    parent's registry and directory, and the claim itself outlives the
    parent, so the namespace stays in use until every process holding it
    has exited. Wool starts its own workers with ``spawn``, which is
    ``spawn(2)`` and inherits neither. A host that forks should enter a
    namespace in the process that owns it, and should not fork between
    entry and exit.

    **Borrowing.** `LocalDiscovery.Publisher` and
    `LocalDiscovery.Subscriber`, including those `publisher`,
    `subscriber` and `subscribe` return, borrow a namespace's registry
    and never create one. A borrower *binds* when it opens the registry:
    a publisher on entry and on each publish, a subscriber when its
    subscription starts. A bind where the namespace has no registry
    raises `DiscoveryNamespaceNotFound`.

    **Orphaning.** A borrower that outlives its owner is orphaned. A
    registry it already holds stays readable. Its next bind raises
    `DiscoveryNamespaceNotFound`, unless a successor owner has entered
    the namespace, in which case the bind reaches the successor's
    registry. Orphaning is defined behavior: a borrower holds no claim on
    the registry, so it has no registry to reclaim. See
    `LocalDiscovery.Subscriber` for which subscriber iterations bind.

    **Lifecycle.** An instance is single-use: any entry attempt spends
    it, so a second entry raises `RuntimeError`. Retrying a rejected
    claim requires a new instance. Exiting never raises, and exiting an
    instance never entered, or already exited, does nothing at all; a
    failed removal surfaces as a `ResourceWarning`. Once the owner and
    every publisher have exited *gracefully*, the namespace leaves
    nothing on the filesystem.

    :param namespace:
        Identifier of the registry. It names one directory under this
        module's root, so it must be a single path component: not empty,
        free of path separators and NUL, neither ``.`` nor ``..``, and
        short enough to render within the filesystem's limit on a name.
        Defaults to a unique ``workerpool-<uuid>`` name.
    :param filter:
        Optional default predicate function to filter workers.
        Used by `subscriber` and as the default for `subscribe` when no
        explicit filter is provided.
    :param poll_interval:
        Optional default seconds between fallback rescans, used by
        `subscriber` and as the default for `subscribe` when no explicit
        interval is provided. ``None`` is a mode rather than an absence:
        a subscription rescans only when a publisher writes. A value set
        here therefore cannot be overridden back to ``None`` at the call
        site, exactly as ``filter`` cannot. Defaults to ``None``.
    :param capacity:
        Maximum number of workers registered at once. The owner stamps
        it on entry and a borrower binds at the owner's capacity, so this
        value applies only when this instance is entered. See
        `LocalDiscovery.Publisher.publish` for exhaustion. Defaults to
        128.
    :param block_size:
        Size in bytes for each worker's serialized data block. Each
        block spends 4 bytes on a length prefix, leaving
        ``block_size - 4`` for the serialized metadata, so it must exceed
        4. Note that a block only a few bytes above the prefix still
        cannot hold the smallest serialized metadata and makes every
        publish fail; the floor rejects what is unusable by arithmetic,
        not what is unusable in practice. Defaults to 1024.
    :param lock_timeout:
        Maximum seconds each publisher waits for the cross-process file
        lock; see `LocalDiscovery.Publisher`. Defaults to 30.0.
    :raises ValueError:
        If ``namespace`` does not name a single path component, if
        ``capacity`` is less than 1, if ``block_size`` does not exceed
        the 4-byte length prefix, if ``poll_interval`` is not positive,
        or if ``lock_timeout`` is negative.

    Example — publish workers:

    .. code-block:: python

        async def publish(metadata):
            with LocalDiscovery("my-worker-pool") as discovery:
                async with discovery.publisher as publisher:
                    await publisher.publish("worker-added", metadata)

    Example — subscribe to workers:

    .. code-block:: python

        async def watch():
            with LocalDiscovery("my-worker-pool") as discovery:
                async for event in discovery.subscriber:
                    print(f"Discovered worker: {event.metadata}")

    Example — borrow a namespace another process owns:

    .. code-block:: python

        async def watch_borrowed():
            async for event in LocalDiscovery.Subscriber("my-worker-pool"):
                print(f"Discovered worker: {event.metadata}")

    .. rubric:: Implementation notes

    A namespace lives in one directory, ``wool-<namespace>``, under
    ``/dev/shm`` where Linux provides it and the temporary directory
    otherwise. The directory holds the registry, i.e., a header and
    fixed-width slots of worker references, the ``notify`` file
    subscribers watch, and one metadata block file per published worker,
    named by the worker's UUID. Every process reads and writes those
    files at fixed offsets, through the page cache, so a write is visible
    to every other process holding the same file.

    The files are read and written rather than memory-mapped. CPython
    forces a device-level flush (``F_FULLFSYNC``) whenever it maps a file
    descriptor on Apple platforms, which costs milliseconds per mapping,
    varies with unrelated disk activity, and imposes that flush on the
    whole host. ``pread`` and ``pwrite`` over a held descriptor carry the
    same cross-process visibility without it.

    The claim is an exclusive ``flock`` on the directory, held on a
    descriptor the owner keeps open. The kernel releases it only when
    every descriptor sharing it closes — hence the fork behavior above —
    so a claim cannot be taken from an owner that is merely slow, and a
    successor that takes it knows any registry it finds is stranded. The
    lock is taken on a descriptor opened before locking, so the claimant
    re-checks that the path still names that directory: a directory
    removed in between would otherwise hand a claim on an unlinked inode
    to one owner while another claims its replacement. A claim that finds
    the directory replaced under it retries, and each retry follows
    another process's completed reclaim rather than contending with one,
    so a claim resolves rather than livelocking.

    The owner writes the registry, header included, under a staging name
    and renames it into place, so no borrower ever binds a registry with
    an unstamped header.

    Publishers lock the registry file itself rather than a separate lock
    file. The lock and the data it guards therefore always share an
    inode: an orphaned publisher still locking a reclaimed registry can
    only write to that registry, never to a successor's. A publish opens
    its own handle and closes it on return, rather than a publisher
    holding one for its life. A file lock is held per open file
    description with no nesting count, so a shared handle would let a
    second publish on the same publisher take a lock the first already
    held and release the first's lock when it finished — leaving the
    invariant above asserted rather than enforced. Opening per publish
    makes it unrepresentable instead: the descriptor a publish locks is
    the descriptor it writes.

    The registry is fixed-width by design. Files can grow — `_File`
    sizes with ``ftruncate``, ``pwrite`` extends past the end, and the
    capacity is re-read from the header on every scan — so ``capacity``
    is not a limitation inherited from the storage the way it was when a
    registry was a mapped shared-memory segment. It is kept because
    growth would need a resize protocol between unrelated processes,
    where subscribers scan without any lock, in exchange for removing a
    bound that callers can already set.

    Borrowers never create the directory or anything in it but their
    own blocks. Exiting removes the registry, the notification file and
    the directory; the directory stays while an orphaned publisher still
    holds a block in it, and that publisher removes the directory with
    its last block.

    Every teardown path removes files through `_unlink_quietly`, so
    `__exit__` cannot replace an exception its caller is already
    unwinding. The shutdown fallback is an `atexit` handler registered on
    entry and unregistered on exit before the removal runs, so a failed
    removal leaves no handler armed to fire again at interpreter
    shutdown.

    That fallback is the only reclaimer, and it is interpreter state: it
    does not run when a process dies from a signal. A ``SIGKILL``, or a
    ``SIGTERM`` with no handler installed — the ordinary container
    shutdown path — therefore strands the namespace's directory and one
    block file per registered worker. A successor entering the same
    namespace replaces the registry and the notification file but sweeps
    nothing else, so its own clean exit then fails to remove a directory
    that is no longer empty, silently. The residue is bounded by the
    filesystem rather than reclaimed by this module: ``/dev/shm`` is
    cleared on reboot, and a temporary directory is reaped by the
    platform's temporary-file sweeper. Because a pool mints a fresh
    namespace per lifecycle, nothing re-claims an abandoned one.
    """

    _claim: int
    _cleanup: Callable[[], None] | None
    _filter: Final[PredicateFunction | None]
    _fork_generation: int
    _liveness: int
    _namespace: Final[str]
    _owner_pid: int
    _poll_interval: Final[float | None]

    def __init__(
        self,
        namespace: str | None = None,
        *,
        filter: PredicateFunction | None = None,
        poll_interval: float | None = None,
        capacity: int = DEFAULT_CAPACITY,
        block_size: int = 1024,
        lock_timeout: float | None = DEFAULT_LOCK_TIMEOUT,
    ):
        if capacity < 1:
            raise ValueError(f"Expected capacity of at least 1, got {capacity}")
        _validate_block_size(block_size)
        if poll_interval is not None and poll_interval <= 0:
            raise ValueError(f"Expected positive poll interval, got {poll_interval}")
        if lock_timeout is not None and lock_timeout < 0:
            raise ValueError("Lock timeout must be non-negative")
        if namespace is None:
            namespace = f"workerpool-{uuid4()}"
        _validate_namespace(namespace)
        self._namespace = namespace
        self._filter = filter
        self._poll_interval = poll_interval
        self._capacity = capacity
        self._block_size = block_size
        self._lock_timeout = lock_timeout
        self._claim = -1
        self._liveness = -1
        self._cleanup = None
        # Both are restamped by `__enter__`, which is the entry that owns
        # the claim; an instance constructed before a fork and entered in
        # the child owns its own claim and must not disown it.
        self._fork_generation = _fork_generation
        self._owner_pid = os.getpid()

    @noreentry
    def __enter__(self) -> Self:
        """Claim the namespace and create its registry.

        See `LocalDiscovery` for the ownership and teardown contract.

        :returns:
            This instance.
        :raises RuntimeError:
            If entry has already been attempted on this instance.
        :raises DiscoveryNamespaceInUse:
            If a live owner holds the namespace; see `LocalDiscovery`.
        """
        # Stamped before anything can fail, so a fork racing this entry
        # either sees an instance that was never entered — whose release
        # is a no-op — or one stamped with the pre-fork generation, which
        # disowns. See **Forks** in `LocalDiscovery`.
        self._fork_generation = _fork_generation
        self._owner_pid = os.getpid()
        directory = _namespace_directory(self._namespace)
        while True:
            directory.mkdir(parents=True, exist_ok=True)
            try:
                claim = os.open(directory, os.O_RDONLY)
            except FileNotFoundError:
                continue
            # Taking the claim is its own step, because a failure here
            # means the opposite of a failure below it: until the lock
            # is held this process owns nothing, so there is nothing of
            # this owner's to release and releasing would purge the
            # incumbent's generation. Only the claim itself is closed.
            try:
                fcntl.flock(claim, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                os.close(claim)
                raise DiscoveryNamespaceInUse(self._namespace) from error
            except BaseException:
                os.close(claim)
                raise
            # Stored only once the lock is held, so `_release` never sees
            # a descriptor this owner did not claim; see `_release`.
            self._claim = claim
            try:
                # See the directory re-check in the implementation notes.
                if _same_file(claim, directory):
                    # Holding the claim proves no live writer, now that a
                    # borrower's binding ends with its owner, so whatever
                    # a killed predecessor left is this owner's to sweep.
                    # Before staging, and on every retry, so a partially
                    # staged generation is self-cleaning.
                    _purge(claim)
                    self._stage(directory)
                    break
            except (FileNotFoundError, NotADirectoryError):
                # The directory was removed, or replaced by something
                # that is not a directory, after it was opened. Both are
                # progress by another process, so the claim is retried;
                # every other error reaches the caller rather than
                # spinning this loop, which has no timeout and no sleep.
                pass
            except BaseException:
                self._release()
                raise
            # Exchanged for the sentinel before closing, so a retry that
            # then fails outside this block cannot leave a closed number
            # on the instance for `_release` to act on.
            claim, self._claim = self._claim, -1
            os.close(claim)
        self._cleanup = atexit.register(self._release)
        return self

    def __exit__(self, *_):
        """Reclaim the namespace's registry and release the claim.

        Exiting an instance that was never entered, or one that has
        already exited, removes nothing and raises nothing. See
        `LocalDiscovery` for the ownership and teardown contract.
        """
        if self._cleanup is not None:
            atexit.unregister(self._cleanup)
            self._cleanup = None
        self._release()

    def __hash__(self) -> int:
        return hash((type(self), self._namespace))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, LocalDiscovery):
            return self._namespace == other._namespace
        return NotImplemented

    @property
    def namespace(self) -> str:
        """The namespace identifier for this discovery service.

        :returns:
            The namespace string.
        """
        return self._namespace

    @property
    def publisher(self) -> DiscoveryPublisherLike:
        """A new publisher instance for this discovery service.

        :returns:
            A publisher instance for broadcasting worker events.
        """
        return self.Publisher(self._namespace, lock_timeout=self._lock_timeout)

    @property
    def subscriber(self) -> DiscoverySubscriberLike:
        """A subscriber using the constructor's default filter and interval.

        :returns:
            A subscriber instance for receiving worker discovery
            events.
        """
        return self.subscribe()

    def subscribe(
        self,
        filter: PredicateFunction | None = None,
        *,
        poll_interval: float | None = None,
    ) -> DiscoverySubscriberLike:
        """Return a subscriber to this namespace with optional filtering.

        :param filter:
            Optional predicate function to filter workers. Only workers
            for which the predicate returns True will be included in
            events. Falls back to the constructor's filter if not
            provided.
        :param poll_interval:
            Optional seconds between fallback rescans. Falls back to the
            constructor's interval if not provided; see
            `LocalDiscovery.Subscriber`.
        :returns:
            A subscriber instance that receives filtered worker
            discovery events.
        """
        effective = filter if filter is not None else self._filter
        subscriber = self.Subscriber(
            self._namespace,
            poll_interval=(
                poll_interval if poll_interval is not None else self._poll_interval
            ),
        )
        if effective is not None:
            return afilter(effective, subscriber)
        return subscriber

    def _stage(self, directory: Path) -> None:
        """Create this owner's generation and publish it as the live one.

        Every artifact of the namespace lives under a generation
        directory this owner creates and locks, so a borrower binds one
        owner's incarnation and never drifts onto a successor's. See
        `LocalDiscovery`'s implementation notes for why the registry is
        staged and why the pointer is published last.

        Staging either publishes a generation or leaves this instance as
        it found it, so a caller may retry without cleaning up after a
        failed attempt. The generation's own directory may survive a
        failure; the next attempt's sweep removes it.

        :param directory:
            The claimed namespace's directory.
        :raises FileNotFoundError:
            If the directory was removed after it was claimed.
        """
        generation = directory / uuid4().hex
        (generation / _BLOCKS).mkdir(parents=True)
        # Held for this generation's whole life. A borrower that cannot
        # take it shared knows the owner's process is alive; see
        # `_owner_alive`.
        liveness = os.open(generation, os.O_RDONLY)
        try:
            fcntl.flock(liveness, fcntl.LOCK_EX | fcntl.LOCK_NB)
            staging = generation / _STAGING
            descriptor = os.open(staging, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o600)
            try:
                # Truncation zero-fills, so every slot already reads as
                # `_NULL_REF`.
                os.ftruncate(descriptor, _HEADER_SIZE + self._capacity * _REF_WIDTH)
                os.pwrite(
                    descriptor,
                    struct.pack(
                        _HEADER_FORMAT,
                        _HEADER_MAGIC,
                        self._capacity,
                        self._block_size,
                    ),
                    0,
                )
            finally:
                os.close(descriptor)
            os.replace(staging, generation / _REGISTRY)
            (generation / _NOTIFY).touch()
            # Published last and atomically, so no borrower resolves a
            # generation before its registry is stamped.
            pointer = directory / f"{_CURRENT}.{os.getpid()}"
            os.symlink(generation.name, pointer)
            os.rename(pointer, directory / _CURRENT)
        except BaseException:
            _close_quietly(liveness)
            raise
        # Assigned only once the generation is published, so a caller
        # that retries a half-staged attempt has nothing to clean up:
        # every failure above closed what it opened.
        self._liveness = liveness

    def _release(self) -> None:
        """Remove the namespace's files and directory, then release the claim.

        Releasing is idempotent and final. The claim is exchanged for a
        sentinel before anything acts on it, so a release that follows
        another removes nothing and closes nothing. Without that, a
        repeat release would resolve its removals against a descriptor
        already closed, and the number may by then have been reissued —
        so the removals would land in an unrelated directory and the
        close would strip the claim of whatever now holds it.

        The directory is removed while the claim is still held; see the
        directory re-check in `LocalDiscovery`'s implementation notes.

        Every removal resolves against the claim's own descriptor rather
        than the namespace's path, so an owner reclaims the directory it
        holds and never one that replaced it. A directory removed from
        under a live owner, by a temporary-file sweeper or a careless
        hand, frees the namespace for a successor while this claim lives;
        without that, this owner's ordinary exit would go on to delete
        the successor's registry.

        A process that inherited this owner by forking removes nothing.
        It drops only its own references, which leaves the parent's locks
        held, because the descriptors it closes share the parent's open
        file description. See **Forks** in `LocalDiscovery`.
        """
        claim, self._claim = self._claim, -1
        if claim < 0:
            return
        liveness, self._liveness = self._liveness, -1
        if self._fork_generation != _fork_generation or os.getpid() != self._owner_pid:
            _close_quietly(liveness)
            os.close(claim)
            return
        try:
            directory = _namespace_directory(self._namespace)
            # The pointer first, so no borrower resolves this generation
            # once it is going away; then the contents, so a borrower
            # already bound fails at its next operation; then the
            # directory itself.
            _unlink_quietly(directory, _CURRENT, dir_fd=claim)
            _purge(claim)
            # The name is all `rmdir` has, so it is removed only while the
            # path still resolves to the claimed directory.
            try:
                claimed = _same_file(claim, directory)
            except OSError:
                # Exiting never raises, and a directory this claim may no
                # longer hold is not this owner's to remove.
                claimed = False
            if claimed:
                _rmdir_quietly(directory)
        finally:
            _close_quietly(liveness)
            os.close(claim)

    class Publisher:
        """Publisher for broadcasting worker discovery events.

        Publishes worker discovery events (see `~wool.DiscoveryEvent`) to
        a namespace's registry, where subscribers discover them.
        Publishers in different processes write to one namespace under a
        cross-process file lock. A publisher borrows the namespace, and
        its binding ends with the owner it bound; see `LocalDiscovery`
        for the ownership contract.

        :param namespace:
            The namespace identifier for the registry to borrow. See
            `LocalDiscovery` for the domain it must lie in.
        :param block_size:
            Size in bytes for worker metadata storage blocks. Each
            block spends 4 bytes on a length prefix, leaving
            ``block_size - 4`` for the serialized metadata, so it must
            exceed 4; see `LocalDiscovery` for what that floor does and
            does not rule out. Defaults to 1024 bytes, which
            accommodates typical worker metadata including tags and
            extra metadata.
        :param lock_timeout:
            Maximum seconds to wait for the cross-process file lock before
            raising `TimeoutError`. ``None`` waits forever. Defaults to
            30.0.
        :raises ValueError:
            If ``namespace`` is outside the domain `LocalDiscovery`
            documents, if ``block_size`` does not exceed the 4-byte
            length prefix, or if ``lock_timeout`` is negative.
        """

        _binding: _Binding | None
        _block_pool: ResourcePool[_File]
        _blocks: dict[str, AsyncExitStack]
        _lock_timeout: float | None
        _namespace: Final[str]

        #: Registry announcements are only discoverable on a common
        #: host, so this publisher prescribes the loopback bind.
        #: See `~wool.DiscoveryPublisherLike.bind_host` for the contract.
        bind_host: str = "127.0.0.1"

        def __init__(
            self,
            namespace: str,
            *,
            lock_timeout: float | None = DEFAULT_LOCK_TIMEOUT,
        ):
            _validate_namespace(namespace)
            if lock_timeout is not None and lock_timeout < 0:
                raise ValueError("Lock timeout must be non-negative")
            self._namespace = namespace
            self._lock_timeout = lock_timeout
            self._binding = None
            # The block each published worker holds, keyed by its ref;
            # a drop exits the handle and the exit closes the rest.
            self._blocks = {}
            self._block_pool = ResourcePool(
                factory=self._block_factory,
                finalizer=self._block_finalizer,
                ttl=0,
            )

        async def __aenter__(self) -> Self:
            """Bind the namespace's live generation, then enter the block pool.

            See `LocalDiscovery` for the borrowing contract and
            `_Binding` for what binding proves.

            :returns:
                This instance.
            :raises DiscoveryNamespaceNotFound:
                If the namespace has no live owner; see
                `DiscoveryNamespaceNotFound`.
            """
            # Bound before the pool is entered, so a namespace with no
            # live owner leaves no pool entered. The binding pins the
            # generation, so every later publish reaches the owner this
            # publisher bound and a successor's registry is unreachable
            # rather than merely guarded against; see `_Binding`.
            self._binding = _Binding.bind(self._namespace)
            try:
                await self._block_pool.__aenter__()
            except BaseException:
                self._binding.close()
                self._binding = None
                raise
            return self

        async def __aexit__(self, *args):
            """Close every block this publisher holds, then exit the pool.

            The first close that fails is the one that propagates; a
            later block's failure is discarded, so one bad handle cannot
            hide the others or stop them being released. The block pool
            is exited regardless, and its own failure supersedes.

            Nothing is unlinked here. The blocks belong to the
            namespace's owner, which frees whatever this publisher did
            not drop, so exiting after the owner has gone is silent
            rather than a failure to remove what is already removed.

            :param args:
                The exception info the block is exiting with, forwarded
                to the pool.
            """
            failure: BaseException | None = None
            try:
                for ref in list(self._blocks):
                    try:
                        await self._blocks.pop(ref).aclose()
                    except BaseException as error:
                        if failure is None:
                            failure = error
                if failure is not None:
                    raise failure
            finally:
                if self._binding is not None:
                    self._binding.close()
                    self._binding = None
                await self._block_pool.__aexit__(*args)

        @property
        def namespace(self) -> str:
            """The namespace identifier for this publisher.

            :returns:
                The namespace string.
            """
            return self._namespace

        async def publish(self, type: DiscoveryEventType, metadata: WorkerMetadata):
            """Publish a worker discovery event.

            Writes the event to the namespace's registry where subscribers
            can discover it. The operation is synchronized across
            processes using file locking to ensure consistency. After
            publishing, touches a notification file to wake subscribers
            via filesystem events.

            Publishing ``worker-added`` for a worker that is already
            registered refreshes the registration in place — last write
            wins — consuming no additional slot, so a single
            ``worker-dropped`` always fully unregisters the worker. Live
            subscribers observe a refresh as a ``worker-updated`` event.
            A refresh writes into the block
            created at the worker's first registration, so ``block_size``
            governs only blocks this publisher creates. If that block has
            vanished — e.g., its publisher exited without dropping the
            worker — the re-add reclaims the stale registration and
            registers the worker fresh.

            Each call opens its own registry handle and releases it on
            return, so the descriptor it locks is the descriptor it
            writes; see `LocalDiscovery`'s implementation notes.

            :param type:
                The type of discovery event.
            :param metadata:
                Worker metadata to publish.
            :raises RuntimeError:
                If an unexpected event type is provided, or the registry's
                header is not stamped.
            :raises DiscoveryCapacityExhausted:
                For ``worker-added``, if the registry is already at
                capacity and the worker is not already registered.
            :raises DiscoveryWorkerNotFound:
                For ``worker-updated``, if the worker is not registered,
                or if its registration names a block that has since been
                reclaimed, which leaves nothing to update in place.
            :raises DiscoveryBlockExhausted:
                For ``worker-added`` and ``worker-updated``, if the
                serialized metadata exceeds the worker's block, which is
                rejected before anything is written, so the prior
                registration is left intact.
            :raises TimeoutError:
                If the cross-process file lock is not acquired within this
                publisher's ``lock_timeout``.
            :raises DiscoveryNamespaceNotFound:
                If the namespace has no live owner; see
                `DiscoveryNamespaceNotFound`.
            :raises RuntimeError:
                If this publisher's context is not entered, or was
                already exited.
            """
            binding = self._bound
            registry = _open_registry(self._namespace, binding.generation)
            with closing(registry):
                async with _lock(
                    registry, namespace=self._namespace, timeout=self._lock_timeout
                ):
                    # Checked under the lock, so a write never lands in a
                    # registry whose owner is gone. Opening the registry
                    # already catches an owner that exited and reclaimed
                    # it; this catches one killed with no successor,
                    # which leaves the file in place and readable.
                    if not binding.alive():
                        raise DiscoveryNamespaceNotFound(self._namespace)
                    match type:
                        case "worker-added":
                            await self._add(metadata, registry)
                        case "worker-dropped":
                            await self._drop(metadata, registry)
                        case "worker-updated":
                            await self._update(metadata, registry)
                        case _:
                            raise RuntimeError(
                                f"Unexpected discovery event type: {type}"
                            )

                    _notify(binding.generation)

        @property
        def _bound(self) -> _Binding:
            """The binding this publisher made when it was entered.

            :returns:
                The binding on the generation this publisher borrows.
            :raises RuntimeError:
                If this publisher's context is not entered.
            """
            binding = self._binding
            if binding is None:
                raise RuntimeError(
                    "Publisher is not entered - use it as an async context manager"
                )
            return binding

        @property
        def _generation(self) -> Path:
            """The generation this publisher bound.

            :returns:
                The bound generation's directory.
            """
            return self._bound.generation

        async def _add(self, metadata: WorkerMetadata, registry: _File):
            """Register a worker, or refresh one already registered.

            See `publish` for the re-add contract. The refresh opens the
            existing block by name rather than acquiring it from the pool,
            so it holds no pool reference and reaches blocks created by
            another publisher's pool.

            The ledger holds at most one handle per ref, and a
            registration that displaces one closes it last: the new
            handle is installed first, so no ``await`` separates the
            pop from the install and a cancellation cannot leave the
            ref with no handle at all.

            The block the pool hands back is re-checked against its
            name. The pool returns a cached handle on a key hit, so a
            block reclaimed underneath this publisher would otherwise be
            written through an unlinked inode and its slot published
            naming a file no reader can open.

            :param metadata:
                The worker to publish to the namespace's registry.
            :raises DiscoveryCapacityExhausted:
                If no slots are available and the worker is not already
                registered.
            """
            ref = _WorkerReference(metadata.uid)
            serialized = metadata.to_protobuf().SerializeToString()

            free_offset = None
            match_offset = None
            for offset, slot in _iter_slots(registry):
                if slot == ref.bytes:
                    match_offset = offset
                    break
                if free_offset is None and slot == _NULL_REF:
                    free_offset = offset

            if match_offset is not None:
                try:
                    with closing(_block(self._generation, ref)) as block_file:
                        _write_block(block_file, serialized)
                    return
                except FileNotFoundError:
                    # Stale slot; see the re-add contract in `publish`.
                    registry.write(_NULL_REF, match_offset)
                    if free_offset is None:
                        free_offset = match_offset
                    # A handle this publisher still holds names the block
                    # that vanished. Release it so the registration below
                    # creates the block afresh; the pool would otherwise
                    # hand back that same unlinked handle, leaving a slot
                    # naming a file no reader can open.
                    if (vanished := self._blocks.pop(str(ref), None)) is not None:
                        await vanished.aclose()

            if free_offset is None:
                raise DiscoveryCapacityExhausted(self._bound.header.capacity)

            block = AsyncExitStack()
            try:
                block_file = await block.enter_async_context(
                    self._block_pool.get(str(ref))
                )
                if not block_file.current():
                    # The pool handed back a cached handle on a block
                    # that no longer bears this name. The ledger still
                    # holds a reference to that entry, so the pool would
                    # return the same stale handle again; drop the
                    # ledger's reference first so the count reaches zero
                    # and the factory recreates the block.
                    await block.aclose()
                    if (gone := self._blocks.pop(str(ref), None)) is not None:
                        await gone.aclose()
                    block = AsyncExitStack()
                    block_file = await block.enter_async_context(
                        self._block_pool.get(str(ref))
                    )
                _write_block(block_file, serialized)
                registry.write(ref.bytes, free_offset)
            except BaseException:
                # Release what this method acquired rather than delegating
                # to `_drop`, whose slot scan cannot find a ref that only
                # lands on the last line of this block. `BaseException`,
                # so a cancellation cannot strand the block and its pool
                # reference; the bare `raise` preserves its semantics.
                await block.aclose()
                raise
            # One handle per ref — see the docstring. The new handle is
            # installed before the displaced one is released, so no
            # `await` separates the pop from the install.
            stale = self._blocks.pop(str(ref), None)
            self._blocks[str(ref)] = block
            if stale is not None:
                await stale.aclose()

        async def _drop(self, metadata: WorkerMetadata, registry: _File):
            """Unregister a worker by removing it from the registry.

            The handle this publisher holds is released whether or not
            the slot scan matched, since a peer that dropped the same
            worker first leaves nothing to match and the block is this
            publisher's to release either way.

            The block is removed here rather than by the pool's
            finalizer, so the slot and the block it names are freed in
            one critical section under the registry lock this already
            holds. Removing it outside that lock is what let one
            publisher's teardown unlink a block another was writing.

            :param metadata:
                The worker to unpublish from the namespace's registry.
            """
            target_ref = _WorkerReference(metadata.uid)

            for offset, slot in _iter_slots(registry):
                if slot == target_ref.bytes:
                    registry.write(_NULL_REF, offset)
                    break
            # Released outside the scan — see the docstring.
            block = self._blocks.pop(str(target_ref), None)
            if block is not None:
                await block.aclose()
            _unlink_quietly(self._generation / _BLOCKS, str(target_ref))

        async def _update(self, metadata: WorkerMetadata, registry: _File):
            """Update a registered worker's metadata block.

            Opens the worker's block by name — holding no pool reference,
            and reaching blocks created by another publisher's pool — and
            rewrites it via `_write_block`.

            An update has nothing to create: a slot whose block has been
            reclaimed names a registration that exists in name only, so
            it is reported as an unregistered worker rather than as a
            missing file. Re-registering with ``worker-added`` is the
            caller's route back, and it recreates the block.

            :param metadata:
                The updated worker to publish to the namespace's registry.
            :raises DiscoveryWorkerNotFound:
                If the worker is not registered, or its registration
                names a block that has since been reclaimed.
            """
            target_ref = _WorkerReference(metadata.uid)
            serialized = metadata.to_protobuf().SerializeToString()

            for _, slot in _iter_slots(registry):
                if slot == target_ref.bytes:
                    try:
                        with closing(_block(self._generation, target_ref)) as block_file:
                            _write_block(block_file, serialized)
                    except FileNotFoundError as error:
                        raise DiscoveryWorkerNotFound(metadata.uid) from error
                    return

            raise DiscoveryWorkerNotFound(metadata.uid)

        def _block_factory(self, name: str) -> _File:
            """Create a worker's metadata block in the generation's directory.

            The block belongs to the namespace's owner, not to this
            publisher: it is created under the generation this publisher
            bound and is reclaimed when that owner exits. Nothing is
            armed against this process's own exit, because a publisher
            that never exits leaves nothing the owner does not free.

            :param name:
                The block's file name, i.e., the worker reference.
            :returns:
                The new block's handle.
            """
            bound = self._bound
            return _File.create(
                bound.generation / _BLOCKS / name, bound.header.block_size
            )

        def _block_finalizer(self, block: _File):
            """Release a metadata block's handle without removing it.

            A block is the owner's artifact, so releasing a handle on one
            never unlinks it: the slot and the block are removed together
            by `_drop`, under the registry lock that serializes them, and
            anything a publisher does not drop is freed when the owner
            exits. See `LocalDiscovery` for the ownership contract.

            :param block:
                The block to finalize.
            """
            block.close()

    class Subscriber(
        metaclass=SubscriberMeta,
        key=lambda cls, namespace, *, poll_interval=None: (
            cls,
            namespace,
            poll_interval,
        ),
    ):
        """Subscriber for receiving worker discovery events.

        Yields worker discovery events (see `~wool.DiscoveryEvent`) from a
        namespace's registry as workers are added, updated, or dropped,
        rescanning the registry whenever a publisher writes to it. A
        subscriber borrows the namespace, and its subscription ends with
        the owner it bound; see `LocalDiscovery` for the ownership
        contract.

        Constructions sharing a ``namespace`` and ``poll_interval``
        within one `contextvars.Context` are served from one
        subscription; see `SubscriberMeta`. Only the iteration that
        starts a subscription binds, so only that iteration raises
        `DiscoveryNamespaceNotFound` at the bind. A subscription whose
        owner then goes away fails for the iteration pulling it; every
        other iteration sharing it currently ends without events instead
        of raising, and an iteration that joins a failing bind ends
        without events too.

        :param namespace:
            The namespace identifier for the registry to borrow. See
            `LocalDiscovery` for the domain it must lie in.
        :param poll_interval:
            Seconds between rescans in addition to the rescans publisher
            writes trigger. ``None`` rescans only when a publisher writes.
            Part of the subscription key.
        :raises ValueError:
            If ``namespace`` is outside the domain `LocalDiscovery`
            documents, or ``poll_interval`` is not positive. Construction
            is deferred, so both surface from the iteration that starts
            the subscription rather than from the constructor.
        """

        _namespace: Final[str]
        _poll_interval: Final[float | None]

        if TYPE_CHECKING:

            def __new__(
                cls, namespace: str, *, poll_interval: float | None = None
            ) -> DiscoverySubscriberLike: ...

        def __init__(
            self,
            namespace: str,
            *,
            poll_interval: float | None = None,
        ):
            _validate_namespace(namespace)
            self._namespace = namespace
            if poll_interval is not None and poll_interval <= 0:
                raise ValueError(f"Expected positive poll interval, got {poll_interval}")
            self._poll_interval = poll_interval

        def __aiter__(self) -> AsyncIterator[DiscoveryEvent]:
            return self._event_stream()

        async def _event_stream(self) -> AsyncGenerator[DiscoveryEvent, None]:
            """Bind the registry and yield events from each rescan of it.

            A watchdog observer watches the notification file publishers
            touch after each write, and each notification triggers a
            rescan of the registry. A ``poll_interval`` adds a rescan
            whenever that many seconds pass without a notification.

            A slot this scan cannot read does not end the subscription
            and does not drop the worker; see `_carry_forward`.

            :yields:
                Discovery events as changes are detected in the registry.
            """
            cached_workers: dict[str, WorkerMetadata] = {}
            carried: dict[str, int] = {}
            notification = asyncio.Event()
            loop = asyncio.get_running_loop()

            # Every resource this iteration takes is held in a local and
            # released by this stack, so concurrent iterations of one
            # subscriber never share a descriptor or an observer; see
            # `~wool.DiscoverySubscriberLike`.
            with ExitStack() as resources:
                # The generation is resolved and pinned once, so this
                # subscription follows one owner's incarnation and ends
                # with it rather than drifting onto a successor's
                # registry. Bound first, so a bind that raises starts no
                # observer.
                binding = resources.enter_context(
                    closing(_Binding.bind(self._namespace))
                )
                generation = binding.generation
                registry = resources.enter_context(
                    closing(_open_registry(self._namespace, generation))
                )
                watchdog = generation / _NOTIFY
                handler = _Watchdog(notification, watchdog, loop)
                observer = Observer()
                observer.schedule(handler, path=str(watchdog.parent), recursive=False)
                try:
                    observer.start()
                except OSError as error:
                    # The owner exited between the bind and the watch.
                    raise DiscoveryNamespaceNotFound(self._namespace) from error
                # Registered the instant the thread exists, so every
                # later failure retires it rather than stranding it.
                resources.callback(observer.join)
                resources.callback(observer.stop)
                while True:
                    # Cleared before the read, so a notification arriving
                    # at any point during this scan is preserved and
                    # wakes the next one.
                    notification.clear()
                    # Checked before every read, so a borrower that
                    # outlived its owner fails here rather than serving
                    # the snapshot it last saw. `current` catches a
                    # reclaimed or superseded generation; the liveness
                    # probe catches an owner killed with no successor,
                    # which leaves the file in place.
                    if not registry.current() or not binding.alive():
                        raise DiscoveryNamespaceNotFound(self._namespace)
                    discovered_workers: dict[str, WorkerMetadata] = {}
                    for _, slot in _iter_slots(registry):
                        if slot == _NULL_REF:
                            continue
                        try:
                            ref = _WorkerReference.from_bytes(slot)
                            metadata = self._deserialize_metadata(generation, ref)
                        except Exception:
                            self._carry_forward(
                                slot, cached_workers, discovered_workers, carried
                            )
                            continue
                        uid = str(metadata.uid)
                        carried.pop(uid, None)
                        discovered_workers[uid] = metadata
                    carried = {
                        uid: scans
                        for uid, scans in carried.items()
                        if uid in discovered_workers
                    }

                    for event in self._diff(cached_workers, discovered_workers):
                        yield event
                    await self._park(binding, registry, notification, loop)

        async def _park(
            self,
            binding: _Binding,
            registry: _File,
            notification: asyncio.Event,
            loop: asyncio.AbstractEventLoop,
        ) -> None:
            """Wait for the next rescan, re-checking the owner while waiting.

            Returns when a publisher writes or the poll interval
            elapses. Waking to check the owner is not waking to rescan:
            the check is two non-blocking locks and a `os.readlink`,
            where a rescan reads every registered worker's block and
            emits an event for each, so a subscription with no poll
            interval stays silent over an idle namespace however long
            it lives.

            :param binding:
                The generation binding to probe.
            :param registry:
                The registry handle to test for supersession.
            :param notification:
                The event a publisher's write sets.
            :param loop:
                The running loop, for its clock.
            :raises DiscoveryNamespaceNotFound:
                If the owner this subscription bound is gone.
            """
            deadline = (
                None
                if self._poll_interval is None
                else loop.time() + self._poll_interval
            )
            while True:
                timeout = _LIVENESS_INTERVAL
                if deadline is not None:
                    timeout = min(timeout, max(0.0, deadline - loop.time()))
                try:
                    await asyncio.wait_for(notification.wait(), timeout=timeout)
                except asyncio.TimeoutError:
                    if deadline is not None and loop.time() >= deadline:
                        return
                    # See the same pair at the top of the scan loop for
                    # what each of the two checks catches.
                    if not registry.current() or not binding.alive():
                        raise DiscoveryNamespaceNotFound(self._namespace)
                else:
                    return

        def _carry_forward(
            self,
            slot: bytes,
            cached_workers: dict[str, WorkerMetadata],
            discovered_workers: dict[str, WorkerMetadata],
            carried: dict[str, int],
        ) -> None:
            """Report an unreadable slot's worker from the last scan that read it.

            A slot can fail to read for reasons that say nothing about
            whether its worker is live: its block may have been unlinked
            between the slot read and the block open, a refresh may have
            landed mid-read, or the bytes may be short or foreign.
            Omitting the worker would make `_diff` emit
            ``worker-dropped`` for it and ``worker-added`` again on the
            next wake, so a transient fault becomes an eviction and
            readmission. Carrying the last good metadata forward reports
            the worker unchanged instead.

            The carry has a floor, because boxes above would otherwise
            turn a *persistent* fault — a corrupted registry, a foreign
            file — into a subscription serving stale metadata forever
            with no signal at all. After `_CARRY_FORWARD_LIMIT`
            consecutive scans the worker is still reported, so no
            spurious drop is emitted, but each further scan says so.

            A slot whose bytes do not even yield a UUID, or whose worker
            was never read successfully, has nothing to carry and is
            skipped.

            :param slot:
                The raw slot bytes that could not be read.
            :param cached_workers:
                The workers as of the last scan that read them.
            :param discovered_workers:
                This scan's workers, updated in place.
            :param carried:
                Consecutive carried scans per worker, updated in place.
            """
            try:
                uid = str(UUID(bytes=slot))
            except (ValueError, TypeError):
                return
            cached = cached_workers.get(uid)
            if cached is None:
                return
            scans = carried[uid] = carried.get(uid, 0) + 1
            if scans >= _CARRY_FORWARD_LIMIT:
                logger.warning(
                    "Worker %s in discovery namespace %r has been unreadable for "
                    "%d consecutive scans; still reporting its last known metadata",
                    uid,
                    self._namespace,
                    scans,
                )
            discovered_workers[uid] = cached

        def _deserialize_metadata(self, generation: Path, ref: _WorkerReference):
            """Load and deserialize a worker's metadata from its block.

            :param generation:
                The generation whose blocks to read, held by the calling
                iteration rather than by this subscriber.
            :param ref:
                The reference identifying the worker's metadata block.
            :returns:
                The deserialized WorkerMetadata instance.
            :raises FileNotFoundError:
                If the block does not exist.
            """
            with closing(_block(generation, ref, write=False)) as block_file:
                protobuf = wire.WorkerMetadata.FromString(_read_block(block_file))
                return WorkerMetadata.from_protobuf(protobuf)

        def _diff(
            self,
            cached_workers: dict[str, WorkerMetadata],
            discovered_workers: dict[str, WorkerMetadata],
        ):
            """Detect and emit events for worker changes.

            Performs a three-way comparison between the cached worker state and
            the newly discovered workers, identifying which workers have been
            added, dropped, or updated. Updates the cache in-place and yields
            appropriate discovery events for each change.

            Every still-registered worker yields ``worker-updated`` on
            every scan, whether or not its metadata changed. That is
            deliberate and load-bearing rather than merely wasteful: it
            is the only route by which a consumer that refused a worker
            — `~wool.WorkerProxy` refusing one against its lease — is
            offered that worker again once it has room. Suppressing the
            unchanged case would leave such a worker unadmitted until it
            happened to change or re-register.

            :param cached_workers:
                Dictionary of previously discovered workers (UID string ->
                WorkerMetadata). Modified in-place to reflect current state.
            :param discovered_workers:
                Dictionary of workers found in the current scan (UID string ->
                WorkerMetadata).
            :yields:
                Discovery events for each detected change (worker-added,
                worker-dropped, worker-updated).
            """

            for uid in set(discovered_workers) - set(cached_workers):
                cached_workers[uid] = discovered_workers[uid]
                event = DiscoveryEvent("worker-added", metadata=discovered_workers[uid])
                yield event

            for uid in set(cached_workers) - set(discovered_workers):
                discovered_worker = cached_workers.pop(uid)
                event = DiscoveryEvent("worker-dropped", metadata=discovered_worker)
                yield event

            for uid in set(cached_workers) & set(discovered_workers):
                cached_workers[uid] = discovered_workers[uid]
                event = DiscoveryEvent(
                    "worker-updated", metadata=discovered_workers[uid]
                )
                yield event


def _same_file(fd: int, path: Path) -> bool:
    """Return whether ``path`` still names the file ``fd`` holds open.

    Three call sites ask this question and each wants a different answer
    when it cannot be asked at all — a reclaimed handle reports itself
    stale, a claim loop retries, and teardown declines to remove what it
    may no longer own — so this returns only the comparison and lets
    every error reach the caller that has a policy for it.

    :param fd:
        An open descriptor on the file to compare against.
    :param path:
        The path whose current target to compare.
    :returns:
        True while ``path`` resolves to the file ``fd`` holds.
    :raises OSError:
        If ``path`` cannot be stated.
    """
    return os.path.samestat(os.fstat(fd), os.stat(path))


def _validate_namespace(namespace: str) -> None:
    """Reject a namespace that would not name one directory under `_root`.

    `_namespace_directory` interpolates the namespace into a single path
    component, so a namespace carrying a separator or a relative-path
    element would claim, write and unlink outside this module's root. The
    domain is one non-empty path component that renders within the
    filesystem's limit on a name.

    :param namespace:
        The namespace to check.
    :raises ValueError:
        If ``namespace`` is empty, contains a path separator or a NUL, is
        ``.`` or ``..``, or renders a directory name exceeding
        `_NAME_MAX` bytes.
    """
    if not namespace:
        raise ValueError("Expected a non-empty namespace")
    if "/" in namespace:
        raise ValueError(f"Expected a namespace without '/', got {namespace!r}")
    if "\x00" in namespace:
        raise ValueError(f"Expected a namespace without a NUL, got {namespace!r}")
    if namespace in (os.curdir, os.pardir):
        raise ValueError(
            f"Expected a namespace that is not a relative path element, "
            f"got {namespace!r}"
        )
    rendered = len(namespace.encode())
    if rendered > _NAME_MAX:
        raise ValueError(
            f"Expected a namespace rendering within {_NAME_MAX} bytes, "
            f"got one rendering {rendered}"
        )


def _validate_block_size(block_size: int) -> None:
    """Reject a block size that cannot hold the length prefix and a payload.

    `_write_block` spends ``struct.calcsize("I")`` bytes on the length
    prefix and bounds the payload against the block's live size, so a
    block no larger than the prefix leaves nothing for metadata and makes
    every publish raise `DiscoveryBlockExhausted` permanently.

    This bound is necessary and not sufficient: a block a few bytes above
    the prefix still cannot hold the smallest serialized `WorkerMetadata`,
    and fails the same permanent way. It rejects only the sizes that are
    unusable by arithmetic rather than by payload.

    :raises ValueError:
        If ``block_size`` does not exceed the length prefix.
    """
    prefix = struct.calcsize("I")
    if block_size <= prefix:
        raise ValueError(
            f"Expected block size greater than the {prefix}-byte length prefix, "
            f"got {block_size}"
        )


def _read_header(registry: _File) -> _Header:
    """Return the sizes the owner stamped into a registry's header.

    The owner writes `_HEADER_MAGIC` and both sizes into the header of a
    staging file, renames it into place, and only then publishes the
    pointer, so a borrower resolves a fully stamped registry or none at
    all. A file this module did not create therefore cannot be reached
    through the pointer; the one way to meet an unstamped header is to
    plant a file under a world-writable root, which `_open_registry`
    reports as a namespace with nothing to borrow.

    :param registry:
        The open registry.
    :returns:
        The stamped capacity and block size.
    :raises ValueError:
        If the header is short or does not carry `_HEADER_MAGIC`.
    """
    size = struct.calcsize(_HEADER_FORMAT)
    header = registry.read(size, 0)
    if len(header) < size:
        raise ValueError(f"Discovery registry header is short: {len(header)} of {size}")
    magic, capacity, block_size = struct.unpack(_HEADER_FORMAT, header)
    if magic != _HEADER_MAGIC:
        raise ValueError(f"Expected discovery registry magic, got {magic!r}")
    return _Header(capacity, block_size)


def _iter_slots(registry: _File) -> Iterator[tuple[int, bytes]]:
    """Yield each ``(offset, ref_bytes)`` slot bounded by the stamped capacity.

    Reads the owner-stamped capacity from the header and walks exactly
    that many slots, so ``capacity`` — not the file's size — is the
    enforced ceiling. Yields nothing for a registry whose header is not
    stamped. The capacity is re-read on every call, so a scan always
    reflects the current header.

    The slots are read in one pass, so an iteration walks the registry as
    it stood when the iteration began rather than picking up writes made
    part way through it.

    :param registry:
        The open registry to scan.
    :yields:
        ``(offset, ref_bytes)`` for each 16-byte slot, in order.
    """
    capacity = _read_header(registry).capacity
    slots = registry.read(capacity * _REF_WIDTH, _HEADER_SIZE)
    for index in range(capacity):
        start = index * _REF_WIDTH
        yield _HEADER_SIZE + start, slots[start : start + _REF_WIDTH]


def _write_block(block: _File, serialized: bytes) -> None:
    """Write size-prefixed metadata over a block's current contents.

    A payload that does not fit the block is rejected before anything is
    written, so a rejected write leaves the block's prior registration
    intact.

    :param block:
        The open block to write.
    :param serialized:
        The serialized metadata to write.
    :raises DiscoveryBlockExhausted:
        If the payload does not fit the block.
    """
    size = len(serialized)
    if struct.calcsize("I") + size > block.size:
        raise DiscoveryBlockExhausted(size)
    block.write(struct.pack(f"I{size}s", size, serialized), 0)


def _read_block(block: _File) -> bytes:
    """Return the serialized metadata a block holds.

    The prefix and the payload come from one read sized by the block, so
    a refresh landing between them cannot pair a new size with an old
    payload. State the guarantee honestly: one read against
    `_write_block`'s one write narrows tearing to a page-level property
    of the platform, and POSIX promises no atomicity between them.

    Sizing the read from the block rather than from the prefix also keeps
    a torn or foreign prefix from driving an allocation of up to 4 GiB.

    :param block:
        The open block to read.
    :returns:
        The serialized metadata, as written by `_write_block`.
    :raises ValueError:
        If the block is shorter than the length prefix, or declares a
        payload longer than the block holds.
    """
    prefix = struct.calcsize("I")
    buffer = block.read(block.size, 0)
    if len(buffer) < prefix:
        raise ValueError(
            f"Block holds {len(buffer)} bytes, short of the {prefix}-byte prefix"
        )
    size = struct.unpack("I", buffer[:prefix])[0]
    if prefix + size > len(buffer):
        raise ValueError(
            f"Block declares a {size}-byte payload but holds "
            f"{len(buffer) - prefix} bytes after its prefix"
        )
    return buffer[prefix : prefix + size]


def _unlink_quietly(directory: Path, name: str, *, dir_fd: int | None = None) -> None:
    """Remove a file without raising, warning if it fails unexpectedly.

    Every teardown path in this module removes files through here, so no
    teardown can raise: an exception escaping a ``__exit__`` or an
    `atexit` handler would replace the exception the caller was already
    unwinding, or crash the interpreter at shutdown.

    A file that is already gone is the expected case and passes silently.
    Any other failure leaves the file in place, which is a leak the caller
    cannot act on but an operator can, so it surfaces as a
    `ResourceWarning` rather than being swallowed.

    The directory and the name are separate so that ``dir_fd`` cannot
    disagree with the path a warning names: with the two joined, a caller
    could pass one namespace's path and another's descriptor, and remove
    the second while reporting the first.

    :param directory:
        The directory holding the file. It is walked only when ``dir_fd``
        is absent, and names the file in any warning either way.
    :param name:
        The file's name within ``directory``.
    :param dir_fd:
        A descriptor on ``directory``, which the removal resolves ``name``
        against instead of walking the path. Pass it wherever the
        directory may be replaced between opening it and removing from
        it; see `LocalDiscovery._release`.
    """
    try:
        if dir_fd is None:
            os.unlink(directory / name)
        else:
            os.unlink(name, dir_fd=dir_fd)
    except FileNotFoundError:
        pass
    except OSError as error:
        warnings.warn(
            f"failed to remove {str(directory / name)!r}: {error}",
            ResourceWarning,
            stacklevel=2,
        )


def _rmdir_quietly(directory: Path) -> None:
    """Remove a directory if it is empty, without raising.

    A directory that is already gone, or that still holds files, passes
    silently. Any other failure surfaces as a `ResourceWarning`; see
    `_unlink_quietly`.

    :param directory:
        The directory to remove.
    """
    try:
        os.rmdir(directory)
    except OSError as error:
        if error.errno not in (errno.ENOENT, errno.ENOTEMPTY, errno.EEXIST):
            warnings.warn(
                f"failed to remove {str(directory)!r}: {error}",
                ResourceWarning,
                stacklevel=2,
            )


def _close_quietly(fd: int) -> None:
    """Close a descriptor if it is open, without raising.

    A sentinel descriptor, i.e., one never opened or already closed,
    passes silently, so teardown can close unconditionally.

    :param fd:
        The descriptor to close, or a negative sentinel.
    """
    if fd < 0:
        return
    try:
        os.close(fd)
    except OSError:
        pass


@cache
def _root() -> Path:
    """Return the directory that holds every namespace's directory.

    :returns:
        ``/dev/shm`` on Linux when it is writable and searchable, i.e.,
        a usable memory-backed directory, otherwise the temporary
        directory. Symlinks are resolved, so watchdog event paths compare
        equal to paths built from it.
    """
    shm = Path("/dev/shm")
    if sys.platform.startswith("linux") and os.access(shm, os.W_OK | os.X_OK):
        return shm.resolve()  # pragma: no cover — Linux only
    return Path(tempfile.gettempdir()).resolve()


def _namespace_directory(namespace: str) -> Path:
    """Return the path of a namespace's claim directory, without creating it.

    This is the directory an owner claims, and it outlives any one
    owner: it holds the `_CURRENT` pointer and the generation directory
    that pointer names. See `LocalDiscovery` for the layout.

    :param namespace:
        The namespace identifying the directory.
    :returns:
        The path of the namespace's claim directory.
    """
    return _root() / _ROOT_DIRNAME / namespace


def _resolve_generation(namespace: str) -> Path:
    """Return the live generation directory a borrower should bind.

    Reads the `_CURRENT` pointer an owner published, so a borrower binds
    the generation that was live when it bound and never drifts onto a
    successor's.

    :param namespace:
        The namespace whose live generation to resolve.
    :returns:
        The path of the live generation's directory.
    :raises DiscoveryNamespaceNotFound:
        If the namespace has no owner, i.e., no pointer or no target.
    """
    directory = _namespace_directory(namespace)
    try:
        generation = os.readlink(directory / _CURRENT)
    except OSError as error:
        raise DiscoveryNamespaceNotFound(namespace) from error
    resolved = directory / generation
    if not resolved.is_dir():
        raise DiscoveryNamespaceNotFound(namespace)
    return resolved


def _block(generation: Path, ref: _WorkerReference, *, write: bool = True) -> _File:
    """Open an existing worker metadata block.

    :param generation:
        The generation directory whose blocks to look in.
    :param ref:
        The reference identifying the worker's block.
    :param write:
        Whether the handle may write; see `_File.open`. A scan passes
        ``False``, which is the difference that matters, since it opens
        one block per registered worker per pass.
    :returns:
        A handle on the block.
    :raises FileNotFoundError:
        If the block does not exist.
    """
    return _File.open(generation / _BLOCKS / str(ref), write=write)


def _open_registry(namespace: str, generation: Path) -> _File:
    """Open a generation's registry for a borrower's bind.

    Every borrower binds through here. It never creates a registry or
    any directory, and reports a missing registry as
    `DiscoveryNamespaceNotFound` — which is what a borrower sees once
    its owner has exited and reclaimed the generation.

    :param namespace:
        The namespace the generation belongs to, for the error.
    :param generation:
        The generation directory whose registry to open.
    :returns:
        The open registry.
    :raises DiscoveryNamespaceNotFound:
        If the generation has no registry.
    """
    try:
        return _File.open(generation / _REGISTRY)
    except FileNotFoundError as error:
        raise DiscoveryNamespaceNotFound(namespace) from error


def _owner_alive(generation_fd: int) -> bool:
    """Report whether the owner that staged a generation still holds it.

    An owner holds an exclusive lock on its generation's directory for
    that generation's whole life, so a shared lock that cannot be taken
    proves the owner's process is alive, and one that can proves the
    kernel dropped the owner's lock — which it does however the owner
    died, signals included.

    Probed shared, so concurrent borrowers never exclude each other and
    mistake a peer's probe for a live owner. Probed on the generation
    rather than the namespace, so it can never make a claim fail and
    report a live namespace as in use.

    Only contention answers ``True``. A descriptor this process no
    longer holds, or never held, answers ``False``: it proves nothing
    about the owner, and a borrower told its owner is alive on that
    evidence serves the snapshot it last read forever. Anything else is
    a question the probe cannot answer and is raised rather than
    guessed, so a filesystem without ``flock`` fails loudly instead of
    reporting every dead owner as alive.

    :param generation_fd:
        A descriptor on the generation's directory, held since the bind,
        or a negative sentinel once the borrower has released it. Pinned
        rather than reopened per call because the open dominates the
        cost of the probe by an order of magnitude.
    :returns:
        True while the owner's process holds the generation.
    :raises OSError:
        If the lock can be neither taken nor refused.
    """
    if generation_fd < 0:
        return False
    try:
        fcntl.flock(generation_fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
    except BlockingIOError:
        return True
    except OSError as error:
        if error.errno == errno.EBADF:
            return False
        raise
    else:
        fcntl.flock(generation_fd, fcntl.LOCK_UN)
        return False


def _purge(dir_fd: int) -> None:
    """Remove every entry beneath the directory a descriptor holds open.

    Resolves nothing: every operation is relative to a descriptor and
    descends only through `os.O_NOFOLLOW`, so a symlink planted in a
    world-writable root — ``/dev/shm`` is mode 1777 and shared by every
    user on the host — cannot redirect a removal outside the tree. The
    directory itself is left in place for the caller to remove, which is
    what lets an owner purge the generation it is claiming without
    dropping the claim.

    Never raises: this runs on teardown paths that never raise. An entry
    another process removed first passes silently, and any other failure
    surfaces as a `ResourceWarning`; see `_unlink_quietly`. What is left
    behind is a leak the caller cannot act on but an operator can, and a
    whole generation is the larger of the two leaks this module can
    produce.

    :param dir_fd:
        A descriptor on the directory to empty.
    """
    try:
        names = os.listdir(dir_fd)
    except OSError as error:
        warnings.warn(
            f"failed to list a directory to purge: {error}",
            ResourceWarning,
            stacklevel=2,
        )
        return
    for name in names:
        # Directory-ness is decided up front rather than inferred from
        # the errno a failed unlink reports: `PermissionError` is `EPERM`
        # or `EACCES`, and the sticky bit on a mode-1777 root yields
        # `EPERM` for another user's plain file too.
        try:
            entry = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
        except FileNotFoundError:
            continue
        except OSError as error:
            warnings.warn(
                f"failed to remove {name!r}: {error}",
                ResourceWarning,
                stacklevel=2,
            )
            continue
        if not stat.S_ISDIR(entry.st_mode):
            try:
                os.unlink(name, dir_fd=dir_fd)
            except FileNotFoundError:
                pass
            except OSError as error:
                warnings.warn(
                    f"failed to remove {name!r}: {error}",
                    ResourceWarning,
                    stacklevel=2,
                )
            continue
        try:
            child = os.open(
                name,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=dir_fd,
            )
        except OSError:
            # Replaced by a symlink or removed since it was listed.
            continue
        try:
            _purge(child)
        finally:
            os.close(child)
        try:
            os.rmdir(name, dir_fd=dir_fd)
        except FileNotFoundError:
            continue
        except OSError as error:
            warnings.warn(
                f"failed to remove {name!r}: {error}",
                ResourceWarning,
                stacklevel=2,
            )


@asynccontextmanager
async def _lock(
    registry: _File,
    *,
    namespace: str,
    timeout: float | None = DEFAULT_LOCK_TIMEOUT,
):
    """Acquire an exclusive lock on an open registry.

    Locks the registry file itself to synchronize writes across
    unrelated processes publishing to the same registry, without
    blocking the event loop while waiting for acquisition. The lock and
    the data it guards therefore share an inode; see `LocalDiscovery`'s
    implementation notes.

    ``timeout`` bounds **acquisition only, never the held section**. Once the
    lock is held the ``with`` body runs to completion regardless of how long
    it takes.

    :param registry:
        The open registry to lock.
    :param namespace:
        The namespace the registry belongs to, named in a timeout.
    :param timeout:
        Maximum seconds to wait for acquisition. ``None`` waits forever.
        Defaults to `DEFAULT_LOCK_TIMEOUT`.
    :raises TimeoutError:
        If the lock is not acquired within ``timeout`` seconds.

    .. rubric:: Implementation notes

    Acquisition uses non-blocking `fcntl.flock` attempts, retrying every
    1ms (``await asyncio.sleep(0.001)``) until the lock is acquired or
    ``timeout`` elapses. The holder is another open file description,
    which may belong to another process or to another publish on this
    loop, since a publish opens its own handle. A hot spin cannot shorten
    a cross-process hold, and it drives this loop's ``select`` timeout to
    zero, so it burns CPU and wakeups for no gain — hence the 1ms poll
    rather than a zero-second yield.

    The held section runs to completion because interrupting a holder
    mid-write would corrupt the registry for every process reading it.
    """
    loop = asyncio.get_running_loop()
    deadline = None if timeout is None else loop.time() + timeout
    while True:
        try:
            fcntl.flock(registry.file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except BlockingIOError:
            if deadline is not None and loop.time() >= deadline:
                raise TimeoutError(
                    f"Timed out after {timeout}s waiting to acquire the "
                    f"discovery lock for namespace {namespace!r}"
                )
            await asyncio.sleep(0.001)

    try:
        yield
    finally:
        fcntl.flock(registry.file.fileno(), fcntl.LOCK_UN)


def _notify(generation: Path) -> None:
    """Wake a generation's subscribers by touching its notification file.

    A generation whose owner has exited has no notification file, and no
    subscriber can start watching it, so a missing file passes silently
    rather than being recreated.

    :param generation:
        The generation directory whose subscribers to wake.
    """
    try:
        os.utime(generation / _NOTIFY)
    except FileNotFoundError:
        pass
