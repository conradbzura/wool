from __future__ import annotations

import asyncio
import contextvars
import logging
import threading
import warnings
import weakref
from contextlib import contextmanager
from inspect import isawaitable
from typing import TYPE_CHECKING
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Coroutine
from typing import Final
from typing import Iterator
from typing import NoReturn
from typing import TypeVar
from typing import cast
from uuid import UUID
from uuid import uuid4

import cloudpickle

import wool.protocol as protocol
from wool.runtime.typing import Serializer
from wool.runtime.typing import Undefined
from wool.runtime.typing import UndefinedType

if TYPE_CHECKING:
    from wool.runtime.context.var import ContextVar
    from wool.runtime.context.var import StubPin
    from wool.runtime.context.var import Token

T = TypeVar("T")

_log = logging.getLogger(__name__)


# Ambient per-Context dispatch timeout in seconds. ``None`` means no
# timeout. The value scopes to whichever execution chain is currently
# active and rides through nested dispatches until reset or overridden.
dispatch_timeout: Final[contextvars.ContextVar[float | None]] = contextvars.ContextVar(
    "dispatch_timeout", default=None
)


# public
class RuntimeContext:
    """Block-scoped runtime option overrides for wool routines.

    Used as a context manager to override runtime options (currently
    only :data:`dispatch_timeout`) for the duration of a block. Auto-
    captured on every :class:`Task` at construction time, which ships
    the caller's snapshot across the wire so the worker restores it
    before running the routine.

    :param dispatch_timeout:
        Default timeout for task dispatch operations. ``None`` means
        no timeout. Leaving this argument out (the default sentinel)
        causes ``__enter__`` to skip setting the stdlib var — useful
        for constructing an empty :class:`RuntimeContext` whose sole
        purpose is to ride the wire.
    """

    _dispatch_timeout: float | None | UndefinedType
    _dispatch_timeout_token: contextvars.Token[float | None] | None

    def __init__(
        self,
        *,
        dispatch_timeout: float | None | UndefinedType = Undefined,
    ) -> None:
        self._dispatch_timeout = dispatch_timeout
        self._dispatch_timeout_token = None

    def __enter__(self) -> RuntimeContext:
        if self._dispatch_timeout is not Undefined:
            self._dispatch_timeout_token = dispatch_timeout.set(self._dispatch_timeout)
        return self

    def __exit__(self, *_):
        if self._dispatch_timeout_token is not None:
            dispatch_timeout.reset(self._dispatch_timeout_token)
            self._dispatch_timeout_token = None

    @classmethod
    def get_current(cls) -> RuntimeContext:
        """Capture the current stdlib :data:`dispatch_timeout` value."""
        return cls(dispatch_timeout=dispatch_timeout.get())

    @classmethod
    def from_protobuf(cls, context: protocol.RuntimeContext) -> RuntimeContext:
        """Reconstruct from a :class:`protocol.RuntimeContext` message."""
        return cls(
            dispatch_timeout=(
                context.dispatch_timeout
                if context.HasField("dispatch_timeout")
                else None
            )
        )

    def to_protobuf(self) -> protocol.RuntimeContext:
        """Serialize to a :class:`protocol.RuntimeContext` message."""
        message = protocol.RuntimeContext()
        timeout = self._dispatch_timeout
        if timeout is Undefined:
            timeout = dispatch_timeout.get()
        if timeout is not None:
            message.dispatch_timeout = timeout
        return message


class _Missing:
    """Sentinel distinguishing "unset" from "explicitly None"."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "<missing>"


_MISSING: Final[_Missing] = _Missing()


class _Token:
    """Restore cookie returned by :func:`attach` and consumed by
    :func:`detach` to undo a :class:`Context` installation.

    Mirrors :class:`contextvars.Token` in spirit: opaque to the
    caller, carrying enough state to return the registry slot to
    exactly where it was (including the "was-unset" case, which
    :func:`detach` pops rather than rewriting to ``None``).
    """

    __slots__ = ("_key", "_previous", "_used")

    _key: asyncio.Task[Any] | threading.Thread
    _previous: Context | _Missing
    _used: bool

    def __init__(
        self,
        key: asyncio.Task[Any] | threading.Thread,
        previous: Context | _Missing,
    ) -> None:
        self._key = key
        self._previous = previous
        self._used = False


_context_registry: Final[
    weakref.WeakKeyDictionary[asyncio.Task[Any] | threading.Thread, Context]
] = weakref.WeakKeyDictionary()
_context_registry_lock: Final[threading.Lock] = threading.Lock()

var_registry: Final[weakref.WeakValueDictionary[str, ContextVar[Any]]] = (
    weakref.WeakValueDictionary()
)
var_registry_lock: Final[threading.Lock] = threading.Lock()


# Process-wide weak registry of live :class:`Token` instances keyed
# by id. Populated in :meth:`Token.__init__` and
# :meth:`Token._reconstitute`. Used for (a) preserving pickle
# identity within a process and (b) resolving incoming wire ids to
# live tokens so their ``_used`` flag can be flipped on merge. Weak
# values auto-prune when a token is GC'd, so transient tokens from a
# ``set``/``reset`` loop do not accumulate.
token_registry: Final[weakref.WeakValueDictionary[UUID, Token]] = (
    weakref.WeakValueDictionary()
)


def carries_state(wire_ctx: protocol.Context) -> bool:
    """Return True iff a wire :class:`protocol.Context` has state
    worth applying to the local context on receive.

    The context id is used separately for chain tracking; only var
    bindings and consumed tokens need to be merged via
    :meth:`Context.update`. When both maps are empty, the receiver
    can skip deserialization entirely.
    """
    return bool(wire_ctx.vars) or bool(wire_ctx.consumed_tokens)


def _scope_key() -> asyncio.Task[Any] | threading.Thread:
    """Identify the current execution scope (asyncio task, or
    thread for sync callers).
    """
    try:
        return asyncio.current_task() or threading.current_thread()
    except RuntimeError:
        return threading.current_thread()


# public
def current_context() -> Context:
    """Return the live :class:`wool.Context` for the current execution scope.

    Inside an asyncio task, looks up the task's :class:`Context` in
    the process-wide registry. Outside a task (sync code), uses a
    per-thread fallback. If no :class:`Context` exists for the
    current scope, one is created lazily and registered.
    """
    with _context_registry_lock:
        ctx = _context_registry.get((key := _scope_key()))
        if ctx is None:
            ctx = Context()
            _context_registry[key] = ctx
        return ctx


def peek() -> Context | None:
    """Return the :class:`Context` bound to the current scope, or
    ``None`` if none is bound.

    Non-registering counterpart to :func:`current_context` — for
    introspective callers that must not produce a lazy-
    registration side effect (e.g. pickling a var from an
    unrelated code path).
    """
    with _context_registry_lock:
        return _context_registry.get(_scope_key())


def attach(ctx: Context) -> _Token:
    """Install *ctx* as the current scope's :class:`Context` and
    return a token for :func:`detach`.

    The token records whether the slot was previously unset, so
    :func:`detach` can pop the binding entirely rather than
    rewriting it to an unrelated :class:`Context`.
    """
    with _context_registry_lock:
        previous = _context_registry.get((key := _scope_key()), _MISSING)
        _context_registry[key] = ctx
    return _Token(key, previous)


def detach(token: _Token) -> None:
    """Undo an :func:`attach`, restoring the registry slot to its
    prior state.

    If the slot was unset at :func:`attach` time, the binding is
    popped. Otherwise the previous :class:`Context` is reinstalled.
    """
    if token._used:
        raise RuntimeError("token already consumed by detach")
    token._used = True
    with _context_registry_lock:
        if isinstance(token._previous, _Missing):
            _context_registry.pop(token._key, None)
        else:
            _context_registry[token._key] = token._previous


class ContextAlreadyBound(RuntimeError):
    """Raised when :func:`register` is called for a task that is
    already bound to a :class:`Context`.

    Enforces the one-shot contract: a task is bound exactly once,
    at creation time. A second :func:`register` call (or a call
    that races against the wool task factory's copy-on-fork seed)
    would silently stomp the prior binding, masking bugs where the
    wrong :class:`Context` (and thus the wrong chain ID) rides
    through nested dispatches.
    """


def register(task: asyncio.Task[Any], ctx: Context) -> None:
    """Bind *ctx* to *task* in the per-task registry.

    Out-of-band registration for tasks running outside the current
    scope — e.g. worker-loop tasks whose :class:`Context` is fixed
    by the caller's wire-shipped ID rather than by the scope that
    spawned the task.

    :raises ContextAlreadyBound:
        If *task* is already bound to a :class:`Context` (one-shot
        contract — see :class:`ContextAlreadyBound`).
    """
    with _context_registry_lock:
        if task in _context_registry:
            raise ContextAlreadyBound(
                f"task {task!r} is already bound to {_context_registry[task]!r}"
            )
        _context_registry[task] = ctx


# public
class Context:
    """Snapshot of :class:`wool.ContextVar` state and context ID,
    scoped to a single task at a time.

    Mirrors :class:`contextvars.Context`: supports the mapping and
    container protocols and scopes mutations via :meth:`Context.run`.

    Beyond the snapshot of :class:`ContextVar` values, a
    :class:`Context` carries a ``UUID`` that identifies the
    logical chain it belongs to.

    .. caution::
       At most one task may run inside a given :class:`Context` at
       a time. :meth:`run` raises :class:`RuntimeError` on
       re-entry.
    """

    __slots__ = (
        "_id",
        "_data",
        "_running",
        "_running_lock",
        "_stub_pins",
        "_used_token_ids",
        "_incoming_used_ids",
    )

    _id: UUID
    _data: dict[ContextVar[Any], Any]
    _running: bool
    _running_lock: threading.Lock
    _stub_pins: set[StubPin]
    # Ids of tokens consumed within this Context's logical chain. A
    # per-Context set lets wire emission run in O(k) over actually-
    # used tokens rather than scanning the process-wide token registry.
    _used_token_ids: set[UUID]
    # Wire-ingress staging buffer holding token ids the peer reported
    # as consumed, pending per-id merge against the local token
    # registry. Empty on a freshly constructed Context; written only
    # by the wire-deserialization path.
    _incoming_used_ids: set[UUID]

    def __init__(self) -> None:
        self._id = uuid4()
        self._data = {}
        self._running = False
        self._running_lock = threading.Lock()
        self._stub_pins = set()
        self._used_token_ids = set()
        self._incoming_used_ids = set()

    def __iter__(self) -> Iterator[ContextVar[Any]]:
        return iter(self._data)

    def __getitem__(self, var: ContextVar[T]) -> T:
        return self._data[var]

    def __contains__(self, var: Any) -> bool:
        return var in self._data

    def __len__(self) -> int:
        return len(self._data)

    def __bool__(self) -> bool:
        """Return True if this :class:`Context` carries any observable
        state — var bindings, locally-consumed token ids, or
        incoming used-token ids pending merge.

        Diverges intentionally from :meth:`__len__` (which counts
        only var bindings, per the mapping-container contract). The
        bool surface aligns with the union of the inbound merge
        criterion (``_incoming_used_ids``) and the outbound wire
        emission criterion (``_data`` and ``_used_token_ids``, the
        two sources :meth:`to_protobuf` populates ``wire_ctx.vars`` and
        ``wire_ctx.consumed_tokens`` from), so a :class:`Context` reports
        truthy whenever it has anything observable in either
        direction.
        """
        return (
            bool(self._data)
            or bool(self._used_token_ids)
            or bool(self._incoming_used_ids)
        )

    def __repr__(self) -> str:
        return f"<wool.Context id={self._id} vars={len(self)}>"

    def __reduce__(self) -> NoReturn:
        # Mirror :class:`contextvars.Context`: refuse pickle, copy.copy,
        # and copy.deepcopy. All three would yield a snapshot
        # disconnected from any live state, which is uniformly a
        # footgun. Callers wanting in-process duplication must use
        # :meth:`Context.copy` explicitly.
        raise TypeError(
            "cannot pickle 'wool.Context' object — use Context.copy() "
            "for in-process duplication"
        )

    @property
    def id(self) -> UUID:
        """The UUID that identifies this :class:`Context`'s logical chain."""
        return self._id

    @classmethod
    def from_protobuf(
        cls,
        wire_ctx: protocol.Context,
        *,
        serializer: Serializer | None = None,
    ) -> Context:
        """Reconstruct a :class:`Context` from a wire :class:`protocol.Context`.

        Deserializes each var value in ``wire_ctx.vars`` against
        the process-wide :class:`wool.ContextVar` registry. Keys
        unknown on this process are resolved to a stub pinned to the
        current :class:`Context`, so a lazy-import receiver sees the
        propagated value as soon as it later declares the var — the
        same stub-promotion path a pickled :class:`ContextVar`
        instance uses when it rides through ``__reduce__`` embedded
        in a routine argument.

        :param wire_ctx:
            The wire :class:`protocol.Context` to decode.
        :param serializer:
            Deserializer for values. ``None`` (default) selects
            ``cloudpickle``.
        :raises ValueError:
            If a key's serialized value fails to decode, or if
            ``wire_ctx.id`` is non-empty but not valid hex.
        """
        # Deferred import to break the structural base ↔ var cycle:
        # ``var`` imports ``Context`` and friends at module load,
        # so importing ``resolve_stub`` here at function scope keeps
        # base.py's load order independent of var.py.
        from wool.runtime.context.var import resolve_stub

        if serializer is None:
            serializer = cast(Serializer, cloudpickle)
        ctx_id = UUID(hex=wire_ctx.id) if wire_ctx.id else uuid4()
        incoming_used_ids: set[UUID] = set()
        for token_hex in wire_ctx.consumed_tokens:
            try:
                incoming_used_ids.add(UUID(hex=token_hex))
            except ValueError:
                warnings.warn(
                    f"Skipping malformed consumed-token id on wire: {token_hex!r}",
                    RuntimeWarning,
                    stacklevel=2,
                )
        data: dict[ContextVar[Any], Any] = {}
        # Build the receiving Context up front so resolved stubs pin
        # to it, not to whatever Context happens to be active on the
        # decoding task. This keeps stub lifetime aligned with the
        # Context the routine reads from and avoids a side-effect
        # registration of a Context on the decoding task's scope.
        ctx = cls._reconstitute(ctx_id, data, incoming_used_ids)
        for key, encoded in wire_ctx.vars.items():
            var = resolve_stub(key, ctx)
            try:
                data[var] = serializer.loads(encoded)
            except Exception as e:
                raise ValueError(
                    f"Failed to deserialize wool.ContextVar {key!r}: {e}"
                ) from e
        return ctx

    def copy(self) -> Context:
        """Return a shallow copy of this :class:`Context` with a fresh ID.

        Mirrors :meth:`contextvars.Context.copy` — the copy is a
        new logical chain with its own UUID, so mutations to the
        copy do not affect this :class:`Context` and nested
        dispatches fired under the copy carry its fresh ID, not this
        :class:`Context`'s. The consumed-token set is not carried
        across: any :class:`Token` minted under this
        :class:`Context`'s UUID is already incompatible with the
        copy's UUID for :meth:`ContextVar.reset` purposes.
        """
        return Context._reconstitute(uuid4(), dict(self))

    def run(self, fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
        """Run the specified callable in this :class:`Context`.

        Installs this :class:`Context` as the current scope's active
        :class:`Context`, runs the callable, then restores the
        previous :class:`Context`. Mutations made by the callable go
        directly into this :class:`Context`.

        :raises RuntimeError:
            If this :class:`Context` is already running a task.
        """
        with self._guard():
            token = attach(self)
            try:
                return fn(*args, **kwargs)
            finally:
                detach(token)

    async def run_async(
        self,
        fn: Callable[..., Awaitable[T] | T],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Run *fn* in this :class:`Context`, awaiting the result if awaitable.

        Mirrors :meth:`run` for callables that may return a coroutine.
        The :class:`Context` stays attached across the await, so
        :meth:`ContextVar.get` calls inside the awaited body resolve
        against this :class:`Context`. The single-task invariant via
        :meth:`_guard` is held across the suspension — concurrent
        entry from another task raises.

        :raises RuntimeError:
            If this :class:`Context` is already running a task.
        """
        with self._guard():
            token = attach(self)
            try:
                result = fn(*args, **kwargs)
                if isawaitable(result):
                    result = await result
                return cast(T, result)
            finally:
                detach(token)

    def get(self, var: ContextVar[T], default: Any = None) -> Any:
        """Return *var*'s value in this :class:`Context`, or *default* if unset."""
        return self._data.get(var, default)

    def keys(self):
        return self._data.keys()

    def values(self):
        return self._data.values()

    def items(self):
        return self._data.items()

    def update(self, other: Context) -> None:
        """Apply *other*'s vars and used-token state to this :class:`Context`.

        One-way: *other* is the source of truth for overlapping
        keys. This :class:`Context`'s ID is unchanged. Mirrors
        :meth:`dict.update` semantics for the var map. Each id in
        *other*'s incoming-used-ids set is resolved against the
        process-wide :data:`token_registry`; if a live
        :class:`Token` is found, its ``_used`` flag is flipped so a
        subsequent :meth:`ContextVar.reset` raises. The id is also
        recorded in this :class:`Context`'s used-token-id set so a
        subsequent :meth:`to_protobuf` on this :class:`Context`
        forwards the consumed-token state onward.
        """
        self._data.update(other._data)
        for token_id in other._incoming_used_ids:
            token = token_registry.get(token_id)
            if token is not None and not token._used:
                token._used = True
            self._used_token_ids.add(token_id)

    def to_protobuf(
        self,
        *,
        serializer: Serializer | None = None,
    ) -> protocol.Context:
        """Snapshot this :class:`Context` to a wire :class:`protocol.Context`.

        Serializes every var:value pair observable via the mapping
        protocol under ``"<namespace>:<name>"`` keys. Default-only
        values — vars that have never been explicitly set in this
        :class:`Context` — are absent from the snapshot and do not
        ride the wire.

        :param serializer:
            Serializer for values. ``None`` (default) selects
            ``cloudpickle``.
        :raises TypeError:
            If a value fails to serialize.
        """
        if serializer is None:
            serializer = cast(Serializer, cloudpickle)
        wire_ctx = protocol.Context(id=self._id.hex)
        for var, value in self.items():
            try:
                wire_ctx.vars[var._key] = serializer.dumps(value)
            except Exception as e:
                raise TypeError(
                    f"Failed to serialize wool.ContextVar {var._key!r}: {e}"
                ) from e
        wire_ctx.consumed_tokens.extend(id.hex for id in self._used_token_ids)
        return wire_ctx

    @classmethod
    def _reconstitute(
        cls,
        context_id: UUID,
        data: dict[ContextVar[Any], Any],
        incoming_used_ids: set[UUID] | None = None,
    ) -> Context:
        """Rebuild a :class:`Context` from externally-supplied parts.

        Bypasses ``__init__`` to adopt an externally-supplied
        context id, data dict, and incoming-used-ids buffer, for
        callers that already hold the canonical identity and state
        to rebuild a :class:`Context` around. Not a copy — the dict
        and set references are taken as-is.
        """
        instance: Context = object.__new__(cls)
        instance._id = context_id
        instance._data = data
        instance._running = False
        instance._running_lock = threading.Lock()
        instance._stub_pins = set()
        instance._used_token_ids = set()
        instance._incoming_used_ids = (
            incoming_used_ids if incoming_used_ids is not None else set()
        )
        return instance

    @contextmanager
    def _guard(self) -> Iterator[None]:
        """Enforce the single-task invariant for the wrapped block.

        Acquires the running flag under ``_running_lock`` on entry
        (raising :class:`RuntimeError` if another task already holds
        it) and releases it on exit. Thread-safe.
        """
        with self._running_lock:
            if self._running:
                raise RuntimeError(
                    "wool.Context is already running; at most one "
                    "task may run inside a given Context at a time"
                )
            self._running = True
        try:
            yield
        finally:
            with self._running_lock:
                self._running = False


# public
def copy_context() -> Context:
    """Return a shallow copy of the current :class:`wool.Context`.

    Mirrors :func:`contextvars.copy_context` — returns a shallow
    copy of the current scope's context as a new :class:`Context`
    instance. The copy receives a fresh logical-chain ID, so it is
    independent of the source's chain for dispatch, tracing, and
    :class:`Token` scoping purposes.
    """
    return current_context().copy()


def install_task_factory(
    loop: asyncio.AbstractEventLoop | None = None,
) -> None:
    """Install Wool's task factory on the given (or running) loop.

    Composes with an existing factory if one is set, so that
    asyncio child tasks created via ``create_task`` inherit a
    forked :class:`Context`. Safe to call multiple times — a
    subsequent call on a loop that already has the Wool-wrapped
    factory installed is a no-op.

    **Ordering contract** — If a user installs their own task factory
    *after* Wool's, Wool's seeding via :data:`_context_registry` is
    dropped and copy-on-fork breaks silently for subsequently-created
    tasks. Install Wool's factory last (or compose manually) when
    other libraries also want a factory on the same loop.
    """
    if loop is None:
        loop = asyncio.get_running_loop()

    existing = loop.get_task_factory()
    if existing is not None and getattr(existing, "__wool_wrapped__", False):
        _log.debug(f"wool-composed task factory already installed on {loop}")
        return
    inner = existing if existing is not None else _default_task_factory

    def wool_factory(
        loop: asyncio.AbstractEventLoop,
        coro: Coroutine[Any, Any, Any],
        **kwargs: Any,
    ) -> asyncio.Task[Any]:
        task: asyncio.Task[Any] = inner(loop, coro, **kwargs)  # type: ignore[assignment]
        # Fork the parent's Context (shallow copy with a fresh chain id)
        # onto the new task. If no parent is bound for the current scope,
        # seed an empty Context.
        child = parent.copy() if (parent := peek()) is not None else Context()
        register(task, child)
        return task

    wool_factory.__wool_wrapped__ = True  # type: ignore[attr-defined]
    wool_factory.__wool_inner__ = inner  # type: ignore[attr-defined]
    loop.set_task_factory(wool_factory)
    if existing is None:
        _log.debug(f"wool task factory installed on {loop}")
    else:
        _log.debug(
            f"wool task factory composed with existing factory {existing} on {loop}",
        )


def _default_task_factory(
    loop: asyncio.AbstractEventLoop,
    coro: Coroutine[Any, Any, Any],
    **kwargs: Any,
) -> asyncio.Task[Any]:
    """Fall-back task factory matching :meth:`AbstractEventLoop.create_task`.

    Used when no user factory is installed, so wool's factory has
    a uniform inner layer to delegate to.
    """
    return asyncio.Task(coro, loop=loop, **kwargs)


def create_bound_task(
    loop: asyncio.AbstractEventLoop,
    coro: Coroutine[Any, Any, Any],
    ctx: Context,
    **kwargs: Any,
) -> asyncio.Task[Any]:
    """Create a task pre-bound to *ctx*, skipping the wool task
    factory's copy-on-fork seed.

    Invokes the configured task factory (respecting any user-
    provided composition), then binds *ctx* to the resulting task
    via :func:`register`. Used when the caller has already
    captured the :class:`Context` the task should run under — e.g.
    worker-loop tasks spawned from a dispatch handler, whose ctx
    was captured on the handler thread before the worker-loop
    hop.

    :raises ContextAlreadyBound:
        Never raised in practice — fresh tasks aren't yet in the
        registry. Surfaces only if the caller somehow passes a
        task that was already bound.
    """
    factory = loop.get_task_factory()
    task: asyncio.Task[Any]
    if factory is not None and getattr(factory, "__wool_wrapped__", False):
        inner = factory.__wool_inner__  # type: ignore[attr-defined]
        task = inner(loop, coro, **kwargs)
    elif factory is not None:
        task = factory(loop, coro, **kwargs)  # type: ignore[assignment]
    else:
        task = _default_task_factory(loop, coro, **kwargs)
    register(task, ctx)
    return task
