from __future__ import annotations

import sys
import weakref
from typing import Any
from typing import ClassVar
from typing import Final
from typing import Generic
from typing import TypeVar
from typing import overload
from uuid import UUID
from uuid import uuid4

from wool.runtime.context import base
from wool.runtime.context.base import Context
from wool.runtime.context.base import current_context
from wool.runtime.context.base import peek
from wool.runtime.typing import Undefined

T = TypeVar("T")

_PACKAGE: Final = "wool.runtime.context"


# public
class ContextVarCollision(Exception):
    """Raised when two distinct :class:`ContextVar` instances are
    constructed with the same ``(namespace, name)`` key.

    Keys must be unique within the inferred package namespace. Library
    authors should pass ``namespace=`` explicitly when constructing
    vars from shared factory code; application code can rely on the
    implicit package-name inference.
    """


class _UnsetType:
    """Picklable singleton representing a context variable with no value.

    Exposed via :attr:`Token.MISSING` to mirror the stdlib
    ``Token.MISSING`` sentinel. Used internally to distinguish "var
    has no value in this Context" from "var is set to None."
    """

    _instance: _UnsetType | None = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __reduce__(self):
        return (_UnsetType, ())

    def __repr__(self):
        return "<Token.MISSING>"

    def __bool__(self):
        return False


_UNSET: Final = _UnsetType()


class StubPin:
    """Severable anchor that keeps a stub :class:`ContextVar` alive.

    Held strongly by each :class:`Context` that observed the stub's
    creation (via :data:`Context._stub_pins`), and weakly indexed by
    var key in :data:`_stub_pin_anchors`. Because the only path from
    a Context to its pinned stub goes through this anchor, promotion
    can release the stub in O(1) by nulling :attr:`stub` — live
    Contexts retain the gutted anchor until they are themselves
    collected, but the stub itself is free to be reclaimed as soon
    as user references drop.
    """

    __slots__ = ("stub", "__weakref__")

    stub: ContextVar[Any] | None

    def __init__(self, stub: ContextVar[Any]):
        self.stub = stub


# Weakly indexes anchors by var key for O(1) release on promotion.
# Entries auto-prune when the anchor has no strong refs (all pinning
# Contexts have died without promotion occurring).
_stub_pin_anchors: weakref.WeakValueDictionary[str, StubPin] = (
    weakref.WeakValueDictionary()
)


def _pin_stub(stub: ContextVar[Any], ctx: Context) -> None:
    """Pin a freshly reconstructed stub to ``ctx`` and the global index."""
    anchor = StubPin(stub)
    _stub_pin_anchors[stub._key] = anchor
    ctx._stub_pins.add(anchor)


def resolve_stub(key: str, ctx: Context, *, default: Any = Undefined) -> ContextVar[Any]:
    """Return the :class:`ContextVar` registered under *key*, creating a
    stub pinned to *ctx* if no authoritative declaration exists yet.

    Unifies the two ingress paths that may encounter an unregistered
    var key on a receiving process: the pickle-embedded :class:`ContextVar`
    instance path (via :meth:`ContextVar._reconstitute`) and the wire
    snapshot path (via :meth:`Context.from_protobuf`). Both route
    through this helper so a lazy-import receiver converges on the
    same state regardless of whether the value arrived as a bare
    wire entry or embedded in a pickled var reference.

    Pass *default* to seed the stub's default before promotion when
    that information is available on the ingress side (the pickle
    path carries it; the wire-snapshot path does not).
    """
    with base.var_registry_lock:
        existing = base.var_registry.get(key)
        if existing is not None:
            return existing
        namespace, _, name = key.partition(":")
        stub: ContextVar[Any] = object.__new__(ContextVar)
        stub._name = name
        stub._namespace = namespace
        stub._key = key
        stub._default = default
        stub._stub = True
        base.var_registry[key] = stub
        _pin_stub(stub, ctx)
        return stub


def _release_stub(key: str) -> None:
    """Release the stub pin for ``key`` so the stub can be reclaimed.

    Called on stub promotion. Pops the anchor from the global index
    and severs its strong reference to the stub; any live Context
    still holding the (now gutted) anchor in its pin set drops it
    naturally when the Context itself is collected.
    """
    anchor = _stub_pin_anchors.pop(key, None)
    if anchor is not None:
        anchor.stub = None


def _infer_namespace() -> str:
    """Infer the namespace for a :class:`ContextVar` constructor call.

    Walks up the call stack from the current frame, skipping frames
    from any ``wool.runtime.context`` submodule, and returns the
    top-level package of the first user frame encountered. Falls back
    to ``"__main__"`` if the walk reaches the top of the stack.
    """
    frame = sys._getframe(1)
    while frame is not None:
        mod = frame.f_globals.get("__name__", "")
        if mod and mod != _PACKAGE and not mod.startswith(_PACKAGE + "."):
            return mod.partition(".")[0]
        frame = frame.f_back
    return "__main__"  # pragma: no cover — stack always has a caller


# public
class Token(Generic[T]):
    """Picklable token for reverting a :class:`ContextVar` mutation.

    Mirrors :class:`contextvars.Token`: single-use, same-var
    rejection, and scoped to the :class:`wool.Context` in which it
    was created. Attempting to :meth:`ContextVar.reset` with a token
    minted in a different :class:`wool.Context` raises
    :class:`ValueError`.

    In-process, the :class:`Context` is checked by object identity.
    Cross-process (unpickled token), the :class:`Context`'s UUID is
    compared as a fallback.
    """

    __slots__ = (
        "_id",
        "_var",
        "_old_value",
        "_context",
        "_context_id",
        "_used",
        "__weakref__",
    )

    MISSING: ClassVar[_UnsetType] = _UNSET

    _id: UUID
    _var: ContextVar[T]
    _old_value: T | _UnsetType
    _context: Context | None
    _context_id: UUID
    _used: bool

    def __init__(
        self,
        var: ContextVar[T],
        old_value: T | _UnsetType,
        context: Context,
    ):
        self._id = uuid4()
        self._var = var
        self._old_value = old_value
        self._context = context
        self._context_id = context._id
        self._used = False
        base.token_registry[self._id] = self

    def __reduce__(self):
        # Reduce by key rather than by var instance: a Token is a
        # reset receipt, not a value-bearing wire payload. Pickling
        # the full :class:`ContextVar` would route through
        # :meth:`ContextVar.__reduce__`, which embeds the current
        # value and applies it to the receiver's :class:`Context` on
        # unpickle — a side effect the dispatch path covers via
        # :meth:`Context.from_protobuf` independently. Carrying just
        # the key keeps Token round-trips side-effect-free outside
        # the dispatch wire path.
        return (
            Token._reconstitute,
            (
                self._id,
                self._var._key,
                self._old_value,
                self._context_id,
                self._used,
            ),
        )

    def __repr__(self) -> str:
        used_marker = " used" if self._used else ""
        return f"<wool.Token var={self._var._key!r}{used_marker}>"

    @property
    def id(self) -> UUID:
        """The UUID that identifies this :class:`Token` on the wire."""
        return self._id

    @property
    def var(self) -> ContextVar[T]:
        """The :class:`ContextVar` this token was created for."""
        return self._var

    @property
    def old_value(self) -> T | _UnsetType:
        """The prior value the var held before the :meth:`ContextVar.set`
        call that produced this token. Returns :attr:`Token.MISSING`
        if the var had no value set.

        :attr:`Token.MISSING` is a singleton; check for it via
        identity (``token.old_value is Token.MISSING``) rather than
        :func:`isinstance` since the underlying sentinel type is
        internal and not part of the public API.
        """
        return self._old_value

    @property
    def used(self) -> bool:
        """Whether this :class:`Token` has been consumed by :meth:`ContextVar.reset`.

        ``True`` once a successful :meth:`ContextVar.reset` call has
        passed this token, whether the call occurred in this process
        or in another process running in a :class:`Context` whose
        used-token state has since been merged back (via pickle
        round-trip or back-propagation into this token via the
        process-wide token registry). Tokens are single-use across
        the logical chain; a second :meth:`ContextVar.reset` raises
        :class:`RuntimeError`.
        """
        return self._used

    @classmethod
    def _reconstitute(
        cls,
        token_id: UUID,
        key: str,
        old_value: T | _UnsetType,
        context_id: UUID,
        used: bool,
    ) -> Token[T]:
        """Rebuild a :class:`Token` from externally-supplied parts.

        Same-process pickle identity is preserved via the
        :data:`base.token_registry` registry: if a live :class:`Token`
        already exists under *token_id*, it is returned unchanged and
        its ``_used`` flag is OR'd with *used* so a used-then-pickled
        copy does not silently revert the live token's state.
        Cross-process callers see a fresh stub with ``_context=None``
        and ``_used`` adopted from the pickled tuple. The owning
        :class:`ContextVar` is resolved from *key* via the process-
        wide registry — falling back to a stub pinned to the active
        :class:`Context` when the key is unknown — so Token pickle
        round-trips do not transitively re-apply the originating
        Context's value to the receiver.
        """
        existing: Token[T] | None = base.token_registry.get(token_id)
        if existing is not None:
            if used and not existing._used:
                existing._used = True
            return existing
        var = resolve_stub(key, current_context())
        instance: Token[T] = object.__new__(cls)
        instance._id = token_id
        instance._var = var
        instance._old_value = old_value
        instance._context = None
        instance._context_id = context_id
        instance._used = used
        base.token_registry[token_id] = instance
        return instance


# public
class ContextVar(Generic[T]):
    """Propagating context variable that crosses worker boundaries.

    Mirrors :class:`contextvars.ContextVar` at the surface: construct
    with a name and optional default; call :meth:`get`, :meth:`set`,
    :meth:`reset`. Unlike :class:`contextvars.ContextVar`, instances
    pickle across process boundaries and their values propagate through
    ``@wool.routine`` dispatches.

    **Identity model** — Every :class:`ContextVar` has a unique
    ``(namespace, name)`` key. The ``name`` is the first positional
    argument; the ``namespace`` is inferred from the top-level package
    of the calling frame or provided explicitly via ``namespace=``.
    Two distinct instances constructed under the same key raise
    :class:`ContextVarCollision`.

    **Namespace stability** — The inferred namespace is the top-level
    package of the calling frame. This is deliberately coarse so that
    wire keys stay stable when a module is refactored deeper within
    its package — a rolling deploy that moves
    ``myapp.auth.tokens`` to ``myapp.auth.credentials.tokens``
    continues to propagate values between caller and worker. The
    trade-off is that two subpackages of the same library cannot
    define distinct vars with the same ``name`` without one of them
    passing ``namespace=`` explicitly; the construction raises
    :class:`ContextVarCollision` instead.

    **Storage model** — Values are stored in the current
    :class:`Context` (one per :class:`asyncio.Task`, one per thread
    for sync code). Child tasks fork a copy of the parent's
    :class:`Context` on creation, mirroring
    :func:`contextvars.copy_context` semantics.

    Values propagated across the wire must be cloudpicklable.
    """

    __slots__ = (
        "_name",
        "_namespace",
        "_key",
        "_default",
        "_stub",
        "__weakref__",
    )

    _name: str
    _namespace: str
    _key: str
    _default: Any
    _stub: bool

    @overload
    def __new__(
        cls,
        name: str,
        /,
        *,
        namespace: str | None = None,
    ) -> ContextVar[T]: ...

    @overload
    def __new__(
        cls,
        name: str,
        /,
        *,
        namespace: str | None = None,
        default: T,
    ) -> ContextVar[T]: ...

    def __new__(
        cls,
        name: str,
        /,
        *,
        namespace: str | None = None,
        default: Any = Undefined,
    ) -> ContextVar[T]:
        # Fields are initialized here under var_registry_lock so the
        # registry insert and the instance's observable state become
        # visible as one atomic step.
        if namespace is None:
            namespace = _infer_namespace()
        key = f"{namespace}:{name}"
        with base.var_registry_lock:
            existing = base.var_registry.get(key)
            if existing is not None:
                if existing._stub:
                    existing._default = default
                    existing._stub = False
                    _release_stub(key)
                    return existing
                raise ContextVarCollision(
                    f"wool.ContextVar {key!r} is already registered "
                    f"({existing!r}). Keys must be unique within a "
                    f"namespace."
                )
            instance = super().__new__(cls)
            instance._name = name
            instance._namespace = namespace
            instance._key = key
            instance._default = default
            instance._stub = False
            base.var_registry[key] = instance
            return instance

    def __reduce__(self):
        """Reduce for pickling: embeds the current value from the active
        :class:`Context` so the receiver's :meth:`_reconstitute` can
        apply it on the wire.

        Thread safety: the current-value read is unlocked because
        Python dict reads are GIL-serialized, but the
        single-task-per-:class:`Context` invariant does not extend to
        pickle paths. Calling this from a thread that does not own the
        active :class:`Context` concurrently with a
        :meth:`ContextVar.set` / :meth:`ContextVar.reset` from the
        owning task may snapshot neither the pre- nor post-mutation
        state cleanly. In practice asyncio tasks do not share a
        :class:`Context` across threads, so this is only a concern
        for callers that deliberately pickle from another thread.
        """
        ctx = peek()
        current_value = ctx._data.get(self, _UNSET) if ctx is not None else _UNSET
        return (
            ContextVar._reconstitute,
            (self._key, self._default, current_value),
        )

    def __repr__(self) -> str:
        default_part = (
            f" default={self._default!r}" if self._default is not Undefined else ""
        )
        return f"<wool.ContextVar {self._key!r}{default_part} at 0x{id(self):x}>"

    @property
    def name(self) -> str:
        """The variable's name, matching the :class:`contextvars.ContextVar` API."""
        return self._name

    @property
    def namespace(self) -> str:
        """The namespace this var belongs to."""
        return self._namespace

    @property
    def key(self) -> str:
        """The full ``"namespace:name"`` key that identifies this var on the wire."""
        return self._key

    @overload
    def get(self) -> T: ...

    @overload
    def get(self, default: T, /) -> T: ...

    def get(self, *args: T) -> T:
        """Return the current value in the active Context.

        :param default:
            Optional fallback returned when the variable has no value
            and no constructor default.
        :returns:
            The current value, the supplied fallback, or the
            constructor default.
        :raises LookupError:
            If the variable has no value, no fallback, and no default.
        """
        ctx = current_context()
        try:
            return ctx._data[self]
        except KeyError:
            if args:
                return args[0]
            if self._default is not Undefined:
                return self._default
            raise LookupError(self)

    def set(self, value: T) -> Token[T]:
        """Set the variable's value in the active Context.

        :param value:
            The new value.
        :returns:
            A :class:`Token` usable with :meth:`reset` to restore
            the previous value.
        """
        ctx = current_context()
        old_value = ctx._data.get(self, _UNSET)
        ctx._data[self] = value
        return Token(self, old_value, ctx)

    def reset(self, token: Token[T]) -> None:
        """Restore the variable to the value it had before *token*.

        Matches :meth:`contextvars.ContextVar.reset` semantics,
        scoped to the :class:`wool.Context`: the token must have
        been created in the same :class:`wool.Context` as the one
        currently active.

        In-process, the :class:`Context` is checked by object
        identity. Cross-process (unpickled tokens), the
        :class:`Context` UUID is compared as a fallback.

        :param token:
            A token previously returned by :meth:`set`.
        :raises RuntimeError:
            If the token has already been used.
        :raises ValueError:
            If the token was created by a different
            :class:`ContextVar` or in a different
            :class:`wool.Context`.
        """
        if token._var is not self:
            raise ValueError("Token was created by a different ContextVar")
        ctx = current_context()
        # These two branches are exhaustive: ``_context`` is populated
        # for tokens minted in this process (by :meth:`__init__`) and
        # for tokens whose pickle round-trip reused the live instance
        # via :data:`base.token_registry` in :meth:`_reconstitute`.
        # ``_context is None`` therefore implies a cross-process
        # reconstitution that found no matching live token, in which
        # case we fall back to UUID equality.
        if token._context is not None:
            if token._context is not ctx:
                raise ValueError("Token was created in a different wool.Context")
        elif token._context_id != ctx._id:
            raise ValueError("Token was created in a different wool.Context")
        if token._used:
            raise RuntimeError("Token has already been used")
        token._used = True
        ctx._used_token_ids.add(token._id)
        if isinstance(token._old_value, _UnsetType):
            ctx._data.pop(self, None)
        else:
            ctx._data[self] = token._old_value

    @classmethod
    def _reconstitute(
        cls,
        key: str,
        default: Any,
        current_value: Any,
    ) -> ContextVar[Any]:
        """Rebuild or resolve a :class:`ContextVar` from externally-
        supplied parts.

        Routes through :func:`resolve_stub` for the lookup-or-stub
        path so the wire-snapshot ingress (via
        :meth:`Context.from_protobuf`) and the pickle-embedded ingress
        (this method) converge on a single creation site. The
        pickle-embedded path additionally seeds the stub's default
        (which may be :data:`Undefined`) and applies *current_value*
        to the receiver's :class:`Context` so the sender's binding is
        observable on the receiver.
        """
        ctx = current_context()
        existing = resolve_stub(key, ctx, default=default)
        if current_value is not _UNSET:
            ctx._data[existing] = current_value
        return existing
