from __future__ import annotations

import asyncio
import threading
import weakref
from typing import TYPE_CHECKING
from typing import Any
from typing import Final

from wool.runtime.typing import Undefined
from wool.runtime.typing import UndefinedType

if TYPE_CHECKING:
    from wool.runtime.context.base import Context
    from wool.runtime.context.var import ContextVar


lock: Final[threading.Lock] = threading.Lock()


var_registry: Final[weakref.WeakValueDictionary[tuple[str, str], ContextVar[Any]]] = (
    weakref.WeakValueDictionary()
)


class _ContextToken:
    """Restore cookie returned by :meth:`_ContextRegistry.set` and
    consumed by :meth:`_ContextRegistry.reset` to undo a
    :class:`Context` installation.

    Mirrors :class:`contextvars.Token` in spirit: opaque to the
    caller, carrying enough state to return the registry slot to
    exactly where it was (including the "was-unset" case, which
    :meth:`_ContextRegistry.reset` pops rather than rewriting to
    ``None``).
    """

    __slots__ = ("_key", "_previous", "_used")

    _key: asyncio.Task[Any] | threading.Thread
    _previous: Context | None
    _used: bool

    def __init__(
        self,
        key: asyncio.Task[Any] | threading.Thread,
        previous: Context | None,
    ) -> None:
        self._key = key
        self._previous = previous
        self._used = False


class _ContextRegistry(
    weakref.WeakKeyDictionary[asyncio.Task[Any] | threading.Thread, "Context"]
):
    """WeakKey registry of per-scope :class:`Context` bindings.

    Implements the standard :class:`MutableMapping` protocol —
    ``__getitem__`` raises :class:`KeyError` on miss (no auto-create).
    Overrides :meth:`get` and :meth:`setdefault` to default the *key*
    argument to :func:`scope_key` when omitted (or passed as
    :data:`Undefined`), so callers asking about "the current scope"
    do not have to spell out the lookup. Adds two wool-specific
    methods: :meth:`set` returns a :class:`_ContextToken` that
    captures the previous slot state for later :meth:`reset`
    restoration.

    The lock serializing concurrent access lives at the public
    :func:`~wool.runtime.context.base.current_context` boundary and
    on :meth:`set`/:meth:`reset`; callers reaching into the registry
    via the mapping methods are responsible for their own ordering.
    """

    def get(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        key: asyncio.Task[Any] | threading.Thread | UndefinedType = Undefined,
        default: Context | None = None,
    ) -> Context | None:
        """Return the :class:`Context` bound to *key*, or *default* if
        *key* is not bound. When *key* is :data:`Undefined` (the
        ``default`` for the parameter), :func:`scope_key` is used.
        """
        if key is Undefined:
            key = scope_key()
        return super().get(key, default)

    def setdefault(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        key: asyncio.Task[Any] | threading.Thread | UndefinedType = Undefined,
        default: Context | None = None,
    ) -> Context | None:
        """Return the :class:`Context` bound to *key*; if absent, bind
        *default* and return it. When *key* is :data:`Undefined` (the
        ``default`` for the parameter), :func:`scope_key` is used.
        """
        if key is Undefined:
            key = scope_key()
        return super().setdefault(key, default)  # pyright: ignore[reportArgumentType]

    def set(self, ctx: Context) -> _ContextToken:
        """Bind *ctx* to the current scope's slot and return a
        single-use :class:`_ContextToken` for restoring the prior
        binding.

        The token captures the previous slot state so a subsequent
        :meth:`reset` call can either pop the slot (when it was
        unset before this call) or reinstall the prior
        :class:`Context`.
        """
        with lock:
            previous = self.get(key := scope_key())
            self[key] = ctx
        return _ContextToken(key, previous)

    def reset(self, token: _ContextToken) -> None:
        """Undo the :meth:`set` that produced *token*.

        If the slot was unset at :meth:`set` time, the binding is
        popped. Otherwise the previous :class:`Context` is reinstalled.
        Tokens are single-use; a second call against the same token
        raises :class:`RuntimeError`.
        """
        if token._used:
            raise RuntimeError("token already consumed by reset")
        token._used = True
        with lock:
            if token._previous is None:
                self.pop(token._key, None)
            else:
                self[token._key] = token._previous


context_registry: Final[_ContextRegistry] = _ContextRegistry()


def scope_key() -> asyncio.Task[Any] | threading.Thread:
    """Identify the current execution scope (asyncio task, or
    thread for sync callers).
    """
    try:
        return asyncio.current_task() or threading.current_thread()
    except RuntimeError:
        return threading.current_thread()
