from __future__ import annotations

import contextvars
import threading
import weakref
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Final
from typing import NoReturn
from typing import SupportsIndex
from uuid import UUID

if TYPE_CHECKING:
    from wool.runtime.context.base import Context
    from wool.runtime.context.token import Token
    from wool.runtime.context.var import ContextVar


lock: Final[threading.Lock] = threading.Lock()


var_registry: Final[weakref.WeakValueDictionary[tuple[str, str], ContextVar[Any]]] = (
    weakref.WeakValueDictionary()
)


# Active wool.Context for the current execution scope, keyed off the
# stdlib's own ContextVar machinery so wool inherits stdlib's
# copy-on-fork / fresh-Context semantics for free. ``None`` (the
# default) indicates "no Context bound to this scope yet"; a lazy
# create happens on first read inside
# :func:`wool.runtime.context.base.current_context`.
_anchor: Final[contextvars.ContextVar["Context | None"]] = contextvars.ContextVar(
    "_wool_context_anchor", default=None
)


class _TokenRegistry(weakref.WeakValueDictionary[UUID, "Token[Any]"]):
    """Process-wide registry of live :class:`Token` instances that
    pickles by module-attribute reference under Wool's pickler.

    Plain :class:`weakref.WeakValueDictionary` is unpicklable
    (weakrefs reject pickle), and cloudpickle serializes bound
    classmethods like :meth:`Token._reconstitute` by walking the
    function's globals and capturing each name by value. Without
    this override the by-value walk crashes on the registry; with
    it, the registry reduces to a zero-arg lookup of this module's
    :data:`token_registry` attribute, keeping the actual contents
    process-local.

    The reduction is exposed only to Wool's pickler via
    :meth:`__wool_reduce__`; vanilla :func:`pickle.dumps` and
    :func:`cloudpickle.dumps` are rejected by :meth:`__reduce_ex__`
    so the registry never silently leaves the dispatch path.
    """

    def __wool_reduce__(self) -> tuple[Callable[..., _TokenRegistry], tuple[()]]:
        return (_resolve_token_registry, ())

    def __reduce_ex__(self, _protocol: SupportsIndex) -> NoReturn:
        raise TypeError(
            "_TokenRegistry cannot be pickled via vanilla pickle/cloudpickle; "
            "it is serialized automatically when dispatched through Wool's "
            "runtime."
        )


# Process-wide weak registry of live :class:`Token` instances keyed
# by ID. Populated in :meth:`Token.__init__` and
# :meth:`Token._reconstitute`. Used for (a) preserving pickle
# identity within a process and (b) resolving incoming wire IDs to
# live tokens so their ``_used`` flag can be flipped on merge. Weak
# values auto-prune when a token is GC'd, so transient tokens from a
# ``set``/``reset`` loop do not accumulate.
token_registry: Final[_TokenRegistry] = _TokenRegistry()


def _resolve_token_registry() -> _TokenRegistry:
    """Return the process-wide :class:`Token` registry.

    Module-level shim used by :meth:`_TokenRegistry.__wool_reduce__`
    so cloudpickle's lookup-and-qualname path can pickle the registry
    reference by name instead of by value. MUST stay at module level;
    cloudpickle's by-reference lookup requires a stable qualname.
    """
    return token_registry
