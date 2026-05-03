from __future__ import annotations

import io
import weakref
from typing import Any
from typing import Protocol
from typing import runtime_checkable
from uuid import UUID
from uuid import uuid4

import cloudpickle


# public
@runtime_checkable
class Serializer(Protocol):
    """Protocol for pluggable serialization of dispatch payloads.

    Implementations must be hashable.  Wool caches the pickled form of each
    serializer instance via :func:`functools.lru_cache`.  A class that
    overrides ``__eq__`` without supplying a compatible ``__hash__``
    (which Python silently sets to ``None``) fails ``isinstance(obj,
    Serializer)`` because the runtime-checkable protocol consults
    ``hasattr``, and ``hasattr(obj, "__hash__")`` returns ``False`` when
    ``__hash__`` is ``None``.  An instance that bypasses both static
    type checking and the ``isinstance`` gate would trip the cache with
    a :exc:`TypeError` on first dispatch.
    """

    def __hash__(self) -> int: ...

    def dumps(self, obj: Any) -> bytes:
        """Serialize *obj* to bytes for transport across a worker boundary.

        :param obj:
            The object to serialize.  Implementations decide which payloads
            are supported.
        :returns:
            The serialized representation of *obj*.
        """
        ...

    def loads(self, data: bytes) -> Any:
        """Deserialize *data* produced by a matching :meth:`dumps` call.

        :param data:
            Bytes previously produced by :meth:`dumps`.
        :returns:
            The reconstructed object.
        """
        ...


class _WoolPickler(cloudpickle.Pickler):
    """Cloudpickle-based pickler that honors the ``__wool_reduce__`` protocol.

    For objects whose type defines ``__wool_reduce__``, the method's return
    value (a standard ``(callable, args)`` reduce tuple) is used in place of
    the standard reduction protocol.  All other objects are handled exactly
    as :class:`cloudpickle.Pickler` would handle them.

    The override fires before ``__reduce_ex__``, so a type that pairs
    ``__wool_reduce__`` with a ``__reduce_ex__`` guard (raising
    :exc:`TypeError`) can be guarded against vanilla pickling while still
    serializing correctly through this pickler.
    """

    def reducer_override(self, obj: Any) -> Any:
        if hasattr(type(obj), "__wool_reduce__"):
            return obj.__wool_reduce__()
        return super().reducer_override(obj)


class CloudpickleSerializer:
    """Default :class:`Serializer` implementation.

    Used by :class:`~wool.runtime.routine.task.Task.to_protobuf` when the
    caller does not provide an explicit serializer.  Semantically the
    contract is cloudpickle's: callables and arguments must be
    cloudpickle-picklable.  The fact that ``dumps`` routes through Wool's
    internal pickler — and therefore respects the ``__wool_reduce__``
    protocol on guarded Wool types — is an implementation detail.
    """

    def __hash__(self) -> int:
        """Return a constant hash so all instances share an LRU-cache slot."""
        return hash(CloudpickleSerializer)

    def __eq__(self, other: object) -> bool:
        """Return True for any other CloudpickleSerializer; instances are interchangeable."""
        return isinstance(other, CloudpickleSerializer)

    def dumps(self, obj: Any) -> bytes:
        """Serialize *obj* via Wool's internal pickler.

        Honors the ``__wool_reduce__`` protocol on guarded Wool types and
        otherwise reduces via cloudpickle.
        """
        buffer = io.BytesIO()
        _WoolPickler(buffer).dump(obj)
        return buffer.getvalue()

    def loads(self, data: bytes) -> Any:
        """Deserialize *data* via :func:`cloudpickle.loads`."""
        return cloudpickle.loads(data)


class _PassthroughKey:
    """Weak-referenceable key for the passthrough store."""

    __slots__ = ("__weakref__", "token")

    def __init__(self, token: UUID | None = None) -> None:
        self.token = token if token is not None else uuid4()

    def __hash__(self) -> int:
        return hash(self.token)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _PassthroughKey):
            return super().__eq__(other)
        return self.token == other.token


# Weak keys let the owning PassthroughSerializer's `_keys` list act as the
# lifetime anchor: when the serializer goes out of scope, its keys are
# garbage-collected and the corresponding entries here are removed
# automatically without any explicit cleanup.
_passthrough_store: weakref.WeakKeyDictionary[_PassthroughKey, Any] = (
    weakref.WeakKeyDictionary()
)


class PassthroughSerializer:
    """In-process serializer that avoids pickling entirely.

    Each instance acts as a scope guard for one dispatch.
    ``dumps`` creates a weakly-referenceable key, stores the object
    in a module-level :class:`~weakref.WeakKeyDictionary`, and
    retains a strong reference to the key on ``self``.  When the
    serializer goes out of scope the keys are garbage-collected and
    the weak-dict entries are removed automatically.

    ``loads`` is static — it reconstructs the key from the bytes
    token in the protobuf message and pops the entry from the store.

    All instances hash and compare equal so the LRU cache that pickles
    serializer instances for transport hits on every call.
    """

    def __init__(self) -> None:
        """Initialize an empty list of strong key references for this dispatch scope."""
        self._keys: list[_PassthroughKey] = []

    def __hash__(self) -> int:
        """Return a constant hash so all instances share an LRU-cache slot."""
        return hash(PassthroughSerializer)

    def __eq__(self, other: object) -> bool:
        """Return True for any other PassthroughSerializer; instances are interchangeable."""
        return isinstance(other, PassthroughSerializer)

    def __reduce__(self) -> tuple[type[PassthroughSerializer], tuple[()]]:
        """Reduce to a fresh instance; the per-scope key list does not survive transport."""
        return (PassthroughSerializer, ())

    def dumps(self, obj: Any) -> bytes:
        """Stash *obj* in the module store and return its weak-key token."""
        key = _PassthroughKey()
        self._keys.append(key)
        _passthrough_store[key] = obj
        return key.token.bytes

    @staticmethod
    def loads(data: bytes) -> Any:
        """Pop and return the object stashed under the token in *data*."""
        key = _PassthroughKey(UUID(bytes=data))
        return _passthrough_store.pop(key)
