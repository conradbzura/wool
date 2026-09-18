"""The discovery subsystem's exceptions.

Single home for the typed errors the discovery backends raise.

.. rubric:: Implementation notes

Each exception passes its fields positionally to ``super().__init__``,
so the fallback in the worker's exception serializer, which rebuilds an
exception as ``cls(*exc.args)``, restores them (see
`wool.runtime.worker.frame`). A keyword-only field kept out of ``args``
fails that rebuild, so every field is positional and required.
"""

from __future__ import annotations

from uuid import UUID

from wool.exceptions import WoolError


# public
class DiscoveryCapacityExhausted(WoolError):
    """Raised when a registration would exceed the registry's capacity.

    A namespace's registry holds a fixed number of worker slots, set by
    its owner (`LocalDiscovery`'s ``capacity``). Once that many workers
    are registered, publishing another raises this.

    The condition is transient and namespace-wide. Dropping a worker
    frees a slot, so a retry can succeed. The ceiling itself does not
    grow.

    :param capacity:
        The number of worker slots the namespace's owner stamped into
        the registry.
    """

    def __init__(self, capacity: int):
        self.capacity = capacity
        super().__init__(capacity)

    def __str__(self) -> str:
        return f"No available slots in discovery registry (capacity {self.capacity})"


# public
class DiscoveryBlockExhausted(WoolError):
    """Raised when serialized worker metadata exceeds its block.

    A worker's metadata lives in a fixed-size block file created
    at its first registration (`LocalDiscovery`'s ``block_size``). A
    publish whose serialized metadata does not fit that block raises this,
    leaving the prior registration intact.

    The condition is permanent and per-worker: a retry with the same
    metadata fails. Shrink the metadata. A block's size is fixed at the
    worker's first registration and a re-registration writes into that
    same block, so publishing again through a publisher configured with
    a larger ``block_size`` does not enlarge it; see
    `LocalDiscovery.Publisher.publish`.

    :param size:
        The attempted payload size in bytes.
    """

    def __init__(self, size: int):
        self.size = size
        super().__init__(size)

    def __str__(self) -> str:
        return f"Worker metadata exceeds its registered block ({self.size} bytes)"


# public
class DiscoveryWorkerNotFound(WoolError):
    """Raised when an update targets a worker that is not registered.

    ``worker-updated`` requires an existing registration.
    ``worker-added`` registers a new worker or refreshes an existing one.

    :param uid:
        The unmatched worker's UID.
    """

    def __init__(self, uid: UUID):
        self.uid = uid
        super().__init__(uid)

    def __str__(self) -> str:
        return f"Worker {self.uid} not found in discovery registry"


# public
class DiscoveryNamespaceInUse(WoolError):
    """Raised when claiming a namespace another process still holds.

    A namespace is in use while any process holding its claim lives. That
    is the owner's process, and anything forked from it after entry; see
    `LocalDiscovery` for the ownership contract and what ends a claim.

    :param namespace:
        The namespace whose claim was rejected.
    """

    def __init__(self, namespace: str):
        self.namespace = namespace
        super().__init__(namespace)

    def __str__(self) -> str:
        return f"Discovery namespace {self.namespace!r} is already in use"


# public
class DiscoveryNamespaceNotFound(WoolError):
    """Raised when a namespace has no live owner to borrow from.

    Raised at a bind where no owner has created the namespace yet, and
    again at any later operation by a borrower whose owner has since
    gone: a binding ends with the owner it was made against, so a
    publisher's next publish and a subscriber's next scan both fail
    rather than reaching a successor or serving what they last read.
    This holds whether that owner exited or was killed outright. A
    borrower that wants to follow the namespace re-binds after this
    error. See `LocalDiscovery` for the ownership contract.

    :param namespace:
        The namespace that has no live owner.
    """

    def __init__(self, namespace: str):
        self.namespace = namespace
        super().__init__(namespace)

    def __str__(self) -> str:
        return f"Discovery namespace {self.namespace!r} has no live owner to borrow from"
