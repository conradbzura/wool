from contextlib import contextmanager
from typing import Iterator
from uuid import UUID

from wool.runtime.context import Context
from wool.runtime.context import attach
from wool.runtime.context import detach


@contextmanager
def scoped_context(id: UUID | None = None) -> Iterator[Context]:
    """Test helper — install a wool.Context for the duration of the block.

    Mints a fresh chain id by default. Pass *id* to construct a
    Context with a specific chain id, used by tests that exercise
    chain-id-dependent semantics (e.g. ContextVar.reset's same-id
    fallback). On exit the prior scope's Context is restored.
    """
    ctx = Context() if id is None else Context._reconstitute(id, {})
    token = attach(ctx)
    try:
        yield ctx
    finally:
        detach(token)
