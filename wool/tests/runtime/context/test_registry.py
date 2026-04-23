import asyncio
import contextvars

import pytest

from wool.runtime.context import Context
from wool.runtime.context import ContextVar
from wool.runtime.context import current_context


def test_current_context_with_set_vars():
    """Test current_context() returns the live Context for the scope.

    Given:
        A ContextVar with an explicit value set
    When:
        current_context() is called twice from the same scope
    Then:
        The returned Context contains the var with its value and
        both calls yield the same Context instance (idempotent
        within a scope)
    """
    # Arrange
    var = ContextVar("cur_ctx", default=0)
    var.set(1)

    # Act
    ctx = current_context()

    # Assert
    assert ctx[var] == 1
    assert current_context() is ctx


@pytest.mark.asyncio
async def test_anchor_get_when_scope_has_no_bound_ctx():
    """Test the wool anchor returns None on a scope without a bound
    Context and is not lazy-stamped as a side effect of a non-
    creating accessor.

    Given:
        An asyncio task whose stdlib Context starts empty of any
        wool anchor — third-party-style entry where a fresh
        ``contextvars.Context()`` was supplied to
        ``asyncio.create_task``, bypassing wool's task factory
        wrapping for the lifetime of that stdlib Context.
    When:
        The wool anchor is read directly from inside that task,
        twice.
    Then:
        Both reads observe ``None`` — the non-creating accessor
        does not materialize a Context, while
        :func:`current_context` does (covered separately by
        :func:`test_current_context_with_set_vars`).
    """
    # Arrange
    from wool.runtime.context.registry import _anchor

    observed: list[Context | None] = []

    async def body():
        observed.append(_anchor.get())
        observed.append(_anchor.get())

    # Act — run under a fresh stdlib Context so no parent anchor
    # leaks in. Using the asyncio default factory (no wool wrapping)
    # plus an explicit fresh stdlib context avoids both the wool
    # task-factory wrapper and any inherited anchor.
    await asyncio.get_running_loop().create_task(body(), context=contextvars.Context())

    # Assert
    assert observed == [None, None]
