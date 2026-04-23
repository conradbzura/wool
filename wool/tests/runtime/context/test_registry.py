import asyncio

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
async def test_context_registry_get_when_scope_has_no_bound_ctx():
    """Test context_registry.get returns None on a scope without a
    registered Context and does not lazy-register one as a side effect.

    Given:
        An asyncio task whose scope has no Context registered (a
        fresh asyncio.create_task child built without wool's task
        factory installed).
    When:
        context_registry.get() is called from inside that task, then
        a second time.
    Then:
        Both calls return None — the non-creating accessor is
        observable while the auto-creating one
        (``context_registry.setdefault(default=Context())``, wrapped by
        ``current_context()``) materializes a Context on first access.
    """
    # Arrange
    from wool.runtime.context import context_registry

    observed: list[Context | None] = []

    async def body():
        observed.append(context_registry.get())
        observed.append(context_registry.get())

    # Act
    await asyncio.create_task(body())

    # Assert
    assert observed == [None, None]
