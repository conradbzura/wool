"""A failure held for later re-raising.

Provides `FailureReport`: the record a container keeps of the one
exception it owes every caller that asks it for something the failure
made impossible.
"""

from __future__ import annotations

from types import TracebackType
from typing import NoReturn


class FailureReport:
    """One exception, raised to many callers, without accumulating.

    Wraps a failure that outlives the frame that produced it and is owed
    to every caller that later asks for what it made impossible — the
    consumers of a shared subscription, the dispatches of a proxy whose
    membership feed died. Those callers must each receive an exception
    they can match on, so the instance itself is handed to all of them
    rather than rebuilt: reconstructing it assumes a constructor that
    accepts its own ``args``, which an arbitrary exception need not, and
    wrapping it would defeat the ``except`` clause the caller wrote.

    Raising one instance repeatedly costs what the first raise does not.
    Each propagation prepends a traceback node per frame it unwinds
    through, and each node pins that frame's locals, so a held failure
    raised often enough retains a slice of every call that ever received
    it. `contextlib.AsyncExitStack` compounds it from the other side,
    writing a body's exception onto whatever a teardown callback raises.

    This holds that chaining state as it stood at the first report and
    restores it before each raise. A caller's traceback is then the
    origin plus their own call, rather than the origin plus every call
    that came before, and identity and type are untouched.

    :param failure:
        The exception to hold. Its chaining state is captured as it
        stands, so construct the report where the failure is first
        caught rather than after re-raising it.
    """

    __slots__ = ("failure", "_context", "_suppress_context", "_traceback")

    def __init__(self, failure: BaseException) -> None:
        self.failure = failure
        self._traceback: TracebackType | None = failure.__traceback__
        self._context: BaseException | None = failure.__context__
        self._suppress_context: bool = failure.__suppress_context__

    def raise_failure(self) -> NoReturn:
        """Raise the held failure with the chaining it had when first reported.

        :raises BaseException:
            The held failure, always.
        """
        failure = self.failure
        failure.__traceback__ = self._traceback
        failure.__context__ = self._context
        failure.__suppress_context__ = self._suppress_context
        raise failure
