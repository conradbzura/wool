"""Splice the Rust tonic client into wool's caller at WorkerConnection.dispatch,
for the coroutine case (streaming falls back to the Python path). Keeps the
load-balancer's worker selection: self._target is the chosen worker address.
Prototype simplifications: no chain-manifest context, no dispatch-phase timeout /
semaphore / transient-retry (happy path only).
"""

import os
import sys
from inspect import iscoroutinefunction

sys.path.insert(0, "wool/src")

import cloudpickle  # noqa: E402
import wool_client_rs  # noqa: E402

from wool import protocol  # noqa: E402
from wool.runtime.worker.connection import WorkerConnection  # noqa: E402

_orig_dispatch = WorkerConnection.dispatch


# WorkerConnection.dispatch is a COROUTINE that primes the handshake then returns
# a (primed) async generator. Match that: await the Rust dispatch, then return a
# 1-item async generator over the result.
async def _rust_dispatch(self, task, *, timeout=None):
    if iscoroutinefunction(task.callable) and os.environ.get("WOOL_RUST_CALLER") == "1":
        kind, payload = await wool_client_rs.dispatch(
            self._target, task.to_protobuf().SerializeToString()
        )

        # The dispatch stream yields the already-decoded value (not a raw
        # protocol.Message); _stream_to_coroutine returns anext(stream).
        async def _gen():
            if kind == 0:
                yield cloudpickle.loads(payload)
            else:
                raise cloudpickle.loads(payload)

        return _gen()
    return await _orig_dispatch(self, task, timeout=timeout)


def apply():
    WorkerConnection.dispatch = _rust_dispatch


apply()
