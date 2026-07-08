"""#273 pickle-memoize as a monkeypatch (env-gated WOOL_MEMOIZE_PICKLE=1): memoize
dumps() of the stable callable + proxy by object identity, so only args/kwargs
re-serialize per dispatch. Runs on the caller in Task.to_protobuf — stacks with the
Rust caller (the memoized bytes are what get handed to the Rust client). Keeps the
wool clone pristine.
"""

import os
import sys
import weakref

sys.path.insert(0, "wool/src")

import wool  # noqa: E402
from wool import protocol  # noqa: E402
from wool.runtime.routine.task import Task  # noqa: E402

_pickle_cache: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()


def _dumps_memoized(obj, dumps):
    try:
        cached = _pickle_cache.get(obj)
        if cached is not None:
            return cached
    except TypeError:
        return dumps(obj)
    data = dumps(obj)
    try:
        _pickle_cache[obj] = data
    except TypeError:
        pass
    return data


def _memoized_to_protobuf(self) -> protocol.Task:
    dumps = wool.__serializer__.dumps
    if os.environ.get("WOOL_MEMOIZE_PICKLE") == "1":
        callable_bytes = _dumps_memoized(self.callable, dumps)
        proxy_bytes = _dumps_memoized(self.proxy, dumps)
    else:
        callable_bytes = dumps(self.callable)
        proxy_bytes = dumps(self.proxy)
    return protocol.Task(
        version=protocol.__version__,
        id=str(self.id),
        callable=callable_bytes,
        args=dumps(self.args),
        kwargs=dumps(self.kwargs),
        caller=str(self.caller) if self.caller else "",
        proxy=proxy_bytes,
        proxy_id=str(self.proxy.id),
        timeout=int(self.timeout) if self.timeout else 0,
        tag=self.tag if self.tag else "",
        runtime_context=(
            self.runtime_context.to_protobuf() if self.runtime_context else None
        ),
    )


Task.to_protobuf = _memoized_to_protobuf
