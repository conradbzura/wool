"""Ray adapter — each shape as an idiomatic remote-task / actor construction.

Dependencies are expressed with ``ObjectRef``s (S4/S6 chain refs directly; S3's
reduce resolves a list of refs inside the task to avoid a driver round-trip).
S5 uses nested ``.remote()`` calls with ``ray.get`` inside the task — the
idiomatic recursive pattern, which holds a worker slot per internal node while it
waits (kept small here; see shapes.py). S7 uses a streaming actor generator.
Ray's API is synchronous; the blocking ``ray.get`` inside these async methods is
fine because the runner drives one shape at a time.
"""

import ray

from shapebench.workloads import spin


@ray.remote
def _leaf(g: float) -> None:
    spin(g)


@ray.remote
def _stage(x: int, g: float) -> int:
    spin(g)
    return x + 1


@ray.remote
def _reduce(refs: list, g: float) -> int:
    spin(g)
    return sum(ray.get(refs))


@ray.remote
def _tree(depth: int, branch: int, g: float) -> int:
    spin(g)
    if depth <= 0:
        return 1
    children = [_tree.remote(depth - 1, branch, g) for _ in range(branch)]
    return 1 + sum(ray.get(children))


@ray.remote
class _Streamer:
    def stream(self, n: int, g: float):
        for i in range(n):
            spin(g)
            yield i


class RayAdapter:
    name = "ray"

    def __init__(self) -> None:
        self._streamer = None

    def version(self) -> str:
        return ray.__version__

    async def setup(self, workers: int) -> None:
        if not ray.is_initialized():
            ray.init(
                num_cpus=workers,
                include_dashboard=False,
                logging_level="ERROR",
                log_to_driver=False,
            )
        self._streamer = _Streamer.remote()

    async def teardown(self) -> None:
        self._streamer = None
        if ray.is_initialized():
            ray.shutdown()

    async def reset(self) -> None:
        # Ray's global runtime has no cross-shape contamination to clear, so a
        # per-shape reset would only pay init/shutdown cost for nothing.
        return None

    async def s1_point_to_point(self, g: float) -> None:
        ray.get(_leaf.remote(g))

    async def s2_fanout(self, g: float, n: int) -> None:
        ray.get([_leaf.remote(g) for _ in range(n)])

    async def s3_scatter_gather(self, g: float, n: int) -> None:
        refs = [_stage.remote(0, g) for _ in range(n)]
        ray.get(_reduce.remote(refs, g))

    async def s4_pipeline(self, g: float, depth: int) -> None:
        ref = _stage.remote(0, g)
        for _ in range(depth - 1):
            ref = _stage.remote(ref, g)
        ray.get(ref)

    async def s5_recursive_tree(self, g: float, depth: int, branch: int) -> None:
        ray.get(_tree.remote(depth, branch, g))

    async def s6_diamond(self, g: float) -> None:
        a = _stage.remote(0, g)
        b = _stage.remote(a, g)
        c = _stage.remote(a, g)
        ray.get(_reduce.remote([b, c], g))

    async def s7_streaming(self, g: float, n: int) -> None:
        gen = self._streamer.stream.options(num_returns="streaming").remote(n, g)
        for ref in gen:
            ray.get(ref)
