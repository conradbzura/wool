"""Single-shot Ray probe: run a fan-out either flat or nested (blocking), print
the makespan, and exit. Invoked as a subprocess with a wall-clock timeout by
nested.py so that a nested deadlock is observed as a timeout rather than hanging
the experiment.

Nested here is the direct analog of Wool's caller-parallel fan-out: an internal
task blocks on ray.get of its children while holding a worker slot. When the
number of simultaneously-blocked internal tasks reaches the worker count, the
leaves can never schedule — deadlock.

    python ray_nested_probe.py <mode flat|nested> <n> <branch> <workers>
"""

import sys
import time

import ray

from shapebench.workloads import spin


@ray.remote
def leaf():
    spin(0.0)


@ray.remote
def subfanout(k):
    ray.get([leaf.remote() for _ in range(k)])
    return None


def split(n, parts):
    per, rem = divmod(n, parts)
    return [per + (1 if i < rem else 0) for i in range(parts)]


def main():
    mode, n, branch, workers = (
        sys.argv[1],
        int(sys.argv[2]),
        int(sys.argv[3]),
        int(sys.argv[4]),
    )
    ray.init(
        num_cpus=workers,
        include_dashboard=False,
        logging_level="ERROR",
        log_to_driver=False,
    )
    # warm
    ray.get([leaf.remote() for _ in range(8)])
    start = time.perf_counter()
    if mode == "flat":
        ray.get([leaf.remote() for _ in range(n)])
    else:
        sizes = [s for s in split(n, branch) if s > 0]
        ray.get([subfanout.remote(s) for s in sizes])
    print(f"OK {(time.perf_counter() - start) * 1e3:.1f}")


if __name__ == "__main__":
    main()
