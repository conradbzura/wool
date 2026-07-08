"""RustWorker: a wool `Worker` subclass whose worker plane is the Rust tonic
server + Python executor (instead of the in-process Python WorkerService). Spawns
the two processes, waits for the gRPC port, and reports a WorkerMetadata so
`WorkerPool(spawn=N, worker=RustWorker)` routes to it like any worker. This lets
wool's REAL caller drive the Rust worker through the shapebench suite.
"""

import os
import socket
import subprocess
import sys
import time
import uuid
from types import MappingProxyType

sys.path.insert(0, "wool/src")

import wool  # noqa: E402,F401
from wool import protocol  # noqa: E402
from wool.runtime.worker.base import Worker  # noqa: E402
from wool.runtime.worker.base import WorkerOptions  # noqa: E402
from wool.runtime.worker.metadata import WorkerMetadata  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
RUST_BIN = os.path.join(HERE, "rust_worker", "target", "release", "rust_worker")
EXECUTOR_PY = os.path.join(HERE, "rust_worker", "executor.py")
PROJECT_ROOT = "/Users/conrad/Projects/wool-labs/wool-staging/.sandbox/wool-benchmark"


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _wait_port(host: str, port: int, timeout: float) -> bool:
    deadline = time.time() + (timeout or 15.0)
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.05)
    return False


# public
class RustWorker(Worker):
    def __init__(self, *tags: str, host: str = "127.0.0.1", credentials=None, **extra):
        super().__init__(*tags, **extra)
        self._host = host
        self._credentials = credentials  # ignored — insecure prototype
        self._port: int | None = None
        self._sock_path: str | None = None
        self._proc_exec: subprocess.Popen | None = None
        self._proc_rust: subprocess.Popen | None = None

    @property
    def address(self) -> str | None:
        return f"{self._host}:{self._port}" if self._port else None

    async def _start(self, timeout: float | None):
        port = _free_port()
        sock_path = f"/tmp/wool_exec_{uuid.uuid4().hex}.sock"
        env = {
            **os.environ,
            "WOOL_EXEC_SOCK": sock_path,
            "WOOL_RUST_PORT": str(port),
            "GRPC_VERBOSITY": "NONE",
        }
        # Executor first (creates the socket); the Rust worker retries connecting.
        elog = open(f"/tmp/wool_exec_{port}.log", "w")
        rlog = open(f"/tmp/wool_rust_{port}.log", "w")
        self._proc_exec = subprocess.Popen(
            [sys.executable, EXECUTOR_PY],
            cwd=PROJECT_ROOT,
            env=env,
            stdout=elog,
            stderr=elog,
        )
        self._proc_rust = subprocess.Popen(
            [RUST_BIN],
            cwd=PROJECT_ROOT,
            env=env,
            stdout=rlog,
            stderr=rlog,
        )
        if not _wait_port(self._host, port, timeout or 15.0):
            self._teardown()
            raise RuntimeError(f"Rust worker did not come up on {self._host}:{port}")
        self._port = port
        self._sock_path = sock_path
        self._info = WorkerMetadata(
            uid=self._uid,
            address=self.address,
            pid=self._proc_rust.pid,
            version=protocol.__version__,
            tags=frozenset(self._tags),
            extra=MappingProxyType(self._extra),
            secure=False,
            options=WorkerOptions().channel,
        )

    async def _stop(self, timeout: float | None):
        self._teardown()

    def _teardown(self):
        for p in (self._proc_rust, self._proc_exec):
            if p is not None and p.poll() is None:
                p.terminate()
        for p in (self._proc_rust, self._proc_exec):
            if p is not None:
                try:
                    p.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    p.kill()
        if self._sock_path and os.path.exists(self._sock_path):
            try:
                os.unlink(self._sock_path)
            except OSError:
                pass


# Shapebench adapter: same routines as WoolAdapter, but the pool spawns RustWorkers.
try:
    from shapebench.frameworks.wool import WoolAdapter

    class RustWoolAdapter(WoolAdapter):
        name = "rust-wool"

        async def setup(self, workers: int) -> None:
            self._workers = workers
            self._pool = wool.WorkerPool(spawn=workers, worker=RustWorker)
            await self._pool.__aenter__()

        def version(self) -> str:
            return f"{wool.__version__}+rust"
except ImportError:
    pass
