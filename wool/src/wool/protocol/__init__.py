import os
import sys
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version
from typing import Protocol

try:
    __version__ = version("wool")
except PackageNotFoundError:
    __version__ = "unknown"

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from . import wire as wire


class AddServicerToServerProtocol(Protocol):
    @staticmethod
    def __call__(servicer, server) -> None: ...


add_to_server: dict[type[wire.WorkerServicer], AddServicerToServerProtocol] = {
    wire.WorkerServicer: wire.add_WorkerServicer_to_server,
}
