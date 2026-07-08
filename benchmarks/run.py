"""Entry point for the shapebench PoC runner.

The ``if __name__ == "__main__"`` guard is mandatory: Wool's ``WorkerPool`` spawns
worker subprocesses with the 'spawn' start method, which re-imports this module
in each child. Without the guard, that re-import would recursively launch the
benchmark in every worker (a fork bomb).

Usage:
    python run.py --framework wool --workers 4
    python run.py --framework ray  --workers 4 --shapes s1,s2,s5,s7
"""

from shapebench.cli import main

if __name__ == "__main__":
    main()
