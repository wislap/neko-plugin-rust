from __future__ import annotations

import sys

from .runtime import run


def main() -> None:
    raise SystemExit(run(sys.argv[1:]))
