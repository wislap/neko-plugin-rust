from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def _platform_tag() -> str:
    if sys.platform.startswith("linux"):
        return "linux"
    if sys.platform == "darwin":
        return "macos"
    if sys.platform in ("win32", "cygwin"):
        return "windows"
    return sys.platform


def get_binary_path() -> str:
    override = os.getenv("NEKO_MESSAGE_PLANE_RUST_BIN")
    if override:
        return override

    pkg_dir = Path(__file__).resolve().parent
    tag = _platform_tag()

    candidates = []
    if tag == "windows":
        candidates.append(pkg_dir / "bin" / tag / "neko-message-plane.exe")
        candidates.append(pkg_dir / "bin" / "neko-message-plane.exe")
    else:
        candidates.append(pkg_dir / "bin" / tag / "neko-message-plane")
        candidates.append(pkg_dir / "bin" / "neko-message-plane")

    for p in candidates:
        if p.exists() and p.is_file():
            return str(p)

    tried = ", ".join(str(p) for p in candidates)
    raise FileNotFoundError(
        "neko-message-plane bundled binary not found. Tried: " + tried + ". "
        "Set NEKO_MESSAGE_PLANE_RUST_BIN to a real binary path or rebuild the wheel with "
        "python/neko_message_plane_wheel/bin/<platform>/neko-message-plane included."
    )


def run(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    bin_path = get_binary_path()
    try:
        proc = subprocess.run([bin_path, *argv], check=False)
        return int(proc.returncode)
    except FileNotFoundError as e:
        raise RuntimeError(str(e)) from e
