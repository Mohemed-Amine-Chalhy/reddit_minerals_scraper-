"""Run pytest with an isolated workspace-local temporary directory.

Using a unique path avoids stale Windows ACLs and concurrent check processes
without falling back to a shared operating-system temporary directory.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from contextlib import suppress
from pathlib import Path


def main() -> int:
    repository_root = Path(__file__).resolve().parents[1]
    base_temporary = Path(tempfile.mkdtemp(prefix=".pytest-run-", dir=repository_root)).resolve()
    command = [
        sys.executable,
        "-m",
        "pytest",
        f"--basetemp={base_temporary}",
        "-p",
        "no:cacheprovider",
        *sys.argv[1:],
    ]
    try:
        return subprocess.run(command, cwd=repository_root, check=False).returncode
    finally:
        # A failed test can leave a Windows process holding a file. The unique,
        # ignored directory cannot affect a later run.
        with suppress(OSError):
            shutil.rmtree(base_temporary)


if __name__ == "__main__":
    raise SystemExit(main())
