#!/usr/bin/env bash
set -Eeuo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required. Run scripts/bootstrap.sh first." >&2
  exit 1
fi

uv lock --check
uv run --locked pre-commit run --all-files --show-diff-on-failure
uv run --locked mypy src/reddit_minerals
uv run --locked pytest
uv run --locked pip-audit --progress-spinner=off
uv build --no-build-isolation --out-dir dist
uv run --locked python scripts/check_artifacts.py
bash scripts/smoke.sh
