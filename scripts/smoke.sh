#!/usr/bin/env bash
set -Eeuo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required. Run scripts/bootstrap.sh first." >&2
  exit 1
fi

uv run --locked python -c "import reddit_minerals; import reddit_minerals.cli"
uv run --locked reddit-minerals --help
