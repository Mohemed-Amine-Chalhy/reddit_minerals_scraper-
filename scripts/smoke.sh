#!/usr/bin/env bash
set -Eeuo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"
export UV_CACHE_DIR="$repository_root/.uv-cache"

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required. Run scripts/bootstrap.sh first." >&2
  exit 1
fi

uv run --locked --no-build-isolation python -c "import reddit_minerals; import reddit_minerals.cli"
uv run --locked --no-build-isolation reddit-minerals --help
