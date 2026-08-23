#!/usr/bin/env bash
set -Eeuo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"
export UV_CACHE_DIR="$repository_root/.uv-cache"

if [[ ! -f web/package.json ]]; then
  echo "web/package.json is missing; this script requires the web workspace." >&2
  exit 1
fi

for command in uv node pnpm; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "$command is required for the web workspace. Run scripts/bootstrap-web.sh first." >&2
    exit 1
  fi
done

node_version="$(node -p 'process.versions.node')"
IFS=. read -r node_major node_minor _ <<<"$node_version"
if (( node_major < 22 || node_major >= 27 || (node_major == 22 && node_minor < 12) )); then
  echo "Node.js >=22.12 and <27 is required; found $node_version. See .node-version." >&2
  exit 1
fi

expected_pnpm="$(node -p "require('./web/package.json').packageManager.replace(/^pnpm@/, '')")"
actual_pnpm="$(pnpm --version)"
if [[ "$actual_pnpm" != "$expected_pnpm" ]]; then
  echo "pnpm $expected_pnpm is required by web/package.json; found $actual_pnpm. Run scripts/bootstrap-web.sh after activating the pinned version." >&2
  exit 1
fi
if [[ ! -d web/node_modules ]]; then
  echo "Web dependencies are not installed. Run scripts/bootstrap-web.sh first." >&2
  exit 1
fi
if [[ ! -f web/node_modules/.pnpm/lock.yaml ]] || \
  ! cmp -s web/pnpm-lock.yaml web/node_modules/.pnpm/lock.yaml; then
  echo "Web dependencies do not match web/pnpm-lock.yaml. Run scripts/bootstrap-web.sh first." >&2
  exit 1
fi

uv lock --check
uv run --locked --extra web --no-build-isolation mypy src/reddit_minerals
uv run --locked --extra web --no-build-isolation python scripts/run_tests.py tests/web --no-cov
pnpm --dir web run check
