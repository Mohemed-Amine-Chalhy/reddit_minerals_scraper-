#!/usr/bin/env bash
set -Eeuo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"
export UV_CACHE_DIR="$repository_root/.uv-cache"

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required. Run scripts/bootstrap.sh first." >&2
  exit 1
fi

uv_run_arguments=(run --locked --no-build-isolation)
if [[ -f web/package.json ]]; then
  uv_run_arguments+=(--extra web)
fi

uv "${uv_run_arguments[@]}" ruff check --fix .
uv "${uv_run_arguments[@]}" ruff format .

if [[ -f web/package.json ]]; then
  for command in node pnpm; do
    if ! command -v "$command" >/dev/null 2>&1; then
      echo "$command is required to format the web workspace. Run scripts/bootstrap.sh first." >&2
      exit 1
    fi
  done
  if [[ ! -d web/node_modules ]]; then
    echo "Web dependencies are not installed. Run scripts/bootstrap.sh first." >&2
    exit 1
  fi

  node_version="$(node -p 'process.versions.node')"
  IFS=. read -r node_major node_minor _ <<<"$node_version"
  if (( node_major < 22 || node_major >= 27 || (node_major == 22 && node_minor < 12) )); then
    echo "Node.js >=22.12 and <27 is required; found $node_version. See .node-version." >&2
    exit 1
  fi
  expected_pnpm="$(node -p "require('./web/package.json').packageManager.replace(/^pnpm@/, '')")"
  actual_pnpm="$(pnpm --version)"
  if [[ "$actual_pnpm" != "$expected_pnpm" ]]; then
    echo "pnpm $expected_pnpm is required by web/package.json; found $actual_pnpm. Run scripts/bootstrap.sh after activating the pinned version." >&2
    exit 1
  fi
  if [[ ! -f web/node_modules/.pnpm/lock.yaml ]] || \
    ! cmp -s web/pnpm-lock.yaml web/node_modules/.pnpm/lock.yaml; then
    echo "Web dependencies do not match web/pnpm-lock.yaml. Run scripts/bootstrap.sh first." >&2
    exit 1
  fi
  pnpm --dir web run format
fi
