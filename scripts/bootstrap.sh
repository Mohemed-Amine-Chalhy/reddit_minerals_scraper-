#!/usr/bin/env bash
set -Eeuo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"
export UV_CACHE_DIR="$repository_root/.uv-cache"
export PRE_COMMIT_HOME="$repository_root/.cache/pre-commit"

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required. Install it from https://docs.astral.sh/uv/getting-started/installation/ and rerun this script." >&2
  exit 1
fi

has_web_workspace=false
if [[ -f web/package.json ]]; then
  has_web_workspace=true
fi

assert_web_toolchain() {
  local command node_version node_major node_minor expected_pnpm actual_pnpm
  for command in node pnpm; do
    if ! command -v "$command" >/dev/null 2>&1; then
      echo "$command is required because web/package.json is present. See docs/web-app.md." >&2
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
    echo "pnpm $expected_pnpm is required by web/package.json; found $actual_pnpm. Activate the pinned version and rerun this script." >&2
    exit 1
  fi
}

uv python install 3.12
if "$has_web_workspace"; then
  assert_web_toolchain
fi
uv sync --locked
uv run --locked --no-build-isolation python scripts/validate_env_example.py
uv run --locked --no-build-isolation pre-commit install --install-hooks --hook-type pre-commit --hook-type pre-push

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "Created .env from the safe template. Replace its credential placeholders before live commands."
fi

bash scripts/smoke.sh

if "$has_web_workspace"; then
  uv sync --locked --extra web
  pnpm --dir web install --frozen-lockfile
  uv run --locked --extra web --no-build-isolation python -c \
    "from reddit_minerals.web import create_app; assert create_app().title == 'MineralLens API'"
fi
