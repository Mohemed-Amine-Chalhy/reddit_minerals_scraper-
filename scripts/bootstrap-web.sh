#!/usr/bin/env bash
set -Eeuo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export UV_CACHE_DIR="$repository_root/.uv-cache"

if [[ ! -f "$repository_root/web/package.json" ]]; then
  echo "web/package.json is missing; this script requires the web workspace." >&2
  exit 1
fi

for command in uv node pnpm; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "$command is required for the web workspace. See docs/web-app.md." >&2
    exit 1
  fi
done

node_version="$(node -p 'process.versions.node')"
IFS=. read -r node_major node_minor _ <<<"$node_version"
if (( node_major < 22 || node_major >= 27 || (node_major == 22 && node_minor < 12) )); then
  echo "Node.js >=22.12 and <27 is required; found $node_version. See .node-version." >&2
  exit 1
fi

cd "$repository_root"
expected_pnpm="$(node -p "require('./web/package.json').packageManager.replace(/^pnpm@/, '')")"
actual_pnpm="$(pnpm --version)"
if [[ "$actual_pnpm" != "$expected_pnpm" ]]; then
  echo "pnpm $expected_pnpm is required by web/package.json; found $actual_pnpm. Activate the pinned version and rerun this script." >&2
  exit 1
fi

uv python install 3.12
uv sync --locked --extra web
pnpm --dir web install --frozen-lockfile
uv run --locked --extra web --no-build-isolation python -c \
  "from reddit_minerals.web import create_app; assert create_app().title == 'MineralLens API'"

echo "MineralLens web environment is ready. Run scripts/dev-web.sh."
