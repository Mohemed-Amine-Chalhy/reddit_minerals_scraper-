#!/usr/bin/env bash
set -Eeuo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required. Install it from https://docs.astral.sh/uv/getting-started/installation/ and rerun this script." >&2
  exit 1
fi

uv python install 3.12
uv sync --locked
uv run --locked python scripts/validate_env_example.py
uv run --locked pre-commit install --install-hooks --hook-type pre-commit --hook-type pre-push

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "Created .env from the safe template. Replace its credential placeholders before live commands."
fi

bash scripts/smoke.sh
