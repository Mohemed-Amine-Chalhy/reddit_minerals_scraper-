#!/usr/bin/env bash
set -Eeuo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"

uv run --locked --no-build-isolation ruff check --fix .
uv run --locked --no-build-isolation ruff format .
