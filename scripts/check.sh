#!/usr/bin/env bash
set -Eeuo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"
export UV_CACHE_DIR="$repository_root/.uv-cache"
export PRE_COMMIT_HOME="$repository_root/.cache/pre-commit"

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required. Run scripts/bootstrap.sh first." >&2
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
      echo "$command is required because web/package.json is present. Run scripts/bootstrap.sh." >&2
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
    echo "pnpm $expected_pnpm is required by web/package.json; found $actual_pnpm. Run scripts/bootstrap.sh after activating the pinned version." >&2
    exit 1
  fi
  if [[ ! -d web/node_modules ]]; then
    echo "Web dependencies are not installed. Run scripts/bootstrap.sh first." >&2
    exit 1
  fi
  if [[ ! -f web/node_modules/.pnpm/lock.yaml ]] || \
    ! cmp -s web/pnpm-lock.yaml web/node_modules/.pnpm/lock.yaml; then
    echo "Web dependencies do not match web/pnpm-lock.yaml. Run scripts/bootstrap.sh first." >&2
    exit 1
  fi
}

uv_run() {
  local uv_arguments=(run --locked --no-build-isolation)
  if "$has_web_workspace"; then
    uv_arguments+=(--extra web)
  fi
  uv "${uv_arguments[@]}" "$@"
}

run_pre_commit_without_duplicate_web_checks() {
  local previous_skip="${SKIP-}" pre_commit_status=0
  local skip_was_set=false
  if [[ ${SKIP+x} ]]; then
    skip_was_set=true
  fi
  if "$has_web_workspace"; then
    if [[ -n "$previous_skip" ]]; then
      export SKIP="$previous_skip,web-prettier,web-eslint,web-typecheck"
    else
      export SKIP="web-prettier,web-eslint,web-typecheck"
    fi
  fi

  uv_run pre-commit run --all-files --show-diff-on-failure || pre_commit_status=$?
  if "$skip_was_set"; then
    export SKIP="$previous_skip"
  else
    unset SKIP
  fi
  return "$pre_commit_status"
}

if "$has_web_workspace"; then
  assert_web_toolchain
fi

uv lock --check
run_pre_commit_without_duplicate_web_checks
bash scripts/smoke.sh
uv_run mypy src/reddit_minerals
uv_run python scripts/run_tests.py
if "$has_web_workspace"; then
  pnpm --dir web run check
fi
uv_run pip-audit --progress-spinner=off --cache-dir "$repository_root/.cache/pip-audit"
uv build --clear --no-build-isolation --out-dir dist
uv_run python scripts/check_artifacts.py
