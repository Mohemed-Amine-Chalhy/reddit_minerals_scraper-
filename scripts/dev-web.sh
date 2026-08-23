#!/usr/bin/env bash
set -Eeuo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export UV_CACHE_DIR="$repository_root/.uv-cache"
host_name="${RMS_WEB_HOST:-127.0.0.1}"
api_port="${RMS_WEB_API_PORT:-8000}"
web_port="${RMS_WEB_PORT:-5173}"
log_root="${TMPDIR:-/tmp}/reddit-minerals-web"

cd "$repository_root"

if [[ ! -f web/package.json ]]; then
  echo "web/package.json is missing; this script requires the web workspace." >&2
  exit 1
fi

for command in uv node pnpm curl; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "$command is required for the web workspace. Run scripts/bootstrap-web.sh first." >&2
    exit 1
  fi
done
if [[ ! -d web/node_modules ]]; then
  echo "Web dependencies are not installed. Run scripts/bootstrap-web.sh first." >&2
  exit 1
fi
if [[ ! -f web/node_modules/.pnpm/lock.yaml ]] || \
  ! cmp -s web/pnpm-lock.yaml web/node_modules/.pnpm/lock.yaml; then
  echo "Web dependencies do not match web/pnpm-lock.yaml. Run scripts/bootstrap-web.sh first." >&2
  exit 1
fi
for port in "$api_port" "$web_port"; do
  if [[ ! "$port" =~ ^[0-9]+$ ]] || (( port < 1 || port > 65535 )); then
    echo "Ports must be integers between 1 and 65535; received $port." >&2
    exit 1
  fi
done
if [[ "$api_port" == "$web_port" ]]; then
  echo "RMS_WEB_API_PORT and RMS_WEB_PORT must be different." >&2
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
  echo "pnpm $expected_pnpm is required by web/package.json; found $actual_pnpm. Run scripts/bootstrap-web.sh after activating the pinned version." >&2
  exit 1
fi

mkdir -p "$log_root"

connect_host="$host_name"
if [[ "$connect_host" == "0.0.0.0" ]]; then
  connect_host="127.0.0.1"
elif [[ "$connect_host" == "::" || "$connect_host" == "[::]" ]]; then
  connect_host="::1"
else
  connect_host="${connect_host#[}"
  connect_host="${connect_host%]}"
fi
if (exec 3<>"/dev/tcp/${connect_host}/${api_port}") 2>/dev/null; then
  exec 3>&- 3<&-
  echo "RMS_WEB_API_PORT $api_port is already accepting connections on $connect_host. Choose a free port." >&2
  exit 1
fi

uv run --locked --extra web --no-build-isolation uvicorn reddit_minerals.web.app:create_app \
  --factory --reload --host "$host_name" --port "$api_port" \
  >"$log_root/api.out.log" 2>"$log_root/api.err.log" &
api_pid=$!

cleanup() {
  if kill -0 "$api_pid" 2>/dev/null; then
    kill "$api_pid" 2>/dev/null || true
    wait "$api_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

probe_host="$host_name"
if [[ "$probe_host" == "0.0.0.0" ]]; then
  probe_host="127.0.0.1"
elif [[ "$probe_host" == "::" || "$probe_host" == "[::]" ]]; then
  probe_host="[::1]"
elif [[ "$probe_host" == *:* && "$probe_host" != \[*\] ]]; then
  probe_host="[$probe_host]"
fi

readiness_deadline=$((SECONDS + 10))
while ((SECONDS < readiness_deadline)); do
  if ! kill -0 "$api_pid" 2>/dev/null; then
    break
  fi
  if curl --fail --silent --max-time 1 "http://${probe_host}:${api_port}/api/v1/health" >/dev/null; then
    sleep 0.1
    if kill -0 "$api_pid" 2>/dev/null; then
      break
    fi
  fi
  sleep 0.25
done

if ! kill -0 "$api_pid" 2>/dev/null; then
  wait "$api_pid" || api_status=$?
  echo "The API exited with code ${api_status:-unknown}. Inspect $log_root/api.err.log." >&2
  exit 1
fi
if ! curl --fail --silent --max-time 1 "http://${probe_host}:${api_port}/api/v1/health" >/dev/null; then
  echo "The API did not become ready within 10 seconds. Inspect $log_root/api.err.log." >&2
  exit 1
fi

pnpm --dir web run dev -- --host "$host_name" --port "$web_port" --strictPort
