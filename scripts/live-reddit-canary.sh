#!/usr/bin/env bash
set -Eeuo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"

if [[ -x "$repository_root/.venv/bin/python" ]]; then
  canary_python="$repository_root/.venv/bin/python"
elif [[ -x "$repository_root/.venv/Scripts/python.exe" ]]; then
  canary_python="$repository_root/.venv/Scripts/python.exe"
elif command -v python3 >/dev/null 2>&1; then
  canary_python="$(command -v python3)"
elif command -v python >/dev/null 2>&1; then
  canary_python="$(command -v python)"
else
  echo "Python 3 is required. Run scripts/bootstrap.sh first." >&2
  exit 1
fi

# Python's standard library keeps request bodies and the job token off child
# process argument lists. No provider value or collected record is printed.
"$canary_python" - "$@" <<'PY'
from __future__ import annotations

import argparse
import json
import os
import secrets
import signal
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

TERMINAL_STATUSES = {"cancelled", "succeeded", "partial", "failed"}
TIME_FILTERS = ("hour", "day", "week", "month", "year", "all")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a low-limit Live Reddit job without printing credentials or collected text."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--mineral", default="gold")
    parser.add_argument("--subreddit", default="mining")
    parser.add_argument("--time-filter", choices=TIME_FILTERS, default="week")
    parser.add_argument("--max-posts", type=int, default=2)
    parser.add_argument("--max-comments", type=int, default=5)
    parser.add_argument("--credential-mode", choices=("server", "provided"), default="server")
    parser.add_argument("--timeout-seconds", type=int, default=180)
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    args = parser.parse_args()
    if not 1 <= args.max_posts <= 100:
        parser.error("--max-posts must be between 1 and 100")
    if not 0 <= args.max_comments <= 500:
        parser.error("--max-comments must be between 0 and 500")
    if not 5 <= args.timeout_seconds <= 3_600:
        parser.error("--timeout-seconds must be between 5 and 3600")
    if not 0.2 <= args.poll_seconds <= 60:
        parser.error("--poll-seconds must be between 0.2 and 60")
    return args


def request_json(
    method: str,
    url: str,
    *,
    payload: dict[str, Any] | None = None,
    token: str | None = None,
    deployment_token: str | None = None,
) -> dict[str, Any]:
    body = None if payload is None else json.dumps(payload, separators=(",", ":")).encode()
    headers = {"Accept": "application/json"}
    if body is not None:
        headers["Content-Type"] = "application/json"
    if token is not None:
        headers["X-Live-Job-Token"] = token
    if deployment_token is not None:
        headers["X-Live-Access-Token"] = deployment_token
    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
            decoded = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        # Response bodies can contain application details; keep canary output
        # deliberately metadata-only.
        raise RuntimeError(f"Live API returned HTTP {error.code} for {method}") from error
    except urllib.error.URLError as error:
        raise RuntimeError(f"Cannot reach the Live API for {method}") from error
    if not isinstance(decoded, dict):
        raise RuntimeError("Live API returned a non-object JSON response")
    return decoded


def require_text(container: dict[str, Any], key: str) -> str:
    value = container.get(key)
    if not isinstance(value, str) or not value:
        raise RuntimeError(f"Live API response is missing {key}")
    return value


def main() -> int:
    args = parse_args()
    parsed = urllib.parse.urlsplit(args.base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise RuntimeError("--base-url must be an absolute http or https URL")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise RuntimeError("--base-url must not contain credentials, a query, or a fragment")
    is_loopback = parsed.hostname in {"localhost", "127.0.0.1", "::1"}
    if args.credential_mode == "provided" and parsed.scheme != "https" and not is_loopback:
        raise RuntimeError("provided credentials require HTTPS unless the backend is on loopback")

    api_root = args.base_url.rstrip("/") + "/api/v1/live"
    deployment_token = os.environ.get("RMS_LIVE_ACCESS_TOKEN", "")
    if len(deployment_token) < 32:
        raise RuntimeError(
            "RMS_LIVE_ACCESS_TOKEN must contain at least 32 characters "
            "in the canary process environment"
        )
    capabilities = request_json("GET", api_root + "/capabilities")
    if capabilities.get("enabled") is not True:
        raise RuntimeError("Live Reddit is disabled on this FastAPI instance")
    modes = capabilities.get("credential_modes")
    if not isinstance(modes, list) or args.credential_mode not in modes:
        raise RuntimeError(
            f"credential mode {args.credential_mode!r} is not available on this FastAPI instance"
        )
    limits = capabilities.get("limits")
    if not isinstance(limits, dict):
        raise RuntimeError("capability response is missing live-job limits")
    if args.max_posts > int(limits.get("max_posts_per_mineral", -1)):
        raise RuntimeError("--max-posts exceeds this deployment's advertised live-job limit")
    if args.max_comments > int(limits.get("max_comments_per_post", -1)):
        raise RuntimeError("--max-comments exceeds this deployment's advertised live-job limit")

    payload: dict[str, Any] = {
        "targets": [{"mineral": args.mineral, "subreddits": [args.subreddit]}],
        "time_filter": args.time_filter,
        "max_posts_per_mineral": args.max_posts,
        "max_comments_per_post": args.max_comments,
        "credential_mode": args.credential_mode,
    }
    if args.credential_mode == "provided":
        names = (
            "RMS_REDDIT_CLIENT_ID",
            "RMS_REDDIT_CLIENT_SECRET",
            "RMS_REDDIT_USER_AGENT",
        )
        missing = [name for name in names if not os.environ.get(name, "").strip()]
        if missing:
            raise RuntimeError(
                "provided mode requires these variables in the canary process environment: "
                + ", ".join(missing)
            )
        payload["credentials"] = {
            "client_id": os.environ["RMS_REDDIT_CLIENT_ID"],
            "client_secret": os.environ["RMS_REDDIT_CLIENT_SECRET"],
            "user_agent": os.environ["RMS_REDDIT_USER_AGENT"],
        }

    interrupted = False

    def mark_interrupted(_signum: int, _frame: Any) -> None:
        nonlocal interrupted
        interrupted = True

    signal.signal(signal.SIGINT, mark_interrupted)
    signal.signal(signal.SIGTERM, mark_interrupted)

    job_id: str | None = None
    token: str | None = secrets.token_urlsafe(32)
    terminal = False
    try:
        created: dict[str, Any] | None = None
        for attempt in range(2):
            try:
                created = request_json(
                    "POST",
                    api_root + "/jobs",
                    payload=payload,
                    token=token,
                    deployment_token=deployment_token,
                )
                break
            except RuntimeError:
                if attempt == 1:
                    raise
                time.sleep(0.5)
        if created is None:  # defensive; the loop either creates or raises
            raise RuntimeError("job creation did not return a response")
        payload.pop("credentials", None)
        job = created.get("job")
        if not isinstance(job, dict):
            raise RuntimeError("job-creation response is missing job metadata")
        job_id = require_text(job, "id")
        echoed_token = require_text(created, "access_token")
        if echoed_token != token:
            raise RuntimeError("job-creation response did not echo the expected access token")
        job_url = f"{api_root}/jobs/{job_id}"
        print(
            f"Created live canary job {job_id}; the access token will not be displayed.",
            flush=True,
        )

        deadline = time.monotonic() + args.timeout_seconds
        while not terminal:
            if interrupted:
                raise RuntimeError("live canary was interrupted")
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"live canary did not reach a terminal state within {args.timeout_seconds} seconds"
                )
            time.sleep(args.poll_seconds)
            job = request_json("GET", job_url, token=token)
            status = require_text(job, "status")
            stage = require_text(job, "stage")
            progress = job.get("progress")
            if not isinstance(progress, dict):
                raise RuntimeError("job response is missing progress metadata")
            posts = int(progress.get("posts_stored", 0))
            comments = int(progress.get("comments_stored", 0))
            failures = int(progress.get("posts_failed", 0)) + int(
                progress.get("searches_failed", 0)
            )
            print(
                f"status={status} stage={stage} posts={posts} comments={comments} "
                f"failures={failures}",
                flush=True,
            )
            terminal = status in TERMINAL_STATUSES

        if status in {"succeeded", "partial"}:
            snapshot = request_json("GET", job_url + "/snapshot", token=token)
            records = snapshot.get("records")
            if not isinstance(records, list):
                raise RuntimeError("snapshot response is missing its record list")
            if len(records) != int(job.get("record_count", -1)):
                raise RuntimeError("snapshot count did not match the terminal job summary")
            print(
                f"Verified snapshot metadata for {len(records)} record(s); content was not printed.",
                flush=True,
            )

        if status != "succeeded":
            error = job.get("error")
            error_code = error.get("code", "none") if isinstance(error, dict) else "none"
            raise RuntimeError(
                f"live canary ended with status {status!r} and safe error code {error_code!r}"
            )
        print("Live Reddit canary succeeded.", flush=True)
        return 0
    finally:
        if job_id and token:
            try:
                request_json("DELETE", f"{api_root}/jobs/{job_id}", token=token)
                action = "cleanup" if terminal else "cancellation"
                print(f"Requested {action} for canary job {job_id}.", file=sys.stderr)
            except Exception:
                print(
                    f"Warning: could not cancel or clean up canary job {job_id}; "
                    "server retention must remove it.",
                    file=sys.stderr,
                )


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"Live Reddit canary failed: {error}", file=sys.stderr)
        raise SystemExit(1) from None
PY
