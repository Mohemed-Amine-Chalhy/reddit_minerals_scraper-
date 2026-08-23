from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from base64 import urlsafe_b64encode
from collections.abc import AsyncIterator, Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from pydantic import ValidationError

import reddit_minerals.web.jobs as live_jobs_module
from reddit_minerals.clients.base import RedditClient
from reddit_minerals.config import AppSettings
from reddit_minerals.errors import ConfigurationError, PermanentProviderError
from reddit_minerals.models import CommentRecord, PostRecord
from reddit_minerals.web import create_app
from reddit_minerals.web.app import MAX_LIVE_JOB_REQUEST_BODY_BYTES
from reddit_minerals.web.jobs import (
    LiveJobCapacityError,
    LiveJobManager,
    RedditCredentialValues,
)
from reddit_minerals.web.live_models import LiveJobCreateRequest
from reddit_minerals.web.repository import SyntheticReadRepository
from tests.fakes import FakeRedditClient

_CREATION_ACCESS_TOKEN = "unit-live-access-token-0123456789abcdef"


def _job_token(label: str) -> str:
    return urlsafe_b64encode(hashlib.sha256(label.encode("utf-8")).digest()).rstrip(b"=").decode()


def _creation_headers(label: str) -> dict[str, str]:
    return {
        "X-Live-Access-Token": _CREATION_ACCESS_TOKEN,
        "X-Live-Job-Token": _job_token(label),
    }


def _request(app: FastAPI, method: str, path: str, **kwargs: Any) -> httpx.Response:
    async def send() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="https://live.test") as client:
            return await client.request(method, path, **kwargs)

    return asyncio.run(send())


def _settings(tmp_path: Path, **overrides: Any) -> AppSettings:
    values: dict[str, Any] = {
        "database_path": tmp_path / "unused.sqlite3",
        "live_reddit_enabled": True,
        "live_reddit_allow_byo_credentials": True,
        "live_access_token": _CREATION_ACCESS_TOKEN,
        "live_job_root": tmp_path / "live-jobs",
        "live_job_max_workers": 1,
        "live_job_max_active": 4,
        "live_job_retention_seconds": 3_600,
        "live_job_max_retained": 10,
        "max_retries": 1,
        "retry_base_delay_seconds": 0,
        "retry_max_delay_seconds": 0,
        "operation_timeout_seconds": 10,
        "reddit_request_timeout_seconds": 5,
        "gemini_request_timeout_seconds": 10,
        "max_posts_per_mineral": 20,
        "max_comments_per_post": 50,
        "reddit_client_id": None,
        "reddit_client_secret": None,
        "reddit_user_agent": None,
    }
    values.update(overrides)
    return AppSettings(**values)


def _provided_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "targets": [{"mineral": "Copper", "subreddits": ["mining"]}],
        "time_filter": "week",
        "max_posts_per_mineral": 2,
        "max_comments_per_post": 3,
        "credential_mode": "provided",
        "credentials": {
            "client_id": "live-client-id",
            "client_secret": "live-client-secret",  # pragma: allowlist secret
            "user_agent": "script:minerallens:test (by u/researcher)",
        },
    }
    payload.update(overrides)
    return payload


def _app(
    settings: AppSettings,
    factory: Any,
) -> tuple[FastAPI, LiveJobManager]:
    manager = LiveJobManager(settings=settings, client_factory=factory)
    app = create_app(
        repository=SyntheticReadRepository(),
        asset_root=Path("missing-web-assets"),
        settings=settings,
        live_job_manager=manager,
    )
    return app, manager


def _wait_for_terminal(app: FastAPI, job_id: str, token: str) -> dict[str, Any]:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        response = _request(
            app,
            "GET",
            f"/api/v1/live/jobs/{job_id}",
            headers={"X-Live-Job-Token": token},
        )
        assert response.status_code == 200
        payload = response.json()
        if payload["status"] in {"cancelled", "succeeded", "partial", "failed"}:
            return payload
        time.sleep(0.01)
    raise AssertionError("live job did not reach a terminal state")


def _post() -> PostRecord:
    return PostRecord(
        id="p-live",
        title="Copper recycling capacity",
        selftext="A public discussion of secondary copper supply.",
        subreddit="mining",
        created_at=datetime(2026, 8, 20, 12, tzinfo=UTC),
        score=42,
        num_comments=1,
        upvote_ratio=0.91,
        permalink="https://www.reddit.com/r/mining/comments/p-live/example/",
        fetched_at=datetime(2026, 8, 23, 12, tzinfo=UTC),
    )


def _comment() -> CommentRecord:
    return CommentRecord(
        id="c-live",
        post_id="p-live",
        parent_id="t3_p-live",
        body="Smelter constraints matter too.",
        score=8,
        created_at=datetime(2026, 8, 20, 13, tzinfo=UTC),
        depth=0,
        subreddit="mining",
        permalink="https://www.reddit.com/r/mining/comments/p-live/example/c-live/",
        fetched_at=datetime(2026, 8, 23, 12, tzinfo=UTC),
    )


def test_disabled_live_api_is_discoverable_but_cannot_start_jobs(tmp_path: Path) -> None:
    settings = _settings(
        tmp_path,
        live_reddit_enabled=False,
        live_reddit_allow_byo_credentials=True,
    )
    app = create_app(
        repository=SyntheticReadRepository(),
        asset_root=Path("missing-web-assets"),
        settings=settings,
    )

    capabilities = _request(app, "GET", "/api/v1/live/capabilities")
    assert capabilities.status_code == 200
    assert capabilities.json() | {"defaults": {}, "limits": {}} == {
        "enabled": False,
        "provider": "reddit",
        "library": "PRAW",
        "server_credentials_configured": False,
        "byo_credentials_allowed": False,
        "credential_modes": [],
        "creation_access_token_required": True,
        "creation_access_token_header": "X-Live-Access-Token",
        "access_token_header": "X-Live-Job-Token",
        "time_filters": ["hour", "day", "week", "month", "year", "all"],
        "defaults": {},
        "limits": {},
    }
    assert app.state.live_job_manager is None
    config = _request(app, "GET", "/api/v1/config").json()
    assert config["providers_enabled"] is False
    assert config["features"]["live_collection"] is False
    assert config["features"]["mutation"] is False

    rejected = _request(
        app,
        "POST",
        "/api/v1/live/jobs",
        json=_provided_payload(),
        headers=_creation_headers("disabled"),
    )
    assert rejected.status_code == 503
    assert rejected.json()["code"] == "live_collection_disabled"
    assert "live-client-secret" not in rejected.text


@pytest.mark.parametrize(
    "supplied_token",
    [None, "too-short", _job_token("wrong-deployment-access")],
)
def test_live_job_creation_requires_sanitized_deployment_access(
    tmp_path: Path,
    supplied_token: str | None,
) -> None:
    settings = _settings(tmp_path)
    app, manager = _app(
        settings,
        lambda _values, _settings: pytest.fail("unauthorized creation reached provider setup"),
    )
    headers = {"X-Live-Job-Token": _job_token(f"unauthorized-{len(supplied_token or '')}")}
    if supplied_token is not None:
        headers["X-Live-Access-Token"] = supplied_token
    try:
        response = _request(
            app,
            "POST",
            "/api/v1/live/jobs",
            json=_provided_payload(),
            headers=headers,
        )
        assert response.status_code == 401
        assert response.json() == {
            "code": "live_access_unauthorized",
            "message": "Live job creation requires valid access credentials.",
            "issues": [],
        }
        rendered = response.text + json.dumps(
            _request(app, "GET", "/api/v1/live/capabilities").json()
        )
        assert _CREATION_ACCESS_TOKEN not in rendered
        assert supplied_token is None or supplied_token not in rendered
        assert manager._jobs == {}
    finally:
        manager.shutdown()


@pytest.mark.parametrize("supplied_token", [None, "b" * 32])
def test_live_job_creation_authenticates_before_parsing_a_malformed_body(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    supplied_token: str | None,
) -> None:
    settings = _settings(tmp_path)
    app, manager = _app(
        settings,
        lambda _values, _settings: pytest.fail("unauthorized creation reached provider setup"),
    )
    manager_calls = 0

    def fail_if_manager_reached(*_args: Any, **_kwargs: Any) -> None:
        nonlocal manager_calls
        manager_calls += 1
        pytest.fail("unauthorized malformed body reached the live job manager")

    monkeypatch.setattr(manager, "create_job", fail_if_manager_reached)
    headers = {"X-Live-Job-Token": _job_token("malformed-pre-auth")}
    if supplied_token is not None:
        headers["X-Live-Access-Token"] = supplied_token
    try:
        response = _request(
            app,
            "POST",
            "/api/v1/live/jobs",
            content=b'{"credentials":{"client_secret":"must-not-be-parsed"}',  # pragma: allowlist secret
            headers={**headers, "Content-Type": "application/json"},
        )
        assert response.status_code == 401
        assert response.json() == {
            "code": "live_access_unauthorized",
            "message": "Live job creation requires valid access credentials.",
            "issues": [],
        }
        assert "must-not-be-parsed" not in response.text
        assert response.headers["Cache-Control"] == "no-store"
        assert manager_calls == 0
        assert manager._jobs == {}
    finally:
        manager.shutdown()


def test_live_job_creation_rejects_oversized_declared_body_before_manager(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    app, manager = _app(
        settings,
        lambda _values, _settings: pytest.fail("oversized creation reached provider setup"),
    )
    manager_calls = 0

    def fail_if_manager_reached(*_args: Any, **_kwargs: Any) -> None:
        nonlocal manager_calls
        manager_calls += 1
        pytest.fail("oversized declared body reached the live job manager")

    monkeypatch.setattr(manager, "create_job", fail_if_manager_reached)
    try:
        response = _request(
            app,
            "POST",
            "/api/v1/live/jobs",
            content=b"{}",
            headers={
                **_creation_headers("oversized-declared-body"),
                "Content-Length": str(MAX_LIVE_JOB_REQUEST_BODY_BYTES + 1),
                "Content-Type": "application/json",
            },
        )
        assert response.status_code == 413
        assert response.json()["code"] == "live_job_request_too_large"
        assert response.headers["Cache-Control"] == "no-store"
        assert manager_calls == 0
        assert manager._jobs == {}
    finally:
        manager.shutdown()


def test_live_job_creation_rejects_oversized_stream_without_content_length(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    app, manager = _app(
        settings,
        lambda _values, _settings: pytest.fail("oversized stream reached provider setup"),
    )
    manager_calls = 0

    def fail_if_manager_reached(*_args: Any, **_kwargs: Any) -> None:
        nonlocal manager_calls
        manager_calls += 1
        pytest.fail("oversized streamed body reached the live job manager")

    async def oversized_stream() -> AsyncIterator[bytes]:
        yield b'{"padding":"'
        yield b"x" * MAX_LIVE_JOB_REQUEST_BODY_BYTES
        yield b'"}'

    monkeypatch.setattr(manager, "create_job", fail_if_manager_reached)
    try:
        response = _request(
            app,
            "POST",
            "/api/v1/live/jobs",
            content=oversized_stream(),
            headers={
                **_creation_headers("oversized-streamed-body"),
                "Content-Type": "application/json",
            },
        )
        assert response.status_code == 413
        assert response.json()["code"] == "live_job_request_too_large"
        assert response.headers["Cache-Control"] == "no-store"
        assert manager_calls == 0
        assert manager._jobs == {}
    finally:
        manager.shutdown()


@pytest.mark.parametrize(
    "headers",
    [
        {"X-Live-Access-Token": _CREATION_ACCESS_TOKEN},
        {
            "X-Live-Access-Token": _CREATION_ACCESS_TOKEN,
            "X-Live-Job-Token": "short",
        },
        {
            "X-Live-Access-Token": _CREATION_ACCESS_TOKEN,
            "X-Live-Job-Token": "Z" * 43,
        },
    ],
)
def test_live_job_creation_rejects_missing_or_noncanonical_job_token(
    tmp_path: Path,
    headers: dict[str, str],
) -> None:
    settings = _settings(tmp_path)
    app, manager = _app(settings, lambda _values, _settings: FakeRedditClient())
    try:
        response = _request(
            app,
            "POST",
            "/api/v1/live/jobs",
            json=_provided_payload(),
            headers=headers,
        )
        assert response.status_code == 422
        assert "Z" * 43 not in response.text
        assert manager._jobs == {}
    finally:
        manager.shutdown()


def test_live_job_creation_rejects_oversized_access_header_without_echoing_it(
    tmp_path: Path,
) -> None:
    oversized = "x" * 513
    settings = _settings(tmp_path)
    app, manager = _app(settings, lambda _values, _settings: FakeRedditClient())
    try:
        response = _request(
            app,
            "POST",
            "/api/v1/live/jobs",
            json=_provided_payload(),
            headers={
                "X-Live-Access-Token": oversized,
                "X-Live-Job-Token": _job_token("oversized-creation-access"),
            },
        )
        assert response.status_code == 401
        assert response.json()["code"] == "live_access_unauthorized"
        assert oversized not in response.text
        assert manager._jobs == {}
    finally:
        manager.shutdown()


def test_provided_credentials_job_is_token_isolated_and_snapshot_is_raw(
    tmp_path: Path,
) -> None:
    fake = FakeRedditClient()
    fake.queue_search("copper", "mining", [_post()])
    fake.queue_comments("p-live", [_comment()])
    credential_checks: list[bool] = []

    def factory(values: RedditCredentialValues, _settings: AppSettings) -> RedditClient:
        credential_checks.append(
            values.client_id == "live-client-id"
            and values.client_secret == "live-client-secret"  # pragma: allowlist secret
            and values.user_agent.startswith("script:minerallens")
        )
        return fake

    settings = _settings(tmp_path)
    app, manager = _app(settings, factory)
    try:
        capabilities = _request(app, "GET", "/api/v1/live/capabilities").json()
        assert capabilities["credential_modes"] == ["provided"]
        assert capabilities["byo_credentials_allowed"] is True
        assert capabilities["limits"]["max_posts_per_mineral"] == 20
        assert capabilities["limits"]["max_records_per_job"] == 10_000
        assert capabilities["limits"]["max_active_jobs"] == 4

        request_model = LiveJobCreateRequest.model_validate(_provided_payload())
        assert "live-client-secret" not in repr(request_model)
        created = _request(
            app,
            "POST",
            "/api/v1/live/jobs",
            json=_provided_payload(),
            headers=_creation_headers("provided-snapshot"),
        )
        assert created.status_code == 202
        created_payload = created.json()
        encoded = json.dumps(created_payload)
        assert "live-client-id" not in encoded
        assert "live-client-secret" not in encoded
        token = created_payload["access_token"]
        assert token == _job_token("provided-snapshot")
        job_id = created_payload["job"]["id"]
        assert created_payload["job"]["expires_at"] is None

        assert _request(app, "GET", f"/api/v1/live/jobs/{job_id}").status_code == 404
        assert (
            _request(
                app,
                "GET",
                f"/api/v1/live/jobs/{job_id}",
                headers={"X-Live-Job-Token": "wrong-token-value"},
            ).status_code
            == 404
        )

        job = _wait_for_terminal(app, job_id, token)
        assert job["status"] == "succeeded"
        assert job["stage"] == "complete"
        assert job["expires_at"] is not None
        assert job["progress"] == {
            "minerals_total": 1,
            "minerals_completed": 1,
            "subreddits_total": 1,
            "subreddits_completed": 1,
            "posts_discovered": 1,
            "posts_stored": 1,
            "posts_failed": 0,
            "comments_stored": 1,
            "searches_failed": 0,
        }
        assert job["record_count"] == 2
        assert credential_checks == [True]

        snapshot = _request(
            app,
            "GET",
            f"/api/v1/live/jobs/{job_id}/snapshot",
            headers={"X-Live-Job-Token": token},
        )
        assert snapshot.status_code == 200
        records = snapshot.json()["records"]
        assert [record["kind"] for record in records] == ["post", "comment"]
        assert records[0]["post_id"] is None
        assert records[0]["depth"] is None
        assert records[0]["body"] == "A public discussion of secondary copper supply."
        assert records[1]["post_id"] == "p-live"
        assert records[1]["parent_id"] == "t3_p-live"
        assert records[1]["depth"] == 0
        assert records[1]["body"] == "Smelter constraints matter too."

        job_directory = settings.live_job_root / job_id
        assert (job_directory / "reddit.sqlite3").is_file()
        deleted = _request(
            app,
            "DELETE",
            f"/api/v1/live/jobs/{job_id}",
            headers={"X-Live-Job-Token": token},
        )
        assert deleted.status_code == 202
        assert deleted.json()["record_count"] == 0
        assert "deleted" in deleted.json()["message"].casefold()
        assert not job_directory.exists()
        assert (
            _request(
                app,
                "GET",
                f"/api/v1/live/jobs/{job_id}",
                headers={"X-Live-Job-Token": token},
            ).status_code
            == 404
        )
    finally:
        manager.shutdown()


class _BlockingRedditClient:
    def __init__(self) -> None:
        self.started = Event()
        self.release = Event()
        self.closed = Event()

    def search_posts(
        self,
        *,
        mineral: str,
        subreddit: str,
        limit: int,
        time_filter: str,
        deadline: float | None = None,
    ) -> Iterable[PostRecord]:
        del mineral, subreddit, limit, time_filter, deadline
        self.started.set()
        if not self.release.wait(timeout=5):
            raise TimeoutError("test release was not signalled")
        return ()

    def fetch_comments(
        self,
        *,
        post_id: str,
        limit: int,
        deadline: float | None = None,
    ) -> Sequence[CommentRecord]:
        del post_id, limit, deadline
        return ()

    def close(self) -> None:
        self.closed.set()


def test_concurrent_create_is_idempotent_before_capacity_and_credentials(
    tmp_path: Path,
) -> None:
    blocking = _BlockingRedditClient()
    resolved_credentials: list[str] = []

    def factory(values: RedditCredentialValues, _settings: AppSettings) -> RedditClient:
        resolved_credentials.append(values.client_id)
        return blocking

    settings = _settings(tmp_path, live_job_max_active=1)
    app, manager = _app(settings, factory)
    headers = _creation_headers("idempotent-concurrent")
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(
                    _request,
                    app,
                    "POST",
                    "/api/v1/live/jobs",
                    json=_provided_payload(),
                    headers=headers,
                )
                for _index in range(2)
            ]
            responses = [future.result(timeout=3) for future in futures]
        assert [response.status_code for response in responses] == [202, 202]
        created = [response.json() for response in responses]
        assert created[0]["job"]["id"] == created[1]["job"]["id"]
        assert (
            created[0]["access_token"] == created[1]["access_token"] == headers["X-Live-Job-Token"]
        )
        expected_id = (
            hashlib.sha256(
                b"minerallens-live-job-v1\0" + headers["X-Live-Job-Token"].encode("ascii")
            )
            .digest()[:16]
            .hex()
        )
        assert created[0]["job"]["id"] == expected_id
        assert blocking.started.wait(timeout=2)
        assert resolved_credentials == ["live-client-id"]

        changed_credentials = _provided_payload(
            credentials={
                "client_id": "different-client-id",
                "client_secret": "different-client-secret",  # pragma: allowlist secret
                "user_agent": "script:other-client:test (by u/researcher)",
            }
        )
        duplicate = _request(
            app,
            "POST",
            "/api/v1/live/jobs",
            json=changed_credentials,
            headers=headers,
        )
        assert duplicate.status_code == 202
        assert duplicate.json()["job"]["id"] == expected_id
        assert resolved_credentials == ["live-client-id"]

        conflict = _request(
            app,
            "POST",
            "/api/v1/live/jobs",
            json=_provided_payload(time_filter="month"),
            headers=headers,
        )
        assert conflict.status_code == 409
        assert conflict.json()["code"] == "live_job_idempotency_conflict"
        assert headers["X-Live-Job-Token"] not in conflict.text

        capacity_before_credentials = _request(
            app,
            "POST",
            "/api/v1/live/jobs",
            json={
                "targets": [{"mineral": "copper", "subreddits": ["mining"]}],
                "max_posts_per_mineral": 1,
                "max_comments_per_post": 0,
                "credential_mode": "server",
            },
            headers=_creation_headers("capacity-before-credentials"),
        )
        assert capacity_before_credentials.status_code == 429
        assert capacity_before_credentials.json()["code"] == "live_job_capacity_reached"
        assert resolved_credentials == ["live-client-id"]

        blocking.release.set()
        terminal = _wait_for_terminal(app, expected_id, headers["X-Live-Job-Token"])
        assert terminal["status"] == "succeeded"
    finally:
        blocking.release.set()
        manager.shutdown()


def test_create_submission_failure_rolls_back_idempotency_reservation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    manager = LiveJobManager(
        settings=settings,
        client_factory=lambda _values, _settings: FakeRedditClient(),
    )
    request = LiveJobCreateRequest.model_validate(_provided_payload())
    token = _job_token("submission-rollback")
    original_submit = manager._executor.submit

    def reject_submission(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("executor rejected submission")

    try:
        monkeypatch.setattr(manager._executor, "submit", reject_submission)
        with pytest.raises(LiveJobCapacityError):
            manager.create_job(
                request,
                creation_access_token=_CREATION_ACCESS_TOKEN,
                job_access_token=token,
            )
        assert manager._jobs == {}
        assert manager._credential_vault == {}

        monkeypatch.setattr(manager._executor, "submit", original_submit)
        view, echoed = manager.create_job(
            request,
            creation_access_token=_CREATION_ACCESS_TOKEN,
            job_access_token=token,
        )
        assert echoed == token
        future = manager._jobs[view.id].future
        assert future is not None
        future.result(timeout=3)
        assert manager.get_job(view.id, token).status.value == "succeeded"
    finally:
        manager.shutdown()


def test_queued_job_can_be_cancelled_and_terminal_delete_removes_it(tmp_path: Path) -> None:
    blocking = _BlockingRedditClient()

    def factory(_values: RedditCredentialValues, _settings: AppSettings) -> RedditClient:
        return blocking

    settings = _settings(tmp_path)
    app, manager = _app(settings, factory)
    try:
        first = _request(
            app,
            "POST",
            "/api/v1/live/jobs",
            json=_provided_payload(),
            headers=_creation_headers("queued-first"),
        ).json()
        assert blocking.started.wait(timeout=2)
        second = _request(
            app,
            "POST",
            "/api/v1/live/jobs",
            json=_provided_payload(),
            headers=_creation_headers("queued-second"),
        ).json()

        cancelled = _request(
            app,
            "DELETE",
            f"/api/v1/live/jobs/{second['job']['id']}",
            headers={"X-Live-Job-Token": second["access_token"]},
        )
        assert cancelled.status_code == 202
        assert cancelled.json()["status"] == "cancelled"
        unavailable = _request(
            app,
            "GET",
            f"/api/v1/live/jobs/{second['job']['id']}/snapshot",
            headers={"X-Live-Job-Token": second["access_token"]},
        )
        assert unavailable.status_code == 409
        assert unavailable.json()["code"] == "live_snapshot_unavailable"

        removed = _request(
            app,
            "DELETE",
            f"/api/v1/live/jobs/{second['job']['id']}",
            headers={"X-Live-Job-Token": second["access_token"]},
        )
        assert removed.status_code == 202
        assert "deleted" in removed.json()["message"].casefold()

        cancel_running = _request(
            app,
            "DELETE",
            f"/api/v1/live/jobs/{first['job']['id']}",
            headers={"X-Live-Job-Token": first["access_token"]},
        )
        assert cancel_running.status_code == 202
        assert cancel_running.json()["status"] == "cancel_requested"
        blocking.release.set()
        first_job = _wait_for_terminal(app, first["job"]["id"], first["access_token"])
        assert first_job["status"] == "cancelled"
    finally:
        blocking.release.set()
        manager.shutdown()


def test_unexpected_provider_error_never_reflects_provided_secret(
    tmp_path: Path,
    caplog: Any,
) -> None:
    def failing_factory(values: RedditCredentialValues, _settings: AppSettings) -> RedditClient:
        raise RuntimeError(values.client_secret)

    settings = _settings(tmp_path)
    app, manager = _app(settings, failing_factory)
    try:
        with caplog.at_level(logging.ERROR):
            created = _request(
                app,
                "POST",
                "/api/v1/live/jobs",
                json=_provided_payload(),
                headers=_creation_headers("provider-failure"),
            ).json()
            job = _wait_for_terminal(app, created["job"]["id"], created["access_token"])
        assert job["status"] == "failed"
        assert job["error"] == {
            "code": "internal_error",
            "message": "Live collection failed unexpectedly.",
        }
        rendered = json.dumps(job) + " ".join(record.getMessage() for record in caplog.records)
        assert "live-client-secret" not in rendered
        snapshot = _request(
            app,
            "GET",
            f"/api/v1/live/jobs/{created['job']['id']}/snapshot",
            headers={"X-Live-Job-Token": created["access_token"]},
        )
        assert snapshot.status_code == 409
    finally:
        manager.shutdown()


def test_deployment_bounds_return_validation_error_and_server_mode_is_discoverable(
    tmp_path: Path,
) -> None:
    settings = _settings(
        tmp_path,
        live_reddit_allow_byo_credentials=False,
        max_posts_per_mineral=2,
        reddit_client_id="configured-client",
        reddit_client_secret="configured-secret",  # pragma: allowlist secret
        reddit_user_agent="script:minerallens:test (by u/researcher)",
    )
    fake = FakeRedditClient()
    resolved_server_secret: list[str] = []

    def factory(values: RedditCredentialValues, _settings: AppSettings) -> RedditClient:
        resolved_server_secret.append(values.client_secret)
        return fake

    app, manager = _app(settings, factory)
    try:
        capabilities = _request(app, "GET", "/api/v1/live/capabilities").json()
        assert capabilities["server_credentials_configured"] is True
        assert capabilities["byo_credentials_allowed"] is False
        assert capabilities["credential_modes"] == ["server"]
        assert _request(app, "GET", "/api/v1/config").json()["providers_enabled"] is True

        payload = {
            "targets": [{"mineral": "copper", "subreddits": ["mining"]}],
            "max_posts_per_mineral": 3,
            "max_comments_per_post": 0,
            "credential_mode": "server",
        }
        response = _request(
            app,
            "POST",
            "/api/v1/live/jobs",
            json=payload,
            headers=_creation_headers("bounds-rejected"),
        )
        assert response.status_code == 422
        assert response.json()["code"] == "live_job_limit_exceeded"

        payload["max_posts_per_mineral"] = 2
        created = _request(
            app,
            "POST",
            "/api/v1/live/jobs",
            json=payload,
            headers=_creation_headers("server-mode"),
        ).json()
        job = _wait_for_terminal(app, created["job"]["id"], created["access_token"])
        assert job["status"] == "succeeded"
        assert resolved_server_secret == ["configured-secret"]
        assert "configured-secret" not in json.dumps(created)
    finally:
        manager.shutdown()


def test_partial_job_exposes_completed_records_and_sanitized_error_counts(
    tmp_path: Path,
) -> None:
    fake = FakeRedditClient()
    fake.queue_search("copper", "mining", [_post()])
    fake.queue_comments("p-live", [_comment()])
    fake.queue_search("copper", "geology", PermanentProviderError("private detail"))
    settings = _settings(tmp_path)
    app, manager = _app(settings, lambda _values, _settings: fake)
    try:
        payload = _provided_payload(
            targets=[{"mineral": "copper", "subreddits": ["mining", "geology"]}]
        )
        created = _request(
            app,
            "POST",
            "/api/v1/live/jobs",
            json=payload,
            headers=_creation_headers("partial"),
        ).json()
        job = _wait_for_terminal(app, created["job"]["id"], created["access_token"])
        assert job["status"] == "partial"
        assert job["progress"]["searches_failed"] == 1
        assert "private detail" not in json.dumps(job)
        snapshot = _request(
            app,
            "GET",
            f"/api/v1/live/jobs/{created['job']['id']}/snapshot",
            headers={"X-Live-Job-Token": created["access_token"]},
        )
        assert snapshot.status_code == 200
        assert len(snapshot.json()["records"]) == 2
    finally:
        manager.shutdown()


def test_live_request_model_closes_credential_and_aggregate_shapes() -> None:
    with pytest.raises(ValidationError, match="credentials are required"):
        LiveJobCreateRequest.model_validate(_provided_payload(credentials=None))
    with pytest.raises(ValidationError, match="credentials must be omitted"):
        LiveJobCreateRequest.model_validate(_provided_payload(credential_mode="server"))
    with pytest.raises(ValidationError, match="only one target"):
        LiveJobCreateRequest.model_validate(
            _provided_payload(
                targets=[
                    {"mineral": "Copper", "subreddits": ["mining"]},
                    {"mineral": " copper ", "subreddits": ["geology"]},
                ]
            )
        )
    with pytest.raises(ValidationError, match="record safety budget"):
        LiveJobCreateRequest.model_validate(
            _provided_payload(max_posts_per_mineral=100, max_comments_per_post=500)
        )

    normalized = LiveJobCreateRequest.model_validate(
        _provided_payload(
            targets=[{"mineral": " Copper  Supply ", "subreddits": ["Mining", "mining"]}]
        )
    )
    assert normalized.targets[0].mineral == "copper supply"
    assert normalized.targets[0].subreddits == ("Mining",)


def test_manager_startup_purges_only_exact_orphan_job_directories(tmp_path: Path) -> None:
    root = tmp_path / "live-jobs"
    owned_orphan = root / ("a" * 32)
    unmarked_orphan = root / ("c" * 32)
    unrelated = root / "keep-me"
    owned_orphan.mkdir(parents=True)
    unmarked_orphan.mkdir()
    unrelated.mkdir()
    (owned_orphan / ".minerallens-live-job").write_bytes(
        f"minerallens-live-job:v1:{owned_orphan.name}\n".encode()
    )
    (owned_orphan / "reddit.sqlite3").write_bytes(b"orphan")
    (unmarked_orphan / "reddit.sqlite3").write_bytes(b"unowned")
    (unrelated / "note.txt").write_text("keep", encoding="utf-8")

    manager = LiveJobManager(
        settings=_settings(tmp_path),
        client_factory=lambda _values, _settings: FakeRedditClient(),
    )
    try:
        assert not owned_orphan.exists()
        assert (unmarked_orphan / "reddit.sqlite3").read_bytes() == b"unowned"
        assert (unrelated / "note.txt").read_text(encoding="utf-8") == "keep"
    finally:
        manager.shutdown()


def test_job_ownership_marker_exists_before_database_initialization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    manager = LiveJobManager(
        settings=settings,
        client_factory=lambda _values, _settings: FakeRedditClient(),
    )
    app = create_app(
        repository=SyntheticReadRepository(),
        asset_root=Path("missing-web-assets"),
        settings=settings,
        live_job_manager=manager,
    )
    original_initialize = live_jobs_module.Database.initialize
    observed_markers: list[str] = []

    def initialize_after_marker(database: Any) -> None:
        if database.path.name == "reddit.sqlite3":
            marker = database.path.parent / ".minerallens-live-job"
            observed_markers.append(marker.read_text(encoding="utf-8"))
        original_initialize(database)

    monkeypatch.setattr(live_jobs_module.Database, "initialize", initialize_after_marker)
    try:
        created = _request(
            app,
            "POST",
            "/api/v1/live/jobs",
            json=_provided_payload(),
            headers=_creation_headers("marker-before-database"),
        ).json()
        job_id = created["job"]["id"]
        assert _wait_for_terminal(app, job_id, created["access_token"])["status"] == "succeeded"
        assert observed_markers == [f"minerallens-live-job:v1:{job_id}\n"]
    finally:
        manager.shutdown()


def test_terminal_cleanup_refuses_an_invalid_ownership_marker(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    app, manager = _app(settings, lambda _values, _settings: FakeRedditClient())
    try:
        created = _request(
            app,
            "POST",
            "/api/v1/live/jobs",
            json=_provided_payload(),
            headers=_creation_headers("invalid-cleanup-marker"),
        ).json()
        job_id = created["job"]["id"]
        token = created["access_token"]
        assert _wait_for_terminal(app, job_id, token)["status"] == "succeeded"
        marker = settings.live_job_root / job_id / ".minerallens-live-job"
        marker.write_bytes(b"not-owned-by-minerallens\n")

        rejected = _request(
            app,
            "DELETE",
            f"/api/v1/live/jobs/{job_id}",
            headers={"X-Live-Job-Token": token},
        )
        assert rejected.status_code == 503
        assert (settings.live_job_root / job_id).is_dir()
        assert (
            _request(
                app,
                "GET",
                f"/api/v1/live/jobs/{job_id}",
                headers={"X-Live-Job-Token": token},
            ).status_code
            == 200
        )

        marker.write_bytes(f"minerallens-live-job:v1:{job_id}\n".encode())
        deleted = _request(
            app,
            "DELETE",
            f"/api/v1/live/jobs/{job_id}",
            headers={"X-Live-Job-Token": token},
        )
        assert deleted.status_code == 202
    finally:
        manager.shutdown()


def test_idle_expiry_sweeper_removes_terminal_job_and_stops_cleanly(tmp_path: Path) -> None:
    current_time = [datetime(2026, 8, 23, 12, tzinfo=UTC)]

    def clock() -> datetime:
        return current_time[0]

    settings = _settings(tmp_path, live_job_retention_seconds=1)
    manager = LiveJobManager(
        settings=settings,
        client_factory=lambda _values, _settings: FakeRedditClient(),
        clock=clock,
        sweep_interval_seconds=0.01,
    )
    app = create_app(
        repository=SyntheticReadRepository(),
        asset_root=Path("missing-web-assets"),
        settings=settings,
        live_job_manager=manager,
    )
    try:
        created = _request(
            app,
            "POST",
            "/api/v1/live/jobs",
            json=_provided_payload(),
            headers=_creation_headers("idle-expiry"),
        ).json()
        job_id = created["job"]["id"]
        token = created["access_token"]
        terminal = _wait_for_terminal(app, job_id, token)
        job_directory = settings.live_job_root / job_id
        assert job_directory.is_dir()
        assert terminal["expires_at"] is not None

        current_time[0] = datetime.fromisoformat(terminal["expires_at"]) + timedelta(seconds=1)
        deadline = time.monotonic() + 2
        while job_directory.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert not job_directory.exists()
        assert (
            _request(
                app,
                "GET",
                f"/api/v1/live/jobs/{job_id}",
                headers={"X-Live-Job-Token": token},
            ).status_code
            == 404
        )
    finally:
        manager.shutdown()
    assert not manager._sweeper_thread.is_alive()


def test_live_job_root_has_exclusive_single_process_ownership(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    manager = LiveJobManager(
        settings=settings,
        client_factory=lambda _values, _settings: FakeRedditClient(),
    )
    active_directory = settings.live_job_root / ("b" * 32)
    active_directory.mkdir()
    try:
        with pytest.raises(ConfigurationError, match="another application process"):
            LiveJobManager(
                settings=settings,
                client_factory=lambda _values, _settings: FakeRedditClient(),
            )
        assert active_directory.is_dir()
    finally:
        manager.shutdown()


def test_terminal_delete_retains_access_when_artifact_cleanup_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    fake = FakeRedditClient()
    app, manager = _app(settings, lambda _values, _settings: fake)
    try:
        created = _request(
            app,
            "POST",
            "/api/v1/live/jobs",
            json=_provided_payload(),
            headers=_creation_headers("cleanup-failure"),
        ).json()
        job_id = created["job"]["id"]
        token = created["access_token"]
        assert _wait_for_terminal(app, job_id, token)["status"] == "succeeded"
        original_rmtree = live_jobs_module.shutil.rmtree

        def fail_cleanup(_path: Path) -> None:
            raise PermissionError("simulated locked database")

        monkeypatch.setattr(live_jobs_module.shutil, "rmtree", fail_cleanup)
        failed = _request(
            app,
            "DELETE",
            f"/api/v1/live/jobs/{job_id}",
            headers={"X-Live-Job-Token": token},
        )
        assert failed.status_code == 503
        assert failed.json()["code"] == "live_job_cleanup_failed"
        assert (
            _request(
                app,
                "GET",
                f"/api/v1/live/jobs/{job_id}",
                headers={"X-Live-Job-Token": token},
            ).status_code
            == 200
        )

        monkeypatch.setattr(live_jobs_module.shutil, "rmtree", original_rmtree)
        deleted = _request(
            app,
            "DELETE",
            f"/api/v1/live/jobs/{job_id}",
            headers={"X-Live-Job-Token": token},
        )
        assert deleted.status_code == 202
    finally:
        manager.shutdown()


def test_app_lifespan_and_active_client_shutdown_release_root_ownership(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    owned_app = create_app(
        repository=SyntheticReadRepository(),
        asset_root=Path("missing-web-assets"),
        settings=settings,
    )

    async def run_lifespan() -> None:
        async with owned_app.router.lifespan_context(owned_app):
            assert owned_app.state.live_job_manager is not None

    asyncio.run(run_lifespan())
    replacement = LiveJobManager(
        settings=settings,
        client_factory=lambda _values, _settings: FakeRedditClient(),
    )
    replacement.shutdown()

    blocking = _BlockingRedditClient()
    app, manager = _app(settings, lambda _values, _settings: blocking)
    _request(
        app,
        "POST",
        "/api/v1/live/jobs",
        json=_provided_payload(),
        headers=_creation_headers("lifecycle-blocking"),
    )
    assert blocking.started.wait(timeout=2)
    blocking.release.set()
    started = time.monotonic()
    manager.shutdown()
    assert time.monotonic() - started < 2
    assert blocking.closed.is_set()
    assert not manager._sweeper_thread.is_alive()
    assert not any(
        child.is_dir() and len(child.name) == 32 for child in settings.live_job_root.iterdir()
    )

    after_worker = LiveJobManager(
        settings=settings,
        client_factory=lambda _values, _settings: FakeRedditClient(),
    )
    after_worker.shutdown()
