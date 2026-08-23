from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi import FastAPI

from reddit_minerals.clients.gemini import GeminiAnalysisClient
from reddit_minerals.clients.reddit import PrawRedditClient
from reddit_minerals.web import create_app
from reddit_minerals.web.repository import (
    KAGGLE_SAMPLE_PATH,
    KaggleSampleReadRepository,
    SyntheticReadRepository,
)


def _request(app: FastAPI, method: str, path: str, **kwargs: Any) -> httpx.Response:
    async def send() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="https://portfolio.test",
        ) as client:
            return await client.request(method, path, **kwargs)

    return asyncio.run(send())


def _get(app: FastAPI, path: str, **kwargs: Any) -> httpx.Response:
    return _request(app, "GET", path, **kwargs)


def _synthetic_app(*, asset_root: Path | None = None) -> FastAPI:
    return create_app(repository=SyntheticReadRepository(), asset_root=asset_root)


def test_health_metadata_and_config_are_explicitly_synthetic_and_safe() -> None:
    app = _synthetic_app(asset_root=Path("missing-web-assets"))
    assert app.title == "MineralLens API"

    health = _get(app, "/api/v1/health")
    assert health.status_code == 200
    health_payload = health.json()
    assert health_payload["status"] == "healthy"
    assert health_payload["api_version"] == "v1"
    assert health_payload["mode"] == "synthetic-demo"
    assert health_payload["synthetic"] is True
    assert health_payload["public_sample"] is False
    assert health_payload["read_only"] is True
    assert health_payload["source"]["kind"] == "synthetic-demo"

    metadata = _get(app, "/api/v1/meta")
    assert metadata.status_code == 200
    payload = metadata.json()
    assert payload["application_name"] == "MineralLens"
    assert payload["dataset_label"] == "Synthetic Minerals Engineering Demo"
    assert payload["synthetic"] is True
    assert payload["public_sample"] is False
    assert "No record was collected from Reddit" in payload["dataset_description"]
    assert payload["minerals"] == ["cobalt", "copper", "gold", "graphite", "lithium", "nickel"]
    assert payload["totals"] == {
        "minerals": 6,
        "records": 12,
        "posts": 6,
        "comments": 6,
        "analyses": 24,
        "runs": 5,
    }

    config = _get(app, "/api/v1/config")
    assert config.status_code == 200
    config_payload = config.json()
    assert config_payload["read_only"] is True
    assert config_payload["providers_enabled"] is False
    assert config_payload["features"]["mutation"] is False
    assert config_payload["features"]["live_collection"] is False
    encoded = json.dumps(config_payload).casefold()
    for forbidden in ("api_key", "client_secret", "database_path", "reddit_client_id"):
        assert forbidden not in encoded


def test_default_app_exposes_the_bounded_public_kaggle_sample_with_provenance() -> None:
    app = create_app(asset_root=Path("missing-web-assets"))
    assert isinstance(app.state.read_repository, KaggleSampleReadRepository)

    health = _get(app, "/api/v1/health")
    assert health.status_code == 200
    health_payload = health.json()
    assert health_payload["mode"] == "public-research-sample"
    assert health_payload["synthetic"] is False
    assert health_payload["public_sample"] is True

    source = health_payload["source"]
    assert source["kind"] == "public-research-sample"
    assert source["source_url"] == (
        "https://www.kaggle.com/datasets/mohamedaminechalhy/reddit-mining-stance"
    )
    assert source["dataset_version"] == "2"
    assert source["archive_sha256"] == (
        "3A299CEC89CB091E9AD9E8F4158FD264A761C92BD9CA5B37B94924D99C3D7407"
    )
    assert source["license"] == "MIT"
    assert source["full_counts"] == {
        "minerals": 26,
        "records": 1_042_563,
        "posts": 15_779,
        "comments": 1_026_784,
    }
    assert source["sample_counts"] == {
        "minerals": 26,
        "records": 104,
        "posts": 52,
        "comments": 52,
    }
    assert source["published_date_range"] == {
        "start": "2008-02-19",
        "end": "2025-08-27",
    }
    assert source["raw_text_included"] is False
    assert source["authors_included"] is False

    metadata = _get(app, "/api/v1/meta")
    assert metadata.status_code == 200
    meta_payload = metadata.json()
    assert meta_payload["source"] == source
    assert meta_payload["totals"] == {
        "minerals": 26,
        "records": 104,
        "posts": 52,
        "comments": 52,
        "analyses": 156,
        "runs": 0,
    }
    assert len(meta_payload["minerals"]) == 26

    snapshot = _get(app, "/api/v1/snapshot", headers={"Accept-Encoding": "gzip"})
    assert snapshot.status_code == 200
    assert snapshot.headers["content-encoding"] == "gzip"
    assert snapshot.headers["cache-control"] == ("public, max-age=300, stale-while-revalidate=60")
    assert snapshot.headers["etag"].startswith('W/"')
    snapshot_payload = snapshot.json()
    assert snapshot_payload["source"] == source
    assert snapshot_payload["synthetic"] is False
    assert snapshot_payload["public_sample"] is True
    assert len(snapshot_payload["records"]) == 104
    assert {record["id"] for record in snapshot_payload["records"]} == {
        record.id for record in app.state.read_repository.snapshot().records
    }
    revalidated = _get(
        app,
        "/api/v1/snapshot",
        headers={"If-None-Match": snapshot.headers["etag"]},
    )
    assert revalidated.status_code == 304
    assert revalidated.content == b""

    records = _get(app, "/api/v1/records", params={"page_size": 50})
    assert records.status_code == 200
    records_payload = records.json()
    assert records_payload["source"] == source
    assert records_payload["total"] == 104
    assert records_payload["pages"] == 3
    assert len(records_payload["items"]) == 50
    assert all(item["synthetic"] is False for item in records_payload["items"])
    assert all(item["public_sample"] is True for item in records_payload["items"])
    assert all(item["content_available"] is False for item in records_payload["items"])

    record_id = records_payload["items"][0]["id"]
    detail = _get(app, f"/api/v1/records/{record_id}")
    assert detail.status_code == 200
    detail_payload = detail.json()
    assert detail_payload["id"] == record_id
    assert detail_payload["source"] == source
    assert detail_payload["synthetic"] is False
    assert detail_payload["public_sample"] is True
    assert detail_payload["content_available"] is False
    assert set(detail_payload["analysis"]) == {"relevance", "enrichment", "reputation"}
    assert detail_payload["analysis"]["relevance"] is None
    assert detail_payload["analysis"]["reputation"] is None
    assert "Original Reddit text is not included" in detail_payload["body"]

    runs = _get(app, "/api/v1/runs", params={"command": "scrape"})
    assert runs.status_code == 200
    assert runs.json() | {"source": "ignored"} == {
        "mode": "public-research-sample",
        "synthetic": False,
        "public_sample": True,
        "source": "ignored",
        "page": 1,
        "page_size": 10,
        "total": 0,
        "pages": 0,
        "items": [],
    }

    config = _get(app, "/api/v1/config")
    assert config.status_code == 200
    assert config.json()["source"] == source
    assert config.json()["features"]["run_history"] is False
    assert config.json()["providers_enabled"] is False


def test_kaggle_repository_rejects_source_boundary_drift(tmp_path: Path) -> None:
    payload = json.loads(KAGGLE_SAMPLE_PATH.read_text(encoding="utf-8"))
    payload["provenance"]["raw_text_included"] = True
    tampered = tmp_path / "tampered-kaggle-sample.json"
    tampered.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="raw_text_included"):
        KaggleSampleReadRepository(sample_path=tampered)


def test_dashboard_aggregates_all_six_minerals_and_supports_filtering() -> None:
    app = _synthetic_app(asset_root=Path("missing-web-assets"))

    response = _get(app, "/api/v1/dashboard")
    assert response.status_code == 200
    payload = response.json()
    assert payload["totals"]["minerals"] == 6
    assert payload["totals"]["records"] == 12
    assert len(payload["mineral_metrics"]) == 6
    assert len(payload["recent_records"]) == 6
    assert payload["top_concerns"][0]["average_score"] >= 0.8

    filtered = _get(app, "/api/v1/dashboard", params={"mineral": " Lithium "})
    assert filtered.status_code == 200
    filtered_payload = filtered.json()
    assert filtered_payload["selected_mineral"] == "lithium"
    assert filtered_payload["totals"]["records"] == 2
    assert filtered_payload["totals"]["analyses"] == 4
    assert [item["mineral"] for item in filtered_payload["mineral_metrics"]] == ["lithium"]


def test_record_browser_paginates_filters_searches_and_sorts() -> None:
    app = _synthetic_app(asset_root=Path("missing-web-assets"))

    first_page = _get(app, "/api/v1/records", params={"page_size": 5})
    assert first_page.status_code == 200
    page = first_page.json()
    assert page["total"] == 12
    assert page["pages"] == 3
    assert len(page["items"]) == 5
    assert all(item["synthetic"] is True for item in page["items"])
    graphite_comment = page["items"][0]
    assert graphite_comment["kind"] == "comment"
    assert graphite_comment["title"] is None
    assert graphite_comment["comment_count"] is None
    assert graphite_comment["relevance_confidence"] is None
    assert graphite_comment["reputation_score"] is None
    assert graphite_comment["controversy"] is None

    graphite_post = page["items"][1]
    assert graphite_post["kind"] == "post"
    assert graphite_post["parent_id"] is None

    comments = _get(app, "/api/v1/records", params={"kind": "comment"})
    assert comments.status_code == 200
    assert comments.json()["total"] == 6
    assert {item["kind"] for item in comments.json()["items"]} == {"comment"}

    cobalt = _get(app, "/api/v1/records", params={"mineral": "cobalt"})
    assert cobalt.status_code == 200
    assert cobalt.json()["total"] == 2
    assert {item["mineral"] for item in cobalt.json()["items"]} == {"cobalt"}

    search = _get(app, "/api/v1/records", params={"q": "watershed"})
    assert search.status_code == 200
    assert search.json()["total"] == 1
    assert search.json()["items"][0]["id"] == "synthetic-lithium-post"

    sorted_records = _get(app, "/api/v1/records", params={"sort": "score", "page_size": 1})
    assert sorted_records.status_code == 200
    assert sorted_records.json()["items"][0]["id"] == "synthetic-lithium-post"

    reputation = _get(
        app,
        "/api/v1/records",
        params={"sort": "reputation", "page_size": 1},
    )
    assert reputation.status_code == 200
    assert reputation.json()["items"][0]["id"] == "synthetic-gold-post"


def test_record_detail_is_rich_and_missing_record_error_is_sanitized() -> None:
    app = _synthetic_app(asset_root=Path("missing-web-assets"))

    response = _get(app, "/api/v1/records/synthetic-gold-post")
    assert response.status_code == 200
    payload = response.json()
    assert payload["kind"] == "post"
    assert payload["analysis"]["relevance"]["confidence"] == 97
    assert payload["analysis"]["reputation"]["score"] == 82
    assert payload["synthetic"] is True
    assert "not collected from Reddit" in payload["source_note"]

    comment = _get(app, "/api/v1/records/synthetic-gold-comment")
    assert comment.status_code == 200
    comment_payload = comment.json()
    assert comment_payload["title"] is None
    assert comment_payload["comment_count"] is None
    assert comment_payload["analysis"]["relevance"] is None
    assert comment_payload["analysis"]["reputation"] is None

    missing_id = "credential-looking-but-missing-record"
    missing = _get(app, f"/api/v1/records/{missing_id}")
    assert missing.status_code == 404
    assert missing.json() == {
        "code": "not_found",
        "message": "The requested resource was not found.",
        "issues": [],
    }
    assert missing_id not in missing.text


def test_run_history_supports_pagination_and_closed_filters() -> None:
    app = _synthetic_app(asset_root=Path("missing-web-assets"))

    response = _get(app, "/api/v1/runs", params={"page_size": 2})
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 5
    assert payload["pages"] == 3
    assert len(payload["items"]) == 2

    scrape = _get(app, "/api/v1/runs", params={"command": "SCRAPE"})
    assert scrape.status_code == 200
    assert scrape.json()["total"] == 1
    assert scrape.json()["items"][0]["command"] == "scrape"

    succeeded = _get(app, "/api/v1/runs", params={"status": "succeeded"})
    assert succeeded.status_code == 200
    assert succeeded.json()["total"] == 5


@pytest.mark.parametrize(
    ("path", "forbidden_value"),
    [
        ("/api/v1/records?page=0", "0"),
        ("/api/v1/records?page_size=999", "999"),
        ("/api/v1/records?kind=private-message", "private-message"),
        ("/api/v1/dashboard?mineral=not-a-demo-mineral", "not-a-demo-mineral"),
        ("/api/v1/runs?command=not-a-command", "not-a-command"),
    ],
)
def test_invalid_parameters_return_sanitized_422(path: str, forbidden_value: str) -> None:
    response = _get(_synthetic_app(asset_root=Path("missing-web-assets")), path)

    assert response.status_code == 422
    payload = response.json()
    assert payload["code"] in {"validation_error", "unsupported_filter"}
    assert (
        "invalid" in payload["message"].casefold() or "unavailable" in payload["message"].casefold()
    )
    assert forbidden_value not in response.text


def test_app_never_constructs_network_providers(monkeypatch: pytest.MonkeyPatch) -> None:
    def reject_construction(*_args: object, **_kwargs: object) -> None:
        pytest.fail("read-only web application attempted to construct a provider")

    monkeypatch.setattr(PrawRedditClient, "__init__", reject_construction)
    monkeypatch.setattr(GeminiAnalysisClient, "__init__", reject_construction)

    app = create_app(asset_root=Path("missing-web-assets"))
    for path in (
        "/api/v1/health",
        "/api/v1/meta",
        "/api/v1/dashboard",
        "/api/v1/records",
        "/api/v1/runs",
        "/api/v1/config",
    ):
        assert _get(app, path).status_code == 200


def test_openapi_contract_contains_only_read_operations() -> None:
    response = _get(_synthetic_app(asset_root=Path("missing-web-assets")), "/api/v1/openapi.json")

    assert response.status_code == 200
    paths = response.json()["paths"]
    assert set(paths) == {
        "/api/v1/config",
        "/api/v1/dashboard",
        "/api/v1/health",
        "/api/v1/meta",
        "/api/v1/records",
        "/api/v1/records/{record_id}",
        "/api/v1/runs",
        "/api/v1/snapshot",
    }
    assert all(set(operations) == {"get"} for operations in paths.values())


def test_spa_assets_are_optional_and_never_shadow_api_routes(tmp_path: Path) -> None:
    missing_assets = tmp_path / "missing"
    api_only = _synthetic_app(asset_root=missing_assets)
    root_response = _get(api_only, "/")
    assert root_response.status_code == 404
    assert root_response.headers["content-type"].startswith("application/json")

    asset_root = tmp_path / "dist"
    assets = asset_root / "assets"
    assets.mkdir(parents=True)
    (asset_root / "index.html").write_text(
        "<!doctype html><title>Minerals SPA</title><main id='root'></main>",
        encoding="utf-8",
    )
    (assets / "app.js").write_text("globalThis.syntheticDemo = true;", encoding="utf-8")
    app = _synthetic_app(asset_root=asset_root)

    assert "Minerals SPA" in _get(app, "/").text
    assert "Minerals SPA" in _get(app, "/dashboard/gold").text
    asset_response = _get(app, "/assets/app.js")
    assert "syntheticDemo" in asset_response.text
    assert asset_response.headers["cache-control"] == "public, max-age=31536000, immutable"
    assert _get(app, "/").headers["cache-control"] == "no-cache"
    assert _get(app, "/api/v1/health").headers["content-type"].startswith("application/json")
    missing_api = _get(app, "/api/v1/not-real")
    assert missing_api.status_code == 404
    assert missing_api.headers["content-type"].startswith("application/json")
    assert "Minerals SPA" not in missing_api.text


def test_spa_asset_root_precedence_is_explicit_then_environment_then_working_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    explicit = tmp_path / "explicit"
    configured = tmp_path / "configured"
    working = tmp_path / "web" / "dist"
    for directory, title in (
        (explicit, "Explicit SPA"),
        (configured, "Configured SPA"),
        (working, "Working-directory SPA"),
    ):
        directory.mkdir(parents=True)
        (directory / "index.html").write_text(title, encoding="utf-8")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("RMS_WEB_ASSET_DIR", str(configured))
    assert "Explicit SPA" in _get(_synthetic_app(asset_root=explicit), "/").text
    assert "Configured SPA" in _get(_synthetic_app(), "/").text

    monkeypatch.delenv("RMS_WEB_ASSET_DIR")
    assert "Working-directory SPA" in _get(_synthetic_app(), "/").text


def test_create_app_accepts_an_injected_repository_boundary() -> None:
    app = _synthetic_app(asset_root=Path("missing-web-assets"))
    repository = app.state.read_repository
    delegated = create_app(repository=repository, asset_root=Path("missing-web-assets"))

    assert _get(delegated, "/api/v1/meta").json()["totals"]["records"] == 12
