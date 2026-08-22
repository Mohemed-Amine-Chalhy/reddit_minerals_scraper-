from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from reddit_minerals.config import (
    MAX_MAPPING_BYTES,
    MAX_MAPPING_MINERALS,
    MAX_SUBREDDITS_PER_MINERAL,
    AppSettings,
    MappingReport,
    load_subreddit_mapping,
)
from reddit_minerals.errors import ConfigurationError
from reddit_minerals.models import (
    SQLITE_INTEGER_MAX,
    SQLITE_INTEGER_MIN,
    CommentRecord,
    ConcernScores,
    ContentInput,
    ContentKind,
    EnrichmentAnalysis,
    PostRecord,
    ProviderResult,
    RelevanceAnalysis,
    ReputationAnalysis,
)

_REDDIT_TEST_VALUE = "valid-test-reddit-value"
_GEMINI_TEST_VALUE = "valid-test-gemini-value"


def test_records_normalize_dates_and_strip_boundary_strings() -> None:
    post = PostRecord(
        id=" p1 ",
        subreddit=" mining ",
        created_at=datetime.fromisoformat("2026-01-01T00:00:00"),
        permalink=" /p1 ",
    )
    assert post.id == "p1"
    assert post.subreddit == "mining"
    assert post.created_at.tzinfo is UTC

    eastern = timezone(timedelta(hours=-5))
    comment = CommentRecord(
        id="c1",
        post_id="p1",
        subreddit="mining",
        created_at=datetime(2025, 12, 31, 19, tzinfo=eastern),
        permalink="/c1",
    )
    assert comment.created_at == datetime(2026, 1, 1, tzinfo=UTC)


def test_persisted_content_integers_are_bounded_to_sqlite_range() -> None:
    post = PostRecord(
        id="p1",
        subreddit="mining",
        created_at=datetime.now(UTC),
        permalink="/p1",
        score=SQLITE_INTEGER_MIN,
        num_comments=SQLITE_INTEGER_MAX,
    )
    comment = CommentRecord(
        id="c1",
        post_id="p1",
        subreddit="mining",
        created_at=datetime.now(UTC),
        permalink="/c1",
        score=SQLITE_INTEGER_MAX,
        depth=SQLITE_INTEGER_MAX,
    )
    assert post.score == SQLITE_INTEGER_MIN
    assert comment.score == SQLITE_INTEGER_MAX

    for model, values in (
        (PostRecord, {**post.model_dump(), "score": SQLITE_INTEGER_MIN - 1}),
        (PostRecord, {**post.model_dump(), "num_comments": SQLITE_INTEGER_MAX + 1}),
        (CommentRecord, {**comment.model_dump(), "score": SQLITE_INTEGER_MAX + 1}),
        (CommentRecord, {**comment.model_dump(), "depth": SQLITE_INTEGER_MAX + 1}),
    ):
        with pytest.raises(ValidationError):
            model.model_validate(values)


@pytest.mark.parametrize(
    ("model", "values"),
    [
        (
            PostRecord,
            {
                "id": "p",
                "subreddit": "mining",
                "created_at": datetime.now(UTC),
                "permalink": "/p",
                "num_comments": -1,
            },
        ),
        (
            CommentRecord,
            {
                "id": "c",
                "post_id": "p",
                "subreddit": "mining",
                "created_at": datetime.now(UTC),
                "permalink": "/c",
                "depth": -1,
            },
        ),
        (
            RelevanceAnalysis,
            {"relevant": True, "confidence": 101, "rationale": "why"},
        ),
        (
            EnrichmentAnalysis,
            {
                "sentiment": "neutral",
                "keywords": [],
                "themes": [],
                "mining_stance": "neutral",
                "topic_classification": "topic",
                "relevance_score": 1.1,
            },
        ),
        (
            ReputationAnalysis,
            {
                "overall_reputation_score": -1,
                "sentiment": "neutral",
                "sentiment_score": 50,
                "credibility": "unknown",
                "credibility_score": 50,
                "market_impact": "unclear",
                "market_impact_score": 50,
                "controversy_level": "low",
                "rationale": "why",
            },
        ),
    ],
)
def test_domain_models_reject_out_of_range_values(model: object, values: object) -> None:
    with pytest.raises(ValidationError):
        model.model_validate(values)  # type: ignore[union-attr]


def test_models_forbid_unknown_fields_and_bound_nested_concerns() -> None:
    with pytest.raises(ValidationError, match="extra"):
        ContentInput(
            kind=ContentKind.POST,
            content_id="p1",
            mineral="gold",
            subreddit="mining",
            unexpected=True,  # type: ignore[call-arg]
        )
    with pytest.raises(ValidationError):
        ConcernScores(environment=-0.01)


def test_provider_result_validates_operational_metadata() -> None:
    value = RelevanceAnalysis(relevant=True, confidence=80, rationale="direct mention")
    result = ProviderResult(value=value, model="offline", input_tokens=0, latency_ms=4)
    assert result.value is value
    with pytest.raises(ValidationError):
        ProviderResult(value=value, model="offline", output_tokens=-1)


def test_settings_normalize_log_level_and_reject_invalid_bounds() -> None:
    settings = AppSettings(log_level="warning")
    assert settings.log_level == "WARNING"
    with pytest.raises(ValidationError):
        AppSettings(log_level="verbose")
    with pytest.raises(ValidationError):
        AppSettings(max_retries=0)
    with pytest.raises(ValidationError):
        AppSettings(relevance_threshold=float("nan"))


@pytest.mark.parametrize(
    "values",
    [
        {
            "operation_timeout_seconds": 10,
            "reddit_request_timeout_seconds": 11,
            "gemini_request_timeout_seconds": 10,
        },
        {
            "operation_timeout_seconds": 10,
            "reddit_request_timeout_seconds": 10,
            "gemini_request_timeout_seconds": 11,
        },
    ],
)
def test_settings_require_request_timeouts_within_operation_budget(
    values: dict[str, float],
) -> None:
    with pytest.raises(ValidationError, match="must not exceed"):
        AppSettings(**values)


def test_settings_report_missing_provider_configuration() -> None:
    settings = AppSettings(
        reddit_client_id=None,
        reddit_client_secret=None,
        reddit_user_agent=None,
        gemini_api_key=None,
        gemini_model=None,
    )
    with pytest.raises(ConfigurationError, match="RMS_REDDIT_CLIENT_ID"):
        settings.require_reddit()
    with pytest.raises(ConfigurationError, match="RMS_GEMINI_API_KEY"):
        settings.require_gemini()
    assert settings.safe_summary()["reddit_configured"] is False
    assert settings.safe_summary()["gemini_configured"] is False


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("reddit_client_id", "replace-me"),
        ("reddit_client_secret", "YOUR_SECRET"),
        ("reddit_user_agent", "example-agent"),
    ],
)
def test_reddit_placeholder_values_are_rejected(field: str, value: str) -> None:
    values = {
        "reddit_client_id": "valid-client-id",
        "reddit_client_secret": _REDDIT_TEST_VALUE,
        "reddit_user_agent": "minerals-research/1.0 contact@test.invalid",
    }
    values[field] = value
    settings = AppSettings(**values)
    with pytest.raises(ConfigurationError, match=field.upper()):
        settings.require_reddit()
    assert settings.safe_summary()["reddit_configured"] is False


@pytest.mark.parametrize(
    ("field", "value"),
    [("gemini_api_key", "changeme"), ("gemini_model", "placeholder-model")],
)
def test_ai_placeholder_values_are_rejected(field: str, value: str) -> None:
    values = {
        "gemini_api_key": _GEMINI_TEST_VALUE,
        "gemini_model": "gemini-2.5-flash",
    }
    values[field] = value
    settings = AppSettings(**values)
    with pytest.raises(ConfigurationError, match=field.upper()):
        settings.require_gemini()
    assert settings.safe_summary()["gemini_configured"] is False


def test_valid_provider_settings_are_returned_but_secrets_are_not_summarized() -> None:
    settings = AppSettings(
        reddit_client_id="valid-client-id",
        reddit_client_secret=_REDDIT_TEST_VALUE,
        reddit_user_agent="minerals-research/1.0 contact@test.invalid",
        gemini_api_key=_GEMINI_TEST_VALUE,
        gemini_model="gemini-2.5-flash",
    )
    assert settings.require_reddit() == (
        "valid-client-id",
        _REDDIT_TEST_VALUE,
        "minerals-research/1.0 contact@test.invalid",
    )
    assert settings.require_gemini() == (_GEMINI_TEST_VALUE, "gemini-2.5-flash")
    rendered = json.dumps(settings.safe_summary())
    assert _REDDIT_TEST_VALUE not in rendered
    assert _GEMINI_TEST_VALUE not in rendered
    assert settings.safe_summary()["reddit_configured"] is True


def test_mapping_is_normalized_and_deduplicated(tmp_path: Path) -> None:
    path = tmp_path / "mapping.json"
    path.write_text(
        json.dumps(
            {
                " Gold ": ["Mining", "mining", "Geology"],
                "LITHIUM  ION": ["batteries", "Batteries", "energy"],
            }
        ),
        encoding="utf-8",
    )
    report = load_subreddit_mapping(path)
    assert report.mapping == {
        "gold": ("Mining", "Geology"),
        "lithium ion": ("batteries", "energy"),
    }
    assert report.duplicate_entries_removed == 2
    assert report.mineral_count == 2
    assert report.subreddit_count == 4


@pytest.mark.parametrize(
    "raw",
    [
        [],
        {},
        {"": ["mining"]},
        {"gold": []},
        {"gold": "mining"},
        {"gold": [1]},
        {"gold": ["r/mining"]},
        {"Gold": ["mining"], " gold ": ["geology"]},
    ],
)
def test_mapping_rejects_invalid_shapes(tmp_path: Path, raw: object) -> None:
    path = tmp_path / "mapping.json"
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ConfigurationError):
        load_subreddit_mapping(path)


def test_mapping_reports_missing_malformed_and_unreadable_files(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError, match="does not exist"):
        load_subreddit_mapping(tmp_path / "missing.json")

    malformed = tmp_path / "bad.json"
    malformed.write_text("{", encoding="utf-8")
    with pytest.raises(ConfigurationError, match="Cannot read"):
        load_subreddit_mapping(malformed)

    with pytest.raises(ConfigurationError, match="Cannot read"):
        load_subreddit_mapping(tmp_path)

    duplicate = tmp_path / "duplicate.json"
    duplicate.write_text('{"gold":["mining"],"gold":["geology"]}', encoding="utf-8")
    with pytest.raises(ConfigurationError, match="duplicate JSON object keys"):
        load_subreddit_mapping(duplicate)


def test_mapping_enforces_file_and_collection_safety_bounds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    oversized = tmp_path / "oversized.json"
    oversized.write_bytes(b" " * (MAX_MAPPING_BYTES + 1))
    with pytest.raises(ConfigurationError, match="byte safety limit"):
        load_subreddit_mapping(oversized)

    too_many_minerals = tmp_path / "too-many-minerals.json"
    too_many_minerals.write_text(
        json.dumps({f"mineral-{index}": ["aa"] for index in range(MAX_MAPPING_MINERALS + 1)}),
        encoding="utf-8",
    )
    with pytest.raises(ConfigurationError, match="mineral safety limit"):
        load_subreddit_mapping(too_many_minerals)

    too_many_subreddits = tmp_path / "too-many-subreddits.json"
    too_many_subreddits.write_text(
        json.dumps({"gold": [f"r{index}" for index in range(MAX_SUBREDDITS_PER_MINERAL + 1)]}),
        encoding="utf-8",
    )
    with pytest.raises(ConfigurationError, match="subreddit safety limit"):
        load_subreddit_mapping(too_many_subreddits)

    monkeypatch.setattr("reddit_minerals.config.MAX_MAPPING_SUBREDDIT_ENTRIES", 2)
    too_many_entries = tmp_path / "too-many-entries.json"
    too_many_entries.write_text(
        json.dumps({"gold": ["aa", "bb"], "silver": ["cc"]}),
        encoding="utf-8",
    )
    with pytest.raises(ConfigurationError, match="aggregate subreddit-entry safety limit"):
        load_subreddit_mapping(too_many_entries)


def test_mapping_validation_never_echoes_an_invalid_value(tmp_path: Path) -> None:
    sensitive_invalid_value = "private-value-that-must-not-reach-logs/invalid"
    path = tmp_path / "mapping.json"
    path.write_text(json.dumps({"gold": [sensitive_invalid_value]}), encoding="utf-8")

    with pytest.raises(ConfigurationError) as raised:
        load_subreddit_mapping(path)

    assert sensitive_invalid_value not in str(raised.value)


def test_mapping_report_forbids_unexpected_fields() -> None:
    with pytest.raises(ValidationError):
        MappingReport(
            mapping={"gold": ("mining",)},
            duplicate_entries_removed=0,
            mineral_count=1,
            subreddit_count=1,
            extra_field=True,  # type: ignore[call-arg]
        )
