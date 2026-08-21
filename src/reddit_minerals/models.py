"""Validated domain and provider models."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

SQLITE_INTEGER_MIN = -(2**63)
SQLITE_INTEGER_MAX = 2**63 - 1

SqliteInteger = Annotated[int, Field(ge=SQLITE_INTEGER_MIN, le=SQLITE_INTEGER_MAX)]
SqliteNonnegativeInteger = Annotated[int, Field(ge=0, le=SQLITE_INTEGER_MAX)]


def utc_now() -> datetime:
    """Return an aware UTC timestamp."""

    return datetime.now(UTC)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


class StrictModel(BaseModel):
    """Default validation policy for data crossing package boundaries."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class ContentKind(StrEnum):
    POST = "post"
    COMMENT = "comment"


class WorkStatus(StrEnum):
    PENDING = "pending"
    PARTIAL = "partial"
    COMPLETE = "complete"
    RETRYABLE_FAILURE = "retryable_failure"
    PERMANENT_FAILURE = "permanent_failure"
    BLOCKED = "blocked"


class AnalysisKind(StrEnum):
    RELEVANCE = "relevance"
    ENRICHMENT = "enrichment"
    REPUTATION = "reputation"


class Sentiment(StrEnum):
    POSITIVE = "positive"
    NEGATIVE = "negative"
    NEUTRAL = "neutral"
    MIXED = "mixed"


class MiningStance(StrEnum):
    PRO_MINING = "pro-mining"
    ANTI_MINING = "anti-mining"
    NEUTRAL = "neutral"
    MIXED = "mixed"


class Credibility(StrEnum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    UNKNOWN = "unknown"


class MarketImpact(StrEnum):
    POSITIVE = "positive"
    NEGATIVE = "negative"
    NEUTRAL = "neutral"
    UNCLEAR = "unclear"


class ControversyLevel(StrEnum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class PostRecord(StrictModel):
    """Canonical representation of a Reddit submission."""

    id: Annotated[str, Field(min_length=1, max_length=32)]
    title: Annotated[str, Field(max_length=1000)] = ""
    selftext: Annotated[str, Field(max_length=1_000_000)] = ""
    subreddit: Annotated[str, Field(min_length=1, max_length=64)]
    created_at: datetime
    score: SqliteInteger = 0
    num_comments: SqliteNonnegativeInteger = 0
    upvote_ratio: Annotated[float | None, Field(ge=0, le=1)] = None
    permalink: Annotated[str, Field(max_length=2048)]
    fetched_at: datetime = Field(default_factory=utc_now)

    _utc_created_at = field_validator("created_at")(_ensure_utc)
    _utc_fetched_at = field_validator("fetched_at")(_ensure_utc)


class CommentRecord(StrictModel):
    """Canonical representation of a Reddit comment."""

    id: Annotated[str, Field(min_length=1, max_length=32)]
    post_id: Annotated[str, Field(min_length=1, max_length=32)]
    parent_id: Annotated[str | None, Field(max_length=64)] = None
    body: Annotated[str, Field(max_length=1_000_000)] = ""
    score: SqliteInteger = 0
    created_at: datetime
    depth: SqliteNonnegativeInteger = 0
    subreddit: Annotated[str, Field(min_length=1, max_length=64)]
    permalink: Annotated[str, Field(max_length=2048)]
    fetched_at: datetime = Field(default_factory=utc_now)

    _utc_created_at = field_validator("created_at")(_ensure_utc)
    _utc_fetched_at = field_validator("fetched_at")(_ensure_utc)


class AnalysisCandidateState(StrictModel):
    """Internal storage state used to reject stale analysis persistence."""

    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    kind: AnalysisKind
    schema_version: int | None = None
    prompt_version: str | None = None
    model: str | None = None
    relevance_threshold: Annotated[float, Field(ge=0, le=100, allow_inf_nan=False)]
    max_context_comments: Annotated[int, Field(ge=0, le=20)]
    max_content_chars: Annotated[int, Field(ge=500, le=100_000)]
    config_revision: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    input_revision: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    dependency_revision: Annotated[str | None, Field(pattern=r"^[0-9a-f]{64}$")] = None
    expected_analysis_revision: Annotated[str | None, Field(pattern=r"^[0-9a-f]{64}$")] = None
    base_result_revision: Annotated[str | None, Field(pattern=r"^[0-9a-f]{64}$")] = None


class ContentInput(StrictModel):
    """Text and metadata passed to an analysis provider."""

    kind: ContentKind
    content_id: str
    mineral: str
    title: str = ""
    body: str = ""
    subreddit: str
    score: int = 0
    upvote_ratio: float | None = None
    comment_context: list[str] = Field(default_factory=list, max_length=20)
    analysis_state: AnalysisCandidateState | None = Field(default=None, exclude=True, repr=False)


Percent = Annotated[float, Field(ge=0, le=100)]
UnitScore = Annotated[float, Field(ge=0, le=1)]


class ConcernScores(StrictModel):
    environment: UnitScore = 0
    health: UnitScore = 0
    working_conditions: UnitScore = 0
    child_labor: UnitScore = 0
    pollution: UnitScore = 0
    deforestation: UnitScore = 0
    biodiversity_loss: UnitScore = 0
    water_contamination: UnitScore = 0
    air_quality: UnitScore = 0
    government_policy: UnitScore = 0
    corruption: UnitScore = 0
    economic_benefits: UnitScore = 0
    local_employment: UnitScore = 0
    displacement: UnitScore = 0
    community_rights: UnitScore = 0
    indigenous_rights: UnitScore = 0
    waste_management: UnitScore = 0
    foreign_exploitation: UnitScore = 0
    sustainability: UnitScore = 0
    safety_regulations: UnitScore = 0


class RelevanceAnalysis(StrictModel):
    relevant: bool
    confidence: Percent
    rationale: Annotated[str, Field(min_length=1, max_length=1000)]
    matched_topics: list[Annotated[str, Field(max_length=80)]] = Field(
        default_factory=list, max_length=10
    )


class EnrichmentAnalysis(StrictModel):
    sentiment: Sentiment
    keywords: list[Annotated[str, Field(max_length=80)]] = Field(max_length=10)
    themes: list[Annotated[str, Field(max_length=120)]] = Field(max_length=8)
    concerns: ConcernScores = Field(default_factory=ConcernScores)
    mining_stance: MiningStance
    topic_classification: Annotated[str, Field(min_length=1, max_length=100)]
    relevance_score: UnitScore


class ReputationAnalysis(StrictModel):
    """Model-estimated indicators; these are not assertions of objective truth."""

    overall_reputation_score: Percent
    sentiment: Sentiment
    sentiment_score: Percent
    credibility: Credibility
    credibility_score: Percent
    market_impact: MarketImpact
    market_impact_score: Percent
    controversy_level: ControversyLevel
    rationale: Annotated[str, Field(min_length=1, max_length=1500)]
    evidence_signals: list[Annotated[str, Field(max_length=160)]] = Field(
        default_factory=list, max_length=10
    )


AnalysisModel = RelevanceAnalysis | EnrichmentAnalysis | ReputationAnalysis


class ProviderResult[AnalysisT: StrictModel](StrictModel):
    """Validated provider result plus non-sensitive operational metadata."""

    value: AnalysisT
    model: str
    provider_request_id: str | None = None
    input_tokens: Annotated[int | None, Field(ge=0)] = None
    output_tokens: Annotated[int | None, Field(ge=0)] = None
    latency_ms: Annotated[int, Field(ge=0)] = 0


class StatusSnapshot(StrictModel):
    schema_version: int
    posts: int
    comments: int
    mineral_posts: int
    mineral_comments: int
    tombstones_by_kind: dict[str, int]
    work_by_status: dict[str, int]
    analyses_by_kind_and_status: dict[str, int]
    runs_by_status: dict[str, int]
    recent_runs: list[dict[str, Any]]
