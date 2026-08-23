"""Strict public models for the read-only web API."""

from __future__ import annotations

from datetime import date, datetime
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import ConfigDict, Field, model_validator

from reddit_minerals.models import (
    ContentKind,
    ControversyLevel,
    MiningStance,
    Sentiment,
    StrictModel,
)

ApiVersion = Literal["v1"]


class ApiModel(StrictModel):
    """Immutable API model with an explicit, closed response shape."""

    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class DatasetKind(StrEnum):
    """Origin of the bounded dataset exposed by the read-only API."""

    PUBLIC_RESEARCH_SAMPLE = "public-research-sample"
    SYNTHETIC_DEMO = "synthetic-demo"


class DatasetCounts(ApiModel):
    """Content counts reported by a source or selected sample."""

    minerals: Annotated[int, Field(ge=0)]
    records: Annotated[int, Field(ge=0)]
    posts: Annotated[int, Field(ge=0)]
    comments: Annotated[int, Field(ge=0)]

    @model_validator(mode="after")
    def records_equal_posts_and_comments(self) -> DatasetCounts:
        if self.records != self.posts + self.comments:
            raise ValueError("records must equal posts plus comments")
        return self


class DatasetDateRange(ApiModel):
    """Inclusive date range published for the complete source dataset."""

    start: date
    end: date

    @model_validator(mode="after")
    def starts_before_it_ends(self) -> DatasetDateRange:
        if self.start > self.end:
            raise ValueError("dataset date range start must not be after end")
        return self


class DatasetProvenance(ApiModel):
    """Public, non-sensitive provenance attached to every API surface."""

    kind: DatasetKind
    public_sample: bool
    dataset_label: Annotated[str, Field(min_length=1, max_length=160)]
    dataset_description: Annotated[str, Field(min_length=1, max_length=800)]
    owner_name: Annotated[str | None, Field(max_length=160)] = None
    dataset_ref: Annotated[str | None, Field(max_length=240)] = None
    source_url: Annotated[
        str | None,
        Field(max_length=2048, pattern=r"^https://"),
    ] = None
    dataset_version: Annotated[str, Field(min_length=1, max_length=80)]
    archive_sha256: Annotated[
        str | None,
        Field(pattern=r"^[0-9A-Fa-f]{64}$"),
    ] = None
    license: Annotated[str | None, Field(max_length=120)] = None
    published_at: datetime | None = None
    source_note: Annotated[str, Field(min_length=1, max_length=600)]
    full_counts: DatasetCounts
    sample_counts: DatasetCounts
    published_date_range: DatasetDateRange | None = None
    sample_method: Annotated[str, Field(min_length=1, max_length=600)]
    raw_text_included: bool
    authors_included: bool

    @model_validator(mode="after")
    def facts_are_consistent(self) -> DatasetProvenance:
        is_public_sample = self.kind is DatasetKind.PUBLIC_RESEARCH_SAMPLE
        if self.public_sample is not is_public_sample:
            raise ValueError("public_sample must match the dataset kind")
        if self.sample_counts.records > self.full_counts.records:
            raise ValueError("sample records cannot exceed full dataset records")
        if self.sample_counts.posts > self.full_counts.posts:
            raise ValueError("sample posts cannot exceed full dataset posts")
        if self.sample_counts.comments > self.full_counts.comments:
            raise ValueError("sample comments cannot exceed full dataset comments")
        if self.sample_counts.minerals > self.full_counts.minerals:
            raise ValueError("sample minerals cannot exceed full dataset minerals")
        if is_public_sample:
            required_public_fields = (
                self.owner_name,
                self.dataset_ref,
                self.source_url,
                self.archive_sha256,
                self.license,
                self.published_at,
                self.published_date_range,
            )
            if any(value is None for value in required_public_fields):
                raise ValueError("public sample provenance is incomplete")
            if self.raw_text_included or self.authors_included:
                raise ValueError("the public research sample cannot expose raw text or authors")
        return self


class SourceAwareApiModel(ApiModel):
    """API model whose source flags are validated against its provenance."""

    mode: DatasetKind
    synthetic: bool
    public_sample: bool
    source: DatasetProvenance

    @model_validator(mode="after")
    def source_flags_are_consistent(self) -> SourceAwareApiModel:
        if self.mode is not self.source.kind:
            raise ValueError("mode must match source kind")
        if self.public_sample is not self.source.public_sample:
            raise ValueError("public_sample must match source provenance")
        expected_synthetic = self.source.kind is DatasetKind.SYNTHETIC_DEMO
        if self.synthetic is not expected_synthetic:
            raise ValueError("synthetic must match source kind")
        return self


class HealthResponse(SourceAwareApiModel):
    status: Literal["healthy"] = "healthy"
    api_version: ApiVersion = "v1"
    read_only: Literal[True] = True
    checked_at: datetime


class DatasetTotals(ApiModel):
    minerals: Annotated[int, Field(ge=0)]
    records: Annotated[int, Field(ge=0)]
    posts: Annotated[int, Field(ge=0)]
    comments: Annotated[int, Field(ge=0)]
    analyses: Annotated[int, Field(ge=0)]
    runs: Annotated[int, Field(ge=0)]


class MetaResponse(SourceAwareApiModel):
    api_version: ApiVersion = "v1"
    application_name: str
    application_version: str
    dataset_label: str
    dataset_description: str
    read_only: Literal[True] = True
    generated_at: datetime
    minerals: tuple[str, ...]
    totals: DatasetTotals


class ConcernSignal(ApiModel):
    name: Annotated[str, Field(min_length=1, max_length=80)]
    score: Annotated[float, Field(ge=0, le=1, allow_inf_nan=False)]


class RelevanceSnapshot(ApiModel):
    relevant: bool
    confidence: Annotated[float, Field(ge=0, le=100, allow_inf_nan=False)]
    rationale: Annotated[str, Field(min_length=1, max_length=600)]


class EnrichmentSnapshot(ApiModel):
    sentiment: Sentiment
    stance: MiningStance
    keywords: tuple[Annotated[str, Field(min_length=1, max_length=80)], ...]
    themes: tuple[Annotated[str, Field(min_length=1, max_length=120)], ...]
    concerns: tuple[ConcernSignal, ...]


class ReputationSnapshot(ApiModel):
    score: Annotated[float, Field(ge=0, le=100, allow_inf_nan=False)]
    controversy: ControversyLevel
    rationale: Annotated[str, Field(min_length=1, max_length=600)]


class AnalysisSnapshot(ApiModel):
    relevance: RelevanceSnapshot | None = None
    enrichment: EnrichmentSnapshot
    reputation: ReputationSnapshot | None = None


class RecordSummary(SourceAwareApiModel):
    id: Annotated[str, Field(min_length=1, max_length=128)]
    kind: ContentKind
    parent_id: Annotated[str | None, Field(max_length=128)] = None
    mineral: Annotated[str, Field(min_length=1, max_length=128)]
    topic_label: Annotated[str, Field(min_length=1, max_length=120)]
    title: Annotated[str | None, Field(max_length=1000)] = None
    body_preview: Annotated[str, Field(max_length=280)]
    subreddit: Annotated[str, Field(min_length=1, max_length=64)]
    created_at: datetime
    score: int
    comment_count: Annotated[int | None, Field(ge=0)] = None
    sentiment: Sentiment
    stance: MiningStance
    relevance_confidence: Annotated[float | None, Field(ge=0, le=100, allow_inf_nan=False)] = None
    reputation_score: Annotated[float | None, Field(ge=0, le=100, allow_inf_nan=False)] = None
    controversy: ControversyLevel | None = None
    themes: tuple[str, ...]
    content_available: bool


class RecordDetail(SourceAwareApiModel):
    id: Annotated[str, Field(min_length=1, max_length=128)]
    kind: ContentKind
    parent_id: Annotated[str | None, Field(max_length=128)] = None
    mineral: Annotated[str, Field(min_length=1, max_length=128)]
    topic_label: Annotated[str, Field(min_length=1, max_length=120)]
    title: Annotated[str | None, Field(max_length=1000)] = None
    body: Annotated[str, Field(max_length=20_000)]
    subreddit: Annotated[str, Field(min_length=1, max_length=64)]
    created_at: datetime
    score: int
    comment_count: Annotated[int | None, Field(ge=0)] = None
    analysis: AnalysisSnapshot
    source_note: Annotated[str, Field(min_length=1, max_length=600)]
    content_available: bool

    def summary(self) -> RecordSummary:
        """Return the compact representation used by collection endpoints."""

        relevance = self.analysis.relevance
        reputation = self.analysis.reputation
        body_preview = self.body if len(self.body) <= 277 else f"{self.body[:277]}…"
        return RecordSummary(
            mode=self.mode,
            synthetic=self.synthetic,
            public_sample=self.public_sample,
            source=self.source,
            id=self.id,
            kind=self.kind,
            parent_id=self.parent_id,
            mineral=self.mineral,
            topic_label=self.topic_label,
            title=self.title,
            body_preview=body_preview,
            subreddit=self.subreddit,
            created_at=self.created_at,
            score=self.score,
            comment_count=self.comment_count,
            sentiment=self.analysis.enrichment.sentiment,
            stance=self.analysis.enrichment.stance,
            relevance_confidence=relevance.confidence if relevance else None,
            reputation_score=reputation.score if reputation else None,
            controversy=reputation.controversy if reputation else None,
            themes=self.analysis.enrichment.themes,
            content_available=self.content_available,
        )


class SnapshotResponse(SourceAwareApiModel):
    """One bounded immutable dataset transfer for first-party web clients."""

    generated_at: datetime
    records: Annotated[tuple[RecordDetail, ...], Field(max_length=5_000)]


class RecordSort(StrEnum):
    NEWEST = "newest"
    SCORE = "score"
    REPUTATION = "reputation"


class RecordPage(SourceAwareApiModel):
    page: Annotated[int, Field(ge=1)]
    page_size: Annotated[int, Field(ge=1, le=50)]
    total: Annotated[int, Field(ge=0)]
    pages: Annotated[int, Field(ge=0)]
    items: tuple[RecordSummary, ...]


class LabelCount(ApiModel):
    label: str
    count: Annotated[int, Field(ge=0)]


class ConcernMetric(ApiModel):
    name: str
    average_score: Annotated[float, Field(ge=0, le=1, allow_inf_nan=False)]
    records: Annotated[int, Field(ge=0)]


class MineralMetric(ApiModel):
    mineral: str
    records: Annotated[int, Field(ge=0)]
    posts: Annotated[int, Field(ge=0)]
    comments: Annotated[int, Field(ge=0)]
    average_relevance: Annotated[float | None, Field(ge=0, le=100, allow_inf_nan=False)] = None
    average_reputation: Annotated[float | None, Field(ge=0, le=100, allow_inf_nan=False)] = None


class DashboardResponse(SourceAwareApiModel):
    selected_mineral: str | None
    totals: DatasetTotals
    sentiment_distribution: tuple[LabelCount, ...]
    stance_distribution: tuple[LabelCount, ...]
    top_concerns: tuple[ConcernMetric, ...]
    mineral_metrics: tuple[MineralMetric, ...]
    recent_records: tuple[RecordSummary, ...]


class RunStatus(StrEnum):
    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    FAILED = "failed"


class RunSummary(ApiModel):
    id: Annotated[str, Field(min_length=1, max_length=128)]
    command: Annotated[str, Field(min_length=1, max_length=40)]
    status: RunStatus
    started_at: datetime
    finished_at: datetime
    processed: Annotated[int, Field(ge=0)]
    failed: Annotated[int, Field(ge=0)]
    duration_ms: Annotated[int, Field(ge=0)]
    synthetic: bool


class RunPage(SourceAwareApiModel):
    page: Annotated[int, Field(ge=1)]
    page_size: Annotated[int, Field(ge=1, le=50)]
    total: Annotated[int, Field(ge=0)]
    pages: Annotated[int, Field(ge=0)]
    items: tuple[RunSummary, ...]


class FeatureFlags(ApiModel):
    dashboard: bool = True
    record_browser: bool = True
    run_history: bool = True
    live_collection: bool = False
    live_analysis: bool = False
    mutation: bool = False
    exports: bool = False


class PaginationConfig(ApiModel):
    default_page_size: Annotated[int, Field(ge=1, le=50)]
    maximum_page_size: Annotated[int, Field(ge=1, le=50)]


class FilterConfig(ApiModel):
    minerals: tuple[str, ...]
    content_kinds: tuple[ContentKind, ...]
    sentiments: tuple[Sentiment, ...]
    run_statuses: tuple[RunStatus, ...]
    record_sorts: tuple[RecordSort, ...]


class UiConfigResponse(SourceAwareApiModel):
    api_version: ApiVersion = "v1"
    api_base_path: Literal["/api/v1"] = "/api/v1"
    dataset_label: str
    read_only: Literal[True] = True
    providers_enabled: bool = False
    features: FeatureFlags
    pagination: PaginationConfig
    filters: FilterConfig


class ValidationIssue(ApiModel):
    field: str
    message: str
    type: str


class ErrorResponse(ApiModel):
    code: str
    message: str
    issues: tuple[ValidationIssue, ...] = ()
