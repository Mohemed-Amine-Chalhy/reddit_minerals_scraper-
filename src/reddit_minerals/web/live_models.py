"""Strict request and response contracts for opt-in live Reddit collection."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal, Self

from pydantic import ConfigDict, Field, SecretStr, field_validator, model_validator

from reddit_minerals.models import ContentKind, StrictModel, WorkStatus
from reddit_minerals.web.models import ApiModel

MAX_LIVE_TARGETS = 10
MAX_LIVE_SUBREDDITS_PER_TARGET = 20
MAX_LIVE_POSTS_PER_MINERAL = 100
MAX_LIVE_COMMENTS_PER_POST = 500
MAX_LIVE_RECORDS_PER_JOB = 10_000


class LiveCredentialMode(StrEnum):
    """Credential source selected for one job."""

    SERVER = "server"
    PROVIDED = "provided"


class LiveTimeFilter(StrEnum):
    """Finite Reddit listing windows accepted by PRAW."""

    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    YEAR = "year"
    ALL = "all"


class LiveJobStatus(StrEnum):
    """Externally visible lifecycle for an isolated collection job."""

    QUEUED = "queued"
    RUNNING = "running"
    CANCEL_REQUESTED = "cancel_requested"
    CANCELLED = "cancelled"
    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    FAILED = "failed"


class LiveJobStage(StrEnum):
    """Coarse progress stage suitable for polling clients."""

    QUEUED = "queued"
    SEARCHING = "searching"
    COLLECTING = "collecting"
    FINALIZING = "finalizing"
    COMPLETE = "complete"


class LiveProvidedCredentials(StrictModel):
    """Write-only credentials accepted for one submitted job.

    Secret values use ``SecretStr`` and every field is hidden from ``repr``.  The
    job manager extracts them into a short-lived worker value and never stores
    this request model in job metadata.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    client_id: Annotated[SecretStr, Field(min_length=1, max_length=256, repr=False)]
    client_secret: Annotated[SecretStr, Field(min_length=1, max_length=512, repr=False)]
    user_agent: Annotated[str, Field(min_length=10, max_length=256, repr=False)]


class LiveScrapeTarget(StrictModel):
    """One normalized mineral query and its explicitly selected subreddits."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, frozen=True)

    mineral: Annotated[str, Field(min_length=1, max_length=128)]
    subreddits: Annotated[
        tuple[Annotated[str, Field(pattern=r"^[A-Za-z0-9_]{2,64}$")], ...],
        Field(min_length=1, max_length=MAX_LIVE_SUBREDDITS_PER_TARGET),
    ]

    @field_validator("mineral")
    @classmethod
    def normalize_mineral(cls, value: str) -> str:
        return " ".join(value.casefold().split())

    @field_validator("subreddits")
    @classmethod
    def deduplicate_subreddits(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        unique: list[str] = []
        seen: set[str] = set()
        for value in values:
            key = value.casefold()
            if key not in seen:
                seen.add(key)
                unique.append(value)
        return tuple(unique)


class LiveJobCreateRequest(StrictModel):
    """Bounded live scrape request; credential values are never a response field."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    targets: Annotated[
        tuple[LiveScrapeTarget, ...],
        Field(min_length=1, max_length=MAX_LIVE_TARGETS),
    ]
    time_filter: LiveTimeFilter = LiveTimeFilter.WEEK
    max_posts_per_mineral: Annotated[
        int,
        Field(ge=1, le=MAX_LIVE_POSTS_PER_MINERAL),
    ] = 10
    max_comments_per_post: Annotated[
        int,
        Field(ge=0, le=MAX_LIVE_COMMENTS_PER_POST),
    ] = 25
    credential_mode: LiveCredentialMode = LiveCredentialMode.SERVER
    credentials: Annotated[LiveProvidedCredentials | None, Field(default=None, repr=False)]

    @model_validator(mode="after")
    def validate_request_shape(self) -> Self:
        minerals = [target.mineral for target in self.targets]
        if len(set(minerals)) != len(minerals):
            raise ValueError("each mineral may appear in only one target")
        if self.credential_mode is LiveCredentialMode.PROVIDED and self.credentials is None:
            raise ValueError("credentials are required when credential_mode is provided")
        if self.credential_mode is LiveCredentialMode.SERVER and self.credentials is not None:
            raise ValueError("credentials must be omitted when credential_mode is server")
        estimated_records = (
            len(self.targets) * self.max_posts_per_mineral * (1 + self.max_comments_per_post)
        )
        if estimated_records > MAX_LIVE_RECORDS_PER_JOB:
            raise ValueError(
                "the requested targets and limits exceed the per-job record safety budget"
            )
        return self


class LiveJobDefaults(ApiModel):
    time_filter: LiveTimeFilter
    max_posts_per_mineral: Annotated[int, Field(ge=1)]
    max_comments_per_post: Annotated[int, Field(ge=0)]


class LiveJobLimits(ApiModel):
    max_targets: Annotated[int, Field(ge=1)]
    max_subreddits_per_target: Annotated[int, Field(ge=1)]
    max_posts_per_mineral: Annotated[int, Field(ge=1)]
    max_comments_per_post: Annotated[int, Field(ge=0)]
    max_records_per_job: Annotated[int, Field(ge=1)]
    max_active_jobs: Annotated[int, Field(ge=1)]
    retention_seconds: Annotated[int, Field(ge=1)]


class LiveCapabilitiesResponse(ApiModel):
    enabled: bool
    provider: Literal["reddit"] = "reddit"
    library: Literal["PRAW"] = "PRAW"
    server_credentials_configured: bool
    byo_credentials_allowed: bool
    credential_modes: tuple[LiveCredentialMode, ...]
    creation_access_token_required: Literal[True] = True
    creation_access_token_header: Literal["X-Live-Access-Token"] = "X-Live-Access-Token"  # noqa: S105
    access_token_header: Literal["X-Live-Job-Token"] = "X-Live-Job-Token"  # noqa: S105
    time_filters: tuple[LiveTimeFilter, ...]
    defaults: LiveJobDefaults
    limits: LiveJobLimits


class LiveJobProgress(ApiModel):
    minerals_total: Annotated[int, Field(ge=1)]
    minerals_completed: Annotated[int, Field(ge=0)] = 0
    subreddits_total: Annotated[int, Field(ge=1)]
    subreddits_completed: Annotated[int, Field(ge=0)] = 0
    posts_discovered: Annotated[int, Field(ge=0)] = 0
    posts_stored: Annotated[int, Field(ge=0)] = 0
    posts_failed: Annotated[int, Field(ge=0)] = 0
    comments_stored: Annotated[int, Field(ge=0)] = 0
    searches_failed: Annotated[int, Field(ge=0)] = 0


class LiveJobError(ApiModel):
    code: Annotated[str, Field(min_length=1, max_length=80)]
    message: Annotated[str, Field(min_length=1, max_length=240)]


class LiveJobView(ApiModel):
    id: Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]
    status: LiveJobStatus
    stage: LiveJobStage
    credential_mode: LiveCredentialMode
    targets: tuple[LiveScrapeTarget, ...]
    time_filter: LiveTimeFilter
    max_posts_per_mineral: Annotated[int, Field(ge=1)]
    max_comments_per_post: Annotated[int, Field(ge=0)]
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    expires_at: datetime | None = None
    progress: LiveJobProgress
    record_count: Annotated[int, Field(ge=0)] = 0
    message: Annotated[str, Field(min_length=1, max_length=240)]
    error: LiveJobError | None = None


class LiveJobCreatedResponse(ApiModel):
    job: LiveJobView
    access_token: Annotated[
        str,
        Field(
            min_length=43,
            max_length=43,
            pattern=r"^[A-Za-z0-9_-]{43}$",
            repr=False,
        ),
    ]


class LiveRecord(ApiModel):
    """Raw Reddit record held only inside one authenticated live-job snapshot."""

    id: Annotated[str, Field(min_length=1, max_length=32)]
    kind: ContentKind
    post_id: Annotated[str | None, Field(max_length=32)] = None
    parent_id: Annotated[str | None, Field(max_length=64)] = None
    depth: Annotated[int | None, Field(ge=0)] = None
    mineral: Annotated[str, Field(min_length=1, max_length=128)]
    title: Annotated[str | None, Field(max_length=1_000)] = None
    body: Annotated[str, Field(max_length=1_000_000)]
    subreddit: Annotated[str, Field(min_length=1, max_length=64)]
    created_at: datetime
    fetched_at: datetime
    score: int
    comment_count: Annotated[int | None, Field(ge=0)] = None
    upvote_ratio: Annotated[float | None, Field(ge=0, le=1)] = None
    permalink: Annotated[str, Field(max_length=2_048)]
    scrape_status: WorkStatus

    @model_validator(mode="after")
    def content_shape_matches_kind(self) -> Self:
        if self.kind is ContentKind.POST:
            if self.post_id is not None or self.parent_id is not None or self.depth is not None:
                raise ValueError("post records cannot declare comment relationships")
            if self.title is None or self.comment_count is None:
                raise ValueError("post records require title and comment_count")
            return self
        if self.post_id is None or self.depth is None:
            raise ValueError("comment records require post_id and depth")
        if (
            self.title is not None
            or self.comment_count is not None
            or self.upvote_ratio is not None
        ):
            raise ValueError("comment records cannot declare post-only fields")
        return self


class LiveSnapshotResponse(ApiModel):
    job_id: Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]
    status: LiveJobStatus
    generated_at: datetime
    records: Annotated[tuple[LiveRecord, ...], Field(max_length=MAX_LIVE_RECORDS_PER_JOB)]
