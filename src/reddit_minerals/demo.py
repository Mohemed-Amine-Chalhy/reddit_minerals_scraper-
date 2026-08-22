"""Deterministic offline demonstration of the production pipeline boundaries."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import overload

from pydantic import Field

from reddit_minerals.export import export_database
from reddit_minerals.models import (
    AnalysisKind,
    CommentRecord,
    ConcernScores,
    ContentInput,
    ControversyLevel,
    Credibility,
    EnrichmentAnalysis,
    MarketImpact,
    MiningStance,
    PostRecord,
    ProviderResult,
    RelevanceAnalysis,
    ReputationAnalysis,
    Sentiment,
    StrictModel,
)
from reddit_minerals.services import AnalysisService, ScrapeService
from reddit_minerals.storage import Database

_DEMO_MINERAL = "gold"
_DEMO_SUBREDDIT = "mining"
_DEMO_MODEL = "synthetic-demo-v1"


class DemoArtifactLifecycle(StrEnum):
    """Whether the CLI preserves the generated demo workspace."""

    REMOVED_AFTER_COMMAND = "removed_after_command"
    RETAINED = "retained"


class DemoArtifacts(StrictModel):
    """Locations and lifetime of generated demonstration artifacts."""

    workspace: Path
    database: Path
    export: Path
    lifecycle: DemoArtifactLifecycle


class DemoCounts(StrictModel):
    """Canonical and exported record counts after the demonstration."""

    posts: int = Field(ge=0)
    comments: int = Field(ge=0)
    analyses: int = Field(ge=0)
    exported_records: int = Field(ge=0)


class DemoStages(StrictModel):
    """Successful work performed by each real pipeline service."""

    scraped_posts: int = Field(ge=0)
    relevance: int = Field(ge=0)
    enrichment: int = Field(ge=0)
    reputation: int = Field(ge=0)


class DemoSummary(StrictModel):
    """Concise, machine-readable result returned by the offline demo."""

    mode: str = "offline-synthetic"
    network_access: bool = False
    mineral: str = _DEMO_MINERAL
    database_schema: int = Field(ge=1)
    counts: DemoCounts
    stages: DemoStages
    artifacts: DemoArtifacts


@dataclass(frozen=True, slots=True)
class _CompleteCommentBatch(Sequence[CommentRecord]):
    comments: tuple[CommentRecord, ...]
    snapshot_complete: bool = True

    def __len__(self) -> int:
        return len(self.comments)

    @overload
    def __getitem__(self, index: int) -> CommentRecord: ...

    @overload
    def __getitem__(self, index: slice) -> tuple[CommentRecord, ...]: ...

    def __getitem__(self, index: int | slice) -> CommentRecord | tuple[CommentRecord, ...]:
        return self.comments[index]


class _SyntheticRedditClient:
    """In-memory Reddit adapter with fixed, public synthetic records."""

    def __init__(self) -> None:
        first_seen = datetime(2026, 1, 10, 9, 0, tzinfo=UTC)
        second_seen = datetime(2026, 1, 11, 14, 30, tzinfo=UTC)
        self._posts = (
            PostRecord(
                id="demo-gold-supply",
                title="Gold recycling strengthens supply resilience",
                selftext="A new refining process could recover more gold from electronics.",
                subreddit=_DEMO_SUBREDDIT,
                created_at=first_seen,
                score=128,
                num_comments=1,
                upvote_ratio=0.91,
                permalink="https://www.reddit.com/r/mining/comments/demo-gold-supply",
                fetched_at=first_seen + timedelta(hours=1),
            ),
            PostRecord(
                id="demo-gold-water",
                title="Water management remains central to responsible gold mining",
                selftext="Engineers discuss monitoring, treatment, and community reporting.",
                subreddit=_DEMO_SUBREDDIT,
                created_at=second_seen,
                score=84,
                num_comments=1,
                upvote_ratio=0.86,
                permalink="https://www.reddit.com/r/mining/comments/demo-gold-water",
                fetched_at=second_seen + timedelta(hours=1),
            ),
        )
        self._comments = {
            "demo-gold-supply": (
                CommentRecord(
                    id="demo-comment-recycling",
                    post_id="demo-gold-supply",
                    parent_id="t3_demo-gold-supply",
                    body="Closed-loop recovery can reduce pressure on primary supply.",
                    score=31,
                    created_at=first_seen + timedelta(minutes=20),
                    depth=0,
                    subreddit=_DEMO_SUBREDDIT,
                    permalink=(
                        "https://www.reddit.com/r/mining/comments/"
                        "demo-gold-supply/_/demo-comment-recycling"
                    ),
                    fetched_at=first_seen + timedelta(hours=1),
                ),
            ),
            "demo-gold-water": (
                CommentRecord(
                    id="demo-comment-water",
                    post_id="demo-gold-water",
                    parent_id="t3_demo-gold-water",
                    body="Transparent water-quality measurements help communities verify progress.",
                    score=22,
                    created_at=second_seen + timedelta(minutes=15),
                    depth=0,
                    subreddit=_DEMO_SUBREDDIT,
                    permalink=(
                        "https://www.reddit.com/r/mining/comments/"
                        "demo-gold-water/_/demo-comment-water"
                    ),
                    fetched_at=second_seen + timedelta(hours=1),
                ),
            ),
        }

    def search_posts(
        self,
        *,
        mineral: str,
        subreddit: str,
        limit: int,
        time_filter: str,
        deadline: float | None = None,
    ) -> Iterable[PostRecord]:
        del time_filter, deadline
        if mineral != _DEMO_MINERAL or subreddit != _DEMO_SUBREDDIT:
            return ()
        return self._posts[:limit]

    def fetch_comments(
        self, *, post_id: str, limit: int, deadline: float | None = None
    ) -> Sequence[CommentRecord]:
        del deadline
        comments = self._comments.get(post_id, ())
        return _CompleteCommentBatch(
            comments[:limit],
            snapshot_complete=limit >= len(comments),
        )


class _SyntheticAnalysisClient:
    """Schema-aware analysis adapter whose results depend only on synthetic input."""

    model = _DEMO_MODEL

    def analyze_relevance(self, content: ContentInput) -> ProviderResult[RelevanceAnalysis]:
        return ProviderResult(
            value=RelevanceAnalysis(
                relevant=True,
                confidence=96,
                rationale="The synthetic record directly discusses gold supply or mining.",
                matched_topics=["gold", "mining"],
            ),
            model=self.model,
            provider_request_id=f"demo-relevance-{content.content_id}",
            input_tokens=24,
            output_tokens=12,
            latency_ms=0,
        )

    def analyze_enrichment(self, content: ContentInput) -> ProviderResult[EnrichmentAnalysis]:
        discusses_water = "water" in f"{content.title} {content.body}".lower()
        concerns = (
            ConcernScores(water_contamination=0.82, sustainability=0.74)
            if discusses_water
            else ConcernScores(economic_benefits=0.71, sustainability=0.66)
        )
        return ProviderResult(
            value=EnrichmentAnalysis(
                sentiment=Sentiment.MIXED if discusses_water else Sentiment.POSITIVE,
                keywords=["gold", "water"] if discusses_water else ["gold", "recycling"],
                themes=["environmental monitoring"] if discusses_water else ["circular supply"],
                concerns=concerns,
                mining_stance=MiningStance.NEUTRAL,
                topic_classification=(
                    "environmental management" if discusses_water else "supply resilience"
                ),
                relevance_score=0.96,
            ),
            model=self.model,
            provider_request_id=f"demo-enrichment-{content.content_id}",
            input_tokens=36,
            output_tokens=20,
            latency_ms=0,
        )

    def analyze_reputation(self, content: ContentInput) -> ProviderResult[ReputationAnalysis]:
        discusses_water = "water" in f"{content.title} {content.body}".lower()
        return ProviderResult(
            value=ReputationAnalysis(
                overall_reputation_score=68 if discusses_water else 79,
                sentiment=Sentiment.MIXED if discusses_water else Sentiment.POSITIVE,
                sentiment_score=64 if discusses_water else 82,
                credibility=Credibility.MEDIUM,
                credibility_score=72,
                market_impact=MarketImpact.UNCLEAR,
                market_impact_score=50,
                controversy_level=(
                    ControversyLevel.MEDIUM if discusses_water else ControversyLevel.LOW
                ),
                rationale="The synthetic discussion contains concrete, reviewable signals.",
                evidence_signals=(
                    ["monitoring and community reporting"]
                    if discusses_water
                    else ["closed-loop recovery"]
                ),
            ),
            model=self.model,
            provider_request_id=f"demo-reputation-{content.content_id}",
            input_tokens=42,
            output_tokens=24,
            latency_ms=0,
        )


def run_offline_demo(
    workspace: Path,
    *,
    lifecycle: DemoArtifactLifecycle,
    protected_database_path: Path | None = None,
) -> DemoSummary:
    """Run the real pipeline over synthetic adapters inside an isolated workspace."""

    workspace = workspace.resolve()
    database_path = workspace / "demo.sqlite3"
    export_path = workspace / "gold.jsonl"
    if protected_database_path is not None and database_path == protected_database_path.resolve():
        raise ValueError("Demo database must not be the configured application database")
    workspace.mkdir(parents=True, exist_ok=True)
    if database_path.exists() or export_path.exists():
        raise ValueError(f"Demo workspace already contains generated artifacts: {workspace}")

    database = Database(database_path)
    database.initialize()
    scrape = ScrapeService(
        client=_SyntheticRedditClient(),
        database=database,
        max_retries=1,
        retry_base_delay_seconds=0,
        retry_max_delay_seconds=0,
        operation_timeout_seconds=30,
    ).run(
        mapping={_DEMO_MINERAL: (_DEMO_SUBREDDIT,)},
        minerals=[_DEMO_MINERAL],
        max_posts_per_mineral=2,
        max_comments_per_post=2,
        refresh_after=timedelta(0),
        time_filter="all",
        dry_run=False,
        force=False,
    )

    analysis = AnalysisService(
        client=_SyntheticAnalysisClient(),
        database=database,
        max_retries=1,
        retry_base_delay_seconds=0,
        retry_max_delay_seconds=0,
        operation_timeout_seconds=30,
        model=_DEMO_MODEL,
    )
    relevance = analysis.run(
        AnalysisKind.RELEVANCE,
        mineral=_DEMO_MINERAL,
        limit=10,
        force=False,
        relevance_threshold=70,
        max_context_comments=2,
    )
    enrichment = analysis.run(
        AnalysisKind.ENRICHMENT,
        mineral=_DEMO_MINERAL,
        limit=10,
        force=False,
        relevance_threshold=70,
        max_context_comments=2,
    )
    reputation = analysis.run(
        AnalysisKind.REPUTATION,
        mineral=_DEMO_MINERAL,
        limit=10,
        force=False,
        relevance_threshold=70,
        max_context_comments=2,
    )
    exported_records = export_database(
        database,
        output=export_path,
        format_name="jsonl",
        mineral=_DEMO_MINERAL,
    )
    status = database.status()
    analyses = sum(status.analyses_by_kind_and_status.values())
    return DemoSummary(
        database_schema=status.schema_version,
        counts=DemoCounts(
            posts=status.posts,
            comments=status.comments,
            analyses=analyses,
            exported_records=exported_records,
        ),
        stages=DemoStages(
            scraped_posts=scrape.posts_completed,
            relevance=relevance.completed,
            enrichment=enrichment.completed,
            reputation=reputation.completed,
        ),
        artifacts=DemoArtifacts(
            workspace=workspace,
            database=database_path,
            export=export_path,
            lifecycle=lifecycle,
        ),
    )
