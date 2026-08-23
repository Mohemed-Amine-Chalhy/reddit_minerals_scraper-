"""Deterministic, clearly synthetic records for the public web demonstration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from reddit_minerals.models import (
    ContentKind,
    ControversyLevel,
    MiningStance,
    Sentiment,
)
from reddit_minerals.web.models import (
    AnalysisSnapshot,
    ConcernSignal,
    DatasetCounts,
    DatasetDateRange,
    DatasetKind,
    DatasetProvenance,
    EnrichmentSnapshot,
    RecordDetail,
    RelevanceSnapshot,
    ReputationSnapshot,
    RunStatus,
    RunSummary,
)

DATASET_LABEL = "Synthetic Minerals Engineering Demo"
DATASET_DESCRIPTION = (
    "Deterministic fictional discussions generated for this portfolio interface. "
    "No record was collected from Reddit and no provider credentials are used."
)
DATASET_GENERATED_AT = datetime(2026, 8, 23, 9, 0, tzinfo=UTC)
SOURCE_NOTE = "Synthetic portfolio demo record; it was not collected from Reddit."
SYNTHETIC_PROVENANCE = DatasetProvenance(
    kind=DatasetKind.SYNTHETIC_DEMO,
    public_sample=False,
    dataset_label=DATASET_LABEL,
    dataset_description=DATASET_DESCRIPTION,
    dataset_version="1",
    source_note=SOURCE_NOTE,
    full_counts=DatasetCounts(minerals=6, records=12, posts=6, comments=6),
    sample_counts=DatasetCounts(minerals=6, records=12, posts=6, comments=6),
    published_date_range=DatasetDateRange(
        start=DATASET_GENERATED_AT.date() - timedelta(days=12),
        end=DATASET_GENERATED_AT.date(),
    ),
    sample_method="Six deterministic fictional scenarios with one post and one comment each.",
    raw_text_included=False,
    authors_included=False,
)


@dataclass(frozen=True, slots=True)
class _Scenario:
    mineral: str
    subreddit: str
    title: str
    body: str
    comment: str
    sentiment: Sentiment
    comment_sentiment: Sentiment
    stance: MiningStance
    confidence: float
    reputation: float
    controversy: ControversyLevel
    score: int
    themes: tuple[str, ...]
    keywords: tuple[str, ...]
    concerns: tuple[tuple[str, float], ...]


_SCENARIOS = (
    _Scenario(
        mineral="gold",
        subreddit="synthetic-mining",
        title="Closed-loop electronics recovery strengthens gold supply resilience",
        body=(
            "A fictional refining team reports a higher recovery rate from discarded circuit "
            "boards while tracking energy use and material losses across the process."
        ),
        comment="A transparent mass-balance report would make the recovery claim easier to verify.",
        sentiment=Sentiment.POSITIVE,
        comment_sentiment=Sentiment.NEUTRAL,
        stance=MiningStance.NEUTRAL,
        confidence=97,
        reputation=82,
        controversy=ControversyLevel.LOW,
        score=184,
        themes=("circular supply", "process efficiency"),
        keywords=("gold", "recycling", "electronics"),
        concerns=(("sustainability", 0.78), ("economic benefits", 0.61)),
    ),
    _Scenario(
        mineral="copper",
        subreddit="synthetic-energy",
        title="Grid expansion raises demand for traceable copper projects",
        body=(
            "A fictional transmission plan compares recycled copper with new production and "
            "publishes water, land, and community-engagement assumptions for each scenario."
        ),
        comment="The demand case is persuasive, but the water baseline needs independent review.",
        sentiment=Sentiment.MIXED,
        comment_sentiment=Sentiment.MIXED,
        stance=MiningStance.MIXED,
        confidence=95,
        reputation=71,
        controversy=ControversyLevel.MEDIUM,
        score=143,
        themes=("grid infrastructure", "traceable supply"),
        keywords=("copper", "transmission", "recycling"),
        concerns=(("water contamination", 0.69), ("community rights", 0.58)),
    ),
    _Scenario(
        mineral="lithium",
        subreddit="synthetic-batteries",
        title="Lithium brine pilot publishes a shared watershed monitoring plan",
        body=(
            "A fictional pilot proposes open hydrology measurements, staged extraction limits, "
            "and community review checkpoints before any expansion decision."
        ),
        comment="Publishing raw measurements is useful only if local observers can challenge them.",
        sentiment=Sentiment.MIXED,
        comment_sentiment=Sentiment.NEGATIVE,
        stance=MiningStance.MIXED,
        confidence=98,
        reputation=65,
        controversy=ControversyLevel.HIGH,
        score=226,
        themes=("water stewardship", "battery materials"),
        keywords=("lithium", "brine", "watershed"),
        concerns=(("water contamination", 0.91), ("community rights", 0.82)),
    ),
    _Scenario(
        mineral="cobalt",
        subreddit="synthetic-supply-chain",
        title="Cobalt traceability trial links batches to workplace audits",
        body=(
            "A fictional procurement trial joins batch identifiers with corrective-action records "
            "and worker feedback rather than treating a single certificate as final evidence."
        ),
        comment="Worker-controlled reporting channels matter more than another static dashboard.",
        sentiment=Sentiment.MIXED,
        comment_sentiment=Sentiment.NEGATIVE,
        stance=MiningStance.NEUTRAL,
        confidence=96,
        reputation=69,
        controversy=ControversyLevel.HIGH,
        score=197,
        themes=("supply-chain traceability", "working conditions"),
        keywords=("cobalt", "traceability", "audits"),
        concerns=(("working conditions", 0.94), ("child labor", 0.87)),
    ),
    _Scenario(
        mineral="nickel",
        subreddit="synthetic-processing",
        title="Nickel processing comparison measures energy and air-quality trade-offs",
        body=(
            "A fictional engineering study compares two process routes using the same ore basis, "
            "system boundary, emissions factors, and uncertainty ranges."
        ),
        comment="Comparable boundaries are a good start; local air measurements should validate the model.",
        sentiment=Sentiment.NEUTRAL,
        comment_sentiment=Sentiment.MIXED,
        stance=MiningStance.NEUTRAL,
        confidence=94,
        reputation=76,
        controversy=ControversyLevel.MEDIUM,
        score=121,
        themes=("process comparison", "air quality"),
        keywords=("nickel", "processing", "emissions"),
        concerns=(("air quality", 0.84), ("pollution", 0.73)),
    ),
    _Scenario(
        mineral="graphite",
        subreddit="synthetic-materials",
        title="Graphite anode project evaluates local processing and skills transfer",
        body=(
            "A fictional project model compares export-only production with local purification, "
            "including training capacity, waste handling, margins, and electricity intensity."
        ),
        comment="The employment case looks stronger when training completion and retention are reported.",
        sentiment=Sentiment.POSITIVE,
        comment_sentiment=Sentiment.POSITIVE,
        stance=MiningStance.NEUTRAL,
        confidence=93,
        reputation=78,
        controversy=ControversyLevel.LOW,
        score=109,
        themes=("local value addition", "skills transfer"),
        keywords=("graphite", "anode", "processing"),
        concerns=(("local employment", 0.81), ("waste management", 0.59)),
    ),
)


def synthetic_records() -> tuple[RecordDetail, ...]:
    """Return a fresh, immutable set of fictional posts and comments."""

    records: list[RecordDetail] = []
    base_time = DATASET_GENERATED_AT - timedelta(days=12)
    for index, scenario in enumerate(_SCENARIOS):
        created_at = base_time + timedelta(days=index * 2, hours=index)
        post_id = f"synthetic-{scenario.mineral}-post"
        concerns = tuple(ConcernSignal(name=name, score=score) for name, score in scenario.concerns)
        post_enrichment = EnrichmentSnapshot(
            sentiment=scenario.sentiment,
            stance=scenario.stance,
            keywords=scenario.keywords,
            themes=scenario.themes,
            concerns=concerns,
        )
        records.append(
            RecordDetail(
                mode=DatasetKind.SYNTHETIC_DEMO,
                synthetic=True,
                public_sample=False,
                source=SYNTHETIC_PROVENANCE,
                id=post_id,
                kind=ContentKind.POST,
                mineral=scenario.mineral,
                topic_label=scenario.themes[0],
                title=scenario.title,
                body=scenario.body,
                subreddit=scenario.subreddit,
                created_at=created_at,
                score=scenario.score,
                comment_count=1,
                analysis=AnalysisSnapshot(
                    relevance=RelevanceSnapshot(
                        relevant=True,
                        confidence=scenario.confidence,
                        rationale=(
                            f"The fictional record directly discusses {scenario.mineral} "
                            "production, processing, or supply."
                        ),
                    ),
                    enrichment=post_enrichment,
                    reputation=ReputationSnapshot(
                        score=scenario.reputation,
                        controversy=scenario.controversy,
                        rationale=(
                            "The score is a deterministic synthetic signal for demonstrating "
                            "typed analysis and dashboard aggregation."
                        ),
                    ),
                ),
                source_note=SOURCE_NOTE,
                content_available=True,
            )
        )
        records.append(
            RecordDetail(
                mode=DatasetKind.SYNTHETIC_DEMO,
                synthetic=True,
                public_sample=False,
                source=SYNTHETIC_PROVENANCE,
                id=f"synthetic-{scenario.mineral}-comment",
                kind=ContentKind.COMMENT,
                parent_id=post_id,
                mineral=scenario.mineral,
                topic_label=scenario.themes[0],
                body=scenario.comment,
                subreddit=scenario.subreddit,
                created_at=created_at + timedelta(minutes=20),
                score=max(1, scenario.score // 6),
                analysis=AnalysisSnapshot(
                    enrichment=EnrichmentSnapshot(
                        sentiment=scenario.comment_sentiment,
                        stance=scenario.stance,
                        keywords=scenario.keywords[:2],
                        themes=scenario.themes,
                        concerns=concerns,
                    )
                ),
                source_note=SOURCE_NOTE,
                content_available=True,
            )
        )
    return tuple(records)


def synthetic_runs() -> tuple[RunSummary, ...]:
    """Return deterministic run history matching the synthetic dataset."""

    definitions = (
        ("export", RunStatus.SUCCEEDED, 12, 0, 240),
        ("reputation", RunStatus.SUCCEEDED, 6, 0, 1_940),
        ("enrichment", RunStatus.SUCCEEDED, 12, 0, 2_610),
        ("relevance", RunStatus.SUCCEEDED, 6, 0, 1_480),
        ("scrape", RunStatus.SUCCEEDED, 12, 0, 1_120),
    )
    runs: list[RunSummary] = []
    for index, (command, status, processed, failed, duration_ms) in enumerate(definitions):
        finished = DATASET_GENERATED_AT - timedelta(minutes=index * 4)
        runs.append(
            RunSummary(
                id=f"synthetic-run-{command}",
                command=command,
                status=status,
                started_at=finished - timedelta(milliseconds=duration_ms),
                finished_at=finished,
                processed=processed,
                failed=failed,
                duration_ms=duration_ms,
                synthetic=True,
            )
        )
    return tuple(runs)
