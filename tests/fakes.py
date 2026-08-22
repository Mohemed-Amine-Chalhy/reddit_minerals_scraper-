from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any

from reddit_minerals.models import (
    AnalysisKind,
    CommentRecord,
    ContentInput,
    EnrichmentAnalysis,
    PostRecord,
    ProviderResult,
    RelevanceAnalysis,
    ReputationAnalysis,
)


class FakeRedditClient:
    """Deterministic Reddit adapter whose outcomes may include exceptions."""

    def __init__(self) -> None:
        self.search_outcomes: dict[tuple[str, str], list[object]] = {}
        self.comment_outcomes: dict[str, list[object]] = {}
        self.search_calls: list[tuple[str, str, int, str]] = []
        self.comment_calls: list[tuple[str, int]] = []
        self.search_deadlines: list[float | None] = []
        self.comment_deadlines: list[float | None] = []

    def queue_search(self, mineral: str, subreddit: str, *outcomes: object) -> None:
        self.search_outcomes[(mineral, subreddit)] = list(outcomes)

    def queue_comments(self, post_id: str, *outcomes: object) -> None:
        self.comment_outcomes[post_id] = list(outcomes)

    def search_posts(
        self,
        *,
        mineral: str,
        subreddit: str,
        limit: int,
        time_filter: str,
        deadline: float | None = None,
    ) -> Iterable[PostRecord]:
        self.search_calls.append((mineral, subreddit, limit, time_filter))
        self.search_deadlines.append(deadline)
        outcome = _next(self.search_outcomes, (mineral, subreddit), ())
        if isinstance(outcome, BaseException):
            raise outcome
        return tuple(outcome)  # type: ignore[arg-type]

    def fetch_comments(
        self, *, post_id: str, limit: int, deadline: float | None = None
    ) -> Sequence[CommentRecord]:
        self.comment_calls.append((post_id, limit))
        self.comment_deadlines.append(deadline)
        outcome = _next(self.comment_outcomes, post_id, ())
        if isinstance(outcome, BaseException):
            raise outcome
        if hasattr(outcome, "snapshot_complete"):
            return outcome  # type: ignore[return-value]
        return tuple(outcome)  # type: ignore[arg-type]


class FakeAnalysisClient:
    """Schema-aware fake that records calls and returns queued outcomes."""

    def __init__(self, *, model: str = "offline-model") -> None:
        self.outcomes: dict[tuple[AnalysisKind, str], list[object]] = {}
        self.calls: list[tuple[AnalysisKind, str]] = []
        self.model = model

    def queue(self, kind: AnalysisKind, content_id: str, *outcomes: object) -> None:
        self.outcomes[(kind, content_id)] = list(outcomes)

    def analyze_relevance(self, content: ContentInput) -> ProviderResult[RelevanceAnalysis]:
        return self._run(AnalysisKind.RELEVANCE, content)  # type: ignore[return-value]

    def analyze_enrichment(self, content: ContentInput) -> ProviderResult[EnrichmentAnalysis]:
        return self._run(AnalysisKind.ENRICHMENT, content)  # type: ignore[return-value]

    def analyze_reputation(self, content: ContentInput) -> ProviderResult[ReputationAnalysis]:
        return self._run(AnalysisKind.REPUTATION, content)  # type: ignore[return-value]

    def _run(self, kind: AnalysisKind, content: ContentInput) -> Any:
        self.calls.append((kind, content.content_id))
        outcome = _next(self.outcomes, (kind, content.content_id), None)
        if isinstance(outcome, BaseException):
            raise outcome
        if outcome is None:
            return result_for(kind, model=self.model)
        return outcome


def result_for(kind: AnalysisKind, **overrides: Any) -> ProviderResult[Any]:
    if kind is AnalysisKind.RELEVANCE:
        value: Any = RelevanceAnalysis(
            relevant=overrides.pop("relevant", True),
            confidence=overrides.pop("confidence", 91),
            rationale=overrides.pop("rationale", "The mineral is discussed directly."),
            matched_topics=overrides.pop("matched_topics", ["supply"]),
        )
    elif kind is AnalysisKind.ENRICHMENT:
        value = EnrichmentAnalysis(
            sentiment=overrides.pop("sentiment", "neutral"),
            keywords=overrides.pop("keywords", ["mine"]),
            themes=overrides.pop("themes", ["supply"]),
            mining_stance=overrides.pop("mining_stance", "neutral"),
            topic_classification=overrides.pop("topic_classification", "supply chain"),
            relevance_score=overrides.pop("relevance_score", 0.9),
        )
    else:
        value = ReputationAnalysis(
            overall_reputation_score=overrides.pop("overall_reputation_score", 60),
            sentiment=overrides.pop("sentiment", "mixed"),
            sentiment_score=overrides.pop("sentiment_score", 55),
            credibility=overrides.pop("credibility", "medium"),
            credibility_score=overrides.pop("credibility_score", 50),
            market_impact=overrides.pop("market_impact", "unclear"),
            market_impact_score=overrides.pop("market_impact_score", 45),
            controversy_level=overrides.pop("controversy_level", "medium"),
            rationale=overrides.pop("rationale", "Mixed perceptions are present."),
            evidence_signals=overrides.pop("evidence_signals", ["mixed comments"]),
        )
    return ProviderResult(
        value=value,
        model=overrides.pop("model", "offline-model"),
        provider_request_id=overrides.pop("provider_request_id", "offline-request"),
        input_tokens=overrides.pop("input_tokens", 12),
        output_tokens=overrides.pop("output_tokens", 8),
        latency_ms=overrides.pop("latency_ms", 1),
        **overrides,
    )


def _next(mapping: dict[Any, list[object]], key: Any, default: object) -> object:
    outcomes = mapping.get(key)
    if not outcomes:
        return default
    if len(outcomes) == 1:
        return outcomes[0]
    return outcomes.pop(0)
