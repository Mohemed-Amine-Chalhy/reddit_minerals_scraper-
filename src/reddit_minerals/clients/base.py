"""Protocols implemented by real providers and offline test fakes."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Protocol

from reddit_minerals.models import (
    CommentRecord,
    ContentInput,
    EnrichmentAnalysis,
    PostRecord,
    ProviderResult,
    RelevanceAnalysis,
    ReputationAnalysis,
)


class RedditClient(Protocol):
    """Minimal Reddit operations required by the scraper."""

    def search_posts(
        self,
        *,
        mineral: str,
        subreddit: str,
        limit: int,
        time_filter: str,
        deadline: float | None = None,
    ) -> Iterable[PostRecord]: ...

    def fetch_comments(
        self, *, post_id: str, limit: int, deadline: float | None = None
    ) -> Sequence[CommentRecord]: ...


class AnalysisClient(Protocol):
    """Schema-specific AI operations used by the analysis pipeline."""

    def analyze_relevance(self, content: ContentInput) -> ProviderResult[RelevanceAnalysis]: ...

    def analyze_enrichment(self, content: ContentInput) -> ProviderResult[EnrichmentAnalysis]: ...

    def analyze_reputation(self, content: ContentInput) -> ProviderResult[ReputationAnalysis]: ...
