"""Bounded, resumable Reddit collection orchestration."""

from __future__ import annotations

import logging
import math
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import timedelta
from functools import partial
from time import monotonic, sleep

from pydantic import Field

from reddit_minerals.clients.base import RedditClient
from reddit_minerals.errors import (
    BatchProviderFailureError,
    OperationDeadlineExceededError,
    ProviderError,
)
from reddit_minerals.models import StrictModel, WorkStatus
from reddit_minerals.retry import with_retries
from reddit_minerals.storage import Database
from reddit_minerals.storage.database import RefreshDecision

logger = logging.getLogger(__name__)


class ScrapeOperationDeadlineExceededError(OperationDeadlineExceededError, TimeoutError):
    """Scrape deadline compatible with domain and built-in timeout handling."""


class ScrapeSummary(StrictModel):
    minerals: list[str]
    posts_discovered: int = Field(ge=0)
    posts_completed: int = Field(ge=0)
    posts_skipped_fresh: int = Field(ge=0)
    posts_skipped_terminal: int = Field(ge=0)
    posts_skipped_deleted: int = Field(ge=0)
    posts_failed: int = Field(ge=0)
    comments_stored: int = Field(ge=0)
    comments_skipped_deleted: int = Field(ge=0)
    comment_associations_removed: int = Field(ge=0)
    searches_failed: int = Field(ge=0)
    dry_run: bool


class ScrapeService:
    """Coordinate search, comment collection, persistence, and resume state."""

    def __init__(
        self,
        *,
        client: RedditClient,
        database: Database,
        max_retries: int,
        retry_base_delay_seconds: float,
        retry_max_delay_seconds: float,
        operation_timeout_seconds: float = 1_800.0,
    ) -> None:
        if not 1 <= max_retries <= 10:
            raise ValueError("max_retries must be between 1 and 10")
        if not math.isfinite(retry_base_delay_seconds) or not 0 <= retry_base_delay_seconds <= 60:
            raise ValueError("retry_base_delay_seconds must be between 0 and 60")
        if not math.isfinite(retry_max_delay_seconds) or not 0 <= retry_max_delay_seconds <= 600:
            raise ValueError("retry_max_delay_seconds must be between 0 and 600")
        if (
            not math.isfinite(operation_timeout_seconds)
            or not 0 < operation_timeout_seconds <= 86_400
        ):
            raise ValueError(
                "operation_timeout_seconds must be a positive finite value no greater than 86400"
            )
        self._client = client
        self._database = database
        self._max_retries = max_retries
        self._retry_base = retry_base_delay_seconds
        self._retry_max = retry_max_delay_seconds
        self._operation_timeout = operation_timeout_seconds

    def run(
        self,
        *,
        mapping: Mapping[str, Sequence[str]],
        minerals: Sequence[str] | None,
        max_posts_per_mineral: int,
        max_comments_per_post: int,
        refresh_after: timedelta,
        time_filter: str,
        dry_run: bool,
        force: bool,
    ) -> ScrapeSummary:
        _validate_run_arguments(
            mapping=mapping,
            max_posts_per_mineral=max_posts_per_mineral,
            max_comments_per_post=max_comments_per_post,
            refresh_after=refresh_after,
            time_filter=time_filter,
        )
        deadline = monotonic() + self._operation_timeout
        selected = _select_minerals(mapping, minerals)
        summary = ScrapeSummary(
            minerals=selected,
            posts_discovered=0,
            posts_completed=0,
            posts_skipped_fresh=0,
            posts_skipped_terminal=0,
            posts_skipped_deleted=0,
            posts_failed=0,
            comments_stored=0,
            comments_skipped_deleted=0,
            comment_associations_removed=0,
            searches_failed=0,
            dry_run=dry_run,
        )
        successful_provider_operations = 0

        for mineral in selected:
            seen_posts: set[str] = set()
            for subreddit in mapping[mineral]:
                remaining = max_posts_per_mineral - len(seen_posts)
                if remaining <= 0:
                    break
                try:
                    search = partial(
                        self._client.search_posts,
                        mineral=mineral,
                        subreddit=subreddit,
                        limit=remaining,
                        time_filter=time_filter,
                        deadline=deadline,
                    )
                    posts = self._retry(
                        partial(_materialize, search),
                        deadline=deadline,
                        summary=summary,
                    )
                    successful_provider_operations += 1
                except ProviderError as exc:
                    _raise_if_expired(deadline, summary)
                    summary.searches_failed += 1
                    logger.warning(
                        "subreddit search failed",
                        extra={
                            "mineral": mineral,
                            "subreddit": subreddit,
                            "error_type": type(exc).__name__,
                        },
                    )
                    continue

                for post in posts:
                    _raise_if_expired(deadline, summary)
                    if post.id in seen_posts:
                        continue
                    seen_posts.add(post.id)
                    summary.posts_discovered += 1
                    decision = self._database.refresh_decision(
                        post.id, mineral, refresh_after, force=force
                    )
                    if decision is RefreshDecision.TOMBSTONED:
                        summary.posts_skipped_deleted += 1
                        continue
                    if decision is RefreshDecision.TERMINAL:
                        summary.posts_skipped_terminal += 1
                        continue
                    if decision is RefreshDecision.FRESH:
                        summary.posts_skipped_fresh += 1
                        continue
                    if dry_run:
                        continue

                    try:
                        fetch_comments = partial(
                            self._client.fetch_comments,
                            post_id=post.id,
                            limit=max_comments_per_post,
                            deadline=deadline,
                        )
                        comments, snapshot_complete = self._retry(
                            partial(_materialize_comments, fetch_comments),
                            deadline=deadline,
                            summary=summary,
                        )
                        successful_provider_operations += 1
                    except ProviderError as exc:
                        _raise_if_expired(deadline, summary)
                        state = (
                            WorkStatus.RETRYABLE_FAILURE
                            if exc.retryable
                            else WorkStatus.PERMANENT_FAILURE
                        )
                        stored = self._database.store_scraped_post(
                            post,
                            (),
                            mineral=mineral,
                            status=state,
                            error=type(exc).__name__,
                        )
                        if stored.post_skipped_tombstone:
                            summary.posts_skipped_deleted += 1
                            summary.comments_skipped_deleted += stored.comments_skipped_tombstone
                            continue
                        summary.posts_failed += 1
                        logger.warning(
                            "comment collection failed",
                            extra={
                                "mineral": mineral,
                                "content_id": post.id,
                                "error_type": type(exc).__name__,
                            },
                        )
                        continue

                    stored = self._database.store_scraped_post(
                        post,
                        comments,
                        mineral=mineral,
                        status=WorkStatus.COMPLETE,
                        comment_snapshot_complete=snapshot_complete,
                    )
                    if stored.post_skipped_tombstone:
                        summary.posts_skipped_deleted += 1
                        summary.comments_skipped_deleted += stored.comments_skipped_tombstone
                        continue
                    summary.posts_completed += 1
                    summary.comments_stored += stored.comments_stored
                    summary.comments_skipped_deleted += stored.comments_skipped_tombstone
                    summary.comment_associations_removed += stored.comment_associations_removed
                    logger.info(
                        "post collection complete",
                        extra={
                            "mineral": mineral,
                            "content_id": post.id,
                            "comments": stored.comments_stored,
                            "comments_skipped_deleted": (stored.comments_skipped_tombstone),
                            "comment_associations_removed": (stored.comment_associations_removed),
                        },
                    )
        provider_failures = summary.searches_failed + summary.posts_failed
        if provider_failures > 0 and successful_provider_operations == 0:
            raise BatchProviderFailureError(
                "Every attempted Reddit provider operation failed",
                summary=summary.model_dump(mode="json"),
            )
        return summary

    def _retry[ResultT](
        self,
        operation: Callable[[float], ResultT],
        *,
        deadline: float,
        summary: ScrapeSummary,
    ) -> ResultT:
        def guarded_operation() -> ResultT:
            _raise_if_expired(deadline)
            result = operation(deadline)
            _raise_if_expired(deadline)
            return result

        def bounded_sleep(delay: float) -> None:
            remaining = deadline - monotonic()
            if remaining <= 0:
                _raise_if_expired(deadline)
            sleep(min(delay, remaining))
            _raise_if_expired(deadline)

        try:
            return with_retries(
                guarded_operation,
                attempts=self._max_retries,
                base_delay_seconds=self._retry_base,
                max_delay_seconds=self._retry_max,
                sleep=bounded_sleep,
            )
        except TimeoutError as exc:
            raise ScrapeOperationDeadlineExceededError(
                "Reddit scrape exceeded the configured timeout deadline",
                summary=summary.model_dump(mode="json"),
            ) from exc


def _select_minerals(
    mapping: Mapping[str, Sequence[str]], requested: Sequence[str] | None
) -> list[str]:
    if not requested:
        return sorted(mapping)
    normalized = list(dict.fromkeys(" ".join(item.lower().split()) for item in requested))
    unknown = sorted(set(normalized) - set(mapping))
    if unknown:
        raise ValueError("Unknown mineral(s): " + ", ".join(unknown))
    return normalized


def _validate_run_arguments(
    *,
    mapping: Mapping[str, Sequence[str]],
    max_posts_per_mineral: int,
    max_comments_per_post: int,
    refresh_after: timedelta,
    time_filter: str,
) -> None:
    if not mapping:
        raise ValueError("mapping must contain at least one mineral")
    if any(not mineral.strip() or not subreddits for mineral, subreddits in mapping.items()):
        raise ValueError("every mapped mineral must have at least one subreddit")
    if any(not subreddit.strip() for subreddits in mapping.values() for subreddit in subreddits):
        raise ValueError("mapped subreddit names must not be blank")
    if not 1 <= max_posts_per_mineral <= 10_000:
        raise ValueError("max_posts_per_mineral must be between 1 and 10000")
    if not 0 <= max_comments_per_post <= 10_000:
        raise ValueError("max_comments_per_post must be between 0 and 10000")
    if not timedelta(0) <= refresh_after <= timedelta(hours=8_760):
        raise ValueError("refresh_after must be between zero and 8760 hours")
    if time_filter not in {"hour", "day", "week", "month", "year", "all"}:
        raise ValueError("time_filter must be hour, day, week, month, year, or all")


def _materialize[ItemT](
    operation: Callable[[], Iterable[ItemT]], deadline: float
) -> tuple[ItemT, ...]:
    result: list[ItemT] = []
    for item in operation():
        _raise_if_expired(deadline)
        result.append(item)
    _raise_if_expired(deadline)
    return tuple(result)


def _materialize_comments[ItemT](
    operation: Callable[[], Sequence[ItemT]], deadline: float
) -> tuple[tuple[ItemT, ...], bool]:
    comments = operation()
    snapshot_complete = bool(getattr(comments, "snapshot_complete", False))
    return _materialize(lambda: comments, deadline), snapshot_complete


def _raise_if_expired(deadline: float, summary: ScrapeSummary | None = None) -> None:
    if monotonic() >= deadline:
        if summary is not None:
            raise ScrapeOperationDeadlineExceededError(
                "Reddit scrape exceeded the configured timeout deadline",
                summary=summary.model_dump(mode="json"),
            )
        raise TimeoutError("Reddit operation exceeded the configured timeout")
