"""Resumable schema-constrained AI analysis orchestration."""

from __future__ import annotations

import logging
import math
import time
from collections.abc import Callable
from functools import partial
from typing import cast

from pydantic import Field

from reddit_minerals.clients.base import AnalysisClient
from reddit_minerals.clients.gemini import PROMPT_VERSION
from reddit_minerals.errors import (
    BatchOperationError,
    BatchProviderFailureError,
    ContentBlockedError,
    OperationDeadlineExceededError,
    ProviderError,
)
from reddit_minerals.models import (
    AnalysisKind,
    ContentInput,
    ProviderResult,
    StrictModel,
    WorkStatus,
)
from reddit_minerals.retry import with_retries
from reddit_minerals.storage import Database, StaleAnalysisCandidateError

logger = logging.getLogger(__name__)

ANALYSIS_SCHEMA_VERSION = 1
_MAX_BATCH_SIZE = 10_000
_MAX_CONTEXT_COMMENTS = 20


class AnalysisSummary(StrictModel):
    kind: AnalysisKind
    selected: int = Field(ge=0)
    completed: int = Field(ge=0)
    retryable_failures: int = Field(ge=0)
    permanent_failures: int = Field(ge=0)
    blocked: int = Field(ge=0)
    stale_discarded: int = Field(ge=0)


class AnalysisService:
    """Apply an injected analysis client to bounded eligible database content."""

    def __init__(
        self,
        *,
        client: AnalysisClient,
        database: Database,
        max_retries: int,
        retry_base_delay_seconds: float,
        retry_max_delay_seconds: float,
        operation_timeout_seconds: float | None = None,
        model: str | None = None,
        max_content_chars: int = 12_000,
    ) -> None:
        if not 1 <= max_retries <= 10:
            raise ValueError("max_retries must be between 1 and 10")
        if not math.isfinite(retry_base_delay_seconds) or not 0 <= retry_base_delay_seconds <= 60:
            raise ValueError("retry_base_delay_seconds must be between 0 and 60")
        if not math.isfinite(retry_max_delay_seconds) or not 0 <= retry_max_delay_seconds <= 600:
            raise ValueError("retry_max_delay_seconds must be between 0 and 600")
        if operation_timeout_seconds is not None and (
            not math.isfinite(operation_timeout_seconds) or operation_timeout_seconds <= 0
        ):
            raise ValueError("operation_timeout_seconds must be a positive finite value")
        if not 500 <= max_content_chars <= 100_000:
            raise ValueError("max_content_chars must be between 500 and 100000")
        self._client = client
        self._database = database
        self._max_retries = max_retries
        self._retry_base = retry_base_delay_seconds
        self._retry_max = retry_max_delay_seconds
        self._operation_timeout = operation_timeout_seconds
        self._max_content_chars = max_content_chars
        selected_model: object = model if model is not None else getattr(client, "model", None)
        if selected_model is not None and not isinstance(selected_model, str):
            raise ValueError("analysis model provenance must be a string")
        if isinstance(selected_model, str) and not selected_model.strip():
            raise ValueError("analysis model provenance must not be blank")
        self._model: str | None = (
            selected_model.strip() if isinstance(selected_model, str) else None
        )

    def run(
        self,
        kind: AnalysisKind,
        *,
        mineral: str | None,
        limit: int,
        force: bool,
        relevance_threshold: float,
        max_context_comments: int,
    ) -> AnalysisSummary:
        if not isinstance(kind, AnalysisKind):
            raise ValueError("kind must be a supported AnalysisKind")
        if mineral is not None and not mineral.strip():
            raise ValueError("mineral must be non-empty when provided")
        _validate_run_arguments(
            limit=limit,
            relevance_threshold=relevance_threshold,
            max_context_comments=max_context_comments,
        )
        deadline = (
            time.monotonic() + self._operation_timeout
            if self._operation_timeout is not None
            else None
        )
        candidates = self._database.analysis_candidates(
            kind,
            mineral=mineral,
            limit=limit,
            force=force,
            relevance_threshold=relevance_threshold,
            max_context_comments=max_context_comments,
            max_content_chars=self._max_content_chars,
            schema_version=ANALYSIS_SCHEMA_VERSION,
            prompt_version=PROMPT_VERSION,
            model=self._model,
        )
        summary = AnalysisSummary(
            kind=kind,
            selected=len(candidates),
            completed=0,
            retryable_failures=0,
            permanent_failures=0,
            blocked=0,
            stale_discarded=0,
        )
        analyze = self._operation_for(kind)
        for content in candidates:
            try:
                result = with_retries(
                    partial(
                        self._invoke_before_deadline,
                        analyze,
                        content,
                        deadline,
                        summary,
                    ),
                    attempts=self._max_retries,
                    base_delay_seconds=self._retry_base,
                    max_delay_seconds=self._retry_max,
                    sleep=partial(
                        self._sleep_before_deadline,
                        deadline=deadline,
                        summary=summary,
                    ),
                )
            except ContentBlockedError as exc:
                if self._record_failure(kind, content, WorkStatus.BLOCKED, exc):
                    summary.blocked += 1
                else:
                    summary.stale_discarded += 1
                continue
            except ProviderError as exc:
                status = (
                    WorkStatus.RETRYABLE_FAILURE if exc.retryable else WorkStatus.PERMANENT_FAILURE
                )
                if not self._record_failure(kind, content, status, exc):
                    summary.stale_discarded += 1
                elif exc.retryable:
                    summary.retryable_failures += 1
                else:
                    summary.permanent_failures += 1
                continue

            try:
                self._database.save_analysis(
                    kind=kind,
                    content=content,
                    result=result,
                    schema_version=ANALYSIS_SCHEMA_VERSION,
                    prompt_version=PROMPT_VERSION,
                )
            except StaleAnalysisCandidateError:
                summary.stale_discarded += 1
                self._log_stale_candidate(kind, content)
                continue
            summary.completed += 1
            logger.info(
                "analysis complete",
                extra={
                    "analysis_kind": kind.value,
                    "content_kind": content.kind.value,
                    "content_id": content.content_id,
                    "mineral": content.mineral,
                },
            )

        failures = summary.retryable_failures + summary.permanent_failures + summary.blocked
        if summary.selected > 0 and summary.completed == 0 and failures == summary.selected:
            raise BatchProviderFailureError(
                f"All {summary.selected} selected {kind.value} items failed at the provider boundary",
                summary=summary.model_dump(mode="json"),
            )
        if (
            summary.selected > 0
            and summary.completed == 0
            and failures + summary.stale_discarded == summary.selected
        ):
            raise BatchOperationError(
                f"No selected {kind.value} item could be persisted; source inputs changed",
                summary=summary.model_dump(mode="json"),
            )
        if failures or summary.stale_discarded:
            logger.warning(
                "analysis batch completed with failed or stale items",
                extra={
                    "analysis_kind": kind.value,
                    "selected": summary.selected,
                    "completed": summary.completed,
                    "retryable_failures": summary.retryable_failures,
                    "permanent_failures": summary.permanent_failures,
                    "blocked": summary.blocked,
                    "stale_discarded": summary.stale_discarded,
                },
            )
        return summary

    def _operation_for(
        self, kind: AnalysisKind
    ) -> Callable[[ContentInput], ProviderResult[StrictModel]]:
        if kind is AnalysisKind.RELEVANCE:
            return cast(
                Callable[[ContentInput], ProviderResult[StrictModel]],
                self._client.analyze_relevance,
            )
        if kind is AnalysisKind.ENRICHMENT:
            return cast(
                Callable[[ContentInput], ProviderResult[StrictModel]],
                self._client.analyze_enrichment,
            )
        return cast(
            Callable[[ContentInput], ProviderResult[StrictModel]],
            self._client.analyze_reputation,
        )

    @staticmethod
    def _sleep_before_deadline(
        delay: float,
        *,
        deadline: float | None,
        summary: AnalysisSummary,
    ) -> None:
        if deadline is None:
            time.sleep(delay)
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise OperationDeadlineExceededError(
                "Analysis operation deadline reached during retry backoff",
                summary=summary.model_dump(mode="json"),
            )
        time.sleep(min(delay, remaining))
        if time.monotonic() >= deadline:
            raise OperationDeadlineExceededError(
                "Analysis operation deadline reached during retry backoff",
                summary=summary.model_dump(mode="json"),
            )

    @staticmethod
    def _invoke_before_deadline(
        operation: Callable[[ContentInput], ProviderResult[StrictModel]],
        content: ContentInput,
        deadline: float | None,
        summary: AnalysisSummary,
    ) -> ProviderResult[StrictModel]:
        if deadline is not None and time.monotonic() >= deadline:
            raise OperationDeadlineExceededError(
                "Analysis operation deadline reached after "
                f"{summary.completed} of {summary.selected} selected items completed",
                summary=summary.model_dump(mode="json"),
            )
        result = operation(content)
        if deadline is not None and time.monotonic() >= deadline:
            raise OperationDeadlineExceededError(
                "Analysis provider call completed after the operation deadline; "
                "the late result was discarded",
                summary=summary.model_dump(mode="json"),
            )
        return result

    def _record_failure(
        self,
        kind: AnalysisKind,
        content: ContentInput,
        status: WorkStatus,
        error: ProviderError,
    ) -> bool:
        try:
            self._database.record_analysis_failure(
                kind=kind,
                content=content,
                status=status,
                error=type(error).__name__,
                schema_version=ANALYSIS_SCHEMA_VERSION,
                prompt_version=PROMPT_VERSION,
                model=self._model,
            )
        except StaleAnalysisCandidateError:
            self._log_stale_candidate(kind, content)
            return False
        logger.warning(
            "analysis failed",
            extra={
                "analysis_kind": kind.value,
                "content_kind": content.kind.value,
                "content_id": content.content_id,
                "mineral": content.mineral,
                "status": status.value,
                "error_type": type(error).__name__,
            },
        )
        return True

    @staticmethod
    def _log_stale_candidate(kind: AnalysisKind, content: ContentInput) -> None:
        logger.warning(
            "analysis result discarded because the source or dependency changed",
            extra={
                "analysis_kind": kind.value,
                "content_kind": content.kind.value,
                "content_id": content.content_id,
                "mineral": content.mineral,
                "stale_discarded": 1,
            },
        )


def _validate_run_arguments(
    *, limit: int, relevance_threshold: float, max_context_comments: int
) -> None:
    if not 1 <= limit <= _MAX_BATCH_SIZE:
        raise ValueError(f"limit must be between 1 and {_MAX_BATCH_SIZE}")
    if not math.isfinite(relevance_threshold) or not 0 <= relevance_threshold <= 100:
        raise ValueError("relevance_threshold must be between 0 and 100")
    if not 0 <= max_context_comments <= _MAX_CONTEXT_COMMENTS:
        raise ValueError(f"max_context_comments must be between 0 and {_MAX_CONTEXT_COMMENTS}")
