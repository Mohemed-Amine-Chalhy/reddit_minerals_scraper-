"""Content-safe structured logging."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any, TextIO

_SAFE_EXTRA_FIELDS = frozenset(
    {
        "analysis_kind",
        "blocked",
        "command",
        "comment_associations_removed",
        "comments",
        "comments_skipped_deleted",
        "completed",
        "content_id",
        "content_kind",
        "detail",
        "error_type",
        "mineral",
        "permanent_failures",
        "retryable_failures",
        "reconciled_runs",
        "selected",
        "stale_discarded",
        "status",
        "subreddit",
        "validation_errors",
    }
)
_APPLICATION_LOGGER = "reddit_minerals"
_DEPENDENCY_LOGGERS = (
    "google",
    "google_genai",
    "httpcore",
    "httpx",
    "praw",
    "prawcore",
    "urllib3",
)


class _ApplicationStreamHandler(logging.StreamHandler[TextIO]):
    """Marker for handlers owned by this package's logging configuration."""


class _ApplicationNullHandler(logging.NullHandler):
    """Marker for package-owned suppression handlers."""


class JsonFormatter(logging.Formatter):
    """Emit compact JSON without serializing arbitrary exception objects."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in _SAFE_EXTRA_FIELDS:
            if key in record.__dict__:
                payload[key] = record.__dict__[key]
        if record.exc_info and record.exc_info[0] is not None:
            payload["exception_type"] = record.exc_info[0].__name__
        return json.dumps(payload, ensure_ascii=False, default=str)


def configure_logging(level: str) -> None:
    """Configure one app-only stderr handler and silence dependency/root records."""

    handler = _ApplicationStreamHandler()
    handler.setFormatter(JsonFormatter())

    application = logging.getLogger(_APPLICATION_LOGGER)
    _replace_handlers(application, handler)
    application.setLevel(level)
    application.propagate = False

    # Keep a root handler installed so a dependency cannot implicitly call
    # ``basicConfig`` and begin emitting request data. Only package descendants
    # reach the structured application handler above.
    root = logging.getLogger()
    _replace_handlers(root, _ApplicationNullHandler())
    root.setLevel(logging.CRITICAL + 1)

    for name in _dependency_logger_names():
        dependency = logging.getLogger(name)
        _replace_handlers(dependency, _ApplicationNullHandler())
        dependency.propagate = False


def _replace_handlers(logger: logging.Logger, handler: logging.Handler) -> None:
    """Replace handlers and close only resources created by prior calls here."""

    for existing in tuple(logger.handlers):
        logger.removeHandler(existing)
        if isinstance(existing, (_ApplicationStreamHandler, _ApplicationNullHandler)):
            existing.close()
    logger.addHandler(handler)


def _dependency_logger_names() -> list[str]:
    """Return configured dependency roots plus descendants created so far."""

    names = set(_DEPENDENCY_LOGGERS)
    for name in logging.Logger.manager.loggerDict:
        if any(name.startswith(f"{root}.") for root in _DEPENDENCY_LOGGERS):
            names.add(name)
    return sorted(names)
