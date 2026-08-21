from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from reddit_minerals.models import CommentRecord, PostRecord
from reddit_minerals.observability import _APPLICATION_LOGGER, _dependency_logger_names
from reddit_minerals.storage import Database


@pytest.fixture(autouse=True)
def restore_root_logging() -> object:
    """Keep application-wide logging reconfiguration from leaking across tests."""

    loggers = [
        logging.getLogger(),
        logging.getLogger(_APPLICATION_LOGGER),
        *(logging.getLogger(name) for name in _dependency_logger_names()),
    ]
    original_states = {
        logger: (
            list(logger.handlers),
            logger.level,
            logger.disabled,
            logger.propagate,
        )
        for logger in loggers
    }
    yield
    current_loggers = set(original_states)
    current_loggers.update(logging.getLogger(name) for name in _dependency_logger_names())
    for logger in current_loggers:
        handlers, level, disabled, propagate = original_states.get(
            logger,
            ([], logging.NOTSET, False, True),
        )
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            if handler not in handlers:
                handler.close()
        for handler in handlers:
            logger.addHandler(handler)
        logger.setLevel(level)
        logger.disabled = disabled
        logger.propagate = propagate


@pytest.fixture
def database(tmp_path: Path) -> Database:
    value = Database(tmp_path / "state" / "minerals.sqlite3")
    value.initialize()
    return value


@pytest.fixture
def make_post() -> Callable[..., PostRecord]:
    def factory(
        post_id: str = "post-1",
        *,
        created_offset: int = 0,
        title: str | None = None,
        selftext: str | None = None,
        subreddit: str = "mining",
        score: int = 7,
        num_comments: int = 2,
        upvote_ratio: float | None = 0.8,
    ) -> PostRecord:
        created = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=created_offset)
        return PostRecord(
            id=post_id,
            title=title if title is not None else f"Title for {post_id}",
            selftext=selftext if selftext is not None else f"Body for {post_id}",
            subreddit=subreddit,
            created_at=created,
            score=score,
            num_comments=num_comments,
            upvote_ratio=upvote_ratio,
            permalink=f"https://www.reddit.com/r/{subreddit}/comments/{post_id}",
            fetched_at=created + timedelta(hours=1),
        )

    return factory


@pytest.fixture
def make_comment() -> Callable[..., CommentRecord]:
    def factory(
        comment_id: str = "comment-1",
        *,
        post_id: str = "post-1",
        created_offset: int = 0,
        body: str | None = None,
        subreddit: str = "mining",
        score: int = 3,
        depth: int = 0,
    ) -> CommentRecord:
        created = datetime(2026, 1, 2, tzinfo=UTC) + timedelta(seconds=created_offset)
        return CommentRecord(
            id=comment_id,
            post_id=post_id,
            parent_id=f"t3_{post_id}" if depth == 0 else "t1_parent",
            body=body if body is not None else f"Body for {comment_id}",
            score=score,
            created_at=created,
            depth=depth,
            subreddit=subreddit,
            permalink=f"https://www.reddit.com/r/{subreddit}/comments/{post_id}/_/{comment_id}",
            fetched_at=created + timedelta(hours=1),
        )

    return factory
