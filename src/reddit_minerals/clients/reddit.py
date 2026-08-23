"""Read-only PRAW adapter."""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Iterable, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, overload

from prawcore import Requestor
from requests import Session

from reddit_minerals.errors import (
    PermanentProviderError,
    ProviderAuthenticationError,
    ProviderConfigurationError,
    ProviderError,
    ProviderWideError,
    RetryableProviderError,
)
from reddit_minerals.models import CommentRecord, PostRecord, utc_now

_MAX_PROVIDER_ITEMS = 10_000
_TIME_FILTERS = frozenset({"hour", "day", "week", "month", "year", "all"})


class _RedditDeadlineExceeded(TimeoutError):
    """Internal deadline signal that must not be classified as a provider fault."""


@dataclass(slots=True)
class _RequestorCloseHandle:
    close: Callable[[], None] | None = None


class _ManagedRequestor(Requestor):
    """Capture PRAWcore cleanup through PRAW's public requestor extension seam."""

    def __init__(
        self,
        *,
        close_handle: _RequestorCloseHandle,
        oauth_url: str = "https://oauth.reddit.com",
        reddit_url: str = "https://www.reddit.com",
        session: Session | None = None,
        timeout: float = 16.0,
        user_agent: str,
    ) -> None:
        super().__init__(
            oauth_url=oauth_url,
            reddit_url=reddit_url,
            session=session,
            timeout=timeout,
            user_agent=user_agent,
        )
        close_handle.close = self.close


@dataclass(frozen=True, slots=True)
class CommentBatch(Sequence[CommentRecord]):
    """A bounded comment sequence plus whether it is a complete provider snapshot."""

    comments: tuple[CommentRecord, ...]
    snapshot_complete: bool

    def __len__(self) -> int:
        return len(self.comments)

    @overload
    def __getitem__(self, index: int) -> CommentRecord: ...

    @overload
    def __getitem__(self, index: slice) -> tuple[CommentRecord, ...]: ...

    def __getitem__(self, index: int | slice) -> CommentRecord | tuple[CommentRecord, ...]:
        return self.comments[index]


class PrawRedditClient:
    """Bounded, read-only Reddit client using application authentication."""

    def __init__(
        self,
        *,
        client_id: str,
        client_secret: str,
        user_agent: str,
        replace_more_limit: int,
        request_timeout_seconds: float = 30.0,
    ) -> None:
        if not client_id.strip() or not client_secret.strip():
            raise ProviderConfigurationError("Reddit credentials must be non-empty")
        if len(user_agent.strip()) < 10:
            raise ProviderConfigurationError("Reddit user agent must identify the application")
        if not 0 <= replace_more_limit <= 100:
            raise ProviderConfigurationError("Reddit replace-more limit must be between 0 and 100")
        _validate_timeout(request_timeout_seconds)
        try:
            import praw
        except ImportError as exc:  # pragma: no cover - depends on optional runtime
            raise ProviderConfigurationError(
                "PRAW is not installed; synchronize the project environment"
            ) from exc

        close_handle = _RequestorCloseHandle()
        try:
            self._reddit: Any = praw.Reddit(
                client_id=client_id,
                client_secret=client_secret,
                user_agent=user_agent,
                check_for_async=False,
                requestor_class=_ManagedRequestor,
                requestor_kwargs={
                    "timeout": request_timeout_seconds,
                    "close_handle": close_handle,
                },
            )
        except (TypeError, ValueError) as exc:
            _close_requestor_handle(close_handle)
            raise ProviderConfigurationError("Reddit client configuration was rejected") from exc
        except Exception as exc:
            _close_requestor_handle(close_handle)
            raise _classify_reddit_error(exc) from exc
        self._reddit.read_only = True
        self._replace_more_limit = replace_more_limit
        self._more_comments_type: type[Any] = praw.models.MoreComments
        self._close_requestor = close_handle.close

    def close(self) -> None:
        """Close PRAWcore's owned HTTP session and release pooled connections."""

        close_requestor = self._close_requestor
        self._close_requestor = None
        if close_requestor is not None:
            close_requestor()

    def search_posts(
        self,
        *,
        mineral: str,
        subreddit: str,
        limit: int,
        time_filter: str,
        deadline: float | None = None,
    ) -> Iterable[PostRecord]:
        """Search a single subreddit; ``limit`` is always finite."""

        _validate_search_arguments(
            mineral=mineral,
            subreddit=subreddit,
            limit=limit,
            time_filter=time_filter,
        )
        _validate_deadline(deadline)
        try:
            _raise_if_deadline_expired(deadline)
            results = self._reddit.subreddit(subreddit).search(
                mineral,
                sort="new",
                time_filter=time_filter,
                limit=limit,
            )
            # Materialize inside the exception boundary because PRAW listings are
            # lazy and may perform another request between yielded pages.
            posts: list[PostRecord] = []
            iterator = iter(results)
            while True:
                _raise_if_deadline_expired(deadline)
                try:
                    item = next(iterator)
                except StopIteration:
                    break
                _raise_if_deadline_expired(deadline)
                posts.append(self._post_from_submission(item))
            return tuple(posts)
        except _RedditDeadlineExceeded:
            raise
        except (ProviderError, ProviderWideError):
            raise
        except Exception as exc:  # provider exceptions are intentionally isolated here
            raise _classify_reddit_error(exc) from exc

    def fetch_comments(
        self, *, post_id: str, limit: int, deadline: float | None = None
    ) -> CommentBatch:
        """Fetch a bounded depth-first comment batch without flattening the full tree."""

        if not post_id.strip():
            raise ValueError("post_id must be non-empty")
        if not 0 <= limit <= _MAX_PROVIDER_ITEMS:
            raise ValueError(f"limit must be between 0 and {_MAX_PROVIDER_ITEMS}")
        _validate_deadline(deadline)
        if limit == 0:
            return CommentBatch((), snapshot_complete=False)
        try:
            _raise_if_deadline_expired(deadline)
            submission = self._reddit.submission(id=post_id)
            # PRAW removes every unexpanded MoreComments placeholder from the
            # forest and returns it. Calling replace_more repeatedly therefore
            # cannot resume the first call and can incorrectly make an incomplete
            # forest look complete. Apply the configured request bound once.
            unexpanded = submission.comments.replace_more(limit=self._replace_more_limit)
            _raise_if_deadline_expired(deadline)
            snapshot_complete = not bool(unexpanded)
            result: list[CommentRecord] = []
            iterators = [iter(submission.comments)]
            while iterators:
                _raise_if_deadline_expired(deadline)
                try:
                    comment = next(iterators[-1])
                except StopIteration:
                    iterators.pop()
                    continue
                _raise_if_deadline_expired(deadline)
                if isinstance(comment, self._more_comments_type):
                    snapshot_complete = False
                    continue

                if getattr(comment, "id", None) and hasattr(comment, "body"):
                    created = datetime.fromtimestamp(float(comment.created_utc), tz=UTC)
                    result.append(
                        CommentRecord(
                            id=str(comment.id),
                            post_id=post_id,
                            parent_id=str(getattr(comment, "parent_id", "")) or None,
                            body=str(comment.body or ""),
                            score=int(comment.score or 0),
                            created_at=created,
                            depth=max(0, int(getattr(comment, "depth", 0) or 0)),
                            subreddit=str(submission.subreddit.display_name),
                            permalink=_absolute_permalink(str(comment.permalink)),
                            fetched_at=utc_now(),
                        )
                    )
                    if len(result) >= limit:
                        snapshot_complete = False
                        break

                replies = getattr(comment, "replies", None)
                if replies is not None:
                    iterators.append(iter(replies))
            return CommentBatch(tuple(result), snapshot_complete=snapshot_complete)
        except _RedditDeadlineExceeded:
            raise
        except (ProviderError, ProviderWideError):
            raise
        except Exception as exc:
            raise _classify_reddit_error(exc) from exc

    @staticmethod
    def _post_from_submission(submission: Any) -> PostRecord:
        created = datetime.fromtimestamp(float(submission.created_utc), tz=UTC)
        ratio = getattr(submission, "upvote_ratio", None)
        return PostRecord(
            id=str(submission.id),
            title=str(submission.title or ""),
            selftext=str(submission.selftext or ""),
            subreddit=str(submission.subreddit.display_name),
            created_at=created,
            score=int(submission.score or 0),
            num_comments=max(0, int(submission.num_comments or 0)),
            upvote_ratio=float(ratio) if ratio is not None else None,
            permalink=_absolute_permalink(str(submission.permalink)),
            fetched_at=utc_now(),
        )


def _absolute_permalink(permalink: str) -> str:
    if permalink.startswith(("http://", "https://")):
        return permalink
    return f"https://www.reddit.com{permalink}"


def _close_requestor_handle(handle: _RequestorCloseHandle) -> None:
    close_requestor = handle.close
    handle.close = None
    if close_requestor is None:
        return
    # Preserve the constructor's sanitized provider error if cleanup itself fails.
    with suppress(Exception):
        close_requestor()


def _classify_reddit_error(exc: Exception) -> Exception:
    """Convert unstable provider exception classes into stable domain failures."""

    response = getattr(exc, "response", None)
    status = _coerce_status(getattr(response, "status_code", None))
    name = type(exc).__name__.lower()
    text = f"{name} {exc}".lower()
    wrapped = getattr(exc, "original_exception", None)
    wrapped_name = type(wrapped).__name__.lower() if isinstance(wrapped, Exception) else ""
    if name == "requestexception" or any(
        token in wrapped_name for token in ("timeout", "connection")
    ):
        return RetryableProviderError(f"Temporary Reddit provider error ({type(exc).__name__})")
    if status == 408 or status == 429 or (isinstance(status, int) and status >= 500):
        return RetryableProviderError(f"Reddit request failed with HTTP {status}")
    auth_error = str(getattr(exc, "error", "")).lower()
    if (
        status == 401
        or name in {"insufficientscope", "invalidtoken", "oauthexception"}
        or auth_error in {"invalid_grant", "invalid_client", "unauthorized_client"}
    ):
        return ProviderAuthenticationError("Reddit authentication or API access was rejected")
    if status == 403:
        return PermanentProviderError("Reddit request failed with HTTP 403")
    if any(
        token in text
        for token in (
            "invalid_grant",
            "invalid_client",
            "invalid client",
            "unauthorized",
            "authentication",
        )
    ):
        return ProviderAuthenticationError("Reddit authentication or API access was rejected")
    if status in {400, 404}:
        return PermanentProviderError(f"Reddit request failed with HTTP {status}")
    if any(token in name for token in ("timeout", "connection", "ratelimit", "server")):
        return RetryableProviderError(f"Temporary Reddit provider error ({type(exc).__name__})")
    return PermanentProviderError(f"Reddit provider error ({type(exc).__name__})")


def _validate_search_arguments(
    *, mineral: str, subreddit: str, limit: int, time_filter: str
) -> None:
    if not mineral.strip():
        raise ValueError("mineral must be non-empty")
    if not subreddit.strip():
        raise ValueError("subreddit must be non-empty")
    if not 1 <= limit <= _MAX_PROVIDER_ITEMS:
        raise ValueError(f"limit must be between 1 and {_MAX_PROVIDER_ITEMS}")
    if time_filter not in _TIME_FILTERS:
        raise ValueError("time_filter must be hour, day, week, month, year, or all")


def _validate_timeout(value: float) -> None:
    if not math.isfinite(value) or not 0 < value <= 300:
        raise ProviderConfigurationError("Reddit request timeout must be between 0 and 300 seconds")


def _validate_deadline(deadline: float | None) -> None:
    if deadline is not None and not math.isfinite(deadline):
        raise ValueError("deadline must be finite when provided")


def _raise_if_deadline_expired(deadline: float | None) -> None:
    if deadline is not None and time.monotonic() >= deadline:
        raise _RedditDeadlineExceeded("Reddit operation exceeded the run-wide deadline")


def _coerce_status(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    candidate = getattr(value, "value", value)
    try:
        return int(candidate)
    except (TypeError, ValueError):
        return None
