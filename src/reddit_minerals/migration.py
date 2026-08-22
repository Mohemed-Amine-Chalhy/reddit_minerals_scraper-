"""Validated migration of the prototype's per-mineral JSON files."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pydantic import Field

from reddit_minerals.models import (
    CommentRecord,
    ContentKind,
    PostRecord,
    StrictModel,
    utc_now,
)
from reddit_minerals.storage import Database

MAX_LEGACY_FILE_BYTES = 100_000_000
MAX_LEGACY_RECORDS_PER_FILE = 100_000
MAX_LEGACY_MINERAL_DIRECTORIES = 1_000
MAX_LEGACY_SOURCE_ENTRIES = 5_000
MAX_LEGACY_MINERAL_NAME_CHARS = 128


class MigrationReport(StrictModel):
    minerals_seen: int = Field(ge=0)
    posts_imported: int = Field(ge=0)
    comments_imported: int = Field(ge=0)
    posts_suppressed_by_tombstone: int = Field(ge=0)
    comments_suppressed_by_tombstone: int = Field(ge=0)
    invalid_posts: int = Field(ge=0)
    invalid_comments: int = Field(ge=0)
    orphan_comments: int = Field(ge=0)
    dry_run: bool


def migrate_legacy_data(database: Database, *, source: Path, dry_run: bool) -> MigrationReport:
    """Import legacy ``data/<mineral>/{posts,comments}.json`` files."""

    try:
        source_root = source.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise ValueError(f"Legacy data directory does not exist: {source}") from exc
    if not source_root.is_dir():
        raise ValueError(f"Legacy data directory does not exist: {source}")

    report = MigrationReport(
        minerals_seen=0,
        posts_imported=0,
        comments_imported=0,
        posts_suppressed_by_tombstone=0,
        comments_suppressed_by_tombstone=0,
        invalid_posts=0,
        invalid_comments=0,
        orphan_comments=0,
        dry_run=dry_run,
    )
    for mineral_index, mineral_dir in enumerate(
        _contained_mineral_directories(source_root), start=1
    ):
        posts_path = _contained_optional_file(source_root, mineral_dir / "posts.json")
        comments_path = _contained_optional_file(source_root, mineral_dir / "comments.json")
        if posts_path is None and comments_path is None:
            continue
        report.minerals_seen += 1
        mineral = " ".join(mineral_dir.name.lower().split())
        if not mineral or len(mineral_dir.name) > MAX_LEGACY_MINERAL_NAME_CHARS:
            raise ValueError(
                f"Legacy mineral directory {mineral_index} must have a non-empty name no longer "
                f"than {MAX_LEGACY_MINERAL_NAME_CHARS} characters"
            )
        raw_posts = _read_list(posts_path, record_kind="posts") if posts_path else []
        raw_comments = _read_list(comments_path, record_kind="comments") if comments_path else []

        posts: dict[str, PostRecord] = {}
        for raw in raw_posts:
            try:
                post = _legacy_post(raw)
            except (KeyError, OSError, OverflowError, TypeError, ValueError):
                report.invalid_posts += 1
                continue
            posts[post.id] = post

        deduplicated_comments: dict[str, CommentRecord] = {}
        for raw in raw_comments:
            try:
                comment = _legacy_comment(raw)
            except (KeyError, OSError, OverflowError, TypeError, ValueError):
                report.invalid_comments += 1
                continue
            deduplicated_comments[comment.id] = comment

        comments_by_post: dict[str, list[CommentRecord]] = {key: [] for key in posts}
        for comment in deduplicated_comments.values():
            if comment.post_id not in posts:
                report.orphan_comments += 1
                continue
            comments_by_post[comment.post_id].append(comment)

        for post in posts.values():
            post_comments = comments_by_post[post.id]
            if dry_run:
                if database.is_tombstoned(ContentKind.POST, post.id):
                    report.posts_suppressed_by_tombstone += 1
                    report.comments_suppressed_by_tombstone += len(post_comments)
                    continue
                report.posts_imported += 1
                for comment in post_comments:
                    if database.is_tombstoned(ContentKind.COMMENT, comment.id):
                        report.comments_suppressed_by_tombstone += 1
                    else:
                        report.comments_imported += 1
                continue

            stored = database.store_scraped_post(post, post_comments, mineral=mineral)
            if stored.post_skipped_tombstone:
                report.posts_suppressed_by_tombstone += 1
                report.comments_suppressed_by_tombstone += stored.comments_skipped_tombstone
                continue
            report.posts_imported += 1
            report.comments_imported += stored.comments_stored
            report.comments_suppressed_by_tombstone += stored.comments_skipped_tombstone
    return report


def _contained_mineral_directories(source_root: Path) -> list[Path]:
    directories: list[Path] = []
    seen: set[Path] = set()
    try:
        candidates = source_root.iterdir()
        for entry_index, candidate in enumerate(candidates, start=1):
            if entry_index > MAX_LEGACY_SOURCE_ENTRIES:
                raise ValueError(
                    f"Legacy data exceeds the {MAX_LEGACY_SOURCE_ENTRIES}-entry safety limit"
                )
            if candidate.is_symlink() or candidate.is_junction():
                raise ValueError("Legacy data must not contain directory aliases")
            if not candidate.is_dir():
                continue
            resolved = candidate.resolve(strict=True)
            if not resolved.is_relative_to(source_root):
                raise ValueError("Legacy data contains a directory outside the selected source")
            if resolved in seen:
                raise ValueError("Legacy data contains duplicate directory aliases")
            seen.add(resolved)
            directories.append(resolved)
            if len(directories) > MAX_LEGACY_MINERAL_DIRECTORIES:
                raise ValueError(
                    "Legacy data exceeds the mineral-directory safety limit of "
                    f"{MAX_LEGACY_MINERAL_DIRECTORIES}"
                )
    except (OSError, RuntimeError) as exc:
        raise ValueError("Cannot enumerate the legacy data directory") from exc
    return sorted(directories)


def _contained_optional_file(source_root: Path, path: Path) -> Path | None:
    try:
        if path.is_symlink() or path.is_junction():
            raise ValueError("Legacy data must not contain file aliases")
        resolved = path.resolve(strict=True)
    except FileNotFoundError:
        return None
    except (OSError, RuntimeError) as exc:
        raise ValueError("Cannot resolve a legacy data file") from exc
    if not resolved.is_relative_to(source_root):
        raise ValueError("Legacy data contains a file outside the selected source")
    if not resolved.is_file():
        raise ValueError("A legacy posts/comments path is not a regular file")
    return resolved


def _read_list(path: Path, *, record_kind: str) -> list[dict[str, Any]]:
    try:
        with path.open("rb") as handle:
            encoded = handle.read(MAX_LEGACY_FILE_BYTES + 1)
    except OSError as exc:
        raise ValueError(f"Cannot read legacy JSON {record_kind} file") from exc
    if len(encoded) > MAX_LEGACY_FILE_BYTES:
        raise ValueError(
            f"Legacy {record_kind} file exceeds the {MAX_LEGACY_FILE_BYTES}-byte safety limit"
        )
    try:
        value: Any = json.loads(encoded.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read legacy JSON {record_kind} file") from exc
    if not isinstance(value, list):
        raise ValueError(f"Legacy {record_kind} file must contain a JSON array")
    if len(value) > MAX_LEGACY_RECORDS_PER_FILE:
        raise ValueError(
            f"Legacy {record_kind} file exceeds the {MAX_LEGACY_RECORDS_PER_FILE}-record safety limit"
        )
    if not all(isinstance(item, dict) for item in value):
        raise ValueError(f"Legacy {record_kind} file contains non-object entries")
    return value


def _legacy_post(raw: dict[str, Any]) -> PostRecord:
    post_id = str(raw["id"])
    return PostRecord(
        id=post_id,
        title=str(raw.get("title", "")),
        selftext=str(raw.get("selftext", "")),
        subreddit=str(raw["subreddit"]),
        created_at=_legacy_datetime(raw),
        score=int(raw.get("score", 0)),
        num_comments=max(0, int(raw.get("num_comments", 0))),
        upvote_ratio=(float(raw["upvote_ratio"]) if raw.get("upvote_ratio") is not None else None),
        permalink=str(raw.get("permalink") or f"https://www.reddit.com/comments/{post_id}"),
        fetched_at=utc_now(),
    )


def _legacy_comment(raw: dict[str, Any]) -> CommentRecord:
    comment_id = str(raw["id"])
    return CommentRecord(
        id=comment_id,
        post_id=str(raw["post_id"]),
        parent_id=str(raw["parent_id"]) if raw.get("parent_id") else None,
        body=str(raw.get("body", "")),
        score=int(raw.get("score", 0)),
        created_at=_legacy_datetime(raw),
        depth=max(0, int(raw.get("level", raw.get("depth", 0)))),
        subreddit=str(raw["subreddit"]),
        permalink=str(raw.get("permalink") or f"https://www.reddit.com/comments/{comment_id}"),
        fetched_at=utc_now(),
    )


def _legacy_datetime(raw: dict[str, Any]) -> datetime:
    timestamp = raw.get("created_utc")
    if timestamp is not None:
        return datetime.fromtimestamp(float(timestamp), tz=UTC)
    text = raw.get("created_date") or raw.get("created_at")
    if not text:
        raise ValueError("record has no creation timestamp")
    value = datetime.fromisoformat(str(text).replace("Z", "+00:00"))
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
