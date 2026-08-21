"""Transactional SQLite persistence for canonical content and pipeline state."""

from __future__ import annotations

import hashlib
import importlib
import json
import sqlite3
import sys
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any, BinaryIO, cast

from reddit_minerals.errors import ConcurrentOperationError, ConfigurationError
from reddit_minerals.models import (
    AnalysisCandidateState,
    AnalysisKind,
    CommentRecord,
    ContentInput,
    ContentKind,
    PostRecord,
    ProviderResult,
    StatusSnapshot,
    StrictModel,
    WorkStatus,
    utc_now,
)

SCHEMA_VERSION = 3
_SCHEMA_VERSION_PRAGMA = f"PRAGMA user_version = {SCHEMA_VERSION:d}"


@dataclass(frozen=True, slots=True)
class ScrapeStoreResult:
    """Outcome of a tombstone-aware atomic scrape write."""

    post_stored: bool
    post_skipped_tombstone: bool
    comments_received: int
    comments_stored: int
    comments_skipped_tombstone: int
    comment_associations_removed: int


class RefreshDecision(StrEnum):
    """Reason a discovered post should be refreshed or skipped."""

    REFRESH = "refresh"
    FRESH = "fresh"
    TERMINAL = "terminal"
    TOMBSTONED = "tombstoned"


class StaleAnalysisCandidateError(RuntimeError):
    """An analysis candidate changed or disappeared before persistence."""


_SCHEMA = """
CREATE TABLE IF NOT EXISTS posts (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    selftext TEXT NOT NULL,
    subreddit TEXT NOT NULL,
    created_at TEXT NOT NULL,
    score INTEGER NOT NULL,
    num_comments INTEGER NOT NULL CHECK (num_comments >= 0),
    upvote_ratio REAL CHECK (upvote_ratio IS NULL OR upvote_ratio BETWEEN 0 AND 1),
    permalink TEXT NOT NULL,
    fetched_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS post_minerals (
    post_id TEXT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    mineral TEXT NOT NULL,
    discovered_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    scrape_status TEXT NOT NULL,
    last_error TEXT,
    PRIMARY KEY (post_id, mineral)
);

CREATE TABLE IF NOT EXISTS comments (
    id TEXT PRIMARY KEY,
    post_id TEXT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    parent_id TEXT,
    body TEXT NOT NULL,
    score INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    depth INTEGER NOT NULL CHECK (depth >= 0),
    subreddit TEXT NOT NULL,
    permalink TEXT NOT NULL,
    fetched_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS comment_minerals (
    comment_id TEXT NOT NULL REFERENCES comments(id) ON DELETE CASCADE,
    mineral TEXT NOT NULL,
    discovered_at TEXT NOT NULL,
    PRIMARY KEY (comment_id, mineral)
);

CREATE TABLE IF NOT EXISTS work_items (
    stage TEXT NOT NULL,
    content_kind TEXT NOT NULL,
    content_id TEXT NOT NULL,
    mineral TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error TEXT,
    next_retry_at TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (stage, content_kind, content_id, mineral)
);

CREATE TABLE IF NOT EXISTS analyses (
    kind TEXT NOT NULL,
    content_kind TEXT NOT NULL,
    content_id TEXT NOT NULL,
    mineral TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    prompt_version TEXT NOT NULL,
    model TEXT,
    input_revision TEXT CHECK (input_revision IS NULL OR length(input_revision) = 64),
    config_revision TEXT CHECK (config_revision IS NULL OR length(config_revision) = 64),
    dependency_revision TEXT CHECK (
        dependency_revision IS NULL OR length(dependency_revision) = 64
    ),
    result_revision TEXT CHECK (result_revision IS NULL OR length(result_revision) = 64),
    status TEXT NOT NULL,
    payload TEXT,
    error TEXT,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    provider_request_id TEXT,
    input_tokens INTEGER,
    output_tokens INTEGER,
    latency_ms INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (kind, content_kind, content_id, mineral)
);

CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    command TEXT NOT NULL,
    parameters TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    summary TEXT,
    error_type TEXT
);

CREATE TABLE IF NOT EXISTS content_tombstones (
    content_kind TEXT NOT NULL CHECK (content_kind IN ('post', 'comment')),
    content_id TEXT NOT NULL,
    deleted_at TEXT NOT NULL,
    cascade_from_post_id TEXT,
    PRIMARY KEY (content_kind, content_id)
);

CREATE INDEX IF NOT EXISTS idx_post_minerals_mineral
    ON post_minerals(mineral, scrape_status, updated_at);
CREATE INDEX IF NOT EXISTS idx_comments_post
    ON comments(post_id, score DESC);
CREATE INDEX IF NOT EXISTS idx_comment_minerals_mineral
    ON comment_minerals(mineral);
CREATE INDEX IF NOT EXISTS idx_work_status
    ON work_items(stage, status, mineral);
CREATE INDEX IF NOT EXISTS idx_analyses_lookup
    ON analyses(kind, status, mineral);
CREATE INDEX IF NOT EXISTS idx_tombstones_cascade
    ON content_tombstones(cascade_from_post_id);
"""

_MIGRATION_1_TO_2 = (
    """
    CREATE TABLE IF NOT EXISTS content_tombstones (
        content_kind TEXT NOT NULL CHECK (content_kind IN ('post', 'comment')),
        content_id TEXT NOT NULL,
        deleted_at TEXT NOT NULL,
        cascade_from_post_id TEXT,
        PRIMARY KEY (content_kind, content_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_tombstones_cascade
        ON content_tombstones(cascade_from_post_id)
    """,
)

_MIGRATION_2_TO_3_COLUMNS = {
    "input_revision": (
        "ALTER TABLE analyses ADD COLUMN input_revision TEXT "
        "CHECK (input_revision IS NULL OR length(input_revision) = 64)"
    ),
    "config_revision": (
        "ALTER TABLE analyses ADD COLUMN config_revision TEXT "
        "CHECK (config_revision IS NULL OR length(config_revision) = 64)"
    ),
    "dependency_revision": (
        "ALTER TABLE analyses ADD COLUMN dependency_revision TEXT "
        "CHECK (dependency_revision IS NULL OR length(dependency_revision) = 64)"
    ),
    "result_revision": (
        "ALTER TABLE analyses ADD COLUMN result_revision TEXT "
        "CHECK (result_revision IS NULL OR length(result_revision) = 64)"
    ),
}

_REQUIRED_SCHEMA_COLUMNS = {
    "posts": frozenset(
        {
            "id",
            "title",
            "selftext",
            "subreddit",
            "created_at",
            "score",
            "num_comments",
            "upvote_ratio",
            "permalink",
            "fetched_at",
        }
    ),
    "post_minerals": frozenset(
        {"post_id", "mineral", "discovered_at", "updated_at", "scrape_status", "last_error"}
    ),
    "comments": frozenset(
        {
            "id",
            "post_id",
            "parent_id",
            "body",
            "score",
            "created_at",
            "depth",
            "subreddit",
            "permalink",
            "fetched_at",
        }
    ),
    "comment_minerals": frozenset({"comment_id", "mineral", "discovered_at"}),
    "work_items": frozenset(
        {
            "stage",
            "content_kind",
            "content_id",
            "mineral",
            "status",
            "attempts",
            "last_error",
            "next_retry_at",
            "updated_at",
        }
    ),
    "analyses": frozenset(
        {
            "kind",
            "content_kind",
            "content_id",
            "mineral",
            "schema_version",
            "prompt_version",
            "model",
            "input_revision",
            "config_revision",
            "dependency_revision",
            "result_revision",
            "status",
            "payload",
            "error",
            "attempts",
            "provider_request_id",
            "input_tokens",
            "output_tokens",
            "latency_ms",
            "created_at",
            "updated_at",
        }
    ),
    "runs": frozenset(
        {
            "id",
            "command",
            "parameters",
            "status",
            "started_at",
            "finished_at",
            "summary",
            "error_type",
        }
    ),
    "content_tombstones": frozenset(
        {"content_kind", "content_id", "deleted_at", "cascade_from_post_id"}
    ),
}


class Database:
    """Small repository layer; each public write is an atomic transaction."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._operation_lock_held = False

    @property
    def operation_lock_path(self) -> Path:
        """Return the stable advisory-lock path associated with this database."""

        database_path = self.path.resolve(strict=False)
        return database_path.with_name(f"{database_path.name}.operation.lock")

    @contextmanager
    def operation_lock(self) -> Iterator[None]:
        """Prevent overlapping supported writer commands across processes."""

        if self._operation_lock_held:
            raise ConcurrentOperationError("This database operation lock is already held")
        lock_path = self.operation_lock_path
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a+b") as handle:
            try:
                _acquire_operation_file_lock(handle)
            except OSError as exc:
                raise ConcurrentOperationError(
                    "Another tracked command is already using this database"
                ) from exc
            self._operation_lock_held = True
            try:
                yield
            finally:
                self._operation_lock_held = False
                _release_operation_file_lock(handle)

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection(configure_durability=False) as connection:
            current = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if current > SCHEMA_VERSION:
                raise ConfigurationError(
                    f"Database schema {current} is newer than supported schema {SCHEMA_VERSION}"
                )

            journal_mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()
            if journal_mode is None or str(journal_mode[0]).lower() != "wal":
                raise ConfigurationError("SQLite WAL journal mode is unavailable")
            connection.execute("PRAGMA synchronous = FULL")
            if current < SCHEMA_VERSION:
                # sqlite3 does not implicitly begin a transaction for DDL. An
                # explicit write transaction keeps every migration step and the
                # user_version advance on the same rollback boundary. Re-read
                # under the writer reservation so concurrent initializers can
                # safely converge on the schema installed by the first process.
                connection.execute("BEGIN IMMEDIATE")
                current = int(connection.execute("PRAGMA user_version").fetchone()[0])
                if current > SCHEMA_VERSION:
                    raise ConfigurationError(
                        f"Database schema {current} is newer than supported schema {SCHEMA_VERSION}"
                    )
                if current == 0:
                    user_objects = connection.execute(
                        """
                        SELECT name FROM sqlite_schema
                        WHERE name NOT LIKE 'sqlite_%'
                        LIMIT 1
                        """
                    ).fetchone()
                    if user_objects is not None:
                        raise ConfigurationError(
                            "Database has application objects but no supported schema version"
                        )
                    for statement in _schema_statements(_SCHEMA):
                        connection.execute(statement)
                    self._validate_schema(connection)
                    connection.execute(_SCHEMA_VERSION_PRAGMA)
                elif current < SCHEMA_VERSION:
                    self._migrate(connection, current)
                    self._validate_schema(connection)
                else:
                    self._validate_schema(connection)
            else:
                self._validate_schema(connection)

    @contextmanager
    def _connection(self, *, configure_durability: bool = True) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        if configure_durability:
            connection.execute("PRAGMA synchronous = FULL")
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    @staticmethod
    def _migrate(connection: sqlite3.Connection, current: int) -> None:
        while current < SCHEMA_VERSION:
            if current == 1:
                run_columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(runs)")}
                if "parameters" not in run_columns:
                    connection.execute(
                        "ALTER TABLE runs ADD COLUMN parameters TEXT NOT NULL DEFAULT '{}'"
                    )
                for statement in _MIGRATION_1_TO_2:
                    connection.execute(statement)
                connection.execute("PRAGMA user_version = 2")
                current = 2
                continue
            if current == 2:
                analysis_columns = {
                    str(row[1]) for row in connection.execute("PRAGMA table_info(analyses)")
                }
                if not analysis_columns:
                    raise ConfigurationError("Schema 2 database is missing the analyses table")
                for column, statement in _MIGRATION_2_TO_3_COLUMNS.items():
                    if column not in analysis_columns:
                        connection.execute(statement)
                connection.execute("PRAGMA user_version = 3")
                current = 3
                continue
            raise ConfigurationError(f"No migration path from database schema {current}")

    @staticmethod
    def _validate_schema(connection: sqlite3.Connection) -> None:
        for table, expected_columns in _REQUIRED_SCHEMA_COLUMNS.items():
            actual_columns = {
                str(row[0])
                for row in connection.execute("SELECT name FROM pragma_table_info(?)", (table,))
            }
            if actual_columns != expected_columns:
                raise ConfigurationError(f"Database table {table!r} does not match schema")

    @contextmanager
    def write_barrier(self) -> Iterator[None]:
        """Hold SQLite's cross-process writer reservation across caller work."""

        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            yield

    def start_run(self, command: str, parameters: Mapping[str, Any] | None = None) -> str:
        """Start an audit row; ``parameters`` must contain no secrets or raw content."""

        run_id = uuid.uuid4().hex
        with self._connection() as connection:
            connection.execute(
                """
                INSERT INTO runs(id, command, parameters, status, started_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    command,
                    _json(dict(parameters or {})),
                    "running",
                    _iso(utc_now()),
                ),
            )
        return run_id

    def reconcile_stale_runs(self) -> int:
        """Mark interrupted rows failed while holding exclusive operation ownership."""

        if not self._operation_lock_held:
            raise RuntimeError("Run reconciliation requires the database operation lock")

        with self._connection() as connection:
            reconciled = connection.execute(
                """
                UPDATE runs
                SET status = 'failed', finished_at = ?, summary = COALESCE(summary, '{}'),
                    error_type = 'InterruptedRun'
                WHERE status = 'running'
                """,
                (_iso(utc_now()),),
            ).rowcount
        return reconciled

    def finish_run(
        self,
        run_id: str,
        *,
        success: bool,
        summary: dict[str, Any],
        error_type: str | None = None,
    ) -> None:
        with self._connection() as connection:
            updated = connection.execute(
                """
                UPDATE runs
                SET status = ?, finished_at = ?, summary = ?, error_type = ?
                WHERE id = ?
                """,
                (
                    "complete" if success else "failed",
                    _iso(utc_now()),
                    _json(summary),
                    error_type,
                    run_id,
                ),
            ).rowcount
            if updated != 1:
                raise RuntimeError("Run audit row does not exist")

    def should_refresh(
        self, post_id: str, mineral: str, after: timedelta, *, force: bool = False
    ) -> bool:
        return (
            self.refresh_decision(post_id, mineral, after, force=force) is RefreshDecision.REFRESH
        )

    def refresh_decision(
        self, post_id: str, mineral: str, after: timedelta, *, force: bool = False
    ) -> RefreshDecision:
        """Classify resume state without conflating freshness and terminal work."""

        with self._connection() as connection:
            if self._is_tombstoned(connection, ContentKind.POST, post_id):
                return RefreshDecision.TOMBSTONED
            row = connection.execute(
                """
                SELECT updated_at, scrape_status FROM post_minerals
                WHERE post_id = ? AND mineral = ?
                """,
                (post_id, mineral),
            ).fetchone()
        if force:
            return RefreshDecision.REFRESH
        if row is None:
            return RefreshDecision.REFRESH
        if row["scrape_status"] in {
            WorkStatus.PERMANENT_FAILURE.value,
            WorkStatus.BLOCKED.value,
        }:
            return RefreshDecision.TERMINAL
        if row["scrape_status"] != WorkStatus.COMPLETE.value:
            return RefreshDecision.REFRESH
        updated = datetime.fromisoformat(str(row["updated_at"])).astimezone(UTC)
        if utc_now() - updated >= after:
            return RefreshDecision.REFRESH
        return RefreshDecision.FRESH

    def is_tombstoned(self, content_kind: ContentKind, content_id: str) -> bool:
        """Return whether an explicit deletion permanently suppresses an ID."""

        with self._connection() as connection:
            return self._is_tombstoned(connection, content_kind, content_id)

    @staticmethod
    def _is_tombstoned(
        connection: sqlite3.Connection, content_kind: ContentKind, content_id: str
    ) -> bool:
        return bool(
            _scalar(
                connection,
                """
                SELECT COUNT(*) FROM content_tombstones
                WHERE content_kind = ? AND content_id = ?
                """,
                (content_kind.value, content_id),
            )
        )

    def store_scraped_post(
        self,
        post: PostRecord,
        comments: Sequence[CommentRecord],
        *,
        mineral: str,
        status: WorkStatus = WorkStatus.COMPLETE,
        error: str | None = None,
        comment_snapshot_complete: bool = False,
    ) -> ScrapeStoreResult:
        """Atomically persist a tombstone-aware post and optional comment snapshot.

        Comment associations are reconciled only when the caller explicitly marks
        the provider snapshot complete. Bounded or otherwise partial snapshots are
        additive and can never erase older associations.
        """

        comment_ids: set[str] = set()
        for comment in comments:
            if comment.post_id != post.id:
                raise ValueError(
                    f"Comment {comment.id} belongs to {comment.post_id}, expected {post.id}"
                )
            if comment.id in comment_ids:
                raise ValueError(f"Duplicate comment ID in snapshot: {comment.id}")
            comment_ids.add(comment.id)

        now = _iso(utc_now())
        with self._connection() as connection:
            # The tombstone check and all subsequent upserts must share the
            # writer reservation with deletion; otherwise a delete can commit
            # between the check and insert and the scrape could resurrect data.
            connection.execute("BEGIN IMMEDIATE")
            if self._is_tombstoned(connection, ContentKind.POST, post.id):
                return ScrapeStoreResult(
                    post_stored=False,
                    post_skipped_tombstone=True,
                    comments_received=len(comments),
                    comments_stored=0,
                    comments_skipped_tombstone=len(comments),
                    comment_associations_removed=0,
                )

            if self._upsert_post(connection, post):
                self._invalidate_post_analyses(connection, post.id)
            connection.execute(
                """
                INSERT INTO post_minerals(
                    post_id, mineral, discovered_at, updated_at, scrape_status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(post_id, mineral) DO UPDATE SET
                    updated_at = excluded.updated_at,
                    scrape_status = excluded.scrape_status,
                    last_error = excluded.last_error
                """,
                (post.id, mineral, now, now, status.value, _safe_error(error)),
            )

            stored_comment_ids: set[str] = set()
            tombstoned_comments = 0
            reputation_context_changed = False
            for comment in comments:
                if self._is_tombstoned(connection, ContentKind.COMMENT, comment.id):
                    tombstoned_comments += 1
                    continue

                existing_minerals = self._comment_minerals(connection, comment.id)
                comment_changed = self._upsert_comment(connection, comment)
                inserted = connection.execute(
                    """
                    INSERT INTO comment_minerals(comment_id, mineral, discovered_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(comment_id, mineral) DO NOTHING
                    """,
                    (comment.id, mineral, now),
                ).rowcount
                stored_comment_ids.add(comment.id)

                if comment_changed:
                    affected_minerals = existing_minerals | {mineral}
                    self._invalidate_comment_enrichment(connection, comment.id, affected_minerals)
                    self._invalidate_reputation(connection, post.id, affected_minerals)
                    reputation_context_changed = True
                elif inserted:
                    reputation_context_changed = True

            associations_removed = 0
            if comment_snapshot_complete:
                associations_removed = self._reconcile_comment_associations(
                    connection,
                    post_id=post.id,
                    mineral=mineral,
                    retained_comment_ids=stored_comment_ids,
                )
                reputation_context_changed = reputation_context_changed or associations_removed > 0

            if reputation_context_changed:
                self._invalidate_reputation(connection, post.id, {mineral})

            self._upsert_work(
                connection,
                stage="scrape",
                content_kind=ContentKind.POST,
                content_id=post.id,
                mineral=mineral,
                status=status,
                error=error,
                increment_attempt=True,
            )

        return ScrapeStoreResult(
            post_stored=True,
            post_skipped_tombstone=False,
            comments_received=len(comments),
            comments_stored=len(stored_comment_ids),
            comments_skipped_tombstone=tombstoned_comments,
            comment_associations_removed=associations_removed,
        )

    @staticmethod
    def _upsert_post(connection: sqlite3.Connection, post: PostRecord) -> bool:
        existing = connection.execute(
            """
            SELECT title, selftext, subreddit, score, upvote_ratio
            FROM posts WHERE id = ?
            """,
            (post.id,),
        ).fetchone()
        prompt_changed = existing is not None and (
            str(existing["title"]) != post.title
            or str(existing["selftext"]) != post.selftext
            or str(existing["subreddit"]) != post.subreddit
            or int(existing["score"]) != post.score
            or _optional_float(existing["upvote_ratio"]) != post.upvote_ratio
        )
        connection.execute(
            """
            INSERT INTO posts(
                id, title, selftext, subreddit, created_at, score, num_comments,
                upvote_ratio, permalink, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                title = excluded.title,
                selftext = excluded.selftext,
                subreddit = excluded.subreddit,
                created_at = excluded.created_at,
                score = excluded.score,
                num_comments = excluded.num_comments,
                upvote_ratio = excluded.upvote_ratio,
                permalink = excluded.permalink,
                fetched_at = excluded.fetched_at
            """,
            (
                post.id,
                post.title,
                post.selftext,
                post.subreddit,
                _iso(post.created_at),
                post.score,
                post.num_comments,
                post.upvote_ratio,
                post.permalink,
                _iso(post.fetched_at),
            ),
        )
        return prompt_changed

    @staticmethod
    def _upsert_comment(connection: sqlite3.Connection, comment: CommentRecord) -> bool:
        existing = connection.execute(
            """
            SELECT post_id, body, subreddit, score FROM comments WHERE id = ?
            """,
            (comment.id,),
        ).fetchone()
        if existing is not None and str(existing["post_id"]) != comment.post_id:
            raise ValueError(
                f"Comment {comment.id} is already attached to post {existing['post_id']}"
            )
        prompt_changed = existing is not None and (
            str(existing["body"]) != comment.body
            or str(existing["subreddit"]) != comment.subreddit
            or int(existing["score"]) != comment.score
        )
        connection.execute(
            """
            INSERT INTO comments(
                id, post_id, parent_id, body, score, created_at, depth,
                subreddit, permalink, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                post_id = excluded.post_id,
                parent_id = excluded.parent_id,
                body = excluded.body,
                score = excluded.score,
                created_at = excluded.created_at,
                depth = excluded.depth,
                subreddit = excluded.subreddit,
                permalink = excluded.permalink,
                fetched_at = excluded.fetched_at
            """,
            (
                comment.id,
                comment.post_id,
                comment.parent_id,
                comment.body,
                comment.score,
                _iso(comment.created_at),
                comment.depth,
                comment.subreddit,
                comment.permalink,
                _iso(comment.fetched_at),
            ),
        )
        return prompt_changed

    @staticmethod
    def _comment_minerals(connection: sqlite3.Connection, comment_id: str) -> set[str]:
        return {
            str(row["mineral"])
            for row in connection.execute(
                "SELECT mineral FROM comment_minerals WHERE comment_id = ?",
                (comment_id,),
            )
        }

    @staticmethod
    def _invalidate_post_analyses(connection: sqlite3.Connection, post_id: str) -> None:
        connection.execute(
            "DELETE FROM analyses WHERE content_kind = 'post' AND content_id = ?",
            (post_id,),
        )
        connection.execute(
            """
            DELETE FROM work_items
            WHERE content_kind = 'post' AND content_id = ?
              AND stage IN ('relevance', 'enrichment', 'reputation')
            """,
            (post_id,),
        )

    @staticmethod
    def _invalidate_comment_enrichment(
        connection: sqlite3.Connection,
        comment_id: str,
        minerals: set[str],
    ) -> None:
        for mineral in minerals:
            connection.execute(
                """
                DELETE FROM analyses
                WHERE kind = 'enrichment' AND content_kind = 'comment'
                  AND content_id = ? AND mineral = ?
                """,
                (comment_id, mineral),
            )
            connection.execute(
                """
                DELETE FROM work_items
                WHERE stage = 'enrichment' AND content_kind = 'comment'
                  AND content_id = ? AND mineral = ?
                """,
                (comment_id, mineral),
            )

    @staticmethod
    def _invalidate_reputation(
        connection: sqlite3.Connection,
        post_id: str,
        minerals: set[str],
    ) -> None:
        for mineral in minerals:
            connection.execute(
                """
                DELETE FROM analyses
                WHERE kind = 'reputation' AND content_kind = 'post'
                  AND content_id = ? AND mineral = ?
                """,
                (post_id, mineral),
            )
            connection.execute(
                """
                DELETE FROM work_items
                WHERE stage = 'reputation' AND content_kind = 'post'
                  AND content_id = ? AND mineral = ?
                """,
                (post_id, mineral),
            )

    @staticmethod
    def _reconcile_comment_associations(
        connection: sqlite3.Connection,
        *,
        post_id: str,
        mineral: str,
        retained_comment_ids: set[str],
    ) -> int:
        existing_ids = {
            str(row["id"])
            for row in connection.execute(
                """
                SELECT c.id FROM comments c
                JOIN comment_minerals cm ON cm.comment_id = c.id
                WHERE c.post_id = ? AND cm.mineral = ?
                """,
                (post_id, mineral),
            )
        }
        removed_ids = existing_ids - retained_comment_ids
        for comment_id in removed_ids:
            connection.execute(
                """
                DELETE FROM analyses
                WHERE content_kind = 'comment' AND content_id = ? AND mineral = ?
                """,
                (comment_id, mineral),
            )
            connection.execute(
                """
                DELETE FROM work_items
                WHERE content_kind = 'comment' AND content_id = ? AND mineral = ?
                """,
                (comment_id, mineral),
            )
            connection.execute(
                """
                DELETE FROM comment_minerals
                WHERE comment_id = ? AND mineral = ?
                """,
                (comment_id, mineral),
            )
            connection.execute(
                """
                DELETE FROM comments
                WHERE id = ?
                  AND NOT EXISTS (
                    SELECT 1 FROM comment_minerals WHERE comment_id = ?
                  )
                """,
                (comment_id, comment_id),
            )
        return len(removed_ids)

    @staticmethod
    def _upsert_work(
        connection: sqlite3.Connection,
        *,
        stage: str,
        content_kind: ContentKind,
        content_id: str,
        mineral: str,
        status: WorkStatus,
        error: str | None,
        increment_attempt: bool,
    ) -> None:
        increment = 1 if increment_attempt else 0
        connection.execute(
            """
            INSERT INTO work_items(
                stage, content_kind, content_id, mineral, status, attempts,
                last_error, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(stage, content_kind, content_id, mineral) DO UPDATE SET
                status = excluded.status,
                attempts = work_items.attempts + ?,
                last_error = excluded.last_error,
                next_retry_at = NULL,
                updated_at = excluded.updated_at
            """,
            (
                stage,
                content_kind.value,
                content_id,
                mineral,
                status.value,
                increment,
                _safe_error(error),
                _iso(utc_now()),
                increment,
            ),
        )

    def analysis_candidates(
        self,
        kind: AnalysisKind,
        *,
        mineral: str | None,
        limit: int,
        force: bool,
        relevance_threshold: float,
        max_context_comments: int,
        max_content_chars: int = 12_000,
        schema_version: int | None = None,
        prompt_version: str | None = None,
        model: str | None = None,
    ) -> list[ContentInput]:
        """Return bounded eligible content, respecting supplied provenance."""

        config_revision = _analysis_config_revision(
            kind=kind,
            schema_version=schema_version,
            prompt_version=prompt_version,
            model=model,
            relevance_threshold=relevance_threshold,
            max_context_comments=max_context_comments,
            max_content_chars=max_content_chars,
        )
        relevance_config_revision = _analysis_config_revision(
            kind=AnalysisKind.RELEVANCE,
            schema_version=schema_version,
            prompt_version=prompt_version,
            model=model,
            relevance_threshold=relevance_threshold,
            max_context_comments=max_context_comments,
            max_content_chars=max_content_chars,
        )
        with self._connection() as connection:
            # Candidate eligibility, provider input, dependencies, and the CAS
            # base must come from one WAL snapshot. A plain sqlite3 SELECT does
            # not implicitly open a transaction.
            connection.execute("BEGIN")
            posts = self._post_candidate_rows(
                connection,
                kind=kind,
                mineral=mineral,
                limit=limit,
                force=force,
                relevance_threshold=relevance_threshold,
                schema_version=schema_version,
                prompt_version=prompt_version,
                model=model,
                config_revision=config_revision,
                relevance_config_revision=relevance_config_revision,
            )
            post_inputs: list[ContentInput] = []
            for row in posts:
                context_rows = (
                    self._comment_context_rows(
                        connection,
                        post_id=str(row["id"]),
                        mineral=str(row["mineral"]),
                        limit=max_context_comments,
                    )
                    if kind is AnalysisKind.REPUTATION
                    else []
                )
                content = _content_from_post(
                    row,
                    [str(context_row["body"]) for context_row in context_rows],
                )
                post_inputs.append(
                    self._with_candidate_state(
                        connection,
                        kind=kind,
                        content=content,
                        source_manifest=_post_source_manifest(row, context_rows),
                        schema_version=schema_version,
                        prompt_version=prompt_version,
                        model=model,
                        relevance_threshold=relevance_threshold,
                        max_context_comments=max_context_comments,
                        max_content_chars=max_content_chars,
                    )
                )
            if kind is not AnalysisKind.ENRICHMENT:
                return post_inputs[:limit]

            comment_inputs: list[ContentInput] = []
            for row in self._comment_candidate_rows(
                connection,
                mineral=mineral,
                limit=limit,
                force=force,
                schema_version=schema_version,
                prompt_version=prompt_version,
                model=model,
                config_revision=config_revision,
            ):
                content = _content_from_comment(row)
                comment_inputs.append(
                    self._with_candidate_state(
                        connection,
                        kind=kind,
                        content=content,
                        source_manifest=_comment_source_manifest(row),
                        schema_version=schema_version,
                        prompt_version=prompt_version,
                        model=model,
                        relevance_threshold=relevance_threshold,
                        max_context_comments=max_context_comments,
                        max_content_chars=max_content_chars,
                    )
                )
            counts = {ContentKind.POST: 0, ContentKind.COMMENT: 0}
            for row in connection.execute(
                """
                SELECT content_kind, COUNT(*) AS count
                FROM analyses
                WHERE kind = 'enrichment' AND (? IS NULL OR mineral = ?)
                GROUP BY content_kind
                """,
                (mineral, mineral),
            ):
                counts[ContentKind(str(row["content_kind"]))] = int(row["count"])

            queues = (
                (comment_inputs, post_inputs)
                if counts[ContentKind.COMMENT] < counts[ContentKind.POST]
                else (post_inputs, comment_inputs)
            )
            result: list[ContentInput] = []
            for index in range(max(len(post_inputs), len(comment_inputs))):
                for queue in queues:
                    if index < len(queue):
                        result.append(queue[index])
                        if len(result) >= limit:
                            return result
            return result

    @staticmethod
    def _post_candidate_rows(
        connection: sqlite3.Connection,
        *,
        kind: AnalysisKind,
        mineral: str | None,
        limit: int,
        force: bool,
        relevance_threshold: float,
        schema_version: int | None,
        prompt_version: str | None,
        model: str | None,
        config_revision: str,
        relevance_config_revision: str,
    ) -> list[sqlite3.Row]:
        return list(
            connection.execute(
                """
                SELECT p.*, pm.mineral
                FROM posts p
                JOIN post_minerals pm ON pm.post_id = p.id
                LEFT JOIN work_items wi
                  ON wi.stage = ? AND wi.content_kind = 'post'
                 AND wi.content_id = p.id AND wi.mineral = pm.mineral
                WHERE pm.scrape_status = ?
                  AND (? IS NULL OR pm.mineral = ?)
                  AND (? = 1 OR NOT EXISTS (
                    SELECT 1 FROM analyses a
                    WHERE a.kind = ? AND a.content_kind = 'post'
                      AND a.content_id = p.id AND a.mineral = pm.mineral
                      AND a.status IN ('complete', 'permanent_failure', 'blocked')
                      AND (? IS NULL OR a.schema_version = ?)
                      AND (? IS NULL OR a.prompt_version = ?)
                      AND (? IS NULL OR a.model = ?)
                      AND a.config_revision = ?
                      AND a.input_revision IS NOT NULL
                  ))
                  AND (? != 'reputation' OR EXISTS (
                    SELECT 1 FROM analyses relevance
                    WHERE relevance.kind = 'relevance'
                      AND relevance.content_kind = 'post'
                      AND relevance.content_id = p.id
                      AND relevance.mineral = pm.mineral
                      AND relevance.status = 'complete'
                      AND (? IS NULL OR relevance.schema_version = ?)
                      AND (? IS NULL OR relevance.prompt_version = ?)
                      AND (? IS NULL OR relevance.model = ?)
                      AND relevance.config_revision = ?
                      AND relevance.input_revision IS NOT NULL
                      AND relevance.result_revision IS NOT NULL
                      AND json_extract(relevance.payload, '$.relevant') = 1
                      AND CAST(json_extract(relevance.payload, '$.confidence') AS REAL) >= ?
                  ))
                ORDER BY
                    COALESCE(wi.updated_at, p.created_at) ASC,
                    p.id, pm.mineral
                LIMIT ?
                """,
                (
                    kind.value,
                    WorkStatus.COMPLETE.value,
                    mineral,
                    mineral,
                    int(force),
                    kind.value,
                    schema_version,
                    schema_version,
                    prompt_version,
                    prompt_version,
                    model,
                    model,
                    config_revision,
                    kind.value,
                    schema_version,
                    schema_version,
                    prompt_version,
                    prompt_version,
                    model,
                    model,
                    relevance_config_revision,
                    relevance_threshold,
                    limit,
                ),
            ).fetchall()
        )

    @staticmethod
    def _comment_candidate_rows(
        connection: sqlite3.Connection,
        *,
        mineral: str | None,
        limit: int,
        force: bool,
        schema_version: int | None,
        prompt_version: str | None,
        model: str | None,
        config_revision: str,
    ) -> list[sqlite3.Row]:
        return list(
            connection.execute(
                """
                SELECT c.*, cm.mineral
                FROM comments c
                JOIN comment_minerals cm ON cm.comment_id = c.id
                LEFT JOIN work_items wi
                  ON wi.stage = 'enrichment' AND wi.content_kind = 'comment'
                 AND wi.content_id = c.id AND wi.mineral = cm.mineral
                WHERE (? IS NULL OR cm.mineral = ?)
                  AND (? = 1 OR NOT EXISTS (
                    SELECT 1 FROM analyses a
                    WHERE a.kind = 'enrichment' AND a.content_kind = 'comment'
                      AND a.content_id = c.id AND a.mineral = cm.mineral
                      AND a.status IN ('complete', 'permanent_failure', 'blocked')
                      AND (? IS NULL OR a.schema_version = ?)
                      AND (? IS NULL OR a.prompt_version = ?)
                      AND (? IS NULL OR a.model = ?)
                      AND a.config_revision = ?
                      AND a.input_revision IS NOT NULL
                  ))
                ORDER BY
                    COALESCE(wi.updated_at, c.created_at) ASC,
                    c.id, cm.mineral
                LIMIT ?
                """,
                (
                    mineral,
                    mineral,
                    int(force),
                    schema_version,
                    schema_version,
                    prompt_version,
                    prompt_version,
                    model,
                    model,
                    config_revision,
                    limit,
                ),
            ).fetchall()
        )

    @staticmethod
    def _comment_context_rows(
        connection: sqlite3.Connection,
        *,
        post_id: str,
        mineral: str,
        limit: int,
    ) -> list[sqlite3.Row]:
        if limit <= 0:
            return []
        return list(
            connection.execute(
                """
                SELECT c.id, c.body, c.score, c.created_at
                FROM comments c
                JOIN comment_minerals cm ON cm.comment_id = c.id
                WHERE c.post_id = ? AND cm.mineral = ?
                ORDER BY c.score DESC, c.created_at ASC, c.id ASC
                LIMIT ?
                """,
                (post_id, mineral, limit),
            ).fetchall()
        )

    def _with_candidate_state(
        self,
        connection: sqlite3.Connection,
        *,
        kind: AnalysisKind,
        content: ContentInput,
        source_manifest: dict[str, Any],
        schema_version: int | None,
        prompt_version: str | None,
        model: str | None,
        relevance_threshold: float,
        max_context_comments: int,
        max_content_chars: int,
    ) -> ContentInput:
        normalized_threshold = (
            float(relevance_threshold) if kind is AnalysisKind.REPUTATION else 0.0
        )
        normalized_context = max_context_comments if kind is AnalysisKind.REPUTATION else 0
        config_revision = _analysis_config_revision(
            kind=kind,
            schema_version=schema_version,
            prompt_version=prompt_version,
            model=model,
            relevance_threshold=normalized_threshold,
            max_context_comments=normalized_context,
            max_content_chars=max_content_chars,
        )
        dependency_revision = self._relevance_dependency_revision(
            connection,
            kind=kind,
            content=content,
            schema_version=schema_version,
            prompt_version=prompt_version,
            model=model,
            max_content_chars=max_content_chars,
        )
        input_revision = _revision(
            {
                "config_revision": config_revision,
                "dependency_revision": dependency_revision,
                "source": source_manifest,
            }
        )
        state = AnalysisCandidateState(
            kind=kind,
            schema_version=schema_version,
            prompt_version=prompt_version,
            model=model,
            relevance_threshold=normalized_threshold,
            max_context_comments=normalized_context,
            max_content_chars=max_content_chars,
            config_revision=config_revision,
            input_revision=input_revision,
            dependency_revision=dependency_revision,
            expected_analysis_revision=self._current_analysis_revision(
                connection,
                kind=kind,
                content=content,
            ),
            base_result_revision=self._current_result_revision(
                connection,
                kind=kind,
                content=content,
            ),
        )
        return content.model_copy(update={"analysis_state": state})

    @staticmethod
    def _relevance_dependency_revision(
        connection: sqlite3.Connection,
        *,
        kind: AnalysisKind,
        content: ContentInput,
        schema_version: int | None,
        prompt_version: str | None,
        model: str | None,
        max_content_chars: int,
    ) -> str | None:
        if kind is not AnalysisKind.REPUTATION:
            return None
        relevance_config_revision = _analysis_config_revision(
            kind=AnalysisKind.RELEVANCE,
            schema_version=schema_version,
            prompt_version=prompt_version,
            model=model,
            relevance_threshold=0.0,
            max_context_comments=0,
            max_content_chars=max_content_chars,
        )
        row = connection.execute(
            """
            SELECT result_revision FROM analyses
            WHERE kind = 'relevance' AND content_kind = 'post'
              AND content_id = ? AND mineral = ? AND status = 'complete'
              AND (? IS NULL OR schema_version = ?)
              AND (? IS NULL OR prompt_version = ?)
              AND (? IS NULL OR model = ?)
              AND config_revision = ?
              AND input_revision IS NOT NULL
              AND result_revision IS NOT NULL
            """,
            (
                content.content_id,
                content.mineral,
                schema_version,
                schema_version,
                prompt_version,
                prompt_version,
                model,
                model,
                relevance_config_revision,
            ),
        ).fetchone()
        return str(row["result_revision"]) if row is not None else None

    @staticmethod
    def _current_analysis_revision(
        connection: sqlite3.Connection,
        *,
        kind: AnalysisKind,
        content: ContentInput,
    ) -> str | None:
        row = connection.execute(
            """
            SELECT kind, content_kind, content_id, mineral, schema_version,
                   prompt_version, model, input_revision, config_revision,
                   dependency_revision, result_revision, status, payload, error,
                   attempts, updated_at
            FROM analyses
            WHERE kind = ? AND content_kind = ? AND content_id = ? AND mineral = ?
            """,
            (kind.value, content.kind.value, content.content_id, content.mineral),
        ).fetchone()
        return _analysis_row_revision(row) if row is not None else None

    @staticmethod
    def _current_result_revision(
        connection: sqlite3.Connection,
        *,
        kind: AnalysisKind,
        content: ContentInput,
    ) -> str | None:
        row = connection.execute(
            """
            SELECT kind, content_kind, content_id, mineral, schema_version,
                   prompt_version, model, input_revision, config_revision,
                   dependency_revision, result_revision, status, payload, error
            FROM analyses
            WHERE kind = ? AND content_kind = ? AND content_id = ? AND mineral = ?
            """,
            (kind.value, content.kind.value, content.content_id, content.mineral),
        ).fetchone()
        return _effective_result_revision(row) if row is not None else None

    def _validate_candidate(
        self,
        connection: sqlite3.Connection,
        *,
        kind: AnalysisKind,
        content: ContentInput,
        schema_version: int,
        prompt_version: str,
        model: str | None,
    ) -> AnalysisCandidateState:
        state = content.analysis_state
        if state is None:
            raise ValueError("analysis content must come from Database.analysis_candidates")
        if state.kind is not kind:
            raise ValueError("analysis candidate kind does not match the persistence operation")
        if state.schema_version is not None and state.schema_version != schema_version:
            raise ValueError("analysis candidate schema provenance does not match")
        if state.prompt_version is not None and state.prompt_version != prompt_version:
            raise ValueError("analysis candidate prompt provenance does not match")
        if state.model is not None and state.model != model:
            raise ValueError("analysis candidate model provenance does not match")
        if self._is_tombstoned(connection, content.kind, content.content_id):
            raise self._stale_candidate_error(kind, content)

        if content.kind is ContentKind.POST:
            row = connection.execute(
                """
                SELECT p.*, pm.mineral
                FROM posts p JOIN post_minerals pm ON pm.post_id = p.id
                WHERE p.id = ? AND pm.mineral = ? AND pm.scrape_status = ?
                """,
                (content.content_id, content.mineral, WorkStatus.COMPLETE.value),
            ).fetchone()
            if row is None:
                raise self._stale_candidate_error(kind, content)
            context_rows = (
                self._comment_context_rows(
                    connection,
                    post_id=content.content_id,
                    mineral=content.mineral,
                    limit=state.max_context_comments,
                )
                if kind is AnalysisKind.REPUTATION
                else []
            )
            current_content = _content_from_post(
                row,
                [str(context_row["body"]) for context_row in context_rows],
            )
            source_manifest = _post_source_manifest(row, context_rows)
        else:
            row = connection.execute(
                """
                SELECT c.*, cm.mineral
                FROM comments c JOIN comment_minerals cm ON cm.comment_id = c.id
                WHERE c.id = ? AND cm.mineral = ?
                """,
                (content.content_id, content.mineral),
            ).fetchone()
            if row is None:
                raise self._stale_candidate_error(kind, content)
            current_content = _content_from_comment(row)
            source_manifest = _comment_source_manifest(row)

        current = self._with_candidate_state(
            connection,
            kind=kind,
            content=current_content,
            source_manifest=source_manifest,
            schema_version=state.schema_version,
            prompt_version=state.prompt_version,
            model=state.model,
            relevance_threshold=state.relevance_threshold,
            max_context_comments=state.max_context_comments,
            max_content_chars=state.max_content_chars,
        ).analysis_state
        if current is None or (
            current.config_revision != state.config_revision
            or current.input_revision != state.input_revision
            or current.dependency_revision != state.dependency_revision
            or current.expected_analysis_revision != state.expected_analysis_revision
            or current.base_result_revision != state.base_result_revision
        ):
            raise self._stale_candidate_error(kind, content)
        return state

    @staticmethod
    def _stale_candidate_error(
        kind: AnalysisKind, content: ContentInput
    ) -> StaleAnalysisCandidateError:
        return StaleAnalysisCandidateError(
            f"{kind.value} candidate {content.kind.value}:{content.content_id} changed "
            "before persistence"
        )

    def save_analysis(
        self,
        *,
        kind: AnalysisKind,
        content: ContentInput,
        result: ProviderResult[StrictModel],
        schema_version: int,
        prompt_version: str,
    ) -> None:
        now = _iso(utc_now())
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            state = self._validate_candidate(
                connection,
                kind=kind,
                content=content,
                schema_version=schema_version,
                prompt_version=prompt_version,
                model=result.model,
            )
            payload = result.value.model_dump_json()
            result_revision = _analysis_result_revision(
                kind=kind,
                content=content,
                state=state,
                schema_version=schema_version,
                prompt_version=prompt_version,
                model=result.model,
                status=WorkStatus.COMPLETE,
                payload=payload,
                error=None,
            )
            relevance_changed = (
                kind is AnalysisKind.RELEVANCE and state.base_result_revision != result_revision
            )
            connection.execute(
                """
                INSERT INTO analyses(
                    kind, content_kind, content_id, mineral, schema_version,
                    prompt_version, model, input_revision, config_revision,
                    dependency_revision, result_revision, status, payload, attempts,
                    provider_request_id, input_tokens, output_tokens, latency_ms,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(kind, content_kind, content_id, mineral) DO UPDATE SET
                    schema_version = excluded.schema_version,
                    prompt_version = excluded.prompt_version,
                    model = excluded.model,
                    input_revision = excluded.input_revision,
                    config_revision = excluded.config_revision,
                    dependency_revision = excluded.dependency_revision,
                    result_revision = excluded.result_revision,
                    status = excluded.status,
                    payload = excluded.payload,
                    error = NULL,
                    attempts = analyses.attempts + 1,
                    provider_request_id = excluded.provider_request_id,
                    input_tokens = excluded.input_tokens,
                    output_tokens = excluded.output_tokens,
                    latency_ms = excluded.latency_ms,
                    updated_at = excluded.updated_at
                """,
                (
                    kind.value,
                    content.kind.value,
                    content.content_id,
                    content.mineral,
                    schema_version,
                    prompt_version,
                    result.model,
                    state.input_revision,
                    state.config_revision,
                    state.dependency_revision,
                    result_revision,
                    WorkStatus.COMPLETE.value,
                    payload,
                    result.provider_request_id,
                    result.input_tokens,
                    result.output_tokens,
                    result.latency_ms,
                    now,
                    now,
                ),
            )
            self._upsert_work(
                connection,
                stage=kind.value,
                content_kind=content.kind,
                content_id=content.content_id,
                mineral=content.mineral,
                status=WorkStatus.COMPLETE,
                error=None,
                increment_attempt=True,
            )
            if relevance_changed and content.kind is ContentKind.POST:
                self._invalidate_reputation(connection, content.content_id, {content.mineral})

    def record_analysis_failure(
        self,
        *,
        kind: AnalysisKind,
        content: ContentInput,
        status: WorkStatus,
        error: str,
        schema_version: int,
        prompt_version: str,
        model: str | None = None,
    ) -> None:
        if status not in {
            WorkStatus.RETRYABLE_FAILURE,
            WorkStatus.PERMANENT_FAILURE,
            WorkStatus.BLOCKED,
        }:
            raise ValueError(f"Invalid failure state: {status}")
        now = _iso(utc_now())
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            state = self._validate_candidate(
                connection,
                kind=kind,
                content=content,
                schema_version=schema_version,
                prompt_version=prompt_version,
                model=model,
            )
            safe_error = _safe_error(error)
            result_revision = _analysis_result_revision(
                kind=kind,
                content=content,
                state=state,
                schema_version=schema_version,
                prompt_version=prompt_version,
                model=model,
                status=status,
                payload=None,
                error=safe_error,
            )
            relevance_changed = (
                kind is AnalysisKind.RELEVANCE and state.base_result_revision != result_revision
            )
            connection.execute(
                """
                INSERT INTO analyses(
                    kind, content_kind, content_id, mineral, schema_version,
                    prompt_version, model, input_revision, config_revision,
                    dependency_revision, result_revision, status, error, attempts,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                ON CONFLICT(kind, content_kind, content_id, mineral) DO UPDATE SET
                    schema_version = excluded.schema_version,
                    prompt_version = excluded.prompt_version,
                    model = excluded.model,
                    input_revision = excluded.input_revision,
                    config_revision = excluded.config_revision,
                    dependency_revision = excluded.dependency_revision,
                    result_revision = excluded.result_revision,
                    status = excluded.status,
                    payload = NULL,
                    error = excluded.error,
                    attempts = analyses.attempts + 1,
                    provider_request_id = NULL,
                    input_tokens = NULL,
                    output_tokens = NULL,
                    latency_ms = NULL,
                    updated_at = excluded.updated_at
                """,
                (
                    kind.value,
                    content.kind.value,
                    content.content_id,
                    content.mineral,
                    schema_version,
                    prompt_version,
                    model,
                    state.input_revision,
                    state.config_revision,
                    state.dependency_revision,
                    result_revision,
                    status.value,
                    safe_error,
                    now,
                    now,
                ),
            )
            self._upsert_work(
                connection,
                stage=kind.value,
                content_kind=content.kind,
                content_id=content.content_id,
                mineral=content.mineral,
                status=status,
                error=error,
                increment_attempt=True,
            )
            if relevance_changed and content.kind is ContentKind.POST:
                self._invalidate_reputation(connection, content.content_id, {content.mineral})

    def status(self) -> StatusSnapshot:
        with self._connection() as connection:
            connection.execute("BEGIN")
            schema_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            counts = {
                "posts": _scalar(connection, "SELECT COUNT(*) FROM posts"),
                "comments": _scalar(connection, "SELECT COUNT(*) FROM comments"),
                "mineral_posts": _scalar(connection, "SELECT COUNT(*) FROM post_minerals"),
                "mineral_comments": _scalar(connection, "SELECT COUNT(*) FROM comment_minerals"),
            }
            tombstones = {kind.value: 0 for kind in ContentKind}
            for row in connection.execute(
                """
                SELECT content_kind, COUNT(*) AS count
                FROM content_tombstones GROUP BY content_kind
                """
            ):
                tombstones[str(row["content_kind"])] = int(row["count"])
            work = {
                str(row["status"]): int(row["count"])
                for row in connection.execute(
                    "SELECT status, COUNT(*) AS count FROM work_items GROUP BY status"
                )
            }
            analyses = {
                f"{row['kind']}:{row['status']}": int(row["count"])
                for row in connection.execute(
                    """
                    SELECT kind, status, COUNT(*) AS count
                    FROM analyses GROUP BY kind, status
                    """
                )
            }
            runs_by_status = {
                str(row["status"]): int(row["count"])
                for row in connection.execute(
                    "SELECT status, COUNT(*) AS count FROM runs GROUP BY status"
                )
            }
            runs = [
                dict(row)
                for row in connection.execute(
                    """
                SELECT id, command, parameters, status, started_at, finished_at,
                       summary, error_type
                FROM runs ORDER BY started_at DESC LIMIT 10
                """
                )
            ]
            for run in runs:
                run["parameters"] = json.loads(str(run["parameters"]))
                if run["summary"]:
                    run["summary"] = json.loads(str(run["summary"]))
        return StatusSnapshot(
            schema_version=schema_version,
            **counts,
            tombstones_by_kind=tombstones,
            work_by_status=work,
            analyses_by_kind_and_status=analyses,
            runs_by_status=runs_by_status,
            recent_runs=runs,
        )

    def export_records(self, mineral: str | None = None) -> Iterator[dict[str, Any]]:
        """Stream canonical records from one explicit, consistent read snapshot."""

        with self._connection() as connection:
            # Python's sqlite3 driver does not implicitly start a transaction for
            # SELECT statements.  Begin explicitly so posts, comments, and their
            # nested analysis lookups all observe the same WAL snapshot even while
            # a writer commits during a long-running streaming export.
            connection.execute("BEGIN")
            parameters: tuple[str, ...] = (mineral,) if mineral else ()
            if mineral:
                post_rows = connection.execute(
                    """
                    SELECT p.*, pm.mineral, pm.scrape_status
                    FROM posts p JOIN post_minerals pm ON pm.post_id = p.id
                    WHERE pm.mineral = ?
                    ORDER BY pm.mineral, p.created_at, p.id
                    """,
                    parameters,
                )
            else:
                post_rows = connection.execute(
                    """
                    SELECT p.*, pm.mineral, pm.scrape_status
                    FROM posts p JOIN post_minerals pm ON pm.post_id = p.id
                    ORDER BY pm.mineral, p.created_at, p.id
                    """
                )
            for row in post_rows:
                yield self._export_row(connection, ContentKind.POST, row)

            if mineral:
                comment_rows = connection.execute(
                    """
                    SELECT c.*, cm.mineral
                    FROM comments c JOIN comment_minerals cm ON cm.comment_id = c.id
                    WHERE cm.mineral = ?
                    ORDER BY cm.mineral, c.created_at, c.id
                    """,
                    parameters,
                )
            else:
                comment_rows = connection.execute(
                    """
                    SELECT c.*, cm.mineral
                    FROM comments c JOIN comment_minerals cm ON cm.comment_id = c.id
                    ORDER BY cm.mineral, c.created_at, c.id
                    """
                )
            for row in comment_rows:
                yield self._export_row(connection, ContentKind.COMMENT, row)

    @staticmethod
    def _export_row(
        connection: sqlite3.Connection,
        kind: ContentKind,
        row: sqlite3.Row,
    ) -> dict[str, Any]:
        data = dict(row)
        mineral = str(data.pop("mineral"))
        analyses: dict[str, Any] = {}
        for analysis in connection.execute(
            """
            SELECT kind, schema_version, prompt_version, model, status, payload,
                   error, input_tokens, output_tokens, latency_ms, updated_at
            FROM analyses
            WHERE content_kind = ? AND content_id = ? AND mineral = ?
            ORDER BY kind
            """,
            (kind.value, row["id"], mineral),
        ):
            item = dict(analysis)
            payload = item.pop("payload")
            item["result"] = json.loads(str(payload)) if payload else None
            analyses[str(item.pop("kind"))] = item
        return {
            "export_schema_version": 1,
            "record_type": kind.value,
            "mineral": mineral,
            "content": data,
            "analyses": analyses,
        }

    def delete_content(
        self, *, content_kind: ContentKind, content_id: str, dry_run: bool
    ) -> dict[str, int | str | bool]:
        """Delete content transactionally and retain a durable suppression tombstone."""

        with self._connection() as connection:
            # Real deletion takes the writer reservation before discovering
            # descendants so every deleted child is tombstoned. A preview uses
            # one read snapshot and remains strictly non-writing.
            connection.execute("BEGIN" if dry_run else "BEGIN IMMEDIATE")
            if content_kind is ContentKind.POST:
                exists = _scalar(
                    connection, "SELECT COUNT(*) FROM posts WHERE id = ?", (content_id,)
                )
                comments = _scalar(
                    connection,
                    "SELECT COUNT(*) FROM comments WHERE post_id = ?",
                    (content_id,),
                )
                analyses = _scalar(
                    connection,
                    """
                    SELECT COUNT(*) FROM analyses
                    WHERE (content_kind = 'post' AND content_id = ?)
                       OR (content_kind = 'comment' AND content_id IN
                           (SELECT id FROM comments WHERE post_id = ?))
                    """,
                    (content_id, content_id),
                )
                if not dry_run:
                    child_ids = [
                        str(row["id"])
                        for row in connection.execute(
                            "SELECT id FROM comments WHERE post_id = ?",
                            (content_id,),
                        )
                    ]
                    self._upsert_tombstone(
                        connection,
                        ContentKind.POST,
                        content_id,
                        cascade_from_post_id=None,
                    )
                    for child_id in child_ids:
                        self._upsert_tombstone(
                            connection,
                            ContentKind.COMMENT,
                            child_id,
                            cascade_from_post_id=content_id,
                        )
                    connection.execute(
                        """
                        DELETE FROM analyses
                        WHERE (content_kind = 'post' AND content_id = ?)
                           OR (content_kind = 'comment' AND content_id IN
                               (SELECT id FROM comments WHERE post_id = ?))
                        """,
                        (content_id, content_id),
                    )
                    connection.execute(
                        """
                        DELETE FROM work_items
                        WHERE (content_kind = 'post' AND content_id = ?)
                           OR (content_kind = 'comment' AND content_id IN
                               (SELECT id FROM comments WHERE post_id = ?))
                        """,
                        (content_id, content_id),
                    )
                    connection.execute("DELETE FROM posts WHERE id = ?", (content_id,))
                return {
                    "content_kind": content_kind.value,
                    "content_found": bool(exists),
                    "posts": exists,
                    "comments": comments,
                    "analyses": analyses,
                    "dry_run": dry_run,
                }

            exists = _scalar(
                connection, "SELECT COUNT(*) FROM comments WHERE id = ?", (content_id,)
            )
            analyses = _scalar(
                connection,
                """
                SELECT COUNT(*) FROM analyses a
                WHERE (a.content_kind = 'comment' AND a.content_id = ?)
                   OR (
                       a.kind = 'reputation' AND a.content_kind = 'post'
                       AND EXISTS (
                           SELECT 1 FROM comments c
                           JOIN comment_minerals cm ON cm.comment_id = c.id
                           WHERE c.id = ? AND a.content_id = c.post_id
                             AND a.mineral = cm.mineral
                       )
                   )
                """,
                (content_id, content_id),
            )
            if not dry_run:
                row = connection.execute(
                    "SELECT post_id FROM comments WHERE id = ?", (content_id,)
                ).fetchone()
                minerals = self._comment_minerals(connection, content_id)
                self._upsert_tombstone(
                    connection,
                    ContentKind.COMMENT,
                    content_id,
                    cascade_from_post_id=None,
                )
                if row is not None:
                    self._invalidate_reputation(connection, str(row["post_id"]), minerals)
                connection.execute(
                    "DELETE FROM analyses WHERE content_kind = 'comment' AND content_id = ?",
                    (content_id,),
                )
                connection.execute(
                    "DELETE FROM work_items WHERE content_kind = 'comment' AND content_id = ?",
                    (content_id,),
                )
                connection.execute("DELETE FROM comments WHERE id = ?", (content_id,))
            return {
                "content_kind": content_kind.value,
                "content_found": bool(exists),
                "posts": 0,
                "comments": exists,
                "analyses": analyses,
                "dry_run": dry_run,
            }

    @staticmethod
    def _upsert_tombstone(
        connection: sqlite3.Connection,
        content_kind: ContentKind,
        content_id: str,
        *,
        cascade_from_post_id: str | None,
    ) -> None:
        connection.execute(
            """
            INSERT INTO content_tombstones(
                content_kind, content_id, deleted_at, cascade_from_post_id
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(content_kind, content_id) DO NOTHING
            """,
            (
                content_kind.value,
                content_id,
                _iso(utc_now()),
                cascade_from_post_id,
            ),
        )


def _uses_windows_file_locking() -> bool:
    return sys.platform == "win32"


def _acquire_operation_file_lock(handle: BinaryIO) -> None:
    handle.seek(0, 2)
    if handle.tell() == 0:
        handle.write(b"\0")
        handle.flush()
    handle.seek(0)
    if _uses_windows_file_locking():
        locking_module = importlib.import_module("msvcrt")
        windows_locking = cast(
            Callable[[int, int, int], object],
            locking_module.__dict__["locking"],
        )
        windows_locking(handle.fileno(), int(locking_module.__dict__["LK_NBLCK"]), 1)
        return

    locking_module = importlib.import_module("fcntl")  # pragma: no cover - Linux CI
    posix_flock = cast(  # pragma: no cover
        Callable[[int, int], object],
        locking_module.__dict__["flock"],
    )
    mode = int(locking_module.__dict__["LOCK_EX"]) | int(  # pragma: no cover
        locking_module.__dict__["LOCK_NB"]
    )
    posix_flock(handle.fileno(), mode)  # pragma: no cover


def _release_operation_file_lock(handle: BinaryIO) -> None:
    handle.seek(0)
    if _uses_windows_file_locking():
        locking_module = importlib.import_module("msvcrt")
        windows_locking = cast(
            Callable[[int, int, int], object],
            locking_module.__dict__["locking"],
        )
        windows_locking(handle.fileno(), int(locking_module.__dict__["LK_UNLCK"]), 1)
        return

    locking_module = importlib.import_module("fcntl")  # pragma: no cover - Linux CI
    posix_flock = cast(  # pragma: no cover
        Callable[[int, int], object],
        locking_module.__dict__["flock"],
    )
    posix_flock(  # pragma: no cover
        handle.fileno(),
        int(locking_module.__dict__["LOCK_UN"]),
    )


def _content_from_post(row: sqlite3.Row, comments: list[str]) -> ContentInput:
    return ContentInput(
        kind=ContentKind.POST,
        content_id=str(row["id"]),
        mineral=str(row["mineral"]),
        title=str(row["title"]),
        body=str(row["selftext"]),
        subreddit=str(row["subreddit"]),
        score=int(row["score"]),
        upvote_ratio=float(row["upvote_ratio"]) if row["upvote_ratio"] is not None else None,
        comment_context=comments,
    )


def _content_from_comment(row: sqlite3.Row) -> ContentInput:
    return ContentInput(
        kind=ContentKind.COMMENT,
        content_id=str(row["id"]),
        mineral=str(row["mineral"]),
        body=str(row["body"]),
        subreddit=str(row["subreddit"]),
        score=int(row["score"]),
    )


def _schema_statements(script: str) -> Iterator[str]:
    for statement in script.split(";"):
        normalized = statement.strip()
        if normalized:
            yield normalized


def _analysis_config_revision(
    *,
    kind: AnalysisKind,
    schema_version: int | None,
    prompt_version: str | None,
    model: str | None,
    relevance_threshold: float,
    max_context_comments: int,
    max_content_chars: int,
) -> str:
    return _revision(
        {
            "kind": kind.value,
            "schema_version": schema_version,
            "prompt_version": prompt_version,
            "model": model,
            "relevance_threshold": (
                float(relevance_threshold) if kind is AnalysisKind.REPUTATION else 0.0
            ),
            "max_context_comments": (
                max_context_comments if kind is AnalysisKind.REPUTATION else 0
            ),
            "max_content_chars": max_content_chars,
        }
    )


def _post_source_manifest(row: sqlite3.Row, context_rows: Sequence[sqlite3.Row]) -> dict[str, Any]:
    return {
        "content_kind": ContentKind.POST.value,
        "content_id": str(row["id"]),
        "mineral": str(row["mineral"]),
        "title": str(row["title"]),
        "body": str(row["selftext"]),
        "subreddit": str(row["subreddit"]),
        "score": int(row["score"]),
        "upvote_ratio": _optional_float(row["upvote_ratio"]),
        "comment_context": [
            {
                "id": str(context["id"]),
                "body": str(context["body"]),
                "score": int(context["score"]),
                "created_at": str(context["created_at"]),
            }
            for context in context_rows
        ],
    }


def _comment_source_manifest(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "content_kind": ContentKind.COMMENT.value,
        "content_id": str(row["id"]),
        "mineral": str(row["mineral"]),
        "post_id": str(row["post_id"]),
        "parent_id": str(row["parent_id"]) if row["parent_id"] is not None else None,
        "body": str(row["body"]),
        "subreddit": str(row["subreddit"]),
        "score": int(row["score"]),
    }


def _analysis_result_revision(
    *,
    kind: AnalysisKind,
    content: ContentInput,
    state: AnalysisCandidateState,
    schema_version: int,
    prompt_version: str,
    model: str | None,
    status: WorkStatus,
    payload: str | None,
    error: str | None,
) -> str:
    return _revision(
        {
            "kind": kind.value,
            "content_kind": content.kind.value,
            "content_id": content.content_id,
            "mineral": content.mineral,
            "schema_version": schema_version,
            "prompt_version": prompt_version,
            "model": model,
            "input_revision": state.input_revision,
            "config_revision": state.config_revision,
            "dependency_revision": state.dependency_revision,
            "status": status.value,
            "payload": payload,
            "error": error,
        }
    )


def _effective_result_revision(row: sqlite3.Row) -> str:
    persisted = row["result_revision"]
    if persisted is not None:
        return str(persisted)
    return _revision(
        {
            "kind": str(row["kind"]),
            "content_kind": str(row["content_kind"]),
            "content_id": str(row["content_id"]),
            "mineral": str(row["mineral"]),
            "schema_version": int(row["schema_version"]),
            "prompt_version": str(row["prompt_version"]),
            "model": str(row["model"]) if row["model"] is not None else None,
            "input_revision": (
                str(row["input_revision"]) if row["input_revision"] is not None else None
            ),
            "config_revision": (
                str(row["config_revision"]) if row["config_revision"] is not None else None
            ),
            "dependency_revision": (
                str(row["dependency_revision"]) if row["dependency_revision"] is not None else None
            ),
            "status": str(row["status"]),
            "payload": str(row["payload"]) if row["payload"] is not None else None,
            "error": str(row["error"]) if row["error"] is not None else None,
        }
    )


def _analysis_row_revision(row: sqlite3.Row) -> str:
    return _revision(
        {
            "result_revision": _effective_result_revision(row),
            "attempts": int(row["attempts"]),
            "updated_at": str(row["updated_at"]),
        }
    )


def _revision(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _scalar(connection: sqlite3.Connection, query: str, parameters: tuple[Any, ...] = ()) -> int:
    row = connection.execute(query, parameters).fetchone()
    return int(row[0]) if row is not None else 0


def _optional_float(value: Any) -> float | None:
    return float(value) if value is not None else None


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _safe_error(value: str | None) -> str | None:
    # Error text is operational metadata only. Bound it to avoid retaining response bodies.
    return value[:500] if value else None
