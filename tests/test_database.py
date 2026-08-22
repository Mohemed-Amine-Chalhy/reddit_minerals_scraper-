from __future__ import annotations

import sqlite3
from collections.abc import Callable
from contextlib import closing
from datetime import timedelta
from pathlib import Path
from threading import Event, Thread

import pytest

import reddit_minerals.storage.database as database_module
from reddit_minerals.errors import ConcurrentOperationError, ConfigurationError
from reddit_minerals.models import (
    AnalysisKind,
    CommentRecord,
    ContentInput,
    ContentKind,
    PostRecord,
    WorkStatus,
)
from reddit_minerals.storage import Database
from reddit_minerals.storage.database import (
    SCHEMA_VERSION,
    RefreshDecision,
    StaleAnalysisCandidateError,
)
from tests.fakes import result_for


def _candidates(
    database: Database,
    kind: AnalysisKind,
    *,
    limit: int = 100,
    force: bool = False,
    mineral: str | None = None,
    threshold: float = 70,
    context: int = 10,
    schema_version: int = 1,
    prompt_version: str = "test-v1",
    model: str = "offline-model",
    max_content_chars: int = 12_000,
) -> list[ContentInput]:
    return database.analysis_candidates(
        kind,
        mineral=mineral,
        limit=limit,
        force=force,
        relevance_threshold=threshold,
        max_context_comments=context,
        schema_version=schema_version,
        prompt_version=prompt_version,
        model=model,
        max_content_chars=max_content_chars,
    )


def _save_result(
    database: Database,
    kind: AnalysisKind,
    content: ContentInput,
    **overrides: object,
) -> None:
    database.save_analysis(
        kind=kind,
        content=content,
        result=result_for(kind, **overrides),
        schema_version=1,
        prompt_version="test-v1",
    )


def _failure(
    database: Database,
    kind: AnalysisKind,
    content: ContentInput,
    status: WorkStatus,
    error: str = "provider error",
) -> None:
    database.record_analysis_failure(
        kind=kind,
        content=content,
        status=status,
        error=error,
        schema_version=1,
        prompt_version="test-v1",
        model="offline-model",
    )


def test_export_stream_uses_one_snapshot_across_concurrent_writes(
    database: Database,
    make_post: Callable[..., PostRecord],
    make_comment: Callable[..., CommentRecord],
) -> None:
    database.store_scraped_post(
        make_post("p1"),
        [make_comment("c1", post_id="p1")],
        mineral="gold",
    )
    records = database.export_records("gold")
    first = next(records)
    assert first["record_type"] == "post"

    database.delete_content(
        content_kind=ContentKind.COMMENT,
        content_id="c1",
        dry_run=False,
    )

    remaining = list(records)
    assert [(record["record_type"], record["content"]["id"]) for record in remaining] == [
        ("comment", "c1")
    ]
    with database._connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM comments").fetchone()[0] == 0


def test_write_barrier_holds_cross_connection_writer_reservation(database: Database) -> None:
    with (
        closing(sqlite3.connect(database.path, timeout=0)) as competing,
        database.write_barrier(),
    ):
        competing.execute("PRAGMA busy_timeout = 0")
        assert list(database.export_records()) == []
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            competing.execute("BEGIN IMMEDIATE")

    with closing(sqlite3.connect(database.path, timeout=0)) as competing:
        competing.execute("BEGIN IMMEDIATE")
        competing.rollback()


def test_initialize_is_idempotent_and_creates_schema_in_parent(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "state.sqlite3"
    database = Database(path)
    database.initialize()
    database.initialize()
    assert path.is_file()
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
    assert {
        "posts",
        "post_minerals",
        "comments",
        "comment_minerals",
        "work_items",
        "analyses",
        "runs",
        "content_tombstones",
    } <= tables
    snapshot = database.status()
    assert snapshot.schema_version == SCHEMA_VERSION
    assert snapshot.tombstones_by_kind == {"post": 0, "comment": 0}
    assert snapshot.posts == 0


def test_fresh_schema_bootstrap_rolls_back_all_ddl_on_late_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "broken-bootstrap.sqlite3"
    monkeypatch.setattr(
        database_module,
        "_SCHEMA",
        database_module._SCHEMA
        + "; CREATE TABLE bootstrap_probe(id INTEGER); CREATE TABLE broken(",
    )

    with pytest.raises(sqlite3.OperationalError):
        Database(path).initialize()

    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 0
        user_objects = connection.execute(
            "SELECT name FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'"
        ).fetchall()
    assert user_objects == []


def test_unversioned_database_with_user_objects_is_not_stamped_current(tmp_path: Path) -> None:
    path = tmp_path / "unversioned.sqlite3"
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("CREATE TABLE unexpected(value TEXT)")
        connection.commit()

    with pytest.raises(ConfigurationError, match="no supported schema version"):
        Database(path).initialize()

    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 0
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM sqlite_schema WHERE name = 'unexpected'"
            ).fetchone()[0]
            == 1
        )


def test_initialize_rejects_newer_or_unmigratable_schema(tmp_path: Path) -> None:
    path = tmp_path / "newer.sqlite3"
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("PRAGMA journal_mode = DELETE").fetchone()[0] == "delete"
        connection.execute("PRAGMA user_version = 99")
        connection.commit()
    with pytest.raises(ConfigurationError, match="newer than supported"):
        Database(path).initialize()
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "delete"

    with (
        closing(sqlite3.connect(":memory:")) as connection,
        pytest.raises(ConfigurationError, match="No migration path"),
    ):
        Database._migrate(connection, -1)


def test_initialize_migrates_a_true_v1_runs_table_and_preserves_rows(
    tmp_path: Path,
) -> None:
    path = tmp_path / "true-v1.sqlite3"
    Database(path).initialize()
    with closing(sqlite3.connect(path)) as connection:
        connection.executescript(
            """
            DROP INDEX idx_tombstones_cascade;
            DROP TABLE content_tombstones;
            ALTER TABLE analyses DROP COLUMN result_revision;
            ALTER TABLE analyses DROP COLUMN dependency_revision;
            ALTER TABLE analyses DROP COLUMN config_revision;
            ALTER TABLE analyses DROP COLUMN input_revision;
            ALTER TABLE runs RENAME TO runs_current;
            CREATE TABLE runs (
                id TEXT PRIMARY KEY,
                command TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                summary TEXT,
                error_type TEXT
            );
            INSERT INTO runs(id, command, status, started_at)
            VALUES ('legacy-run', 'status', 'complete', '2026-01-01T00:00:00+00:00');
            DROP TABLE runs_current;
            PRAGMA user_version = 1;
            """
        )

    Database(path).initialize()

    with closing(sqlite3.connect(path)) as connection:
        columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(runs)")}
        assert "parameters" in columns
        assert (
            connection.execute("SELECT parameters FROM runs WHERE id = 'legacy-run'").fetchone()[0]
            == "{}"
        )
        assert connection.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION


def test_v1_migration_rolls_back_all_ddl_when_a_later_step_fails(
    tmp_path: Path,
) -> None:
    path = tmp_path / "broken-v1.sqlite3"
    with closing(sqlite3.connect(path)) as connection:
        connection.executescript(
            """
            CREATE TABLE runs (
                id TEXT PRIMARY KEY,
                command TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                summary TEXT,
                error_type TEXT
            );
            CREATE TABLE content_tombstones (
                content_kind TEXT NOT NULL,
                content_id TEXT NOT NULL,
                deleted_at TEXT NOT NULL,
                PRIMARY KEY (content_kind, content_id)
            );
            PRAGMA user_version = 1;
            """
        )

    with pytest.raises(sqlite3.OperationalError, match="cascade_from_post_id"):
        Database(path).initialize()

    with closing(sqlite3.connect(path)) as connection:
        columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(runs)")}
        assert "parameters" not in columns
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 1


def test_status_reports_schema_version_and_durable_tombstones(
    database: Database,
) -> None:
    database.delete_content(content_kind=ContentKind.POST, content_id="missing-post", dry_run=False)
    database.delete_content(
        content_kind=ContentKind.COMMENT,
        content_id="missing-comment",
        dry_run=False,
    )

    snapshot = database.status()
    assert snapshot.schema_version == SCHEMA_VERSION
    assert snapshot.tombstones_by_kind == {"post": 1, "comment": 1}


def test_initialize_migrates_v1_to_current_without_losing_existing_data(
    tmp_path: Path, make_post: Callable[..., PostRecord]
) -> None:
    path = tmp_path / "v1.sqlite3"
    database = Database(path)
    database.initialize()
    database.store_scraped_post(make_post("legacy"), [], mineral="gold")
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("DROP INDEX idx_tombstones_cascade")
        connection.execute("DROP TABLE content_tombstones")
        connection.execute("ALTER TABLE runs RENAME TO runs_v2")
        connection.execute(
            """
            CREATE TABLE runs (
                id TEXT PRIMARY KEY,
                command TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                summary TEXT,
                error_type TEXT
            )
            """
        )
        connection.execute(
            """
            INSERT INTO runs(id, command, status, started_at)
            VALUES ('legacy-run', 'legacy', 'running', '2026-01-01T00:00:00+00:00')
            """
        )
        connection.execute("DROP TABLE runs_v2")
        connection.execute("PRAGMA user_version = 1")
        connection.commit()

    database.initialize()
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        assert connection.execute("SELECT COUNT(*) FROM content_tombstones").fetchone()[0] == 0
    assert database.status().posts == 1
    assert database.status().recent_runs[0]["parameters"] == {}


def test_scraped_post_write_is_atomic_when_comment_has_wrong_parent(
    database: Database,
    make_post: Callable[..., PostRecord],
    make_comment: Callable[..., CommentRecord],
) -> None:
    post = make_post("p1")
    wrong = make_comment("c1", post_id="some-other-post")
    with pytest.raises(ValueError, match="belongs to"):
        database.store_scraped_post(post, [wrong], mineral="gold")
    snapshot = database.status()
    assert snapshot.posts == 0
    assert snapshot.comments == 0


def test_scrape_holds_writer_reservation_between_tombstone_check_and_upsert(
    database: Database,
    make_post: Callable[..., PostRecord],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered_upsert = Event()
    release_upsert = Event()
    errors: list[Exception] = []
    original_upsert = database._upsert_post

    def paused_upsert(connection: sqlite3.Connection, post: PostRecord) -> bool:
        entered_upsert.set()
        if not release_upsert.wait(timeout=5):
            raise AssertionError("test did not release scrape upsert")
        return original_upsert(connection, post)

    monkeypatch.setattr(database, "_upsert_post", paused_upsert)

    def store() -> None:
        try:
            database.store_scraped_post(make_post("p1"), [], mineral="gold")
        except Exception as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    worker = Thread(target=store)
    worker.start()
    try:
        assert entered_upsert.wait(timeout=5)
        with closing(sqlite3.connect(database.path, timeout=0)) as competing:
            competing.execute("PRAGMA busy_timeout = 0")
            with pytest.raises(sqlite3.OperationalError, match="locked"):
                competing.execute("BEGIN IMMEDIATE")
    finally:
        release_upsert.set()
        worker.join(timeout=5)

    assert not worker.is_alive()
    assert errors == []
    assert database.status().posts == 1


def test_store_upserts_records_and_tracks_attempts_without_duplicates(
    database: Database,
    make_post: Callable[..., PostRecord],
    make_comment: Callable[..., CommentRecord],
) -> None:
    database.store_scraped_post(
        make_post("p1", title="old"),
        [make_comment("c1", post_id="p1", body="old comment")],
        mineral="gold",
    )
    database.store_scraped_post(
        make_post("p1", title="new", score=99),
        [make_comment("c1", post_id="p1", body="new comment", score=20)],
        mineral="gold",
    )
    snapshot = database.status()
    assert (snapshot.posts, snapshot.comments, snapshot.mineral_posts) == (1, 1, 1)
    assert snapshot.work_by_status == {"complete": 1}
    records = list(database.export_records())
    post_record = next(item for item in records if item["record_type"] == "post")
    comment_record = next(item for item in records if item["record_type"] == "comment")
    assert post_record["content"]["title"] == "new"
    assert post_record["content"]["score"] == 99
    assert comment_record["content"]["body"] == "new comment"
    with database._connection() as connection:
        attempts = connection.execute(
            "SELECT attempts FROM work_items WHERE stage = 'scrape'"
        ).fetchone()[0]
    assert attempts == 2


def test_store_result_reports_received_stored_and_tombstoned_comments(
    database: Database,
    make_post: Callable[..., PostRecord],
    make_comment: Callable[..., CommentRecord],
) -> None:
    post = make_post("p1")
    deleted = make_comment("deleted", post_id="p1")
    database.store_scraped_post(post, [deleted], mineral="gold")
    database.delete_content(content_kind=ContentKind.COMMENT, content_id="deleted", dry_run=False)
    result = database.store_scraped_post(
        post,
        [deleted, make_comment("kept", post_id="p1")],
        mineral="gold",
    )
    assert result.post_stored is True
    assert result.post_skipped_tombstone is False
    assert result.comments_received == 2
    assert result.comments_stored == 1
    assert result.comments_skipped_tombstone == 1
    assert result.comment_associations_removed == 0


def test_store_rejects_duplicate_snapshot_ids_and_cross_post_comment_reassignment(
    database: Database,
    make_post: Callable[..., PostRecord],
    make_comment: Callable[..., CommentRecord],
) -> None:
    post = make_post("p1")
    comment = make_comment("c1", post_id="p1")
    with pytest.raises(ValueError, match="Duplicate comment ID"):
        database.store_scraped_post(post, [comment, comment], mineral="gold")
    assert database.status().posts == 0

    database.store_scraped_post(post, [comment], mineral="gold")
    with pytest.raises(ValueError, match="already attached to post p1"):
        database.store_scraped_post(
            make_post("p2"),
            [make_comment("c1", post_id="p2")],
            mineral="gold",
        )
    assert database.status().posts == 1


def test_partial_comment_snapshot_is_additive_but_complete_snapshot_reconciles(
    database: Database,
    make_post: Callable[..., PostRecord],
    make_comment: Callable[..., CommentRecord],
) -> None:
    post = make_post("p1")
    c1 = make_comment("c1", post_id="p1")
    c2 = make_comment("c2", post_id="p1")
    database.store_scraped_post(post, [c1, c2], mineral="gold", comment_snapshot_complete=True)

    partial = database.store_scraped_post(
        post, [c1], mineral="gold", comment_snapshot_complete=False
    )
    assert partial.comment_associations_removed == 0
    assert database.status().comments == 2

    complete = database.store_scraped_post(
        post, [c1], mineral="gold", comment_snapshot_complete=True
    )
    assert complete.comment_associations_removed == 1
    assert database.status().comments == 1
    assert {record["content"]["id"] for record in database.export_records("gold")} == {
        "p1",
        "c1",
    }


def test_complete_snapshot_removes_only_the_current_mineral_association(
    database: Database,
    make_post: Callable[..., PostRecord],
    make_comment: Callable[..., CommentRecord],
) -> None:
    post = make_post("p1")
    c1 = make_comment("c1", post_id="p1")
    c2 = make_comment("c2", post_id="p1")
    for mineral in ("gold", "silver"):
        database.store_scraped_post(post, [c1, c2], mineral=mineral, comment_snapshot_complete=True)

    result = database.store_scraped_post(post, [c1], mineral="gold", comment_snapshot_complete=True)
    assert result.comment_associations_removed == 1
    assert database.status().comments == 2
    assert "c2" not in {record["content"]["id"] for record in database.export_records("gold")}
    assert "c2" in {record["content"]["id"] for record in database.export_records("silver")}

    database.store_scraped_post(post, [c1], mineral="silver", comment_snapshot_complete=True)
    assert database.status().comments == 1


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (WorkStatus.PENDING, True),
        (WorkStatus.PARTIAL, True),
        (WorkStatus.RETRYABLE_FAILURE, True),
        (WorkStatus.PERMANENT_FAILURE, False),
        (WorkStatus.BLOCKED, False),
    ],
)
def test_refresh_decision_respects_terminal_states(
    database: Database,
    make_post: Callable[..., PostRecord],
    status: WorkStatus,
    expected: bool,
) -> None:
    database.store_scraped_post(make_post("p1"), [], mineral="gold", status=status)
    assert database.should_refresh("p1", "gold", timedelta(days=365)) is expected
    assert database.should_refresh("p1", "gold", timedelta(days=365), force=True) is True


def test_refresh_decision_handles_new_fresh_and_expired_posts(
    database: Database, make_post: Callable[..., PostRecord]
) -> None:
    assert database.should_refresh("new", "gold", timedelta(days=1)) is True
    database.store_scraped_post(make_post("p1"), [], mineral="gold")
    assert database.should_refresh("p1", "gold", timedelta(hours=1)) is False
    assert database.should_refresh("p1", "gold", timedelta(0)) is True
    assert database.should_refresh("p1", "silver", timedelta(days=1)) is True


def test_refresh_decision_exposes_distinct_fresh_terminal_and_tombstone_reasons(
    database: Database, make_post: Callable[..., PostRecord]
) -> None:
    assert database.refresh_decision("new", "gold", timedelta(days=1)) is RefreshDecision.REFRESH
    database.store_scraped_post(make_post("fresh"), [], mineral="gold")
    assert database.refresh_decision("fresh", "gold", timedelta(days=1)) is RefreshDecision.FRESH
    database.store_scraped_post(
        make_post("terminal"),
        [],
        mineral="gold",
        status=WorkStatus.PERMANENT_FAILURE,
    )
    assert database.refresh_decision("terminal", "gold", timedelta(0)) is RefreshDecision.TERMINAL
    database.delete_content(content_kind=ContentKind.POST, content_id="fresh", dry_run=False)
    assert (
        database.refresh_decision("fresh", "gold", timedelta(0), force=True)
        is RefreshDecision.TOMBSTONED
    )


def test_relevance_candidates_exclude_incomplete_and_terminal_analysis_unless_forced(
    database: Database, make_post: Callable[..., PostRecord]
) -> None:
    for index, status in enumerate(
        (WorkStatus.COMPLETE, WorkStatus.COMPLETE, WorkStatus.COMPLETE, WorkStatus.PARTIAL),
        start=1,
    ):
        database.store_scraped_post(
            make_post(f"p{index}", created_offset=index),
            [],
            mineral="gold",
            status=status,
        )
    by_id = {item.content_id: item for item in _candidates(database, AnalysisKind.RELEVANCE)}
    _save_result(database, AnalysisKind.RELEVANCE, by_id["p1"])
    _failure(
        database,
        AnalysisKind.RELEVANCE,
        by_id["p2"],
        WorkStatus.PERMANENT_FAILURE,
    )
    _failure(
        database,
        AnalysisKind.RELEVANCE,
        by_id["p3"],
        WorkStatus.RETRYABLE_FAILURE,
    )

    assert [item.content_id for item in _candidates(database, AnalysisKind.RELEVANCE)] == ["p3"]
    forced = _candidates(database, AnalysisKind.RELEVANCE, force=True)
    assert {item.content_id for item in forced} == {"p1", "p2", "p3"}


def test_candidate_order_prioritizes_oldest_unattempted_work_before_retries(
    database: Database, make_post: Callable[..., PostRecord]
) -> None:
    database.store_scraped_post(make_post("old", created_offset=1), [], mineral="gold")
    database.store_scraped_post(make_post("new", created_offset=2), [], mineral="gold")
    initial = _candidates(database, AnalysisKind.RELEVANCE, limit=2)
    assert [item.content_id for item in initial] == ["old", "new"]

    _failure(
        database,
        AnalysisKind.RELEVANCE,
        initial[0],
        WorkStatus.RETRYABLE_FAILURE,
    )
    subsequent = _candidates(database, AnalysisKind.RELEVANCE, limit=2)
    assert [item.content_id for item in subsequent] == ["new", "old"]


def test_blocked_analysis_is_terminal_without_force(
    database: Database, make_post: Callable[..., PostRecord]
) -> None:
    database.store_scraped_post(make_post("p1"), [], mineral="gold")
    content = _candidates(database, AnalysisKind.RELEVANCE)[0]
    _failure(database, AnalysisKind.RELEVANCE, content, WorkStatus.BLOCKED)
    assert _candidates(database, AnalysisKind.RELEVANCE) == []
    assert _candidates(database, AnalysisKind.RELEVANCE, force=True)[0].content_id == "p1"


def test_enrichment_batch_includes_posts_and_comments_to_prevent_starvation(
    database: Database,
    make_post: Callable[..., PostRecord],
    make_comment: Callable[..., CommentRecord],
) -> None:
    for index in range(1, 4):
        database.store_scraped_post(
            make_post(f"p{index}", created_offset=index),
            [make_comment(f"c{index}", post_id=f"p{index}", created_offset=index)],
            mineral="gold",
        )
    candidates = _candidates(database, AnalysisKind.ENRICHMENT, limit=2)
    assert len(candidates) == 2
    assert {candidate.kind for candidate in candidates} == {
        ContentKind.POST,
        ContentKind.COMMENT,
    }


def test_enrichment_fairness_prefers_the_kind_with_less_completed_work(
    database: Database,
    make_post: Callable[..., PostRecord],
    make_comment: Callable[..., CommentRecord],
) -> None:
    database.store_scraped_post(make_post("p1"), [make_comment("c1", post_id="p1")], mineral="gold")
    initial = _candidates(database, AnalysisKind.ENRICHMENT, limit=2)
    post = next(item for item in initial if item.kind is ContentKind.POST)
    _save_result(database, AnalysisKind.ENRICHMENT, post)
    next_candidate = _candidates(database, AnalysisKind.ENRICHMENT, limit=1)
    assert next_candidate[0].kind is ContentKind.COMMENT


def test_analysis_provenance_mismatch_makes_terminal_work_eligible_again(
    database: Database, make_post: Callable[..., PostRecord]
) -> None:
    database.store_scraped_post(make_post("p1"), [], mineral="gold")
    content = _candidates(
        database,
        AnalysisKind.RELEVANCE,
        schema_version=1,
        prompt_version="prompt-a",
        model="model-a",
    )[0]
    database.save_analysis(
        kind=AnalysisKind.RELEVANCE,
        content=content,
        result=result_for(AnalysisKind.RELEVANCE, model="model-a"),
        schema_version=1,
        prompt_version="prompt-a",
    )

    common = {
        "mineral": "gold",
        "limit": 1,
        "force": False,
        "relevance_threshold": 70,
        "max_context_comments": 0,
    }
    assert (
        database.analysis_candidates(
            AnalysisKind.RELEVANCE,
            schema_version=1,
            prompt_version="prompt-a",
            model="model-a",
            **common,
        )
        == []
    )
    for provenance in (
        {"schema_version": 2, "prompt_version": "prompt-a", "model": "model-a"},
        {"schema_version": 1, "prompt_version": "prompt-b", "model": "model-a"},
        {"schema_version": 1, "prompt_version": "prompt-a", "model": "model-b"},
    ):
        candidates = database.analysis_candidates(AnalysisKind.RELEVANCE, **provenance, **common)
        assert [item.content_id for item in candidates] == ["p1"]


def test_content_truncation_bound_is_analysis_and_dependency_provenance(
    database: Database, make_post: Callable[..., PostRecord]
) -> None:
    database.store_scraped_post(make_post("p1", selftext="x" * 2_000), [], mineral="gold")
    relevance = _candidates(
        database,
        AnalysisKind.RELEVANCE,
        max_content_chars=500,
    )[0]
    _save_result(database, AnalysisKind.RELEVANCE, relevance)

    assert _candidates(database, AnalysisKind.RELEVANCE, max_content_chars=500) == []
    assert (
        _candidates(
            database,
            AnalysisKind.RELEVANCE,
            max_content_chars=501,
        )[0].content_id
        == "p1"
    )
    assert (
        _candidates(
            database,
            AnalysisKind.REPUTATION,
            context=0,
            max_content_chars=501,
        )
        == []
    )


def test_analysis_compare_and_save_rejects_changed_and_deleted_post(
    database: Database, make_post: Callable[..., PostRecord]
) -> None:
    database.store_scraped_post(make_post("p1", title="before"), [], mineral="gold")
    stale_success = _candidates(database, AnalysisKind.RELEVANCE)[0]

    database.store_scraped_post(make_post("p1", title="after"), [], mineral="gold")
    with pytest.raises(StaleAnalysisCandidateError, match="changed before persistence"):
        _save_result(database, AnalysisKind.RELEVANCE, stale_success)
    assert database.status().analyses_by_kind_and_status == {}

    stale_failure = _candidates(database, AnalysisKind.RELEVANCE)[0]
    database.delete_content(content_kind=ContentKind.POST, content_id="p1", dry_run=False)
    with pytest.raises(StaleAnalysisCandidateError, match="changed before persistence"):
        _failure(
            database,
            AnalysisKind.RELEVANCE,
            stale_failure,
            WorkStatus.RETRYABLE_FAILURE,
        )
    assert database.status().analyses_by_kind_and_status == {}
    assert database.status().work_by_status == {}


def test_analysis_compare_and_save_allows_metadata_only_refresh(
    database: Database, make_post: Callable[..., PostRecord]
) -> None:
    database.store_scraped_post(make_post("p1", num_comments=1), [], mineral="gold")
    candidate = _candidates(database, AnalysisKind.RELEVANCE)[0]

    database.store_scraped_post(make_post("p1", num_comments=99), [], mineral="gold")
    _save_result(database, AnalysisKind.RELEVANCE, candidate)

    assert database.status().analyses_by_kind_and_status == {"relevance:complete": 1}


def test_comment_candidate_refuses_removed_mineral_association(
    database: Database,
    make_post: Callable[..., PostRecord],
    make_comment: Callable[..., CommentRecord],
) -> None:
    post = make_post("p1")
    comment = make_comment("c1", post_id="p1")
    database.store_scraped_post(post, [comment], mineral="gold")
    database.store_scraped_post(post, [comment], mineral="silver")
    candidate = next(
        item
        for item in _candidates(database, AnalysisKind.ENRICHMENT, mineral="gold")
        if item.kind is ContentKind.COMMENT
    )

    database.store_scraped_post(
        post,
        [],
        mineral="gold",
        comment_snapshot_complete=True,
    )
    with pytest.raises(StaleAnalysisCandidateError):
        _save_result(database, AnalysisKind.ENRICHMENT, candidate)

    assert {record["content"]["id"] for record in database.export_records("silver")} == {
        "p1",
        "c1",
    }
    assert {record["content"]["id"] for record in database.export_records("gold")} == {"p1"}


def test_compare_and_save_rejects_a_second_detached_candidate(
    database: Database, make_post: Callable[..., PostRecord]
) -> None:
    database.store_scraped_post(make_post("p1"), [], mineral="gold")
    first = _candidates(database, AnalysisKind.RELEVANCE)[0]
    second = _candidates(database, AnalysisKind.RELEVANCE)[0]

    _save_result(database, AnalysisKind.RELEVANCE, first)
    with pytest.raises(StaleAnalysisCandidateError):
        _save_result(database, AnalysisKind.RELEVANCE, second, confidence=12)

    exported = next(iter(database.export_records("gold")))
    assert exported["analyses"]["relevance"]["result"]["confidence"] == 91


def test_compare_and_save_row_generation_prevents_an_identical_result_aba(
    database: Database, make_post: Callable[..., PostRecord]
) -> None:
    database.store_scraped_post(make_post("p1"), [], mineral="gold")
    _save_result(
        database,
        AnalysisKind.RELEVANCE,
        _candidates(database, AnalysisKind.RELEVANCE)[0],
    )
    _save_result(
        database,
        AnalysisKind.REPUTATION,
        _candidates(database, AnalysisKind.REPUTATION, context=0)[0],
    )
    first = _candidates(database, AnalysisKind.RELEVANCE, force=True)[0]
    second = _candidates(database, AnalysisKind.RELEVANCE, force=True)[0]

    _save_result(database, AnalysisKind.RELEVANCE, first)
    with pytest.raises(StaleAnalysisCandidateError):
        _save_result(database, AnalysisKind.RELEVANCE, second, relevant=False)

    exported = next(iter(database.export_records("gold")))
    assert "reputation" in exported["analyses"]
    assert exported["analyses"]["relevance"]["result"]["relevant"] is True


def test_reputation_requires_current_relevance_provenance(
    database: Database, make_post: Callable[..., PostRecord]
) -> None:
    database.store_scraped_post(make_post("p1"), [], mineral="gold")
    common = {
        "mineral": "gold",
        "limit": 1,
        "force": False,
        "relevance_threshold": 70,
        "max_context_comments": 0,
        "schema_version": 1,
        "model": "model-a",
    }
    old = database.analysis_candidates(
        AnalysisKind.RELEVANCE,
        prompt_version="prompt-a",
        **common,
    )[0]
    database.save_analysis(
        kind=AnalysisKind.RELEVANCE,
        content=old,
        result=result_for(AnalysisKind.RELEVANCE, model="model-a"),
        schema_version=1,
        prompt_version="prompt-a",
    )

    assert (
        database.analysis_candidates(
            AnalysisKind.REPUTATION,
            prompt_version="prompt-b",
            **common,
        )
        == []
    )
    current = database.analysis_candidates(
        AnalysisKind.RELEVANCE,
        prompt_version="prompt-b",
        **common,
    )[0]
    database.save_analysis(
        kind=AnalysisKind.RELEVANCE,
        content=current,
        result=result_for(AnalysisKind.RELEVANCE, model="model-a"),
        schema_version=1,
        prompt_version="prompt-b",
    )
    assert [
        item.content_id
        for item in database.analysis_candidates(
            AnalysisKind.REPUTATION,
            prompt_version="prompt-b",
            **common,
        )
    ] == ["p1"]


def test_changed_relevance_invalidates_reputation_and_inflight_dependency(
    database: Database, make_post: Callable[..., PostRecord]
) -> None:
    database.store_scraped_post(make_post("p1"), [], mineral="gold")
    relevance = _candidates(database, AnalysisKind.RELEVANCE)[0]
    _save_result(database, AnalysisKind.RELEVANCE, relevance)
    stale_reputation = _candidates(database, AnalysisKind.REPUTATION, context=0)[0]
    current_reputation = _candidates(database, AnalysisKind.REPUTATION, context=0)[0]
    _save_result(database, AnalysisKind.REPUTATION, current_reputation)

    forced_relevance = _candidates(database, AnalysisKind.RELEVANCE, force=True)[0]
    _save_result(database, AnalysisKind.RELEVANCE, forced_relevance, relevant=False)

    exported = next(iter(database.export_records("gold")))
    assert "reputation" not in exported["analyses"]
    with pytest.raises(StaleAnalysisCandidateError):
        _save_result(database, AnalysisKind.REPUTATION, stale_reputation)
    assert _candidates(database, AnalysisKind.REPUTATION, context=0) == []


def test_reputation_compare_and_save_rejects_changed_comment_context(
    database: Database,
    make_post: Callable[..., PostRecord],
    make_comment: Callable[..., CommentRecord],
) -> None:
    post = make_post("p1")
    database.store_scraped_post(
        post,
        [make_comment("c1", post_id="p1", body="before")],
        mineral="gold",
    )
    _save_result(
        database,
        AnalysisKind.RELEVANCE,
        _candidates(database, AnalysisKind.RELEVANCE)[0],
    )
    candidate = _candidates(database, AnalysisKind.REPUTATION, context=1)[0]

    database.store_scraped_post(
        post,
        [make_comment("c1", post_id="p1", body="after")],
        mineral="gold",
    )
    with pytest.raises(StaleAnalysisCandidateError):
        _save_result(database, AnalysisKind.REPUTATION, candidate)


def test_unchanged_relevance_preserves_reputation_but_context_config_requeues(
    database: Database, make_post: Callable[..., PostRecord]
) -> None:
    database.store_scraped_post(make_post("p1"), [], mineral="gold")
    _save_result(
        database,
        AnalysisKind.RELEVANCE,
        _candidates(database, AnalysisKind.RELEVANCE)[0],
    )
    reputation = _candidates(database, AnalysisKind.REPUTATION, context=0)[0]
    _save_result(database, AnalysisKind.REPUTATION, reputation)

    forced_relevance = _candidates(database, AnalysisKind.RELEVANCE, force=True)[0]
    _save_result(database, AnalysisKind.RELEVANCE, forced_relevance)
    assert "reputation" in next(iter(database.export_records("gold")))["analyses"]
    assert _candidates(database, AnalysisKind.REPUTATION, context=0) == []
    assert _candidates(database, AnalysisKind.REPUTATION, context=1)[0].content_id == "p1"


def test_post_change_invalidates_post_analyses_but_metadata_only_refresh_does_not(
    database: Database, make_post: Callable[..., PostRecord]
) -> None:
    original = make_post("p1", title="original", num_comments=1)
    database.store_scraped_post(original, [], mineral="gold")
    for kind in AnalysisKind:
        _save_result(database, kind, _candidates(database, kind)[0])
    assert len(next(iter(database.export_records("gold")))["analyses"]) == 3

    metadata_only = make_post("p1", title="original", num_comments=99)
    database.store_scraped_post(metadata_only, [], mineral="gold")
    assert len(next(iter(database.export_records("gold")))["analyses"]) == 3

    changed = make_post("p1", title="changed", num_comments=99)
    database.store_scraped_post(changed, [], mineral="gold")
    assert next(iter(database.export_records("gold")))["analyses"] == {}
    assert _candidates(database, AnalysisKind.RELEVANCE)[0].content_id == "p1"


def test_comment_change_invalidates_enrichment_and_parent_reputation_for_all_minerals(
    database: Database,
    make_post: Callable[..., PostRecord],
    make_comment: Callable[..., CommentRecord],
) -> None:
    post = make_post("p1")
    original = make_comment("c1", post_id="p1", body="old")
    for mineral in ("gold", "silver"):
        database.store_scraped_post(post, [original], mineral=mineral)
        items = _candidates(database, AnalysisKind.ENRICHMENT, mineral=mineral)
        comment_input = next(item for item in items if item.kind is ContentKind.COMMENT)
        _save_result(database, AnalysisKind.ENRICHMENT, comment_input)
        relevance_input = _candidates(database, AnalysisKind.RELEVANCE, mineral=mineral)[0]
        _save_result(database, AnalysisKind.RELEVANCE, relevance_input)
        reputation_input = _candidates(database, AnalysisKind.REPUTATION, mineral=mineral)[0]
        _save_result(database, AnalysisKind.REPUTATION, reputation_input)

    database.store_scraped_post(
        post,
        [make_comment("c1", post_id="p1", body="new")],
        mineral="gold",
    )
    for mineral in ("gold", "silver"):
        records = list(database.export_records(mineral))
        comment_record = next(record for record in records if record["record_type"] == "comment")
        post_record = next(record for record in records if record["record_type"] == "post")
        assert "enrichment" not in comment_record["analyses"]
        assert "reputation" not in post_record["analyses"]


def test_failure_after_success_clears_stale_provider_metadata(
    database: Database, make_post: Callable[..., PostRecord]
) -> None:
    database.store_scraped_post(make_post("p1"), [], mineral="gold")
    content = _candidates(database, AnalysisKind.RELEVANCE)[0]
    _save_result(database, AnalysisKind.RELEVANCE, content)
    content = _candidates(database, AnalysisKind.RELEVANCE, force=True)[0]
    _failure(
        database,
        AnalysisKind.RELEVANCE,
        content,
        WorkStatus.RETRYABLE_FAILURE,
        "provider failed",
    )
    analysis = next(iter(database.export_records("gold")))["analyses"]["relevance"]
    assert analysis["status"] == "retryable_failure"
    assert analysis["result"] is None
    assert analysis["model"] == "offline-model"
    assert analysis["input_tokens"] is None
    assert analysis["output_tokens"] is None
    assert analysis["latency_ms"] is None


def test_enrichment_respects_mineral_terminal_states_and_force(
    database: Database,
    make_post: Callable[..., PostRecord],
    make_comment: Callable[..., CommentRecord],
) -> None:
    post = make_post("p1")
    comment = make_comment("c1", post_id="p1")
    database.store_scraped_post(post, [comment], mineral="gold")
    database.store_scraped_post(post, [comment], mineral="silver")
    gold = _candidates(database, AnalysisKind.ENRICHMENT, mineral="gold")
    assert {item.kind for item in gold} == {ContentKind.POST, ContentKind.COMMENT}
    for content in gold:
        _save_result(database, AnalysisKind.ENRICHMENT, content)
    assert _candidates(database, AnalysisKind.ENRICHMENT, mineral="gold") == []
    assert len(_candidates(database, AnalysisKind.ENRICHMENT, mineral="gold", force=True)) == 2
    assert len(_candidates(database, AnalysisKind.ENRICHMENT, mineral="silver")) == 2


def test_reputation_requires_relevant_confident_result_and_supplies_ranked_context(
    database: Database,
    make_post: Callable[..., PostRecord],
    make_comment: Callable[..., CommentRecord],
) -> None:
    for index in range(1, 4):
        database.store_scraped_post(
            make_post(f"p{index}", created_offset=index),
            (
                [
                    make_comment(f"c{index}-low", post_id=f"p{index}", score=1),
                    make_comment(f"c{index}-high", post_id=f"p{index}", score=20),
                ]
                if index == 1
                else []
            ),
            mineral="gold",
        )
    relevance = {item.content_id: item for item in _candidates(database, AnalysisKind.RELEVANCE)}
    _save_result(database, AnalysisKind.RELEVANCE, relevance["p1"], confidence=90)
    _save_result(database, AnalysisKind.RELEVANCE, relevance["p2"], confidence=69)
    _save_result(database, AnalysisKind.RELEVANCE, relevance["p3"], confidence=99, relevant=False)

    candidates = _candidates(database, AnalysisKind.REPUTATION, threshold=70, context=1)
    assert [item.content_id for item in candidates] == ["p1"]
    assert candidates[0].comment_context == ["Body for c1-high"]
    assert (
        _candidates(database, AnalysisKind.REPUTATION, threshold=70, context=0)[0].comment_context
        == []
    )

    _failure(
        database,
        AnalysisKind.REPUTATION,
        candidates[0],
        WorkStatus.PERMANENT_FAILURE,
    )
    assert _candidates(database, AnalysisKind.REPUTATION, threshold=70, context=1) == []
    assert (
        _candidates(
            database,
            AnalysisKind.REPUTATION,
            threshold=70,
            context=1,
            force=True,
        )[0].content_id
        == "p1"
    )


def test_analysis_save_and_failure_updates_payload_attempts_and_status_summary(
    database: Database, make_post: Callable[..., PostRecord]
) -> None:
    database.store_scraped_post(make_post("p1"), [], mineral="gold")
    content = _candidates(database, AnalysisKind.RELEVANCE)[0]
    _failure(
        database,
        AnalysisKind.RELEVANCE,
        content,
        WorkStatus.RETRYABLE_FAILURE,
        "x" * 700,
    )
    content = _candidates(database, AnalysisKind.RELEVANCE)[0]
    _save_result(database, AnalysisKind.RELEVANCE, content, confidence=88)
    snapshot = database.status()
    assert snapshot.analyses_by_kind_and_status == {"relevance:complete": 1}
    assert snapshot.work_by_status == {"complete": 2}
    exported = next(iter(database.export_records()))
    analysis = exported["analyses"]["relevance"]
    assert analysis["status"] == "complete"
    assert analysis["result"]["confidence"] == 88
    assert analysis["error"] is None
    with database._connection() as connection:
        row = connection.execute(
            "SELECT attempts FROM analyses WHERE kind = 'relevance'"
        ).fetchone()
    assert row[0] == 2


def test_failure_rejects_non_failure_status_and_truncates_errors(
    database: Database, make_post: Callable[..., PostRecord]
) -> None:
    database.store_scraped_post(make_post("p1"), [], mineral="gold")
    content = _candidates(database, AnalysisKind.RELEVANCE)[0]
    with pytest.raises(ValueError, match="Invalid failure state"):
        _failure(database, AnalysisKind.RELEVANCE, content, WorkStatus.COMPLETE)
    _failure(
        database,
        AnalysisKind.RELEVANCE,
        content,
        WorkStatus.RETRYABLE_FAILURE,
        "e" * 900,
    )
    with database._connection() as connection:
        row = connection.execute("SELECT error FROM analyses WHERE kind = 'relevance'").fetchone()
    assert row[0] == "e" * 500


def test_run_tracking_records_success_failure_and_recent_summary(database: Database) -> None:
    success_id = database.start_run("status", {"zeta": 2, "alpha": "safe", "dry_run": False})
    database.finish_run(success_id, success=True, summary={"count": 2})
    failure_id = database.start_run("scrape")
    database.finish_run(
        failure_id,
        success=False,
        summary={},
        error_type="RetryableProviderError",
    )
    runs = {run["id"]: run for run in database.status().recent_runs}
    assert runs[success_id]["status"] == "complete"
    assert runs[success_id]["parameters"] == {
        "alpha": "safe",
        "dry_run": False,
        "zeta": 2,
    }
    assert runs[success_id]["summary"] == {"count": 2}
    assert runs[failure_id]["status"] == "failed"
    assert runs[failure_id]["error_type"] == "RetryableProviderError"
    assert runs[failure_id]["parameters"] == {}
    assert database.status().runs_by_status == {"complete": 1, "failed": 1}
    with database._connection() as connection:
        raw = connection.execute(
            "SELECT parameters FROM runs WHERE id = ?", (success_id,)
        ).fetchone()[0]
    assert raw == '{"alpha":"safe","dry_run":false,"zeta":2}'

    with pytest.raises(RuntimeError, match="Run audit row does not exist"):
        database.finish_run("missing", success=False, summary={}, error_type="MissingRun")


def test_reconcile_stale_runs_marks_only_running_rows_failed(database: Database) -> None:
    complete_id = database.start_run("status")
    database.finish_run(complete_id, success=True, summary={"ok": True})
    stale_ids = {database.start_run("scrape"), database.start_run("relevance")}

    with pytest.raises(RuntimeError, match="requires the database operation lock"):
        database.reconcile_stale_runs()
    with database.operation_lock():
        assert database.reconcile_stale_runs() == 2
        assert database.reconcile_stale_runs() == 0

    snapshot = database.status()
    assert snapshot.runs_by_status == {"complete": 1, "failed": 2}
    runs = {run["id"]: run for run in snapshot.recent_runs}
    for run_id in stale_ids:
        assert runs[run_id]["status"] == "failed"
        assert runs[run_id]["error_type"] == "InterruptedRun"
        assert runs[run_id]["summary"] == {}
        assert runs[run_id]["finished_at"] is not None


def test_operation_lock_rejects_an_overlapping_writer(database: Database) -> None:
    contender = Database(database.path)
    with (
        database.operation_lock(),
        pytest.raises(ConcurrentOperationError, match="already using this database"),
        contender.operation_lock(),
    ):
        pytest.fail("overlapping operation unexpectedly acquired the lock")

    with contender.operation_lock():
        assert contender.reconcile_stale_runs() == 0


def test_delete_post_dry_run_then_real_cascades_comments_analyses_and_work(
    database: Database,
    make_post: Callable[..., PostRecord],
    make_comment: Callable[..., CommentRecord],
) -> None:
    database.store_scraped_post(make_post("p1"), [make_comment("c1", post_id="p1")], mineral="gold")
    candidates = _candidates(database, AnalysisKind.ENRICHMENT)
    for content in candidates:
        _save_result(database, AnalysisKind.ENRICHMENT, content)

    preview = database.delete_content(content_kind=ContentKind.POST, content_id="p1", dry_run=True)
    assert preview == {
        "content_kind": "post",
        "content_found": True,
        "posts": 1,
        "comments": 1,
        "analyses": 2,
        "dry_run": True,
    }
    assert database.status().posts == 1
    assert database.is_tombstoned(ContentKind.POST, "p1") is False
    assert database.is_tombstoned(ContentKind.COMMENT, "c1") is False

    deleted = database.delete_content(content_kind=ContentKind.POST, content_id="p1", dry_run=False)
    assert deleted["dry_run"] is False
    assert database.status().posts == 0
    assert database.status().comments == 0
    assert database.status().analyses_by_kind_and_status == {}
    assert database.status().work_by_status == {}
    assert list(database.export_records()) == []
    assert database.is_tombstoned(ContentKind.POST, "p1") is True
    assert database.is_tombstoned(ContentKind.COMMENT, "c1") is True
    suppressed = database.store_scraped_post(
        make_post("p1"), [make_comment("c1", post_id="p1")], mineral="gold"
    )
    assert suppressed.post_stored is False
    assert suppressed.post_skipped_tombstone is True
    assert database.should_refresh("p1", "gold", timedelta(0), force=True) is False
    assert database.status().posts == 0


def test_delete_single_comment_leaves_post_and_handles_missing_id(
    database: Database,
    make_post: Callable[..., PostRecord],
    make_comment: Callable[..., CommentRecord],
) -> None:
    database.store_scraped_post(make_post("p1"), [make_comment("c1", post_id="p1")], mineral="gold")
    comment = next(
        item
        for item in _candidates(database, AnalysisKind.ENRICHMENT)
        if item.kind is ContentKind.COMMENT
    )
    _save_result(database, AnalysisKind.ENRICHMENT, comment)
    preview = database.delete_content(
        content_kind=ContentKind.COMMENT, content_id="c1", dry_run=True
    )
    assert preview["comments"] == 1
    assert preview["analyses"] == 1
    database.delete_content(content_kind=ContentKind.COMMENT, content_id="c1", dry_run=False)
    assert database.status().posts == 1
    assert database.status().comments == 0
    missing = database.delete_content(
        content_kind=ContentKind.COMMENT, content_id="missing", dry_run=False
    )
    assert missing["content_found"] is False
    assert missing["comments"] == 0
    assert database.is_tombstoned(ContentKind.COMMENT, "c1") is True
    assert database.is_tombstoned(ContentKind.COMMENT, "missing") is True
