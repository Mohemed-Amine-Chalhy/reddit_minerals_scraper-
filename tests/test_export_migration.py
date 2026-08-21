from __future__ import annotations

import json
import os
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Any

import pytest

import reddit_minerals.migration as migration
from reddit_minerals.export import export_database
from reddit_minerals.migration import migrate_legacy_data
from reddit_minerals.models import CommentRecord, ContentKind, PostRecord
from reddit_minerals.storage import Database


def test_json_and_jsonl_exports_are_versioned_filterable_and_replace_existing_file(
    database: Database,
    make_post: Callable[..., PostRecord],
    make_comment: Callable[..., CommentRecord],
    tmp_path: Path,
) -> None:
    database.store_scraped_post(
        make_post("gold-post"),
        [make_comment("gold-comment", post_id="gold-post")],
        mineral="gold",
    )
    database.store_scraped_post(make_post("silver-post"), [], mineral="silver")

    json_output = tmp_path / "nested" / "export.json"
    json_output.parent.mkdir()
    json_output.write_text("stale", encoding="utf-8")
    assert (
        export_database(
            database,
            output=json_output,
            format_name="json",
            mineral="gold",
            overwrite=True,
        )
        == 2
    )
    document = json.loads(json_output.read_text(encoding="utf-8"))
    assert document["export_schema_version"] == 1
    assert len(document["records"]) == 2
    assert {record["mineral"] for record in document["records"]} == {"gold"}

    jsonl_output = tmp_path / "all.jsonl"
    assert export_database(database, output=jsonl_output, format_name="jsonl", mineral=None) == 3
    lines = jsonl_output.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3
    assert all(json.loads(line)["export_schema_version"] == 1 for line in lines)


def test_export_of_empty_database_is_valid(database: Database, tmp_path: Path) -> None:
    json_output = tmp_path / "empty.json"
    jsonl_output = tmp_path / "empty.jsonl"
    assert export_database(database, output=json_output, format_name="json", mineral=None) == 0
    assert json.loads(json_output.read_text(encoding="utf-8"))["records"] == []
    assert export_database(database, output=jsonl_output, format_name="jsonl", mineral=None) == 0
    assert jsonl_output.read_text(encoding="utf-8") == ""


def test_export_rejects_unknown_format_before_touching_output(
    database: Database, tmp_path: Path
) -> None:
    output = tmp_path / "data.csv"
    with pytest.raises(ValueError, match="format_name"):
        export_database(database, output=output, format_name="csv", mineral=None)
    assert not output.exists()


def test_export_refuses_existing_target_without_explicit_overwrite(
    database: Database, tmp_path: Path
) -> None:
    output = tmp_path / "existing.jsonl"
    output.write_text("keep-me\n", encoding="utf-8")
    with pytest.raises(ValueError, match="already exists"):
        export_database(database, output=output, format_name="jsonl", mineral=None)
    assert output.read_text(encoding="utf-8") == "keep-me\n"


def test_export_refuses_directory_and_live_database_aliases(
    database: Database, tmp_path: Path
) -> None:
    with pytest.raises(ValueError, match="file path"):
        export_database(database, output=tmp_path, format_name="jsonl", mineral=None)
    with pytest.raises(ValueError, match="live database"):
        export_database(
            database,
            output=database.path,
            format_name="jsonl",
            mineral=None,
            overwrite=True,
        )

    with pytest.raises(ValueError, match="live database"):
        export_database(
            database,
            output=database.operation_lock_path,
            format_name="jsonl",
            mineral=None,
            overwrite=True,
        )

    hardlink = tmp_path / "database-hardlink.sqlite3"
    os.link(database.path, hardlink)
    with pytest.raises(ValueError, match="live database"):
        export_database(
            database,
            output=hardlink,
            format_name="jsonl",
            mineral=None,
            overwrite=True,
        )


def test_export_create_if_absent_does_not_clobber_a_racing_writer(
    database: Database,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "race.jsonl"

    def racing_link(_source: object, destination: object) -> None:
        Path(destination).write_text("racing-writer\n", encoding="utf-8")
        raise FileExistsError

    monkeypatch.setattr("reddit_minerals.export.os.link", racing_link)
    with pytest.raises(ValueError, match="already exists"):
        export_database(database, output=output, format_name="jsonl", mineral=None)
    assert output.read_text(encoding="utf-8") == "racing-writer\n"
    assert list(tmp_path.glob(".race.jsonl.*.tmp")) == []


def test_export_holds_database_write_barrier_through_publication(
    database: Database,
    make_post: Callable[..., PostRecord],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database.store_scraped_post(make_post("p1"), [], mineral="gold")
    output = tmp_path / "barrier.jsonl"
    original_link = os.link

    def assert_writer_blocked_then_publish(source: object, destination: object) -> None:
        with closing(sqlite3.connect(database.path, timeout=0)) as competing:
            competing.execute("PRAGMA busy_timeout = 0")
            with pytest.raises(sqlite3.OperationalError, match="locked"):
                competing.execute("BEGIN IMMEDIATE")
        original_link(source, destination)

    monkeypatch.setattr("reddit_minerals.export.os.link", assert_writer_blocked_then_publish)
    assert export_database(database, output=output, format_name="jsonl", mineral="gold") == 1
    assert json.loads(output.read_text(encoding="utf-8"))["content"]["id"] == "p1"


def test_failed_export_preserves_previous_file_and_removes_temporary_file(
    tmp_path: Path,
) -> None:
    class ExplodingDatabase:
        @contextmanager
        def write_barrier(self) -> Iterator[None]:
            yield

        def export_records(self, _mineral: str | None) -> Iterator[dict[str, Any]]:
            yield {"id": 1}
            raise RuntimeError("simulated read failure")

    output = tmp_path / "data.jsonl"
    output.write_text("known-good\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="simulated"):
        export_database(  # type: ignore[arg-type]
            ExplodingDatabase(),
            output=output,
            format_name="jsonl",
            mineral=None,
            overwrite=True,
        )
    assert output.read_text(encoding="utf-8") == "known-good\n"
    assert list(tmp_path.glob(".data.jsonl.*.tmp")) == []


def _write_legacy_fixture(source: Path) -> None:
    mineral = source / "Gold"
    mineral.mkdir(parents=True)
    (mineral / "posts.json").write_text(
        json.dumps(
            [
                {
                    "id": "p1",
                    "title": "Legacy post",
                    "selftext": "Gold supply",
                    "subreddit": "mining",
                    "created_utc": 1_700_000_000,
                    "score": "5",
                    "num_comments": -3,
                    "upvote_ratio": 0.75,
                },
                {"id": "missing-subreddit", "created_utc": 1_700_000_000},
                {"id": "missing-time", "subreddit": "mining"},
            ]
        ),
        encoding="utf-8",
    )
    (mineral / "comments.json").write_text(
        json.dumps(
            [
                {
                    "id": "c1",
                    "post_id": "p1",
                    "body": "Legacy comment",
                    "subreddit": "mining",
                    "created_date": "2026-01-01T12:00:00Z",
                    "level": -2,
                },
                {
                    "post_id": "p1",
                    "subreddit": "mining",
                    "created_utc": 1_700_000_000,
                },
                {
                    "id": "orphan",
                    "post_id": "unknown",
                    "subreddit": "mining",
                    "created_utc": 1_700_000_000,
                },
            ]
        ),
        encoding="utf-8",
    )
    ignored = source / "empty-mineral"
    ignored.mkdir()
    (source / "not-a-directory.txt").write_text("ignored", encoding="utf-8")


def test_legacy_migration_dry_run_then_real_reports_invalid_and_orphan_records(
    database: Database, tmp_path: Path
) -> None:
    source = tmp_path / "legacy"
    _write_legacy_fixture(source)

    preview = migrate_legacy_data(database, source=source, dry_run=True)
    assert preview.model_dump() == {
        "minerals_seen": 1,
        "posts_imported": 1,
        "comments_imported": 1,
        "posts_suppressed_by_tombstone": 0,
        "comments_suppressed_by_tombstone": 0,
        "invalid_posts": 2,
        "invalid_comments": 1,
        "orphan_comments": 1,
        "dry_run": True,
    }
    assert database.status().posts == 0

    report = migrate_legacy_data(database, source=source, dry_run=False)
    assert report.dry_run is False
    assert database.status().posts == 1
    assert database.status().comments == 1
    records = list(database.export_records("gold"))
    post = next(record for record in records if record["record_type"] == "post")
    comment = next(record for record in records if record["record_type"] == "comment")
    assert post["content"]["num_comments"] == 0
    assert post["content"]["permalink"].endswith("/comments/p1")
    assert comment["content"]["depth"] == 0
    assert comment["content"]["permalink"].endswith("/comments/c1")


def test_legacy_migration_preview_and_live_import_classify_tombstone_suppression(
    database: Database, tmp_path: Path
) -> None:
    source = tmp_path / "legacy"
    mineral = source / "gold"
    mineral.mkdir(parents=True)
    (mineral / "posts.json").write_text(
        json.dumps(
            [
                {
                    "id": "deleted-post",
                    "subreddit": "mining",
                    "created_utc": 1_700_000_000,
                },
                {
                    "id": "kept-post",
                    "subreddit": "mining",
                    "created_utc": 1_700_000_001,
                },
            ]
        ),
        encoding="utf-8",
    )
    (mineral / "comments.json").write_text(
        json.dumps(
            [
                {
                    "id": "child-of-deleted-post",
                    "post_id": "deleted-post",
                    "subreddit": "mining",
                    "created_utc": 1_700_000_002,
                },
                {
                    "id": "deleted-comment",
                    "post_id": "kept-post",
                    "subreddit": "mining",
                    "created_utc": 1_700_000_003,
                },
                {
                    "id": "kept-comment",
                    "post_id": "kept-post",
                    "subreddit": "mining",
                    "created_utc": 1_700_000_004,
                },
                {
                    "id": "tombstoned-orphan",
                    "post_id": "missing-post",
                    "subreddit": "mining",
                    "created_utc": 1_700_000_005,
                },
            ]
        ),
        encoding="utf-8",
    )
    database.delete_content(
        content_kind=ContentKind.POST,
        content_id="deleted-post",
        dry_run=False,
    )
    database.delete_content(
        content_kind=ContentKind.COMMENT,
        content_id="deleted-comment",
        dry_run=False,
    )
    database.delete_content(
        content_kind=ContentKind.COMMENT,
        content_id="tombstoned-orphan",
        dry_run=False,
    )

    preview = migrate_legacy_data(database, source=source, dry_run=True)
    assert preview.model_dump() == {
        "minerals_seen": 1,
        "posts_imported": 1,
        "comments_imported": 1,
        "posts_suppressed_by_tombstone": 1,
        "comments_suppressed_by_tombstone": 2,
        "invalid_posts": 0,
        "invalid_comments": 0,
        "orphan_comments": 1,
        "dry_run": True,
    }
    assert database.status().posts == 0
    assert database.status().comments == 0

    imported = migrate_legacy_data(database, source=source, dry_run=False)
    assert imported.model_dump() == {**preview.model_dump(), "dry_run": False}
    assert database.status().posts == 1
    assert database.status().comments == 1
    assert {record["content"]["id"] for record in database.export_records("gold")} == {
        "kept-post",
        "kept-comment",
    }


def test_legacy_migration_deduplicates_comments_by_id_with_last_valid_record_winning(
    database: Database, tmp_path: Path
) -> None:
    source = tmp_path / "legacy"
    mineral = source / "gold"
    mineral.mkdir(parents=True)
    (mineral / "posts.json").write_text(
        json.dumps(
            [
                {"id": "first-post", "subreddit": "mining", "created_utc": 1_700_000_000},
                {"id": "last-post", "subreddit": "mining", "created_utc": 1_700_000_001},
            ]
        ),
        encoding="utf-8",
    )
    (mineral / "comments.json").write_text(
        json.dumps(
            [
                {
                    "id": "duplicate-comment",
                    "post_id": "first-post",
                    "body": "superseded",
                    "subreddit": "mining",
                    "created_utc": 1_700_000_002,
                },
                {
                    "id": "duplicate-comment",
                    "post_id": "last-post",
                    "body": "last valid record",
                    "subreddit": "mining",
                    "created_utc": 1_700_000_003,
                },
                {
                    "id": "duplicate-comment",
                    "post_id": "first-post",
                    "created_utc": 1_700_000_004,
                },
            ]
        ),
        encoding="utf-8",
    )

    preview = migrate_legacy_data(database, source=source, dry_run=True)
    assert preview.posts_imported == 2
    assert preview.comments_imported == 1
    assert preview.invalid_comments == 1
    assert preview.orphan_comments == 0

    imported = migrate_legacy_data(database, source=source, dry_run=False)
    assert imported.comments_imported == 1
    records = list(database.export_records("gold"))
    comment = next(record for record in records if record["record_type"] == "comment")
    assert comment["content"]["post_id"] == "last-post"
    assert comment["content"]["body"] == "last valid record"


def test_legacy_migration_counts_comments_without_a_known_post_as_orphans(
    database: Database, tmp_path: Path
) -> None:
    source = tmp_path / "legacy"
    mineral = source / "silver"
    mineral.mkdir(parents=True)
    (mineral / "comments.json").write_text(
        json.dumps(
            [
                {
                    "id": "c1",
                    "post_id": "missing",
                    "subreddit": "mining",
                    "created_at": "2026-01-01T00:00:00",
                }
            ]
        ),
        encoding="utf-8",
    )
    report = migrate_legacy_data(database, source=source, dry_run=False)
    assert report.minerals_seen == 1
    assert report.orphan_comments == 1
    assert report.posts_imported == 0


def test_legacy_migration_rejects_missing_source(tmp_path: Path, database: Database) -> None:
    with pytest.raises(ValueError, match="does not exist"):
        migrate_legacy_data(database, source=tmp_path / "missing", dry_run=False)
    file_source = tmp_path / "file"
    file_source.write_text("not a directory", encoding="utf-8")
    with pytest.raises(ValueError, match="does not exist"):
        migrate_legacy_data(database, source=file_source, dry_run=False)


@pytest.mark.parametrize(
    ("filename", "content", "message"),
    [
        ("posts.json", "{", "Cannot read legacy JSON"),
        ("posts.json", "{}", "must contain a JSON array"),
        ("comments.json", "[1]", "contains non-object entries"),
    ],
)
def test_legacy_migration_rejects_invalid_json_containers(
    database: Database,
    tmp_path: Path,
    filename: str,
    content: str,
    message: str,
) -> None:
    mineral = tmp_path / "legacy" / "gold"
    mineral.mkdir(parents=True)
    (mineral / filename).write_text(content, encoding="utf-8")
    with pytest.raises(ValueError, match=message):
        migrate_legacy_data(database, source=tmp_path / "legacy", dry_run=False)


def test_legacy_migration_enforces_file_record_and_directory_bounds(
    database: Database,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "legacy"
    mineral = source / "gold"
    mineral.mkdir(parents=True)
    posts = mineral / "posts.json"

    monkeypatch.setattr(migration, "MAX_LEGACY_FILE_BYTES", 10)
    posts.write_bytes(b" " * 11)
    with pytest.raises(ValueError, match="byte safety limit"):
        migrate_legacy_data(database, source=source, dry_run=True)

    monkeypatch.setattr(migration, "MAX_LEGACY_FILE_BYTES", 1_000)
    monkeypatch.setattr(migration, "MAX_LEGACY_RECORDS_PER_FILE", 1)
    posts.write_text("[{},{}]", encoding="utf-8")
    with pytest.raises(ValueError, match="record safety limit"):
        migrate_legacy_data(database, source=source, dry_run=True)

    monkeypatch.setattr(migration, "MAX_LEGACY_MINERAL_DIRECTORIES", 1)
    (source / "silver").mkdir()
    with pytest.raises(ValueError, match="mineral-directory safety limit"):
        migrate_legacy_data(database, source=source, dry_run=True)


def test_legacy_migration_rejects_directory_aliases(
    database: Database,
    tmp_path: Path,
) -> None:
    source = tmp_path / "legacy"
    source.mkdir()
    outside_directory = tmp_path / "outside-mineral"
    outside_directory.mkdir()
    (outside_directory / "posts.json").write_text("[]", encoding="utf-8")
    linked_directory = source / "gold"
    try:
        linked_directory.symlink_to(outside_directory, target_is_directory=True)
    except OSError:
        pytest.skip("directory symlinks are unavailable on this platform")

    with pytest.raises(ValueError, match="directory aliases"):
        migrate_legacy_data(database, source=source, dry_run=True)


def test_legacy_migration_rejects_file_aliases(
    database: Database,
    tmp_path: Path,
) -> None:
    source = tmp_path / "legacy"
    mineral = source / "gold"
    mineral.mkdir(parents=True)
    outside_file = tmp_path / "outside-posts.json"
    outside_file.write_text("[]", encoding="utf-8")
    try:
        (mineral / "posts.json").symlink_to(outside_file)
    except OSError:
        pytest.skip("file symlinks are unavailable on this platform")
    with pytest.raises(ValueError, match="file aliases"):
        migrate_legacy_data(database, source=source, dry_run=True)


def test_legacy_migration_rejects_alias_policy_without_platform_symlink_support(
    database: Database,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "legacy"
    mineral = source / "gold"
    mineral.mkdir(parents=True)
    (mineral / "posts.json").write_text("[]", encoding="utf-8")
    original = Path.is_symlink
    monkeypatch.setattr(
        Path,
        "is_symlink",
        lambda path: path == mineral or original(path),
    )

    with pytest.raises(ValueError, match="directory aliases"):
        migrate_legacy_data(database, source=source, dry_run=True)


def test_legacy_migration_rejects_file_alias_policy_without_symlink_support(
    database: Database,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "legacy"
    mineral = source / "gold"
    mineral.mkdir(parents=True)
    posts = mineral / "posts.json"
    posts.write_text("[]", encoding="utf-8")
    original = Path.is_symlink
    monkeypatch.setattr(
        Path,
        "is_symlink",
        lambda path: path == posts or original(path),
    )

    with pytest.raises(ValueError, match="file aliases"):
        migrate_legacy_data(database, source=source, dry_run=True)


def test_legacy_migration_normalizes_symlink_loop_resolution_errors(
    database: Database,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "legacy"
    source.mkdir()
    original = Path.resolve

    def loop_on_source(path: Path, *, strict: bool = False) -> Path:
        if path == source:
            raise RuntimeError("synthetic symlink loop")
        return original(path, strict=strict)

    monkeypatch.setattr(Path, "resolve", loop_on_source)
    with pytest.raises(ValueError, match="does not exist"):
        migrate_legacy_data(database, source=source, dry_run=True)


def test_legacy_migration_counts_out_of_range_timestamps_as_invalid_records(
    database: Database,
    tmp_path: Path,
) -> None:
    mineral = tmp_path / "legacy" / "gold"
    mineral.mkdir(parents=True)
    (mineral / "posts.json").write_text(
        json.dumps(
            [
                {"id": "p1", "subreddit": "mining", "created_utc": 1_700_000_000},
                {"id": "bad-post", "subreddit": "mining", "created_utc": "1e309"},
            ]
        ),
        encoding="utf-8",
    )
    (mineral / "comments.json").write_text(
        json.dumps(
            [
                {
                    "id": "bad-comment",
                    "post_id": "p1",
                    "subreddit": "mining",
                    "created_utc": "1e309",
                }
            ]
        ),
        encoding="utf-8",
    )

    report = migrate_legacy_data(database, source=tmp_path / "legacy", dry_run=True)
    assert report.posts_imported == 1
    assert report.invalid_posts == 1
    assert report.invalid_comments == 1
