from __future__ import annotations

import argparse
import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

import reddit_minerals.cli as cli
from reddit_minerals import __version__
from reddit_minerals.config import AppSettings
from reddit_minerals.errors import PermanentProviderError, RetryableProviderError
from reddit_minerals.models import AnalysisKind, PostRecord, RelevanceAnalysis
from reddit_minerals.storage import Database
from tests.fakes import FakeAnalysisClient, FakeRedditClient

_PRAW_TEST_VALUE = "valid-client-secret"
_GEMINI_TEST_VALUE = "valid-gemini-key"


def _mapping(path: Path) -> Path:
    path.write_text(json.dumps({"gold": ["mining", "Mining"]}), encoding="utf-8")
    return path


def _set_valid_provider_environment(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    values = {
        "RMS_REDDIT_CLIENT_ID": "valid-client-id",
        "RMS_REDDIT_CLIENT_SECRET": _PRAW_TEST_VALUE,
        "RMS_REDDIT_USER_AGENT": "minerals-research/1.0 contact@test.invalid",
        "RMS_GEMINI_API_KEY": _GEMINI_TEST_VALUE,
        "RMS_GEMINI_MODEL": "gemini-2.5-flash",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    return values


def test_cli_reports_installed_version(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc_info:
        cli.main(["--version"])

    assert exc_info.value.code == 0
    assert capsys.readouterr().out.strip() == f"reddit-minerals {__version__}"


def test_cli_validate_config_is_offline_and_never_prints_secret_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mapping = _mapping(tmp_path / "mapping.json")
    secrets = _set_valid_provider_environment(monkeypatch)
    code = cli.main(
        [
            "--mapping-path",
            str(mapping),
            "validate-config",
            "--require",
            "reddit",
            "--require",
            "ai",
        ]
    )
    captured = capsys.readouterr()
    assert code == 0
    payload = json.loads(captured.out)
    assert payload["valid"] is True
    assert payload["minerals"] == ["gold"]
    assert payload["mapping"]["duplicate_entries_removed"] == 1
    assert payload["settings"]["reddit_configured"] is True
    combined = captured.out + captured.err
    assert all(value not in combined for key, value in secrets.items() if key != "RMS_GEMINI_MODEL")


def test_cli_validation_error_log_omits_invalid_environment_input(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mapping = _mapping(tmp_path / "mapping.json")
    invalid_value = "private-invalid-value-93742"
    monkeypatch.setenv("RMS_MAX_RETRIES", invalid_value)
    code = cli.main(["--mapping-path", str(mapping), "validate-config"])
    captured = capsys.readouterr()
    assert code == 2
    assert invalid_value not in captured.err
    payload = json.loads(captured.err)
    assert payload["error_type"] == "ValidationError"
    assert payload["validation_errors"][0]["location"] == "max_retries"


def test_cli_rejects_placeholder_credentials_without_echoing_them(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mapping = _mapping(tmp_path / "mapping.json")
    placeholder = "replace-me-private-placeholder"
    monkeypatch.setenv("RMS_REDDIT_CLIENT_ID", placeholder)
    monkeypatch.setenv("RMS_REDDIT_CLIENT_SECRET", _PRAW_TEST_VALUE)
    monkeypatch.setenv("RMS_REDDIT_USER_AGENT", "minerals-research/1.0 contact@test.invalid")
    code = cli.main(["--mapping-path", str(mapping), "validate-config", "--require", "reddit"])
    captured = capsys.readouterr()
    assert code == 2
    assert placeholder not in captured.err
    assert "RMS_REDDIT_CLIENT_ID" in captured.err


def test_cli_status_export_migrate_and_delete_are_offline(
    tmp_path: Path,
    make_post: Callable[..., PostRecord],
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_path = tmp_path / "state.sqlite3"
    mapping = _mapping(tmp_path / "mapping.json")
    source = tmp_path / "legacy"
    mineral_dir = source / "gold"
    mineral_dir.mkdir(parents=True)
    (mineral_dir / "posts.json").write_text(
        json.dumps(
            [
                {
                    "id": "legacy",
                    "title": "Legacy",
                    "subreddit": "mining",
                    "created_utc": 1_700_000_000,
                }
            ]
        ),
        encoding="utf-8",
    )
    common = ["--database-path", str(database_path), "--mapping-path", str(mapping)]

    assert cli.main([*common, "status", "--json"]) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["posts"] == 0

    assert cli.main([*common, "migrate-legacy", "--source", str(source)]) == 0
    migration = json.loads(capsys.readouterr().out)
    assert migration["posts_imported"] == 1

    output = tmp_path / "export.jsonl"
    assert cli.main([*common, "export", "--output", str(output)]) == 0
    export_report = json.loads(capsys.readouterr().out)
    assert export_report["records"] == 1
    assert output.is_file()
    assert cli.main([*common, "export", "--output", str(output)]) == 2
    assert "already exists" in capsys.readouterr().err
    assert cli.main([*common, "export", "--output", str(output), "--overwrite"]) == 0
    assert json.loads(capsys.readouterr().out)["records"] == 1

    assert cli.main([*common, "delete-content", "--post-id", "legacy"]) == 2
    assert "Refusing deletion" in capsys.readouterr().err
    assert cli.main([*common, "delete-content", "--post-id", "legacy", "--dry-run"]) == 0
    preview = json.loads(capsys.readouterr().out)
    assert preview["content_found"] is True
    assert preview["dry_run"] is True
    assert cli.main([*common, "delete-content", "--post-id", "legacy", "--yes"]) == 0
    deleted = json.loads(capsys.readouterr().out)
    assert deleted["dry_run"] is False

    assert cli.main([*common, "status", "--json"]) == 0
    final_status = json.loads(capsys.readouterr().out)
    assert final_status["posts"] == 0
    assert {run["command"] for run in final_status["recent_runs"]} >= {
        "migrate-legacy",
        "export",
        "delete-content",
    }
    migrate_run = next(
        run for run in final_status["recent_runs"] if run["command"] == "migrate-legacy"
    )
    assert migrate_run["parameters"] == {
        "dry_run": False,
        "source": str(source),
    }
    deletion_parameters = [
        run["parameters"]
        for run in final_status["recent_runs"]
        if run["command"] == "delete-content"
    ]
    assert {"content_kind": "post", "dry_run": False} in deletion_parameters
    assert {"content_kind": "post", "dry_run": True} in deletion_parameters
    export_parameters = [
        run["parameters"] for run in final_status["recent_runs"] if run["command"] == "export"
    ]
    assert {
        "format": "jsonl",
        "mineral": None,
        "output": str(output),
        "overwrite": True,
    } in export_parameters


def test_cli_migration_dry_run_does_not_import(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    database_path = tmp_path / "state.sqlite3"
    source = tmp_path / "legacy"
    mineral = source / "gold"
    mineral.mkdir(parents=True)
    (mineral / "posts.json").write_text("[]", encoding="utf-8")
    code = cli.main(
        [
            "--database-path",
            str(database_path),
            "migrate-legacy",
            "--source",
            str(source),
            "--dry-run",
        ]
    )
    assert code == 0
    assert json.loads(capsys.readouterr().out)["dry_run"] is True


def test_cli_scrape_uses_injected_offline_client(
    tmp_path: Path,
    make_post: Callable[..., PostRecord],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_path = tmp_path / "state.sqlite3"
    mapping = _mapping(tmp_path / "mapping.json")
    _set_valid_provider_environment(monkeypatch)
    fake = FakeRedditClient()
    fake.queue_search("gold", "mining", (make_post("p1"),))
    fake.queue_comments("p1", ())
    received: dict[str, Any] = {}

    def client_factory(**kwargs: Any) -> FakeRedditClient:
        received.update(kwargs)
        return fake

    monkeypatch.setattr(cli, "PrawRedditClient", client_factory)
    code = cli.main(
        [
            "--database-path",
            str(database_path),
            "--mapping-path",
            str(mapping),
            "scrape",
            "--mineral",
            "Gold",
            "--max-posts",
            "1",
            "--max-comments",
            "0",
            "--refresh-after-hours",
            "0",
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    assert code == 0
    assert payload["posts_completed"] == 1
    assert received["client_id"] == "valid-client-id"
    assert received["client_secret"] == _PRAW_TEST_VALUE
    assert received["request_timeout_seconds"] == 30
    run = Database(database_path).status().recent_runs[0]
    assert run["parameters"]["minerals"] == ["gold"]
    assert run["parameters"]["all_configured_minerals"] is False


def test_cli_analysis_uses_injected_offline_client(
    tmp_path: Path,
    make_post: Callable[..., PostRecord],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_path = tmp_path / "state.sqlite3"
    database = Database(database_path)
    database.initialize()
    database.store_scraped_post(make_post("p1"), [], mineral="gold")
    _set_valid_provider_environment(monkeypatch)
    fake = FakeAnalysisClient(model="gemini-2.5-flash")
    received: dict[str, Any] = {}

    def client_factory(**kwargs: Any) -> FakeAnalysisClient:
        received.update(kwargs)
        return fake

    monkeypatch.setattr(cli, "GeminiAnalysisClient", client_factory)
    code = cli.main(
        [
            "--database-path",
            str(database_path),
            "relevance",
            "--mineral",
            " GOLD ",
            "--limit",
            "1",
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    assert code == 0
    assert payload["kind"] == AnalysisKind.RELEVANCE.value
    assert payload["completed"] == 1
    assert received["model"] == "gemini-2.5-flash"
    assert received["request_timeout_seconds"] == 120


def test_cli_all_provider_item_failures_are_nonzero_and_preserve_partial_summary(
    tmp_path: Path,
    make_post: Callable[..., PostRecord],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_path = tmp_path / "state.sqlite3"
    database = Database(database_path)
    database.initialize()
    database.store_scraped_post(make_post("p1"), [], mineral="gold")
    _set_valid_provider_environment(monkeypatch)
    fake = FakeAnalysisClient()
    fake.queue(AnalysisKind.RELEVANCE, "p1", RetryableProviderError("temporary"))
    monkeypatch.setattr(cli, "GeminiAnalysisClient", lambda **_kwargs: fake)

    code = cli.main(
        [
            "--database-path",
            str(database_path),
            "relevance",
            "--mineral",
            "gold",
            "--limit",
            "1",
        ]
    )
    assert code == 1
    assert "BatchProviderFailureError" in capsys.readouterr().err
    run = database.status().recent_runs[0]
    assert run["status"] == "failed"
    assert run["summary"]["retryable_failures"] == 1
    assert run["parameters"]["model"] == "gemini-2.5-flash"
    assert database.status().analyses_by_kind_and_status == {"relevance:retryable_failure": 1}


def test_cli_tracks_failed_local_operation(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    database_path = tmp_path / "state.sqlite3"
    missing = tmp_path / "missing"
    code = cli.main(
        [
            "--database-path",
            str(database_path),
            "migrate-legacy",
            "--source",
            str(missing),
        ]
    )
    assert code == 2
    capsys.readouterr()
    database = Database(database_path)
    runs = database.status().recent_runs
    assert runs[0]["command"] == "migrate-legacy"
    assert runs[0]["status"] == "failed"
    assert runs[0]["error_type"] == "ValueError"


def test_cli_expected_unexpected_and_interrupt_exit_codes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_path = tmp_path / "state.sqlite3"
    mapping = _mapping(tmp_path / "mapping.json")
    _set_valid_provider_environment(monkeypatch)

    def expected_failure(**_kwargs: Any) -> object:
        raise PermanentProviderError("safe adapter initialization failure")

    monkeypatch.setattr(cli, "PrawRedditClient", expected_failure)
    assert (
        cli.main(
            [
                "--database-path",
                str(database_path),
                "--mapping-path",
                str(mapping),
                "scrape",
            ]
        )
        == 1
    )
    assert "PermanentProviderError" in capsys.readouterr().err
    initialization_run = Database(database_path).status().recent_runs[0]
    assert initialization_run["command"] == "scrape"
    assert initialization_run["status"] == "failed"
    assert initialization_run["error_type"] == "PermanentProviderError"
    assert initialization_run["parameters"]["minerals"] == ["gold"]
    assert initialization_run["parameters"]["all_configured_minerals"] is True

    monkeypatch.setattr(
        cli, "_dispatch", lambda _args, _settings: (_ for _ in ()).throw(RuntimeError())
    )
    assert cli.main(["validate-config"]) == 1
    assert "unexpected operation failure" in capsys.readouterr().err

    monkeypatch.setattr(
        cli, "_dispatch", lambda _args, _settings: (_ for _ in ()).throw(KeyboardInterrupt())
    )
    assert cli.main(["validate-config"]) == 130
    assert "operation interrupted" in capsys.readouterr().err


def test_cli_termination_request_returns_sigterm_exit_code(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    cli.configure_logging("INFO")
    monkeypatch.setattr(
        cli,
        "_run",
        lambda _argv: (_ for _ in ()).throw(cli._TerminationRequested()),
    )
    assert cli.main([]) == 143
    assert "termination requested" in capsys.readouterr().err
    with pytest.raises(cli._TerminationRequested):
        cli._request_termination(15, None)


def test_cli_reconciles_an_interrupted_run_only_after_writer_lock_acquisition(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_path = tmp_path / "state.sqlite3"
    database = Database(database_path)
    database.initialize()
    run_id = database.start_run("scrape", parameters={"minerals": ["gold"]})

    assert cli.main(["--database-path", str(database_path), "status", "--json"]) == 0
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["runs_by_status"] == {"running": 1}
    assert "reconciled_runs" not in captured.err

    output = tmp_path / "snapshot.jsonl"
    assert (
        cli.main(
            [
                "--database-path",
                str(database_path),
                "export",
                "--output",
                str(output),
            ]
        )
        == 0
    )
    assert '"reconciled_runs": 1' in capsys.readouterr().err

    assert cli.main(["--database-path", str(database_path), "status", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["runs_by_status"] == {"complete": 1, "failed": 1}
    interrupted = next(run for run in payload["recent_runs"] if run["id"] == run_id)
    assert interrupted["error_type"] == "InterruptedRun"


def test_cli_allows_concurrent_status_but_rejects_an_overlapping_tracked_command(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_path = tmp_path / "state.sqlite3"
    database = Database(database_path)
    database.initialize()

    with database.operation_lock():
        assert cli.main(["--database-path", str(database_path), "status", "--json"]) == 0
        assert json.loads(capsys.readouterr().out)["runs_by_status"] == {}
        assert (
            cli.main(
                [
                    "--database-path",
                    str(database_path),
                    "export",
                    "--output",
                    str(tmp_path / "blocked.jsonl"),
                ]
            )
            == 1
        )

    assert "ConcurrentOperationError" in capsys.readouterr().err
    assert database.status().recent_runs == []


def test_tracking_preserves_original_failure_when_run_finalization_fails(
    capsys: pytest.CaptureFixture[str],
) -> None:
    class BrokenFinalizationDatabase:
        @contextmanager
        def operation_lock(self) -> Iterator[None]:
            yield

        def reconcile_stale_runs(self) -> int:
            return 0

        def start_run(self, _command: str, parameters: object | None = None) -> str:
            assert parameters == {"safe": True}
            return "run-id"

        def finish_run(self, *_args: object, **_kwargs: object) -> None:
            raise RuntimeError("database unavailable")

    def fail_operation() -> object:
        raise PermanentProviderError("original provider failure")

    cli.configure_logging("INFO")
    with pytest.raises(PermanentProviderError, match="original provider failure"):
        cli._tracked(  # type: ignore[arg-type]
            BrokenFinalizationDatabase(),
            "scrape",
            fail_operation,
            parameters={"safe": True},
        )
    assert "failed to finalize unsuccessful run record" in capsys.readouterr().err


@pytest.mark.parametrize(
    "argv",
    [
        ["scrape", "--max-posts", "0"],
        ["scrape", "--max-comments", "-1"],
        ["relevance", "--limit", "0"],
    ],
)
def test_cli_parser_rejects_unbounded_or_negative_limits(argv: list[str]) -> None:
    with pytest.raises(SystemExit) as raised:
        cli.build_parser().parse_args(argv)
    assert raised.value.code == 2


def test_cli_private_json_helpers_cover_dict_model_and_scalar(tmp_path: Path) -> None:
    assert cli._jsonable({"ok": True}) == {"ok": True}
    model = RelevanceAnalysis(relevant=True, confidence=80, rationale="direct")
    assert cli._jsonable(model)["confidence"] == 80
    assert cli._jsonable(3) == {"result": "3"}
    assert cli._normalize_mineral(None) is None
    assert cli._effective_minerals(None, {"silver": (), "gold": ()}) == ["gold", "silver"]
    assert cli._effective_minerals([" Gold ", "gold"], {"gold": ()}) == ["gold"]
    namespace = argparse.Namespace(command="not-real")
    settings = AppSettings(database_path=tmp_path / "state.sqlite3")
    with pytest.raises(AssertionError, match="Unhandled command"):
        cli._dispatch(namespace, settings)
