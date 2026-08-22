from __future__ import annotations

import json
from pathlib import Path

import pytest

import reddit_minerals.cli as cli
from reddit_minerals.demo import DemoArtifactLifecycle, run_offline_demo
from reddit_minerals.storage import Database


def test_offline_demo_runs_real_pipeline_and_retains_inspectable_artifacts(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "demo"

    summary = run_offline_demo(
        workspace,
        lifecycle=DemoArtifactLifecycle.RETAINED,
        protected_database_path=tmp_path / "live.sqlite3",
    )

    assert summary.mode == "offline-synthetic"
    assert summary.network_access is False
    assert summary.database_schema == 3
    assert summary.counts.model_dump() == {
        "posts": 2,
        "comments": 2,
        "analyses": 8,
        "exported_records": 4,
    }
    assert summary.stages.model_dump() == {
        "scraped_posts": 2,
        "relevance": 2,
        "enrichment": 4,
        "reputation": 2,
    }
    assert summary.artifacts.lifecycle is DemoArtifactLifecycle.RETAINED
    assert summary.artifacts.database.is_file()
    assert summary.artifacts.export.is_file()

    records = [
        json.loads(line)
        for line in summary.artifacts.export.read_text(encoding="utf-8").splitlines()
    ]
    assert {record["record_type"] for record in records} == {"post", "comment"}
    assert all(record["mineral"] == "gold" for record in records)
    post_analyses = {
        tuple(sorted(record["analyses"])) for record in records if record["record_type"] == "post"
    }
    comment_analyses = {
        tuple(sorted(record["analyses"]))
        for record in records
        if record["record_type"] == "comment"
    }
    assert post_analyses == {("enrichment", "relevance", "reputation")}
    assert comment_analyses == {("enrichment",)}

    status = Database(summary.artifacts.database).status()
    assert status.posts == 2
    assert status.comments == 2
    assert status.analyses_by_kind_and_status == {
        "enrichment:complete": 4,
        "relevance:complete": 2,
        "reputation:complete": 2,
    }


def test_cli_demo_uses_removed_temporary_workspace_and_ignores_live_database(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    live_database = tmp_path / "live.sqlite3"
    sentinel = b"configured-live-database-must-not-be-opened"
    live_database.write_bytes(sentinel)

    def reject_provider_construction(**_kwargs: object) -> object:
        pytest.fail("offline demo attempted to construct a network provider")

    monkeypatch.setattr(cli, "PrawRedditClient", reject_provider_construction)
    monkeypatch.setattr(cli, "GeminiAnalysisClient", reject_provider_construction)

    code = cli.main(["--database-path", str(live_database), "demo"])

    captured = capsys.readouterr()
    assert code == 0
    assert captured.err == ""
    payload = json.loads(captured.out)
    assert payload["mode"] == "offline-synthetic"
    assert payload["network_access"] is False
    assert payload["counts"] == {
        "analyses": 8,
        "comments": 2,
        "exported_records": 4,
        "posts": 2,
    }
    assert payload["artifacts"]["lifecycle"] == "removed_after_command"
    assert not Path(payload["artifacts"]["workspace"]).exists()
    assert live_database.read_bytes() == sentinel


def test_cli_demo_help_and_retained_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as raised:
        cli.main(["demo", "--help"])
    assert raised.value.code == 0
    help_text = capsys.readouterr().out
    assert "deterministic synthetic data" in help_text
    assert "--output-dir" in help_text

    output_root = tmp_path / "portfolio-demo"
    assert cli.main(["demo", "--output-dir", str(output_root)]) == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    payload = json.loads(captured.out)
    workspace = Path(payload["artifacts"]["workspace"])
    assert payload["artifacts"]["lifecycle"] == "retained"
    assert workspace.parent == output_root.resolve()
    assert Path(payload["artifacts"]["database"]).is_file()
    assert Path(payload["artifacts"]["export"]).is_file()
