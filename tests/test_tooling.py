from __future__ import annotations

from pathlib import Path

import pytest

import scripts.validate_env_example as env_validator


def test_environment_template_failures_never_echo_template_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project = tmp_path / "project"
    script_path = project / "scripts" / "validate_env_example.py"
    script_path.parent.mkdir(parents=True)
    sensitive_value = "synthetic-sensitive-value-must-not-appear"
    template = f"RMS_GEMINI_API_KEY={sensitive_value}\n"  # pragma: allowlist secret
    (project / ".env.example").write_text(
        template,
        encoding="utf-8",
    )
    monkeypatch.setattr(env_validator, "__file__", str(script_path))

    assert env_validator.main() == 1

    captured = capsys.readouterr()
    assert sensitive_value not in f"{captured.out}\n{captured.err}"
    assert "validation failed" in captured.out
