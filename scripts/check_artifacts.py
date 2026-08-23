"""Install and smoke-test both distributable package formats outside the source tree."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def main() -> int:
    repository_root = Path(__file__).resolve().parent.parent
    distribution_directory = repository_root / "dist"
    wheel = _single_artifact(distribution_directory, "*.whl")
    source_distribution = _single_artifact(distribution_directory, "*.tar.gz")
    uv = shutil.which("uv")
    if uv is None:
        raise RuntimeError("uv is required to verify built artifacts")

    with tempfile.TemporaryDirectory(prefix="reddit-minerals-artifacts-") as raw_directory:
        temporary_root = Path(raw_directory)
        for index, artifact in enumerate((wheel, source_distribution), start=1):
            _check_artifact(
                uv=Path(uv),
                artifact=artifact,
                work_directory=temporary_root / f"artifact-{index}",
                uv_cache=repository_root / ".uv-cache",
            )
    return 0


def _single_artifact(directory: Path, pattern: str) -> Path:
    matches = sorted(directory.glob(pattern))
    if len(matches) != 1:
        raise RuntimeError(
            f"expected exactly one {pattern} artifact in {directory}, found {len(matches)}"
        )
    return matches[0].resolve()


def _check_artifact(
    *,
    uv: Path,
    artifact: Path,
    work_directory: Path,
    uv_cache: Path,
) -> None:
    environment = os.environ.copy()
    for name in tuple(environment):
        if name.startswith("RMS_") or name in {"PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV"}:
            environment.pop(name, None)
    environment["UV_CACHE_DIR"] = str(uv_cache)

    work_directory.mkdir(parents=True)
    virtual_environment = work_directory / "venv"
    _run(
        [str(uv), "venv", str(virtual_environment), "--python", sys.executable],
        cwd=work_directory,
        environment=environment,
    )
    python = _venv_executable(virtual_environment, "python")
    cli = _venv_executable(virtual_environment, "reddit-minerals")
    _run(
        [str(uv), "pip", "install", "--python", str(python), f"{artifact}[web]"],
        cwd=work_directory,
        environment=environment,
    )
    _run([str(cli), "--version"], cwd=work_directory, environment=environment)
    _run([str(cli), "demo"], cwd=work_directory, environment=environment)
    _run([str(cli), "validate-config"], cwd=work_directory, environment=environment)
    _run(
        [
            str(cli),
            "--database-path",
            str(work_directory / "state.sqlite3"),
            "status",
            "--json",
        ],
        cwd=work_directory,
        environment=environment,
    )
    _run(
        [
            str(python),
            "-c",
            (
                "from importlib.resources import files; "
                "package = files('reddit_minerals'); "
                "assert package.joinpath('py.typed').is_file(); "
                "assert package.joinpath('defaults/subreddit_mapping.json').is_file(); "
                "sample = package.joinpath('web/data/kaggle_sample.json'); "
                "assert sample.is_file(); "
                "from reddit_minerals.web import create_app; "
                "from reddit_minerals.web.repository import KaggleSampleReadRepository; "
                "repository = KaggleSampleReadRepository(); "
                "assert len(repository.snapshot().records) == 104; "
                "assert create_app(repository=repository).title == 'MineralLens API'"
            ),
        ],
        cwd=work_directory,
        environment=environment,
    )


def _venv_executable(virtual_environment: Path, name: str) -> Path:
    if os.name == "nt":
        suffix = ".exe"
        return virtual_environment / "Scripts" / f"{name}{suffix}"
    return virtual_environment / "bin" / name


def _run(command: list[str], *, cwd: Path, environment: dict[str, str]) -> None:
    subprocess.run(command, cwd=cwd, env=environment, check=True)


if __name__ == "__main__":
    raise SystemExit(main())
