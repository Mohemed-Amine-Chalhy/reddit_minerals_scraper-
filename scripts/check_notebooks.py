"""Validate that tracked notebooks are reproducible and safe to review."""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import nbformat

IGNORED_DIRECTORIES = {".git", ".ipynb_checkpoints", ".venv", "build", "dist"}


def discover_notebooks(arguments: list[str]) -> list[Path]:
    """Return explicitly provided notebooks or discover them below the repository root."""
    if arguments:
        return sorted(Path(argument) for argument in arguments)

    repository_root = Path(__file__).resolve().parents[1]
    return sorted(
        path
        for path in repository_root.rglob("*.ipynb")
        if not IGNORED_DIRECTORIES.intersection(path.parts)
    )


def validate_notebook(path: Path) -> list[str]:
    """Return validation failures for one notebook."""
    failures: list[str] = []
    try:
        notebook = nbformat.read(path, as_version=4)
        nbformat.validate(notebook)
    except Exception as error:
        return [f"{path}: invalid notebook: {error}"]

    for cell_number, cell in enumerate(notebook.cells, start=1):
        if cell.cell_type != "code":
            continue

        if cell.get("execution_count") is not None:
            failures.append(f"{path}: code cell {cell_number} has an execution count")
        if cell.get("outputs"):
            failures.append(f"{path}: code cell {cell_number} contains committed output")

        source = str(cell.source)
        if not source.strip():
            continue
        try:
            ast.parse(source, filename=f"{path}:cell-{cell_number}")
        except SyntaxError as error:
            failures.append(
                f"{path}: code cell {cell_number} is not valid Python: "
                f"{error.msg} (line {error.lineno})"
            )

    return failures


def main() -> int:
    """Validate all selected notebooks and return a process exit code."""
    notebooks = discover_notebooks(sys.argv[1:])
    failures = [failure for path in notebooks for failure in validate_notebook(path)]
    if failures:
        print("Notebook validation failed:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1

    print(f"Validated {len(notebooks)} clean notebook(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
