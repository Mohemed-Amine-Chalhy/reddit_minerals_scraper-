"""Validate local Markdown links without network access."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

_LINK = re.compile(r"!?\[[^\]]*\]\((?P<target><[^>]+>|[^)\s]+)")
_TOP_LEVEL_DOCUMENTS = ("README.md", "CONTRIBUTING.md", "SECURITY.md", "CHANGELOG.md")


def discover_documents(repository_root: Path) -> list[Path]:
    """Return the maintained Markdown documentation set."""

    documents = [repository_root / name for name in _TOP_LEVEL_DOCUMENTS]
    documents.extend(sorted((repository_root / "docs").glob("**/*.md")))
    documents.extend(sorted((repository_root / "notebooks").glob("**/*.md")))
    return sorted(set(documents))


def validate_document(path: Path, repository_root: Path) -> list[str]:
    """Return failures for missing or workspace-escaping local link targets."""

    failures: list[str] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        for match in _LINK.finditer(line):
            raw_target = match.group("target").strip("<>")
            parsed = urlsplit(raw_target)
            if parsed.scheme or parsed.netloc or not parsed.path:
                continue
            target_path = Path(unquote(parsed.path.replace("\\", "/")))
            resolved = (path.parent / target_path).resolve()
            try:
                resolved.relative_to(repository_root)
            except ValueError:
                failures.append(
                    f"{path}:{line_number}: local link escapes the repository: {raw_target}"
                )
                continue
            if not resolved.exists():
                failures.append(
                    f"{path}:{line_number}: local link target does not exist: {raw_target}"
                )
    return failures


def main() -> int:
    """Validate maintained documentation and return a process exit code."""

    repository_root = Path(__file__).resolve().parents[1]
    documents = discover_documents(repository_root)
    failures = [
        failure
        for document in documents
        for failure in validate_document(document, repository_root)
    ]
    if failures:
        print("Documentation validation failed:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1

    print(f"Validated local links in {len(documents)} Markdown document(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
