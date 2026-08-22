"""Atomic JSON and JSONL exports from canonical storage."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from reddit_minerals.storage import Database


def export_database(
    database: Database,
    *,
    output: Path,
    format_name: str,
    mineral: str | None,
    overwrite: bool = False,
) -> int:
    """Write a versioned export atomically and return its record count.

    By default publication is an atomic create-if-absent operation.  ``overwrite``
    switches publication to atomic replacement, but the live database itself is
    always protected.
    """

    if format_name not in {"json", "jsonl"}:
        raise ValueError("format_name must be 'json' or 'jsonl'")
    # The barrier holds SQLite's cross-process writer reservation through final
    # publication. A deletion can therefore happen before this snapshot begins or
    # after the file is visible, but never between snapshotting and publication.
    with database.write_barrier():
        _validate_export_target(database, output, overwrite=overwrite)
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary_path: Path | None = None
        count = 0
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                newline="\n",
                prefix=f".{output.name}.",
                suffix=".tmp",
                dir=output.parent,
                delete=False,
            ) as handle:
                temporary_path = Path(handle.name)
                if format_name == "json":
                    handle.write('{"export_schema_version":1,"records":[')
                    first = True
                    for record in database.export_records(mineral):
                        if not first:
                            handle.write(",")
                        json.dump(record, handle, ensure_ascii=False, separators=(",", ":"))
                        first = False
                        count += 1
                    handle.write("]}\n")
                else:
                    for record in database.export_records(mineral):
                        json.dump(record, handle, ensure_ascii=False, separators=(",", ":"))
                        handle.write("\n")
                        count += 1
                handle.flush()
                os.fsync(handle.fileno())
            if overwrite:
                os.replace(temporary_path, output)
            else:
                try:
                    # A same-directory hard link publishes the fully-fsynced inode only
                    # if the target name is still absent. Unlike a preflight exists()
                    # check followed by replace(), this cannot clobber a racing writer.
                    os.link(temporary_path, output)
                except FileExistsError as exc:
                    raise ValueError(
                        f"Export output already exists: {output}; pass --overwrite to replace it"
                    ) from exc
                temporary_path.unlink()
            temporary_path = None
            return count
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)


def _validate_export_target(database: Database, output: Path, *, overwrite: bool) -> None:
    database_path = getattr(database, "path", None)
    if isinstance(database_path, Path):
        protected_paths = (
            database_path,
            Path(f"{database_path}-wal"),
            Path(f"{database_path}-shm"),
            Path(f"{database_path}-journal"),
            database.operation_lock_path,
        )
        if any(_same_resolved_file(output, protected) for protected in protected_paths):
            raise ValueError(
                "Refusing to export over the live database or its operational sidecars"
            )
    if output.is_dir():
        raise ValueError(f"Export output must be a file path, not a directory: {output}")
    if not overwrite and os.path.lexists(output):
        raise ValueError(f"Export output already exists: {output}; pass --overwrite to replace it")


def _same_resolved_file(left: Path, right: Path) -> bool:
    if left.resolve(strict=False) == right.resolve(strict=False):
        return True
    try:
        return left.exists() and right.exists() and os.path.samefile(left, right)
    except OSError:
        return False
