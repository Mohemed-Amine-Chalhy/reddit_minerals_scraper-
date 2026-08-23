"""Build the repository-safe MineralLens sample from the public Kaggle archive.

The source release intentionally contains derived labels and metadata, but not the
original Reddit text or authors. This script preserves that boundary and replaces
source identifiers with deterministic, repository-local hashes.
"""

from __future__ import annotations

import argparse
import ast
import csv
import hashlib
import io
import json
import math
import zipfile
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARCHIVE = ROOT / ".dataset-cache" / "reddit-mining-stance.zip"
DEFAULT_OUTPUT = ROOT / "src" / "reddit_minerals" / "web" / "data" / "kaggle_sample.json"

ARCHIVE_SHA256 = "3A299CEC89CB091E9AD9E8F4158FD264A761C92BD9CA5B37B94924D99C3D7407"
DATASET_URL = "https://www.kaggle.com/datasets/mohamedaminechalhy/reddit-mining-stance"
EXPECTED_FILES = {
    "comments_final.csv": 311_383_426,
    "posts_final.csv": 5_063_892,
}
EXPECTED_ROWS = {"comments_final.csv": 1_026_784, "posts_final.csv": 15_779}
VALID_SENTIMENTS = {"positive", "negative", "neutral"}
VALID_STANCES = {"pro-mining", "anti-mining", "neutral"}
SAMPLE_RECORDS_PER_MINERAL = 2
MAX_LIST_ITEMS = 12

DISPLAY_BODY = (
    "Original Reddit text is not included in the public Kaggle release. "
    "This view presents released metadata and model-derived labels only."
)
RECORD_SOURCE_NOTE = (
    "Derived from the public Kaggle v2 metadata export. Source text and authors are absent; "
    "identifiers are repository-local hashes."
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--archive",
        type=Path,
        default=DEFAULT_ARCHIVE,
        help="Path to the downloaded Kaggle ZIP archive.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Destination for the deterministic, repository-safe JSON sample.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify that the committed output is current without changing it.",
    )
    return parser.parse_args()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest().upper()


def _validate_archive(path: Path) -> None:
    if not path.is_file():
        raise FileNotFoundError(
            f"Kaggle archive not found at {path}. See docs/demo-data.md for download steps."
        )
    actual_digest = _sha256(path)
    if actual_digest != ARCHIVE_SHA256:
        raise ValueError(
            "Unexpected Kaggle archive checksum: "
            f"expected {ARCHIVE_SHA256}, received {actual_digest}"
        )
    with zipfile.ZipFile(path) as archive:
        actual_files = {entry.filename: entry.file_size for entry in archive.infolist()}
    if actual_files != EXPECTED_FILES:
        raise ValueError(
            f"Unexpected Kaggle archive contents: expected {EXPECTED_FILES}, received {actual_files}"
        )


def _text(value: object) -> str:
    return " ".join(str(value or "").strip().split())


def _integer(value: object, *, minimum: int | None = None) -> int:
    parsed = int(float(_text(value)))
    return max(parsed, minimum) if minimum is not None else parsed


def _number(value: object, *, default: float = 0.0) -> float:
    try:
        return float(_text(value))
    except ValueError:
        return default


def _timestamp(value: object) -> datetime:
    normalized = _text(value).replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _iso_timestamp(value: object) -> str:
    return _timestamp(value).isoformat(timespec="seconds").replace("+00:00", "Z")


def _normalize_label(value: object) -> str:
    return _text(value).casefold().replace("_", "-")


def _list_field(value: object, *, maximum_length: int) -> list[str]:
    raw = _text(value)
    if not raw:
        return []
    try:
        candidate = ast.literal_eval(raw)
    except (SyntaxError, ValueError):
        candidate = [raw]
    if isinstance(candidate, str):
        candidate = [candidate]
    if not isinstance(candidate, (list, tuple, set)):
        return []

    values: list[str] = []
    seen: set[str] = set()
    for item in candidate:
        normalized = _text(item)[:maximum_length]
        identity = normalized.casefold()
        if normalized and identity not in seen:
            seen.add(identity)
            values.append(normalized)
        if len(values) == MAX_LIST_ITEMS:
            break
    return values


def _concerns(row: Mapping[str, str], *, prefix: str) -> list[dict[str, object]]:
    values: list[dict[str, object]] = []
    for key, raw_score in row.items():
        if not key.startswith(prefix):
            continue
        normalized_score = _text(raw_score)
        if not normalized_score:
            continue
        score = _number(normalized_score)
        if not math.isfinite(score) or not 0 < score <= 1:
            continue
        values.append(
            {
                "name": _text(key.removeprefix(prefix)).casefold(),
                "score": round(score, 4),
            }
        )
    return sorted(values, key=lambda item: (-float(item["score"]), str(item["name"])))[:8]


def _safe_id(kind: str, source_id: str, mineral: str) -> str:
    material = f"minerallens:kaggle:v2:{kind}:{source_id}:{mineral}".encode()
    return f"kg-{kind}-{hashlib.sha256(material).hexdigest()[:20]}"


def _open_rows(archive: zipfile.ZipFile, name: str) -> Iterable[dict[str, str]]:
    with (
        archive.open(name) as raw_stream,
        io.TextIOWrapper(raw_stream, encoding="utf-8-sig", newline="") as text_stream,
    ):
        yield from csv.DictReader(text_stream)


def _is_valid_analysis(sentiment: str, stance: str) -> bool:
    return sentiment in VALID_SENTIMENTS and stance in VALID_STANCES


def _choose_comments(
    archive: zipfile.ZipFile,
) -> tuple[dict[tuple[str, str], dict[str, str]], tuple[datetime, datetime], int]:
    selected: dict[tuple[str, str], dict[str, str]] = {}
    earliest: datetime | None = None
    latest: datetime | None = None
    rows = 0
    for row in _open_rows(archive, "comments_final.csv"):
        rows += 1
        created_at = _timestamp(row["created_utc"])
        earliest = created_at if earliest is None else min(earliest, created_at)
        latest = created_at if latest is None else max(latest, created_at)
        sentiment = _normalize_label(row["analysis_sentiment"])
        stance = _normalize_label(row["analysis_mining_stance"])
        post_id = _text(row["post_id"])
        mineral = _text(row["mineral"]).casefold()
        source_id = _text(row["id"])
        is_direct_reply = _text(row["level"]) == "0" and _text(row["parent_id"]) == f"t3_{post_id}"
        if (
            not post_id
            or not mineral
            or not source_id
            or not is_direct_reply
            or not _is_valid_analysis(sentiment, stance)
        ):
            continue
        identity = (post_id, mineral)
        current = selected.get(identity)
        candidate_key = (_integer(row["score"]), created_at, source_id)
        if current is None:
            selected[identity] = row
            continue
        current_key = (
            _integer(current["score"]),
            _timestamp(current["created_utc"]),
            _text(current["id"]),
        )
        if candidate_key > current_key:
            selected[identity] = row
    if earliest is None or latest is None:
        raise ValueError("The comments export is empty")
    return selected, (earliest, latest), rows


def _choose_posts(
    archive: zipfile.ZipFile,
    comments_by_post: Mapping[tuple[str, str], dict[str, str]],
) -> tuple[list[dict[str, str]], tuple[datetime, datetime], int]:
    candidates: dict[str, dict[str, dict[str, str]]] = defaultdict(dict)
    earliest: datetime | None = None
    latest: datetime | None = None
    rows = 0
    for row in _open_rows(archive, "posts_final.csv"):
        rows += 1
        created_at = _timestamp(row["created_at"])
        earliest = created_at if earliest is None else min(earliest, created_at)
        latest = created_at if latest is None else max(latest, created_at)
        source_id = _text(row[""])
        mineral = _text(row["mineral"]).casefold()
        sentiment = _normalize_label(row["sentiment"])
        stance = _normalize_label(row["mining_stance"])
        if (
            source_id
            and mineral
            and (source_id, mineral) in comments_by_post
            and _is_valid_analysis(sentiment, stance)
        ):
            current = candidates[mineral].get(source_id)
            candidate_key = (created_at, _integer(row["score"]), _text(row["unique_id"]))
            if current is None:
                candidates[mineral][source_id] = row
            else:
                current_key = (
                    _timestamp(current["created_at"]),
                    _integer(current["score"]),
                    _text(current["unique_id"]),
                )
                if candidate_key > current_key:
                    candidates[mineral][source_id] = row
    if earliest is None or latest is None:
        raise ValueError("The posts export is empty")

    selected: list[dict[str, str]] = []
    for mineral, rows_by_source_id in sorted(candidates.items()):
        rows_for_mineral = list(rows_by_source_id.values())
        by_score = max(
            rows_for_mineral,
            key=lambda row: (_integer(row["score"]), _timestamp(row["created_at"]), row[""]),
        )
        remaining = [row for row in rows_for_mineral if row[""] != by_score[""]]
        if not remaining:
            raise ValueError(f"Mineral {mineral!r} does not have two qualifying posts")
        newest = max(
            remaining,
            key=lambda row: (_timestamp(row["created_at"]), _integer(row["score"]), row[""]),
        )
        selected.extend((by_score, newest))

    if len(selected) != 26 * SAMPLE_RECORDS_PER_MINERAL:
        raise ValueError(
            "Expected two qualifying posts for each of 26 minerals, "
            f"selected {len(selected)} posts across {len(candidates)} minerals"
        )
    return selected, (earliest, latest), rows


def _display_topic(row: Mapping[str, str], *, field: str, themes: list[str]) -> str:
    topic = _text(row.get(field, "")).casefold()
    if topic:
        return topic[:80]
    if themes:
        return themes[0].casefold()[:80]
    return "released classification"


def _enrichment(
    row: Mapping[str, str],
    *,
    sentiment_field: str,
    stance_field: str,
    keywords_field: str,
    themes_field: str,
    topic_field: str,
    concern_prefix: str,
) -> tuple[dict[str, object], str]:
    keywords = _list_field(row.get(keywords_field, ""), maximum_length=80)
    themes = _list_field(row.get(themes_field, ""), maximum_length=120)
    topic = _text(row.get(topic_field, ""))[:120]
    if topic and topic.casefold() not in {value.casefold() for value in themes}:
        themes.append(topic)
    display_topic = _display_topic(row, field=topic_field, themes=themes)
    return (
        {
            "sentiment": _normalize_label(row[sentiment_field]),
            "stance": _normalize_label(row[stance_field]),
            "keywords": keywords,
            "themes": themes[:MAX_LIST_ITEMS],
            "concerns": _concerns(row, prefix=concern_prefix),
        },
        display_topic,
    )


def _post_record(row: Mapping[str, str]) -> dict[str, object]:
    source_id = _text(row[""])
    mineral = _text(row["mineral"]).casefold()
    enrichment, display_topic = _enrichment(
        row,
        sentiment_field="sentiment",
        stance_field="mining_stance",
        keywords_field="keywords",
        themes_field="themes",
        topic_field="topic_classification",
        concern_prefix="concerns_detected_",
    )
    return {
        "id": _safe_id("post", source_id, mineral),
        "kind": "post",
        "parent_id": None,
        "mineral": mineral,
        "topic_label": display_topic,
        "title": f"{mineral.title()} post analysis · {display_topic}",
        "body": DISPLAY_BODY,
        "subreddit": _text(row["subreddit"]),
        "created_at": _iso_timestamp(row["created_at"]),
        "score": _integer(row["score"]),
        "comment_count": _integer(row["num_comments"], minimum=0),
        "analysis": {
            "relevance": {
                "relevant": _normalize_label(row["relevant"]) in {"true", "1", "yes"},
                "confidence": min(max(_number(row["confidence"]), 0.0), 100.0),
                "rationale": (
                    "Released model-derived relevance label; original source text is not included."
                ),
            },
            "enrichment": enrichment,
            "reputation": None,
        },
        "source_note": RECORD_SOURCE_NOTE,
        "synthetic": False,
        "content_available": False,
    }


def _comment_record(row: Mapping[str, str]) -> dict[str, object]:
    source_id = _text(row["id"])
    source_post_id = _text(row["post_id"])
    mineral = _text(row["mineral"]).casefold()
    enrichment, display_topic = _enrichment(
        row,
        sentiment_field="analysis_sentiment",
        stance_field="analysis_mining_stance",
        keywords_field="analysis_keywords",
        themes_field="analysis_themes",
        topic_field="analysis_topic_classification",
        concern_prefix="analysis_concerns_detected_",
    )
    return {
        "id": _safe_id("comment", f"{source_id}:{source_post_id}", mineral),
        "kind": "comment",
        "parent_id": _safe_id("post", source_post_id, mineral),
        "mineral": mineral,
        "topic_label": display_topic,
        "title": None,
        "body": DISPLAY_BODY,
        "subreddit": _text(row["subreddit"]),
        "created_at": _iso_timestamp(row["created_utc"]),
        "score": _integer(row["score"]),
        "comment_count": None,
        "analysis": {
            "relevance": None,
            "enrichment": enrichment,
            "reputation": None,
        },
        "source_note": RECORD_SOURCE_NOTE,
        "synthetic": False,
        "content_available": False,
    }


def build_sample(archive_path: Path) -> dict[str, object]:
    """Build and validate the deterministic sample document."""

    _validate_archive(archive_path)
    with zipfile.ZipFile(archive_path) as archive:
        comments_by_post, comment_range, comment_rows = _choose_comments(archive)
        selected_posts, post_range, post_rows = _choose_posts(archive, comments_by_post)

    if comment_rows != EXPECTED_ROWS["comments_final.csv"]:
        raise ValueError(f"Unexpected comment row count: {comment_rows}")
    if post_rows != EXPECTED_ROWS["posts_final.csv"]:
        raise ValueError(f"Unexpected post row count: {post_rows}")

    records: list[dict[str, object]] = []
    for post in selected_posts:
        records.append(_post_record(post))
        post_identity = (_text(post[""]), _text(post["mineral"]).casefold())
        records.append(_comment_record(comments_by_post[post_identity]))
    records.sort(key=lambda record: (str(record["created_at"]), str(record["id"])), reverse=True)

    record_id_counts = Counter(str(record["id"]) for record in records)
    record_ids = set(record_id_counts)
    if len(record_ids) != len(records):
        duplicates = sorted(record_id for record_id, count in record_id_counts.items() if count > 1)
        raise ValueError(f"Generated sample identifiers are not unique: {duplicates}")
    for record in records:
        parent_id = record["parent_id"]
        if parent_id is not None and parent_id not in record_ids:
            raise ValueError(f"Comment parent {parent_id!r} is not included in the sample")

    earliest = min(comment_range[0], post_range[0])
    latest = max(comment_range[1], post_range[1])
    mineral_count = len({str(record["mineral"]) for record in records})
    post_count = sum(record["kind"] == "post" for record in records)
    comment_count = sum(record["kind"] == "comment" for record in records)
    return {
        "schema_version": 1,
        "provenance": {
            "kind": "public-research-sample",
            "dataset_label": "Reddit Mining Stance · public research sample",
            "dataset_description": (
                "A deterministic, repository-safe sample of the public dataset collected with "
                "this research tool. It contains released metadata and model-derived labels, "
                "not original Reddit text or authors."
            ),
            "owner_name": "Mohamed Amine Chalhy",
            "dataset_ref": "mohamedaminechalhy/reddit-mining-stance",
            "dataset_slug": "reddit-mining-stance",
            "dataset_url": DATASET_URL,
            "dataset_version": 2,
            "published_at": "2025-09-28T16:47:03.01Z",
            "archive_sha256": ARCHIVE_SHA256,
            "license": "MIT",
            "published_totals": {
                "minerals": 26,
                "posts": post_rows,
                "comments": comment_rows,
                "records": post_rows + comment_rows,
            },
            "published_date_range": {
                "start": earliest.date().isoformat(),
                "end": latest.date().isoformat(),
            },
            "sample_totals": {
                "minerals": mineral_count,
                "posts": post_count,
                "comments": comment_count,
                "records": len(records),
            },
            "sample_method": (
                "For each of 26 minerals, select the highest-scoring qualifying post and the "
                "most recent distinct qualifying post, then pair each with its highest-scoring "
                "qualifying direct reply. Ties use timestamp and source ID before IDs are hashed."
            ),
            "raw_text_included": False,
            "authors_included": False,
            "source_note": (
                "Derived from the public Kaggle dataset version 2. The public release omits raw "
                "Reddit text and author fields; this sample preserves that boundary and replaces "
                "source identifiers with deterministic repository-local hashes. Labels are "
                "model-derived research signals, not ground-truth annotations. Exact released "
                "metadata can remain linkable, so these hashes are not an anonymization claim."
            ),
        },
        "records": records,
    }


def _serialized_sample(archive_path: Path) -> bytes:
    payload = build_sample(archive_path)
    return (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def main() -> int:
    args = _parse_args()
    archive_path = args.archive.resolve()
    output_path = args.output.resolve()
    expected = _serialized_sample(archive_path)
    if args.check:
        if not output_path.is_file() or output_path.read_bytes() != expected:
            raise SystemExit(
                f"{output_path} is missing or stale; rerun scripts/prepare_kaggle_sample.py"
            )
        print(f"Verified {output_path} ({len(expected):,} bytes)")
        return 0

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_suffix(f"{output_path.suffix}.tmp")
    temporary_path.write_bytes(expected)
    temporary_path.replace(output_path)
    print(f"Wrote {output_path} ({len(expected):,} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
