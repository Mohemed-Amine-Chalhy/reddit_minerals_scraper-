from __future__ import annotations

import json
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path

import pytest

from reddit_minerals.export import export_database
from reddit_minerals.models import AnalysisKind, CommentRecord, PostRecord
from reddit_minerals.services import AnalysisService, ScrapeService
from reddit_minerals.storage import Database
from tests.fakes import FakeAnalysisClient, FakeRedditClient


@pytest.mark.integration
@pytest.mark.smoke
def test_offline_end_to_end_collection_analysis_and_export(
    tmp_path: Path,
    make_post: Callable[..., PostRecord],
    make_comment: Callable[..., CommentRecord],
) -> None:
    database = Database(tmp_path / "pipeline.sqlite3")
    database.initialize()

    reddit = FakeRedditClient()
    reddit.queue_search(
        "gold",
        "mining",
        (
            make_post("p1", title="Gold supply improves"),
            make_post("p2", title="Gold mine water concerns", created_offset=1),
        ),
    )
    reddit.queue_comments("p1", (make_comment("c1", post_id="p1", body="Demand is stable"),))
    reddit.queue_comments("p2", (make_comment("c2", post_id="p2", body="Protect local water"),))
    scrape = ScrapeService(
        client=reddit,
        database=database,
        max_retries=2,
        retry_base_delay_seconds=0,
        retry_max_delay_seconds=0,
    ).run(
        mapping={"gold": ("mining",)},
        minerals=["gold"],
        max_posts_per_mineral=2,
        max_comments_per_post=2,
        refresh_after=timedelta(hours=24),
        time_filter="year",
        dry_run=False,
        force=False,
    )
    assert scrape.posts_completed == 2
    assert scrape.comments_stored == 2

    analysis_client = FakeAnalysisClient()
    analyzer = AnalysisService(
        client=analysis_client,
        database=database,
        max_retries=2,
        retry_base_delay_seconds=0,
        retry_max_delay_seconds=0,
    )
    relevance = analyzer.run(
        AnalysisKind.RELEVANCE,
        mineral="gold",
        limit=10,
        force=False,
        relevance_threshold=70,
        max_context_comments=2,
    )
    enrichment = analyzer.run(
        AnalysisKind.ENRICHMENT,
        mineral="gold",
        limit=10,
        force=False,
        relevance_threshold=70,
        max_context_comments=2,
    )
    reputation = analyzer.run(
        AnalysisKind.REPUTATION,
        mineral="gold",
        limit=10,
        force=False,
        relevance_threshold=70,
        max_context_comments=2,
    )
    assert relevance.completed == 2
    assert enrichment.completed == 4
    assert reputation.completed == 2

    output = tmp_path / "exports" / "gold.jsonl"
    assert export_database(database, output=output, format_name="jsonl", mineral="gold") == 4
    records = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
    assert {record["record_type"] for record in records} == {"post", "comment"}
    posts = [record for record in records if record["record_type"] == "post"]
    comments = [record for record in records if record["record_type"] == "comment"]
    assert all(
        set(record["analyses"]) == {"enrichment", "relevance", "reputation"} for record in posts
    )
    assert all(set(record["analyses"]) == {"enrichment"} for record in comments)

    status = database.status()
    assert status.posts == 2
    assert status.comments == 2
    assert status.analyses_by_kind_and_status == {
        "enrichment:complete": 4,
        "relevance:complete": 2,
        "reputation:complete": 2,
    }
