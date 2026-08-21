from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta

import pytest

from reddit_minerals.clients.gemini import PROMPT_VERSION
from reddit_minerals.clients.reddit import CommentBatch
from reddit_minerals.errors import (
    BatchOperationError,
    BatchProviderFailureError,
    ContentBlockedError,
    OperationDeadlineExceededError,
    PermanentProviderError,
    ProviderAuthenticationError,
    RetryableProviderError,
)
from reddit_minerals.models import (
    AnalysisKind,
    CommentRecord,
    ContentKind,
    PostRecord,
    WorkStatus,
)
from reddit_minerals.services.analysis import ANALYSIS_SCHEMA_VERSION, AnalysisService
from reddit_minerals.services.scrape import ScrapeService, _select_minerals
from reddit_minerals.storage import Database
from tests.fakes import FakeAnalysisClient, FakeRedditClient, result_for


def _scraper(client: FakeRedditClient, database: Database, *, retries: int = 2) -> ScrapeService:
    return ScrapeService(
        client=client,
        database=database,
        max_retries=retries,
        retry_base_delay_seconds=0,
        retry_max_delay_seconds=0,
    )


def _run_scrape(
    service: ScrapeService,
    *,
    mapping: dict[str, tuple[str, ...]],
    minerals: list[str] | None = None,
    dry_run: bool = False,
    force: bool = False,
    max_posts: int = 10,
) -> object:
    return service.run(
        mapping=mapping,
        minerals=minerals,
        max_posts_per_mineral=max_posts,
        max_comments_per_post=5,
        refresh_after=timedelta(days=1),
        time_filter="year",
        dry_run=dry_run,
        force=force,
    )


def _analyzer(
    client: FakeAnalysisClient, database: Database, *, retries: int = 2
) -> AnalysisService:
    return AnalysisService(
        client=client,
        database=database,
        max_retries=retries,
        retry_base_delay_seconds=0,
        retry_max_delay_seconds=0,
    )


def test_scrape_retries_deduplicates_and_persists_partial_failures(
    database: Database,
    make_post: Callable[..., PostRecord],
    make_comment: Callable[..., CommentRecord],
) -> None:
    client = FakeRedditClient()
    p1, p2, p3 = make_post("p1"), make_post("p2"), make_post("p3")
    client.queue_search(
        "gold",
        "mining",
        RetryableProviderError("transient search"),
        (p1, p2),
    )
    client.queue_search("gold", "geology", (p1, p3))
    client.queue_comments(
        "p1",
        RetryableProviderError("transient comments"),
        (make_comment("c1", post_id="p1"),),
    )
    client.queue_comments("p2", PermanentProviderError("deleted thread"))
    client.queue_comments("p3", ())

    summary = _run_scrape(_scraper(client, database), mapping={"gold": ("mining", "geology")})
    assert summary.model_dump() == {
        "minerals": ["gold"],
        "posts_discovered": 3,
        "posts_completed": 2,
        "posts_skipped_fresh": 0,
        "posts_skipped_terminal": 0,
        "posts_failed": 1,
        "comments_stored": 1,
        "posts_skipped_deleted": 0,
        "comments_skipped_deleted": 0,
        "comment_associations_removed": 0,
        "searches_failed": 0,
        "dry_run": False,
    }
    assert len([call for call in client.search_calls if call[1] == "mining"]) == 2
    assert len([call for call in client.comment_calls if call[0] == "p1"]) == 2
    assert len([call for call in client.comment_calls if call[0] == "p2"]) == 1
    assert database.status().posts == 3
    assert database.status().comments == 1
    with database._connection() as connection:
        states = dict(
            connection.execute(
                "SELECT post_id, scrape_status FROM post_minerals ORDER BY post_id"
            ).fetchall()
        )
    assert states == {
        "p1": WorkStatus.COMPLETE.value,
        "p2": WorkStatus.PERMANENT_FAILURE.value,
        "p3": WorkStatus.COMPLETE.value,
    }


def test_scrape_resume_skips_fresh_and_terminal_but_force_retries_all(
    database: Database, make_post: Callable[..., PostRecord]
) -> None:
    client = FakeRedditClient()
    p1, p2 = make_post("p1"), make_post("p2")
    client.queue_search("gold", "mining", (p1, p2))
    client.queue_comments("p1", ())
    client.queue_comments("p2", PermanentProviderError("forbidden"))
    service = _scraper(client, database)
    first = _run_scrape(service, mapping={"gold": ("mining",)})
    assert first.posts_completed == 1
    assert first.posts_failed == 1
    initial_comment_calls = len(client.comment_calls)

    resumed = _run_scrape(service, mapping={"gold": ("mining",)})
    assert resumed.posts_skipped_fresh == 1
    assert resumed.posts_skipped_terminal == 1
    assert len(client.comment_calls) == initial_comment_calls

    forced = _run_scrape(service, mapping={"gold": ("mining",)}, force=True)
    assert forced.posts_completed == 1
    assert forced.posts_failed == 1
    assert len(client.comment_calls) == initial_comment_calls + 2


def test_scrape_skips_tombstoned_posts_even_when_forced(
    database: Database, make_post: Callable[..., PostRecord]
) -> None:
    post = make_post("p1")
    database.store_scraped_post(post, [], mineral="gold")
    database.delete_content(content_kind=ContentKind.POST, content_id="p1", dry_run=False)
    client = FakeRedditClient()
    client.queue_search("gold", "mining", (post,))

    summary = _run_scrape(
        _scraper(client, database),
        mapping={"gold": ("mining",)},
        force=True,
    )
    assert summary.posts_discovered == 1
    assert summary.posts_skipped_deleted == 1
    assert summary.posts_completed == 0
    assert client.comment_calls == []
    assert database.status().posts == 0


def test_scrape_reports_tombstoned_comments_without_reinserting_them(
    database: Database,
    make_post: Callable[..., PostRecord],
    make_comment: Callable[..., CommentRecord],
) -> None:
    post = make_post("p1")
    comment = make_comment("c1", post_id="p1")
    database.store_scraped_post(post, [comment], mineral="gold")
    database.delete_content(content_kind=ContentKind.COMMENT, content_id="c1", dry_run=False)
    client = FakeRedditClient()
    client.queue_search("gold", "mining", (post,))
    client.queue_comments("p1", CommentBatch((comment,), snapshot_complete=True))

    summary = _run_scrape(_scraper(client, database), mapping={"gold": ("mining",)}, force=True)
    assert summary.posts_completed == 1
    assert summary.comments_stored == 0
    assert summary.comments_skipped_deleted == 1
    assert database.status().comments == 0


def test_scrape_complete_comment_batches_reconcile_removed_associations(
    database: Database,
    make_post: Callable[..., PostRecord],
    make_comment: Callable[..., CommentRecord],
) -> None:
    post = make_post("p1")
    c1 = make_comment("c1", post_id="p1")
    c2 = make_comment("c2", post_id="p1")
    client = FakeRedditClient()
    client.queue_search("gold", "mining", (post,), (post,))
    client.queue_comments(
        "p1",
        CommentBatch((c1, c2), snapshot_complete=True),
        CommentBatch((c1,), snapshot_complete=True),
    )
    service = _scraper(client, database)
    first = _run_scrape(service, mapping={"gold": ("mining",)})
    assert first.comments_stored == 2
    second = _run_scrape(service, mapping={"gold": ("mining",)}, force=True)
    assert second.comments_stored == 1
    assert second.comment_associations_removed == 1
    assert database.status().comments == 1


def test_scrape_operation_deadline_aborts_materialization(
    database: Database,
    make_post: Callable[..., PostRecord],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeRedditClient()
    client.queue_search("gold", "mining", (make_post("p1"),))
    times = iter((0.0, 0.0, 0.0, 2.0))
    monkeypatch.setattr("reddit_minerals.services.scrape.monotonic", lambda: next(times))
    service = ScrapeService(
        client=client,
        database=database,
        max_retries=1,
        retry_base_delay_seconds=0,
        retry_max_delay_seconds=0,
        operation_timeout_seconds=1,
    )
    with pytest.raises(OperationDeadlineExceededError) as raised:
        _run_scrape(service, mapping={"gold": ("mining",)})
    assert raised.value.summary["posts_discovered"] == 0
    assert database.status().posts == 0


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_retries": 0},
        {"max_retries": 11},
        {"retry_base_delay_seconds": -1},
        {"retry_max_delay_seconds": float("inf")},
        {"operation_timeout_seconds": 0},
        {"operation_timeout_seconds": float("nan")},
        {"operation_timeout_seconds": 86_401},
    ],
)
def test_scrape_service_rejects_invalid_runtime_configuration(
    database: Database, kwargs: dict[str, object]
) -> None:
    values: dict[str, object] = {
        "client": FakeRedditClient(),
        "database": database,
        "max_retries": 1,
        "retry_base_delay_seconds": 0,
        "retry_max_delay_seconds": 0,
    }
    values.update(kwargs)
    with pytest.raises(ValueError, match="must"):
        ScrapeService(**values)  # type: ignore[arg-type]


def test_scrape_classifies_exhausted_searches_and_continues_other_subreddits(
    database: Database, make_post: Callable[..., PostRecord]
) -> None:
    client = FakeRedditClient()
    client.queue_search("gold", "bad", RetryableProviderError("unavailable"))
    client.queue_search("gold", "forbidden", PermanentProviderError("forbidden"))
    client.queue_search("gold", "good", (make_post("p1"),))
    client.queue_comments("p1", ())
    summary = _run_scrape(
        _scraper(client, database),
        mapping={"gold": ("bad", "forbidden", "good")},
    )
    assert summary.searches_failed == 2
    assert summary.posts_discovered == 1
    assert summary.posts_completed == 1
    assert len([call for call in client.search_calls if call[1] == "bad"]) == 2
    assert len([call for call in client.search_calls if call[1] == "forbidden"]) == 1


def test_scrape_raises_batch_error_when_every_provider_operation_fails(
    database: Database,
) -> None:
    client = FakeRedditClient()
    client.queue_search("gold", "mining", RetryableProviderError("unavailable"))
    with pytest.raises(BatchProviderFailureError) as raised:
        _run_scrape(_scraper(client, database, retries=1), mapping={"gold": ("mining",)})
    assert raised.value.summary["searches_failed"] == 1
    assert raised.value.summary["posts_completed"] == 0
    assert database.status().posts == 0


@pytest.mark.parametrize("stage", ["search", "comments"])
def test_scrape_provider_wide_failure_aborts_without_persisting_partial_item_state(
    database: Database,
    make_post: Callable[..., PostRecord],
    stage: str,
) -> None:
    client = FakeRedditClient()
    post = make_post("p1")
    if stage == "search":
        client.queue_search("gold", "mining", ProviderAuthenticationError("credentials rejected"))
    else:
        client.queue_search("gold", "mining", (post,))
        client.queue_comments("p1", ProviderAuthenticationError("credentials rejected"))
    with pytest.raises(ProviderAuthenticationError):
        _run_scrape(_scraper(client, database), mapping={"gold": ("mining",)})
    assert database.status().posts == 0


def test_scrape_dry_run_does_not_fetch_comments_or_write_database(
    database: Database, make_post: Callable[..., PostRecord]
) -> None:
    client = FakeRedditClient()
    client.queue_search("gold", "mining", (make_post("p1"),))
    summary = _run_scrape(_scraper(client, database), mapping={"gold": ("mining",)}, dry_run=True)
    assert summary.posts_discovered == 1
    assert summary.posts_completed == 0
    assert summary.dry_run is True
    assert client.comment_calls == []
    assert database.status().posts == 0


def test_scrape_respects_mineral_selection_and_per_mineral_limit(
    database: Database, make_post: Callable[..., PostRecord]
) -> None:
    client = FakeRedditClient()
    client.queue_search("gold", "mining", (make_post("g1"),))
    client.queue_search("silver", "silverbugs", (make_post("s1"),))
    client.queue_comments("g1", ())
    summary = _run_scrape(
        _scraper(client, database),
        mapping={"silver": ("silverbugs",), "gold": ("mining",)},
        minerals=[" GOLD ", "gold"],
        max_posts=1,
    )
    assert summary.minerals == ["gold"]
    assert all(call[0] == "gold" and call[2] == 1 for call in client.search_calls)
    assert all(deadline is not None for deadline in client.search_deadlines)
    assert all(deadline is not None for deadline in client.comment_deadlines)
    assert _select_minerals({"silver": (), "gold": ()}, None) == ["gold", "silver"]
    with pytest.raises(ValueError, match="Unknown mineral"):
        _select_minerals({"gold": ()}, ["copper"])


def test_analysis_service_summarizes_complete_retryable_permanent_and_blocked(
    database: Database, make_post: Callable[..., PostRecord]
) -> None:
    for index in range(1, 6):
        database.store_scraped_post(
            make_post(f"p{index}", created_offset=index), [], mineral="gold"
        )
    client = FakeAnalysisClient()
    client.queue(AnalysisKind.RELEVANCE, "p1", result_for(AnalysisKind.RELEVANCE))
    client.queue(
        AnalysisKind.RELEVANCE,
        "p2",
        RetryableProviderError("transient"),
        result_for(AnalysisKind.RELEVANCE, confidence=82),
    )
    client.queue(AnalysisKind.RELEVANCE, "p3", RetryableProviderError("secret provider text"))
    client.queue(AnalysisKind.RELEVANCE, "p4", PermanentProviderError("secret provider text"))
    client.queue(AnalysisKind.RELEVANCE, "p5", ContentBlockedError("secret provider text"))
    summary = _analyzer(client, database).run(
        AnalysisKind.RELEVANCE,
        mineral="gold",
        limit=10,
        force=False,
        relevance_threshold=70,
        max_context_comments=5,
    )
    assert summary.model_dump(mode="json") == {
        "kind": "relevance",
        "selected": 5,
        "completed": 2,
        "retryable_failures": 1,
        "permanent_failures": 1,
        "blocked": 1,
        "stale_discarded": 0,
    }
    assert client.calls.count((AnalysisKind.RELEVANCE, "p2")) == 2
    assert client.calls.count((AnalysisKind.RELEVANCE, "p3")) == 2
    assert client.calls.count((AnalysisKind.RELEVANCE, "p4")) == 1
    assert client.calls.count((AnalysisKind.RELEVANCE, "p5")) == 1
    assert database.status().analyses_by_kind_and_status == {
        "relevance:blocked": 1,
        "relevance:complete": 2,
        "relevance:permanent_failure": 1,
        "relevance:retryable_failure": 1,
    }
    remaining = database.analysis_candidates(
        AnalysisKind.RELEVANCE,
        mineral="gold",
        limit=10,
        force=False,
        relevance_threshold=70,
        max_context_comments=5,
        schema_version=ANALYSIS_SCHEMA_VERSION,
        prompt_version=PROMPT_VERSION,
        model=client.model,
    )
    assert [item.content_id for item in remaining] == ["p3"]
    exported = list(database.export_records("gold"))
    errors = [
        value["error"]
        for record in exported
        for value in record["analyses"].values()
        if value["error"]
    ]
    assert "secret provider text" not in " ".join(errors)


@pytest.mark.parametrize("kind", list(AnalysisKind))
def test_analysis_service_routes_each_kind_to_the_matching_fake_method(
    database: Database,
    make_post: Callable[..., PostRecord],
    kind: AnalysisKind,
) -> None:
    database.store_scraped_post(make_post(f"post-{kind}"), [], mineral="gold")
    client = FakeAnalysisClient()
    if kind is AnalysisKind.REPUTATION:
        relevance = database.analysis_candidates(
            AnalysisKind.RELEVANCE,
            mineral="gold",
            limit=1,
            force=False,
            relevance_threshold=70,
            max_context_comments=0,
            schema_version=ANALYSIS_SCHEMA_VERSION,
            prompt_version=PROMPT_VERSION,
            model=client.model,
        )[0]
        database.save_analysis(
            kind=AnalysisKind.RELEVANCE,
            content=relevance,
            result=result_for(AnalysisKind.RELEVANCE, confidence=90),
            schema_version=ANALYSIS_SCHEMA_VERSION,
            prompt_version=PROMPT_VERSION,
        )
    summary = _analyzer(client, database, retries=1).run(
        kind,
        mineral="gold",
        limit=1,
        force=False,
        relevance_threshold=70,
        max_context_comments=0,
    )
    assert summary.completed == 1
    assert client.calls == [(kind, f"post-{kind}")]


def test_analysis_service_propagates_unexpected_programming_errors(
    database: Database, make_post: Callable[..., PostRecord]
) -> None:
    database.store_scraped_post(make_post("p1"), [], mineral="gold")
    client = FakeAnalysisClient()
    client.queue(AnalysisKind.RELEVANCE, "p1", RuntimeError("bug"))
    with pytest.raises(RuntimeError, match="bug"):
        _analyzer(client, database).run(
            AnalysisKind.RELEVANCE,
            mineral=None,
            limit=1,
            force=False,
            relevance_threshold=70,
            max_context_comments=0,
        )
    assert database.status().analyses_by_kind_and_status == {}


def test_analysis_retryable_failure_can_be_reprocessed_on_next_run(
    database: Database, make_post: Callable[..., PostRecord]
) -> None:
    database.store_scraped_post(make_post("p1"), [], mineral="gold")
    client = FakeAnalysisClient()
    client.queue(AnalysisKind.RELEVANCE, "p1", RetryableProviderError("temporary"))
    service = _analyzer(client, database, retries=1)
    with pytest.raises(BatchProviderFailureError) as raised:
        service.run(
            AnalysisKind.RELEVANCE,
            mineral="gold",
            limit=1,
            force=False,
            relevance_threshold=70,
            max_context_comments=0,
        )
    assert raised.value.summary["retryable_failures"] == 1
    assert raised.value.summary["completed"] == 0
    client.queue(AnalysisKind.RELEVANCE, "p1", result_for(AnalysisKind.RELEVANCE))
    second = service.run(
        AnalysisKind.RELEVANCE,
        mineral="gold",
        limit=1,
        force=False,
        relevance_threshold=70,
        max_context_comments=0,
    )
    assert second.completed == 1
    assert database.status().analyses_by_kind_and_status == {"relevance:complete": 1}


def test_analysis_discards_provider_result_that_returns_after_deadline(
    database: Database,
    make_post: Callable[..., PostRecord],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database.store_scraped_post(make_post("p1"), [], mineral="gold")
    client = FakeAnalysisClient()
    times = iter((0.0, 0.0, 2.0))
    monkeypatch.setattr("reddit_minerals.services.analysis.time.monotonic", lambda: next(times))
    service = AnalysisService(
        client=client,
        database=database,
        max_retries=1,
        retry_base_delay_seconds=0,
        retry_max_delay_seconds=0,
        operation_timeout_seconds=1,
    )

    with pytest.raises(OperationDeadlineExceededError, match="late result was discarded"):
        service.run(
            AnalysisKind.RELEVANCE,
            mineral="gold",
            limit=1,
            force=False,
            relevance_threshold=70,
            max_context_comments=0,
        )

    assert client.calls == [(AnalysisKind.RELEVANCE, "p1")]
    assert database.status().analyses_by_kind_and_status == {}


def test_analysis_discards_success_when_source_changes_during_provider_call(
    database: Database,
    make_post: Callable[..., PostRecord],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database.store_scraped_post(make_post("p1"), [], mineral="gold")
    client = FakeAnalysisClient()

    def mutate_then_succeed(content: object) -> object:
        client.calls.append((AnalysisKind.RELEVANCE, "p1"))
        database.store_scraped_post(
            make_post("p1", title="Changed while analysis was in flight"),
            [],
            mineral="gold",
        )
        return result_for(AnalysisKind.RELEVANCE)

    monkeypatch.setattr(client, "analyze_relevance", mutate_then_succeed)
    with pytest.raises(BatchOperationError, match="source inputs changed") as raised:
        _analyzer(client, database, retries=1).run(
            AnalysisKind.RELEVANCE,
            mineral="gold",
            limit=1,
            force=False,
            relevance_threshold=70,
            max_context_comments=0,
        )

    assert raised.value.summary["completed"] == 0
    assert raised.value.summary["stale_discarded"] == 1
    assert database.status().analyses_by_kind_and_status == {}


def test_analysis_discards_failure_when_source_changes_during_provider_call(
    database: Database,
    make_post: Callable[..., PostRecord],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database.store_scraped_post(make_post("p1"), [], mineral="gold")
    client = FakeAnalysisClient()

    def mutate_then_fail(content: object) -> object:
        client.calls.append((AnalysisKind.RELEVANCE, "p1"))
        database.store_scraped_post(
            make_post("p1", title="Changed before failure persistence"),
            [],
            mineral="gold",
        )
        raise PermanentProviderError("provider rejected the old input")

    monkeypatch.setattr(client, "analyze_relevance", mutate_then_fail)
    with pytest.raises(BatchOperationError, match="source inputs changed") as raised:
        _analyzer(client, database, retries=1).run(
            AnalysisKind.RELEVANCE,
            mineral="gold",
            limit=1,
            force=False,
            relevance_threshold=70,
            max_context_comments=0,
        )

    assert raised.value.summary["permanent_failures"] == 0
    assert raised.value.summary["stale_discarded"] == 1
    assert database.status().analyses_by_kind_and_status == {}


@pytest.mark.parametrize(
    "error",
    [PermanentProviderError("permanent"), ContentBlockedError("blocked")],
)
def test_analysis_all_item_failures_raise_batch_error_with_persisted_summary(
    database: Database,
    make_post: Callable[..., PostRecord],
    error: Exception,
) -> None:
    database.store_scraped_post(make_post("p1"), [], mineral="gold")
    client = FakeAnalysisClient()
    client.queue(AnalysisKind.RELEVANCE, "p1", error)
    with pytest.raises(BatchProviderFailureError) as raised:
        _analyzer(client, database, retries=1).run(
            AnalysisKind.RELEVANCE,
            mineral="gold",
            limit=1,
            force=False,
            relevance_threshold=70,
            max_context_comments=0,
        )
    key = "blocked" if isinstance(error, ContentBlockedError) else "permanent_failures"
    assert raised.value.summary[key] == 1
    assert sum(database.status().analyses_by_kind_and_status.values()) == 1


def test_analysis_provider_wide_failure_aborts_without_poisoning_item_state(
    database: Database, make_post: Callable[..., PostRecord]
) -> None:
    database.store_scraped_post(make_post("p1"), [], mineral="gold")
    client = FakeAnalysisClient()
    client.queue(
        AnalysisKind.RELEVANCE,
        "p1",
        ProviderAuthenticationError("credentials rejected"),
    )
    with pytest.raises(ProviderAuthenticationError):
        _analyzer(client, database).run(
            AnalysisKind.RELEVANCE,
            mineral="gold",
            limit=1,
            force=False,
            relevance_threshold=70,
            max_context_comments=0,
        )
    assert database.status().analyses_by_kind_and_status == {}
    assert (
        database.analysis_candidates(
            AnalysisKind.RELEVANCE,
            mineral="gold",
            limit=1,
            force=False,
            relevance_threshold=70,
            max_context_comments=0,
            schema_version=ANALYSIS_SCHEMA_VERSION,
            prompt_version=PROMPT_VERSION,
            model=client.model,
        )[0].content_id
        == "p1"
    )


def test_analysis_deadline_reports_partial_summary_without_marking_unstarted_items(
    database: Database,
    make_post: Callable[..., PostRecord],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database.store_scraped_post(make_post("p1", created_offset=1), [], mineral="gold")
    database.store_scraped_post(make_post("p2", created_offset=2), [], mineral="gold")
    client = FakeAnalysisClient()
    times = iter((0.0, 0.0, 0.0, 2.0))
    monkeypatch.setattr("reddit_minerals.services.analysis.time.monotonic", lambda: next(times))
    service = AnalysisService(
        client=client,
        database=database,
        max_retries=1,
        retry_base_delay_seconds=0,
        retry_max_delay_seconds=0,
        operation_timeout_seconds=1,
    )
    with pytest.raises(OperationDeadlineExceededError) as raised:
        service.run(
            AnalysisKind.RELEVANCE,
            mineral="gold",
            limit=2,
            force=False,
            relevance_threshold=70,
            max_context_comments=0,
        )
    assert raised.value.summary["selected"] == 2
    assert raised.value.summary["completed"] == 1
    assert len(client.calls) == 1
    assert database.status().analyses_by_kind_and_status == {"relevance:complete": 1}


def test_analysis_deadline_bounds_retry_backoff(
    database: Database,
    make_post: Callable[..., PostRecord],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database.store_scraped_post(make_post("p1"), [], mineral="gold")
    client = FakeAnalysisClient()
    client.queue(AnalysisKind.RELEVANCE, "p1", RetryableProviderError("temporary"))
    times = iter((0.0, 0.0, 0.0, 1.0))
    sleeps: list[float] = []
    monkeypatch.setattr("reddit_minerals.services.analysis.time.monotonic", lambda: next(times))
    monkeypatch.setattr("reddit_minerals.services.analysis.time.sleep", sleeps.append)
    service = AnalysisService(
        client=client,
        database=database,
        max_retries=2,
        retry_base_delay_seconds=10,
        retry_max_delay_seconds=10,
        operation_timeout_seconds=1,
    )
    with pytest.raises(OperationDeadlineExceededError):
        service.run(
            AnalysisKind.RELEVANCE,
            mineral="gold",
            limit=1,
            force=False,
            relevance_threshold=70,
            max_context_comments=0,
        )
    assert sleeps == [1.0]
    assert client.calls == [(AnalysisKind.RELEVANCE, "p1")]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_retries": 0},
        {"max_retries": 11},
        {"retry_base_delay_seconds": -1},
        {"retry_max_delay_seconds": float("inf")},
        {"operation_timeout_seconds": 0},
        {"model": " "},
        {"max_content_chars": 499},
        {"max_content_chars": 100_001},
    ],
)
def test_analysis_service_rejects_invalid_runtime_configuration(
    database: Database, kwargs: dict[str, object]
) -> None:
    values: dict[str, object] = {
        "client": FakeAnalysisClient(),
        "database": database,
        "max_retries": 1,
        "retry_base_delay_seconds": 0,
        "retry_max_delay_seconds": 0,
    }
    values.update(kwargs)
    with pytest.raises(ValueError, match="must"):
        AnalysisService(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "arguments",
    [
        {"limit": 0, "relevance_threshold": 70, "max_context_comments": 0},
        {"limit": 10_001, "relevance_threshold": 70, "max_context_comments": 0},
        {"limit": 1, "relevance_threshold": float("nan"), "max_context_comments": 0},
        {"limit": 1, "relevance_threshold": 70, "max_context_comments": 21},
    ],
)
def test_analysis_service_validates_batch_arguments_before_database_access(
    database: Database, arguments: dict[str, object]
) -> None:
    with pytest.raises(ValueError, match="must"):
        _analyzer(FakeAnalysisClient(), database).run(  # type: ignore[arg-type]
            AnalysisKind.RELEVANCE,
            mineral=None,
            force=False,
            **arguments,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    ("kind", "mineral"),
    [("relevance", None), (AnalysisKind.RELEVANCE, " ")],
)
def test_analysis_service_rejects_invalid_kind_or_blank_mineral(
    database: Database, kind: object, mineral: str | None
) -> None:
    with pytest.raises(ValueError, match=r"kind|mineral"):
        _analyzer(FakeAnalysisClient(), database).run(  # type: ignore[arg-type]
            kind,
            mineral=mineral,
            limit=1,
            force=False,
            relevance_threshold=70,
            max_context_comments=0,
        )


def test_analysis_service_uses_client_model_for_candidate_provenance(
    database: Database, make_post: Callable[..., PostRecord]
) -> None:
    database.store_scraped_post(make_post("p1"), [], mineral="gold")
    content = database.analysis_candidates(
        AnalysisKind.RELEVANCE,
        mineral="gold",
        limit=1,
        force=False,
        relevance_threshold=70,
        max_context_comments=0,
        schema_version=ANALYSIS_SCHEMA_VERSION,
        prompt_version=PROMPT_VERSION,
        model="model-a",
    )[0]
    database.save_analysis(
        kind=AnalysisKind.RELEVANCE,
        content=content,
        result=result_for(AnalysisKind.RELEVANCE, model="model-a"),
        schema_version=ANALYSIS_SCHEMA_VERSION,
        prompt_version=PROMPT_VERSION,
    )

    client = FakeAnalysisClient()
    client.model = "model-b"  # type: ignore[attr-defined]
    client.queue(
        AnalysisKind.RELEVANCE,
        "p1",
        result_for(AnalysisKind.RELEVANCE, model="model-b"),
    )
    service = _analyzer(client, database, retries=1)
    assert (
        service.run(
            AnalysisKind.RELEVANCE,
            mineral="gold",
            limit=1,
            force=False,
            relevance_threshold=70,
            max_context_comments=0,
        ).completed
        == 1
    )
    assert (
        service.run(
            AnalysisKind.RELEVANCE,
            mineral="gold",
            limit=1,
            force=False,
            relevance_threshold=70,
            max_context_comments=0,
        ).selected
        == 0
    )
