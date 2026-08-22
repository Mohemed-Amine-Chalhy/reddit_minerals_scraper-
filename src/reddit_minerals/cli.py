"""Import-safe command-line interface."""

from __future__ import annotations

import argparse
import json
import logging
import signal
import tempfile
from collections.abc import Callable, Mapping, Sequence
from datetime import timedelta
from pathlib import Path
from types import FrameType
from typing import Any

from pydantic import ValidationError

from reddit_minerals import __version__
from reddit_minerals.clients.gemini import PROMPT_VERSION, GeminiAnalysisClient
from reddit_minerals.clients.reddit import PrawRedditClient
from reddit_minerals.config import (
    MAX_PROVIDER_ITEMS,
    MAX_REFRESH_HOURS,
    AppSettings,
    load_subreddit_mapping,
)
from reddit_minerals.demo import DemoArtifactLifecycle, DemoSummary, run_offline_demo
from reddit_minerals.errors import (
    BatchOperationError,
    ConfigurationError,
    RedditMineralsError,
)
from reddit_minerals.export import export_database
from reddit_minerals.migration import migrate_legacy_data
from reddit_minerals.models import AnalysisKind, ContentKind, StrictModel
from reddit_minerals.observability import configure_logging
from reddit_minerals.services import AnalysisService, ScrapeService
from reddit_minerals.services.analysis import ANALYSIS_SCHEMA_VERSION, AnalysisSummary
from reddit_minerals.services.scrape import ScrapeSummary
from reddit_minerals.storage import Database

logger = logging.getLogger(__name__)


class _TerminationRequested(BaseException):
    """Internal control flow raised by the runtime SIGTERM handler."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="reddit-minerals",
        description="Bounded Reddit mineral collection and schema-validated analysis.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument(
        "--database-path",
        type=_path_argument,
        help="Override RMS_DATABASE_PATH for this invocation.",
    )
    parser.add_argument(
        "--mapping-path",
        type=_path_argument,
        help="Override RMS_SUBREDDIT_MAPPING_PATH for this invocation.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    demo = subparsers.add_parser(
        "demo",
        help="Run the complete pipeline offline with deterministic synthetic data.",
        description=(
            "Run collection, schema-validated analysis, SQLite storage, and JSONL export "
            "offline with deterministic synthetic data."
        ),
    )
    demo.add_argument(
        "--output-dir",
        type=_path_argument,
        help=(
            "Retain artifacts in a unique child of this directory; by default an "
            "isolated temporary workspace is removed after the command."
        ),
    )

    validate = subparsers.add_parser(
        "validate-config", help="Validate settings and the subreddit mapping offline."
    )
    validate.add_argument(
        "--require",
        action="append",
        choices=("reddit", "ai"),
        default=[],
        help="Also require credentials for this provider (repeatable).",
    )

    scrape = subparsers.add_parser("scrape", help="Collect bounded Reddit posts/comments.")
    scrape.add_argument(
        "--mineral",
        action="append",
        type=_nonblank_text,
        help="Mineral to collect (repeatable).",
    )
    scrape.add_argument("--max-posts", type=_bounded_provider_limit)
    scrape.add_argument("--max-comments", type=_bounded_nonnegative_provider_limit)
    scrape.add_argument("--refresh-after-hours", type=_bounded_refresh_hours)
    scrape.add_argument(
        "--time-filter",
        choices=("hour", "day", "week", "month", "year", "all"),
        default="year",
    )
    scrape.add_argument("--dry-run", action="store_true")
    scrape.add_argument(
        "--force",
        action="store_true",
        help="Refresh completed posts and retry permanent failures.",
    )

    for command, help_text in (
        ("relevance", "Classify whether collected posts are mineral-related."),
        ("enrich", "Extract typed topics, sentiment, and concern signals."),
        ("reputation", "Estimate perception signals for relevant posts."),
    ):
        analysis = subparsers.add_parser(command, help=help_text)
        analysis.add_argument(
            "--mineral", type=_nonblank_text, help="Restrict the batch to one mineral."
        )
        analysis.add_argument("--limit", type=_bounded_provider_limit)
        analysis.add_argument("--force", action="store_true", help="Reanalyze completed records.")

    status = subparsers.add_parser("status", help="Show persisted pipeline/run state.")
    status.add_argument("--json", action="store_true", help="Emit compact JSON.")

    export = subparsers.add_parser("export", help="Atomically export canonical data.")
    export.add_argument("--output", type=_path_argument, required=True)
    export.add_argument("--format", choices=("jsonl", "json"), default="jsonl")
    export.add_argument("--mineral", type=_nonblank_text, help="Restrict export to one mineral.")
    export.add_argument(
        "--overwrite",
        action="store_true",
        help="Atomically replace an existing output file (never the live database).",
    )

    delete = subparsers.add_parser(
        "delete-content", help="Delete content and all locally-derived analyses."
    )
    target = delete.add_mutually_exclusive_group(required=True)
    target.add_argument("--post-id", type=_nonblank_text)
    target.add_argument("--comment-id", type=_nonblank_text)
    delete.add_argument("--dry-run", action="store_true")
    delete.add_argument("--yes", action="store_true", help="Confirm a non-dry-run deletion.")

    migrate = subparsers.add_parser(
        "migrate-legacy", help="Validate/import legacy per-mineral JSON data."
    )
    migrate.add_argument("--source", type=_path_argument, default=Path("data"))
    migrate.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Importing this module has no side effects."""

    previous_handler: Any = None
    handler_installed = False
    try:
        previous_handler = signal.signal(signal.SIGTERM, _request_termination)
        handler_installed = True
    except (OSError, ValueError):
        # Signal handlers can only be installed by the main interpreter thread.
        pass
    try:
        return _run(argv)
    except _TerminationRequested:
        logger.warning("termination requested")
        return 143
    finally:
        if handler_installed:
            signal.signal(signal.SIGTERM, previous_handler)


def _run(argv: list[str] | None) -> int:
    """Parse and execute one CLI invocation under the runtime signal boundary."""

    parser = build_parser()
    args = parser.parse_args(argv)
    # Settings validation can fail before its configured log level is available.
    # Establish the safe formatter first so even those failures remain structured.
    configure_logging("INFO")
    try:
        overrides: dict[str, Any] = {}
        if args.database_path is not None:
            overrides["database_path"] = args.database_path
        if args.mapping_path is not None:
            overrides["subreddit_mapping_path"] = args.mapping_path
        settings = AppSettings(**overrides)
        configure_logging(settings.log_level)
        return _dispatch(args, settings)
    except ValidationError as exc:
        logger.error(
            "configuration validation error",
            extra={
                "error_type": type(exc).__name__,
                "validation_errors": [
                    {
                        "location": ".".join(str(part) for part in error["loc"]),
                        "type": error["type"],
                        "message": error["msg"],
                    }
                    for error in exc.errors(include_url=False, include_input=False)
                ],
            },
        )
        return 2
    except (ConfigurationError, ValueError) as exc:
        # These exceptions are created by this package with controlled, non-secret text.
        logger.error(
            "configuration or input error",
            extra={"error_type": type(exc).__name__, "detail": str(exc)[:500]},
        )
        return 2
    except KeyboardInterrupt:
        logger.warning("operation interrupted")
        return 130
    except RedditMineralsError as exc:
        logger.error(
            "operation failed",
            extra={"error_type": type(exc).__name__, "detail": str(exc)},
        )
        return 1
    except Exception as exc:  # unexpected bugs receive a non-zero exit and traceback
        logger.exception("unexpected operation failure", extra={"error_type": type(exc).__name__})
        return 1


def _dispatch(args: argparse.Namespace, settings: AppSettings) -> int:
    if args.command == "demo":
        # Successful service operations log per item at INFO. Keep the portfolio
        # demo focused on its single machine-readable summary while preserving
        # warning and error visibility.
        configure_logging("WARNING")
        demo_summary = _execute_demo(args.output_dir, settings=settings)
        _print_json(demo_summary.model_dump(mode="json"), pretty=True)
        return 0

    if args.command == "validate-config":
        mapping_report = load_subreddit_mapping(settings.subreddit_mapping_path)
        if "reddit" in args.require:
            settings.require_reddit()
        if "ai" in args.require:
            settings.require_gemini()
        _print_json(
            {
                "valid": True,
                "settings": settings.safe_summary(),
                "mapping": mapping_report.model_dump(mode="json", exclude={"mapping"}),
                "minerals": sorted(mapping_report.mapping),
            },
            pretty=True,
        )
        return 0

    database = Database(settings.database_path)
    database.initialize()

    if args.command == "scrape":
        mapping_report = load_subreddit_mapping(settings.subreddit_mapping_path)
        max_posts = args.max_posts if args.max_posts is not None else settings.max_posts_per_mineral
        max_comments = (
            args.max_comments if args.max_comments is not None else settings.max_comments_per_post
        )
        refresh_after_hours = (
            args.refresh_after_hours
            if args.refresh_after_hours is not None
            else settings.refresh_after_hours
        )
        effective_minerals = _effective_minerals(args.mineral, mapping_report.mapping)
        scrape_summary = _tracked(
            database,
            "scrape",
            lambda: _execute_scrape(
                database=database,
                settings=settings,
                mapping=mapping_report.mapping,
                minerals=effective_minerals,
                max_posts_per_mineral=max_posts,
                max_comments_per_post=max_comments,
                refresh_after_hours=refresh_after_hours,
                time_filter=args.time_filter,
                dry_run=args.dry_run,
                force=args.force,
            ),
            parameters={
                "minerals": effective_minerals,
                "all_configured_minerals": args.mineral is None,
                "max_posts_per_mineral": max_posts,
                "max_comments_per_post": max_comments,
                "refresh_after_hours": refresh_after_hours,
                "time_filter": args.time_filter,
                "dry_run": args.dry_run,
                "force": args.force,
                "mapping_path": str(settings.subreddit_mapping_path),
                "request_timeout_seconds": settings.reddit_request_timeout_seconds,
                "operation_timeout_seconds": settings.operation_timeout_seconds,
            },
        )
        _print_json(scrape_summary.model_dump(mode="json"), pretty=True)
        return 0

    analysis_kind_by_command = {
        "relevance": AnalysisKind.RELEVANCE,
        "enrich": AnalysisKind.ENRICHMENT,
        "reputation": AnalysisKind.REPUTATION,
    }
    if args.command in analysis_kind_by_command:
        kind = analysis_kind_by_command[args.command]
        mineral = _normalize_mineral(args.mineral)
        batch_limit = args.limit if args.limit is not None else settings.analysis_batch_size
        configured_model = settings.gemini_model.strip() if settings.gemini_model else None
        analysis_summary = _tracked(
            database,
            args.command,
            lambda: _execute_analysis(
                database=database,
                settings=settings,
                kind=kind,
                mineral=mineral,
                limit=batch_limit,
                force=args.force,
            ),
            parameters={
                "mineral": mineral,
                "limit": batch_limit,
                "force": args.force,
                "relevance_threshold": settings.relevance_threshold,
                "max_context_comments": settings.max_context_comments,
                "schema_version": ANALYSIS_SCHEMA_VERSION,
                "prompt_version": PROMPT_VERSION,
                "model": configured_model,
                "request_timeout_seconds": settings.gemini_request_timeout_seconds,
                "operation_timeout_seconds": settings.operation_timeout_seconds,
            },
        )
        _print_json(analysis_summary.model_dump(mode="json"), pretty=True)
        return 0

    if args.command == "status":
        snapshot = database.status()
        _print_json(snapshot.model_dump(mode="json"), pretty=not args.json)
        return 0

    if args.command == "export":
        mineral = _normalize_mineral(args.mineral)
        count = _tracked(
            database,
            "export",
            lambda: export_database(
                database,
                output=args.output,
                format_name=args.format,
                mineral=mineral,
                overwrite=args.overwrite,
            ),
            parameters={
                "output": str(args.output),
                "format": args.format,
                "mineral": mineral,
                "overwrite": args.overwrite,
            },
        )
        _print_json(
            {"output": str(args.output), "format": args.format, "records": count},
            pretty=True,
        )
        return 0

    if args.command == "delete-content":
        if not args.dry_run and not args.yes:
            raise ConfigurationError("Refusing deletion without --yes; use --dry-run to preview")
        content_kind = ContentKind.POST if args.post_id else ContentKind.COMMENT
        content_id = str(args.post_id or args.comment_id)
        deletion_report = _tracked(
            database,
            "delete-content",
            lambda: database.delete_content(
                content_kind=content_kind,
                content_id=content_id,
                dry_run=args.dry_run,
            ),
            parameters={"content_kind": content_kind.value, "dry_run": args.dry_run},
        )
        _print_json(deletion_report, pretty=True)
        return 0

    if args.command == "migrate-legacy":
        migration_report = _tracked(
            database,
            "migrate-legacy",
            lambda: migrate_legacy_data(database, source=args.source, dry_run=args.dry_run),
            parameters={"source": str(args.source), "dry_run": args.dry_run},
        )
        _print_json(migration_report.model_dump(mode="json"), pretty=True)
        return 0

    raise AssertionError(f"Unhandled command: {args.command}")


def _execute_demo(output_dir: Path | None, *, settings: AppSettings) -> DemoSummary:
    """Create an isolated workspace and run the credential-free demo inside it."""

    if output_dir is None:
        with tempfile.TemporaryDirectory(prefix="reddit-minerals-demo-") as temporary:
            return run_offline_demo(
                Path(temporary),
                lifecycle=DemoArtifactLifecycle.REMOVED_AFTER_COMMAND,
                protected_database_path=settings.database_path,
            )

    output_root = output_dir.resolve()
    if output_root == settings.database_path.resolve():
        raise ConfigurationError(
            "Demo output directory must not be the configured application database path"
        )
    if output_root.exists() and not output_root.is_dir():
        raise ValueError(f"Demo output directory is not a directory: {output_root}")
    output_root.mkdir(parents=True, exist_ok=True)
    workspace = Path(tempfile.mkdtemp(prefix="reddit-minerals-demo-", dir=output_root))
    return run_offline_demo(
        workspace,
        lifecycle=DemoArtifactLifecycle.RETAINED,
        protected_database_path=settings.database_path,
    )


def _execute_scrape(
    *,
    database: Database,
    settings: AppSettings,
    mapping: Mapping[str, Sequence[str]],
    minerals: Sequence[str],
    max_posts_per_mineral: int,
    max_comments_per_post: int,
    refresh_after_hours: int,
    time_filter: str,
    dry_run: bool,
    force: bool,
) -> ScrapeSummary:
    """Construct the Reddit boundary inside run tracking, then execute it."""

    client_id, client_secret, user_agent = settings.require_reddit()
    reddit_client = PrawRedditClient(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent,
        replace_more_limit=settings.reddit_replace_more_limit,
        request_timeout_seconds=settings.reddit_request_timeout_seconds,
    )
    service = ScrapeService(
        client=reddit_client,
        database=database,
        max_retries=settings.max_retries,
        retry_base_delay_seconds=settings.retry_base_delay_seconds,
        retry_max_delay_seconds=settings.retry_max_delay_seconds,
        operation_timeout_seconds=settings.operation_timeout_seconds,
    )
    return service.run(
        mapping=mapping,
        minerals=minerals,
        max_posts_per_mineral=max_posts_per_mineral,
        max_comments_per_post=max_comments_per_post,
        refresh_after=timedelta(hours=refresh_after_hours),
        time_filter=time_filter,
        dry_run=dry_run,
        force=force,
    )


def _execute_analysis(
    *,
    database: Database,
    settings: AppSettings,
    kind: AnalysisKind,
    mineral: str | None,
    limit: int,
    force: bool,
) -> AnalysisSummary:
    """Construct the AI boundary inside run tracking, then execute it."""

    api_key, model = settings.require_gemini()
    model = model.strip()
    analysis_client = GeminiAnalysisClient(
        api_key=api_key,
        model=model,
        max_content_chars=settings.max_content_chars,
        request_timeout_seconds=settings.gemini_request_timeout_seconds,
    )
    service = AnalysisService(
        client=analysis_client,
        database=database,
        max_retries=settings.max_retries,
        retry_base_delay_seconds=settings.retry_base_delay_seconds,
        retry_max_delay_seconds=settings.retry_max_delay_seconds,
        operation_timeout_seconds=settings.operation_timeout_seconds,
        model=model,
        max_content_chars=settings.max_content_chars,
    )
    return service.run(
        kind,
        mineral=mineral,
        limit=limit,
        force=force,
        relevance_threshold=settings.relevance_threshold,
        max_context_comments=settings.max_context_comments,
    )


def _tracked[ResultT](
    database: Database,
    command: str,
    operation: Callable[[], ResultT],
    *,
    parameters: Mapping[str, Any] | None = None,
) -> ResultT:
    with database.operation_lock():
        reconciled_runs = database.reconcile_stale_runs()
        if reconciled_runs:
            logger.warning(
                "reconciled interrupted run records",
                extra={"reconciled_runs": reconciled_runs},
            )
        run_id = database.start_run(command, parameters=parameters)
        try:
            result = operation()
        except BaseException as exc:
            summary = exc.summary if isinstance(exc, BatchOperationError) else {}
            try:
                database.finish_run(
                    run_id,
                    success=False,
                    summary=summary,
                    error_type=type(exc).__name__,
                )
            except Exception as finalization_error:
                logger.exception(
                    "failed to finalize unsuccessful run record",
                    extra={
                        "command": command,
                        "error_type": type(finalization_error).__name__,
                    },
                )
            raise
        summary = _jsonable(result)
        database.finish_run(run_id, success=True, summary=summary)
        return result


def _jsonable(value: Any) -> dict[str, Any]:
    if isinstance(value, StrictModel):
        return value.model_dump(mode="json")
    if isinstance(value, dict):
        return value
    return {"result": str(value)}


def _print_json(value: Any, *, pretty: bool) -> None:
    print(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            indent=2 if pretty else None,
            separators=None if pretty else (",", ":"),
        )
    )


def _normalize_mineral(value: str | None) -> str | None:
    return " ".join(value.lower().split()) if value else None


def _effective_minerals(
    requested: Sequence[str] | None,
    mapping: Mapping[str, Sequence[str]],
) -> list[str]:
    if not requested:
        return sorted(mapping)
    return list(dict.fromkeys(" ".join(item.lower().split()) for item in requested))


def _bounded_provider_limit(value: str) -> int:
    return _bounded_int(value, minimum=1, maximum=MAX_PROVIDER_ITEMS)


def _bounded_nonnegative_provider_limit(value: str) -> int:
    return _bounded_int(value, minimum=0, maximum=MAX_PROVIDER_ITEMS)


def _bounded_refresh_hours(value: str) -> int:
    return _bounded_int(value, minimum=0, maximum=MAX_REFRESH_HOURS)


def _bounded_int(value: str, *, minimum: int, maximum: int) -> int:
    parsed = int(value)
    if not minimum <= parsed <= maximum:
        raise argparse.ArgumentTypeError(f"must be between {minimum} and {maximum}")
    return parsed


def _nonblank_text(value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise argparse.ArgumentTypeError("must not be blank")
    return normalized


def _path_argument(value: str) -> Path:
    return Path(_nonblank_text(value))


def _request_termination(_signum: int, _frame: FrameType | None) -> None:
    raise _TerminationRequested
