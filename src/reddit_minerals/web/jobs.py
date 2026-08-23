"""In-process, token-isolated live Reddit collection jobs.

The manager deliberately uses a bounded ``ThreadPoolExecutor`` instead of web
framework background tasks.  It holds an OS-level exclusive lock on its job
root for its full lifetime, so an accidental second process fails closed before
it can inspect or remove another process's job data.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import math
import re
import shutil
from base64 import urlsafe_b64decode, urlsafe_b64encode
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from threading import Event, RLock, Thread
from typing import Any

from reddit_minerals.clients.base import RedditClient
from reddit_minerals.clients.reddit import PrawRedditClient
from reddit_minerals.config import (
    MAX_LIVE_ACCESS_TOKEN_CHARS,
    MIN_LIVE_ACCESS_TOKEN_CHARS,
    AppSettings,
)
from reddit_minerals.errors import (
    BatchProviderFailureError,
    ConcurrentOperationError,
    ConfigurationError,
    OperationDeadlineExceededError,
    ProviderAuthenticationError,
    ProviderConfigurationError,
)
from reddit_minerals.models import ContentKind, WorkStatus, utc_now
from reddit_minerals.services.scrape import (
    ScrapeCancelledError,
    ScrapeProgress,
    ScrapeProgressStage,
    ScrapeService,
    ScrapeSummary,
)
from reddit_minerals.storage import Database
from reddit_minerals.web.live_models import (
    MAX_LIVE_COMMENTS_PER_POST,
    MAX_LIVE_POSTS_PER_MINERAL,
    MAX_LIVE_RECORDS_PER_JOB,
    MAX_LIVE_SUBREDDITS_PER_TARGET,
    MAX_LIVE_TARGETS,
    LiveCapabilitiesResponse,
    LiveCredentialMode,
    LiveJobCreateRequest,
    LiveJobDefaults,
    LiveJobError,
    LiveJobLimits,
    LiveJobProgress,
    LiveJobStage,
    LiveJobStatus,
    LiveJobView,
    LiveRecord,
    LiveScrapeTarget,
    LiveSnapshotResponse,
    LiveTimeFilter,
)

logger = logging.getLogger(__name__)


class LiveJobsError(Exception):
    """Base class for safe live-job API failures."""


class LiveCollectionDisabledError(LiveJobsError):
    """Live collection was not explicitly enabled by the deployment."""


class LiveCreationUnauthorizedError(LiveJobsError):
    """The caller did not authenticate permission to create live jobs."""


class LiveCredentialModeUnavailableError(LiveJobsError):
    """The requested credential source is not available on this deployment."""


class LiveJobCapacityError(LiveJobsError):
    """The bounded in-memory job registry has no available slot."""


class LiveJobRequestLimitError(LiveJobsError):
    """A request exceeds deployment-specific collection bounds."""


class LiveJobTokenInvalidError(LiveJobsError):
    """The caller did not provide a canonical 32-byte job capability token."""


class LiveJobIdempotencyConflictError(LiveJobsError):
    """A job token was reused for a different non-secret request."""


class LiveJobNotFoundError(LiveJobsError):
    """A job is absent or the caller did not present its opaque capability token."""


class LiveJobSnapshotUnavailableError(LiveJobsError):
    """A job has not produced a successful or partial terminal snapshot."""


class LiveJobCleanupError(LiveJobsError):
    """Job artifacts could not be removed, so access metadata was retained."""


@dataclass(frozen=True, slots=True)
class RedditCredentialValues:
    """Short-lived worker credentials intentionally excluded from representations."""

    client_id: str = field(repr=False)
    client_secret: str = field(repr=False)
    user_agent: str = field(repr=False)


RedditClientFactory = Callable[[RedditCredentialValues, AppSettings], RedditClient]


@dataclass(slots=True)
class _JobState:
    id: str
    token_digest: bytes = field(repr=False)
    request_fingerprint: bytes = field(repr=False)
    database_path: Path
    credential_mode: LiveCredentialMode
    targets: tuple[LiveScrapeTarget, ...]
    time_filter: LiveTimeFilter
    max_posts_per_mineral: int
    max_comments_per_post: int
    created_at: datetime
    progress: LiveJobProgress
    status: LiveJobStatus = LiveJobStatus.QUEUED
    stage: LiveJobStage = LiveJobStage.QUEUED
    started_at: datetime | None = None
    finished_at: datetime | None = None
    expires_at: datetime | None = None
    record_count: int = 0
    message: str = "Waiting for an available live collection worker."
    error: LiveJobError | None = None
    database_ready: bool = False
    cancel_event: Event = field(default_factory=Event, repr=False)
    future: Future[None] | None = field(default=None, repr=False)


_TERMINAL_STATUSES = frozenset(
    {
        LiveJobStatus.CANCELLED,
        LiveJobStatus.SUCCEEDED,
        LiveJobStatus.PARTIAL,
        LiveJobStatus.FAILED,
    }
)
_JOB_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")
_JOB_TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9_-]{43}$")
_JOB_ID_NAMESPACE = b"minerallens-live-job-v1\0"
_JOB_MARKER_NAME = ".minerallens-live-job"
_JOB_MARKER_PREFIX = "minerallens-live-job:v1:"


class LiveJobManager:
    """Bounded single-process executor with per-job storage and capability tokens."""

    def __init__(
        self,
        *,
        settings: AppSettings,
        client_factory: RedditClientFactory | None = None,
        clock: Callable[[], datetime] = utc_now,
        sweep_interval_seconds: float | None = None,
    ) -> None:
        self._settings = settings
        self._root = settings.live_job_root.expanduser().resolve()
        self._client_factory = client_factory or _create_praw_client
        self._clock = clock
        configured_creation_token = (
            settings.require_live_access_token() if settings.live_reddit_enabled else ""
        )
        self._creation_token_digest = hashlib.sha256(
            configured_creation_token.encode("utf-8")
        ).digest()
        del configured_creation_token
        configured_sweep_interval = (
            sweep_interval_seconds
            if sweep_interval_seconds is not None
            else max(0.1, min(30.0, settings.live_job_retention_seconds / 2))
        )
        if not math.isfinite(configured_sweep_interval) or configured_sweep_interval <= 0:
            raise ValueError("sweep interval must be a positive finite number")
        self._sweep_interval_seconds = configured_sweep_interval
        self._sweeper_stop = Event()
        self._lock = RLock()
        self._jobs: dict[str, _JobState] = {}
        self._credential_vault: dict[str, RedditCredentialValues] = {}
        self._active_clients: dict[str, RedditClient] = {}
        self._closed = False
        self._ownership_context: AbstractContextManager[None] = Database(
            self._root / ".live-jobs-manager"
        ).operation_lock()
        try:
            self._ownership_context.__enter__()
        except ConcurrentOperationError as exc:
            raise ConfigurationError(
                "The live job root is already owned by another application process"
            ) from exc
        try:
            self._purge_orphan_directories()
            self._executor = ThreadPoolExecutor(
                max_workers=settings.live_job_max_workers,
                thread_name_prefix="mineral-live-reddit",
            )
            self._sweeper_thread = Thread(
                target=self._sweep_loop,
                name="mineral-live-reddit-expiry",
                daemon=True,
            )
            self._sweeper_thread.start()
        except BaseException:
            executor = getattr(self, "_executor", None)
            if executor is not None:
                executor.shutdown(wait=True, cancel_futures=True)
            self._ownership_context.__exit__(None, None, None)
            raise

    def capabilities(self) -> LiveCapabilitiesResponse:
        """Return only deployment capability flags and public safety bounds."""

        return live_capabilities(self._settings)

    def _authorize_creation(self, access_token: str | None) -> None:
        supplied = access_token if access_token is not None else ""
        supplied_digest = hashlib.sha256(supplied.encode("utf-8")).digest()
        digest_matches = hmac.compare_digest(self._creation_token_digest, supplied_digest)
        length_valid = MIN_LIVE_ACCESS_TOKEN_CHARS <= len(supplied) <= (MAX_LIVE_ACCESS_TOKEN_CHARS)
        if not digest_matches or not length_valid:
            raise LiveCreationUnauthorizedError

    def create_job(
        self,
        request: LiveJobCreateRequest,
        *,
        creation_access_token: str | None,
        job_access_token: str,
    ) -> tuple[LiveJobView, str]:
        """Idempotently queue a validated job using a caller-generated token."""

        if not self._settings.live_reddit_enabled:
            raise LiveCollectionDisabledError
        self._authorize_creation(creation_access_token)
        _validate_job_access_token(job_access_token)
        self._purge_expired()
        encoded_access_token = job_access_token.encode("ascii")
        job_id = hashlib.sha256(_JOB_ID_NAMESPACE + encoded_access_token).digest()[:16].hex()
        token_digest = hashlib.sha256(encoded_access_token).digest()
        request_fingerprint = _request_fingerprint(request)
        with self._lock:
            existing = self._jobs.get(job_id)
            if existing is not None:
                token_matches = hmac.compare_digest(existing.token_digest, token_digest)
                request_matches = hmac.compare_digest(
                    existing.request_fingerprint,
                    request_fingerprint,
                )
                if not token_matches or not request_matches:
                    raise LiveJobIdempotencyConflictError
                return self._view_locked(existing), job_access_token
            capabilities = self.capabilities()
            if request.max_posts_per_mineral > capabilities.limits.max_posts_per_mineral:
                raise LiveJobRequestLimitError
            if request.max_comments_per_post > capabilities.limits.max_comments_per_post:
                raise LiveJobRequestLimitError
            if self._closed:
                raise LiveJobCapacityError
            active_jobs = sum(
                state.status not in _TERMINAL_STATUSES for state in self._jobs.values()
            )
            if active_jobs >= self._settings.live_job_max_active:
                raise LiveJobCapacityError
            self._make_capacity_locked()
            credentials = self._resolve_credentials(request)
            progress = LiveJobProgress(
                minerals_total=len(request.targets),
                subreddits_total=sum(len(target.subreddits) for target in request.targets),
            )
            state = _JobState(
                id=job_id,
                token_digest=token_digest,
                request_fingerprint=request_fingerprint,
                database_path=self._root / job_id / "reddit.sqlite3",
                credential_mode=request.credential_mode,
                targets=tuple(request.targets),
                time_filter=request.time_filter,
                max_posts_per_mineral=request.max_posts_per_mineral,
                max_comments_per_post=request.max_comments_per_post,
                created_at=self._clock(),
                progress=progress,
            )
            self._jobs[job_id] = state
            self._credential_vault[job_id] = credentials
            try:
                state.future = self._executor.submit(self._run_job, job_id)
            except Exception as exc:
                self._jobs.pop(job_id, None)
                self._credential_vault.pop(job_id, None)
                raise LiveJobCapacityError from exc
            return self._view_locked(state), job_access_token

    def get_job(self, job_id: str, access_token: str | None) -> LiveJobView:
        self._purge_expired()
        with self._lock:
            state = self._authorized_state_locked(job_id, access_token)
            return self._view_locked(state)

    def cancel_job(self, job_id: str, access_token: str | None) -> LiveJobView:
        """Cancel a queued future or request cooperative cancellation in a worker."""

        self._purge_expired()
        with self._lock:
            state = self._authorized_state_locked(job_id, access_token)
            if state.status in _TERMINAL_STATUSES:
                view = self._view_locked(state).model_copy(
                    update={
                        "record_count": 0,
                        "message": "Live job metadata and collected artifacts were deleted.",
                    }
                )
                self._remove_state_locked(state)
                return view
            state.cancel_event.set()
            future = state.future
            if state.status is LiveJobStatus.QUEUED and future is not None and future.cancel():
                self._credential_vault.pop(job_id, None)
                self._finish_locked(
                    state,
                    status=LiveJobStatus.CANCELLED,
                    message="Live collection was cancelled before it started.",
                )
            else:
                state.status = LiveJobStatus.CANCEL_REQUESTED
                state.message = (
                    "Cancellation requested; the current Reddit request will finish first."
                )
            return self._view_locked(state)

    def snapshot(self, job_id: str, access_token: str | None) -> LiveSnapshotResponse:
        """Read one consistent snapshot from only the authenticated job database."""

        self._purge_expired()
        with self._lock:
            state = self._authorized_state_locked(job_id, access_token)
            if state.status not in {LiveJobStatus.SUCCEEDED, LiveJobStatus.PARTIAL}:
                raise LiveJobSnapshotUnavailableError
            records = (
                tuple(_live_record(item) for item in Database(state.database_path).export_records())
                if state.database_ready
                else ()
            )
            return LiveSnapshotResponse(
                job_id=state.id,
                status=state.status,
                generated_at=self._clock(),
                records=records,
            )

    def shutdown(self) -> None:
        """Stop accepting work and cooperatively cancel all unfinished jobs."""

        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._sweeper_stop.set()
            for state in self._jobs.values():
                if state.status not in _TERMINAL_STATUSES:
                    state.cancel_event.set()
                    state.status = LiveJobStatus.CANCEL_REQUESTED
                    state.message = "Cancellation requested during application shutdown."
            self._credential_vault.clear()
        self._sweeper_thread.join()
        # The root ownership lock cannot be released while a worker could still
        # write beneath it. Provider requests are separately timeout-bounded, so
        # shutdown always waits for cooperative worker completion.
        self._executor.shutdown(wait=True, cancel_futures=True)
        with self._lock:
            for state in self._jobs.values():
                if state.status not in _TERMINAL_STATUSES:
                    self._finish_locked(
                        state,
                        status=LiveJobStatus.CANCELLED,
                        message="Live collection stopped during application shutdown.",
                    )
            for state in tuple(self._jobs.values()):
                try:
                    self._remove_state_locked(state)
                except LiveJobCleanupError:
                    logger.error(
                        "live job shutdown cleanup failed",
                        extra={"job_id": state.id},
                    )
        self._ownership_context.__exit__(None, None, None)

    def _resolve_credentials(self, request: LiveJobCreateRequest) -> RedditCredentialValues:
        if request.credential_mode is LiveCredentialMode.SERVER:
            try:
                client_id, client_secret, user_agent = self._settings.require_reddit()
            except ConfigurationError as exc:
                raise LiveCredentialModeUnavailableError from exc
            return RedditCredentialValues(client_id, client_secret, user_agent)

        if not self._settings.live_reddit_allow_byo_credentials:
            raise LiveCredentialModeUnavailableError
        provided = request.credentials
        if provided is None:  # defensive; request model validation enforces this
            raise LiveCredentialModeUnavailableError
        return RedditCredentialValues(
            client_id=provided.client_id.get_secret_value(),
            client_secret=provided.client_secret.get_secret_value(),
            user_agent=provided.user_agent,
        )

    def _run_job(self, job_id: str) -> None:
        with self._lock:
            state = self._jobs.get(job_id)
            if state is None or state.status is LiveJobStatus.CANCELLED:
                self._credential_vault.pop(job_id, None)
                return
            credentials = self._credential_vault.pop(job_id, None)
            if credentials is None:
                self._finish_locked(
                    state,
                    status=LiveJobStatus.CANCELLED,
                    message="Live collection stopped before credentials were acquired.",
                )
                return
            if state.cancel_event.is_set():
                self._finish_locked(
                    state,
                    status=LiveJobStatus.CANCELLED,
                    message="Live collection was cancelled before it started.",
                )
                return
            state.status = LiveJobStatus.RUNNING
            state.stage = LiveJobStage.SEARCHING
            state.started_at = self._clock()
            state.message = "Searching the selected subreddits."

        try:
            client = self._client_factory(credentials, self._settings)
            del credentials
            with self._lock:
                current = self._jobs.get(job_id)
                cancelled_before_use = (
                    current is None
                    or self._closed
                    or current.cancel_event.is_set()
                    or current.status in _TERMINAL_STATUSES
                )
                if not cancelled_before_use:
                    self._active_clients[job_id] = client
            if cancelled_before_use:
                _close_reddit_client(client, job_id=job_id)
                self._finish_cancelled(job_id)
                return
            self._prepare_job_directory(state)
            database = Database(state.database_path)
            database.initialize()
            with self._lock:
                current = self._jobs.get(job_id)
                if current is not None:
                    current.database_ready = True

            mapping = {target.mineral: target.subreddits for target in state.targets}
            service = ScrapeService(
                client=client,
                database=database,
                max_retries=self._settings.max_retries,
                retry_base_delay_seconds=self._settings.retry_base_delay_seconds,
                retry_max_delay_seconds=self._settings.retry_max_delay_seconds,
                operation_timeout_seconds=self._settings.operation_timeout_seconds,
            )
            summary = service.run(
                mapping=mapping,
                minerals=None,
                max_posts_per_mineral=state.max_posts_per_mineral,
                max_comments_per_post=state.max_comments_per_post,
                refresh_after=timedelta(0),
                time_filter=state.time_filter.value,
                dry_run=False,
                force=False,
                progress=lambda update: self._update_progress(job_id, update),
                cancel_requested=state.cancel_event.is_set,
            )
            self._finish_success(job_id, database, summary)
        except ScrapeCancelledError:
            self._finish_cancelled(job_id)
        except ProviderAuthenticationError:
            self._finish_failed(
                job_id,
                code="reddit_authentication_failed",
                message="Reddit rejected the configured application credentials.",
            )
        except ProviderConfigurationError:
            self._finish_failed(
                job_id,
                code="reddit_configuration_failed",
                message="The Reddit client configuration could not be initialized.",
            )
        except OperationDeadlineExceededError:
            self._finish_failed(
                job_id,
                code="operation_timed_out",
                message="Live collection reached its configured time limit.",
                partial_if_records=True,
            )
        except BatchProviderFailureError:
            self._finish_failed(
                job_id,
                code="reddit_unavailable",
                message="Every attempted Reddit operation failed.",
                partial_if_records=True,
            )
        except Exception as exc:  # never reflect unexpected provider text to the API or logs
            logger.error(
                "live Reddit job failed",
                extra={"job_id": job_id, "error_type": type(exc).__name__},
            )
            self._finish_failed(
                job_id,
                code="internal_error",
                message="Live collection failed unexpectedly.",
                partial_if_records=True,
            )
        finally:
            with self._lock:
                active_client = self._active_clients.pop(job_id, None)
            if active_client is not None:
                _close_reddit_client(active_client, job_id=job_id)

    def _update_progress(self, job_id: str, update: ScrapeProgress) -> None:
        with self._lock:
            state = self._jobs.get(job_id)
            if state is None or state.status in _TERMINAL_STATUSES:
                return
            cancellation_pending = state.status is LiveJobStatus.CANCEL_REQUESTED
            if not cancellation_pending:
                state.status = LiveJobStatus.RUNNING
            state.stage = (
                LiveJobStage.SEARCHING
                if update.stage is ScrapeProgressStage.SEARCHING
                else LiveJobStage.COLLECTING
            )
            summary = update.summary
            state.progress = LiveJobProgress(
                minerals_total=update.minerals_total,
                minerals_completed=update.minerals_completed,
                subreddits_total=update.subreddits_total,
                subreddits_completed=update.subreddits_completed,
                posts_discovered=summary.posts_discovered,
                posts_stored=summary.posts_completed + summary.posts_failed,
                posts_failed=summary.posts_failed,
                comments_stored=summary.comments_stored,
                searches_failed=summary.searches_failed,
            )
            state.record_count = state.progress.posts_stored + state.progress.comments_stored
            if not cancellation_pending:
                state.message = (
                    "Searching the selected subreddits."
                    if state.stage is LiveJobStage.SEARCHING
                    else "Collecting bounded post and comment records."
                )

    def _finish_success(
        self,
        job_id: str,
        database: Database,
        summary: ScrapeSummary,
    ) -> None:
        with self._lock:
            state = self._jobs.get(job_id)
            if state is None or state.status in _TERMINAL_STATUSES:
                return
            if state.cancel_event.is_set():
                state.record_count = self._record_count_locked(state)
                self._finish_locked(
                    state,
                    status=LiveJobStatus.CANCELLED,
                    message="Live collection was cancelled before a snapshot was finalized.",
                )
                return
            state.stage = LiveJobStage.FINALIZING
            state.message = "Finalizing the isolated live collection snapshot."
        record_count = sum(1 for _item in database.export_records())
        partial = summary.searches_failed > 0 or summary.posts_failed > 0
        with self._lock:
            state = self._jobs.get(job_id)
            if state is None or state.status in _TERMINAL_STATUSES:
                return
            state.record_count = record_count
            if state.cancel_event.is_set():
                self._finish_locked(
                    state,
                    status=LiveJobStatus.CANCELLED,
                    message="Live collection was cancelled before a snapshot was finalized.",
                )
                return
            self._finish_locked(
                state,
                status=LiveJobStatus.PARTIAL if partial else LiveJobStatus.SUCCEEDED,
                message=(
                    "Live collection completed with some unavailable Reddit records."
                    if partial
                    else "Live collection completed successfully."
                ),
            )

    def _finish_cancelled(self, job_id: str) -> None:
        with self._lock:
            state = self._jobs.get(job_id)
            if state is None or state.status in _TERMINAL_STATUSES:
                return
            state.record_count = self._record_count_locked(state)
            self._finish_locked(
                state,
                status=LiveJobStatus.CANCELLED,
                message="Live collection was cancelled before a snapshot was finalized.",
            )

    def _finish_failed(
        self,
        job_id: str,
        *,
        code: str,
        message: str,
        partial_if_records: bool = False,
    ) -> None:
        with self._lock:
            state = self._jobs.get(job_id)
            if state is None or state.status in _TERMINAL_STATUSES:
                return
            if state.cancel_event.is_set():
                state.record_count = self._record_count_locked(state)
                self._finish_locked(
                    state,
                    status=LiveJobStatus.CANCELLED,
                    message="Live collection was cancelled before a snapshot was finalized.",
                )
                return
            state.record_count = self._record_count_locked(state)
            status = (
                LiveJobStatus.PARTIAL
                if partial_if_records and state.record_count > 0
                else LiveJobStatus.FAILED
            )
            state.error = LiveJobError(code=code, message=message)
            self._finish_locked(state, status=status, message=message)

    def _record_count_locked(self, state: _JobState) -> int:
        if not state.database_ready:
            return 0
        try:
            return sum(1 for _item in Database(state.database_path).export_records())
        except Exception:
            return state.record_count

    def _finish_locked(
        self,
        state: _JobState,
        *,
        status: LiveJobStatus,
        message: str,
    ) -> None:
        finished_at = self._clock()
        state.status = status
        state.stage = LiveJobStage.COMPLETE
        state.finished_at = finished_at
        state.expires_at = finished_at + timedelta(
            seconds=self._settings.live_job_retention_seconds
        )
        state.message = message
        self._credential_vault.pop(state.id, None)

    def _authorized_state_locked(
        self,
        job_id: str,
        access_token: str | None,
    ) -> _JobState:
        state = self._jobs.get(job_id)
        if state is None or access_token is None:
            raise LiveJobNotFoundError
        supplied = hashlib.sha256(access_token.encode("utf-8")).digest()
        if not hmac.compare_digest(state.token_digest, supplied):
            raise LiveJobNotFoundError
        return state

    def _view_locked(self, state: _JobState) -> LiveJobView:
        return LiveJobView(
            id=state.id,
            status=state.status,
            stage=state.stage,
            credential_mode=state.credential_mode,
            targets=state.targets,
            time_filter=state.time_filter,
            max_posts_per_mineral=state.max_posts_per_mineral,
            max_comments_per_post=state.max_comments_per_post,
            created_at=state.created_at,
            started_at=state.started_at,
            finished_at=state.finished_at,
            expires_at=state.expires_at,
            progress=state.progress,
            record_count=state.record_count,
            message=state.message,
            error=state.error,
        )

    def _purge_expired(self) -> None:
        with self._lock:
            now = self._clock()
            expired = [
                state
                for state in self._jobs.values()
                if state.expires_at is not None and state.expires_at <= now
            ]
            for state in expired:
                try:
                    self._remove_state_locked(state)
                except LiveJobCleanupError:
                    logger.error(
                        "expired live job cleanup failed",
                        extra={"job_id": state.id},
                    )

    def _sweep_loop(self) -> None:
        while not self._sweeper_stop.wait(self._sweep_interval_seconds):
            try:
                self._purge_expired()
            except Exception as exc:
                logger.error(
                    "live job expiry sweep failed",
                    extra={"error_type": type(exc).__name__},
                )

    def _make_capacity_locked(self) -> None:
        while len(self._jobs) >= self._settings.live_job_max_retained:
            terminal = sorted(
                (state for state in self._jobs.values() if state.status in _TERMINAL_STATUSES),
                key=lambda state: state.finished_at or state.created_at,
            )
            if not terminal:
                raise LiveJobCapacityError
            removed = False
            for candidate in terminal:
                try:
                    self._remove_state_locked(candidate)
                except LiveJobCleanupError:
                    logger.error(
                        "live job capacity cleanup failed",
                        extra={"job_id": candidate.id},
                    )
                    continue
                removed = True
                break
            if not removed:
                raise LiveJobCapacityError

    def _remove_state_locked(self, state: _JobState) -> None:
        job_directory = state.database_path.parent
        if job_directory.parent.resolve() != self._root or job_directory.name != state.id:
            raise LiveJobCleanupError
        if job_directory.exists() or job_directory.is_symlink():
            if not self._is_owned_job_directory(job_directory, state.id):
                raise LiveJobCleanupError
            try:
                shutil.rmtree(job_directory)
            except OSError as exc:
                raise LiveJobCleanupError from exc
            if job_directory.exists():
                raise LiveJobCleanupError
        self._jobs.pop(state.id, None)
        self._credential_vault.pop(state.id, None)

    def _prepare_job_directory(self, state: _JobState) -> None:
        job_directory = state.database_path.parent
        if (
            job_directory.parent.resolve() != self._root
            or _JOB_ID_PATTERN.fullmatch(job_directory.name) is None
            or job_directory.name != state.id
        ):
            raise ConfigurationError("Cannot safely create live job artifacts")
        created_directory = False
        marker_path = job_directory / _JOB_MARKER_NAME
        try:
            job_directory.mkdir(parents=False, exist_ok=False)
            created_directory = True
            with marker_path.open("x", encoding="utf-8", newline="\n") as marker:
                marker.write(_job_marker_content(state.id))
        except OSError as exc:
            if created_directory:
                try:
                    marker_path.unlink(missing_ok=True)
                    job_directory.rmdir()
                except OSError:
                    pass
            raise ConfigurationError("Cannot safely create live job artifacts") from exc

    def _is_owned_job_directory(self, directory: Path, job_id: str) -> bool:
        if (
            _JOB_ID_PATTERN.fullmatch(job_id) is None
            or directory.name != job_id
            or directory.is_symlink()
            or not directory.is_dir()
        ):
            return False
        try:
            if directory.resolve().parent != self._root:
                return False
            marker = directory / _JOB_MARKER_NAME
            if marker.is_symlink() or not marker.is_file():
                return False
            expected = _job_marker_content(job_id)
            if marker.stat().st_size != len(expected.encode("utf-8")):
                return False
            return marker.read_text(encoding="utf-8") == expected
        except (OSError, UnicodeError):
            return False

    def _purge_orphan_directories(self) -> None:
        """Remove unreachable job stores left by an earlier single-process run."""

        if not self._root.is_dir():
            return
        for child in self._root.iterdir():
            if _JOB_ID_PATTERN.fullmatch(child.name) is None or child.is_symlink():
                continue
            if self._is_owned_job_directory(child, child.name):
                try:
                    shutil.rmtree(child)
                except OSError as exc:
                    raise ConfigurationError(
                        "Cannot safely clean unreachable live job artifacts"
                    ) from exc
                if child.exists():
                    raise ConfigurationError("Cannot safely clean unreachable live job artifacts")


def _validate_job_access_token(access_token: str) -> None:
    if _JOB_TOKEN_PATTERN.fullmatch(access_token) is None:
        raise LiveJobTokenInvalidError
    try:
        decoded = urlsafe_b64decode(access_token + "=")
    except (ValueError, TypeError) as exc:
        raise LiveJobTokenInvalidError from exc
    canonical = urlsafe_b64encode(decoded).rstrip(b"=").decode("ascii")
    if len(decoded) != 32 or not hmac.compare_digest(canonical, access_token):
        raise LiveJobTokenInvalidError


def _request_fingerprint(request: LiveJobCreateRequest) -> bytes:
    canonical = {
        "credential_mode": request.credential_mode.value,
        "max_comments_per_post": request.max_comments_per_post,
        "max_posts_per_mineral": request.max_posts_per_mineral,
        "targets": [
            {
                "mineral": target.mineral,
                "subreddits": list(target.subreddits),
            }
            for target in request.targets
        ],
        "time_filter": request.time_filter.value,
    }
    encoded = json.dumps(
        canonical,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).digest()


def _job_marker_content(job_id: str) -> str:
    return f"{_JOB_MARKER_PREFIX}{job_id}\n"


def _create_praw_client(
    credentials: RedditCredentialValues,
    settings: AppSettings,
) -> RedditClient:
    return PrawRedditClient(
        client_id=credentials.client_id,
        client_secret=credentials.client_secret,
        user_agent=credentials.user_agent,
        replace_more_limit=settings.reddit_replace_more_limit,
        request_timeout_seconds=settings.reddit_request_timeout_seconds,
    )


def _close_reddit_client(client: RedditClient, *, job_id: str) -> None:
    close = getattr(client, "close", None)
    if not callable(close):
        return
    try:
        close()
    except Exception as exc:
        logger.error(
            "live Reddit client close failed",
            extra={"job_id": job_id, "error_type": type(exc).__name__},
        )


def _server_reddit_configured(settings: AppSettings) -> bool:
    try:
        settings.require_reddit()
    except ConfigurationError:
        return False
    return True


def live_capabilities(settings: AppSettings) -> LiveCapabilitiesResponse:
    """Build the safe capability document without constructing a job executor."""

    server_configured = _server_reddit_configured(settings)
    modes: list[LiveCredentialMode] = []
    if settings.live_reddit_enabled and server_configured:
        modes.append(LiveCredentialMode.SERVER)
    if settings.live_reddit_enabled and settings.live_reddit_allow_byo_credentials:
        modes.append(LiveCredentialMode.PROVIDED)
    return LiveCapabilitiesResponse(
        enabled=settings.live_reddit_enabled,
        server_credentials_configured=server_configured,
        byo_credentials_allowed=(
            settings.live_reddit_enabled and settings.live_reddit_allow_byo_credentials
        ),
        credential_modes=tuple(modes),
        time_filters=tuple(LiveTimeFilter),
        defaults=LiveJobDefaults(
            time_filter=LiveTimeFilter.WEEK,
            max_posts_per_mineral=min(10, settings.max_posts_per_mineral),
            max_comments_per_post=min(25, settings.max_comments_per_post),
        ),
        limits=LiveJobLimits(
            max_targets=MAX_LIVE_TARGETS,
            max_subreddits_per_target=MAX_LIVE_SUBREDDITS_PER_TARGET,
            max_posts_per_mineral=min(
                MAX_LIVE_POSTS_PER_MINERAL,
                settings.max_posts_per_mineral,
            ),
            max_comments_per_post=min(
                MAX_LIVE_COMMENTS_PER_POST,
                settings.max_comments_per_post,
            ),
            max_records_per_job=MAX_LIVE_RECORDS_PER_JOB,
            max_active_jobs=settings.live_job_max_active,
            retention_seconds=settings.live_job_retention_seconds,
        ),
    )


def _live_record(item: dict[str, Any]) -> LiveRecord:
    content_value = item["content"]
    if not isinstance(content_value, dict):
        raise ValueError("Invalid live record content")
    content = content_value
    kind = ContentKind(str(item["record_type"]))
    if kind is ContentKind.POST:
        return LiveRecord(
            id=str(content["id"]),
            kind=kind,
            post_id=None,
            parent_id=None,
            depth=None,
            mineral=str(item["mineral"]),
            title=str(content["title"]),
            body=str(content["selftext"]),
            subreddit=str(content["subreddit"]),
            created_at=datetime.fromisoformat(str(content["created_at"])),
            fetched_at=datetime.fromisoformat(str(content["fetched_at"])),
            score=int(content["score"]),
            comment_count=int(content["num_comments"]),
            upvote_ratio=(
                float(content["upvote_ratio"]) if content.get("upvote_ratio") is not None else None
            ),
            permalink=str(content["permalink"]),
            scrape_status=WorkStatus(str(content["scrape_status"])),
        )
    return LiveRecord(
        id=str(content["id"]),
        kind=kind,
        post_id=str(content["post_id"]),
        parent_id=str(content["parent_id"]) if content.get("parent_id") is not None else None,
        depth=int(content["depth"]),
        mineral=str(item["mineral"]),
        title=None,
        body=str(content["body"]),
        subreddit=str(content["subreddit"]),
        created_at=datetime.fromisoformat(str(content["created_at"])),
        fetched_at=datetime.fromisoformat(str(content["fetched_at"])),
        score=int(content["score"]),
        comment_count=None,
        upvote_ratio=None,
        permalink=str(content["permalink"]),
        scrape_status=WorkStatus.COMPLETE,
    )
