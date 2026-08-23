"""Import-safe FastAPI application factory for the read-only portfolio API."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from hashlib import sha256
from hmac import compare_digest
from pathlib import Path
from typing import Annotated, cast

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, status
from fastapi import Path as ApiPath
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.responses import Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from reddit_minerals import __version__
from reddit_minerals.config import (
    MAX_LIVE_ACCESS_TOKEN_CHARS,
    MIN_LIVE_ACCESS_TOKEN_CHARS,
    AppSettings,
)
from reddit_minerals.models import ContentKind, Sentiment, utc_now
from reddit_minerals.web.jobs import (
    LiveCollectionDisabledError,
    LiveCreationUnauthorizedError,
    LiveCredentialModeUnavailableError,
    LiveJobCapacityError,
    LiveJobCleanupError,
    LiveJobIdempotencyConflictError,
    LiveJobManager,
    LiveJobNotFoundError,
    LiveJobRequestLimitError,
    LiveJobSnapshotUnavailableError,
    LiveJobTokenInvalidError,
    live_capabilities,
)
from reddit_minerals.web.live_models import (
    LiveCapabilitiesResponse,
    LiveJobCreatedResponse,
    LiveJobCreateRequest,
    LiveJobView,
    LiveSnapshotResponse,
)
from reddit_minerals.web.models import (
    DashboardResponse,
    DatasetKind,
    ErrorResponse,
    HealthResponse,
    MetaResponse,
    RecordDetail,
    RecordPage,
    RecordSort,
    RunPage,
    RunStatus,
    SnapshotResponse,
    UiConfigResponse,
    ValidationIssue,
)
from reddit_minerals.web.repository import (
    KaggleSampleReadRepository,
    ReadRepository,
    UnsupportedFilterError,
)

_API_PREFIX = "/api/v1"
_ASSET_DIRECTORY_ENVIRONMENT = "RMS_WEB_ASSET_DIR"
_LIVE_JOB_CREATE_PATH = f"{_API_PREFIX}/live/jobs"
MAX_LIVE_JOB_REQUEST_BODY_BYTES = 64 * 1_024


class _LiveJobCreationGuardMiddleware:
    """Authenticate and bound live-job creation before the request body is parsed."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        creation_token_digest: bytes | None,
        max_body_bytes: int,
    ) -> None:
        if max_body_bytes < 1:
            raise ValueError("live job request body limit must be positive")
        self._app = app
        self._creation_token_digest = creation_token_digest
        self._max_body_bytes = max_body_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if (
            scope["type"] != "http"
            or scope.get("method") != "POST"
            or scope.get("path") != _LIVE_JOB_CREATE_PATH
        ):
            await self._app(scope, receive, send)
            return

        headers = tuple(scope.get("headers", ()))
        if self._creation_token_digest is not None and not self._authorized(headers):
            await _live_creation_guard_error(
                status_code=401,
                code="live_access_unauthorized",
                message="Live job creation requires valid access credentials.",
            )(scope, receive, send)
            return

        if self._declared_body_exceeds_limit(headers):
            await _live_creation_guard_error(
                status_code=413,
                code="live_job_request_too_large",
                message="The live job request body exceeds the allowed size.",
            )(scope, receive, send)
            return

        body = bytearray()
        disconnected = False
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                disconnected = True
                break
            if message["type"] != "http.request":
                continue
            chunk = message.get("body", b"")
            if len(chunk) > self._max_body_bytes - len(body):
                await _live_creation_guard_error(
                    status_code=413,
                    code="live_job_request_too_large",
                    message="The live job request body exceeds the allowed size.",
                )(scope, receive, send)
                return
            body.extend(chunk)
            if not message.get("more_body", False):
                break

        replayed = False

        async def replay_receive() -> Message:
            nonlocal replayed
            if not replayed:
                replayed = True
                if disconnected:
                    return {"type": "http.disconnect"}
                return {"type": "http.request", "body": bytes(body), "more_body": False}
            return await receive()

        await self._app(scope, replay_receive, send)

    def _authorized(self, headers: tuple[tuple[bytes, bytes], ...]) -> bool:
        values = [value for name, value in headers if name.lower() == b"x-live-access-token"]
        supplied = values[0].decode("latin-1") if len(values) == 1 else ""
        supplied_digest = sha256(supplied.encode("utf-8")).digest()
        expected_digest = self._creation_token_digest or b""
        digest_matches = compare_digest(expected_digest, supplied_digest)
        length_valid = MIN_LIVE_ACCESS_TOKEN_CHARS <= len(supplied) <= (MAX_LIVE_ACCESS_TOKEN_CHARS)
        return digest_matches and length_valid

    def _declared_body_exceeds_limit(
        self,
        headers: tuple[tuple[bytes, bytes], ...],
    ) -> bool:
        values = [value for name, value in headers if name.lower() == b"content-length"]
        if not values:
            return False
        if len(values) != 1:
            return True
        try:
            declared = int(values[0].decode("ascii"))
        except (UnicodeDecodeError, ValueError):
            return True
        return declared < 0 or declared > self._max_body_bytes


def _get_repository(request: Request) -> ReadRepository:
    return cast(ReadRepository, request.app.state.read_repository)


RepositoryDependency = Annotated[ReadRepository, Depends(_get_repository)]


def create_app(
    *,
    repository: ReadRepository | None = None,
    asset_root: Path | None = None,
    settings: AppSettings | None = None,
    live_job_manager: LiveJobManager | None = None,
) -> FastAPI:
    """Create the API; live provider work remains opt-in and executor-isolated."""

    selected_settings = settings if settings is not None else AppSettings()
    owns_live_job_manager = live_job_manager is None and selected_settings.live_reddit_enabled
    selected_live_job_manager = live_job_manager
    if selected_live_job_manager is None and selected_settings.live_reddit_enabled:
        selected_live_job_manager = LiveJobManager(settings=selected_settings)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            if owns_live_job_manager and selected_live_job_manager is not None:
                selected_live_job_manager.shutdown()

    app = FastAPI(
        title="MineralLens API",
        summary="Research sample API with optional isolated live Reddit collection.",
        description=(
            "The public interface exposes a bounded sample of derived research metadata. "
            "Trusted FastAPI deployments may separately enable bounded live Reddit jobs; "
            "that capability is disabled by default and never changes the public sample."
        ),
        version=__version__,
        openapi_url=f"{_API_PREFIX}/openapi.json",
        docs_url=f"{_API_PREFIX}/docs",
        redoc_url=None,
        lifespan=lifespan,
    )
    app.state.read_repository = (
        repository if repository is not None else KaggleSampleReadRepository()
    )
    app.state.live_job_manager = selected_live_job_manager
    snapshot_etag = _snapshot_etag(app.state.read_repository)
    app.add_middleware(GZipMiddleware, minimum_size=1_000, compresslevel=6)
    creation_token_digest: bytes | None = None
    if selected_settings.live_reddit_enabled:
        creation_access_token = selected_settings.require_live_access_token()
        creation_token_digest = sha256(creation_access_token.encode("utf-8")).digest()
        del creation_access_token
    app.add_middleware(
        _LiveJobCreationGuardMiddleware,
        creation_token_digest=creation_token_digest,
        max_body_bytes=MAX_LIVE_JOB_REQUEST_BODY_BYTES,
    )

    @app.middleware("http")
    async def response_policy(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        path = request.url.path
        if (
            request.method == "GET"
            and path == f"{_API_PREFIX}/snapshot"
            and request.headers.get("if-none-match") == snapshot_etag
        ):
            return Response(
                status_code=304,
                headers={
                    "Cache-Control": "public, max-age=300, stale-while-revalidate=60",
                    "ETag": snapshot_etag,
                },
            )

        response = await call_next(request)
        if response.status_code == 200 and path.startswith("/assets/"):
            response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        elif response.status_code == 200 and path == f"{_API_PREFIX}/snapshot":
            response.headers["Cache-Control"] = "public, max-age=300, stale-while-revalidate=60"
            response.headers["ETag"] = snapshot_etag
        elif path.startswith(f"{_API_PREFIX}/"):
            response.headers["Cache-Control"] = "no-store"
        elif response.headers.get("content-type", "").startswith("text/html"):
            response.headers["Cache-Control"] = "no-cache"
        return response

    @app.exception_handler(RequestValidationError)
    async def request_validation_handler(
        _request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        issues = tuple(
            ValidationIssue(
                field=".".join(str(part) for part in error.get("loc", ())),
                message=str(error.get("msg", "Invalid value"))[:160],
                type=str(error.get("type", "value_error"))[:80],
            )
            for error in exc.errors()
        )
        return _error_response(
            status_code=422,
            code="validation_error",
            message="The request contains invalid parameters.",
            issues=issues,
        )

    @app.exception_handler(UnsupportedFilterError)
    async def unsupported_filter_handler(
        _request: Request, _exc: UnsupportedFilterError
    ) -> JSONResponse:
        return _error_response(
            status_code=422,
            code="unsupported_filter",
            message="The request contains a filter that is unavailable in this dataset.",
        )

    @app.exception_handler(LiveCollectionDisabledError)
    async def live_disabled_handler(
        _request: Request, _exc: LiveCollectionDisabledError
    ) -> JSONResponse:
        return _error_response(
            status_code=503,
            code="live_collection_disabled",
            message="Live Reddit collection is disabled on this deployment.",
        )

    @app.exception_handler(LiveCredentialModeUnavailableError)
    async def live_credentials_handler(
        _request: Request, _exc: LiveCredentialModeUnavailableError
    ) -> JSONResponse:
        return _error_response(
            status_code=422,
            code="credential_mode_unavailable",
            message="The selected Reddit credential mode is unavailable.",
        )

    @app.exception_handler(LiveCreationUnauthorizedError)
    async def live_creation_unauthorized_handler(
        _request: Request, _exc: LiveCreationUnauthorizedError
    ) -> JSONResponse:
        return _error_response(
            status_code=401,
            code="live_access_unauthorized",
            message="Live job creation requires valid access credentials.",
        )

    @app.exception_handler(LiveJobTokenInvalidError)
    async def live_job_token_invalid_handler(
        _request: Request, _exc: LiveJobTokenInvalidError
    ) -> JSONResponse:
        return _error_response(
            status_code=422,
            code="invalid_live_job_token",
            message="The live job token has an invalid format.",
        )

    @app.exception_handler(LiveJobIdempotencyConflictError)
    async def live_idempotency_conflict_handler(
        _request: Request, _exc: LiveJobIdempotencyConflictError
    ) -> JSONResponse:
        return _error_response(
            status_code=409,
            code="live_job_idempotency_conflict",
            message="The live job token is already associated with another request.",
        )

    @app.exception_handler(LiveJobCapacityError)
    async def live_capacity_handler(_request: Request, _exc: LiveJobCapacityError) -> JSONResponse:
        return _error_response(
            status_code=429,
            code="live_job_capacity_reached",
            message="The live collection service has reached its configured job limit.",
        )

    @app.exception_handler(LiveJobRequestLimitError)
    async def live_request_limit_handler(
        _request: Request, _exc: LiveJobRequestLimitError
    ) -> JSONResponse:
        return _error_response(
            status_code=422,
            code="live_job_limit_exceeded",
            message="The live collection request exceeds this deployment's safety limits.",
        )

    @app.exception_handler(LiveJobSnapshotUnavailableError)
    async def live_snapshot_unavailable_handler(
        _request: Request, _exc: LiveJobSnapshotUnavailableError
    ) -> JSONResponse:
        return _error_response(
            status_code=409,
            code="live_snapshot_unavailable",
            message="A snapshot is available only after a successful or partial job.",
        )

    @app.exception_handler(LiveJobCleanupError)
    async def live_cleanup_handler(_request: Request, _exc: LiveJobCleanupError) -> JSONResponse:
        return _error_response(
            status_code=503,
            code="live_job_cleanup_failed",
            message="Live job artifacts could not be deleted; access was retained for retry.",
        )

    @app.exception_handler(LiveJobNotFoundError)
    async def live_job_not_found_handler(
        _request: Request, _exc: LiveJobNotFoundError
    ) -> JSONResponse:
        return _error_response(
            status_code=404,
            code="not_found",
            message="The requested resource was not found.",
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(
        _request: Request, exc: StarletteHTTPException
    ) -> JSONResponse:
        if exc.status_code == 404:
            return _error_response(
                status_code=404,
                code="not_found",
                message="The requested resource was not found.",
            )
        return _error_response(
            status_code=exc.status_code,
            code="http_error",
            message="The request could not be completed.",
        )

    @app.get(
        f"{_API_PREFIX}/health",
        response_model=HealthResponse,
        tags=["system"],
    )
    def health(read_repository: RepositoryDependency) -> HealthResponse:
        source = read_repository.source
        return HealthResponse(
            mode=source.kind,
            synthetic=source.kind is DatasetKind.SYNTHETIC_DEMO,
            public_sample=source.public_sample,
            source=source,
            checked_at=utc_now(),
        )

    @app.get(
        f"{_API_PREFIX}/meta",
        response_model=MetaResponse,
        tags=["system"],
    )
    def metadata(read_repository: RepositoryDependency) -> MetaResponse:
        return read_repository.metadata(application_version=__version__)

    @app.get(
        f"{_API_PREFIX}/dashboard",
        response_model=DashboardResponse,
        tags=["dashboard"],
    )
    def dashboard(
        read_repository: RepositoryDependency,
        mineral: Annotated[str | None, Query(min_length=1, max_length=128)] = None,
    ) -> DashboardResponse:
        return read_repository.dashboard(mineral=mineral)

    @app.get(
        f"{_API_PREFIX}/snapshot",
        response_model=SnapshotResponse,
        tags=["records"],
    )
    def snapshot(read_repository: RepositoryDependency) -> SnapshotResponse:
        """Transfer the complete bounded presentation sample in one request."""

        return read_repository.snapshot()

    @app.get(
        f"{_API_PREFIX}/records",
        response_model=RecordPage,
        tags=["records"],
    )
    def records(
        read_repository: RepositoryDependency,
        page: Annotated[int, Query(ge=1, le=10_000)] = 1,
        page_size: Annotated[int, Query(ge=1, le=50)] = 12,
        mineral: Annotated[str | None, Query(min_length=1, max_length=128)] = None,
        kind: ContentKind | None = None,
        sentiment: Sentiment | None = None,
        q: Annotated[str | None, Query(min_length=2, max_length=120)] = None,
        sort: RecordSort = RecordSort.NEWEST,
    ) -> RecordPage:
        return read_repository.list_records(
            page=page,
            page_size=page_size,
            mineral=mineral,
            kind=kind,
            sentiment=sentiment,
            query=q,
            sort=sort,
        )

    @app.get(
        f"{_API_PREFIX}/records/{{record_id}}",
        response_model=RecordDetail,
        tags=["records"],
        responses={404: {"model": ErrorResponse}},
    )
    def record_detail(
        read_repository: RepositoryDependency,
        record_id: Annotated[
            str,
            ApiPath(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_-]+$"),
        ],
    ) -> RecordDetail:
        record = read_repository.get_record(record_id)
        if record is None:
            raise HTTPException(status_code=404)
        return record

    @app.get(
        f"{_API_PREFIX}/runs",
        response_model=RunPage,
        tags=["runs"],
    )
    def runs(
        read_repository: RepositoryDependency,
        page: Annotated[int, Query(ge=1, le=10_000)] = 1,
        page_size: Annotated[int, Query(ge=1, le=50)] = 10,
        status: RunStatus | None = None,
        command: Annotated[str | None, Query(min_length=1, max_length=40)] = None,
    ) -> RunPage:
        return read_repository.list_runs(
            page=page,
            page_size=page_size,
            status=status,
            command=command,
        )

    @app.get(
        f"{_API_PREFIX}/config",
        response_model=UiConfigResponse,
        tags=["system"],
    )
    def ui_config(read_repository: RepositoryDependency) -> UiConfigResponse:
        base = read_repository.ui_config()
        capabilities = (
            selected_live_job_manager.capabilities()
            if selected_live_job_manager is not None
            else live_capabilities(selected_settings)
        )
        payload = base.model_dump()
        live_available = capabilities.enabled and bool(capabilities.credential_modes)
        payload["providers_enabled"] = live_available
        features = dict(payload["features"])
        features["live_collection"] = live_available
        features["mutation"] = live_available
        payload["features"] = features
        return UiConfigResponse.model_validate(payload)

    @app.get(
        f"{_API_PREFIX}/live/capabilities",
        response_model=LiveCapabilitiesResponse,
        tags=["live collection"],
    )
    def live_reddit_capabilities() -> LiveCapabilitiesResponse:
        if selected_live_job_manager is not None:
            return selected_live_job_manager.capabilities()
        return live_capabilities(selected_settings)

    @app.post(
        f"{_API_PREFIX}/live/jobs",
        response_model=LiveJobCreatedResponse,
        status_code=status.HTTP_202_ACCEPTED,
        tags=["live collection"],
        responses={
            401: {"model": ErrorResponse},
            409: {"model": ErrorResponse},
            413: {"model": ErrorResponse},
            422: {"model": ErrorResponse},
            429: {"model": ErrorResponse},
            503: {"model": ErrorResponse},
        },
    )
    def create_live_job(
        request: LiveJobCreateRequest,
        job_access_token: Annotated[
            str,
            Header(
                alias="X-Live-Job-Token",
                min_length=43,
                max_length=43,
                pattern=r"^[A-Za-z0-9_-]{43}$",
                include_in_schema=True,
            ),
        ],
        creation_access_token: Annotated[
            str | None,
            Header(
                alias="X-Live-Access-Token",
                min_length=1,
                max_length=MAX_LIVE_ACCESS_TOKEN_CHARS,
                include_in_schema=True,
            ),
        ] = None,
    ) -> LiveJobCreatedResponse:
        if selected_live_job_manager is None:
            raise LiveCollectionDisabledError
        job, access_token = selected_live_job_manager.create_job(
            request,
            creation_access_token=creation_access_token,
            job_access_token=job_access_token,
        )
        return LiveJobCreatedResponse(job=job, access_token=access_token)

    @app.get(
        f"{_API_PREFIX}/live/jobs/{{job_id}}",
        response_model=LiveJobView,
        tags=["live collection"],
        responses={404: {"model": ErrorResponse}},
    )
    def live_job(
        job_id: Annotated[str, ApiPath(pattern=r"^[0-9a-f]{32}$")],
        access_token: Annotated[
            str | None,
            Header(
                alias="X-Live-Job-Token",
                min_length=1,
                max_length=256,
                include_in_schema=True,
            ),
        ] = None,
    ) -> LiveJobView:
        if selected_live_job_manager is None:
            raise LiveJobNotFoundError
        return selected_live_job_manager.get_job(job_id, access_token)

    @app.delete(
        f"{_API_PREFIX}/live/jobs/{{job_id}}",
        response_model=LiveJobView,
        status_code=status.HTTP_202_ACCEPTED,
        tags=["live collection"],
        responses={404: {"model": ErrorResponse}},
    )
    def cancel_live_job(
        job_id: Annotated[str, ApiPath(pattern=r"^[0-9a-f]{32}$")],
        access_token: Annotated[
            str | None,
            Header(alias="X-Live-Job-Token", min_length=1, max_length=256),
        ] = None,
    ) -> LiveJobView:
        if selected_live_job_manager is None:
            raise LiveJobNotFoundError
        return selected_live_job_manager.cancel_job(job_id, access_token)

    @app.get(
        f"{_API_PREFIX}/live/jobs/{{job_id}}/snapshot",
        response_model=LiveSnapshotResponse,
        tags=["live collection"],
        responses={404: {"model": ErrorResponse}, 409: {"model": ErrorResponse}},
    )
    def live_job_snapshot(
        job_id: Annotated[str, ApiPath(pattern=r"^[0-9a-f]{32}$")],
        access_token: Annotated[
            str | None,
            Header(alias="X-Live-Job-Token", min_length=1, max_length=256),
        ] = None,
    ) -> LiveSnapshotResponse:
        if selected_live_job_manager is None:
            raise LiveJobNotFoundError
        return selected_live_job_manager.snapshot(job_id, access_token)

    resolved_assets = _resolve_asset_root(asset_root)
    index_file = resolved_assets / "index.html"
    if resolved_assets.is_dir() and index_file.is_file():
        _install_spa_routes(app, asset_root=resolved_assets, index_file=index_file)

    return app


def _snapshot_etag(repository: ReadRepository) -> str:
    """Return a weak validator for the exact immutable presentation snapshot."""

    payload = repository.snapshot().model_dump_json().encode("utf-8")
    return f'W/"{sha256(payload).hexdigest()}"'


def _resolve_asset_root(asset_root: Path | None) -> Path:
    """Resolve the explicit, environment, or working-directory SPA root once."""

    if asset_root is not None:
        selected = asset_root
    elif configured := os.environ.get(_ASSET_DIRECTORY_ENVIRONMENT):
        selected = Path(configured)
    else:
        selected = Path.cwd() / "web" / "dist"
    return selected.expanduser().resolve()


def _install_spa_routes(app: FastAPI, *, asset_root: Path, index_file: Path) -> None:
    """Serve one resolved SPA directory without intercepting any API route."""

    @app.get("/{spa_path:path}", include_in_schema=False)
    def spa(spa_path: str) -> FileResponse:
        if spa_path == "api" or spa_path.startswith("api/"):
            raise HTTPException(status_code=404)
        candidate = (asset_root / spa_path).resolve() if spa_path else index_file
        try:
            candidate.relative_to(asset_root)
        except ValueError:
            raise HTTPException(status_code=404) from None
        if candidate.is_file():
            return FileResponse(candidate)
        return FileResponse(index_file)


def _error_response(
    *,
    status_code: int,
    code: str,
    message: str,
    issues: tuple[ValidationIssue, ...] = (),
) -> JSONResponse:
    payload = ErrorResponse(code=code, message=message, issues=issues)
    return JSONResponse(status_code=status_code, content=payload.model_dump(mode="json"))


def _live_creation_guard_error(*, status_code: int, code: str, message: str) -> JSONResponse:
    response = _error_response(status_code=status_code, code=code, message=message)
    response.headers["Cache-Control"] = "no-store"
    return response
