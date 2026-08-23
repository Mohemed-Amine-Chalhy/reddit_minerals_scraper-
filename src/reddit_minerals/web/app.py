"""Import-safe FastAPI application factory for the read-only portfolio API."""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from hashlib import sha256
from pathlib import Path
from typing import Annotated, cast

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi import Path as ApiPath
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.responses import Response

from reddit_minerals import __version__
from reddit_minerals.models import ContentKind, Sentiment, utc_now
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


def _get_repository(request: Request) -> ReadRepository:
    return cast(ReadRepository, request.app.state.read_repository)


RepositoryDependency = Annotated[ReadRepository, Depends(_get_repository)]


def create_app(
    *,
    repository: ReadRepository | None = None,
    asset_root: Path | None = None,
) -> FastAPI:
    """Create the API without provider, credential, network, or database side effects."""

    app = FastAPI(
        title="MineralLens API",
        summary="Read-only API over a curated public critical-minerals research sample.",
        description=(
            "This interface exposes a bounded sample of public derived research metadata. "
            "It omits source text and authors, does not construct Reddit or AI provider "
            "clients, and cannot start live pipeline operations."
        ),
        version=__version__,
        openapi_url=f"{_API_PREFIX}/openapi.json",
        docs_url=f"{_API_PREFIX}/docs",
        redoc_url=None,
    )
    app.state.read_repository = (
        repository if repository is not None else KaggleSampleReadRepository()
    )
    snapshot_etag = _snapshot_etag(app.state.read_repository)
    app.add_middleware(GZipMiddleware, minimum_size=1_000, compresslevel=6)

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
        return read_repository.ui_config()

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
