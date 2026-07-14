"""FastAPI app for the local read-only provenance GUI."""

from __future__ import annotations

from pathlib import Path
import sqlite3
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from nipact.errors import ValidationError

from .models import (
    ApiError,
    ArtifactDetail,
    ArtifactGroupsResponse,
    ArtifactsResponse,
    ManifestDetail,
    ManifestsResponse,
    ObservedTopologyResponse,
    SummaryResponse,
    TraceGraphResponse,
    WorkflowsResponse,
)
from .project import GuiProject, resolve_gui_project
from .service import GuiApiError, GuiService


def create_gui_app(
    *,
    project_dir: Path,
    context: str,
    static_dir: Path | None = None,
) -> FastAPI:
    """Create the local GUI app for one project context."""
    return create_app(
        project=resolve_gui_project(project_dir=project_dir, context=context),
        static_dir=static_dir,
    )


def create_app(*, project: GuiProject, static_dir: Path | None = None) -> FastAPI:
    service = GuiService(project)
    app = FastAPI(title="NIPACT GUI", version="1")
    app.state.gui_service = service

    _install_error_handlers(app)
    _install_api_routes(app, service)
    _install_static_routes(app, static_dir=static_dir)
    return app


def _install_api_routes(app: FastAPI, service: GuiService) -> None:
    @app.get("/api/summary", response_model=SummaryResponse)
    def summary() -> SummaryResponse:
        return service.summary()

    @app.get("/api/workflows", response_model=WorkflowsResponse)
    def workflows() -> WorkflowsResponse:
        return service.workflows()

    @app.get("/api/manifests", response_model=ManifestsResponse)
    def manifests() -> ManifestsResponse:
        return service.manifests()

    @app.get("/api/manifests/{manifest_name}", response_model=ManifestDetail)
    def manifest(manifest_name: str) -> ManifestDetail:
        return service.manifest(manifest_name)

    @app.get("/api/artifacts", response_model=ArtifactsResponse)
    def artifacts(
        request: Request,
        origin: str | None = Query(default=None),
        workflow: str | None = Query(default=None),
        step: str | None = Query(default=None),
        output: str | None = Query(default=None),
        address: str | None = Query(default=None),
        is_selected_output: bool | None = Query(default=None),
        is_published: bool | None = Query(default=None),
    ) -> ArtifactsResponse:
        _reject_unsupported_query_params(
            request,
            allowed={
                "origin",
                "workflow",
                "step",
                "output",
                "address",
                "is_selected_output",
                "is_published",
            },
        )
        return service.artifacts(
            origin=origin,
            workflow_name=workflow,
            step_name=step,
            output_name=output,
            address=address,
            is_selected_output=is_selected_output,
            is_published=is_published,
        )

    @app.get("/api/artifacts/groups", response_model=ArtifactGroupsResponse)
    def artifact_groups(
        request: Request,
        origin: str | None = Query(default=None),
        workflow: str | None = Query(default=None),
        step: str | None = Query(default=None),
        output: str | None = Query(default=None),
        address: str | None = Query(default=None),
        is_selected_output: bool | None = Query(default=None),
        is_published: bool | None = Query(default=None),
    ) -> ArtifactGroupsResponse:
        _reject_unsupported_query_params(
            request,
            allowed={
                "origin",
                "workflow",
                "step",
                "output",
                "address",
                "is_selected_output",
                "is_published",
            },
        )
        return service.artifact_groups(
            origin=origin,
            workflow_name=workflow,
            step_name=step,
            output_name=output,
            address=address,
            is_selected_output=is_selected_output,
            is_published=is_published,
        )

    @app.get("/api/artifacts/resolve", response_model=ArtifactDetail)
    def resolve_artifact(path: str = Query()) -> ArtifactDetail:
        return service.resolve_artifact_path(path)

    @app.get("/api/artifacts/{artifact_id}", response_model=ArtifactDetail)
    def artifact(artifact_id: int) -> ArtifactDetail:
        return service.artifact(artifact_id)

    @app.get("/api/artifacts/{artifact_id}/lineage", response_model=TraceGraphResponse)
    def artifact_lineage(artifact_id: int) -> TraceGraphResponse:
        return service.lineage(artifact_id)

    @app.get(
        "/api/artifacts/{artifact_id}/topology",
        response_model=ObservedTopologyResponse,
    )
    def artifact_topology(artifact_id: int) -> ObservedTopologyResponse:
        return service.topology(artifact_id)


def _install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(GuiApiError)
    async def gui_error_handler(_request: object, exc: GuiApiError) -> JSONResponse:
        return _api_error(
            exc.status_code,
            code=exc.code,
            message=exc.message,
            details=exc.details,
        )

    @app.exception_handler(RequestValidationError)
    async def request_error_handler(
        _request: object,
        exc: RequestValidationError,
    ) -> JSONResponse:
        return _api_error(
            422,
            code="invalid_request",
            message="invalid API request",
            details={"errors": exc.errors()},
        )

    @app.exception_handler(ValidationError)
    async def validation_error_handler(
        _request: object,
        exc: ValidationError,
    ) -> JSONResponse:
        if "database is locked" in str(exc):
            return _api_error(503, code="database_locked", message="database is locked")
        return _api_error(422, code="validation_error", message=str(exc))

    @app.exception_handler(sqlite3.OperationalError)
    async def sqlite_error_handler(
        _request: object,
        exc: sqlite3.OperationalError,
    ) -> JSONResponse:
        if "database is locked" in str(exc):
            return _api_error(503, code="database_locked", message="database is locked")
        return _api_error(500, code="server_error", message="unexpected server error")

    @app.exception_handler(Exception)
    async def unexpected_error_handler(_request: object, _exc: Exception) -> JSONResponse:
        return _api_error(500, code="server_error", message="unexpected server error")


def _install_static_routes(app: FastAPI, *, static_dir: Path | None) -> None:
    root = _usable_static_dir(static_dir)
    if root is not None and (root / "assets").is_dir():
        app.mount("/assets", StaticFiles(directory=root / "assets"), name="assets")

    @app.get("/", include_in_schema=False, response_model=None)
    def index() -> Any:
        if root is None:
            return _api_error(
                503,
                code="frontend_unavailable",
                message="GUI frontend assets are not installed",
            )
        return FileResponse(root / "index.html")

    @app.get("/{full_path:path}", include_in_schema=False, response_model=None)
    def deep_link(full_path: str) -> Any:
        if full_path.startswith("api/"):
            return _api_error(404, code="not_found", message="API route not found")
        if root is None:
            return _api_error(
                503,
                code="frontend_unavailable",
                message="GUI frontend assets are not installed",
            )
        return FileResponse(root / "index.html")


def _usable_static_dir(static_dir: Path | None) -> Path | None:
    if static_dir is None:
        root = Path(__file__).with_name("static")
    else:
        root = static_dir.expanduser().resolve()
    if not (root / "index.html").is_file():
        return None
    return root


def _api_error(
    status_code: int,
    *,
    code: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content=ApiError(
            code=code,
            message=message,
            details=details,
        ).model_dump(exclude_none=True),
    )


def _reject_unsupported_query_params(
    request: Request,
    *,
    allowed: set[str],
) -> None:
    unsupported = sorted(set(request.query_params.keys()) - allowed)
    if unsupported:
        raise GuiApiError(
            422,
            "unsupported_filter",
            "unsupported artifact filter",
            {"filters": unsupported},
        )
