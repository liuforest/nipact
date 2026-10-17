"""Minimal project setup helpers for packaged demo projects."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from .examples.colors_processing_demo import project_template as colors_template
from .examples.directory_outputs_demo import project_template as directories_template
from .examples.dynamic_functional_connectivity_demo import (
    project_template as dfc_template,
)
from .examples.fmri_preprocessing_demo import project_template as fmri_template
from .errors import ValidationError
from .identity import validate_path_token
from .manifest import Manifest
from . import registry

if TYPE_CHECKING:
    from .workflow import DeclarationRecord


_PROJECT_DIRECTORIES = ("manifests", "steps", "workflows")
_RUNTIME_DIRECTORIES = ("data", "database", "outputs", "manifests/generated")
_PREPARED_DEMO_TEMPLATES = {
    colors_template.SUPPORTED_DEMO: colors_template,
    fmri_template.SUPPORTED_DEMO: fmri_template,
    dfc_template.SUPPORTED_DEMO: dfc_template,
    directories_template.SUPPORTED_DEMO: directories_template,
}
_SUPPORTED_DEMOS = (
    colors_template.SUPPORTED_DEMO,
    fmri_template.SUPPORTED_DEMO,
    dfc_template.SUPPORTED_DEMO,
    directories_template.SUPPORTED_DEMO,
)


class ProjectSetupError(RuntimeError):
    """Raised for concise CLI-facing project setup failures."""


@dataclass(frozen=True)
class InitResult:
    project_root: Path
    runtime_root: Path
    context: str
    demo: str
    source_index: str
    manifest_count: int
    source_file_count: int


@dataclass(frozen=True)
class ValidateResult:
    project_root: Path
    runtime_root: Path
    context: str
    declarations: dict[tuple[str, str], tuple[DeclarationRecord, ...]]
    registry_initialized: bool
    registry_error: str | None
    published_outputs: int | None

    @property
    def success(self) -> bool:
        return self.registry_error is None and all(
            record.status == "valid"
            for records in self.declarations.values()
            for record in records
        )


def init_project(
    *,
    demo: str,
    project_dir: Path,
    runtime_dir: Path,
    context: str | None,
) -> InitResult:
    template = _prepared_demo_template(demo)
    if template is None:
        supported = ", ".join(_SUPPORTED_DEMOS)
        raise ProjectSetupError(f"unknown demo {demo!r}; expected one of: {supported}")
    return _init_prepared_demo_project(
        template=template,
        demo=demo,
        project_dir=project_dir,
        runtime_dir=runtime_dir,
        context=context,
    )


def _init_prepared_demo_project(
    *,
    template: Any,
    demo: str,
    project_dir: Path,
    runtime_dir: Path,
    context: str | None,
) -> InitResult:
    context = _validate_context(demo if context is None else context)

    project_root = project_dir.expanduser().resolve()
    runtime_root = runtime_dir.expanduser().resolve()
    _validate_project_runtime_roots(project_root, runtime_root)

    _require_empty_target(project_root, "project dir")
    _require_empty_target(runtime_root, "runtime dir")

    _create_project_runtime_dirs(project_root, runtime_root)
    template.write_runtime_sources(runtime_root)

    manifests = template.build_manifests()
    _write_project_files(
        template=template,
        project_root=project_root,
        runtime_root=runtime_root,
        runtime_arg=runtime_dir,
        context=context,
        manifests=manifests,
    )

    registry.initialize_registry_db(
        runtime_root / registry.REGISTRY_DB_PATH,
        context=context,
        runtime_root=runtime_root,
        manifests=manifests,
        manifest_paths=template.manifest_paths(),
    )

    return InitResult(
        project_root=project_root,
        runtime_root=runtime_root,
        context=context,
        demo=demo,
        source_index=template.SOURCE_INDEX_PATH,
        manifest_count=len(manifests),
        source_file_count=len(template.source_file_paths()),
    )


def validate_project(
    *, project_dir: Path, context: str, verify_outputs: bool = False
) -> ValidateResult:
    loaded_workflow_project = _load_workflow_project(project_dir, context=context)
    project_root = loaded_workflow_project.project_root
    runtime_root = loaded_workflow_project.runtime_root
    context = loaded_workflow_project.context
    if not runtime_root.is_dir():
        raise ProjectSetupError(f"runtime dir does not exist: {runtime_root}")
    _validate_project_runtime_roots(project_root, runtime_root)

    published_outputs = None
    registry_error = None
    registry_db_path = runtime_root / registry.REGISTRY_DB_PATH
    if registry_db_path.exists():
        try:
            registry_counts = registry.validate_prepared_registry_db(
                registry_db_path,
                context=context,
                runtime_root=runtime_root,
                loaded_workflow_project=loaded_workflow_project,
                verify_outputs=verify_outputs,
            )
        except ValidationError as exc:
            registry_error = str(exc)
        else:
            published_outputs = registry_counts["published_outputs"]
    return ValidateResult(
        project_root=project_root,
        runtime_root=runtime_root,
        context=context,
        declarations=dict(
            sorted(
                loaded_workflow_project.declarations.items(),
                key=lambda item: (
                    {"step": 0, "workflow": 1, "manifest": 2}[item[0][0]],
                    item[0][1],
                ),
            )
        ),
        registry_initialized=registry_db_path.exists(),
        registry_error=registry_error,
        published_outputs=published_outputs,
    )


def _load_workflow_project(project_root: Path, *, context: str) -> Any:
    try:
        from .workflow import load_workflow_project

        return load_workflow_project(
            project_dir=project_root,
            context=context,
        )
    except ValidationError as exc:
        raise ProjectSetupError(str(exc)) from exc


def _prepared_demo_template(demo: str) -> Any | None:
    return _PREPARED_DEMO_TEMPLATES.get(demo)


def _require_empty_target(path: Path, label: str) -> None:
    if path.exists() and not path.is_dir():
        raise ProjectSetupError(f"{label} exists and is not a directory: {path}")
    if path.is_dir() and any(path.iterdir()):
        raise ProjectSetupError(f"{label} must be empty: {path}")


def _validate_context(context: str) -> str:
    try:
        return validate_path_token(context, label="context")
    except ValidationError as exc:
        raise ProjectSetupError(str(exc)) from exc


def _validate_project_runtime_roots(project_root: Path, runtime_root: Path) -> None:
    if project_root == runtime_root:
        raise ProjectSetupError("project dir and runtime dir must be different")
    if _path_contains_or_same(project_root, runtime_root) or _path_contains_or_same(
        runtime_root,
        project_root,
    ):
        raise ProjectSetupError("project dir and runtime dir must not contain each other")


def _create_project_runtime_dirs(project_root: Path, runtime_root: Path) -> None:
    project_root.mkdir(parents=True, exist_ok=True)
    runtime_root.mkdir(parents=True, exist_ok=True)
    for relative_dir in _PROJECT_DIRECTORIES:
        (project_root / relative_dir).mkdir()
    for relative_dir in _RUNTIME_DIRECTORIES:
        (runtime_root / relative_dir).mkdir(parents=True)


def _write_project_files(
    *,
    template: Any,
    project_root: Path,
    runtime_root: Path,
    runtime_arg: Path,
    context: str,
    manifests: dict[str, Manifest],
) -> None:
    manifest_paths = template.manifest_paths()
    config = template.project_config(
        context=context,
        runtime=_runtime_config_value(project_root, runtime_root, runtime_arg),
    )
    _write_yaml(project_root / "nipact.yaml", config)
    _write_yaml(
        project_root / template.SOURCE_INDEX_PATH,
        template.source_index_payload(),
    )
    for name, manifest in manifests.items():
        _write_yaml(
            project_root / manifest_paths[name],
            {
                "description": manifest.description,
                "entities": list(manifest.entity_ids),
            },
        )
    for step_name, payload in template.step_files().items():
        _write_yaml(project_root / f"steps/{step_name}.yaml", payload)
    for workflow_name, payload in template.workflow_files().items():
        _write_yaml(project_root / f"workflows/{workflow_name}.yaml", payload)
    (project_root / "README.md").write_text(
        template.project_readme_text(),
        encoding="utf-8",
    )


def _runtime_config_value(project_root: Path, runtime_root: Path, runtime_arg: Path) -> str:
    if runtime_arg.expanduser().is_absolute():
        return str(runtime_root)
    try:
        return os.path.relpath(runtime_root, project_root)
    except ValueError:
        return str(runtime_root)


def _path_contains_or_same(parent: Path, child: Path) -> bool:
    try:
        child.relative_to(parent)
    except ValueError:
        return False
    return True


def _write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
