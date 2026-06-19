"""Minimal project setup helpers for packaged demo projects."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import Any

import yaml

from .examples.colors_processing_demo import project_template as colors_template
from .examples.dynamic_functional_connectivity_demo import (
    project_template as dfc_template,
)
from .examples.fmri_preprocessing_demo import project_template as fmri_template
from .errors import ValidationError
from .hashing import sha256_digest, short_hash
from .identity import validate_path_token
from .manifest import Manifest, load_manifest
from . import registry


_PROJECT_DIRECTORIES = ("manifests", "steps", "workflows")
_RUNTIME_DIRECTORIES = ("data", "database", "outputs", "manifests/generated")
_PREPARED_DEMO_TEMPLATES = {
    fmri_template.SUPPORTED_DEMO: fmri_template,
    dfc_template.SUPPORTED_DEMO: dfc_template,
}
_SUPPORTED_DEMOS = (
    colors_template.SUPPORTED_DEMO,
    fmri_template.SUPPORTED_DEMO,
    dfc_template.SUPPORTED_DEMO,
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
    source_hash: str | None = None
    manifest_hash: str | None = None


@dataclass(frozen=True)
class ValidateResult:
    project_root: Path
    runtime_root: Path
    context: str
    manifest_count: int
    workflow_count: int
    step_count: int
    source_entities: int
    published_outputs: int


def init_project(
    *,
    demo: str,
    project_dir: Path,
    runtime_dir: Path,
    context: str | None,
) -> InitResult:
    if demo == colors_template.SUPPORTED_DEMO:
        return _init_colors_project(
            demo=demo,
            project_dir=project_dir,
            runtime_dir=runtime_dir,
            context=context,
        )
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


def _init_colors_project(
    *,
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

    source_payload = colors_template.source_payload()
    source_digest = _json_digest(source_payload)
    source_hash = short_hash(source_digest)
    source_path = runtime_root / colors_template.SOURCE_ARTIFACT_PATH
    _write_json(source_path, source_payload)

    manifests = colors_template.build_manifests()
    _write_project_files(
        template=colors_template,
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
        source_artifact_path=colors_template.SOURCE_ARTIFACT_PATH,
        source_entity_count=colors_template.SOURCE_ENTITY_COUNT,
        source_digest=source_digest,
        source_hash=source_hash,
        manifests=manifests,
        manifest_paths=colors_template.manifest_paths(),
    )

    return InitResult(
        project_root=project_root,
        runtime_root=runtime_root,
        context=context,
        demo=demo,
        source_index=colors_template.SOURCE_INDEX_PATH,
        manifest_count=len(manifests),
        source_file_count=1,
        source_hash=source_hash,
        manifest_hash=manifests[colors_template.ANALYSIS_COHORT_NAME].manifest_hash,
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

    registry.initialize_prepared_demo_registry_db(
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


def validate_project(*, project_dir: Path, context: str) -> ValidateResult:
    context = _validate_context(context)
    project_root = project_dir.expanduser().resolve()
    if not project_root.is_dir():
        raise ProjectSetupError(f"project dir does not exist: {project_dir}")

    config = _read_yaml_mapping(project_root / "nipact.yaml")
    if config.get("context") != context:
        raise ProjectSetupError(f"context mismatch in nipact.yaml: expected {context!r}")

    runtime_root = _resolve_runtime_root(project_root, config)
    if not runtime_root.is_dir():
        raise ProjectSetupError(f"runtime dir does not exist: {runtime_root}")
    _validate_project_runtime_roots(project_root, runtime_root)

    if _is_colors_template_candidate(config):
        return _validate_colors_project(
            project_root=project_root,
            runtime_root=runtime_root,
            context=context,
            config=config,
        )
    return _validate_generic_prepared_project(
        project_root=project_root,
        runtime_root=runtime_root,
        context=context,
    )


def _validate_colors_project(
    *,
    project_root: Path,
    runtime_root: Path,
    context: str,
    config: dict[str, Any],
) -> ValidateResult:
    manifest_paths, step_dir, workflow_paths = _validate_project_config(
        project_root,
        config,
    )
    manifests = _validate_manifest_files(manifest_paths)
    _validate_yaml_directory(
        step_dir,
        expected_names=set(colors_template.step_files()),
    )
    _validate_yaml_files(
        workflow_paths,
        expected_names=set(colors_template.workflow_files()),
        label="workflow reference",
    )
    loaded_workflow_project = _load_workflow_project(project_root, context=context)
    step_count = len(loaded_workflow_project.steps)
    workflow_count = len(loaded_workflow_project.workflows)
    _validate_colors_source_declarations(loaded_workflow_project)

    source_path = _resolve_runtime_file(
        runtime_root,
        colors_template.SOURCE_ARTIFACT_PATH,
        "source data",
    )
    registry_db_path = _resolve_runtime_file(
        runtime_root,
        registry.REGISTRY_DB_PATH,
        "registry database",
    )

    source_payload = _read_json_mapping(source_path)
    try:
        source_entities = colors_template.validate_source_payload(
            source_payload,
            manifests=manifests,
        )
    except ValidationError as exc:
        raise ProjectSetupError(str(exc)) from exc
    source_digest = _json_digest(source_payload)
    source_hash = short_hash(source_digest)
    if source_digest != _json_digest(colors_template.source_payload()):
        raise ProjectSetupError("source data content does not match colors demo")

    try:
        registry_counts = registry.validate_registry_db(
            registry_db_path,
            project_root=project_root,
            runtime_root=runtime_root,
            manifest_paths=manifest_paths,
            context=context,
            source_artifact_path=colors_template.SOURCE_ARTIFACT_PATH,
            source_entity_count=colors_template.SOURCE_ENTITY_COUNT,
            source_digest=source_digest,
            source_hash=source_hash,
            manifests=manifests,
            loaded_workflow_project=loaded_workflow_project,
        )
    except ValidationError as exc:
        raise ProjectSetupError(str(exc)) from exc
    if registry_counts["manifests"] != len(manifests):
        raise ProjectSetupError("registry manifest row count mismatch")

    return ValidateResult(
        project_root=project_root,
        runtime_root=runtime_root,
        context=context,
        manifest_count=len(manifests),
        workflow_count=workflow_count,
        step_count=step_count,
        source_entities=source_entities,
        published_outputs=registry_counts["published_outputs"],
    )


def _validate_generic_prepared_project(
    *,
    project_root: Path,
    runtime_root: Path,
    context: str,
) -> ValidateResult:
    loaded_workflow_project = _load_workflow_project(project_root, context=context)
    published_outputs = 0
    registry_db_path = runtime_root / registry.REGISTRY_DB_PATH
    if registry_db_path.exists():
        try:
            registry_counts = registry.validate_prepared_registry_db(
                registry_db_path,
                context=context,
                runtime_root=runtime_root,
            )
        except ValidationError as exc:
            raise ProjectSetupError(str(exc)) from exc
        published_outputs = registry_counts["published_outputs"]
    return ValidateResult(
        project_root=project_root,
        runtime_root=runtime_root,
        context=context,
        manifest_count=len(loaded_workflow_project.manifests),
        workflow_count=len(loaded_workflow_project.workflows),
        step_count=len(loaded_workflow_project.steps),
        source_entities=0,
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


def _is_colors_template_candidate(config: Mapping[str, Any]) -> bool:
    manifests_config = config.get("manifests")
    workflows_config = config.get("workflows")
    steps_config = config.get("steps")
    if not isinstance(manifests_config, Mapping):
        return False
    if not isinstance(workflows_config, Mapping):
        return False
    if not isinstance(steps_config, Mapping):
        return False
    if steps_config.get("directory") != "steps":
        return False
    return _contains_expected_paths(
        manifests_config,
        colors_template.manifest_paths(),
    ) and _contains_expected_paths(
        workflows_config,
        {name: f"workflows/{name}.yaml" for name in colors_template.workflow_files()},
    )


def _contains_expected_paths(
    config: Mapping[str, Any],
    expected: Mapping[str, str],
) -> bool:
    return all(config.get(name) == path for name, path in expected.items())


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


def _validate_project_config(
    project_root: Path,
    config: dict[str, Any],
) -> tuple[dict[str, Path], Path, dict[str, Path]]:
    manifests_config = config.get("manifests")
    if not isinstance(manifests_config, dict):
        raise ProjectSetupError("nipact.yaml missing manifests")
    manifest_paths: dict[str, Path] = {}
    expected_manifest_paths = colors_template.manifest_paths()
    extra_manifests = sorted(set(manifests_config) - set(expected_manifest_paths))
    if extra_manifests:
        raise ProjectSetupError(
            "nipact.yaml contains unexpected manifest reference(s): "
            + ", ".join(extra_manifests)
        )
    for name in sorted(expected_manifest_paths):
        entry = manifests_config.get(name)
        if not isinstance(entry, str) or not entry:
            raise ProjectSetupError(f"nipact.yaml missing manifest reference: {name}")
        manifest_paths[name] = _resolve_project_file(
            project_root,
            entry,
            f"manifest reference {name!r}",
        )

    steps_config = config.get("steps")
    if (
        not isinstance(steps_config, dict)
        or not isinstance(steps_config.get("directory"), str)
        or not steps_config["directory"]
    ):
        raise ProjectSetupError("nipact.yaml missing steps.directory")
    step_dir = _resolve_project_file(
        project_root,
        steps_config["directory"],
        "steps.directory",
    )

    workflows_config = config.get("workflows")
    if not isinstance(workflows_config, dict):
        raise ProjectSetupError("nipact.yaml missing workflows")
    expected_workflow_files = colors_template.workflow_files()
    extra_workflows = sorted(set(workflows_config) - set(expected_workflow_files))
    if extra_workflows:
        raise ProjectSetupError(
            "nipact.yaml contains unexpected workflow reference(s): "
            + ", ".join(extra_workflows)
        )
    workflow_paths: dict[str, Path] = {}
    for name in sorted(expected_workflow_files):
        raw_path = workflows_config.get(name)
        if not isinstance(raw_path, str) or not raw_path:
            raise ProjectSetupError(f"nipact.yaml missing workflow reference: {name}")
        workflow_paths[name] = _resolve_project_file(
            project_root,
            raw_path,
            f"workflow reference {name!r}",
        )
    return manifest_paths, step_dir, workflow_paths


def _validate_colors_source_declarations(loaded_workflow_project: Any) -> None:
    expected_global = {
        colors_template.SOURCE_BINDING_NAME: colors_template.SOURCE_ARTIFACT_PATH,
    }
    if loaded_workflow_project.source_index.global_bindings != expected_global:
        raise ProjectSetupError("colors source declarations do not match expected source index")
    if loaded_workflow_project.source_index.entity_bindings != {}:
        raise ProjectSetupError("colors source declarations must not include entity bindings")
    try:
        source_step = loaded_workflow_project.steps["color_source"]
    except KeyError as exc:
        raise ProjectSetupError(
            "colors source declarations are missing color_source step"
        ) from exc
    if source_step.source_inputs != (colors_template.SOURCE_BINDING_NAME,):
        raise ProjectSetupError(
            "colors source declarations do not match color_source source_inputs"
        )


def _resolve_project_file(project_root: Path, raw_path: Any, label: str) -> Path:
    if not isinstance(raw_path, str):
        raise ProjectSetupError(f"{label} must be a string")
    relative_path = Path(raw_path).expanduser()
    if relative_path.is_absolute():
        raise ProjectSetupError(f"{label} must be relative to project dir")
    resolved = (project_root / relative_path).resolve()
    if not _path_contains_or_same(project_root, resolved):
        raise ProjectSetupError(f"{label} must stay inside project dir")
    return resolved


def _path_contains_or_same(parent: Path, child: Path) -> bool:
    try:
        child.relative_to(parent)
    except ValueError:
        return False
    return True


def _validate_manifest_files(manifest_paths: dict[str, Path]) -> dict[str, Manifest]:
    manifests: dict[str, Manifest] = {}
    for name in sorted(colors_template.manifest_paths()):
        try:
            manifests[name] = load_manifest(manifest_paths[name])
        except ValidationError as exc:
            raise ProjectSetupError(str(exc)) from exc
    return manifests


def _validate_yaml_directory(path: Path, *, expected_names: set[str]) -> int:
    if not path.is_dir():
        raise ProjectSetupError(f"missing directory: {path}")
    files = {file.stem: file for file in path.glob("*.yaml")}
    missing = sorted(expected_names - set(files))
    if missing:
        raise ProjectSetupError(f"missing YAML files in {path.name}: {', '.join(missing)}")
    extra = sorted(set(files) - expected_names)
    if extra:
        raise ProjectSetupError(f"unexpected YAML files in {path.name}: {', '.join(extra)}")
    for file in files.values():
        _read_yaml_mapping(file)
    return len(files)


def _validate_yaml_files(
    paths: dict[str, Path],
    *,
    expected_names: set[str],
    label: str,
) -> int:
    missing = sorted(expected_names - set(paths))
    if missing:
        raise ProjectSetupError(f"missing configured YAML files: {', '.join(missing)}")
    for name in expected_names:
        if not paths[name].is_file():
            raise ProjectSetupError(f"{label} {name!r} does not exist: {paths[name]}")
        _read_yaml_mapping(paths[name])
    return len(paths)


def _resolve_runtime_file(runtime_root: Path, relative_path: str, label: str) -> Path:
    resolved = (runtime_root / relative_path).resolve()
    if not _path_contains_or_same(runtime_root, resolved):
        raise ProjectSetupError(f"{label} must stay inside runtime dir")
    return resolved


def _resolve_runtime_root(project_root: Path, config: dict[str, Any]) -> Path:
    paths = config.get("paths")
    if not isinstance(paths, dict) or not isinstance(paths.get("runtime"), str) or not paths["runtime"]:
        raise ProjectSetupError("nipact.yaml missing paths.runtime")
    runtime_path = Path(paths["runtime"]).expanduser()
    if runtime_path.is_absolute():
        return runtime_path.resolve()
    return (project_root / runtime_path).resolve()


def _read_yaml_mapping(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ProjectSetupError(f"missing YAML file: {path}")
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ProjectSetupError(f"invalid YAML file {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ProjectSetupError(f"YAML file must contain a mapping: {path}")
    return payload


def _read_json_mapping(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ProjectSetupError(f"missing JSON file: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ProjectSetupError(f"invalid JSON file {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ProjectSetupError(f"JSON file must contain an object: {path}")
    return payload


def _write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _json_digest(payload: Any) -> str:
    return sha256_digest(_canonical_json(payload).encode("utf-8"))


def _canonical_json(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
