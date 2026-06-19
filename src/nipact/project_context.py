"""Shared project context resolution for read-only command surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .errors import ValidationError
from .identity import validate_path_token
from .registry import REGISTRY_DB_PATH, read_context_runtime_path


@dataclass(frozen=True)
class ResolvedProjectContext:
    project_root: Path
    runtime_root: Path
    registry_path: Path
    context: str


def resolve_project_context(*, project_dir: Path, context: str) -> ResolvedProjectContext:
    """Resolve project/runtime paths and verify registry context binding."""
    context = validate_path_token(context, label="context")
    project_root = project_dir.expanduser().resolve()
    if not project_root.is_dir():
        raise ValidationError(f"project dir does not exist: {project_dir}")

    config_path = project_root / "nipact.yaml"
    config = _read_project_config(config_path)
    if config.get("context") != context:
        raise ValidationError(f"context mismatch in nipact.yaml: expected {context!r}")

    runtime_root = _runtime_root(project_root=project_root, config=config)
    if not runtime_root.is_dir():
        raise ValidationError(f"runtime dir does not exist: {runtime_root}")

    registry_path = runtime_root / REGISTRY_DB_PATH
    registered_runtime_path = read_context_runtime_path(
        registry_path,
        context=context,
    )
    if registered_runtime_path != str(runtime_root):
        raise ValidationError("registry.db context runtime path is out of date")

    return ResolvedProjectContext(
        project_root=project_root,
        runtime_root=runtime_root,
        registry_path=registry_path,
        context=context,
    )


def _read_project_config(config_path: Path) -> dict[str, Any]:
    try:
        payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValidationError(f"missing project config: {config_path}") from exc
    except OSError as exc:
        raise ValidationError(f"could not read project config: {config_path}") from exc
    except yaml.YAMLError as exc:
        raise ValidationError(f"invalid YAML file {config_path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValidationError(f"project config must contain a mapping: {config_path}")
    return payload


def _runtime_root(*, project_root: Path, config: dict[str, Any]) -> Path:
    paths = config.get("paths")
    if not isinstance(paths, dict):
        raise ValidationError("nipact.yaml missing paths.runtime")
    raw_runtime = paths.get("runtime")
    if not isinstance(raw_runtime, str) or not raw_runtime:
        raise ValidationError("nipact.yaml missing paths.runtime")
    runtime_root = Path(raw_runtime).expanduser()
    if not runtime_root.is_absolute():
        runtime_root = project_root / runtime_root
    return runtime_root.resolve()
