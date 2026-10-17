"""Workspace-local context index helpers."""

from __future__ import annotations

import os
from pathlib import Path

import yaml

from .errors import ValidationError
from .identity import validate_path_token

CONTEXT_INDEX_FILENAME = "nipact.contexts.yaml"


def resolve_project(
    *,
    project_dir: Path | None,
    context: str | None,
    cwd: Path | None = None,
) -> tuple[Path, str]:
    """Select a project root and its declared context without loading runtime state."""
    if context is not None:
        context = validate_path_token(context, label="context")
    if project_dir is None:
        start = (cwd or Path.cwd()).expanduser().resolve()
        for directory in (start, *start.parents):
            index_path = directory / CONTEXT_INDEX_FILENAME
            if index_path.exists():
                entries = _read_index(index_path)
                if context is None:
                    if len(entries) != 1:
                        selection_help = (
                            "pass --context to select one"
                            if entries
                            else "add an index entry or pass --project-dir"
                        )
                        raise ValidationError(
                            f"{index_path} has {len(entries)} contexts; {selection_help}"
                        )
                    context = next(iter(entries))
                if context not in entries:
                    raise ValidationError(
                        f"context {context!r} is not registered in {index_path}"
                    )
                project_dir = _resolve_index_project_dir(
                    workspace_root=directory,
                    raw_project_dir=entries[context],
                )
                break
            if (directory / "nipact.yaml").exists():
                project_dir = directory
                break
        else:
            raise ValidationError(
                "could not resolve project dir; no nipact.contexts.yaml or "
                "nipact.yaml found in the current directory or its parents; "
                "pass --project-dir"
            )

    project_root = project_dir.expanduser().resolve()
    if not project_root.is_dir():
        raise ValidationError(f"project dir does not exist: {project_root}")
    declared_context = _read_project_context(project_root / "nipact.yaml")
    if context is not None and context != declared_context:
        raise ValidationError(f"context mismatch in nipact.yaml: expected {context!r}")
    return project_root, declared_context


def _read_project_context(config_path: Path) -> str:
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
    return validate_path_token(payload.get("context"), label="context")


def preflight_context_index_update(
    *,
    workspace_dir: Path,
    context: str,
    project_dir: Path,
) -> None:
    """Reject index conflicts before init writes project/runtime files."""
    context = validate_path_token(context, label="context")
    workspace_root = workspace_dir.expanduser().resolve()
    index_path = workspace_root / CONTEXT_INDEX_FILENAME
    if not index_path.exists():
        return
    entries = _read_index(index_path)
    raw_project_dir = entries.get(context)
    if raw_project_dir is None:
        return
    existing_project_dir = _resolve_index_project_dir(
        workspace_root=workspace_root,
        raw_project_dir=raw_project_dir,
    )
    requested_project_dir = _resolve_project_dir_for_index(
        workspace_root=workspace_root,
        project_dir=project_dir,
    )
    if existing_project_dir != requested_project_dir:
        raise ValidationError(
            f"context {context!r} already points to {raw_project_dir!r} in "
            f"{CONTEXT_INDEX_FILENAME}; edit the file manually before reusing "
            "that context"
        )


def update_context_index(
    *,
    workspace_dir: Path,
    context: str,
    project_dir: Path,
) -> Path:
    """Create or update the workspace-local context index."""
    context = validate_path_token(context, label="context")
    workspace_root = workspace_dir.expanduser().resolve()
    index_path = workspace_root / CONTEXT_INDEX_FILENAME
    entries = _read_index(index_path) if index_path.exists() else {}

    raw_project_dir = entries.get(context)
    if raw_project_dir is not None:
        existing_project_dir = _resolve_index_project_dir(
            workspace_root=workspace_root,
            raw_project_dir=raw_project_dir,
        )
        requested_project_dir = _resolve_project_dir_for_index(
            workspace_root=workspace_root,
            project_dir=project_dir,
        )
        if existing_project_dir != requested_project_dir:
            raise ValidationError(
                f"context {context!r} already points to {raw_project_dir!r} in "
                f"{CONTEXT_INDEX_FILENAME}; edit the file manually before "
                "reusing that context"
            )

    entries[context] = _project_dir_index_value(
        workspace_root=workspace_root,
        project_dir=project_dir,
    )
    payload = {
        "contexts": {
            name: {"project_dir": entries[name]} for name in sorted(entries)
        }
    }
    temp_path = index_path.with_name(f".{index_path.name}.tmp")
    try:
        temp_path.write_text(
            yaml.safe_dump(payload, sort_keys=False),
            encoding="utf-8",
        )
        temp_path.replace(index_path)
    except OSError as exc:
        raise ValidationError(f"could not write context index: {index_path}") from exc
    return index_path


def _read_index(index_path: Path) -> dict[str, str]:
    if not index_path.is_file():
        raise ValidationError(f"context index is not a file: {index_path}")
    try:
        payload = yaml.safe_load(index_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ValidationError(f"could not read context index: {index_path}") from exc
    except yaml.YAMLError as exc:
        raise ValidationError(f"invalid YAML file {index_path}: {exc}") from exc
    if payload is None:
        payload = {}
    if not isinstance(payload, dict):
        raise ValidationError(f"context index must contain a mapping: {index_path}")
    if set(payload) - {"contexts"}:
        raise ValidationError(
            f"context index supports only a top-level 'contexts' mapping: {index_path}"
        )
    contexts = payload.get("contexts", {})
    if contexts is None:
        contexts = {}
    if not isinstance(contexts, dict):
        raise ValidationError(f"contexts must be a mapping in {index_path}")

    entries: dict[str, str] = {}
    for raw_context, entry in contexts.items():
        context = validate_path_token(raw_context, label="context")
        if not isinstance(entry, dict):
            raise ValidationError(
                f"context {context!r} entry must contain project_dir"
            )
        if set(entry) != {"project_dir"}:
            raise ValidationError(
                f"context {context!r} entry supports only project_dir"
            )
        raw_project_dir = entry["project_dir"]
        if not isinstance(raw_project_dir, str) or not raw_project_dir:
            raise ValidationError(
                f"context {context!r} project_dir must be a non-empty string"
            )
        entries[context] = raw_project_dir
    return entries


def _resolve_index_project_dir(
    *,
    workspace_root: Path,
    raw_project_dir: str,
) -> Path:
    project_path = Path(raw_project_dir).expanduser()
    if not project_path.is_absolute():
        project_path = workspace_root / project_path
    return project_path.resolve()


def _resolve_project_dir_for_index(
    *,
    workspace_root: Path,
    project_dir: Path,
) -> Path:
    project_path = project_dir.expanduser()
    if not project_path.is_absolute():
        project_path = workspace_root / project_path
    return project_path.resolve()


def _project_dir_index_value(*, workspace_root: Path, project_dir: Path) -> str:
    resolved_project_dir = _resolve_project_dir_for_index(
        workspace_root=workspace_root,
        project_dir=project_dir,
    )
    try:
        relative = os.path.relpath(resolved_project_dir, workspace_root)
    except ValueError:
        return resolved_project_dir.as_posix()
    if relative == "." or (
        relative != ".." and not relative.startswith(f"..{os.sep}")
    ):
        return Path(relative).as_posix()
    return resolved_project_dir.as_posix()
