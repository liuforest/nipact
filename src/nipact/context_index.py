"""Workspace-local context index helpers."""

from __future__ import annotations

import os
from pathlib import Path

import yaml

from .errors import ValidationError
from .identity import validate_path_token

CONTEXT_INDEX_FILENAME = "nipact.contexts.yaml"


def resolve_project_dir(
    *,
    project_dir: Path | None,
    context: str,
    cwd: Path | None = None,
) -> Path:
    """Resolve a concrete project root for project-scoped commands."""
    context = validate_path_token(context, label="context")
    if project_dir is not None:
        return project_dir.expanduser().resolve()

    workspace_root = (cwd or Path.cwd()).expanduser().resolve()
    index_path = workspace_root / CONTEXT_INDEX_FILENAME
    if index_path.exists():
        entries = _read_index(index_path)
        raw_project_dir = entries.get(context)
        if raw_project_dir is None:
            raise ValidationError(
                f"context {context!r} is not registered in {CONTEXT_INDEX_FILENAME}"
            )
        return _resolve_index_project_dir(
            workspace_root=workspace_root,
            raw_project_dir=raw_project_dir,
        )

    if (workspace_root / "nipact.yaml").is_file():
        return workspace_root

    raise ValidationError(
        "could not resolve project dir; run from a workspace root containing "
        f"{CONTEXT_INDEX_FILENAME}, run from a project root containing "
        "nipact.yaml, or pass --project-dir"
    )


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
