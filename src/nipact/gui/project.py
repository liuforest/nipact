"""Project resolution for the local GUI backend."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from nipact.errors import ValidationError
from nipact.project_context import resolve_project_context
from nipact.registry import read_registry_summary
from nipact.workflow import LoadedWorkflowProject, load_workflow_project


@dataclass(frozen=True)
class GuiProject:
    project_root: Path
    runtime_root: Path
    registry_path: Path
    context: str
    loaded_workflow_project: LoadedWorkflowProject


def resolve_gui_project(*, project_dir: Path, context: str) -> GuiProject:
    """Resolve a GUI project without running colors-specific validation."""
    resolved = resolve_project_context(project_dir=project_dir, context=context)
    loaded = load_workflow_project(project_dir=project_dir, context=context)
    if loaded.runtime_root != resolved.runtime_root:
        raise ValidationError("workflow project runtime root changed during load")
    read_registry_summary(resolved.registry_path, context=context)
    return GuiProject(
        project_root=resolved.project_root,
        runtime_root=resolved.runtime_root,
        registry_path=resolved.registry_path,
        context=context,
        loaded_workflow_project=loaded,
    )
