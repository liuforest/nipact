"""Internal orchestration for freezing immutable specification snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .registry import (
    REGISTRY_DB_PATH,
    insert_or_verify_specification_snapshot,
)
from .runtime_lock import acquire_mutating_runtime_lock
from .specification_canonical import (
    CanonicalSpecificationSnapshot,
    canonicalize_specification_snapshot,
)
from .specification_compiler import compile_specification
from .specification_loading import SpecificationSource, load_specification_project


@dataclass(frozen=True)
class SpecificationFreezeResult:
    snapshot: CanonicalSpecificationSnapshot
    inserted: bool


def freeze_specification_snapshot(
    *,
    project_dir: Path,
    context: str,
    source: SpecificationSource,
) -> SpecificationFreezeResult:
    """Compile, canonicalize, and atomically freeze one selected specification."""
    loaded = load_specification_project(
        project_dir=project_dir,
        context=context,
        source=source,
    )
    compilation = compile_specification(
        loaded.specification.mapping,
        {name: document.mapping for name, document in loaded.libraries.items()},
    )
    snapshot = canonicalize_specification_snapshot(
        loaded=loaded.workflow_project,
        compilation=compilation,
    )
    runtime_root = loaded.workflow_project.runtime_root
    registry_path = runtime_root / REGISTRY_DB_PATH
    with acquire_mutating_runtime_lock(runtime_root):
        inserted = insert_or_verify_specification_snapshot(
            registry_path,
            runtime_root=runtime_root,
            snapshot=snapshot,
        )
    return SpecificationFreezeResult(snapshot=snapshot, inserted=inserted)
