from pathlib import Path

import pytest
import yaml

from nipact.errors import ValidationError
from nipact.context_index import (
    CONTEXT_INDEX_FILENAME,
    preflight_context_index_update,
    resolve_project_dir,
    update_context_index,
)


def test_resolve_project_dir_uses_explicit_path(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"

    assert (
        resolve_project_dir(project_dir=project_dir, context="colors", cwd=tmp_path)
        == project_dir
    )


def test_update_context_index_writes_relative_project_dir(tmp_path: Path) -> None:
    project_dir = tmp_path / "demos/colors/project"
    project_dir.mkdir(parents=True)

    index_path = update_context_index(
        workspace_dir=tmp_path,
        context="colors",
        project_dir=project_dir,
    )

    assert index_path == tmp_path / CONTEXT_INDEX_FILENAME
    payload = yaml.safe_load(index_path.read_text(encoding="utf-8"))
    assert payload == {
        "contexts": {
            "colors": {
                "project_dir": "demos/colors/project",
            }
        }
    }


def test_resolve_project_dir_uses_workspace_context_index(tmp_path: Path) -> None:
    project_dir = tmp_path / "demos/colors/project"
    project_dir.mkdir(parents=True)
    update_context_index(
        workspace_dir=tmp_path,
        context="colors",
        project_dir=project_dir,
    )

    assert (
        resolve_project_dir(project_dir=None, context="colors", cwd=tmp_path)
        == project_dir
    )


def test_resolve_project_dir_falls_back_to_project_root(tmp_path: Path) -> None:
    (tmp_path / "nipact.yaml").write_text("context: colors\n", encoding="utf-8")

    assert (
        resolve_project_dir(project_dir=None, context="colors", cwd=tmp_path)
        == tmp_path
    )


def test_resolve_project_dir_rejects_unknown_context(tmp_path: Path) -> None:
    update_context_index(
        workspace_dir=tmp_path,
        context="colors",
        project_dir=tmp_path / "project",
    )

    with pytest.raises(ValidationError, match="is not registered"):
        resolve_project_dir(project_dir=None, context="other", cwd=tmp_path)


def test_preflight_rejects_same_context_different_project(tmp_path: Path) -> None:
    update_context_index(
        workspace_dir=tmp_path,
        context="colors",
        project_dir=tmp_path / "project-one",
    )

    with pytest.raises(ValidationError, match="already points"):
        preflight_context_index_update(
            workspace_dir=tmp_path,
            context="colors",
            project_dir=tmp_path / "project-two",
        )
