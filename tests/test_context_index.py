from pathlib import Path

import pytest
import yaml

from nipact.errors import ValidationError
from nipact.context_index import (
    CONTEXT_INDEX_FILENAME,
    preflight_context_index_update,
    resolve_project,
    update_context_index,
)


@pytest.mark.parametrize(
    ("files", "cwd", "explicit", "context", "selected", "error"),
    [
        pytest.param(
            {"near/nipact.contexts.yaml": "contexts: {}"},
            "near", "project", None, ("project", "colors"), None,
            id="explicit-bypasses-index",
        ),
        pytest.param(
            {"near/nipact.yaml": "context: nearby"},
            "near/nested", None, None, ("near", "nearby"), None,
            id="nearer-project",
        ),
        pytest.param(
            {"nipact.yaml": "context: local"},
            "nested", None, None, ("project", "colors"), None,
            id="same-directory-index-wins",
        ),
        pytest.param(
            {},
            "nested", None, None, ("project", "colors"), None,
            id="one-entry-relative-index",
        ),
        pytest.param(
            {"nipact.contexts.yaml": "contexts: {colors: {project_dir: project}, other: {project_dir: other}}", "other/nipact.yaml": "context: other"},
            "nested", None, "other", ("other", "other"), None,
            id="explicit-index-selection",
        ),
        pytest.param(
            {"nipact.contexts.yaml": "contexts: {}"},
            "nested", None, None, None, "--project-dir",
            id="empty-index",
        ),
        pytest.param(
            {"nipact.contexts.yaml": "contexts: {colors: {project_dir: project}, other: {project_dir: other}}"},
            "nested", None, None, None, "--context",
            id="ambiguous-index",
        ),
        pytest.param(
            {},
            "nested", None, "unknown", None, "not registered",
            id="unknown-key",
        ),
        pytest.param(
            {"near/nipact.contexts.yaml": "contexts: ["},
            "near/nested", None, None, None, "invalid YAML",
            id="invalid-nearest-index",
        ),
        pytest.param(
            {"near/nipact.yaml": "context: ["},
            "near/nested", None, None, None, "invalid YAML",
            id="invalid-nearest-config",
        ),
        pytest.param(
            {"near/nipact.contexts.yaml": None},
            "near/nested", None, None, None, "not a file",
            id="non-file-index",
        ),
        pytest.param(
            {"near/nipact.yaml": None},
            "near/nested", None, None, None, "could not read project config",
            id="non-file-config",
        ),
        pytest.param(
            {"nipact.contexts.yaml": "contexts: {colors: {project_dir: missing}}"},
            "nested", None, None, None, "project dir does not exist",
            id="missing-index-target",
        ),
        pytest.param(
            {},
            "nested", "project", "other", None, "context mismatch",
            id="explicit-context-mismatch",
        ),
        pytest.param(
            {"project/nipact.yaml": "context: other"},
            "nested", None, None, None, "context mismatch",
            id="index-key-mismatch",
        ),
        pytest.param(
            {"project/nipact.yaml": "[]"},
            "nested", "project", None, None, "must contain a mapping",
            id="config-mapping-required",
        ),
        pytest.param(
            {"project/nipact.yaml": "paths: {}"},
            "nested", "project", None, None, "context",
            id="declared-context-required",
        ),
        pytest.param(
            {},
            "nested", "project", "", None, "context",
            id="empty-explicit-context",
        ),
    ],
)
def test_resolve_project_selects_context_at_nearest_boundary(
    tmp_path: Path,
    files: dict[str, str | None],
    cwd: str,
    explicit: str | None,
    context: str | None,
    selected: tuple[str, str] | None,
    error: str | None,
) -> None:
    declarations = {
        "project/nipact.yaml": "context: colors",
        "nipact.contexts.yaml": "contexts: {colors: {project_dir: project}}",
        **files,
    }
    for name, content in declarations.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        if content is None:
            path.mkdir()
        else:
            path.write_text(content, encoding="utf-8")
    start = tmp_path / cwd
    start.mkdir(parents=True, exist_ok=True)
    project_dir = tmp_path / explicit if explicit is not None else None

    if error is not None:
        with pytest.raises(ValidationError, match=error):
            resolve_project(project_dir=project_dir, context=context, cwd=start)
    else:
        assert resolve_project(
            project_dir=project_dir, context=context, cwd=start
        ) == (tmp_path / selected[0], selected[1])


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
