from __future__ import annotations

import builtins
from dataclasses import fields
import os
from pathlib import Path, PurePosixPath
from typing import Any, Callable

import pytest
import yaml

from conftest import RegistryV18Fixture
from nipact.errors import ValidationError
from nipact.project_context import ResolvedProjectContext, resolve_project_context
from nipact.specification_config import parse_specification_registrations
from nipact.workflow import LoadedWorkflowProject, load_workflow_project


ProjectReader = Callable[..., object]


def _write_config(project_dir: Path, config: dict[str, Any]) -> None:
    (project_dir / "nipact.yaml").write_text(
        yaml.safe_dump(config, sort_keys=False),
        encoding="utf-8",
    )


def _guard_target(
    monkeypatch: pytest.MonkeyPatch,
    target: Path,
) -> list[str]:
    accessed: list[str] = []
    normalized_target = Path(os.path.abspath(target))

    def check(value: object, operation: str) -> None:
        if isinstance(value, int):
            return
        try:
            path = Path(os.path.abspath(os.fspath(value)))
        except TypeError:
            return
        if path == normalized_target:
            accessed.append(operation)
            pytest.fail(f"ordinary config reader touched registered target via {operation}")

    for method_name in (
        "resolve",
        "stat",
        "lstat",
        "exists",
        "is_file",
        "is_dir",
        "open",
        "read_text",
        "read_bytes",
    ):
        original = getattr(Path, method_name)

        def guarded_path_method(
            self: Path,
            *args: object,
            _name: str = method_name,
            _original: object = original,
            **kwargs: object,
        ) -> object:
            check(self, f"Path.{_name}")
            return _original(self, *args, **kwargs)  # type: ignore[operator]

        monkeypatch.setattr(Path, method_name, guarded_path_method)

    original_open = builtins.open

    def guarded_open(file: object, *args: object, **kwargs: object) -> object:
        check(file, "open")
        return original_open(file, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", guarded_open)
    for function_name in ("stat", "lstat"):
        original = getattr(os, function_name)

        def guarded_os_function(
            path: object,
            *args: object,
            _name: str = function_name,
            _original: object = original,
            **kwargs: object,
        ) -> object:
            check(path, f"os.{_name}")
            return _original(path, *args, **kwargs)  # type: ignore[operator]

        monkeypatch.setattr(os, function_name, guarded_os_function)
    return accessed


def test_registration_parser_returns_immutable_normalized_paths() -> None:
    registrations = parse_specification_registrations(
        {
            "specification_libraries": {"models": "specifications/lib/models.yaml"},
            "specifications": {"analysis": "specifications/analysis.yaml"},
        }
    )

    assert registrations.specification_libraries == {
        "models": PurePosixPath("specifications/lib/models.yaml")
    }
    assert registrations.specifications == {
        "analysis": PurePosixPath("specifications/analysis.yaml")
    }
    with pytest.raises(TypeError):
        registrations.specifications["other"] = PurePosixPath("other.yaml")  # type: ignore[index]


def test_registration_parser_is_filesystem_independent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def reject(*_args: object, **_kwargs: object) -> object:
        pytest.fail("pure registration parsing touched the filesystem")

    for method_name in (
        "resolve",
        "stat",
        "lstat",
        "exists",
        "is_file",
        "is_dir",
        "open",
        "read_text",
        "read_bytes",
    ):
        monkeypatch.setattr(Path, method_name, reject)
    monkeypatch.setattr(builtins, "open", reject)
    monkeypatch.setattr(os, "stat", reject)
    monkeypatch.setattr(os, "lstat", reject)

    registrations = parse_specification_registrations(
        {"specifications": {"analysis": "specifications/analysis.yaml"}}
    )
    assert registrations.specification_libraries == {}
    assert registrations.specifications["analysis"] == PurePosixPath(
        "specifications/analysis.yaml"
    )


@pytest.mark.parametrize(
    ("section", "payload"),
    [
        pytest.param("specifications", None, id="null-section"),
        pytest.param("specifications", [], id="non-mapping-section"),
        pytest.param(
            "specifications",
            {"bad/name": "specifications/file.yaml"},
            id="unsafe-name",
        ),
        pytest.param(
            "specifications",
            {1: "specifications/file.yaml"},
            id="non-string-name",
        ),
        pytest.param("specifications", {"bad": 1}, id="non-string-path"),
        pytest.param("specifications", {"bad": ""}, id="empty-path"),
        pytest.param("specifications", {"bad": "/tmp/file.yaml"}, id="absolute"),
        pytest.param(
            "specifications",
            {"bad": "specifications\\file.yaml"},
            id="backslash",
        ),
        pytest.param("specifications", {"bad": "specifications/*.yaml"}, id="glob"),
        pytest.param("specifications", {"bad": "../file.yaml"}, id="traversal"),
        pytest.param(
            "specifications",
            {"bad": "specifications//file.yaml"},
            id="repeated-separator",
        ),
        pytest.param(
            "specifications",
            {"bad": "./specifications/file.yaml"},
            id="dot-prefix",
        ),
        pytest.param(
            "specifications",
            {"bad": "specifications/file.yaml/"},
            id="trailing-separator",
        ),
        pytest.param("specification_libraries", None, id="null-library-section"),
    ],
)
@pytest.mark.parametrize(
    "reader",
    [load_workflow_project, resolve_project_context],
    ids=["workflow-reader", "context-reader"],
)
def test_invalid_registration_fails_in_both_ordinary_readers_without_target_access(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
    section: str,
    payload: object,
    reader: ProjectReader,
) -> None:
    fixture = registry_v18_fixture
    config_path = fixture.project_dir / "nipact.yaml"
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    dormant_target = fixture.project_dir / "specifications/dormant.yaml"
    other_section = (
        "specification_libraries"
        if section == "specifications"
        else "specifications"
    )
    config[other_section] = {"dormant": "specifications/dormant.yaml"}
    config[section] = payload
    _write_config(fixture.project_dir, config)
    accessed = _guard_target(monkeypatch, dormant_target)

    with pytest.raises(ValidationError):
        reader(project_dir=fixture.project_dir, context=fixture.context)

    assert accessed == []


@pytest.mark.parametrize("sections", [{}, {"specification_libraries": {}, "specifications": {}}])
def test_absent_and_empty_registrations_are_valid_for_both_ordinary_readers(
    registry_v18_fixture: RegistryV18Fixture,
    sections: dict[str, object],
) -> None:
    fixture = registry_v18_fixture
    config_path = fixture.project_dir / "nipact.yaml"
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config.update(sections)
    _write_config(fixture.project_dir, config)

    assert load_workflow_project(
        project_dir=fixture.project_dir,
        context=fixture.context,
    ).context == fixture.context
    assert resolve_project_context(
        project_dir=fixture.project_dir,
        context=fixture.context,
    ).context == fixture.context


def test_ordinary_project_values_gain_no_specification_fields() -> None:
    assert all(
        "specification" not in field.name
        for field in fields(LoadedWorkflowProject)
    )
    assert all(
        "specification" not in field.name
        for field in fields(ResolvedProjectContext)
    )
