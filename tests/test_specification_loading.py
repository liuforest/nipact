from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from conftest import RegistryV18Fixture
from nipact.errors import ValidationError
import nipact.registry as registry_module
import nipact.specification_loading as loading_module
from nipact.specification_config import parse_specification_registrations
from nipact.specification_loading import (
    ExplicitDocumentOrigin,
    ExplicitSpecificationSource,
    RegisteredDocumentOrigin,
    RegisteredSpecificationSource,
    load_specification_project,
)
import nipact.workflow as workflow_module


def _write_config(project_dir: Path, **sections: object) -> None:
    config_path = project_dir / "nipact.yaml"
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config.update(sections)
    config_path.write_text(
        yaml.safe_dump(config, sort_keys=False),
        encoding="utf-8",
    )


def _write_document(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


@pytest.mark.parametrize(
    "text",
    [
        pytest.param("value: 1\nvalue: 2\n", id="duplicate-key"),
        pytest.param("first: &shared 1\nsecond: *shared\n", id="alias"),
        pytest.param("value: &unused 1\n", id="unused-anchor"),
        pytest.param("value: {<<: {nested: true}}\n", id="merge-key"),
        pytest.param("value: !custom item\n", id="custom-tag"),
        pytest.param("value: !!binary YQ==\n", id="binary"),
        pytest.param("value: !!set {item: null}\n", id="set"),
        pytest.param("value: 2026-08-13\n", id="timestamp"),
        pytest.param("1: value\n", id="non-string-key"),
        pytest.param("value: .nan\n", id="nan"),
        pytest.param("value: .inf\n", id="positive-infinity"),
        pytest.param("value: -.inf\n", id="negative-infinity"),
        pytest.param("- item\n", id="sequence-root"),
        pytest.param("null\n", id="null-root"),
        pytest.param("", id="empty-document"),
    ],
)
def test_strict_yaml_rejects_unsupported_composition_and_values(text: str) -> None:
    with pytest.raises(ValidationError, match="strict-test.yaml|YAML"):
        loading_module._parse_strict_yaml_mapping(
            text,
            origin="strict-test.yaml",
        )


def test_strict_yaml_loads_nested_json_like_values() -> None:
    assert loading_module._parse_strict_yaml_mapping(
        """
name: example
enabled: true
missing: null
count: 3
ratio: 0.25
items:
  - one
  - nested:
      flag: false
""",
        origin="valid.yaml",
    ) == {
        "name": "example",
        "enabled": True,
        "missing": None,
        "count": 3,
        "ratio": 0.25,
        "items": ["one", {"nested": {"flag": False}}],
    }


def test_registered_selection_loads_only_directly_named_libraries(
    registry_v18_fixture: RegistryV18Fixture,
) -> None:
    fixture = registry_v18_fixture
    specification = _write_document(
        fixture.project_dir / "specifications/analysis.yaml",
        """
libraries: [models]
include: specifications/not-followed.yaml
dimensions:
  method: [one, two]
""",
    )
    models = _write_document(
        fixture.project_dir / "specifications/lib/models.yaml",
        """
libraries: [unused]
models:
  linear:
    family: gaussian
""",
    )
    _write_config(
        fixture.project_dir,
        specifications={"analysis": "specifications/analysis.yaml"},
        specification_libraries={
            "models": "specifications/lib/models.yaml",
            "unused": "specifications/lib/missing.yaml",
        },
    )

    loaded = load_specification_project(
        project_dir=fixture.project_dir,
        context=fixture.context,
        source=RegisteredSpecificationSource("analysis"),
    )

    assert loaded.workflow_project.context == fixture.context
    assert loaded.specification.mapping["include"] == "specifications/not-followed.yaml"
    assert isinstance(loaded.specification.origin, RegisteredDocumentOrigin)
    assert loaded.specification.origin.name == "analysis"
    assert loaded.specification.origin.path == specification.resolve()
    assert list(loaded.libraries) == ["models"]
    assert loaded.libraries["models"].mapping["libraries"] == ["unused"]
    assert isinstance(loaded.libraries["models"].origin, RegisteredDocumentOrigin)
    assert loaded.libraries["models"].origin.path == models.resolve()


def test_explicit_external_selection_uses_only_project_registered_libraries(
    registry_v18_fixture: RegistryV18Fixture,
) -> None:
    fixture = registry_v18_fixture
    external = _write_document(
        fixture.project_dir.parent / "external-specification.yaml",
        "libraries: [models]\npath: ../not-followed.yaml\n",
    )
    _write_document(
        fixture.project_dir / "specifications/lib/models.yaml",
        "models: [linear]\n",
    )
    _write_config(
        fixture.project_dir,
        specification_libraries={"models": "specifications/lib/models.yaml"},
    )

    loaded = load_specification_project(
        project_dir=fixture.project_dir,
        context=fixture.context,
        source=ExplicitSpecificationSource(external),
    )

    assert isinstance(loaded.specification.origin, ExplicitDocumentOrigin)
    assert loaded.specification.origin.path == external.resolve()
    assert loaded.specification.mapping["path"] == "../not-followed.yaml"
    assert list(loaded.libraries) == ["models"]


@pytest.mark.parametrize(
    "libraries",
    [
        pytest.param(["known", "unknown"], id="unknown"),
        pytest.param(["known", "known"], id="duplicate"),
        pytest.param(None, id="present-null"),
    ],
)
def test_library_list_errors_fail_before_any_library_loader_call(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
    libraries: object,
) -> None:
    fixture = registry_v18_fixture
    _write_document(
        fixture.project_dir / "specifications/analysis.yaml",
        yaml.safe_dump({"libraries": libraries}, sort_keys=False),
    )
    _write_config(
        fixture.project_dir,
        specifications={"analysis": "specifications/analysis.yaml"},
        specification_libraries={"known": "specifications/lib/missing.yaml"},
    )
    original = loading_module._load_registered_document
    opened_libraries: list[str] = []

    def guarded_loader(**kwargs: Any) -> object:
        if kwargs["registration_kind"] == "library":
            opened_libraries.append(kwargs["name"])
            pytest.fail("library loader ran before complete name validation")
        return original(**kwargs)

    monkeypatch.setattr(loading_module, "_load_registered_document", guarded_loader)

    with pytest.raises(ValidationError):
        load_specification_project(
            project_dir=fixture.project_dir,
            context=fixture.context,
            source=RegisteredSpecificationSource("analysis"),
        )
    assert opened_libraries == []


def test_registered_selection_rejects_an_unknown_key(
    registry_v18_fixture: RegistryV18Fixture,
) -> None:
    with pytest.raises(ValidationError, match="unknown specification"):
        load_specification_project(
            project_dir=registry_v18_fixture.project_dir,
            context=registry_v18_fixture.context,
            source=RegisteredSpecificationSource("unknown"),
        )


@pytest.mark.parametrize(
    "case",
    ["missing", "directory", "final-symlink", "escaping-parent"],
)
def test_registered_selection_enforces_the_selected_target_gate(
    registry_v18_fixture: RegistryV18Fixture,
    case: str,
) -> None:
    fixture = registry_v18_fixture
    declared_path = "specifications/analysis.yaml"
    candidate = fixture.project_dir / declared_path
    if case == "directory":
        candidate.mkdir(parents=True)
    elif case == "final-symlink":
        target = _write_document(
            fixture.project_dir / "specifications/actual.yaml",
            "value: true\n",
        )
        candidate.symlink_to(target.name)
    elif case == "escaping-parent":
        outside = fixture.project_dir.parent / "outside-specifications"
        _write_document(outside / "analysis.yaml", "value: true\n")
        linked_parent = fixture.project_dir / "linked-specifications"
        linked_parent.symlink_to(outside, target_is_directory=True)
        declared_path = "linked-specifications/analysis.yaml"
    _write_config(
        fixture.project_dir,
        specifications={"analysis": declared_path},
    )

    with pytest.raises(ValidationError):
        load_specification_project(
            project_dir=fixture.project_dir,
            context=fixture.context,
            source=RegisteredSpecificationSource("analysis"),
        )


def test_lower_level_loading_is_independent_of_ordinary_project_side_effects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_root = tmp_path / "project"
    _write_document(project_root / "specifications/analysis.yaml", "value: true\n")
    registrations = parse_specification_registrations(
        {"specifications": {"analysis": "specifications/analysis.yaml"}}
    )

    def reject(*_args: object, **_kwargs: object) -> object:
        pytest.fail("lower-level specification loading crossed an ordinary boundary")

    monkeypatch.setattr(workflow_module, "import_module", reject)
    monkeypatch.setattr(workflow_module, "load_manifest", reject)
    monkeypatch.setattr(workflow_module, "_load_source_index", reject)
    monkeypatch.setattr(registry_module, "read_context_runtime_path", reject)

    document = loading_module._load_selected_specification(
        project_root=project_root.resolve(),
        registrations=registrations,
        source=RegisteredSpecificationSource("analysis"),
    )
    assert document.mapping == {"value": True}


def test_high_level_wrapper_retains_existing_callable_import_boundary(
    registry_v18_fixture: RegistryV18Fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = registry_v18_fixture
    _write_document(
        fixture.project_dir / "specifications/analysis.yaml",
        "value: true\n",
    )
    _write_config(
        fixture.project_dir,
        specifications={"analysis": "specifications/analysis.yaml"},
    )
    original = workflow_module.import_module
    imported: list[str] = []

    def recording_import(name: str) -> object:
        imported.append(name)
        return original(name)

    monkeypatch.setattr(workflow_module, "import_module", recording_import)

    load_specification_project(
        project_dir=fixture.project_dir,
        context=fixture.context,
        source=RegisteredSpecificationSource("analysis"),
    )
    assert "registry_v18_fixture_runtime" in imported
