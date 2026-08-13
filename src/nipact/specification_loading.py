"""Strict, selected-only loading for proposed specification documents."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import math
from pathlib import Path, PurePosixPath
from typing import Any, Literal

import yaml
from yaml.constructor import ConstructorError
from yaml.events import AliasEvent
from yaml.nodes import MappingNode, Node, ScalarNode, SequenceNode
from yaml.resolver import BaseResolver

from .errors import ValidationError
from .identity import validate_path_token
from .specification_config import (
    SpecificationRegistrations,
    parse_specification_registrations,
)
from .workflow import LoadedWorkflowProject, load_workflow_project

type StrictValue = (
    None
    | bool
    | int
    | float
    | str
    | list["StrictValue"]
    | dict[str, "StrictValue"]
)

_ALLOWED_TAGS = frozenset(
    {
        BaseResolver.DEFAULT_MAPPING_TAG,
        BaseResolver.DEFAULT_SEQUENCE_TAG,
        BaseResolver.DEFAULT_SCALAR_TAG,
        "tag:yaml.org,2002:null",
        "tag:yaml.org,2002:bool",
        "tag:yaml.org,2002:int",
        "tag:yaml.org,2002:float",
    }
)
_MERGE_TAG = "tag:yaml.org,2002:merge"


@dataclass(frozen=True)
class RegisteredSpecificationSource:
    name: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "name",
            validate_path_token(self.name, label="specification name"),
        )


@dataclass(frozen=True)
class ExplicitSpecificationSource:
    path: Path


type SpecificationSource = RegisteredSpecificationSource | ExplicitSpecificationSource


@dataclass(frozen=True)
class RegisteredDocumentOrigin:
    registration_kind: Literal["specification", "library"]
    name: str
    path: Path


@dataclass(frozen=True)
class ExplicitDocumentOrigin:
    path: Path


type DocumentOrigin = RegisteredDocumentOrigin | ExplicitDocumentOrigin


@dataclass(frozen=True)
class StrictDocument:
    mapping: dict[str, StrictValue]
    origin: DocumentOrigin


@dataclass(frozen=True)
class LoadedSpecificationProject:
    workflow_project: LoadedWorkflowProject
    specification: StrictDocument
    libraries: dict[str, StrictDocument]


def load_specification_project(
    *,
    project_dir: Path,
    context: str,
    source: SpecificationSource,
) -> LoadedSpecificationProject:
    """Load one selected strict document and its direct registered libraries."""
    workflow_project = load_workflow_project(
        project_dir=project_dir,
        context=context,
    )
    registrations = parse_specification_registrations(
        _read_project_config(workflow_project.project_root / "nipact.yaml")
    )
    specification = _load_selected_specification(
        project_root=workflow_project.project_root,
        registrations=registrations,
        source=source,
    )
    library_names = _requested_library_names(specification.mapping)
    unknown = [
        name
        for name in library_names
        if name not in registrations.specification_libraries
    ]
    if unknown:
        raise ValidationError(f"unknown specification library: {unknown[0]}")

    libraries = {
        name: _load_registered_document(
            project_root=workflow_project.project_root,
            declared_path=registrations.specification_libraries[name],
            registration_kind="library",
            name=name,
        )
        for name in library_names
    }
    return LoadedSpecificationProject(
        workflow_project=workflow_project,
        specification=specification,
        libraries=libraries,
    )


def _load_selected_specification(
    *,
    project_root: Path,
    registrations: SpecificationRegistrations,
    source: SpecificationSource,
) -> StrictDocument:
    if isinstance(source, RegisteredSpecificationSource):
        try:
            declared_path = registrations.specifications[source.name]
        except KeyError as exc:
            raise ValidationError(f"unknown specification: {source.name}") from exc
        return _load_registered_document(
            project_root=project_root,
            declared_path=declared_path,
            registration_kind="specification",
            name=source.name,
        )
    if isinstance(source, ExplicitSpecificationSource):
        path = _resolve_explicit_file(source.path)
        return _read_strict_document(path, origin=ExplicitDocumentOrigin(path=path))
    raise ValidationError("unsupported specification source")


def _load_registered_document(
    *,
    project_root: Path,
    declared_path: PurePosixPath,
    registration_kind: Literal["specification", "library"],
    name: str,
) -> StrictDocument:
    path = _resolve_registered_file(
        project_root=project_root,
        declared_path=declared_path,
        label=f"registered {registration_kind} {name!r}",
    )
    return _read_strict_document(
        path,
        origin=RegisteredDocumentOrigin(
            registration_kind=registration_kind,
            name=name,
            path=path,
        ),
    )


def _resolve_registered_file(
    *,
    project_root: Path,
    declared_path: PurePosixPath,
    label: str,
) -> Path:
    candidate = project_root.joinpath(*declared_path.parts)
    if candidate.is_symlink():
        raise ValidationError(f"{label} cannot be a symlink")
    try:
        resolved = candidate.resolve(strict=True)
    except FileNotFoundError as exc:
        raise ValidationError(f"missing {label}: {candidate}") from exc
    except OSError as exc:
        raise ValidationError(f"could not resolve {label}: {candidate}") from exc
    try:
        resolved.relative_to(project_root)
    except ValueError as exc:
        raise ValidationError(f"{label} must stay inside project dir") from exc
    if not resolved.is_file():
        raise ValidationError(f"{label} must be a regular file")
    return resolved


def _resolve_explicit_file(path: Path) -> Path:
    try:
        resolved = path.expanduser().resolve(strict=True)
    except FileNotFoundError as exc:
        raise ValidationError(f"missing explicit specification: {path}") from exc
    except OSError as exc:
        raise ValidationError(f"could not resolve explicit specification: {path}") from exc
    if not resolved.is_file():
        raise ValidationError("explicit specification must be a regular file")
    return resolved


def _requested_library_names(mapping: Mapping[str, StrictValue]) -> tuple[str, ...]:
    if "libraries" not in mapping:
        return ()
    payload = mapping["libraries"]
    if type(payload) is not list:
        raise ValidationError("specification libraries must be a list")
    names: list[str] = []
    seen: set[str] = set()
    for raw_name in payload:
        name = validate_path_token(raw_name, label="specification library name")
        if name in seen:
            raise ValidationError(f"duplicate specification library: {name}")
        seen.add(name)
        names.append(name)
    return tuple(names)


def _read_strict_document(path: Path, *, origin: DocumentOrigin) -> StrictDocument:
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise ValidationError(f"could not read strict YAML document: {path}") from exc
    return StrictDocument(
        mapping=_parse_strict_yaml_mapping(text, origin=str(path)),
        origin=origin,
    )


class _StrictLoader(yaml.SafeLoader):
    def construct_mapping(self, node: MappingNode, deep: bool = False) -> dict[str, Any]:
        if not isinstance(node, MappingNode):
            raise ConstructorError(None, None, "expected a mapping", node.start_mark)
        mapping: dict[str, Any] = {}
        for key_node, value_node in node.value:
            if key_node.tag == _MERGE_TAG:
                raise ConstructorError(
                    None,
                    None,
                    "YAML merge keys are not supported",
                    key_node.start_mark,
                )
            if key_node.tag != BaseResolver.DEFAULT_SCALAR_TAG:
                raise ConstructorError(
                    None,
                    None,
                    "YAML mapping keys must be strings",
                    key_node.start_mark,
                )
            key = self.construct_object(key_node, deep=deep)
            if key in mapping:
                raise ConstructorError(
                    None,
                    None,
                    f"duplicate YAML mapping key: {key}",
                    key_node.start_mark,
                )
            mapping[key] = self.construct_object(value_node, deep=deep)
        return mapping


def _parse_strict_yaml_mapping(text: str, *, origin: str) -> dict[str, StrictValue]:
    try:
        _reject_yaml_references(text, origin=origin)
        loader = _StrictLoader(text)
        try:
            node = loader.get_single_node()
            if node is None:
                raise ValidationError(f"strict YAML document is empty: {origin}")
            _validate_node_tags(node, origin=origin)
            payload = loader.construct_document(node)
        finally:
            loader.dispose()
    except ValidationError:
        raise
    except yaml.YAMLError as exc:
        problem = getattr(exc, "problem", None) or str(exc).splitlines()[0]
        raise ValidationError(f"invalid strict YAML {origin}: {problem}") from exc
    if type(payload) is not dict:
        raise ValidationError(f"strict YAML document must contain a mapping: {origin}")
    _validate_strict_value(payload, origin=origin)
    return payload


def _reject_yaml_references(text: str, *, origin: str) -> None:
    has_alias = False
    has_anchor = False
    for event in yaml.parse(text, Loader=yaml.SafeLoader):
        has_alias = has_alias or isinstance(event, AliasEvent)
        has_anchor = has_anchor or getattr(event, "anchor", None) is not None
    if has_alias:
        raise ValidationError(f"YAML aliases are not supported: {origin}")
    if has_anchor:
        raise ValidationError(f"YAML anchors are not supported: {origin}")


def _validate_node_tags(node: Node, *, origin: str) -> None:
    if node.tag == _MERGE_TAG:
        raise ValidationError(f"YAML merge keys are not supported: {origin}")
    if node.tag not in _ALLOWED_TAGS:
        raise ValidationError(f"unsupported YAML tag {node.tag!r}: {origin}")
    if isinstance(node, MappingNode):
        for key_node, value_node in node.value:
            _validate_node_tags(key_node, origin=origin)
            _validate_node_tags(value_node, origin=origin)
    elif isinstance(node, SequenceNode):
        for child in node.value:
            _validate_node_tags(child, origin=origin)
    elif not isinstance(node, ScalarNode):
        raise ValidationError(f"unsupported YAML node: {origin}")


def _validate_strict_value(value: object, *, origin: str) -> None:
    if value is None or type(value) in {bool, int, str}:
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise ValidationError(f"non-finite YAML number is not supported: {origin}")
        return
    if type(value) is list:
        for child in value:
            _validate_strict_value(child, origin=origin)
        return
    if type(value) is dict:
        for key, child in value.items():
            if type(key) is not str:
                raise ValidationError(f"YAML mapping keys must be strings: {origin}")
            _validate_strict_value(child, origin=origin)
        return
    raise ValidationError(f"unsupported YAML scalar type: {origin}")


def _read_project_config(path: Path) -> dict[str, Any]:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValidationError(f"missing project config: {path}") from exc
    except OSError as exc:
        raise ValidationError(f"could not read project config: {path}") from exc
    except yaml.YAMLError as exc:
        raise ValidationError(f"invalid YAML file {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValidationError(f"project config must contain a mapping: {path}")
    return payload
