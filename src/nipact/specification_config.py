"""Pure parsing for optional specification registrations."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import PurePosixPath
from types import MappingProxyType
from typing import Any

from .errors import ValidationError
from .identity import validate_path_token

_REGISTRATION_SECTIONS = ("specification_libraries", "specifications")
_PATH_GLOB_CHARS = frozenset("*?[]{}")


@dataclass(frozen=True)
class SpecificationRegistrations:
    """Lexical project-local paths keyed by safe logical names."""

    specification_libraries: Mapping[str, PurePosixPath]
    specifications: Mapping[str, PurePosixPath]


def parse_specification_registrations(
    config: Mapping[str, Any],
) -> SpecificationRegistrations:
    """Validate optional registration maps without touching the filesystem."""
    parsed = {
        section: (
            _parse_registration_section(config[section], section=section)
            if section in config
            else {}
        )
        for section in _REGISTRATION_SECTIONS
    }
    return SpecificationRegistrations(
        specification_libraries=MappingProxyType(parsed["specification_libraries"]),
        specifications=MappingProxyType(parsed["specifications"]),
    )


def _parse_registration_section(
    payload: object,
    *,
    section: str,
) -> dict[str, PurePosixPath]:
    if not isinstance(payload, Mapping):
        raise ValidationError(f"nipact.yaml {section} must be a mapping")

    paths: dict[str, PurePosixPath] = {}
    for raw_name, raw_path in payload.items():
        name = validate_path_token(raw_name, label=f"{section} name")
        paths[name] = _validate_registration_path(
            raw_path,
            label=f"{section} {name!r}",
        )
    return dict(sorted(paths.items()))


def _validate_registration_path(value: object, *, label: str) -> PurePosixPath:
    if not isinstance(value, str) or not value:
        raise ValidationError(f"{label} must be a non-empty string")
    if "\\" in value:
        raise ValidationError(f"{label} must use a POSIX project-relative path")
    if any(char in value for char in _PATH_GLOB_CHARS):
        raise ValidationError(f"{label} cannot contain glob patterns")

    path = PurePosixPath(value)
    if path.is_absolute():
        raise ValidationError(f"{label} must be relative to project dir")
    if path == PurePosixPath("."):
        raise ValidationError(f"{label} cannot be empty or '.'")
    if any(part == ".." for part in path.parts):
        raise ValidationError(f"{label} cannot contain path traversal tokens")
    if path.as_posix() != value:
        raise ValidationError(f"{label} must be a normalized POSIX path")
    return path
