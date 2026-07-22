"""Logical-source authority and stable-observation primitives."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import BinaryIO

from .errors import ValidationError
from .hashing import is_valid_digest
from .identity import validate_entity_id, validate_path_token

SOURCE_SCOPES = frozenset({"global", "entity"})
SOURCE_AUTHORITY_STATUSES = frozenset({"new", "unchanged", "changed"})
_SOURCE_PATH_PREFIX = "data/"
_PATH_GLOB_CHARS = frozenset("*?[]{}")


@dataclass(frozen=True)
class LogicalSourceCoordinate:
    """Stable declaration coordinate for one global or entity source."""

    context: str
    scope: str
    source_name: str
    entity_id: str | None

    def __post_init__(self) -> None:
        validate_path_token(self.context, label="source context")
        if not isinstance(self.scope, str) or self.scope not in SOURCE_SCOPES:
            raise ValidationError(
                "source scope must be one of: " + ", ".join(sorted(SOURCE_SCOPES))
            )
        validate_path_token(self.source_name, label="source name")
        if self.scope == "global":
            if self.entity_id is not None:
                raise ValidationError("global source coordinate cannot have an entity_id")
            return
        if self.entity_id is None:
            raise ValidationError("entity source coordinate requires an entity_id")
        validate_entity_id(self.entity_id)


@dataclass(frozen=True)
class SourceDeclaration:
    """One logical source paired with its declared filesystem occurrence."""

    coordinate: LogicalSourceCoordinate
    declared_path: str
    declared_extension: str

    def __post_init__(self) -> None:
        if not isinstance(self.coordinate, LogicalSourceCoordinate):
            raise ValidationError(
                "source declaration coordinate must be a LogicalSourceCoordinate"
            )
        _validate_declared_source_path(self.declared_path)
        _validate_declared_extension(self.declared_extension)
        if not self.declared_path.endswith(self.declared_extension):
            raise ValidationError("source path must end with declared extension")


@dataclass(frozen=True)
class SourceOccurrenceGuard:
    """Bounded stat facts used to avoid unnecessary source-content reads."""

    st_dev: int
    st_ino: int
    st_size: int
    st_mtime_ns: int
    st_ctime_ns: int

    def __post_init__(self) -> None:
        for name in (
            "st_dev",
            "st_ino",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        ):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ValidationError(f"source occurrence {name} must be non-negative")


@dataclass(frozen=True)
class ObservedSourceAuthority:
    """Validated current source facts prepared by one stable observation."""

    declaration: SourceDeclaration
    content_digest: str
    file_size: int
    guard: SourceOccurrenceGuard
    status: str

    def __post_init__(self) -> None:
        if not isinstance(self.declaration, SourceDeclaration):
            raise ValidationError(
                "observed source declaration must be a SourceDeclaration"
            )
        if not is_valid_digest(self.content_digest):
            raise ValidationError(
                "observed source digest must be a lowercase 64-character "
                "hexadecimal string"
            )
        if type(self.file_size) is not int or self.file_size < 0:
            raise ValidationError("observed source file_size must be non-negative")
        if not isinstance(self.guard, SourceOccurrenceGuard):
            raise ValidationError(
                "observed source guard must be a SourceOccurrenceGuard"
            )
        if self.file_size != self.guard.st_size:
            raise ValidationError(
                "observed source file_size must match occurrence guard st_size"
            )
        if (
            not isinstance(self.status, str)
            or self.status not in SOURCE_AUTHORITY_STATUSES
        ):
            raise ValidationError(
                "observed source status must be one of: "
                + ", ".join(sorted(SOURCE_AUTHORITY_STATUSES))
            )


def logical_source_coordinate_payload(
    coordinate: LogicalSourceCoordinate,
) -> dict[str, str | None]:
    """Return the fixed-shape canonical payload for one logical coordinate."""
    if not isinstance(coordinate, LogicalSourceCoordinate):
        raise ValidationError("source coordinate must be a LogicalSourceCoordinate")
    return {
        "context": coordinate.context,
        "scope": coordinate.scope,
        "source_name": coordinate.source_name,
        "entity_id": coordinate.entity_id,
    }


def canonical_logical_source_coordinate_json(
    coordinate: LogicalSourceCoordinate,
) -> str:
    """Return compact deterministic JSON for one logical coordinate."""
    return json.dumps(
        logical_source_coordinate_payload(coordinate),
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def declared_source_extension(declared_path: str) -> str:
    """Return the current V1 extension interpretation for a source path."""
    path = _validate_declared_source_path(declared_path)
    if path.endswith(".nii.gz"):
        return ".nii.gz"
    extension = PurePosixPath(path).suffix
    if not extension:
        raise ValidationError("source path must include a file extension")
    return extension


def registered_source_authority_from_facts(
    *,
    coordinate: LogicalSourceCoordinate,
    declared_path: str,
    declared_extension: str,
    content_digest: str,
    file_size: int,
    guard: SourceOccurrenceGuard,
) -> ObservedSourceAuthority:
    """Validate persisted source facts without querying a registry."""
    return ObservedSourceAuthority(
        declaration=SourceDeclaration(
            coordinate=coordinate,
            declared_path=declared_path,
            declared_extension=declared_extension,
        ),
        content_digest=content_digest,
        file_size=file_size,
        guard=guard,
        status="unchanged",
    )


def observe_source_authority(
    *,
    runtime_root: Path,
    declaration: SourceDeclaration,
    registered: ObservedSourceAuthority | None,
) -> ObservedSourceAuthority:
    """Observe one source, hashing only when its persisted guard cannot be reused."""
    if not isinstance(runtime_root, Path):
        raise ValidationError("runtime_root must be a Path")
    if not isinstance(declaration, SourceDeclaration):
        raise ValidationError("declaration must be a SourceDeclaration")
    if registered is not None and not isinstance(
        registered, ObservedSourceAuthority
    ):
        raise ValidationError("registered source must be an ObservedSourceAuthority")

    if registered is not None:
        if registered.declaration.coordinate != declaration.coordinate:
            raise ValidationError(
                "registered source coordinate does not match source declaration"
            )
        if registered.declaration.declared_path != declaration.declared_path:
            raise ValidationError(
                "source relocation is unsupported for an already registered coordinate"
            )

    source_path = _resolved_source_path(
        runtime_root=runtime_root,
        declared_path=declaration.declared_path,
    )
    current_guard = read_source_occurrence_guard(
        runtime_root=runtime_root,
        declaration=declaration,
    )

    if registered is not None and current_guard == registered.guard:
        status = (
            "unchanged"
            if declaration.declared_extension
            == registered.declaration.declared_extension
            else "changed"
        )
        return ObservedSourceAuthority(
            declaration=declaration,
            content_digest=registered.content_digest,
            file_size=registered.file_size,
            guard=current_guard,
            status=status,
        )

    try:
        with source_path.open("rb") as handle:
            before = _guard_for_open_file(handle)
            content_digest = _stream_sha256(handle)
            after = _guard_for_open_file(handle)
    except OSError as exc:
        raise ValidationError(
            f"cannot read source occurrence: {declaration.declared_path}"
        ) from exc

    if before != after:
        raise ValidationError(
            f"source occurrence changed while it was being hashed: "
            f"{declaration.declared_path}"
        )

    if registered is None:
        status = "new"
    elif (
        content_digest == registered.content_digest
        and before.st_size == registered.file_size
        and declaration.declared_extension
        == registered.declaration.declared_extension
    ):
        status = "unchanged"
    else:
        status = "changed"

    return ObservedSourceAuthority(
        declaration=declaration,
        content_digest=content_digest,
        file_size=before.st_size,
        guard=before,
        status=status,
    )


def read_source_occurrence_guard(
    *,
    runtime_root: Path,
    declaration: SourceDeclaration,
) -> SourceOccurrenceGuard:
    """Read current source metadata without hashing file content."""
    if not isinstance(runtime_root, Path):
        raise ValidationError("runtime_root must be a Path")
    if not isinstance(declaration, SourceDeclaration):
        raise ValidationError("declaration must be a SourceDeclaration")
    return _guard_for_path(
        _resolved_source_path(
            runtime_root=runtime_root,
            declared_path=declaration.declared_path,
        )
    )


def _validate_declared_source_path(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ValidationError("source path must be a non-empty string")
    if "\\" in value:
        raise ValidationError("source path must use POSIX separators")
    if any(char in value for char in _PATH_GLOB_CHARS):
        raise ValidationError("source path cannot contain glob patterns")
    path = PurePosixPath(value)
    if path.is_absolute():
        raise ValidationError("source path must be runtime-relative")
    if any(part == ".." for part in path.parts):
        raise ValidationError("source path cannot contain traversal tokens")
    if not value.startswith(_SOURCE_PATH_PREFIX):
        raise ValidationError("source path must start with data/")
    if len(path.parts) <= 1:
        raise ValidationError("source path must include a file under data/")
    if path.as_posix() != value:
        raise ValidationError("source path must be a normalized POSIX path")
    return value


def _validate_declared_extension(value: object) -> str:
    if not isinstance(value, str) or not value.startswith("."):
        raise ValidationError("declared source extension must start with '.'")
    if "/" in value or "\\" in value or value in {".", ".."}:
        raise ValidationError("declared source extension must be a file extension")
    return value


def _resolved_source_path(*, runtime_root: Path, declared_path: str) -> Path:
    data_root = (runtime_root / "data").resolve()
    source_path = (runtime_root / Path(declared_path)).resolve()
    if not source_path.is_relative_to(data_root):
        raise ValidationError("source occurrence must resolve under runtime_root/data")
    return source_path


def _guard_for_path(path: Path) -> SourceOccurrenceGuard:
    try:
        source_stat = path.stat()
    except FileNotFoundError as exc:
        raise ValidationError(f"missing source occurrence: {path}") from exc
    except OSError as exc:
        raise ValidationError(f"cannot inspect source occurrence: {path}") from exc
    if not stat.S_ISREG(source_stat.st_mode):
        raise ValidationError(f"source occurrence must be a regular file: {path}")
    return _guard_from_stat(source_stat)


def _guard_for_open_file(handle: BinaryIO) -> SourceOccurrenceGuard:
    source_stat = os.fstat(handle.fileno())
    if not stat.S_ISREG(source_stat.st_mode):
        raise ValidationError("opened source occurrence must be a regular file")
    return _guard_from_stat(source_stat)


def _guard_from_stat(source_stat: os.stat_result) -> SourceOccurrenceGuard:
    return SourceOccurrenceGuard(
        st_dev=source_stat.st_dev,
        st_ino=source_stat.st_ino,
        st_size=source_stat.st_size,
        st_mtime_ns=source_stat.st_mtime_ns,
        st_ctime_ns=source_stat.st_ctime_ns,
    )


def _stream_sha256(handle: BinaryIO, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    for chunk in iter(lambda: handle.read(chunk_size), b""):
        digest.update(chunk)
    return digest.hexdigest()
