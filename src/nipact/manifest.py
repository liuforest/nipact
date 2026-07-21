"""Minimal YAML manifest contract for NIPACT projects."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .errors import ValidationError
from .hashing import is_valid_digest, sha256_digest, short_hash
from .identity import validate_entity_id

MANIFEST_FIELDS = frozenset({"description", "entities"})
MANIFEST_VALUE_SCHEMA = "entity_set_v1"
_MANIFEST_VALUE_DOMAIN_PREFIX = b"nipact.manifest.entity_set_v1\0"


@dataclass(frozen=True)
class Manifest:
    """One declaration description paired with an immutable manifest value."""

    description: str
    value: "ManifestValue"

    @property
    def manifest_value_schema(self) -> str:
        return self.value.value_schema

    @property
    def entity_ids(self) -> tuple[str, ...]:
        return self.value.entity_ids

    @property
    def manifest_body(self) -> str:
        return self.value.canonical_body

    @property
    def canonical_body(self) -> str:
        return self.value.canonical_body

    @property
    def manifest_digest(self) -> str:
        return self.value.manifest_digest

    @property
    def manifest_hash(self) -> str:
        return self.value.manifest_hash

    @property
    def entity_count(self) -> int:
        return len(self.entity_ids)

    @property
    def first_entity_id(self) -> str:
        return self.entity_ids[0]

    @property
    def last_entity_id(self) -> str:
        return self.entity_ids[-1]


@dataclass(frozen=True)
class ManifestValueReference:
    """Schema-qualified identity of one immutable manifest value."""

    value_schema: str
    manifest_digest: str

    def __post_init__(self) -> None:
        _validate_manifest_value_reference(
            value_schema=self.value_schema,
            manifest_digest=self.manifest_digest,
        )


@dataclass(frozen=True)
class ManifestValue:
    """Validated immutable entity-set value and its canonical identity."""

    value_schema: str
    manifest_digest: str
    canonical_body: str

    def __post_init__(self) -> None:
        _validate_manifest_value_reference(
            value_schema=self.value_schema,
            manifest_digest=self.manifest_digest,
        )
        if not isinstance(self.canonical_body, str):
            raise ValidationError("manifest value canonical_body must be a string")
        if not self.canonical_body:
            raise ValidationError("manifest value canonical_body cannot be empty")
        if self.canonical_body.endswith("\n"):
            raise ValidationError("manifest value canonical_body must not end with a newline")

        entity_ids = canonical_entity_ids(self.canonical_body.split("\n"))
        if "\n".join(entity_ids) != self.canonical_body:
            raise ValidationError("manifest value canonical_body is not canonical")
        expected_digest = _manifest_value_digest(self.canonical_body)
        if self.manifest_digest != expected_digest:
            raise ValidationError("manifest value digest does not match canonical body")

    @property
    def reference(self) -> ManifestValueReference:
        return ManifestValueReference(
            value_schema=self.value_schema,
            manifest_digest=self.manifest_digest,
        )

    @property
    def entity_ids(self) -> tuple[str, ...]:
        return tuple(self.canonical_body.split("\n"))

    @property
    def entity_count(self) -> int:
        return len(self.entity_ids)

    @property
    def manifest_hash(self) -> str:
        return short_hash(self.manifest_digest)


def _validate_manifest_value_reference(*, value_schema: str, manifest_digest: str) -> None:
    if value_schema != MANIFEST_VALUE_SCHEMA:
        raise ValidationError(f"unsupported manifest value schema: {value_schema!r}")
    if not is_valid_digest(manifest_digest):
        raise ValidationError(
            "manifest value digest must be a lowercase 64-character hexadecimal string"
        )


def _manifest_value_digest(canonical_body: str) -> str:
    return sha256_digest(_MANIFEST_VALUE_DOMAIN_PREFIX + canonical_body.encode("utf-8"))


def canonical_entity_ids(entity_ids: Iterable[object]) -> tuple[str, ...]:
    """Validate entity IDs, reject duplicates, and return sorted membership."""
    cleaned = [validate_entity_id(entity_id) for entity_id in entity_ids]
    if not cleaned:
        raise ValidationError("manifest entities cannot be empty")

    seen: set[str] = set()
    duplicates: set[str] = set()
    for entity_id in cleaned:
        if entity_id in seen:
            duplicates.add(entity_id)
        seen.add(entity_id)
    if duplicates:
        raise ValidationError(
            "manifest contains duplicate entity_id values: " + ", ".join(sorted(duplicates))
        )
    return tuple(sorted(cleaned))


def manifest_body(entity_ids: Iterable[object]) -> str:
    """Return the canonical manifest body used for identity hashing."""
    return "\n".join(canonical_entity_ids(entity_ids))


def manifest_digest(entity_ids: Iterable[object]) -> str:
    """Return the full 64-character manifest_digest for membership."""
    return sha256_digest(manifest_body(entity_ids).encode("utf-8"))


def manifest_hash(entity_ids: Iterable[object]) -> str:
    """Return the 16-character manifest_hash for membership."""
    return short_hash(manifest_digest(entity_ids))


def build_manifest_value(*, entities: Iterable[object]) -> ManifestValue:
    """Build an immutable entity-set value using the V1 manifest schema."""
    canonical_body = "\n".join(canonical_entity_ids(entities))
    return ManifestValue(
        value_schema=MANIFEST_VALUE_SCHEMA,
        manifest_digest=_manifest_value_digest(canonical_body),
        canonical_body=canonical_body,
    )


def build_manifest(*, description: str, entities: Iterable[object]) -> Manifest:
    """Build a validated manifest from a description and entity membership."""
    if not isinstance(description, str):
        raise ValidationError("manifest description must be a string")
    return Manifest(
        description=description,
        value=build_manifest_value(entities=entities),
    )


def parse_manifest(payload: Mapping[str, Any], *, label: str = "manifest") -> Manifest:
    """Parse a minimal manifest mapping with description and entities fields."""
    keys = set(payload)
    unknown = sorted(keys - MANIFEST_FIELDS)
    if unknown:
        raise ValidationError(f"{label} contains unknown field(s): {', '.join(unknown)}")
    missing = sorted(MANIFEST_FIELDS - keys)
    if missing:
        raise ValidationError(f"{label} is missing required field(s): {', '.join(missing)}")

    entities = payload["entities"]
    if not isinstance(entities, list):
        raise ValidationError(f"{label} entities must be a list")
    return build_manifest(description=payload["description"], entities=entities)


def load_manifest(path: Path) -> Manifest:
    """Load and parse a minimal YAML manifest file."""
    if not path.is_file():
        raise ValidationError(f"missing manifest file: {path}")
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ValidationError(f"invalid manifest YAML file {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValidationError(f"manifest file must contain a mapping: {path}")
    return parse_manifest(payload, label=str(path))
