"""Minimal YAML manifest contract for NIPACT projects."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .errors import ValidationError
from .hashing import sha256_digest, short_hash
from .identity import validate_entity_id

MANIFEST_FIELDS = frozenset({"description", "entities"})


@dataclass(frozen=True)
class Manifest:
    """Validated manifest membership and derived identity."""

    description: str
    entity_ids: tuple[str, ...]
    manifest_body: str
    manifest_digest: str
    manifest_hash: str

    @property
    def entity_count(self) -> int:
        return len(self.entity_ids)

    @property
    def first_entity_id(self) -> str:
        return self.entity_ids[0]

    @property
    def last_entity_id(self) -> str:
        return self.entity_ids[-1]


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


def build_manifest(*, description: str, entities: Iterable[object]) -> Manifest:
    """Build a validated manifest from a description and entity membership."""
    if not isinstance(description, str):
        raise ValidationError("manifest description must be a string")
    canonical_ids = canonical_entity_ids(entities)
    body = "\n".join(canonical_ids)
    digest = sha256_digest(body.encode("utf-8"))
    return Manifest(
        description=description,
        entity_ids=canonical_ids,
        manifest_body=body,
        manifest_digest=digest,
        manifest_hash=short_hash(digest),
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
