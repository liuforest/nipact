"""Identifier validation for NIPACT manifests and path tokens."""

from __future__ import annotations

import re

from .errors import ValidationError
from .hashing import SHORT_HASH_LENGTH

SAFE_TOKEN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
HEX_ALIAS_RE = re.compile(r"^[0-9a-f]+$")


def validate_entity_id(value: object) -> str:
    """Validate a concrete manifest entity_id."""
    if not isinstance(value, str):
        raise ValidationError("entity_id must be a string")
    return validate_path_token(value, label="entity_id")


def validate_path_token(value: object, *, label: str) -> str:
    """Validate a filesystem-safe token used in NIPACT paths or manifests."""
    if not isinstance(value, str):
        raise ValidationError(f"{label} must be a string")
    if value != value.strip():
        raise ValidationError(f"{label} cannot contain leading or trailing whitespace")
    if not value:
        raise ValidationError(f"{label} cannot be empty")
    if "/" in value or "\\" in value:
        raise ValidationError(f"{label} cannot contain path separators: {value}")
    if value in {".", ".."} or ".." in value:
        raise ValidationError(f"{label} cannot contain path traversal tokens: {value}")
    if not SAFE_TOKEN_RE.match(value):
        raise ValidationError(f"{label} contains unsupported characters: {value}")
    return value


def validate_hash_alias(value: object) -> str:
    """Validate a lowercase 16-character manifest_hash or output_hash alias."""
    if not isinstance(value, str):
        raise ValidationError("hash alias must be a string")
    if value != value.strip():
        raise ValidationError("hash alias cannot contain leading or trailing whitespace")
    if not HEX_ALIAS_RE.match(value):
        raise ValidationError("hash alias must be lowercase hexadecimal")
    if len(value) != SHORT_HASH_LENGTH:
        raise ValidationError(f"hash alias must be exactly {SHORT_HASH_LENGTH} characters")
    return value
