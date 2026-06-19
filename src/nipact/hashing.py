"""Hash helpers shared by NIPACT manifest and artifact contracts."""

from __future__ import annotations

import hashlib
from pathlib import Path

from .errors import ValidationError

SHORT_HASH_LENGTH = 16


def sha256_digest(data: bytes) -> str:
    """Return the lowercase 64-character SHA-256 digest for raw bytes."""
    return hashlib.sha256(data).hexdigest()


def sha256_file_digest(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    """Return the lowercase SHA-256 digest for a file without loading it at once."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def is_valid_digest(value: object) -> bool:
    """Return True for lowercase 64-character hexadecimal SHA-256 digests."""
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(char in "0123456789abcdef" for char in value)
    )


def short_hash(full_digest: str, *, length: int = SHORT_HASH_LENGTH) -> str:
    """Return the short display/path alias for a full digest."""
    if length <= 0:
        raise ValidationError("short hash length must be positive")
    if length > 64:
        raise ValidationError("short hash length must not exceed digest length")
    if not is_valid_digest(full_digest):
        raise ValidationError("digest must be a lowercase 64-character hexadecimal string")
    return full_digest[:length]
