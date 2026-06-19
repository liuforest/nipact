"""Small helpers for published artifact filenames."""

from __future__ import annotations

from .errors import ValidationError
from .identity import validate_hash_alias, validate_path_token


def output_filename(*, address: str, output_hash: str, declared_extension: str) -> str:
    """Return the final hash-named output filename."""
    address = validate_path_token(address, label="output address")
    output_hash = validate_hash_alias(output_hash)
    _validate_declared_extension(declared_extension)
    return f"{address}.{output_hash}{declared_extension}"


def parse_output_filename(
    filename: str,
    *,
    declared_extension: str,
) -> tuple[str, str]:
    """Parse a final output filename using the declared extension, not Path.suffix."""
    if not isinstance(filename, str) or not filename:
        raise ValidationError("output filename must be a non-empty string")
    _validate_declared_extension(declared_extension)
    if not filename.endswith(declared_extension):
        raise ValidationError("output filename does not end with declared extension")
    stem = filename[: -len(declared_extension)]
    try:
        address, output_hash = stem.rsplit(".", maxsplit=1)
    except ValueError as exc:
        raise ValidationError("output filename must include output_hash") from exc
    return (
        validate_path_token(address, label="output address"),
        validate_hash_alias(output_hash),
    )


def _validate_declared_extension(value: object) -> str:
    if not isinstance(value, str) or not value.startswith("."):
        raise ValidationError("declared extension must start with '.'")
    if "/" in value or "\\" in value or value in {".", ".."}:
        raise ValidationError("declared extension must be a file extension")
    return value
