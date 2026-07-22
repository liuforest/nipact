"""Helpers for canonical published-artifact paths and filenames."""

from __future__ import annotations

from pathlib import Path

from .errors import ValidationError
from .hashing import is_valid_digest
from .identity import validate_hash_alias, validate_path_token

STORAGE_LAYOUT_VERSION = 1
CANONICAL_OUTPUT_ROOT = Path("outputs") / f"v{STORAGE_LAYOUT_VERSION}"


def canonical_output_directory(
    *,
    context: str,
    step_name: str,
    address: str,
    request_bundle_digest: str,
    output_name: str,
) -> str:
    """Return the canonical runtime-relative directory for one requested output."""
    context = validate_path_token(context, label="output context")
    step_name = validate_path_token(step_name, label="output step name")
    address = validate_path_token(address, label="output address")
    output_name = validate_path_token(output_name, label="output name")
    if not is_valid_digest(request_bundle_digest):
        raise ValidationError(
            "request bundle digest must be a lowercase 64-character hexadecimal string"
        )
    return (
        CANONICAL_OUTPUT_ROOT
        / context
        / step_name
        / address
        / request_bundle_digest
        / output_name
    ).as_posix()


def canonical_output_path(
    *,
    context: str,
    step_name: str,
    address: str,
    request_bundle_digest: str,
    output_name: str,
    output_hash: str,
    declared_extension: str,
) -> str:
    """Return the complete canonical runtime-relative path for one output."""
    directory = canonical_output_directory(
        context=context,
        step_name=step_name,
        address=address,
        request_bundle_digest=request_bundle_digest,
        output_name=output_name,
    )
    filename = output_filename(
        address=address,
        output_hash=output_hash,
        declared_extension=declared_extension,
    )
    return f"{directory}/{filename}"


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
