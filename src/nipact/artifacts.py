"""Helpers for canonical published-artifact paths and filenames."""

from __future__ import annotations

from contextlib import nullcontext
from pathlib import Path
import os
import shutil
import stat
import hashlib
from typing import BinaryIO

from .errors import ValidationError
from .hashing import is_valid_digest, directory_tree_digest
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
    declared_extension: str | None,
    kind: str = "file",
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
        kind=kind,
    )
    return f"{directory}/{filename}"


def output_filename(
    *,
    address: str,
    output_hash: str,
    declared_extension: str | None,
    kind: str = "file",
) -> str:
    """Return the final hash-named output filename."""
    address = validate_path_token(address, label="output address")
    output_hash = validate_hash_alias(output_hash)
    if kind == "directory" and declared_extension is None:
        return f"{address}.{output_hash}"
    if kind != "file":
        raise ValidationError("invalid output kind or extension")
    _validate_declared_extension(declared_extension)
    return f"{address}.{output_hash}{declared_extension}"


def artifact_root_matches(path: Path, kind: str) -> bool:
    """Check only the lexical root; never enumerate a directory payload."""
    try:
        info = path.lstat()
    except FileNotFoundError:
        return False
    return (kind == "directory" and stat.S_ISDIR(info.st_mode)) or (
        kind == "file" and stat.S_ISREG(info.st_mode) and info.st_nlink == 1
    )


def root_observation(info: os.stat_result) -> tuple[int, ...]:
    return (
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_nlink,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


def artifact_content_facts(
    path: Path, kind: str, *, copy_to: Path | None = None
) -> tuple[str, int]:
    """Verify one quiescent payload, returning its digest and payload bytes.

    With ``copy_to``, write the verified bytes to that absent destination too.
    """
    if kind == "directory":
        return directory_tree_digest(path, copy_to=copy_to)
    info = path.lstat()
    if kind != "file" or not stat.S_ISREG(info.st_mode):
        raise ValidationError(f"artifact is not a regular file: {path}")
    if info.st_nlink != 1:
        raise ValidationError(f"artifact has multiple hardlinks: {path}")
    before = root_observation(path.lstat())
    with path.open("rb") as handle:
        if root_observation(os.fstat(handle.fileno())) != before:
            raise ValidationError(f"artifact changed before verification: {path}")
        with nullcontext() if copy_to is None else copy_to.open("xb") as copy:
            digest = _sha256_open_file(handle, copy)
        if root_observation(os.fstat(handle.fileno())) != before:
            raise ValidationError(f"artifact changed during verification: {path}")
    if root_observation(path.lstat()) != before:
        raise ValidationError(f"artifact changed during verification: {path}")
    if copy_to is not None:
        shutil.copystat(path, copy_to)
    return digest, before[4]


def remove_owned_path(path: Path, *, staging_root: Path) -> None:
    """Remove a disposable staging entry without following its root symlink."""
    if path == staging_root or not path.is_relative_to(staging_root):
        raise ValidationError("owned output must stay below staging root")
    if not path.parent.resolve().is_relative_to(staging_root.resolve()):
        raise ValidationError("owned output parent escaped staging root")
    if path.is_symlink() or path.is_file():
        path.unlink()
    elif path.exists():
        shutil.rmtree(path)


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


def _sha256_open_file(handle: BinaryIO, destination: BinaryIO | None = None) -> str:
    digest = hashlib.sha256()
    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
        digest.update(chunk)
        if destination is not None:
            destination.write(chunk)
    return digest.hexdigest()
