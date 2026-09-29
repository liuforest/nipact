"""Hash helpers shared by NIPACT manifest and artifact contracts."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat

from .errors import ValidationError

SHORT_HASH_LENGTH = 16
DIRECTORY_DIGEST_SCHEME = "nipact-directory-tree-sha256-v1"


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


def directory_tree_digest(path: Path) -> tuple[str, int]:
    """Hash a quiescent tree's exact members; return digest and payload byte size."""
    records: list[list[object]] = []
    payload_size = 0

    def observation(info: os.stat_result) -> tuple[int, ...]:
        return (
            info.st_dev,
            info.st_ino,
            info.st_mode,
            info.st_nlink,
            info.st_size,
            info.st_mtime_ns,
            info.st_ctime_ns,
        )

    def visit(member: Path, relative: str) -> None:
        nonlocal payload_size
        try:
            relative.encode("utf-8", errors="strict")
        except UnicodeEncodeError as exc:
            raise ValidationError(
                f"directory member name is not valid UTF-8: {member!s}"
            ) from exc
        before = member.lstat()
        if stat.S_ISDIR(before.st_mode):
            if relative:
                records.append(["directory", relative])
            for child in member.iterdir():
                visit(child, f"{relative}/{child.name}" if relative else child.name)
        elif relative and stat.S_ISREG(before.st_mode) and before.st_nlink == 1:
            digest = hashlib.sha256()
            with member.open("rb") as handle:
                if observation(os.fstat(handle.fileno())) != observation(before):
                    raise ValidationError(
                        f"directory member changed while hashing: {member}"
                    )
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
                if observation(os.fstat(handle.fileno())) != observation(before):
                    raise ValidationError(
                        f"directory member changed while hashing: {member}"
                    )
            records.append(["file", relative, before.st_size, digest.hexdigest()])
            payload_size += before.st_size
        else:
            raise ValidationError(
                f"directory tree requires a directory root and only directories or "
                f"single-link regular files (no symbolic links): {member}"
            )
        if observation(member.lstat()) != observation(before):
            raise ValidationError(f"directory member changed while hashing: {member}")

    try:
        visit(path, "")
    except OSError as exc:
        raise ValidationError(f"cannot read directory tree {path}: {exc}") from exc
    records.sort(key=lambda record: str(record[1]).encode("utf-8"))
    manifest = json.dumps(
        records, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    return sha256_digest(b"nipact.directory-tree.v1\0" + manifest), payload_size


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
