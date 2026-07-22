"""Inactive fail-fast lock primitive for one mutating runtime invocation."""

from __future__ import annotations

import fcntl
from contextlib import contextmanager
from collections.abc import Iterator
from pathlib import Path

from .errors import NipactError, ValidationError

RUNTIME_LOCK_FILENAME = ".nipact-mutating.lock"


class RuntimeLockUnavailableError(NipactError):
    """Raised when another process holds the runtime mutation lock."""


@contextmanager
def acquire_mutating_runtime_lock(runtime_root: Path) -> Iterator[None]:
    """Hold the persistent runtime-root mutation lock for one local process."""
    if not isinstance(runtime_root, Path) or not runtime_root.is_dir():
        raise ValidationError("runtime_root must be an existing directory")
    lock_path = runtime_root / RUNTIME_LOCK_FILENAME
    try:
        handle = lock_path.open("a+b")
    except OSError as exc:
        raise ValidationError(
            f"cannot open runtime mutation lock: {lock_path}"
        ) from exc

    with handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeLockUnavailableError(
                f"runtime root is already in use by another mutating invocation: "
                f"{runtime_root}"
            ) from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
