from __future__ import annotations

import multiprocessing
import time
from pathlib import Path

import pytest

from nipact.errors import ValidationError
from nipact.runtime_lock import (
    RUNTIME_LOCK_FILENAME,
    RuntimeLockUnavailableError,
    acquire_mutating_runtime_lock,
)


def _hold_runtime_lock(
    runtime_root: str,
    ready: multiprocessing.synchronize.Event,
    release: multiprocessing.synchronize.Event,
) -> None:
    with acquire_mutating_runtime_lock(Path(runtime_root)):
        ready.set()
        if not release.wait(timeout=10):
            raise RuntimeError("test process timed out waiting to release lock")


def test_lock_file_persists_after_release(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    runtime_root.mkdir()

    with acquire_mutating_runtime_lock(runtime_root):
        assert (runtime_root / RUNTIME_LOCK_FILENAME).is_file()

    assert (runtime_root / RUNTIME_LOCK_FILENAME).is_file()
    with acquire_mutating_runtime_lock(runtime_root):
        pass


def test_second_process_fails_fast_and_can_acquire_after_release(
    tmp_path: Path,
) -> None:
    runtime_root = tmp_path / "runtime"
    runtime_root.mkdir()
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    release = context.Event()
    process = context.Process(
        target=_hold_runtime_lock,
        args=(str(runtime_root), ready, release),
    )
    process.start()
    try:
        assert ready.wait(timeout=10)
        started = time.monotonic()
        with pytest.raises(RuntimeLockUnavailableError, match="already in use"):
            with acquire_mutating_runtime_lock(runtime_root):
                pass
        assert time.monotonic() - started < 1
    finally:
        release.set()
        process.join(timeout=10)
        if process.is_alive():
            process.terminate()
            process.join(timeout=10)

    assert process.exitcode == 0
    with acquire_mutating_runtime_lock(runtime_root):
        pass


def test_different_runtime_roots_do_not_conflict(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()

    with acquire_mutating_runtime_lock(first):
        with acquire_mutating_runtime_lock(second):
            pass


@pytest.mark.parametrize("kind", ["missing", "file"])
def test_runtime_root_must_be_an_existing_directory(
    tmp_path: Path,
    kind: str,
) -> None:
    runtime_root = tmp_path / kind
    if kind == "file":
        runtime_root.write_text("not a directory\n", encoding="utf-8")

    with pytest.raises(ValidationError, match="existing directory"):
        with acquire_mutating_runtime_lock(runtime_root):
            pass
