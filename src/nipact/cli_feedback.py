"""Small CLI output helpers for user-facing command feedback."""

from __future__ import annotations

import sys
import threading
import time
from types import TracebackType
from typing import TextIO

try:  # pragma: no cover - exercised only when Rich is installed.
    from rich.console import Console
except ModuleNotFoundError:  # pragma: no cover - local editable installs may omit deps.
    Console = None  # type: ignore[assignment]


def format_cli_value(value: object) -> str:
    """Format simple CLI values consistently."""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


class CliFeedback:
    """Minimal stdout renderer with an optional TTY-only stderr spinner."""

    def __init__(
        self,
        *,
        stdout: TextIO | None = None,
        stderr: TextIO | None = None,
    ) -> None:
        self._stdout = stdout or sys.stdout
        self._stderr = stderr or sys.stderr
        self._console = (
            Console(file=self._stdout, highlight=False)
            if Console is not None
            else None
        )

    def line(self, text: str = "", *, style: str | None = None) -> None:
        if self._console is not None:
            self._console.print(
                text,
                style=style,
                markup=False,
                soft_wrap=True,
            )
            return
        print(text, file=self._stdout)

    def heading(self, text: str) -> None:
        self.line(text, style="bold")

    def key_value(self, key: str, value: object) -> None:
        self.line(f"{key}={format_cli_value(value)}")

    def pass_line(self, text: str) -> None:
        self.line(text, style="green")

    def flush(self) -> None:
        self._stdout.flush()

    def spinner(
        self,
        message: str,
        *,
        started_at: float | None = None,
    ) -> "CliSpinner":
        return CliSpinner(
            message=message,
            stream=self._stderr,
            started_at=started_at,
        )


class CliSpinner:
    """Tiny heartbeat spinner. It is not a progress indicator."""

    _FRAMES = "|/-\\"

    def __init__(
        self,
        *,
        message: str,
        stream: TextIO,
        started_at: float | None = None,
    ) -> None:
        self._message = message
        self._stream = stream
        self._started_at = started_at
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._active = bool(getattr(stream, "isatty", lambda: False)())
        self._last_rendered_width = 0

    def __enter__(self) -> "CliSpinner":
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.stop()

    def start(self) -> None:
        if not self._active or self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._thread is None:
            return
        self._stop_event.set()
        self._thread.join()
        clear_width = max(self._last_rendered_width, len(self._message) + 4)
        clear_text = " " * clear_width
        self._stream.write(f"\r{clear_text}\r")
        self._stream.flush()
        self._thread = None

    def _render_text(self, frame: str) -> str:
        message = self._message
        if self._started_at is not None:
            elapsed_seconds = time.perf_counter() - self._started_at
            message = f"{message} elapsed={elapsed_seconds:.1f}s"
        return f"{frame} {message}"

    def _run(self) -> None:
        frame_index = 0
        while not self._stop_event.is_set():
            frame = self._FRAMES[frame_index % len(self._FRAMES)]
            rendered_text = self._render_text(frame)
            self._last_rendered_width = max(
                self._last_rendered_width,
                len(rendered_text),
            )
            self._stream.write(f"\r{rendered_text}")
            self._stream.flush()
            frame_index += 1
            self._stop_event.wait(0.1)
