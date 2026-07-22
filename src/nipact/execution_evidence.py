"""Invocation-scoped control evidence for NIPACT execution."""

from __future__ import annotations

import json
import os
import secrets
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .hashing import is_valid_digest
from .identity import validate_path_token

RUN_PLAN_SCHEMA_VERSION = 2
COMPLETION_RECEIPT_SCHEMA_VERSION = 1
_INVOCATION_TOKEN_BYTES = 16
_RECEIPT_KEYS = {
    "schema_version",
    "invocation_token",
    "job_id",
    "request_bundle_digest",
    "outputs",
}


class ExecutionEvidenceError(ValueError):
    """Raised when invocation-scoped execution evidence is invalid."""


def generate_invocation_token() -> str:
    """Return one opaque token for a real NIPACT invocation."""
    return secrets.token_hex(_INVOCATION_TOKEN_BYTES)


def validate_invocation_token(value: object) -> str:
    """Validate the lowercase hexadecimal invocation-token contract."""
    if (
        not isinstance(value, str)
        or len(value) != _INVOCATION_TOKEN_BYTES * 2
        or any(char not in "0123456789abcdef" for char in value)
    ):
        raise ExecutionEvidenceError("invocation_token is invalid")
    return value


def completion_receipt_relative_path(job_id: object) -> str:
    """Return the deterministic run-workspace-relative receipt path."""
    try:
        validated_job_id = validate_path_token(job_id, label="job_id")
    except ValueError as exc:
        raise ExecutionEvidenceError(str(exc)) from exc
    return f"receipts/{validated_job_id}.json"


@dataclass(frozen=True)
class CompletionReceipt:
    """One current callable-completion observation for a complete sibling bundle."""

    invocation_token: str
    job_id: str
    request_bundle_digest: str
    outputs: tuple[str, ...]

    def __post_init__(self) -> None:
        validate_invocation_token(self.invocation_token)
        try:
            validate_path_token(self.job_id, label="job_id")
        except ValueError as exc:
            raise ExecutionEvidenceError(str(exc)) from exc
        if not is_valid_digest(self.request_bundle_digest):
            raise ExecutionEvidenceError("request_bundle_digest is invalid")
        if not self.outputs:
            raise ExecutionEvidenceError("completion receipt outputs must not be empty")
        try:
            validated_outputs = tuple(
                validate_path_token(output, label="output name")
                for output in self.outputs
            )
        except ValueError as exc:
            raise ExecutionEvidenceError(str(exc)) from exc
        if validated_outputs != self.outputs:
            raise ExecutionEvidenceError("completion receipt outputs are invalid")
        if self.outputs != tuple(sorted(set(self.outputs))):
            raise ExecutionEvidenceError(
                "completion receipt outputs must be unique and sorted"
            )

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": COMPLETION_RECEIPT_SCHEMA_VERSION,
            "invocation_token": self.invocation_token,
            "job_id": self.job_id,
            "request_bundle_digest": self.request_bundle_digest,
            "outputs": list(self.outputs),
        }

    @classmethod
    def from_payload(cls, payload: object) -> "CompletionReceipt":
        if not isinstance(payload, dict) or set(payload) != _RECEIPT_KEYS:
            raise ExecutionEvidenceError("completion receipt fields are invalid")
        if payload.get("schema_version") != COMPLETION_RECEIPT_SCHEMA_VERSION:
            raise ExecutionEvidenceError("completion receipt schema version is invalid")
        raw_outputs = payload.get("outputs")
        if not isinstance(raw_outputs, list) or not all(
            isinstance(output, str) for output in raw_outputs
        ):
            raise ExecutionEvidenceError("completion receipt outputs are invalid")
        return cls(
            invocation_token=payload.get("invocation_token"),
            job_id=payload.get("job_id"),
            request_bundle_digest=payload.get("request_bundle_digest"),
            outputs=tuple(raw_outputs),
        )


def read_completion_receipt(path: Path) -> CompletionReceipt:
    """Read and validate one completion receipt."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ExecutionEvidenceError("completion receipt is unreadable") from exc
    return CompletionReceipt.from_payload(payload)


def write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    """Write one small JSON control file through same-directory replacement."""
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        content = json.dumps(
            payload,
            allow_nan=False,
            indent=2,
            sort_keys=True,
        ) + "\n"
    except (TypeError, ValueError) as exc:
        raise ExecutionEvidenceError("execution evidence is not finite JSON") from exc
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        text=True,
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(content)
        os.replace(temporary_path, path)
    finally:
        if temporary_path.exists() or temporary_path.is_symlink():
            temporary_path.unlink()


def write_completion_receipt_atomic(path: Path, receipt: CompletionReceipt) -> None:
    """Atomically expose one validated current completion receipt."""
    write_json_atomic(path, receipt.to_payload())
