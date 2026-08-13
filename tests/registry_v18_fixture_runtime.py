"""Executable callables for the compact V18 compatibility fixture."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def fixture_source_file(
    *,
    inputs: dict[str, tuple[Path, ...]],
    outputs: dict[str, Path],
    params: dict[str, Any],
    address: str,
) -> None:
    source_value = _one_input(inputs, "source_value").read_text(
        encoding="utf-8"
    ).strip()
    _write_json(
        _one_output(outputs, "source_value"),
        {"address": address, "source_value": source_value},
    )


def fixture_transform_file(
    *,
    inputs: dict[str, tuple[Path, ...]],
    outputs: dict[str, Path],
    params: dict[str, Any],
    address: str,
) -> None:
    source = _read_json(_one_input(inputs, "source_value"))
    for output_name in ("left", "right"):
        _write_json(
            _one_output(outputs, output_name),
            {
                "address": address,
                "side": output_name,
                "source_value": source["source_value"],
                "variant": params["variant"],
            },
        )


def fixture_analysis_file(
    *,
    inputs: dict[str, tuple[Path, ...]],
    outputs: dict[str, Path],
    params: dict[str, Any],
    address: str,
) -> None:
    left = [_read_json(path) for path in inputs["left_values"]]
    right = [_read_json(path) for path in inputs["right_values"]]
    _write_json(
        _one_output(outputs, "summary"),
        {
            "address": address,
            "left": left,
            "right": right,
        },
    )


def _one_input(inputs: dict[str, tuple[Path, ...]], name: str) -> Path:
    paths = inputs[name]
    if len(paths) != 1:
        raise RuntimeError(f"fixture input {name!r} must contain one path")
    return paths[0]


def _one_output(outputs: dict[str, Path], name: str) -> Path:
    path = outputs[name]
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("fixture input must contain a JSON object")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
