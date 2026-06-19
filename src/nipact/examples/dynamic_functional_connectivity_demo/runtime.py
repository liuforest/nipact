"""File adapters for the synthetic DFC demo."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np


def import_timeseries_file(
    *,
    inputs: dict[str, tuple[Path, ...]],
    outputs: dict[str, Path],
    params: dict[str, Any],
    address: str,
) -> None:
    _write_npy(outputs["raw_timeseries"], _single_npy_input(inputs, "timeseries"))


def clean_timeseries_file(
    *,
    inputs: dict[str, tuple[Path, ...]],
    outputs: dict[str, Path],
    params: dict[str, Any],
    address: str,
) -> None:
    series = _single_npy_input(inputs, "raw_timeseries")
    centered = series - np.mean(series, axis=0, keepdims=True)
    scale = np.std(centered, axis=0, keepdims=True)
    scale[scale == 0.0] = 1.0
    _write_npy(outputs["clean_timeseries"], centered / scale)


def group_connectivity_summary_file(
    *,
    inputs: dict[str, tuple[Path, ...]],
    outputs: dict[str, Path],
    params: dict[str, Any],
    address: str,
) -> None:
    matrices = []
    for path in _adapter_input_paths(inputs, "clean_timeseries"):
        series = _read_npy(path)
        matrices.append(np.corrcoef(series, rowvar=False))
    stacked = np.stack(matrices)
    mean_matrix = np.mean(stacked, axis=0)
    off_diagonal = mean_matrix[~np.eye(mean_matrix.shape[0], dtype=bool)]
    _write_json(
        outputs["connectivity_summary"],
        {
            "address": address,
            "entity_count": len(matrices),
            "node_count": int(mean_matrix.shape[0]),
            "mean_connectivity": float(np.mean(off_diagonal)),
            "matrix_trace": float(np.trace(mean_matrix)),
        },
    )


def _adapter_input_paths(
    inputs: dict[str, tuple[Path, ...]],
    name: str,
) -> tuple[Path, ...]:
    paths = inputs.get(name)
    if not isinstance(paths, tuple) or not paths:
        raise RuntimeError(f"job input {name!r} must contain paths")
    if not all(isinstance(path, Path) for path in paths):
        raise RuntimeError(f"job input {name!r} must contain Path values")
    return paths


def _single_npy_input(inputs: dict[str, tuple[Path, ...]], name: str) -> np.ndarray:
    paths = _adapter_input_paths(inputs, name)
    if len(paths) != 1:
        raise RuntimeError(f"job input {name!r} must contain one path")
    return _read_npy(paths[0])


def _read_npy(path: Path) -> np.ndarray:
    with path.open("rb") as handle:
        return np.load(handle, allow_pickle=False)


def _write_npy(path: Path, array: np.ndarray) -> None:
    with path.open("wb") as handle:
        np.save(handle, array)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
