"""File adapters for the synthetic fMRI demo."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np


def import_fmri_sources_file(
    *,
    inputs: dict[str, tuple[Path, ...]],
    outputs: dict[str, Path],
    params: dict[str, Any],
    address: str,
) -> None:
    _write_npy(outputs["raw_bold"], _single_npy_input(inputs, "bold"))
    _write_npy(outputs["raw_t1"], _single_npy_input(inputs, "t1"))


def fmri_registration_file(
    *,
    inputs: dict[str, tuple[Path, ...]],
    outputs: dict[str, Path],
    params: dict[str, Any],
    address: str,
) -> None:
    bold = _single_npy_input(inputs, "raw_bold")
    t1 = _single_npy_input(inputs, "raw_t1")
    registered = bold - float(np.mean(bold)) + float(np.mean(t1))
    forward_transform = np.eye(4, dtype=np.float64)
    forward_transform[0, 3] = float(len(address))
    inverse_transform = np.linalg.inv(forward_transform)
    brain_mask = (t1 >= float(np.mean(t1))).astype(np.uint8)

    _write_npy(outputs["registered_bold"], registered)
    _write_npy(outputs["forward_transform"], forward_transform)
    _write_npy(outputs["inverse_transform"], inverse_transform)
    _write_npy(outputs["brain_mask"], brain_mask)
    _write_json(
        outputs["registration_qc"],
        {
            "address": address,
            "bold_shape": list(bold.shape),
            "t1_shape": list(t1.shape),
            "mask_voxels": int(np.sum(brain_mask)),
            "registered_mean": float(np.mean(registered)),
        },
    )


def _single_npy_input(inputs: dict[str, tuple[Path, ...]], name: str) -> np.ndarray:
    paths = inputs.get(name)
    if not isinstance(paths, tuple) or len(paths) != 1:
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
