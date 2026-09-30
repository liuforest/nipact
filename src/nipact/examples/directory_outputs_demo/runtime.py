"""File and directory adapters for the synthetic directory-output demo."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np

MAP_LABELS = ("low", "high")


def import_image_file(
    *,
    inputs: dict[str, tuple[Path, ...]],
    outputs: dict[str, Path],
    params: dict[str, Any],
    address: str,
) -> None:
    _write_npy(outputs["raw_image"], _single_npy_input(inputs, "image"))


def segment_image_directory(
    *,
    inputs: dict[str, tuple[Path, ...]],
    outputs: dict[str, Path],
    params: dict[str, Any],
    address: str,
) -> None:
    image = _single_npy_input(inputs, "raw_image")
    low = float(params["low_threshold"])
    high = float(params["high_threshold"])
    masks = {
        "low": (image >= low) & (image < high),
        "high": image >= high,
    }
    maps = outputs["maps"]
    maps.mkdir()
    for label, mask in masks.items():
        _write_npy(maps / f"label-{label}.npy", mask.astype(np.uint8))

    # Provider validation: NIPACT accepts any valid tree, so the provider
    # rejects a result without exactly its required members before returning.
    required = {f"label-{label}.npy" for label in MAP_LABELS}
    present = {path.name for path in maps.iterdir()}
    if present != required:
        raise RuntimeError(
            f"maps members {sorted(present)} do not match {sorted(required)}"
        )
    _write_json(
        outputs["volumes"],
        {
            "address": address,
            "voxels": {label: int(np.sum(mask)) for label, mask in masks.items()},
        },
    )


def summarize_maps_file(
    *,
    inputs: dict[str, tuple[Path, ...]],
    outputs: dict[str, Path],
    params: dict[str, Any],
    address: str,
) -> None:
    label = params["label"]
    fractions = [
        float(np.mean(_read_npy(tree / f"label-{label}.npy")))
        for tree in _adapter_input_paths(inputs, "maps")
    ]
    _write_json(
        outputs["summary"],
        {
            "address": address,
            "label": label,
            "tree_count": len(fractions),
            "fractions": fractions,
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
