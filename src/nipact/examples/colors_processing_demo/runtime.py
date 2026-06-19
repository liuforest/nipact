"""File adapters for colors Snakemake jobs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .model import (
    CandidateResult,
    CandidateSelection,
    CohortFit,
    ColorPoint,
    analyze_sectors,
    angular_pull,
    apply_cohort_fit,
    candidate_select,
    fit_cohort,
    sector_label,
)


def extract_color_features(point: ColorPoint) -> dict[str, Any]:
    """Return the source point payload used as the demo feature artifact."""
    payload = point.to_payload()
    payload["rgb"] = list(point.rgb)
    return payload


def import_color_source_file(
    *,
    inputs: dict[str, tuple[Path, ...]],
    outputs: dict[str, Path],
    params: dict[str, Any],
    address: str,
) -> None:
    output = _single_adapter_output_path(outputs)
    source = _read_json(_single_adapter_source_path(inputs))
    records = source.get("records")
    if not isinstance(records, list):
        raise RuntimeError("colors source artifact records must be a list")
    for record in records:
        if isinstance(record, dict) and record.get("entity_id") == address:
            _write_json(output, _point(record).to_payload())
            return
    raise RuntimeError(f"colors source artifact missing entity_id: {address}")


def extract_color_features_file(
    *,
    inputs: dict[str, tuple[Path, ...]],
    outputs: dict[str, Path],
    params: dict[str, Any],
    address: str,
) -> None:
    output = _single_adapter_output_path(outputs)
    _write_json(
        output,
        extract_color_features(_point_from_adapter_input(inputs, "source_color")),
    )


def color_local_transform_file(
    *,
    inputs: dict[str, tuple[Path, ...]],
    outputs: dict[str, Path],
    params: dict[str, Any],
    address: str,
) -> None:
    output = _single_adapter_output_path(outputs)
    _write_json(
        output,
        angular_pull(_point_from_adapter_input(inputs, "features"), **params).to_payload(),
    )


def color_candidate_select_file(
    *,
    inputs: dict[str, tuple[Path, ...]],
    outputs: dict[str, Path],
    params: dict[str, Any],
    address: str,
) -> None:
    output = _single_adapter_output_path(outputs)
    _write_json(
        output,
        _selection_payload(
            candidate_select(_point_from_adapter_input(inputs, "local_color"), **params)
        ),
    )


def color_cohort_fit_file(
    *,
    inputs: dict[str, tuple[Path, ...]],
    outputs: dict[str, Path],
    params: dict[str, Any],
    address: str,
) -> None:
    output = _single_adapter_output_path(outputs)
    points = [
        _selection(_read_json(path)).selected_state
        for path in _adapter_input_paths(inputs, "selected_color")
    ]
    _write_json(output, fit_cohort(points).to_payload())


def color_cohort_apply_file(
    *,
    inputs: dict[str, tuple[Path, ...]],
    outputs: dict[str, Path],
    params: dict[str, Any],
    address: str,
) -> None:
    output = _single_adapter_output_path(outputs)
    point = _selection_from_adapter_input(inputs, "selected_color").selected_state
    fit = _fit_from_adapter_input(inputs, "cohort_fit")
    _write_json(output, apply_cohort_fit(point, fit, **params).to_payload())


def color_sector_label_file(
    *,
    inputs: dict[str, tuple[Path, ...]],
    outputs: dict[str, Path],
    params: dict[str, Any],
    address: str,
) -> None:
    output = _single_adapter_output_path(outputs)
    point = _point_from_adapter_input(inputs, "cohort_color")
    _write_json(
        output,
        {
            "entity_id": point.entity_id,
            "point": point.to_payload(),
            "sector_label": sector_label(point, **params),
        },
    )


def color_sector_analysis_file(
    *,
    inputs: dict[str, tuple[Path, ...]],
    outputs: dict[str, Path],
    params: dict[str, Any],
    address: str,
) -> None:
    output = _single_adapter_output_path(outputs)
    points = [
        _point(_required_mapping(_read_json(path), "point"))
        for path in _adapter_input_paths(inputs, "sector_label")
    ]
    _write_json(
        output,
        analyze_sectors(
            points,
            analysis_manifest_name=address,
            **params,
        ).to_payload(),
    )


def _point_from_adapter_input(inputs: dict[str, tuple[Path, ...]], name: str) -> ColorPoint:
    return _point(_read_json(_single_adapter_input_path(inputs, name)))


def _selection_from_adapter_input(
    inputs: dict[str, tuple[Path, ...]],
    name: str,
) -> CandidateSelection:
    return _selection(_read_json(_single_adapter_input_path(inputs, name)))


def _fit_from_adapter_input(
    inputs: dict[str, tuple[Path, ...]],
    name: str,
) -> CohortFit:
    return _fit(_read_json(_single_adapter_input_path(inputs, name)))


def _point(payload: dict[str, Any]) -> ColorPoint:
    return ColorPoint(
        entity_id=_required_string(payload, "entity_id"),
        index=_required_int(payload, "index"),
        theta=_required_float(payload, "theta"),
        radius=_required_float(payload, "radius"),
        hue_degrees=_required_float(payload, "hue_degrees"),
        saturation=_required_float(payload, "saturation"),
        value=_required_float(payload, "value"),
        x=_required_float(payload, "x"),
        y=_required_float(payload, "y"),
        rgb=_rgb(payload.get("rgb")),
        hex_color=_required_string(payload, "hex_color"),
    )


def _selection(payload: dict[str, Any]) -> CandidateSelection:
    results = payload.get("candidate_results")
    if not isinstance(results, list):
        raise RuntimeError("candidate selection candidate_results must be a list")
    return CandidateSelection(
        entity_id=_required_string(payload, "entity_id"),
        input_state=_point(_required_mapping(payload, "input_state")),
        selection_rule=dict(_required_mapping(payload, "selection_rule")),
        candidate_results=tuple(_candidate_result(result) for result in results),
        selected_candidate_id=_required_string(payload, "selected_candidate_id"),
        selected_state=_point(_required_mapping(payload, "selected_state")),
    )


def _candidate_result(payload: object) -> CandidateResult:
    if not isinstance(payload, dict):
        raise RuntimeError("candidate result must be an object")
    return CandidateResult(
        candidate_id=_required_string(payload, "candidate_id"),
        transform_name=_required_string(payload, "transform_name"),
        params=dict(_required_mapping(payload, "params")),
        theta=_required_float(payload, "theta"),
        radius=_required_float(payload, "radius"),
        x=_required_float(payload, "x"),
        y=_required_float(payload, "y"),
        rgb=_rgb(payload.get("rgb")),
        hex_color=_required_string(payload, "hex_color"),
        score=_required_float(payload, "score"),
        selected=_required_bool(payload, "selected"),
    )


def _selection_payload(selection: CandidateSelection) -> dict[str, Any]:
    payload = selection.to_payload()
    payload["candidate_results"] = list(payload["candidate_results"])
    return payload


def _fit(payload: dict[str, Any]) -> CohortFit:
    return CohortFit(
        cohort_theta_centroid=_required_float(payload, "cohort_theta_centroid"),
        cohort_radius_centroid=_required_float(payload, "cohort_radius_centroid"),
        cohort_x_centroid=_required_float(payload, "cohort_x_centroid"),
        cohort_y_centroid=_required_float(payload, "cohort_y_centroid"),
        cohort_entity_count=_required_int(payload, "cohort_entity_count"),
        cohort_manifest_digest=_required_string(payload, "cohort_manifest_digest"),
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


def _single_adapter_input_path(
    inputs: dict[str, tuple[Path, ...]],
    name: str,
) -> Path:
    paths = _adapter_input_paths(inputs, name)
    if len(paths) != 1:
        raise RuntimeError(f"job input {name!r} must contain one path")
    return paths[0]


def _single_adapter_source_path(inputs: dict[str, tuple[Path, ...]]) -> Path:
    if len(inputs) != 1:
        raise RuntimeError("colors source import expects one source input binding")
    binding_name = next(iter(inputs))
    return _single_adapter_input_path(inputs, binding_name)


def _single_adapter_output_path(outputs: dict[str, Path]) -> Path:
    if len(outputs) != 1:
        raise RuntimeError("colors adapter expects one output binding")
    output = next(iter(outputs.values()))
    if not isinstance(output, Path):
        raise RuntimeError("colors adapter output must be a Path")
    return output


def _rgb(value: object) -> tuple[int, int, int]:
    if (
        not isinstance(value, list)
        or len(value) != 3
        or not all(isinstance(channel, int) for channel in value)
    ):
        raise RuntimeError("rgb must be a three-item integer list")
    return value[0], value[1], value[2]


def _required_mapping(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise RuntimeError(f"{key} must be an object")
    return value


def _required_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise RuntimeError(f"{key} must be a non-empty string")
    return value


def _required_int(payload: dict[str, Any], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int):
        raise RuntimeError(f"{key} must be an integer")
    return value


def _required_float(payload: dict[str, Any], key: str) -> float:
    value = payload.get(key)
    if not isinstance(value, int | float):
        raise RuntimeError(f"{key} must be a number")
    return float(value)


def _required_bool(payload: dict[str, Any], key: str) -> bool:
    value = payload.get(key)
    if not isinstance(value, bool):
        raise RuntimeError(f"{key} must be a boolean")
    return value


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"JSON artifact must contain an object: {path}")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
