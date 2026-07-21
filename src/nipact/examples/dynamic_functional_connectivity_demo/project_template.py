"""Project template declarations for the synthetic DFC demo."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np

from ...manifest import Manifest, build_manifest

SUPPORTED_DEMO = "dfc"
SOURCE_INDEX_PATH = "sources.yaml"
ANALYSIS_COHORT_NAME = "init"
BASE_WORKFLOW_NAME = "base"
SELECTED_STEP_NAME = "dfc_group_summary"
SELECTED_OUTPUT_NAME = "connectivity_summary"

ENTITY_IDS = ("sub_001", "sub_002", "sub_003", "sub_004")
MANIFEST_PATHS = {
    ANALYSIS_COHORT_NAME: "manifests/init.yaml",
}
MANIFEST_DESCRIPTIONS = {
    ANALYSIS_COHORT_NAME: "Synthetic DFC analysis cohort",
}


def source_file_paths() -> list[str]:
    return [f"data/dfc/{entity_id}_timeseries.npy" for entity_id in ENTITY_IDS]


def write_runtime_sources(runtime_root: Path) -> None:
    source_dir = runtime_root / "data/dfc"
    source_dir.mkdir(parents=True, exist_ok=True)
    for index, entity_id in enumerate(ENTITY_IDS):
        base = np.arange(18, dtype=np.float64).reshape(6, 3)
        series = base + float(index + 1)
        _write_npy(source_dir / f"{entity_id}_timeseries.npy", series)


def build_manifests() -> dict[str, Manifest]:
    return {
        ANALYSIS_COHORT_NAME: build_manifest(
            description=MANIFEST_DESCRIPTIONS[ANALYSIS_COHORT_NAME],
            entities=ENTITY_IDS,
        ),
    }


def manifest_paths() -> dict[str, str]:
    return dict(MANIFEST_PATHS)


def source_index_payload() -> dict[str, Any]:
    return {
        "entities": {
            entity_id: {
                "timeseries": f"data/dfc/{entity_id}_timeseries.npy",
            }
            for entity_id in ENTITY_IDS
        },
    }


def project_config(*, context: str, runtime: str) -> dict[str, Any]:
    return {
        "context": context,
        "paths": {
            "runtime": runtime,
        },
        "sources": {
            "index": SOURCE_INDEX_PATH,
        },
        "workflows": {
            BASE_WORKFLOW_NAME: "workflows/base.yaml",
        },
        "steps": {
            "directory": "steps",
        },
        "manifests": manifest_paths(),
    }


def step_files() -> dict[str, dict[str, Any]]:
    return {
        "dfc_source": {
            "step_name": "dfc_source",
            "step_contract_version": "1",
            "pattern_kind": "pattern_a",
            "execution_role": "source_import",
            "address_scope": "entity",
            "callable": (
                "nipact.examples.dynamic_functional_connectivity_demo.runtime:"
                "import_timeseries_file"
            ),
            "source_inputs": ["timeseries"],
            "manifest_binding": {
                "role": "source_population",
                "manifest": ANALYSIS_COHORT_NAME,
            },
            "outputs": {
                "raw_timeseries": {
                    "extension": ".npy",
                    "address_scope": "entity",
                },
            },
        },
        "dfc_clean_timeseries": {
            "step_name": "dfc_clean_timeseries",
            "step_contract_version": "1",
            "pattern_kind": "pattern_a",
            "execution_role": "transform",
            "address_scope": "entity",
            "callable": (
                "nipact.examples.dynamic_functional_connectivity_demo.runtime:"
                "clean_timeseries_file"
            ),
            "inputs": {
                "raw_timeseries": {
                    "artifact": "dfc_source.raw_timeseries",
                    "dependency_role": "source_input",
                },
            },
            "outputs": {
                "clean_timeseries": {
                    "extension": ".npy",
                    "address_scope": "entity",
                },
            },
        },
        "dfc_group_summary": {
            "step_name": SELECTED_STEP_NAME,
            "step_contract_version": "1",
            "pattern_kind": "analysis",
            "execution_role": "analysis",
            "address_scope": "cohort",
            "callable": (
                "nipact.examples.dynamic_functional_connectivity_demo.runtime:"
                "group_connectivity_summary_file"
            ),
            "manifest_binding": {
                "role": "analysis_cohort",
                "manifest": ANALYSIS_COHORT_NAME,
            },
            "inputs": {
                "clean_timeseries": {
                    "artifact": "dfc_clean_timeseries.clean_timeseries",
                    "dependency_role": "analysis_input",
                },
            },
            "outputs": {
                SELECTED_OUTPUT_NAME: {
                    "extension": ".json",
                    "address_scope": "cohort",
                },
            },
        },
    }


def workflow_files() -> dict[str, dict[str, Any]]:
    return {
        BASE_WORKFLOW_NAME: {
            "workflow_name": BASE_WORKFLOW_NAME,
            "steps": [
                "dfc_source",
                "dfc_clean_timeseries",
                {
                    "step_name": SELECTED_STEP_NAME,
                    "output_name": SELECTED_OUTPUT_NAME,
                },
            ],
        },
    }


def project_readme_text() -> str:
    return (
        "# NIPACT Synthetic DFC Demo Project\n\n"
        "Generated by `nipact init --demo dfc`.\n\n"
        "This is a tiny generated fixture for exercising NIPACT source bindings, "
        "NumPy artifacts, and cohort-level analysis input collection.\n"
    )


def _write_npy(path: Path, array: np.ndarray) -> None:
    with path.open("wb") as handle:
        np.save(handle, array)
