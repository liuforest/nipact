"""Project template declarations for the synthetic directory-output demo."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np

from ...manifest import Manifest, build_manifest

SUPPORTED_DEMO = "directories"
SOURCE_INDEX_PATH = "sources.yaml"
ANALYSIS_COHORT_NAME = "init"
BASE_WORKFLOW_NAME = "base"
VARIANT_WORKFLOW_NAME = "low-label"
SELECTED_STEP_NAME = "map_summary"
SELECTED_OUTPUT_NAME = "summary"

ENTITY_IDS = ("sub_001", "sub_002")
MANIFEST_PATHS = {
    ANALYSIS_COHORT_NAME: "manifests/init.yaml",
}
MANIFEST_DESCRIPTIONS = {
    ANALYSIS_COHORT_NAME: "Synthetic directory-output cohort",
}


def source_file_paths() -> list[str]:
    return [f"data/directories/{entity_id}_image.npy" for entity_id in ENTITY_IDS]


def write_runtime_sources(runtime_root: Path) -> None:
    source_dir = runtime_root / "data/directories"
    source_dir.mkdir(parents=True, exist_ok=True)
    for index, entity_id in enumerate(ENTITY_IDS):
        image = np.arange(16, dtype=np.float64).reshape(4, 4) + float(index)
        _write_npy(source_dir / f"{entity_id}_image.npy", image)


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
                "image": f"data/directories/{entity_id}_image.npy",
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
            VARIANT_WORKFLOW_NAME: "workflows/low-label.yaml",
        },
        "steps": {
            "directory": "steps",
        },
        "manifests": manifest_paths(),
    }


def step_files() -> dict[str, dict[str, Any]]:
    return {
        "image_source": {
            "step_name": "image_source",
            "step_contract_version": "1",
            "pattern_kind": "pattern_a",
            "execution_role": "source_import",
            "address_scope": "entity",
            "callable": (
                "nipact.examples.directory_outputs_demo.runtime:"
                "import_image_file"
            ),
            "source_inputs": ["image"],
            "outputs": {
                "raw_image": {
                    "extension": ".npy",
                    "address_scope": "entity",
                },
            },
        },
        "segment_image": {
            "step_name": "segment_image",
            "step_contract_version": "1",
            "pattern_kind": "pattern_a",
            "execution_role": "transform",
            "address_scope": "entity",
            "callable": (
                "nipact.examples.directory_outputs_demo.runtime:"
                "segment_image_directory"
            ),
            "params": {
                "low_threshold": 4.0,
                "high_threshold": 10.0,
            },
            "inputs": {
                "raw_image": {
                    "artifact": "image_source.raw_image",
                    "dependency_role": "source_input",
                },
            },
            "outputs": {
                "volumes": {
                    "extension": ".json",
                    "address_scope": "entity",
                },
                "maps": {
                    "kind": "directory",
                    "address_scope": "entity",
                },
            },
        },
        "map_summary": {
            "step_name": SELECTED_STEP_NAME,
            "step_contract_version": "1",
            "pattern_kind": "analysis",
            "execution_role": "analysis",
            "address_scope": "cohort",
            "callable": (
                "nipact.examples.directory_outputs_demo.runtime:"
                "summarize_maps_file"
            ),
            "params": {
                "label": "high",
            },
            "manifest_binding": {
                "role": "analysis_cohort",
                "manifest": ANALYSIS_COHORT_NAME,
            },
            "inputs": {
                "maps": {
                    "artifact": "segment_image.maps",
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
            "execution_population": ANALYSIS_COHORT_NAME,
            "steps": [
                "image_source",
                {
                    "step_name": "segment_image",
                    "output_name": "maps",
                },
                {
                    "step_name": SELECTED_STEP_NAME,
                    "output_name": SELECTED_OUTPUT_NAME,
                },
            ],
        },
        VARIANT_WORKFLOW_NAME: {
            "workflow_name": VARIANT_WORKFLOW_NAME,
            "base_workflow": BASE_WORKFLOW_NAME,
            "step_overrides": {
                SELECTED_STEP_NAME: {
                    "params": {
                        "label": "low",
                    },
                },
            },
        },
    }


def project_readme_text() -> str:
    return (
        "# NIPACT Synthetic Directory Demo Project\n\n"
        "Generated by `nipact init --demo directories`.\n\n"
        "This is a tiny generated fixture for exercising mixed file and directory "
        "outputs, provider validation of directory members, and reuse of directory "
        "artifacts by a cohort-level analysis.\n"
    )


def _write_npy(path: Path, array: np.ndarray) -> None:
    with path.open("wb") as handle:
        np.save(handle, array)
