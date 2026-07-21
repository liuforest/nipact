"""Project template declarations for the synthetic fMRI demo."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np

from ...manifest import Manifest, build_manifest

SUPPORTED_DEMO = "fmri"
SOURCE_INDEX_PATH = "sources.yaml"
ANALYSIS_COHORT_NAME = "init"
BASE_WORKFLOW_NAME = "base"
SELECTED_STEP_NAME = "fmri_registration"
SELECTED_OUTPUT_NAME = "registration_qc"

ENTITY_IDS = ("sub_001", "sub_002")
MANIFEST_PATHS = {
    ANALYSIS_COHORT_NAME: "manifests/init.yaml",
}
MANIFEST_DESCRIPTIONS = {
    ANALYSIS_COHORT_NAME: "Synthetic fMRI preprocessing cohort",
}


def source_file_paths() -> list[str]:
    paths: list[str] = []
    for entity_id in ENTITY_IDS:
        paths.extend(
            [
                f"data/fmri/{entity_id}_bold.npy",
                f"data/fmri/{entity_id}_t1.npy",
            ]
        )
    return paths


def write_runtime_sources(runtime_root: Path) -> None:
    source_dir = runtime_root / "data/fmri"
    source_dir.mkdir(parents=True, exist_ok=True)
    for index, entity_id in enumerate(ENTITY_IDS):
        bold = np.arange(24, dtype=np.float64).reshape(2, 3, 4) + float(index)
        t1 = np.arange(6, dtype=np.float64).reshape(2, 3) + (10.0 * (index + 1))
        _write_npy(source_dir / f"{entity_id}_bold.npy", bold)
        _write_npy(source_dir / f"{entity_id}_t1.npy", t1)


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
                "bold": f"data/fmri/{entity_id}_bold.npy",
                "t1": f"data/fmri/{entity_id}_t1.npy",
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
        "fmri_source": {
            "step_name": "fmri_source",
            "step_contract_version": "1",
            "pattern_kind": "pattern_a",
            "execution_role": "source_import",
            "address_scope": "entity",
            "callable": (
                "nipact.examples.fmri_preprocessing_demo.runtime:"
                "import_fmri_sources_file"
            ),
            "source_inputs": ["bold", "t1"],
            "manifest_binding": {
                "role": "source_population",
                "manifest": ANALYSIS_COHORT_NAME,
            },
            "outputs": {
                "raw_bold": {
                    "extension": ".npy",
                    "address_scope": "entity",
                },
                "raw_t1": {
                    "extension": ".npy",
                    "address_scope": "entity",
                },
            },
        },
        "fmri_registration": {
            "step_name": "fmri_registration",
            "step_contract_version": "1",
            "pattern_kind": "pattern_a",
            "execution_role": "transform",
            "address_scope": "entity",
            "callable": (
                "nipact.examples.fmri_preprocessing_demo.runtime:"
                "fmri_registration_file"
            ),
            "inputs": {
                "raw_bold": {
                    "artifact": "fmri_source.raw_bold",
                    "dependency_role": "source_input",
                },
                "raw_t1": {
                    "artifact": "fmri_source.raw_t1",
                    "dependency_role": "source_input",
                },
            },
            "outputs": {
                "registered_bold": {
                    "extension": ".npy",
                    "address_scope": "entity",
                },
                "forward_transform": {
                    "extension": ".npy",
                    "address_scope": "entity",
                },
                "inverse_transform": {
                    "extension": ".npy",
                    "address_scope": "entity",
                },
                "brain_mask": {
                    "extension": ".npy",
                    "address_scope": "entity",
                },
                "registration_qc": {
                    "extension": ".json",
                    "address_scope": "entity",
                },
            },
        },
    }


def workflow_files() -> dict[str, dict[str, Any]]:
    return {
        BASE_WORKFLOW_NAME: {
            "workflow_name": BASE_WORKFLOW_NAME,
            "steps": [
                "fmri_source",
                {
                    "step_name": SELECTED_STEP_NAME,
                    "output_name": SELECTED_OUTPUT_NAME,
                },
            ],
        },
    }


def project_readme_text() -> str:
    return (
        "# NIPACT Synthetic fMRI Demo Project\n\n"
        "Generated by `nipact init --demo fmri`.\n\n"
        "This is a tiny generated fixture for exercising NIPACT source bindings, "
        "NumPy artifacts, and multi-output workflow registration.\n"
    )


def _write_npy(path: Path, array: np.ndarray) -> None:
    with path.open("wb") as handle:
        np.save(handle, array)
