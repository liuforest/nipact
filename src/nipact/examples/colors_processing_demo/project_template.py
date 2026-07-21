"""Project template declarations for the colors demo."""

from __future__ import annotations

from typing import Any

from ...errors import ValidationError
from ...manifest import Manifest, build_manifest
from .demo_names import (
    ANALYSIS_COHORT_NAME,
    BASE_WORKFLOW_NAME,
    FIT_COHORT_NAME,
    analysis_entity_ids,
    fit_cohort_entity_ids,
)
from .model import (
    DEFAULT_ANGULAR_BINS,
    DEFAULT_ENTITY_COUNT,
    DEFAULT_QC_TARGET_RADIUS,
    DEFAULT_RADIUS_BINS,
    DEFAULT_SEED,
    DEFAULT_VALUE,
    GREEN_THETA,
    build_source_population,
)

SUPPORTED_DEMO = "colors"
SOURCE_ARTIFACT_PATH = "data/color_source.json"
SOURCE_INDEX_PATH = "sources.yaml"
SOURCE_BINDING_NAME = "colors_source"
SOURCE_ENTITY_COUNT = DEFAULT_ENTITY_COUNT
ANALYSIS_STEP_NAME = "color_sector_analysis"
ANALYSIS_OUTPUT_NAME = "sector_counts"

MANIFEST_PATHS = {
    ANALYSIS_COHORT_NAME: "manifests/init.yaml",
    FIT_COHORT_NAME: "manifests/demo-40.yaml",
}
MANIFEST_DESCRIPTIONS = {
    ANALYSIS_COHORT_NAME: "Full deterministic colors source population",
    FIT_COHORT_NAME: "Outer two radial bins used for the Pattern B fit cohort",
}


def source_payload() -> dict[str, Any]:
    records = []
    for point in build_source_population():
        payload = point.to_payload()
        payload["rgb"] = list(point.rgb)
        records.append(payload)
    return {
        "metadata": {
            "demo": SUPPORTED_DEMO,
            "generator": "deterministic_color_grid",
            "angular_bins": DEFAULT_ANGULAR_BINS,
            "radius_bins": DEFAULT_RADIUS_BINS,
            "value": DEFAULT_VALUE,
            "seed": DEFAULT_SEED,
            "entity_count": DEFAULT_ENTITY_COUNT,
        },
        "records": records,
    }


def build_manifests() -> dict[str, Manifest]:
    return {
        ANALYSIS_COHORT_NAME: build_manifest(
            description=MANIFEST_DESCRIPTIONS[ANALYSIS_COHORT_NAME],
            entities=analysis_entity_ids(),
        ),
        FIT_COHORT_NAME: build_manifest(
            description=MANIFEST_DESCRIPTIONS[FIT_COHORT_NAME],
            entities=fit_cohort_entity_ids(),
        ),
    }


def validate_source_payload(
    payload: dict[str, Any],
    *,
    manifests: dict[str, Manifest],
) -> int:
    records = payload.get("records")
    if not isinstance(records, list):
        raise ValidationError("source data records must be a list")
    entity_ids = [
        record.get("entity_id")
        for record in records
        if isinstance(record, dict)
    ]
    analysis_manifest = manifests[ANALYSIS_COHORT_NAME]
    expected_ids = list(analysis_manifest.entity_ids)
    if entity_ids != expected_ids:
        raise ValidationError("source data entity IDs do not match colors demo manifest")
    metadata = payload.get("metadata")
    if (
        not isinstance(metadata, dict)
        or metadata.get("entity_count") != analysis_manifest.entity_count
    ):
        raise ValidationError("source data metadata has invalid entity count")
    source_ids = set(entity_ids)
    for name, manifest in manifests.items():
        missing = sorted(set(manifest.entity_ids) - source_ids)
        if missing:
            preview = ", ".join(missing[:5])
            raise ValidationError(
                f"manifest {name!r} references unknown source entity_id values: {preview}"
            )
    return len(entity_ids)


def manifest_paths() -> dict[str, str]:
    return dict(MANIFEST_PATHS)


def source_index_payload() -> dict[str, Any]:
    return {
        "global": {
            SOURCE_BINDING_NAME: SOURCE_ARTIFACT_PATH,
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
            "base": "workflows/base.yaml",
            "red-qc-target": "workflows/red-qc-target.yaml",
        },
        "steps": {
            "directory": "steps",
        },
        "manifests": manifest_paths(),
    }


def step_files() -> dict[str, dict[str, Any]]:
    return {
        "color_source": {
            "step_name": "color_source",
            "step_contract_version": "1",
            "pattern_kind": "pattern_a",
            "execution_role": "source_import",
            "address_scope": "entity",
            "callable": "nipact.examples.colors_processing_demo.runtime:import_color_source_file",
            "source_inputs": [SOURCE_BINDING_NAME],
            "params": {
                "angular_bins": DEFAULT_ANGULAR_BINS,
                "radius_bins": DEFAULT_RADIUS_BINS,
                "value": DEFAULT_VALUE,
                "seed": DEFAULT_SEED,
            },
            "outputs": {
                "source_color": {
                    "extension": ".json",
                    "address_scope": "entity",
                },
            },
        },
        "color_features": {
            "step_name": "color_features",
            "step_contract_version": "1",
            "pattern_kind": "pattern_a",
            "execution_role": "transform",
            "address_scope": "entity",
            "callable": "nipact.examples.colors_processing_demo.runtime:extract_color_features_file",
            "inputs": {
                "source_color": {
                    "artifact": "color_source.source_color",
                    "dependency_role": "source_input",
                },
            },
            "outputs": {
                "features": {
                    "extension": ".json",
                    "address_scope": "entity",
                },
            },
        },
        "color_local_transform": {
            "step_name": "color_local_transform",
            "step_contract_version": "1",
            "pattern_kind": "pattern_a",
            "execution_role": "transform",
            "address_scope": "entity",
            "callable": "nipact.examples.colors_processing_demo.runtime:color_local_transform_file",
            "inputs": {
                "features": {
                    "artifact": "color_features.features",
                    "dependency_role": "source_input",
                },
            },
            "params": {
                "target_theta": GREEN_THETA,
                "force": 0.18,
                "radius_gate": 0.35,
            },
            "outputs": {
                "local_color": {
                    "extension": ".json",
                    "address_scope": "entity",
                },
            },
        },
        "color_candidate_select": {
            "step_name": "color_candidate_select",
            "step_contract_version": "1",
            "pattern_kind": "pattern_a",
            "execution_role": "transform",
            "address_scope": "entity",
            "callable": "nipact.examples.colors_processing_demo.runtime:color_candidate_select_file",
            "inputs": {
                "local_color": {
                    "artifact": "color_local_transform.local_color",
                    "dependency_role": "source_input",
                },
            },
            "params": {
                "qc_target_theta": GREEN_THETA,
                "qc_target_radius": DEFAULT_QC_TARGET_RADIUS,
            },
            "outputs": {
                "selected_color": {
                    "extension": ".json",
                    "address_scope": "entity",
                },
            },
        },
        "color_cohort_fit": {
            "step_name": "color_cohort_fit",
            "step_contract_version": "1",
            "pattern_kind": "pattern_b",
            "execution_role": "b_fit",
            "address_scope": "cohort",
            "callable": "nipact.examples.colors_processing_demo.runtime:color_cohort_fit_file",
            "manifest_binding": {
                "role": "fit_cohort",
                "manifest": FIT_COHORT_NAME,
            },
            "inputs": {
                "selected_color": {
                    "artifact": "color_candidate_select.selected_color",
                    "dependency_role": "fit_input",
                },
            },
            "outputs": {
                "cohort_fit": {
                    "extension": ".json",
                    "address_scope": "cohort",
                },
            },
        },
        "color_cohort_apply": {
            "step_name": "color_cohort_apply",
            "step_contract_version": "1",
            "pattern_kind": "pattern_b",
            "execution_role": "b_apply",
            "address_scope": "entity",
            "callable": "nipact.examples.colors_processing_demo.runtime:color_cohort_apply_file",
            "inputs": {
                "selected_color": {
                    "artifact": "color_candidate_select.selected_color",
                    "dependency_role": "apply_input",
                },
                "cohort_fit": {
                    "artifact": "color_cohort_fit.cohort_fit",
                    "dependency_role": "collective_fit",
                },
            },
            "params": {"apply_strength": 0.18},
            "outputs": {
                "cohort_color": {
                    "extension": ".json",
                    "address_scope": "entity",
                },
            },
        },
        "color_sector_label": {
            "step_name": "color_sector_label",
            "step_contract_version": "1",
            "pattern_kind": "pattern_a",
            "execution_role": "transform",
            "address_scope": "entity",
            "callable": "nipact.examples.colors_processing_demo.runtime:color_sector_label_file",
            "inputs": {
                "cohort_color": {
                    "artifact": "color_cohort_apply.cohort_color",
                    "dependency_role": "source_input",
                },
            },
            "params": {
                "arc_half_width": 0.5235987755982988,
                "min_radius": 0.35,
            },
            "outputs": {
                "sector_label": {
                    "extension": ".json",
                    "address_scope": "entity",
                },
            },
        },
        "color_sector_analysis": {
            "step_name": ANALYSIS_STEP_NAME,
            "step_contract_version": "1",
            "pattern_kind": "analysis",
            "execution_role": "analysis",
            "address_scope": "cohort",
            "callable": "nipact.examples.colors_processing_demo.runtime:color_sector_analysis_file",
            "manifest_binding": {
                "role": "analysis_cohort",
                "manifest": ANALYSIS_COHORT_NAME,
            },
            "inputs": {
                "sector_label": {
                    "artifact": "color_sector_label.sector_label",
                    "dependency_role": "analysis_input",
                },
            },
            "params": {
                "arc_half_width": 0.5235987755982988,
                "min_radius": 0.35,
            },
            "outputs": {
                ANALYSIS_OUTPUT_NAME: {
                    "extension": ".json",
                    "address_scope": "cohort",
                },
            },
        },
    }


def workflow_files() -> dict[str, dict[str, Any]]:
    return {
        "base": {
            "workflow_name": BASE_WORKFLOW_NAME,
            "execution_population": ANALYSIS_COHORT_NAME,
            "steps": [
                {"step_name": "color_source"},
                {"step_name": "color_features"},
                {
                    "step_name": "color_local_transform",
                    "output_name": "local_color",
                },
                {
                    "step_name": "color_candidate_select",
                    "output_name": "selected_color",
                },
                {
                    "step_name": "color_cohort_fit",
                    "output_name": "cohort_fit",
                },
                {
                    "step_name": "color_cohort_apply",
                    "output_name": "cohort_color",
                },
                {"step_name": "color_sector_label"},
                {
                    "step_name": ANALYSIS_STEP_NAME,
                    "output_name": ANALYSIS_OUTPUT_NAME,
                },
            ],
        },
        "red-qc-target": {
            "workflow_name": "red-qc-target",
            "base_workflow": BASE_WORKFLOW_NAME,
            "step_overrides": {
                "color_candidate_select": {
                    "params": {
                        "qc_target_theta": 0.0,
                    },
                },
            },
        },
    }


def project_readme_text() -> str:
    return (
        "# NIPACT Colors Demo Project\n\n"
        "Generated by `nipact init --demo colors`.\n\n"
        "This demo project can be validated, inspected with `nipact workflow` "
        "commands, and run through Snakemake with `nipact workflow run`.\n"
    )
