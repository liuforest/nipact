"""Shared declaration constants for the colors-processing demo."""

from __future__ import annotations

from .model import DEFAULT_ANGULAR_BINS, DEFAULT_ENTITY_COUNT, DEFAULT_RADIUS_BINS

FIT_COHORT_NAME = "demo-40"
ANALYSIS_COHORT_NAME = "init"
BASE_WORKFLOW_NAME = "base"


def fit_cohort_entity_ids() -> list[str]:
    start_index = DEFAULT_ANGULAR_BINS * (DEFAULT_RADIUS_BINS - 2)
    return [f"color_{index:03d}" for index in range(start_index, DEFAULT_ENTITY_COUNT)]


def analysis_entity_ids() -> list[str]:
    return [f"color_{index:03d}" for index in range(DEFAULT_ENTITY_COUNT)]
