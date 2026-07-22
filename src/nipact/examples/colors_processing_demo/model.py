"""Pure model for the deterministic colors-processing demo artifacts."""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
import colorsys
import math
from typing import Sequence

from ...manifest import build_manifest_value

TAU = 2 * math.pi
DEFAULT_SEED = 20260519
DEFAULT_ANGULAR_BINS = 20
DEFAULT_RADIUS_BINS = 10
DEFAULT_ENTITY_COUNT = DEFAULT_ANGULAR_BINS * DEFAULT_RADIUS_BINS
DEFAULT_VALUE = 0.95
MIN_SOURCE_RADIUS = 0.15
MAX_SOURCE_RADIUS = 1.0

RED_THETA = 0.0
YELLOW_THETA = math.pi / 3
GREEN_THETA = 2 * math.pi / 3
BLUE_THETA = 4 * math.pi / 3
DEFAULT_ARC_HALF_WIDTH = math.pi / 6
DEFAULT_MIN_ANALYSIS_RADIUS = 0.35
DEFAULT_QC_TARGET_RADIUS = 0.80

RED_SECTOR = "red_sector"
GREEN_SECTOR = "green_sector"
BLUE_SECTOR = "blue_sector"
OTHER_SECTOR = "other"


@dataclass(frozen=True)
class ColorPoint:
    entity_id: str
    index: int
    theta: float
    radius: float
    hue_degrees: float
    saturation: float
    value: float
    x: float
    y: float
    rgb: tuple[int, int, int]
    hex_color: str

    def to_payload(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class CandidateResult:
    candidate_id: str
    transform_name: str
    params: dict[str, float]
    theta: float
    radius: float
    x: float
    y: float
    rgb: tuple[int, int, int]
    hex_color: str
    score: float
    selected: bool

    def to_payload(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class CandidateSelection:
    entity_id: str
    input_state: ColorPoint
    selection_rule: dict[str, object]
    candidate_results: tuple[CandidateResult, ...]
    selected_candidate_id: str
    selected_state: ColorPoint

    def to_payload(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class CohortFit:
    cohort_theta_centroid: float
    cohort_radius_centroid: float
    cohort_x_centroid: float
    cohort_y_centroid: float
    cohort_entity_count: int
    cohort_manifest_digest: str

    def to_payload(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class SectorCounts:
    analysis_manifest_digest: str
    entity_count: int
    red_arc_count: int
    green_arc_count: int
    blue_arc_count: int
    other_count: int
    red_fraction: float
    green_fraction: float
    blue_fraction: float
    red_minus_green: int

    def to_payload(self) -> dict[str, object]:
        return asdict(self)


def clamp01(value: float) -> float:
    return min(1.0, max(0.0, value))


def wrap_angle(theta: float) -> float:
    return theta % TAU


def shortest_periodic_angle(delta: float) -> float:
    return ((delta + math.pi) % TAU) - math.pi


def bounded_weight(radius: float, radius_gate: float) -> float:
    radius = clamp01(radius)
    radius_gate = clamp01(radius_gate)
    if radius <= radius_gate or radius_gate >= 1.0:
        return 0.0
    return (radius - radius_gate) / (1.0 - radius_gate)


def polar_to_xy(theta: float, radius: float) -> tuple[float, float]:
    theta = wrap_angle(theta)
    radius = clamp01(radius)
    return radius * math.cos(theta), radius * math.sin(theta)


def xy_to_polar(x: float, y: float) -> tuple[float, float]:
    radius = clamp01(math.hypot(x, y))
    if radius == 0.0:
        return 0.0, 0.0
    return wrap_angle(math.atan2(y, x)), radius


def hsv_to_rgb(theta: float, radius: float, value: float = DEFAULT_VALUE) -> tuple[int, int, int]:
    red, green, blue = colorsys.hsv_to_rgb(wrap_angle(theta) / TAU, clamp01(radius), clamp01(value))
    return round(red * 255), round(green * 255), round(blue * 255)


def rgb_to_hex(rgb: tuple[int, int, int]) -> str:
    return "#{:02x}{:02x}{:02x}".format(*rgb)


def entity_id(index: int) -> str:
    if index < 0:
        raise ValueError("colors-processing-demo index must be non-negative")
    return f"color_{index:03d}"


def color_point(
    index: int,
    *,
    angular_bins: int = DEFAULT_ANGULAR_BINS,
    radius_bins: int = DEFAULT_RADIUS_BINS,
    value: float = DEFAULT_VALUE,
    min_radius: float = MIN_SOURCE_RADIUS,
    max_radius: float = MAX_SOURCE_RADIUS,
) -> ColorPoint:
    if angular_bins <= 0:
        raise ValueError("angular_bins must be positive")
    if radius_bins <= 0:
        raise ValueError("radius_bins must be positive")
    entity_count = angular_bins * radius_bins
    if index < 0 or index >= entity_count:
        raise ValueError(f"index must be in [0, {entity_count})")

    angle_bin = index % angular_bins
    radius_bin = index // angular_bins
    theta = TAU * ((angle_bin + 0.5) / angular_bins)
    if radius_bins == 1:
        radius = (min_radius + max_radius) / 2
    else:
        radius = min_radius + ((max_radius - min_radius) * radius_bin / (radius_bins - 1))
    return make_point(entity_id(index), index, theta=theta, radius=radius, value=value)


def build_color_grid(
    *,
    angular_bins: int = DEFAULT_ANGULAR_BINS,
    radius_bins: int = DEFAULT_RADIUS_BINS,
    value: float = DEFAULT_VALUE,
) -> list[ColorPoint]:
    if angular_bins <= 0:
        raise ValueError("angular_bins must be positive")
    if radius_bins <= 0:
        raise ValueError("radius_bins must be positive")
    entity_count = angular_bins * radius_bins
    return [
        color_point(index, angular_bins=angular_bins, radius_bins=radius_bins, value=value)
        for index in range(entity_count)
    ]


def make_point(entity_id: str, index: int, *, theta: float, radius: float, value: float) -> ColorPoint:
    theta = wrap_angle(theta)
    radius = clamp01(radius)
    value = clamp01(value)
    x, y = polar_to_xy(theta, radius)
    rgb = hsv_to_rgb(theta, radius, value)
    return ColorPoint(
        entity_id=entity_id,
        index=index,
        theta=theta,
        radius=radius,
        hue_degrees=math.degrees(theta),
        saturation=radius,
        value=value,
        x=x,
        y=y,
        rgb=rgb,
        hex_color=rgb_to_hex(rgb),
    )


def angular_pull(
    point: ColorPoint,
    *,
    target_theta: float,
    force: float,
    radius_gate: float,
) -> ColorPoint:
    radius_weight = bounded_weight(point.radius, radius_gate)
    theta_delta = shortest_periodic_angle(wrap_angle(target_theta) - point.theta)
    theta_out = point.theta + clamp01(force) * radius_weight * theta_delta
    return make_point(
        point.entity_id,
        point.index,
        theta=theta_out,
        radius=point.radius,
        value=point.value,
    )


def candidate_select(
    point: ColorPoint,
    *,
    qc_target_theta: float,
    qc_target_radius: float = DEFAULT_QC_TARGET_RADIUS,
) -> CandidateSelection:
    target_x, target_y = polar_to_xy(qc_target_theta, qc_target_radius)
    attempts = (
        ("keep", "identity", {}, point),
        (
            "red_candidate",
            "angular_pull",
            {"target_theta": RED_THETA, "force": 0.35, "radius_gate": 0.35},
            angular_pull(point, target_theta=RED_THETA, force=0.35, radius_gate=0.35),
        ),
        (
            "yellow_candidate",
            "angular_pull",
            {"target_theta": YELLOW_THETA, "force": 0.35, "radius_gate": 0.35},
            angular_pull(point, target_theta=YELLOW_THETA, force=0.35, radius_gate=0.35),
        ),
        (
            "green_candidate",
            "angular_pull",
            {"target_theta": GREEN_THETA, "force": 0.35, "radius_gate": 0.35},
            angular_pull(point, target_theta=GREEN_THETA, force=0.35, radius_gate=0.35),
        ),
        (
            "blue_candidate",
            "angular_pull",
            {"target_theta": BLUE_THETA, "force": 0.35, "radius_gate": 0.35},
            angular_pull(point, target_theta=BLUE_THETA, force=0.35, radius_gate=0.35),
        ),
    )

    candidate_results = []
    for candidate_id, transform_name, params, candidate_state in attempts:
        score = math.hypot(candidate_state.x - target_x, candidate_state.y - target_y)
        candidate_results.append(
            CandidateResult(
                candidate_id=candidate_id,
                transform_name=transform_name,
                params=params,
                theta=candidate_state.theta,
                radius=candidate_state.radius,
                x=candidate_state.x,
                y=candidate_state.y,
                rgb=candidate_state.rgb,
                hex_color=candidate_state.hex_color,
                score=score,
                selected=False,
            )
        )

    selected_index = min(range(len(candidate_results)), key=lambda index: candidate_results[index].score)
    selected_results = tuple(
        replace(result, selected=(index == selected_index))
        for index, result in enumerate(candidate_results)
    )
    selected_result = selected_results[selected_index]
    return CandidateSelection(
        entity_id=point.entity_id,
        input_state=point,
        selection_rule={
            "name": "nearest_qc_target",
            "qc_target_theta": wrap_angle(qc_target_theta),
            "qc_target_radius": clamp01(qc_target_radius),
            "score": "euclidean_distance(candidate_xy, qc_target_xy)",
        },
        candidate_results=selected_results,
        selected_candidate_id=selected_result.candidate_id,
        selected_state=make_point(
            point.entity_id,
            point.index,
            theta=selected_result.theta,
            radius=selected_result.radius,
            value=point.value,
        ),
    )


def fit_cohort(points: Sequence[ColorPoint]) -> CohortFit:
    if not points:
        raise ValueError("fit_cohort requires at least one point")
    x_centroid = sum(point.x for point in points) / len(points)
    y_centroid = sum(point.y for point in points) / len(points)
    theta_centroid, radius_centroid = xy_to_polar(x_centroid, y_centroid)
    return CohortFit(
        cohort_theta_centroid=theta_centroid,
        cohort_radius_centroid=radius_centroid,
        cohort_x_centroid=x_centroid,
        cohort_y_centroid=y_centroid,
        cohort_entity_count=len(points),
        cohort_manifest_digest=build_manifest_value(
            entities=(point.entity_id for point in points)
        ).manifest_digest,
    )


def apply_cohort_fit(point: ColorPoint, fit: CohortFit, *, apply_strength: float) -> ColorPoint:
    apply_strength = clamp01(apply_strength)
    x = point.x + apply_strength * (fit.cohort_x_centroid - point.x)
    y = point.y + apply_strength * (fit.cohort_y_centroid - point.y)
    theta, radius = xy_to_polar(x, y)
    return make_point(point.entity_id, point.index, theta=theta, radius=radius, value=point.value)


def angle_in_arc(theta: float, *, center_theta: float, half_width: float) -> bool:
    return abs(shortest_periodic_angle(wrap_angle(theta) - wrap_angle(center_theta))) <= half_width


def sector_label(
    point: ColorPoint,
    *,
    arc_half_width: float = DEFAULT_ARC_HALF_WIDTH,
    min_radius: float = DEFAULT_MIN_ANALYSIS_RADIUS,
) -> str:
    if point.radius < min_radius:
        return OTHER_SECTOR
    if angle_in_arc(point.theta, center_theta=RED_THETA, half_width=arc_half_width):
        return RED_SECTOR
    if angle_in_arc(point.theta, center_theta=GREEN_THETA, half_width=arc_half_width):
        return GREEN_SECTOR
    if angle_in_arc(point.theta, center_theta=BLUE_THETA, half_width=arc_half_width):
        return BLUE_SECTOR
    return OTHER_SECTOR


def analyze_sectors(
    points: Sequence[ColorPoint],
    *,
    arc_half_width: float = DEFAULT_ARC_HALF_WIDTH,
    min_radius: float = DEFAULT_MIN_ANALYSIS_RADIUS,
) -> SectorCounts:
    labels = [
        sector_label(point, arc_half_width=arc_half_width, min_radius=min_radius)
        for point in points
    ]
    entity_count = len(points)
    red_count = labels.count(RED_SECTOR)
    green_count = labels.count(GREEN_SECTOR)
    blue_count = labels.count(BLUE_SECTOR)
    other_count = entity_count - red_count - green_count - blue_count
    return SectorCounts(
        analysis_manifest_digest=build_manifest_value(
            entities=(point.entity_id for point in points)
        ).manifest_digest,
        entity_count=entity_count,
        red_arc_count=red_count,
        green_arc_count=green_count,
        blue_arc_count=blue_count,
        other_count=other_count,
        red_fraction=red_count / entity_count if entity_count else 0.0,
        green_fraction=green_count / entity_count if entity_count else 0.0,
        blue_fraction=blue_count / entity_count if entity_count else 0.0,
        red_minus_green=red_count - green_count,
    )
