"""Mobility and walking analysis transformer."""

from influxdb_client import Point

from .base import BaseTransformer, HealthMetric
from ..types import JSONObject

# Metrics that map to mobility measurement
MOBILITY_METRICS = {
    # Walking speed
    "walking_speed": "speed_mps",
    "walkingSpeed": "speed_mps",
    # Step length
    "walking_step_length": "step_length_cm",
    "walkingStepLength": "step_length_cm",
    # Asymmetry
    "walking_asymmetry_percentage": "asymmetry_pct",
    "walkingAsymmetryPercentage": "asymmetry_pct",
    # Double support
    "walking_double_support_percentage": "double_support_pct",
    "walkingDoubleSupportPercentage": "double_support_pct",
    # Stair speed
    "stair_speed_up": "stair_ascent_speed",
    "stairSpeedUp": "stair_ascent_speed",
    "stair_speed_down": "stair_descent_speed",
    "stairSpeedDown": "stair_descent_speed",
    # Six-minute walk
    "six_minute_walk_test_distance": "six_min_walk_m",
    "sixMinuteWalkTestDistance": "six_min_walk_m",
    # Walking steadiness
    "walking_steadiness": "steadiness_pct",
    "walkingSteadiness": "steadiness_pct",
}


class MobilityTransformer(BaseTransformer):
    """Transformer for walking analysis and mobility metrics."""

    measurement = "mobility"

    def can_transform(self, metric_name: str) -> bool:
        """Check if this is a mobility-related metric."""
        lower = metric_name.lower()
        return lower in {k.lower() for k in MOBILITY_METRICS} or any(
            keyword in lower
            for keyword in [
                "walking_speed",
                "walking_step",
                "walking_asymmetry",
                "walking_double",
                "walking_steadiness",
                "stair_speed",
                "six_minute_walk",
            ]
        )

    def transform(self, data: JSONObject) -> list[Point]:
        """Transform mobility metric data to InfluxDB points."""
        points = []

        metrics = data.get("data", [data]) if "data" in data else [data]

        for item in metrics:
            try:
                metric = HealthMetric.model_validate(item)
                if metric.qty is None:
                    continue

                metric_name = metric.name.lower().replace(" ", "_")
                field_name = self._lookup_field(metric_name, MOBILITY_METRICS)

                value = float(metric.qty)

                # Percentage normalization: if value <= 1, treat as fraction
                pct_fields = ("asymmetry_pct", "double_support_pct", "steadiness_pct")
                if field_name in pct_fields and value <= 1:
                    value = value * 100

                point = (
                    Point(self.measurement)
                    .tag("source", self._get_source(item))
                    .field(field_name, value)
                    .time(metric.date)
                )

                points.append(point)

            except Exception as e:
                self._log_transform_error(e, item)
                continue

        return points
