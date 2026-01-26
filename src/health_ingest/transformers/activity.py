"""Activity and fitness transformers."""

from typing import Any

from influxdb_client import Point

from .base import BaseTransformer, HealthMetric

# Metrics that map to activity measurement
ACTIVITY_METRICS = {
    "step_count": "steps",
    "stepCount": "steps",
    "steps": "steps",
    "active_energy": "active_calories",
    "activeEnergy": "active_calories",
    "active_energy_burned": "active_calories",
    "activeEnergyBurned": "active_calories",
    "basal_energy_burned": "basal_calories",
    "basalEnergyBurned": "basal_calories",
    "distance_walking_running": "distance_m",
    "distanceWalkingRunning": "distance_m",
    "exercise_time": "exercise_min",
    "exerciseTime": "exercise_min",
    "apple_exercise_time": "exercise_min",
    "appleExerciseTime": "exercise_min",
    "stand_time": "stand_min",
    "standTime": "stand_min",
    "stand_hour": "stand_hours",
    "standHour": "stand_hours",
    "apple_stand_hour": "stand_hours",
    "appleStandHour": "stand_hours",
    "flights_climbed": "floors_climbed",
    "flightsClimbed": "floors_climbed",
}


class ActivityTransformer(BaseTransformer):
    """Transformer for activity and fitness metrics."""

    measurement = "activity"

    def can_transform(self, metric_name: str) -> bool:
        """Check if this is an activity-related metric."""
        return metric_name.lower() in [k.lower() for k in ACTIVITY_METRICS] or any(
            keyword in metric_name.lower()
            for keyword in ["step", "energy", "exercise", "stand", "flight", "distance"]
        )

    def transform(self, data: dict[str, Any]) -> list[Point]:
        """Transform activity metric data to InfluxDB points."""
        points = []

        metrics = data.get("data", [data]) if "data" in data else [data]

        for item in metrics:
            try:
                metric = HealthMetric.model_validate(item)
                if metric.qty is None:
                    continue

                # Determine field name
                metric_name = metric.name.lower().replace(" ", "_")
                field_name = "value"  # default fallback

                for key, field in ACTIVITY_METRICS.items():
                    if key.lower() in metric_name or metric_name in key.lower():
                        field_name = field
                        break

                point = (
                    Point(self.measurement)
                    .tag("source", self._get_source(item))
                    .field(field_name, float(metric.qty))
                    .time(metric.date)
                )

                points.append(point)

            except Exception:
                continue

        return points
