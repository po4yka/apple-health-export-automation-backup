"""Activity and fitness transformers."""

from influxdb_client import Point

from ..types import JSONObject
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
    "walking_running_distance": "distance_m",
    "walkingRunningDistance": "distance_m",
    "exercise_time": "exercise_min",
    "exerciseTime": "exercise_min",
    "apple_exercise_time": "exercise_min",
    "appleExerciseTime": "exercise_min",
    "stand_time": "stand_min",
    "standTime": "stand_min",
    "apple_stand_time": "stand_min",
    "appleStandTime": "stand_min",
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
        lower = metric_name.lower()
        return lower in {k.lower() for k in ACTIVITY_METRICS} or any(
            keyword in lower
            for keyword in ["energy", "exercise", "stand", "flight", "walking_running"]
        )

    def transform(self, data: JSONObject) -> list[Point]:
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
                field_name = self._lookup_field(metric_name, ACTIVITY_METRICS)

                point = (
                    Point(self.measurement)
                    .tag("source", self._get_source(item))
                    .field(field_name, float(metric.qty))
                    .time(metric.date)
                )

                points.append(point)

            except Exception as e:
                self._log_transform_error(e, item)
                continue

        return points
