"""Workout transformer."""

from influxdb_client import Point

from .base import BaseTransformer, WorkoutMetric
from ..types import JSONObject


class WorkoutTransformer(BaseTransformer):
    """Transformer for workout/exercise data."""

    measurement = "workout"

    def can_transform(self, metric_name: str) -> bool:
        """Check if this is workout data."""
        return any(
            keyword in metric_name.lower() for keyword in ["workout", "exercise", "training"]
        )

    def transform(self, data: JSONObject) -> list[Point]:
        """Transform workout data to InfluxDB points."""
        points = []

        # Handle both single workouts and arrays
        workouts = data.get("data", [data]) if "data" in data else [data]

        for item in workouts:
            try:
                workout = WorkoutMetric.model_validate(item)

                workout_type = self._sanitize_tag(self._normalize_workout_type(workout.name))
                point = (
                    Point(self.measurement)
                    .tag("source", self._get_source(item))
                    .tag("workout_type", workout_type)
                )

                # Calculate duration if not provided
                duration = workout.duration
                if duration is None and workout.start and workout.end:
                    duration = (workout.end - workout.start).total_seconds() / 60

                if duration is not None:
                    point.field("duration_min", float(duration))

                if workout.activeEnergy is not None:
                    point.field("calories", float(workout.activeEnergy))

                if workout.distance is not None:
                    point.field("distance_m", float(workout.distance))

                if workout.avgHeartRate is not None:
                    point.field("avg_hr", float(workout.avgHeartRate))

                if workout.maxHeartRate is not None:
                    point.field("max_hr", float(workout.maxHeartRate))

                point.time(workout.start)
                points.append(point)

            except Exception as e:
                self._log_transform_error(e, item, context="workout_transform")
                continue

        return points

    def _normalize_workout_type(self, workout_name: str) -> str:
        """Normalize workout type to a consistent format."""
        # Remove common prefixes
        name = workout_name.lower()
        for prefix in ["hkworkoutactivitytype", "workout_"]:
            if name.startswith(prefix):
                name = name[len(prefix) :]

        # Common normalizations
        normalizations = {
            "traditionalstrengthtraining": "strength_training",
            "functionalstrengthtraining": "functional_training",
            "highintensityintervaltraining": "hiit",
            "running": "running",
            "walking": "walking",
            "cycling": "cycling",
            "swimming": "swimming",
            "yoga": "yoga",
            "pilates": "pilates",
            "elliptical": "elliptical",
            "rowing": "rowing",
            "stairclimbing": "stair_climbing",
            "coretraining": "core_training",
            "flexibility": "flexibility",
            "cooldown": "cooldown",
            "mindandbody": "mind_and_body",
        }

        return normalizations.get(name, name.replace(" ", "_"))
