"""Heart rate and HRV transformers."""

from typing import Any

from influxdb_client import Point

from .base import BaseTransformer, HealthMetric

# Metrics that map to heart measurement
HEART_METRICS = {
    "heart_rate": "bpm",
    "heartRate": "bpm",
    "resting_heart_rate": "resting_bpm",
    "restingHeartRate": "resting_bpm",
    "heart_rate_variability": "hrv_ms",
    "heartRateVariabilitySDNN": "hrv_ms",
    "hrv": "hrv_ms",
}


class HeartTransformer(BaseTransformer):
    """Transformer for heart rate and HRV metrics."""

    measurement = "heart"

    def can_transform(self, metric_name: str) -> bool:
        """Check if this is a heart-related metric."""
        return metric_name.lower() in [k.lower() for k in HEART_METRICS] or any(
            keyword in metric_name.lower()
            for keyword in ["heart", "hrv", "pulse"]
        )

    def transform(self, data: dict[str, Any]) -> list[Point]:
        """Transform heart metric data to InfluxDB points."""
        points = []

        # Handle both single metrics and arrays
        metrics = data.get("data", [data]) if "data" in data else [data]

        for item in metrics:
            try:
                metric = HealthMetric.model_validate(item)
                if metric.qty is None:
                    continue

                # Determine field name
                metric_name = metric.name.lower().replace(" ", "_")
                field_name = self._lookup_field(metric_name, HEART_METRICS, default="bpm")

                point = (
                    Point(self.measurement)
                    .tag("source", self._get_source(item))
                    .field(field_name, float(metric.qty))
                    .time(metric.date)
                )

                # Add min/max/avg if available
                if metric.min is not None:
                    point.field(f"{field_name}_min", float(metric.min))
                if metric.max is not None:
                    point.field(f"{field_name}_max", float(metric.max))
                if metric.avg is not None:
                    point.field(f"{field_name}_avg", float(metric.avg))

                points.append(point)

            except Exception as e:
                self._log_transform_error(e, item)
                continue

        return points
