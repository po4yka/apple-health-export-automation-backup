"""Heart rate and HRV transformers."""

import math

import structlog
from influxdb_client import Point

from ..types import JSONObject
from .base import BaseTransformer, HealthMetric

logger = structlog.get_logger(__name__)

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

# Physiological bounds per field
_FIELD_BOUNDS: dict[str, tuple[float, float]] = {
    "bpm": (20.0, 300.0),
    "resting_bpm": (20.0, 200.0),
    "hrv_ms": (0.0, 500.0),
}


class HeartTransformer(BaseTransformer):
    """Transformer for heart rate and HRV metrics."""

    measurement = "heart"

    def can_transform(self, metric_name: str) -> bool:
        """Check if this is a heart-related metric."""
        return metric_name.lower() in [k.lower() for k in HEART_METRICS] or any(
            keyword in metric_name.lower() for keyword in ["heart", "hrv", "pulse"]
        )

    def transform(self, data: JSONObject) -> list[Point]:
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

                lo, hi = _FIELD_BOUNDS.get(field_name, (0.0, math.inf))
                value = float(metric.qty)

                if not (lo <= value <= hi):
                    logger.warning(
                        "heart_value_out_of_range",
                        field=field_name,
                        value=value,
                        lo=lo,
                        hi=hi,
                    )
                    continue

                point = (
                    Point(self.measurement)
                    .tag("source", self._get_source(item))
                    .field(field_name, value)
                    .time(metric.date)
                )

                # Add min/max/avg if available and within bounds
                for suffix, stat_val in [
                    ("min", metric.min),
                    ("max", metric.max),
                    ("avg", metric.avg),
                ]:
                    if stat_val is not None:
                        sv = float(stat_val)
                        if lo <= sv <= hi:
                            point.field(f"{field_name}_{suffix}", sv)
                        else:
                            logger.warning(
                                "heart_stat_out_of_range",
                                field=field_name,
                                stat=suffix,
                                value=sv,
                                lo=lo,
                                hi=hi,
                            )

                points.append(point)

            except Exception as e:
                self._log_transform_error(e, item)
                continue

        return points
