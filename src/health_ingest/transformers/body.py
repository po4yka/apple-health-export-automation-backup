"""Body composition transformer."""

from influxdb_client import Point

from .base import BaseTransformer, HealthMetric
from ..types import JSONObject

# Metrics that map to body measurement
BODY_METRICS = {
    "body_mass": "weight_kg",
    "bodyMass": "weight_kg",
    "weight": "weight_kg",
    "body_fat_percentage": "body_fat_pct",
    "bodyFatPercentage": "body_fat_pct",
    "body_mass_index": "bmi",
    "bodyMassIndex": "bmi",
    "bmi": "bmi",
    "lean_body_mass": "lean_mass_kg",
    "leanBodyMass": "lean_mass_kg",
    "waist_circumference": "waist_cm",
    "waistCircumference": "waist_cm",
    "height": "height_cm",
}


class BodyTransformer(BaseTransformer):
    """Transformer for body composition metrics."""

    measurement = "body"

    def can_transform(self, metric_name: str) -> bool:
        """Check if this is a body composition metric."""
        return metric_name.lower() in [k.lower() for k in BODY_METRICS] or any(
            keyword in metric_name.lower()
            for keyword in ["body", "weight", "mass", "fat", "bmi", "lean", "waist", "height"]
        )

    def transform(self, data: JSONObject) -> list[Point]:
        """Transform body composition data to InfluxDB points."""
        points = []

        metrics = data.get("data", [data]) if "data" in data else [data]

        for item in metrics:
            try:
                metric = HealthMetric.model_validate(item)
                if metric.qty is None:
                    continue

                # Determine field name
                metric_name = metric.name.lower().replace(" ", "_")
                field_name = self._lookup_field(metric_name, BODY_METRICS)

                # Unit conversions
                value = float(metric.qty)
                units = (metric.units or "").lower()

                # Convert pounds to kg for weight
                if field_name == "weight_kg" and "lb" in units:
                    value = value * 0.453592

                # Convert inches to cm for height/waist
                if field_name in ("height_cm", "waist_cm") and "in" in units:
                    value = value * 2.54

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
