"""Vitals transformer for SpO2, respiratory rate, blood pressure, temperature."""

from typing import Any

from influxdb_client import Point

from .base import BaseTransformer, HealthMetric

# Metrics that map to vitals measurement
VITALS_METRICS = {
    "oxygen_saturation": "spo2_pct",
    "oxygenSaturation": "spo2_pct",
    "blood_oxygen": "spo2_pct",
    "bloodOxygen": "spo2_pct",
    "spo2": "spo2_pct",
    "respiratory_rate": "respiratory_rate",
    "respiratoryRate": "respiratory_rate",
    "blood_pressure_systolic": "bp_systolic",
    "bloodPressureSystolic": "bp_systolic",
    "systolic": "bp_systolic",
    "blood_pressure_diastolic": "bp_diastolic",
    "bloodPressureDiastolic": "bp_diastolic",
    "diastolic": "bp_diastolic",
    "body_temperature": "temp_c",
    "bodyTemperature": "temp_c",
    "temperature": "temp_c",
    "vo2max": "vo2max",
    "vo2Max": "vo2max",
}


class VitalsTransformer(BaseTransformer):
    """Transformer for vital signs metrics."""

    measurement = "vitals"

    def can_transform(self, metric_name: str) -> bool:
        """Check if this is a vitals metric."""
        return metric_name.lower() in [k.lower() for k in VITALS_METRICS] or any(
            keyword in metric_name.lower()
            for keyword in [
                "oxygen",
                "spo2",
                "respiratory",
                "blood_pressure",
                "bloodpressure",
                "systolic",
                "diastolic",
                "temperature",
                "vo2",
            ]
        )

    def transform(self, data: dict[str, Any]) -> list[Point]:
        """Transform vitals data to InfluxDB points."""
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

                for key, field in VITALS_METRICS.items():
                    if key.lower() in metric_name or metric_name in key.lower():
                        field_name = field
                        break

                # Unit conversions
                value = float(metric.qty)
                units = (metric.units or "").lower()

                # Convert Fahrenheit to Celsius for temperature
                if field_name == "temp_c" and ("f" in units or "fahrenheit" in units):
                    value = (value - 32) * 5 / 9

                # SpO2 should be percentage (0-100)
                if field_name == "spo2_pct" and value <= 1:
                    value = value * 100

                point = (
                    Point(self.measurement)
                    .tag("source", self._get_source(item))
                    .field(field_name, value)
                    .time(metric.date)
                )

                # Add min/max if available
                if metric.min is not None:
                    point.field(f"{field_name}_min", float(metric.min))
                if metric.max is not None:
                    point.field(f"{field_name}_max", float(metric.max))

                points.append(point)

            except Exception:
                continue

        return points
