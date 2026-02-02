"""Vitals transformer for SpO2, respiratory rate, blood pressure, temperature."""

import math

import structlog
from influxdb_client import Point

from ..types import JSONObject
from .base import BaseTransformer, HealthMetric

logger = structlog.get_logger(__name__)

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
    "vo2_max": "vo2max",
    "blood_oxygen_saturation": "spo2_pct",
    "bloodOxygenSaturation": "spo2_pct",
}

# Physiological bounds per field
_FIELD_BOUNDS: dict[str, tuple[float, float]] = {
    "spo2_pct": (0.0, 100.0),
    "respiratory_rate": (1.0, 80.0),
    "bp_systolic": (40.0, 300.0),
    "bp_diastolic": (20.0, 200.0),
    "temp_c": (25.0, 45.0),
    "vo2max": (5.0, 100.0),
}

_FAHRENHEIT_UNITS = {"f", "degf", "fahrenheit"}


def _convert_temp(value: float, field_name: str, units: str) -> float:
    """Convert Fahrenheit to Celsius if needed."""
    if field_name == "temp_c" and units in _FAHRENHEIT_UNITS:
        return (value - 32) * 5 / 9
    return value


def _normalize_spo2(value: float, field_name: str) -> float:
    """Normalize SpO2 from fraction to percentage if needed."""
    if field_name == "spo2_pct" and value <= 1.0:
        return value * 100
    return value


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

    def transform(self, data: JSONObject) -> list[Point]:
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
                field_name = self._lookup_field(metric_name, VITALS_METRICS)

                # Unit conversions
                value = float(metric.qty)
                units = (metric.units or "").lower()

                value = _convert_temp(value, field_name, units)
                value = _normalize_spo2(value, field_name)

                # Range validation
                lo, hi = _FIELD_BOUNDS.get(field_name, (0.0, math.inf))
                if not (lo <= value <= hi):
                    logger.warning(
                        "vitals_value_out_of_range",
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

                # Add min/max with same conversions and validation
                for suffix, stat_val in [("min", metric.min), ("max", metric.max)]:
                    if stat_val is not None:
                        sv = float(stat_val)
                        sv = _convert_temp(sv, field_name, units)
                        sv = _normalize_spo2(sv, field_name)
                        if lo <= sv <= hi:
                            point.field(f"{field_name}_{suffix}", sv)
                        else:
                            logger.warning(
                                "vitals_stat_out_of_range",
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
