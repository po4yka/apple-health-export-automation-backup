"""Generic transformer for unrecognized metrics."""

from datetime import datetime
from typing import Any

from influxdb_client import Point

from .base import BaseTransformer


class GenericTransformer(BaseTransformer):
    """Fallback transformer for metrics that don't match specific transformers."""

    measurement = "other"

    def can_transform(self, metric_name: str) -> bool:
        """Generic transformer accepts any metric."""
        return True

    def transform(self, data: dict[str, Any]) -> list[Point]:
        """Transform generic metric data to InfluxDB points."""
        points = []

        metrics = data.get("data", [data]) if "data" in data else [data]

        for item in metrics:
            try:
                name = item.get("name")
                qty = item.get("qty")
                date = item.get("date")

                if name is None or qty is None or date is None:
                    continue

                # Parse date if string
                if isinstance(date, str):
                    date = datetime.fromisoformat(date.replace("Z", "+00:00"))

                # Normalize and sanitize metric name for tag
                metric_type = self._sanitize_tag(self._normalize_metric_name(name))

                point = (
                    Point(self.measurement)
                    .tag("source", self._get_source(item))
                    .tag("metric_type", metric_type)
                    .field("value", float(qty))
                    .time(date)
                )

                # Add unit if available (sanitized)
                units = item.get("units")
                if units:
                    point.tag("unit", self._sanitize_tag(units))

                # Add min/max/avg if available
                if item.get("min") is not None:
                    point.field("min", float(item["min"]))
                if item.get("max") is not None:
                    point.field("max", float(item["max"]))
                if item.get("avg") is not None:
                    point.field("avg", float(item["avg"]))

                points.append(point)

            except Exception as e:
                self._log_transform_error(e, item, context="generic_transform")
                continue

        return points

    def _normalize_metric_name(self, name: str) -> str:
        """Normalize metric name to snake_case."""
        # Handle camelCase
        import re

        # Insert underscore before uppercase letters
        s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
        s2 = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1)

        # Replace spaces and hyphens with underscores
        result = s2.replace(" ", "_").replace("-", "_").lower()

        # Remove any double underscores
        while "__" in result:
            result = result.replace("__", "_")

        return result.strip("_")
