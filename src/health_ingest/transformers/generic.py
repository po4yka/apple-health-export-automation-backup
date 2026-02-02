"""Generic transformer for unrecognized metrics."""

import re
from datetime import datetime

import structlog
from influxdb_client import Point

from ..types import JSONObject
from .base import BaseTransformer

logger = structlog.get_logger(__name__)


class GenericTransformer(BaseTransformer):
    """Fallback transformer for metrics that don't match specific transformers."""

    measurement = "other"

    def can_transform(self, metric_name: str) -> bool:
        """Generic transformer accepts any metric."""
        return True

    def transform(self, data: JSONObject) -> list[Point]:
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
                metric_type = self._normalize_metric_name(name)

                # Validate metric name contains only safe characters
                if not metric_type or not self._SAFE_METRIC_RE.match(metric_type):
                    logger.warning(
                        "invalid_metric_name",
                        raw_name=name[: self._MAX_METRIC_NAME_LEN],
                        normalized=metric_type,
                    )
                    continue

                metric_type = self._sanitize_tag(metric_type)

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

    _MAX_METRIC_NAME_LEN = 200
    _SAFE_METRIC_RE = re.compile(r"^[a-zA-Z0-9_]+$")

    def _normalize_metric_name(self, name: str) -> str:
        """Normalize metric name to snake_case.

        Applies a length limit, converts camelCase to snake_case,
        collapses consecutive underscores, and strips unsafe characters.
        """
        # Truncate to prevent abuse before any processing
        truncated = name[: self._MAX_METRIC_NAME_LEN]

        # Insert underscore before uppercase letters (camelCase -> snake_case)
        s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", truncated)
        s2 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1)

        # Replace spaces and hyphens with underscores
        result = s2.replace(" ", "_").replace("-", "_").lower()

        # Collapse consecutive underscores in a single pass (avoids ReDoS)
        result = re.sub(r"_+", "_", result)

        # Strip leading/trailing underscores
        result = result.strip("_")

        # Remove any characters outside the safe set [a-zA-Z0-9_]
        result = re.sub(r"[^a-zA-Z0-9_]", "", result)

        return result
