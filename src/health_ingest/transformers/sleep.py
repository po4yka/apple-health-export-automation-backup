"""Sleep analysis transformer."""

from datetime import datetime
from typing import Any

import structlog
from influxdb_client import Point

from .base import BaseTransformer, SleepAnalysis

logger = structlog.get_logger(__name__)


class SleepTransformer(BaseTransformer):
    """Transformer for sleep analysis metrics."""

    measurement = "sleep"

    def can_transform(self, metric_name: str) -> bool:
        """Check if this is a sleep-related metric."""
        return any(keyword in metric_name.lower() for keyword in ["sleep", "inbed", "in_bed"])

    def transform(self, data: dict[str, Any]) -> list[Point]:
        """Transform sleep data to InfluxDB points."""
        points = []

        # Handle both single records and arrays
        records = data.get("data", [data]) if "data" in data else [data]

        for item in records:
            try:
                # Try to parse as SleepAnalysis first (aggregated data)
                if "asleep" in item or "inBed" in item or "deep" in item:
                    points.extend(self._transform_sleep_analysis(item))
                else:
                    # Handle raw sleep stage data
                    points.extend(self._transform_sleep_stage(item))
            except Exception as e:
                self._log_transform_error(e, item, context="sleep_transform")
                continue

        return points

    def _transform_sleep_analysis(self, item: dict[str, Any]) -> list[Point]:
        """Transform aggregated sleep analysis data."""
        points = []

        try:
            sleep = SleepAnalysis.model_validate(item)

            point = Point(self.measurement).tag("source", self._get_source(item))

            # Add sleep duration fields
            if sleep.asleep is not None:
                point.field("duration_min", float(sleep.asleep))
            if sleep.deep is not None:
                point.field("deep_min", float(sleep.deep))
            if sleep.rem is not None:
                point.field("rem_min", float(sleep.rem))
            if sleep.core is not None:
                point.field("core_min", float(sleep.core))
            if sleep.awake is not None:
                point.field("awake_min", float(sleep.awake))
            if sleep.inBed is not None:
                point.field("in_bed_min", float(sleep.inBed))

            # Calculate sleep quality score if we have enough data
            if sleep.asleep and sleep.inBed and sleep.inBed > 0:
                quality = (sleep.asleep / sleep.inBed) * 100
                point.field("quality_score", round(quality, 1))

            point.time(sleep.date)
            points.append(point)

        except Exception as e:
            logger.warning(
                "sleep_analysis_transform_failed",
                error=str(e),
                error_type=type(e).__name__,
                date=str(item.get("date", "unknown")),
            )

        return points

    def _transform_sleep_stage(self, item: dict[str, Any]) -> list[Point]:
        """Transform individual sleep stage data."""
        points = []

        try:
            name = item.get("name", "").lower()
            qty = item.get("qty")
            date = item.get("date")

            if qty is None or date is None:
                return points

            # Parse date if string
            if isinstance(date, str):
                date = datetime.fromisoformat(date.replace("Z", "+00:00"))

            point = Point(self.measurement).tag("source", self._get_source(item))

            # Map sleep stage to field
            if "asleep" in name and "deep" in name:
                point.field("deep_min", float(qty))
            elif "asleep" in name and "rem" in name:
                point.field("rem_min", float(qty))
            elif "asleep" in name and "core" in name:
                point.field("core_min", float(qty))
            elif "awake" in name:
                point.field("awake_min", float(qty))
            elif "inbed" in name or "in_bed" in name:
                point.field("in_bed_min", float(qty))
            elif "asleep" in name:
                point.field("duration_min", float(qty))
            else:
                # Unknown sleep metric, store as generic
                return points

            point.time(date)
            points.append(point)

        except Exception as e:
            logger.warning(
                "sleep_stage_transform_failed",
                error=str(e),
                error_type=type(e).__name__,
                name=item.get("name"),
                date=str(item.get("date", "unknown")),
            )

        return points
