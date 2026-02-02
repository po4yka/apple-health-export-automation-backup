"""Sleep analysis transformer."""

from datetime import datetime

import structlog
from influxdb_client import Point

from ..types import JSONObject
from .base import BaseTransformer, SleepAnalysis

logger = structlog.get_logger(__name__)

# Maximum plausible sleep duration in minutes (24 hours)
_MAX_DURATION_MIN = 1440.0


class SleepTransformer(BaseTransformer):
    """Transformer for sleep analysis metrics."""

    measurement = "sleep"

    def can_transform(self, metric_name: str) -> bool:
        """Check if this is a sleep-related metric."""
        return any(keyword in metric_name.lower() for keyword in ["sleep", "inbed", "in_bed"])

    def transform(self, data: JSONObject) -> list[Point]:
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

    def _transform_sleep_analysis(self, item: JSONObject) -> list[Point]:
        """Transform aggregated sleep analysis data."""
        points = []

        try:
            sleep = SleepAnalysis.model_validate(item)

            # Health Auto Export sends units: "hr"; convert to minutes
            units = str(item.get("units", "min")).lower()
            mul = 60.0 if units == "hr" else 1.0

            point = Point(self.measurement).tag("source", self._get_source(item))
            has_fields = False

            # totalSleep is the actual total; asleep is just the unspecified stage
            total = sleep.totalSleep if sleep.totalSleep is not None else sleep.asleep
            if total is not None:
                duration_min = float(total) * mul
                if duration_min <= 0 or duration_min > _MAX_DURATION_MIN:
                    logger.warning(
                        "sleep_duration_out_of_range",
                        duration_min=duration_min,
                        date=str(item.get("date", "unknown")),
                    )
                    return points
                point.field("duration_min", duration_min)
                has_fields = True

            # Stage fields: skip individual field if out of range, don't reject whole point
            for attr, field_name in [
                ("deep", "deep_min"),
                ("rem", "rem_min"),
                ("core", "core_min"),
                ("awake", "awake_min"),
                ("inBed", "in_bed_min"),
            ]:
                val = getattr(sleep, attr)
                if val is not None:
                    converted = float(val) * mul
                    if converted < 0 or converted > _MAX_DURATION_MIN:
                        logger.warning(
                            "sleep_field_out_of_range",
                            field=field_name,
                            value=converted,
                            date=str(item.get("date", "unknown")),
                        )
                        continue
                    point.field(field_name, converted)
                    has_fields = True

            # Calculate sleep quality score if we have enough data
            if total and sleep.inBed and sleep.inBed > 0:
                quality = min((total / sleep.inBed) * 100, 100.0)
                point.field("quality_score", round(quality, 1))
                has_fields = True

            # Cross-field consistency: warn if stages exceed total by >5%
            if total is not None and total > 0:
                stage_sum = sum(
                    getattr(sleep, attr) or 0 for attr in ("deep", "rem", "core", "awake")
                )
                if stage_sum > total * 1.05:
                    logger.warning(
                        "sleep_stages_exceed_total",
                        stage_sum=stage_sum,
                        total=total,
                        date=str(item.get("date", "unknown")),
                    )

            # Skip empty points (all values were None or out of range)
            if not has_fields:
                return points

            # Use sleepStart as timestamp so sub-sessions don't overwrite each other
            timestamp = sleep.sleepStart if sleep.sleepStart else sleep.date
            point.time(timestamp)
            points.append(point)

        except Exception as e:
            logger.warning(
                "sleep_analysis_transform_failed",
                error=str(e),
                error_type=type(e).__name__,
                date=str(item.get("date", "unknown")),
            )

        return points

    def _transform_sleep_stage(self, item: JSONObject) -> list[Point]:
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

            # Health Auto Export may send units: "hr"; convert to minutes
            units = str(item.get("units", "min")).lower()
            mul = 60.0 if units == "hr" else 1.0

            converted = float(qty) * mul
            if converted < 0 or converted > _MAX_DURATION_MIN:
                logger.warning(
                    "sleep_stage_out_of_range",
                    name=item.get("name"),
                    value=converted,
                    date=str(item.get("date", "unknown")),
                )
                return points

            point = Point(self.measurement).tag("source", self._get_source(item))

            # Map sleep stage to field
            if "asleep" in name and "deep" in name:
                point.field("deep_min", converted)
            elif "asleep" in name and "rem" in name:
                point.field("rem_min", converted)
            elif "asleep" in name and "core" in name:
                point.field("core_min", converted)
            elif "awake" in name:
                point.field("awake_min", converted)
            elif "inbed" in name or "in_bed" in name:
                point.field("in_bed_min", converted)
            elif "asleep" in name:
                point.field("duration_min", converted)
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
