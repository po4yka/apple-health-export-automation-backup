"""Base transformer class and common models."""

import re
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

import structlog
from influxdb_client import Point
from pydantic import BaseModel, Field, field_validator

logger = structlog.get_logger(__name__)


# Regex to normalize Health Auto Export date format:
# "2022-06-12 23:59:00 +0400" -> "2022-06-12T23:59:00+04:00"
_DATE_SPACE_TZ_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\s(\d{2}:\d{2}:\d{2})\s([+-])(\d{2})(\d{2})$")


def _normalize_date(value: Any) -> Any:
    """Normalize date strings from Health Auto Export format to ISO 8601."""
    if not isinstance(value, str):
        return value
    m = _DATE_SPACE_TZ_RE.match(value)
    if m:
        return f"{m[1]}T{m[2]}{m[3]}{m[4]}:{m[5]}"
    return value


class HealthMetric(BaseModel):
    """Base model for health metrics from Health Auto Export."""

    name: str = Field(description="Metric name/type")
    date: datetime = Field(description="Timestamp of the measurement")
    qty: float | None = Field(default=None, description="Quantity value")
    source: str | None = Field(default=None, description="Source of the measurement")
    units: str | None = Field(default=None, description="Unit of measurement")

    # Optional fields that may be present in certain metrics
    min: float | None = Field(default=None, description="Minimum value")
    max: float | None = Field(default=None, description="Maximum value")
    avg: float | None = Field(default=None, description="Average value")

    @field_validator("date", mode="before")
    @classmethod
    def normalize_date(cls, v: Any) -> Any:
        return _normalize_date(v)


class WorkoutMetric(BaseModel):
    """Model for workout metrics."""

    name: str = Field(description="Workout type")
    start: datetime = Field(description="Workout start time")
    end: datetime = Field(description="Workout end time")
    duration: float | None = Field(default=None, description="Duration in minutes")
    activeEnergy: float | None = Field(default=None, alias="activeEnergy")
    distance: float | None = Field(default=None, description="Distance in meters")
    avgHeartRate: float | None = Field(default=None, alias="avgHeartRate")
    maxHeartRate: float | None = Field(default=None, alias="maxHeartRate")
    source: str | None = Field(default=None)

    @field_validator("start", "end", mode="before")
    @classmethod
    def normalize_date(cls, v: Any) -> Any:
        return _normalize_date(v)


class SleepAnalysis(BaseModel):
    """Model for sleep analysis data."""

    date: datetime = Field(description="Sleep date")
    sleepStart: datetime | None = Field(default=None, alias="sleepStart")
    sleepEnd: datetime | None = Field(default=None, alias="sleepEnd")
    inBed: float | None = Field(default=None, description="Time in bed (minutes)")
    asleep: float | None = Field(default=None, description="Time asleep (minutes)")
    deep: float | None = Field(default=None, description="Deep sleep (minutes)")
    rem: float | None = Field(default=None, description="REM sleep (minutes)")
    core: float | None = Field(default=None, description="Core sleep (minutes)")
    awake: float | None = Field(default=None, description="Awake time (minutes)")
    source: str | None = Field(default=None)

    @field_validator("date", "sleepStart", "sleepEnd", mode="before")
    @classmethod
    def normalize_date(cls, v: Any) -> Any:
        return _normalize_date(v)


class BaseTransformer(ABC):
    """Base class for metric transformers."""

    measurement: str  # InfluxDB measurement name

    def __init__(self, default_source: str = "health_auto_export") -> None:
        """Initialize transformer with default source."""
        self._default_source = default_source

    @abstractmethod
    def can_transform(self, metric_name: str) -> bool:
        """Check if this transformer can handle the given metric name."""
        pass

    @abstractmethod
    def transform(self, data: dict[str, Any]) -> list[Point]:
        """Transform raw data into InfluxDB points.

        Args:
            data: Raw metric data from Health Auto Export.

        Returns:
            List of InfluxDB Point objects.
        """
        pass

    def _get_source(self, data: dict[str, Any]) -> str:
        """Extract source from data or use default."""
        return self._sanitize_tag(data.get("source") or self._default_source)

    def _sanitize_tag(self, value: str, max_length: int = 256) -> str:
        """Sanitize a tag value to prevent injection and cardinality issues.

        Args:
            value: Raw tag value.
            max_length: Maximum allowed length.

        Returns:
            Sanitized tag value with only allowed characters.
        """
        if not value:
            return "unknown"
        # Allow only alphanumeric, underscore, hyphen, and dot
        sanitized = re.sub(r"[^a-zA-Z0-9_.\-]", "_", str(value))
        return sanitized[:max_length]

    def _lookup_field(
        self,
        metric_name: str,
        field_map: dict[str, str],
        default: str = "value",
    ) -> str:
        """Look up the InfluxDB field name for a metric.

        Uses normalized exact matching instead of substring containment.
        """
        if metric_name in field_map:
            return field_map[metric_name]
        lower = metric_name.lower()
        for key, field in field_map.items():
            if key.lower() == lower:
                return field
        return default

    def _log_transform_error(
        self,
        error: Exception,
        item: dict[str, Any],
        context: str | None = None,
    ) -> None:
        """Log a transformation error with context.

        Args:
            error: The exception that occurred.
            item: The data item that failed to transform.
            context: Additional context about the error.
        """
        logger.warning(
            "transform_failed",
            transformer=self.__class__.__name__,
            measurement=self.measurement,
            error=str(error),
            error_type=type(error).__name__,
            metric_name=item.get("name"),
            metric_date=str(item.get("date", "unknown")),
            context=context,
        )
