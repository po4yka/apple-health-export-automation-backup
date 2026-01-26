"""Base transformer class and common models."""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from influxdb_client import Point
from pydantic import BaseModel, Field


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
        return data.get("source") or self._default_source
