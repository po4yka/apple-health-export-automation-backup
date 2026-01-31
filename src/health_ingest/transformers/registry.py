"""Transformer registry for routing metrics to appropriate transformers."""

from typing import Any

import structlog

from .activity import ActivityTransformer
from .audio import AudioTransformer
from .base import BaseTransformer
from .body import BodyTransformer
from .generic import GenericTransformer
from .heart import HeartTransformer
from .mobility import MobilityTransformer
from .sleep import SleepTransformer
from .vitals import VitalsTransformer
from .workout import WorkoutTransformer

logger = structlog.get_logger(__name__)


class TransformerRegistry:
    """Registry for metric transformers with priority-based routing."""

    def __init__(self, default_source: str = "health_auto_export") -> None:
        """Initialize registry with all available transformers."""
        self._default_source = default_source

        # Transformers in priority order (more specific first)
        self._transformers: list[BaseTransformer] = [
            HeartTransformer(default_source),
            MobilityTransformer(default_source),
            ActivityTransformer(default_source),
            SleepTransformer(default_source),
            WorkoutTransformer(default_source),
            BodyTransformer(default_source),
            VitalsTransformer(default_source),
            AudioTransformer(default_source),
            # Generic transformer is always last (catches everything)
            GenericTransformer(default_source),
        ]

    def get_transformer(self, metric_name: str) -> BaseTransformer:
        """Get the appropriate transformer for a metric name.

        Args:
            metric_name: The name/type of the metric.

        Returns:
            The first transformer that can handle this metric.
        """
        for transformer in self._transformers:
            if transformer.can_transform(metric_name):
                logger.debug(
                    "transformer_selected",
                    metric_name=metric_name,
                    transformer=transformer.__class__.__name__,
                )
                return transformer

        # Should never reach here as GenericTransformer accepts everything
        return self._transformers[-1]

    def _normalize_payload(self, data: dict[str, Any]) -> list[dict[str, Any]]:
        """Normalize payload into a flat list of individual metric dicts.

        Handles two formats from Health Auto Export:

        MQTT format (flat list):
            {"data": [{"name": "heart_rate", "date": "...", "qty": 72}]}

        REST API format (nested metrics):
            {"data": {"metrics": [{"name": "heart_rate", "units": "bpm",
                                   "data": [{"date": "...", "qty": 72}]}]}}
        """
        inner = data.get("data")

        # REST API format: {"data": {"metrics": [...]}}
        if isinstance(inner, dict) and "metrics" in inner:
            items: list[dict[str, Any]] = []
            for metric in inner["metrics"]:
                if not isinstance(metric, dict):
                    continue
                name = metric.get("name", "")
                units = metric.get("units", "")
                for point in metric.get("data", []):
                    if isinstance(point, dict):
                        item = {**point, "name": name}
                        if units:
                            item.setdefault("units", units)
                        items.append(item)
            return items

        # MQTT format: {"data": [...]}
        if isinstance(inner, list):
            base = {k: v for k, v in data.items() if k != "data"}
            return [{**base, **item} for item in inner if isinstance(item, dict)]

        # Single metric (no wrapping)
        return [data]

    def transform(self, data: dict[str, Any]) -> list:
        """Transform metric data using the appropriate transformer.

        Args:
            data: Raw metric data from Health Auto Export.

        Returns:
            List of InfluxDB Point objects.
        """
        items = self._normalize_payload(data)
        points: list = []

        for item in items:
            metric_name = self._extract_metric_name(item)
            if not metric_name:
                logger.warning("no_metric_name_found", data_keys=list(item.keys()))
                continue

            transformer = self.get_transformer(metric_name)
            points.extend(transformer.transform(item))

        return points

    def _extract_metric_name(self, data: dict[str, Any]) -> str | None:
        """Extract metric name from data payload."""
        # Try common field names
        for field in ["name", "type", "metric", "dataType"]:
            if field in data and data[field]:
                return str(data[field])

        # Try to get from nested data array
        if "data" in data and isinstance(data["data"], list) and data["data"]:
            first_item = data["data"][0]
            if isinstance(first_item, dict):
                return first_item.get("name")

        return None


# Global registry instance
_registry: TransformerRegistry | None = None


def get_transformer(
    metric_name: str, default_source: str = "health_auto_export"
) -> BaseTransformer:
    """Get a transformer for the given metric name.

    Args:
        metric_name: The name/type of the metric.
        default_source: Default source tag for metrics.

    Returns:
        The appropriate transformer.
    """
    global _registry
    if _registry is None:
        _registry = TransformerRegistry(default_source)
    return _registry.get_transformer(metric_name)
