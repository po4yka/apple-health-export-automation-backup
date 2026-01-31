"""Schema validation for incoming health metrics using Pandera."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd
import pandera as pa

_BASE_SCHEMA = pa.DataFrameSchema(
    {
        "name": pa.Column(str, nullable=False, coerce=True),
        "date": pa.Column(pa.DateTime, nullable=False, coerce=True),
        "qty": pa.Column(float, nullable=True, required=False, coerce=True),
        "source": pa.Column(str, nullable=True, required=False, coerce=True),
        "units": pa.Column(str, nullable=True, required=False, coerce=True),
        "min": pa.Column(float, nullable=True, required=False, coerce=True),
        "max": pa.Column(float, nullable=True, required=False, coerce=True),
        "avg": pa.Column(float, nullable=True, required=False, coerce=True),
    },
    coerce=True,
    strict=False,
)

_WORKOUT_SCHEMA = pa.DataFrameSchema(
    {
        "name": pa.Column(str, nullable=False, coerce=True),
        "start": pa.Column(pa.DateTime, nullable=False, coerce=True),
        "end": pa.Column(pa.DateTime, nullable=False, coerce=True),
        "duration": pa.Column(float, nullable=True, required=False, coerce=True),
        "activeEnergy": pa.Column(float, nullable=True, required=False, coerce=True),
        "distance": pa.Column(float, nullable=True, required=False, coerce=True),
        "avgHeartRate": pa.Column(float, nullable=True, required=False, coerce=True),
        "maxHeartRate": pa.Column(float, nullable=True, required=False, coerce=True),
        "source": pa.Column(str, nullable=True, required=False, coerce=True),
    },
    coerce=True,
    strict=False,
)


@dataclass(frozen=True)
class ValidationFailure:
    """Represents a schema validation failure for a metric item."""

    item: dict[str, Any]
    schema: str
    error: str


class MetricSchemaValidator:
    """Validate metric payloads before transformation and ingestion."""

    def _schema_for_item(self, item: dict[str, Any]) -> tuple[pa.DataFrameSchema, str]:
        if "start" in item or "end" in item:
            return _WORKOUT_SCHEMA, "workout"
        return _BASE_SCHEMA, "base"

    def validate_items(
        self, items: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], list[ValidationFailure]]:
        """Validate items and separate valid from invalid metrics.

        Args:
            items: List of metric dictionaries.

        Returns:
            Tuple of (valid_items, failures).
        """
        valid: list[dict[str, Any]] = []
        failures: list[ValidationFailure] = []

        for item in items:
            if not isinstance(item, dict):
                failures.append(
                    ValidationFailure(
                        item={"value": item},
                        schema="base",
                        error="metric item is not a dictionary",
                    )
                )
                continue

            schema, schema_name = self._schema_for_item(item)
            try:
                schema.validate(pd.DataFrame([item]), lazy=True)
            except (pa.errors.SchemaError, pa.errors.SchemaErrors) as exc:
                failures.append(
                    ValidationFailure(
                        item=item,
                        schema=schema_name,
                        error=str(exc),
                    )
                )
                continue

            valid.append(item)

        return valid, failures


_validator: MetricSchemaValidator | None = None


def get_metric_validator() -> MetricSchemaValidator:
    """Get a singleton schema validator."""
    global _validator
    if _validator is None:
        _validator = MetricSchemaValidator()
    return _validator
