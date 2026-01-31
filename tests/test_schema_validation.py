"""Tests for Pandera schema validation."""

from health_ingest.schema_validation import get_metric_validator


def test_validate_items_accepts_valid_metric():
    validator = get_metric_validator()
    valid_items, failures = validator.validate_items(
        [
            {
                "name": "heart_rate",
                "date": "2024-01-15T10:30:00+00:00",
                "qty": 72,
            }
        ]
    )

    assert len(valid_items) == 1
    assert failures == []


def test_validate_items_rejects_missing_fields():
    validator = get_metric_validator()
    valid_items, failures = validator.validate_items(
        [
            {"name": "heart_rate", "qty": 72},
            {"date": "2024-01-15T10:30:00+00:00", "qty": 72},
        ]
    )

    assert valid_items == []
    assert len(failures) == 2


def test_validate_items_rejects_incomplete_workout():
    validator = get_metric_validator()
    valid_items, failures = validator.validate_items(
        [
            {
                "name": "HKWorkoutActivityTypeRunning",
                "start": "2024-01-15T07:00:00+00:00",
                "distance": 5200,
            }
        ]
    )

    assert valid_items == []
    assert len(failures) == 1
