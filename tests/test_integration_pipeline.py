"""Integration tests: HTTP payload -> transformer -> dedup -> points returned.

Uses real TransformerRegistry and DeduplicationCache; only InfluxDB writer is mocked.
"""

from unittest.mock import AsyncMock

import pytest
from influxdb_client import Point

from health_ingest.dedup import DeduplicationCache
from health_ingest.transformers import TransformerRegistry


@pytest.fixture
def registry() -> TransformerRegistry:
    """Create a real TransformerRegistry."""
    return TransformerRegistry()


@pytest.fixture
def dedup() -> DeduplicationCache:
    """Create a real DeduplicationCache."""
    return DeduplicationCache(max_size=1000, ttl_hours=1)


def _simulate_pipeline(
    registry: TransformerRegistry,
    dedup: DeduplicationCache,
    payload: dict,
) -> list[Point]:
    """Simulate the processing pipeline: transform -> dedup -> return points.

    This mirrors _process_message in main.py without the InfluxDB write step.
    """
    points = registry.transform(payload)
    if not points:
        return []
    points = dedup.filter_duplicates(points)
    return points


# --- Realistic payloads ---

FLAT_LIST_PAYLOAD = {
    "data": [
        {
            "name": "heart_rate",
            "date": "2026-01-30T10:00:00+00:00",
            "qty": 72,
            "source": "Apple Watch",
        },
        {
            "name": "heart_rate",
            "date": "2026-01-30T10:05:00+00:00",
            "qty": 75,
            "source": "Apple Watch",
        },
        {
            "name": "step_count",
            "date": "2026-01-30T23:59:00+00:00",
            "qty": 10523,
            "source": "iPhone",
        },
    ]
}

REST_API_PAYLOAD = {
    "data": {
        "metrics": [
            {
                "name": "heart_rate",
                "units": "bpm",
                "data": [
                    {
                        "date": "2026-01-30T10:00:00+00:00",
                        "qty": 72,
                        "source": "Apple Watch",
                    },
                    {
                        "date": "2026-01-30T10:05:00+00:00",
                        "qty": 75,
                        "source": "Apple Watch",
                    },
                ],
            },
            {
                "name": "step_count",
                "units": "count",
                "data": [
                    {
                        "date": "2026-01-30T23:59:00+00:00",
                        "qty": 10523,
                        "source": "iPhone",
                    },
                ],
            },
        ]
    }
}

SINGLE_METRIC_PAYLOAD = {
    "name": "heart_rate",
    "date": "2026-01-30T12:00:00+00:00",
    "qty": 80,
    "source": "Apple Watch",
}

SLEEP_PAYLOAD = {
    "data": [
        {
            "name": "sleep_analysis",
            "date": "2026-01-30T07:00:00+00:00",
            "asleep": 420,
            "inBed": 480,
            "deep": 90,
            "rem": 120,
            "core": 210,
            "awake": 30,
            "sleepStart": "2026-01-29T23:00:00+00:00",
            "sleepEnd": "2026-01-30T07:00:00+00:00",
            "source": "Apple Watch",
        }
    ]
}

WORKOUT_PAYLOAD = {
    "data": [
        {
            "name": "HKWorkoutActivityTypeRunning",
            "start": "2026-01-30T07:00:00+00:00",
            "end": "2026-01-30T07:45:00+00:00",
            "duration": 45,
            "activeEnergy": 350,
            "distance": 5200,
            "avgHeartRate": 155,
            "maxHeartRate": 175,
            "source": "Apple Watch",
        }
    ]
}


class TestFlatListFormat:
    """Tests with the flat list payload format: {"data": [...]}."""

    def test_flat_list_produces_points(self, registry, dedup):
        """A flat list payload produces InfluxDB Points for each metric."""
        points = _simulate_pipeline(registry, dedup, FLAT_LIST_PAYLOAD)

        assert len(points) >= 2  # At least heart_rate and step_count points

    def test_flat_list_heart_rate_measurement(self, registry, dedup):
        """Heart rate entries in flat list are transformed to 'heart' measurement."""
        points = _simulate_pipeline(registry, dedup, FLAT_LIST_PAYLOAD)

        heart_points = [p for p in points if p._name == "heart"]
        assert len(heart_points) >= 1

    def test_flat_list_step_count_measurement(self, registry, dedup):
        """Step count entries in flat list are transformed to 'activity' measurement."""
        points = _simulate_pipeline(registry, dedup, FLAT_LIST_PAYLOAD)

        activity_points = [p for p in points if p._name == "activity"]
        assert len(activity_points) >= 1

    def test_flat_list_source_tag_sanitized(self, registry, dedup):
        """Source tags have spaces replaced with underscores."""
        points = _simulate_pipeline(registry, dedup, FLAT_LIST_PAYLOAD)

        heart_points = [p for p in points if p._name == "heart"]
        for p in heart_points:
            assert " " not in p._tags.get("source", ""), "Source tag should not contain spaces"


class TestRESTAPIFormat:
    """Tests with the REST API nested format: {"data": {"metrics": [...]}}."""

    def test_rest_api_produces_points(self, registry, dedup):
        """A REST API format payload produces InfluxDB Points."""
        points = _simulate_pipeline(registry, dedup, REST_API_PAYLOAD)

        assert len(points) >= 2

    def test_rest_api_heart_rate_measurement(self, registry, dedup):
        """Heart rate series in REST API format are transformed correctly."""
        points = _simulate_pipeline(registry, dedup, REST_API_PAYLOAD)

        heart_points = [p for p in points if p._name == "heart"]
        assert len(heart_points) >= 1

    def test_rest_api_step_count_measurement(self, registry, dedup):
        """Step count series in REST API format are transformed correctly."""
        points = _simulate_pipeline(registry, dedup, REST_API_PAYLOAD)

        activity_points = [p for p in points if p._name == "activity"]
        assert len(activity_points) >= 1


class TestSingleMetricPayload:
    """Tests with a single metric payload (no wrapping data array)."""

    def test_single_metric_produces_point(self, registry, dedup):
        """A single metric dict (no 'data' wrapper) produces a point."""
        points = _simulate_pipeline(registry, dedup, SINGLE_METRIC_PAYLOAD)

        assert len(points) == 1
        assert points[0]._name == "heart"


class TestSleepAndWorkout:
    """Tests for sleep and workout payloads through the pipeline."""

    def test_sleep_payload_produces_sleep_point(self, registry, dedup):
        """Sleep analysis data produces a 'sleep' measurement point."""
        points = _simulate_pipeline(registry, dedup, SLEEP_PAYLOAD)

        sleep_points = [p for p in points if p._name == "sleep"]
        assert len(sleep_points) >= 1

    def test_workout_payload_produces_workout_point(self, registry, dedup):
        """Workout data produces a 'workout' measurement point."""
        points = _simulate_pipeline(registry, dedup, WORKOUT_PAYLOAD)

        workout_points = [p for p in points if p._name == "workout"]
        assert len(workout_points) == 1
        assert workout_points[0]._tags["workout_type"] == "running"


class TestDeduplication:
    """Tests that deduplication works correctly in the pipeline."""

    def test_duplicate_payload_filtered(self, registry, dedup):
        """Processing the same payload twice returns zero points the second time."""
        first_points = _simulate_pipeline(registry, dedup, FLAT_LIST_PAYLOAD)
        assert len(first_points) > 0

        # Mark first batch as processed (simulating what happens after InfluxDB write)
        dedup.mark_processed_batch(first_points)

        # Second run with the same payload
        second_points = _simulate_pipeline(registry, dedup, FLAT_LIST_PAYLOAD)
        assert len(second_points) == 0

    def test_different_payloads_not_filtered(self, registry, dedup):
        """Different payloads are not filtered by dedup."""
        payload_a = {
            "name": "heart_rate",
            "date": "2026-01-30T10:00:00+00:00",
            "qty": 72,
            "source": "Apple Watch",
        }
        payload_b = {
            "name": "heart_rate",
            "date": "2026-01-30T10:00:00+00:00",
            "qty": 80,  # Different value
            "source": "Apple Watch",
        }

        points_a = _simulate_pipeline(registry, dedup, payload_a)
        dedup.mark_processed_batch(points_a)

        points_b = _simulate_pipeline(registry, dedup, payload_b)

        assert len(points_a) == 1
        assert len(points_b) == 1

    def test_partial_duplicate_mixed_payload(self, registry, dedup):
        """A payload with some new and some duplicate entries filters correctly."""
        # Process first batch
        first_batch = {
            "data": [
                {
                    "name": "heart_rate",
                    "date": "2026-01-30T10:00:00+00:00",
                    "qty": 72,
                    "source": "Apple Watch",
                },
            ]
        }
        first_points = _simulate_pipeline(registry, dedup, first_batch)
        dedup.mark_processed_batch(first_points)

        # Second batch has the same entry plus a new one
        mixed_batch = {
            "data": [
                {
                    "name": "heart_rate",
                    "date": "2026-01-30T10:00:00+00:00",
                    "qty": 72,
                    "source": "Apple Watch",
                },
                {
                    "name": "heart_rate",
                    "date": "2026-01-30T10:10:00+00:00",
                    "qty": 78,
                    "source": "Apple Watch",
                },
            ]
        }
        second_points = _simulate_pipeline(registry, dedup, mixed_batch)

        # Only the new entry should survive
        assert len(second_points) == 1


class TestEmptyAndInvalidPayloads:
    """Tests for edge case payloads through the pipeline."""

    def test_empty_data_array(self, registry, dedup):
        """An empty data array produces no points."""
        payload = {"data": []}
        points = _simulate_pipeline(registry, dedup, payload)

        assert len(points) == 0

    def test_empty_metrics_list(self, registry, dedup):
        """An empty metrics list in REST API format produces no points."""
        payload = {"data": {"metrics": []}}
        points = _simulate_pipeline(registry, dedup, payload)

        assert len(points) == 0

    def test_payload_without_name_skipped(self, registry, dedup):
        """Entries without a 'name' field are skipped."""
        payload = {
            "data": [
                {
                    "date": "2026-01-30T10:00:00+00:00",
                    "qty": 72,
                    # No "name" field
                },
            ]
        }
        points = _simulate_pipeline(registry, dedup, payload)

        assert len(points) == 0


class TestEndToEndWithMockedWriter:
    """Integration test that mocks only the InfluxDB writer."""

    async def test_full_pipeline_mock_writer(self, registry, dedup):
        """Full pipeline: transform -> dedup -> mock write -> mark processed."""
        mock_writer = AsyncMock()

        # Simulate _process_message logic
        payload = FLAT_LIST_PAYLOAD
        points = registry.transform(payload)
        assert len(points) > 0

        points = dedup.filter_duplicates(points)
        assert len(points) > 0

        # Mock the InfluxDB write
        await mock_writer.write(points)
        mock_writer.write.assert_awaited_once_with(points)

        # Mark processed
        dedup.mark_processed_batch(points)

        # Verify duplicates are now detected
        points_again = registry.transform(payload)
        filtered = dedup.filter_duplicates(points_again)
        assert len(filtered) == 0
