"""Pytest configuration and fixtures."""

from pathlib import Path
import sys

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))


@pytest.fixture
def sample_heart_rate_data():
    """Sample heart rate data from Health Auto Export."""
    return {
        "name": "heart_rate",
        "date": "2024-01-15T10:30:00+00:00",
        "qty": 72,
        "source": "Apple Watch",
        "units": "bpm",
    }


@pytest.fixture
def sample_steps_data():
    """Sample step count data."""
    return {
        "name": "step_count",
        "date": "2024-01-15T23:59:00+00:00",
        "qty": 10523,
        "source": "iPhone",
    }


@pytest.fixture
def sample_sleep_data():
    """Sample sleep analysis data."""
    return {
        "date": "2024-01-15T07:00:00+00:00",
        "sleepStart": "2024-01-14T23:00:00+00:00",
        "sleepEnd": "2024-01-15T07:00:00+00:00",
        "asleep": 420,
        "inBed": 480,
        "deep": 90,
        "rem": 120,
        "core": 210,
        "awake": 30,
        "source": "Apple Watch",
    }


@pytest.fixture
def sample_workout_data():
    """Sample workout data."""
    return {
        "name": "HKWorkoutActivityTypeRunning",
        "start": "2024-01-15T07:00:00+00:00",
        "end": "2024-01-15T07:45:00+00:00",
        "duration": 45,
        "activeEnergy": 350,
        "distance": 5200,
        "avgHeartRate": 155,
        "maxHeartRate": 175,
        "source": "Apple Watch",
    }


@pytest.fixture
def sample_body_data():
    """Sample body composition data."""
    return {
        "name": "body_mass",
        "date": "2024-01-15T08:00:00+00:00",
        "qty": 75.5,
        "units": "kg",
        "source": "Withings Scale",
    }


@pytest.fixture
def sample_batch_data():
    """Sample batch of metrics."""
    return {
        "data": [
            {
                "name": "heart_rate",
                "date": "2024-01-15T10:00:00+00:00",
                "qty": 70,
                "source": "Apple Watch",
            },
            {
                "name": "heart_rate",
                "date": "2024-01-15T10:05:00+00:00",
                "qty": 72,
                "source": "Apple Watch",
            },
            {
                "name": "heart_rate",
                "date": "2024-01-15T10:10:00+00:00",
                "qty": 75,
                "source": "Apple Watch",
            },
        ]
    }
