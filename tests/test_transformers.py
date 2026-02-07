"""Tests for health data transformers."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from health_ingest.transformers import (
    ActivityTransformer,
    AudioTransformer,
    BodyTransformer,
    GenericTransformer,
    HeartTransformer,
    MobilityTransformer,
    SleepAnalysis,
    SleepTransformer,
    TransformerRegistry,
    VitalsTransformer,
    WorkoutTransformer,
)


class TestHeartTransformer:
    """Tests for HeartTransformer."""

    def setup_method(self):
        self.transformer = HeartTransformer()

    def test_can_transform_heart_rate(self):
        assert self.transformer.can_transform("heart_rate")
        assert self.transformer.can_transform("heartRate")
        assert self.transformer.can_transform("resting_heart_rate")
        assert self.transformer.can_transform("heartRateVariabilitySDNN")

    def test_cannot_transform_unrelated(self):
        # HeartTransformer checks for keywords, so most unrelated should fail
        assert not self.transformer.can_transform("step_count")
        assert not self.transformer.can_transform("body_mass")

    def test_transform_heart_rate(self):
        data = {
            "name": "heart_rate",
            "date": "2024-01-15T10:30:00+00:00",
            "qty": 72,
            "source": "Apple Watch",
        }

        points = self.transformer.transform(data)

        assert len(points) == 1
        point = points[0]
        assert point._name == "heart"
        # Source is sanitized: spaces become underscores
        assert point._tags["source"] == "Apple_Watch"

    def test_transform_hrv(self):
        data = {
            "name": "heartRateVariabilitySDNN",
            "date": "2024-01-15T10:30:00+00:00",
            "qty": 45.5,
            "source": "Apple Watch",
        }

        points = self.transformer.transform(data)

        assert len(points) == 1

    def test_transform_with_min_max(self):
        data = {
            "name": "heart_rate",
            "date": "2024-01-15T10:30:00+00:00",
            "qty": 72,
            "min": 55,
            "max": 120,
            "source": "Apple Watch",
        }

        points = self.transformer.transform(data)

        assert len(points) == 1

    def test_transform_heart_rate_value(self):
        data = {
            "name": "heart_rate",
            "date": "2024-01-15T10:30:00+00:00",
            "qty": 72.0,
            "source": "Apple Watch",
        }
        points = self.transformer.transform(data)
        assert len(points) == 1
        assert points[0]._fields["bpm"] == 72.0

    def test_transform_hrv_value(self):
        data = {
            "name": "heartRateVariabilitySDNN",
            "date": "2024-01-15T10:30:00+00:00",
            "qty": 45.5,
            "source": "Apple Watch",
        }
        points = self.transformer.transform(data)
        assert len(points) == 1
        assert points[0]._fields["hrv_ms"] == 45.5

    def test_negative_heart_rate_rejected(self):
        data = {
            "name": "heart_rate",
            "date": "2024-01-15T10:30:00+00:00",
            "qty": -5,
            "source": "Apple Watch",
        }
        points = self.transformer.transform(data)
        assert len(points) == 0

    def test_zero_heart_rate_rejected(self):
        data = {
            "name": "heart_rate",
            "date": "2024-01-15T10:30:00+00:00",
            "qty": 0,
            "source": "Apple Watch",
        }
        points = self.transformer.transform(data)
        assert len(points) == 0

    def test_extreme_heart_rate_rejected(self):
        data = {
            "name": "heart_rate",
            "date": "2024-01-15T10:30:00+00:00",
            "qty": 500,
            "source": "Apple Watch",
        }
        points = self.transformer.transform(data)
        assert len(points) == 0

    def test_boundary_heart_rate_accepted(self):
        data = {
            "name": "heart_rate",
            "date": "2024-01-15T10:30:00+00:00",
            "qty": 20,
            "source": "Apple Watch",
        }
        points = self.transformer.transform(data)
        assert len(points) == 1

    def test_hrv_negative_rejected(self):
        data = {
            "name": "hrv",
            "date": "2024-01-15T10:30:00+00:00",
            "qty": -10,
            "source": "Apple Watch",
        }
        points = self.transformer.transform(data)
        assert len(points) == 0

    def test_hrv_zero_accepted(self):
        data = {
            "name": "hrv",
            "date": "2024-01-15T10:30:00+00:00",
            "qty": 0,
            "source": "Apple Watch",
        }
        points = self.transformer.transform(data)
        assert len(points) == 1

    def test_out_of_range_min_max_skipped(self):
        data = {
            "name": "heart_rate",
            "date": "2024-01-15T10:30:00+00:00",
            "qty": 72,
            "min": -10,
            "max": 500,
            "avg": 75,
            "source": "Apple Watch",
        }
        points = self.transformer.transform(data)
        assert len(points) == 1
        assert points[0]._fields["bpm"] == 72.0
        assert "bpm_min" not in points[0]._fields
        assert "bpm_max" not in points[0]._fields
        assert points[0]._fields["bpm_avg"] == 75.0

    def test_none_qty_skipped(self):
        data = {
            "name": "heart_rate",
            "date": "2024-01-15T10:30:00+00:00",
            "qty": None,
            "source": "Apple Watch",
        }
        points = self.transformer.transform(data)
        assert len(points) == 0


class TestActivityTransformer:
    """Tests for ActivityTransformer."""

    def setup_method(self):
        self.transformer = ActivityTransformer()

    def test_can_transform_activity(self):
        assert self.transformer.can_transform("step_count")
        assert self.transformer.can_transform("stepCount")
        assert self.transformer.can_transform("active_energy")
        assert self.transformer.can_transform("exercise_time")

    def test_transform_steps(self):
        data = {
            "name": "step_count",
            "date": "2024-01-15T23:59:00+00:00",
            "qty": 10523,
            "source": "iPhone",
        }

        points = self.transformer.transform(data)

        assert len(points) == 1
        point = points[0]
        assert point._name == "activity"
        assert point._tags["source"] == "iPhone"

    def test_transform_array_of_metrics(self):
        data = {
            "data": [
                {
                    "name": "step_count",
                    "date": "2024-01-15T12:00:00+00:00",
                    "qty": 5000,
                },
                {
                    "name": "step_count",
                    "date": "2024-01-15T18:00:00+00:00",
                    "qty": 3000,
                },
            ]
        }

        points = self.transformer.transform(data)

        assert len(points) == 2


class TestSleepAnalysisModel:
    """Tests for SleepAnalysis Pydantic model validation."""

    def test_rejects_negative_total_sleep(self):
        with pytest.raises(ValidationError, match="negative"):
            SleepAnalysis.model_validate(
                {
                    "date": "2024-01-15T07:00:00+00:00",
                    "totalSleep": -1.0,
                }
            )

    def test_rejects_negative_in_bed(self):
        with pytest.raises(ValidationError, match="negative"):
            SleepAnalysis.model_validate(
                {
                    "date": "2024-01-15T07:00:00+00:00",
                    "inBed": -5.0,
                }
            )

    def test_accepts_zero_values(self):
        sleep = SleepAnalysis.model_validate(
            {
                "date": "2024-01-15T07:00:00+00:00",
                "totalSleep": 0.0,
            }
        )
        assert sleep.totalSleep == 0.0

    def test_accepts_typical_hour_values(self):
        sleep = SleepAnalysis.model_validate(
            {
                "date": "2024-01-15T07:00:00+00:00",
                "totalSleep": 7.5,
                "inBed": 8.0,
                "deep": 1.5,
                "rem": 2.0,
                "core": 3.0,
                "awake": 0.5,
            }
        )
        assert sleep.totalSleep == 7.5
        assert sleep.deep == 1.5


class TestSleepTransformer:
    """Tests for SleepTransformer."""

    def setup_method(self):
        self.transformer = SleepTransformer()

    def test_can_transform_sleep(self):
        assert self.transformer.can_transform("sleep_analysis")
        assert self.transformer.can_transform("sleepAnalysis")
        assert self.transformer.can_transform("inBed")

    def test_transform_sleep_analysis(self):
        data = {
            "date": "2024-01-15T07:00:00+00:00",
            "asleep": 420,
            "inBed": 480,
            "deep": 90,
            "rem": 120,
            "core": 210,
            "awake": 30,
            "source": "Apple Watch",
        }

        points = self.transformer.transform(data)

        assert len(points) == 1
        point = points[0]
        assert point._name == "sleep"
        assert point._fields["duration_min"] == 420.0
        assert point._fields["deep_min"] == 90.0
        assert point._fields["rem_min"] == 120.0
        assert point._fields["core_min"] == 210.0
        assert point._fields["awake_min"] == 30.0
        assert point._fields["in_bed_min"] == 480.0

    def test_sleep_quality_calculation(self):
        data = {
            "date": "2024-01-15T07:00:00+00:00",
            "asleep": 400,
            "inBed": 480,
            "source": "Apple Watch",
        }

        points = self.transformer.transform(data)

        assert len(points) == 1
        assert points[0]._fields["quality_score"] == pytest.approx(83.3, abs=0.1)

    def test_hours_to_minutes_conversion(self):
        data = {
            "date": "2024-01-15T07:00:00+00:00",
            "totalSleep": 7.0,
            "inBed": 8.0,
            "deep": 1.5,
            "units": "hr",
            "source": "Apple Watch",
        }

        points = self.transformer.transform(data)

        assert len(points) == 1
        assert points[0]._fields["duration_min"] == 420.0
        assert points[0]._fields["in_bed_min"] == 480.0
        assert points[0]._fields["deep_min"] == 90.0

    def test_minutes_no_conversion(self):
        data = {
            "date": "2024-01-15T07:00:00+00:00",
            "totalSleep": 420,
            "inBed": 480,
            "units": "min",
            "source": "Apple Watch",
        }

        points = self.transformer.transform(data)

        assert len(points) == 1
        assert points[0]._fields["duration_min"] == 420.0

    def test_default_units_minutes(self):
        data = {
            "date": "2024-01-15T07:00:00+00:00",
            "totalSleep": 420,
            "inBed": 480,
            "source": "Apple Watch",
        }

        points = self.transformer.transform(data)

        assert len(points) == 1
        assert points[0]._fields["duration_min"] == 420.0

    def test_total_sleep_precedence(self):
        data = {
            "date": "2024-01-15T07:00:00+00:00",
            "totalSleep": 420,
            "asleep": 300,
            "inBed": 480,
            "source": "Apple Watch",
        }

        points = self.transformer.transform(data)

        assert len(points) == 1
        assert points[0]._fields["duration_min"] == 420.0

    def test_fallback_to_asleep(self):
        data = {
            "date": "2024-01-15T07:00:00+00:00",
            "asleep": 360,
            "inBed": 480,
            "source": "Apple Watch",
        }

        points = self.transformer.transform(data)

        assert len(points) == 1
        assert points[0]._fields["duration_min"] == 360.0

    def test_uses_sleep_start_timestamp(self):
        data = {
            "date": "2024-01-15T07:00:00+00:00",
            "sleepStart": "2024-01-14T23:00:00+00:00",
            "asleep": 420,
            "inBed": 480,
            "source": "Apple Watch",
        }

        points = self.transformer.transform(data)

        assert len(points) == 1
        assert points[0]._time == datetime(2024, 1, 14, 23, 0, 0, tzinfo=UTC)

    def test_falls_back_to_date_timestamp(self):
        data = {
            "date": "2024-01-15T07:00:00+00:00",
            "asleep": 420,
            "inBed": 480,
            "source": "Apple Watch",
        }

        points = self.transformer.transform(data)

        assert len(points) == 1
        assert points[0]._time == datetime(2024, 1, 15, 7, 0, 0, tzinfo=UTC)

    def test_negative_duration_rejected(self):
        data = {
            "date": "2024-01-15T07:00:00+00:00",
            "totalSleep": -1.0,
            "inBed": 480,
            "source": "Apple Watch",
        }

        points = self.transformer.transform(data)

        assert len(points) == 0

    def test_excessive_duration_rejected(self):
        data = {
            "date": "2024-01-15T07:00:00+00:00",
            "totalSleep": 1500,
            "inBed": 1600,
            "source": "Apple Watch",
        }

        points = self.transformer.transform(data)

        assert len(points) == 0

    def test_quality_clamped_at_100(self):
        data = {
            "date": "2024-01-15T07:00:00+00:00",
            "totalSleep": 500,
            "inBed": 400,
            "source": "Apple Watch",
        }

        points = self.transformer.transform(data)

        assert len(points) == 1
        assert points[0]._fields["quality_score"] <= 100.0

    def test_no_quality_when_inbed_zero(self):
        data = {
            "date": "2024-01-15T07:00:00+00:00",
            "asleep": 420,
            "inBed": 0,
            "source": "Apple Watch",
        }

        points = self.transformer.transform(data)

        assert len(points) == 1
        assert "quality_score" not in points[0]._fields

    def test_all_none_fields_no_point(self):
        data = {
            "date": "2024-01-15T07:00:00+00:00",
            "asleep": None,
            "inBed": None,
            "source": "Apple Watch",
        }

        points = self.transformer.transform(data)

        assert len(points) == 0

    def test_stage_hr_conversion(self):
        data = {
            "name": "sleep_asleep_deep",
            "date": "2024-01-15T03:00:00+00:00",
            "qty": 1.5,
            "units": "hr",
            "source": "Apple Watch",
        }

        points = self.transformer.transform(data)

        assert len(points) == 1
        assert points[0]._fields["deep_min"] == 90.0

    def test_stage_negative_rejected(self):
        data = {
            "name": "sleep_asleep_deep",
            "date": "2024-01-15T03:00:00+00:00",
            "qty": -5.0,
            "units": "min",
            "source": "Apple Watch",
        }

        points = self.transformer.transform(data)

        assert len(points) == 0


class TestWorkoutTransformer:
    """Tests for WorkoutTransformer."""

    def setup_method(self):
        self.transformer = WorkoutTransformer()

    def test_can_transform_workout(self):
        assert self.transformer.can_transform("workout")
        assert self.transformer.can_transform("HKWorkoutActivityTypeRunning")

    def test_transform_workout(self):
        data = {
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

        points = self.transformer.transform(data)

        assert len(points) == 1
        point = points[0]
        assert point._name == "workout"
        assert point._tags["workout_type"] == "running"

    def test_normalize_workout_type(self):
        assert self.transformer._normalize_workout_type("HKWorkoutActivityTypeRunning") == "running"
        assert (
            self.transformer._normalize_workout_type("traditionalStrengthTraining")
            == "strength_training"
        )
        assert self.transformer._normalize_workout_type("highIntensityIntervalTraining") == "hiit"


class TestBodyTransformer:
    """Tests for BodyTransformer."""

    def setup_method(self):
        self.transformer = BodyTransformer()

    def test_can_transform_body(self):
        assert self.transformer.can_transform("body_mass")
        assert self.transformer.can_transform("bodyMass")
        assert self.transformer.can_transform("weight")
        assert self.transformer.can_transform("body_fat_percentage")

    def test_transform_weight_kg(self):
        data = {
            "name": "body_mass",
            "date": "2024-01-15T08:00:00+00:00",
            "qty": 75.5,
            "units": "kg",
            "source": "Withings",
        }

        points = self.transformer.transform(data)

        assert len(points) == 1
        point = points[0]
        assert point._name == "body"

    def test_transform_weight_lb_conversion(self):
        data = {
            "name": "body_mass",
            "date": "2024-01-15T08:00:00+00:00",
            "qty": 165,
            "units": "lb",
            "source": "Scale",
        }

        points = self.transformer.transform(data)

        assert len(points) == 1


class TestVitalsTransformer:
    """Tests for VitalsTransformer."""

    def setup_method(self):
        self.transformer = VitalsTransformer()

    def test_can_transform_vitals(self):
        assert self.transformer.can_transform("oxygen_saturation")
        assert self.transformer.can_transform("spo2")
        assert self.transformer.can_transform("respiratory_rate")
        assert self.transformer.can_transform("blood_pressure_systolic")

    def test_transform_spo2(self):
        data = {
            "name": "oxygen_saturation",
            "date": "2024-01-15T03:00:00+00:00",
            "qty": 98,
            "source": "Apple Watch",
        }

        points = self.transformer.transform(data)

        assert len(points) == 1
        point = points[0]
        assert point._name == "vitals"

    def test_transform_spo2_decimal_conversion(self):
        # Some sources report SpO2 as decimal (0.98) instead of percentage
        data = {
            "name": "spo2",
            "date": "2024-01-15T03:00:00+00:00",
            "qty": 0.98,
            "source": "Apple Watch",
        }

        points = self.transformer.transform(data)

        assert len(points) == 1

    def test_spo2_value_check(self):
        data = {
            "name": "oxygen_saturation",
            "date": "2024-01-15T03:00:00+00:00",
            "qty": 98.0,
            "source": "Apple Watch",
        }
        points = self.transformer.transform(data)
        assert len(points) == 1
        assert points[0]._fields["spo2_pct"] == 98.0

    def test_spo2_decimal_to_percentage(self):
        data = {
            "name": "spo2",
            "date": "2024-01-15T03:00:00+00:00",
            "qty": 0.98,
            "source": "Apple Watch",
        }
        points = self.transformer.transform(data)
        assert len(points) == 1
        assert points[0]._fields["spo2_pct"] == pytest.approx(98.0)

    def test_spo2_above_100_rejected(self):
        data = {
            "name": "spo2",
            "date": "2024-01-15T03:00:00+00:00",
            "qty": 150,
            "source": "Apple Watch",
        }
        points = self.transformer.transform(data)
        assert len(points) == 0

    def test_spo2_negative_rejected(self):
        data = {
            "name": "spo2",
            "date": "2024-01-15T03:00:00+00:00",
            "qty": -5,
            "source": "Apple Watch",
        }
        points = self.transformer.transform(data)
        assert len(points) == 0

    def test_temperature_f_to_c(self):
        data = {
            "name": "body_temperature",
            "date": "2024-01-15T10:00:00+00:00",
            "qty": 98.6,
            "units": "degF",
            "source": "Thermometer",
        }
        points = self.transformer.transform(data)
        assert len(points) == 1
        assert points[0]._fields["temp_c"] == pytest.approx(37.0, abs=0.1)

    def test_temperature_c_in_range(self):
        data = {
            "name": "body_temperature",
            "date": "2024-01-15T10:00:00+00:00",
            "qty": 36.6,
            "units": "degC",
            "source": "Thermometer",
        }
        points = self.transformer.transform(data)
        assert len(points) == 1
        assert points[0]._fields["temp_c"] == 36.6

    def test_temperature_out_of_range_rejected(self):
        data = {
            "name": "body_temperature",
            "date": "2024-01-15T10:00:00+00:00",
            "qty": 50.0,
            "units": "degC",
            "source": "Thermometer",
        }
        points = self.transformer.transform(data)
        assert len(points) == 0

    def test_blood_pressure_in_range(self):
        data = {
            "name": "blood_pressure_systolic",
            "date": "2024-01-15T10:00:00+00:00",
            "qty": 120,
            "source": "BPMonitor",
        }
        points = self.transformer.transform(data)
        assert len(points) == 1
        assert points[0]._fields["bp_systolic"] == 120.0

    def test_blood_pressure_out_of_range(self):
        data = {
            "name": "blood_pressure_systolic",
            "date": "2024-01-15T10:00:00+00:00",
            "qty": 400,
            "source": "BPMonitor",
        }
        points = self.transformer.transform(data)
        assert len(points) == 0

    def test_respiratory_rate_in_range(self):
        data = {
            "name": "respiratory_rate",
            "date": "2024-01-15T10:00:00+00:00",
            "qty": 15,
            "source": "Apple Watch",
        }
        points = self.transformer.transform(data)
        assert len(points) == 1
        assert points[0]._fields["respiratory_rate"] == 15.0

    def test_respiratory_rate_negative_rejected(self):
        data = {
            "name": "respiratory_rate",
            "date": "2024-01-15T10:00:00+00:00",
            "qty": -3,
            "source": "Apple Watch",
        }
        points = self.transformer.transform(data)
        assert len(points) == 0

    def test_vo2max_in_range(self):
        data = {
            "name": "vo2max",
            "date": "2024-01-15T10:00:00+00:00",
            "qty": 42.5,
            "source": "Apple Watch",
        }
        points = self.transformer.transform(data)
        assert len(points) == 1
        assert points[0]._fields["vo2max"] == 42.5

    def test_vo2max_out_of_range_rejected(self):
        data = {
            "name": "vo2max",
            "date": "2024-01-15T10:00:00+00:00",
            "qty": 150,
            "source": "Apple Watch",
        }
        points = self.transformer.transform(data)
        assert len(points) == 0

    def test_spo2_min_max_decimal_converted(self):
        data = {
            "name": "spo2",
            "date": "2024-01-15T03:00:00+00:00",
            "qty": 0.97,
            "min": 0.95,
            "max": 0.99,
            "source": "Apple Watch",
        }
        points = self.transformer.transform(data)
        assert len(points) == 1
        assert points[0]._fields["spo2_pct"] == pytest.approx(97.0)
        assert points[0]._fields["spo2_pct_min"] == pytest.approx(95.0)
        assert points[0]._fields["spo2_pct_max"] == pytest.approx(99.0)


class TestGenericTransformer:
    """Tests for GenericTransformer."""

    def setup_method(self):
        self.transformer = GenericTransformer()

    def test_can_transform_anything(self):
        assert self.transformer.can_transform("unknown_metric")
        assert self.transformer.can_transform("random_data")

    def test_transform_generic(self):
        data = {
            "name": "someNewMetric",
            "date": "2024-01-15T12:00:00+00:00",
            "qty": 42,
            "units": "count",
            "source": "Unknown App",
        }

        points = self.transformer.transform(data)

        assert len(points) == 1
        point = points[0]
        assert point._name == "other"
        assert point._tags["metric_type"] == "some_new_metric"

    def test_normalize_metric_name(self):
        assert self.transformer._normalize_metric_name("someMetricName") == "some_metric_name"
        assert self.transformer._normalize_metric_name("XMLParser") == "xml_parser"
        assert self.transformer._normalize_metric_name("already_snake_case") == "already_snake_case"


class TestMobilityTransformer:
    """Tests for MobilityTransformer."""

    def setup_method(self):
        self.transformer = MobilityTransformer()

    def test_can_transform_walking_speed(self):
        assert self.transformer.can_transform("walking_speed")
        assert self.transformer.can_transform("walkingSpeed")

    def test_can_transform_walking_step_length(self):
        assert self.transformer.can_transform("walking_step_length")
        assert self.transformer.can_transform("walkingStepLength")

    def test_can_transform_stair_speed(self):
        assert self.transformer.can_transform("stair_speed_up")
        assert self.transformer.can_transform("stair_speed_down")

    def test_can_transform_walking_asymmetry(self):
        assert self.transformer.can_transform("walking_asymmetry_percentage")

    def test_can_transform_six_minute_walk(self):
        assert self.transformer.can_transform("six_minute_walk_test_distance")

    def test_cannot_transform_step_count(self):
        assert not self.transformer.can_transform("step_count")

    def test_cannot_transform_walking_running_distance(self):
        assert not self.transformer.can_transform("walking_running_distance")

    def test_transform_walking_speed(self):
        data = {
            "name": "walking_speed",
            "date": "2024-01-15T10:30:00+00:00",
            "qty": 1.2,
            "source": "iPhone",
        }

        points = self.transformer.transform(data)

        assert len(points) == 1
        point = points[0]
        assert point._name == "mobility"
        assert point._tags["source"] == "iPhone"

    def test_transform_walking_asymmetry_pct(self):
        data = {
            "name": "walking_asymmetry_percentage",
            "date": "2024-01-15T10:30:00+00:00",
            "qty": 8.5,
            "source": "iPhone",
        }

        points = self.transformer.transform(data)

        assert len(points) == 1

    def test_transform_asymmetry_fraction_normalized(self):
        data = {
            "name": "walking_asymmetry_percentage",
            "date": "2024-01-15T10:30:00+00:00",
            "qty": 0.085,
            "source": "iPhone",
        }

        points = self.transformer.transform(data)

        assert len(points) == 1


class TestAudioTransformer:
    """Tests for AudioTransformer."""

    def setup_method(self):
        self.transformer = AudioTransformer()

    def test_can_transform_headphone_audio(self):
        assert self.transformer.can_transform("headphone_audio_exposure")
        assert self.transformer.can_transform("headphoneAudioExposure")

    def test_can_transform_environmental_audio(self):
        assert self.transformer.can_transform("environmental_audio_exposure")
        assert self.transformer.can_transform("environmentalAudioExposure")

    def test_cannot_transform_unrelated(self):
        assert not self.transformer.can_transform("heart_rate")
        assert not self.transformer.can_transform("step_count")

    def test_transform_headphone_audio(self):
        data = {
            "name": "headphone_audio_exposure",
            "date": "2024-01-15T14:00:00+00:00",
            "qty": 72.5,
            "source": "iPhone",
        }

        points = self.transformer.transform(data)

        assert len(points) == 1
        point = points[0]
        assert point._name == "audio"
        assert point._tags["source"] == "iPhone"

    def test_transform_environmental_audio(self):
        data = {
            "name": "environmental_audio_exposure",
            "date": "2024-01-15T14:00:00+00:00",
            "qty": 65.0,
            "source": "Apple Watch",
        }

        points = self.transformer.transform(data)

        assert len(points) == 1
        point = points[0]
        assert point._name == "audio"


class TestFieldMappingFixes:
    """Tests verifying correct field mapping after substring fix."""

    def test_walking_running_distance_maps_correctly(self):
        transformer = ActivityTransformer()
        data = {
            "name": "walking_running_distance",
            "date": "2024-01-15T23:59:00+00:00",
            "qty": 5200,
            "source": "iPhone",
        }

        points = transformer.transform(data)

        assert len(points) == 1
        assert points[0]._name == "activity"

    def test_vo2_max_maps_correctly(self):
        transformer = VitalsTransformer()
        data = {
            "name": "vo2_max",
            "date": "2024-01-15T10:00:00+00:00",
            "qty": 42.5,
            "source": "Apple Watch",
        }

        points = transformer.transform(data)

        assert len(points) == 1
        assert points[0]._name == "vitals"

    def test_walking_step_length_maps_to_mobility(self):
        transformer = MobilityTransformer()
        data = {
            "name": "walking_step_length",
            "date": "2024-01-15T10:00:00+00:00",
            "qty": 72.0,
            "source": "iPhone",
        }

        points = transformer.transform(data)

        assert len(points) == 1
        assert points[0]._name == "mobility"

    def test_blood_oxygen_saturation_maps_correctly(self):
        transformer = VitalsTransformer()
        data = {
            "name": "blood_oxygen_saturation",
            "date": "2024-01-15T03:00:00+00:00",
            "qty": 97,
            "source": "Apple Watch",
        }

        points = transformer.transform(data)

        assert len(points) == 1
        assert points[0]._name == "vitals"


class TestTransformerRegistry:
    """Tests for TransformerRegistry."""

    def setup_method(self):
        self.registry = TransformerRegistry()

    def test_routes_to_heart_transformer(self):
        transformer = self.registry.get_transformer("heart_rate")
        assert isinstance(transformer, HeartTransformer)

    def test_routes_to_activity_transformer(self):
        transformer = self.registry.get_transformer("step_count")
        assert isinstance(transformer, ActivityTransformer)

    def test_routes_to_sleep_transformer(self):
        transformer = self.registry.get_transformer("sleep_analysis")
        assert isinstance(transformer, SleepTransformer)

    def test_routes_to_workout_transformer(self):
        transformer = self.registry.get_transformer("workout")
        assert isinstance(transformer, WorkoutTransformer)

    def test_routes_to_body_transformer(self):
        transformer = self.registry.get_transformer("body_mass")
        assert isinstance(transformer, BodyTransformer)

    def test_routes_to_vitals_transformer(self):
        transformer = self.registry.get_transformer("oxygen_saturation")
        assert isinstance(transformer, VitalsTransformer)

    def test_routes_to_mobility_transformer(self):
        transformer = self.registry.get_transformer("walking_speed")
        assert isinstance(transformer, MobilityTransformer)

    def test_routes_to_audio_transformer(self):
        transformer = self.registry.get_transformer("headphone_audio_exposure")
        assert isinstance(transformer, AudioTransformer)

    def test_walking_step_length_routes_to_mobility_not_activity(self):
        transformer = self.registry.get_transformer("walking_step_length")
        assert isinstance(transformer, MobilityTransformer)

    def test_walking_running_distance_routes_to_activity(self):
        transformer = self.registry.get_transformer("walking_running_distance")
        assert isinstance(transformer, ActivityTransformer)

    def test_falls_back_to_generic(self):
        transformer = self.registry.get_transformer("completely_unknown_metric_xyz")
        assert isinstance(transformer, GenericTransformer)

    def test_transform_extracts_metric_name(self):
        data = {
            "name": "heart_rate",
            "date": "2024-01-15T10:00:00+00:00",
            "qty": 72,
        }

        points = self.registry.transform(data)

        assert len(points) == 1

    def test_transform_handles_nested_data(self):
        data = {
            "data": [
                {
                    "name": "step_count",
                    "date": "2024-01-15T12:00:00+00:00",
                    "qty": 1000,
                }
            ]
        }

        points = self.registry.transform(data)

        assert len(points) == 1
