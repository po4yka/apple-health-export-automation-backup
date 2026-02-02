"""Tests for bot response formatters."""

from health_ingest.bot.commands import COMMAND_DESCRIPTIONS
from health_ingest.bot.formatter import (
    MAX_MESSAGE_LENGTH,
    format_day_summary,
    format_error,
    format_heart,
    format_help,
    format_no_data,
    format_sleep,
    format_snapshot,
    format_steps,
    format_trends,
    format_weight,
    format_workouts,
)
from health_ingest.bot.queries import (
    DaySummaryData,
    HeartData,
    SleepData,
    SnapshotData,
    StepsDailyBreakdown,
    TrendsData,
    WeightData,
    WorkoutEntry,
)


class TestFormatSnapshot:
    def test_basic_snapshot(self):
        data = SnapshotData(steps=8500, active_calories=320, exercise_min=45)
        result = format_snapshot(data)
        assert "8,500" in result
        assert "320" in result
        assert "45" in result
        assert "Quick Snapshot" in result

    def test_snapshot_with_heart_and_weight(self):
        data = SnapshotData(
            steps=10000,
            active_calories=500,
            exercise_min=60,
            resting_hr=62.0,
            hrv_ms=45.0,
            weight_kg=75.3,
        )
        result = format_snapshot(data)
        assert "62" in result
        assert "45" in result
        assert "75.3" in result


class TestFormatHeart:
    def test_heart_with_comparison(self):
        data = HeartData(
            resting_hr=60.0,
            hrv_ms=42.0,
            hr_min=55.0,
            hr_max=68.0,
            avg_7d_resting_hr=63.0,
            avg_7d_hrv_ms=40.0,
        )
        result = format_heart(data)
        assert "60" in result
        assert "42" in result
        assert "55" in result
        assert "68" in result
        assert "7-day" in result


class TestFormatSleep:
    def test_full_sleep(self):
        data = SleepData(
            duration_min=420.0,
            deep_min=90.0,
            rem_min=110.0,
            core_min=180.0,
            awake_min=40.0,
            quality_score=82.0,
        )
        result = format_sleep(data)
        assert "7.0h" in result
        assert "90" in result
        assert "110" in result
        assert "82" in result
        assert "Sleep" in result


class TestFormatWeight:
    def test_weight_with_trends(self):
        data = WeightData(
            latest_kg=75.2,
            latest_date="2026-01-30 08:00",
            avg_7d=75.5,
            avg_30d=76.0,
            change_7d=-0.3,
            change_30d=-0.8,
        )
        result = format_weight(data)
        assert "75.2" in result
        assert "-0.3" in result
        assert "-0.8" in result


class TestFormatDaySummary:
    def test_today_summary(self):
        data = DaySummaryData(
            steps=12000,
            active_calories=450,
            exercise_min=55,
            stand_hours=10,
            distance_km=8.5,
            resting_hr=61.0,
            hrv_ms=38.0,
            workout_summaries=["Running: 30min, 250cal, 5.0km"],
        )
        result = format_day_summary(data, "Today")
        assert "Today" in result
        assert "12,000" in result
        assert "Running" in result
        assert "Workouts" in result


class TestFormatSteps:
    def test_steps_breakdown(self):
        data = StepsDailyBreakdown(
            total=70000,
            daily_avg=10000,
            daily=[("2026-01-25", 9000), ("2026-01-26", 11000)],
        )
        result = format_steps(data, "7d")
        assert "70,000" in result
        assert "10,000" in result
        assert "2026-01-25" in result


class TestFormatWorkouts:
    def test_workouts_list(self):
        entries = [
            WorkoutEntry(
                workout_type="Running",
                date="2026-01-30 07:00",
                duration_min=30.0,
                calories=250.0,
                distance_km=5.1,
            ),
        ]
        result = format_workouts(entries, "7d")
        assert "Running" in result
        assert "30min" in result
        assert "5.1km" in result

    def test_empty_workouts(self):
        result = format_workouts([], "7d")
        assert "No workouts" in result


class TestFormatTrends:
    def test_trends(self):
        data = TrendsData(
            this_week_steps=70000,
            last_week_steps=65000,
            this_week_exercise=300,
            last_week_exercise=280,
        )
        result = format_trends(data)
        assert "Trends" in result
        assert "70,000" in result
        assert "65,000" in result


class TestFormatHelp:
    def test_help_lists_all_commands(self):
        result = format_help()
        for cmd in COMMAND_DESCRIPTIONS:
            assert f"/{cmd.value}" in result


class TestFormatError:
    def test_error_message(self):
        result = format_error("Something went wrong")
        assert "Something went wrong" in result
        assert "⚠️" in result


class TestFormatNoData:
    def test_no_data_message(self):
        result = format_no_data("sleep")
        assert "sleep" in result
        assert "No" in result


class TestTruncation:
    def test_long_message_truncated(self):
        data = StepsDailyBreakdown(
            total=100000,
            daily_avg=10000,
            daily=[(f"2026-01-{i:02d}", 10000 + i * 100) for i in range(1, 32)] * 20,
        )
        result = format_steps(data, "30d")
        assert len(result) <= MAX_MESSAGE_LENGTH
        assert "truncated" in result
