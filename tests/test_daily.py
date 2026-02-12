"""Tests for daily health summaries."""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from health_ingest.reports.formatter import DailyTelegramFormatter
from health_ingest.reports.models import (
    DailyMetrics,
    InsightResult,
    PrivacySafeDailyMetrics,
    SummaryMode,
)
from health_ingest.reports.rules import RuleEngine

TZ_TBILISI = timezone(timedelta(hours=4))


# --- Fixtures ---


@pytest.fixture
def morning_metrics():
    """Sample morning privacy-safe daily metrics."""
    return PrivacySafeDailyMetrics(
        mode=SummaryMode.MORNING,
        sleep_duration_min=450.0,
        sleep_deep_min=60.0,
        sleep_rem_min=90.0,
        sleep_core_min=240.0,
        sleep_awake_min=30.0,
        sleep_quality_score=85.0,
        resting_hr=58.0,
        hrv_ms=52.0,
        steps=8500,
        active_calories=350,
        exercise_min=45,
        stand_hours=10,
        workout_summaries=["Running: 30min, 280cal, 4.2km"],
        weight_kg=75.5,
        steps_vs_7d_avg=12.0,
        exercise_vs_7d_avg=8.0,
        hrv_vs_7d_avg=5.0,
    )


@pytest.fixture
def evening_metrics():
    """Sample evening privacy-safe daily metrics."""
    return PrivacySafeDailyMetrics(
        mode=SummaryMode.EVENING,
        resting_hr=60.0,
        hrv_ms=48.0,
        steps=11200,
        active_calories=520,
        exercise_min=65,
        stand_hours=12,
        workout_summaries=["Cycling: 45min, 400cal, 15.0km", "Strength: 20min, 120cal"],
        steps_vs_7d_avg=25.0,
        exercise_vs_7d_avg=30.0,
        hrv_vs_7d_avg=-8.0,
    )


@pytest.fixture
def empty_metrics():
    """Metrics with no data."""
    return PrivacySafeDailyMetrics(mode=SummaryMode.MORNING)


@pytest.fixture
def sample_insights():
    """Sample insight results."""
    return [
        InsightResult(
            category="sleep",
            headline="Good sleep quality",
            reasoning="7.5 hours with 85% quality.",
            recommendation="Keep your consistent bedtime routine.",
            confidence=0.85,
            source="rule",
        ),
        InsightResult(
            category="heart",
            headline="HRV above average",
            reasoning="HRV 5% above 7-day average.",
            recommendation="Good recovery. A great day for a workout.",
            confidence=0.8,
            source="rule",
        ),
    ]


@pytest.fixture
def reference_time():
    """Reference time for formatting."""
    return datetime(2025, 1, 15, 9, 0, 0, tzinfo=TZ_TBILISI)


# --- TestPrivacySafeDailyMetrics ---


class TestPrivacySafeDailyMetrics:
    """Tests for PrivacySafeDailyMetrics.to_summary_text()."""

    def test_morning_summary_includes_sleep(self, morning_metrics):
        text = morning_metrics.to_summary_text()
        assert "LAST NIGHT'S SLEEP:" in text
        assert "7.5 hours" in text
        assert "Deep sleep: 60 min" in text
        assert "REM sleep: 90 min" in text
        assert "Quality score: 85%" in text

    def test_morning_summary_includes_vitals(self, morning_metrics):
        text = morning_metrics.to_summary_text()
        assert "MORNING VITALS:" in text
        assert "Resting heart rate: 58 bpm" in text
        assert "HRV: 52 ms" in text
        assert "Weight: 75.5 kg" in text

    def test_morning_summary_includes_yesterday_activity(self, morning_metrics):
        text = morning_metrics.to_summary_text()
        assert "YESTERDAY'S ACTIVITY:" in text
        assert "Steps: 8,500" in text
        assert "+12%" in text

    def test_evening_summary_includes_today_activity(self, evening_metrics):
        text = evening_metrics.to_summary_text()
        assert "TODAY'S ACTIVITY:" in text
        assert "Steps: 11,200" in text
        assert "+25%" in text
        assert "Stand hours: 12" in text

    def test_evening_summary_includes_heart(self, evening_metrics):
        text = evening_metrics.to_summary_text()
        assert "HEART:" in text
        assert "Resting heart rate: 60 bpm" in text
        assert "HRV: 48 ms" in text

    def test_evening_summary_includes_workouts(self, evening_metrics):
        text = evening_metrics.to_summary_text()
        assert "WORKOUTS:" in text
        assert "Cycling: 45min, 400cal, 15.0km" in text

    def test_empty_morning_summary_no_crash(self, empty_metrics):
        text = empty_metrics.to_summary_text()
        assert "LAST NIGHT'S SLEEP:" in text
        assert "YESTERDAY'S ACTIVITY:" in text
        # Should still produce a valid string with defaults
        assert "Steps: 0" in text

    def test_empty_evening_summary_no_crash(self):
        metrics = PrivacySafeDailyMetrics(mode=SummaryMode.EVENING)
        text = metrics.to_summary_text()
        assert "TODAY'S ACTIVITY:" in text
        assert "Steps: 0" in text


# --- TestDailyTelegramFormatter ---


class TestDailyTelegramFormatter:
    """Tests for DailyTelegramFormatter."""

    def test_morning_format_header(self, morning_metrics, sample_insights, reference_time):
        formatter = DailyTelegramFormatter()
        report = formatter.format(morning_metrics, sample_insights, reference_time)
        assert "*Good Morning*" in report
        assert "Jan 15" in report

    def test_evening_format_header(self, evening_metrics, sample_insights, reference_time):
        formatter = DailyTelegramFormatter()
        report = formatter.format(evening_metrics, sample_insights, reference_time)
        assert "*Evening Recap*" in report

    def test_morning_format_includes_sleep(self, morning_metrics, sample_insights, reference_time):
        formatter = DailyTelegramFormatter()
        report = formatter.format(morning_metrics, sample_insights, reference_time)
        assert "*Sleep*" in report
        assert "7.5h" in report

    def test_evening_format_includes_activity(
        self, evening_metrics, sample_insights, reference_time
    ):
        formatter = DailyTelegramFormatter()
        report = formatter.format(evening_metrics, sample_insights, reference_time)
        assert "*Today's Activity*" in report
        assert "11,200" in report

    def test_format_includes_insights(self, morning_metrics, sample_insights, reference_time):
        formatter = DailyTelegramFormatter()
        report = formatter.format(morning_metrics, sample_insights, reference_time)
        assert "*Tips*" in report
        assert "Good sleep quality" in report

    def test_format_no_insights(self, morning_metrics, reference_time):
        formatter = DailyTelegramFormatter()
        report = formatter.format(morning_metrics, [], reference_time)
        assert "*Tips*" in report
        assert "No specific insights" in report

    def test_format_includes_footer(self, morning_metrics, sample_insights, reference_time):
        formatter = DailyTelegramFormatter()
        report = formatter.format(morning_metrics, sample_insights, reference_time)
        assert "RULE-generated insights" in report

    def test_max_length_truncation(self, morning_metrics, reference_time):
        formatter = DailyTelegramFormatter()
        # Create lots of insights to potentially exceed max length
        many_insights = [
            InsightResult(
                category="test",
                headline=f"Insight {i}",
                reasoning="x" * 200,
                recommendation="y" * 200,
                confidence=0.5,
                source="rule",
            )
            for i in range(20)
        ]
        report = formatter.format(morning_metrics, many_insights, reference_time)
        assert len(report) <= DailyTelegramFormatter.MAX_MESSAGE_LENGTH

    def test_morning_includes_workouts(self, morning_metrics, sample_insights, reference_time):
        formatter = DailyTelegramFormatter()
        report = formatter.format(morning_metrics, sample_insights, reference_time)
        assert "*Workouts*" in report
        assert "Running" in report

    def test_evening_includes_heart(self, evening_metrics, sample_insights, reference_time):
        formatter = DailyTelegramFormatter()
        report = formatter.format(evening_metrics, sample_insights, reference_time)
        assert "*Heart*" in report
        assert "60 bpm" in report


# --- TestDailyRules ---


class TestDailyRules:
    """Tests for daily rule evaluation."""

    def test_poor_sleep_fires(self):
        engine = RuleEngine()
        metrics = PrivacySafeDailyMetrics(
            mode=SummaryMode.MORNING,
            sleep_duration_min=300.0,  # 5 hours
        )
        insights = engine.evaluate_daily(metrics)
        headlines = [i.headline for i in insights]
        assert "Short sleep last night" in headlines

    def test_great_sleep_fires(self):
        engine = RuleEngine()
        metrics = PrivacySafeDailyMetrics(
            mode=SummaryMode.MORNING,
            sleep_duration_min=480.0,  # 8 hours
            sleep_quality_score=90.0,
        )
        insights = engine.evaluate_daily(metrics)
        headlines = [i.headline for i in insights]
        assert "Excellent sleep quality" in headlines

    def test_low_deep_sleep_fires(self):
        engine = RuleEngine()
        metrics = PrivacySafeDailyMetrics(
            mode=SummaryMode.MORNING,
            sleep_deep_min=30.0,
        )
        insights = engine.evaluate_daily(metrics)
        headlines = [i.headline for i in insights]
        assert "Low deep sleep" in headlines

    def test_hrv_below_avg_fires(self):
        engine = RuleEngine()
        metrics = PrivacySafeDailyMetrics(
            mode=SummaryMode.MORNING,
            hrv_vs_7d_avg=-20.0,
        )
        insights = engine.evaluate_daily(metrics)
        headlines = [i.headline for i in insights]
        assert "HRV below your average" in headlines

    def test_hrv_above_avg_fires(self):
        engine = RuleEngine()
        metrics = PrivacySafeDailyMetrics(
            mode=SummaryMode.MORNING,
            hrv_vs_7d_avg=15.0,
        )
        insights = engine.evaluate_daily(metrics)
        headlines = [i.headline for i in insights]
        assert "HRV above average" in headlines

    def test_elevated_resting_hr_fires(self):
        engine = RuleEngine()
        metrics = PrivacySafeDailyMetrics(
            mode=SummaryMode.MORNING,
            resting_hr=80.0,
        )
        insights = engine.evaluate_daily(metrics)
        headlines = [i.headline for i in insights]
        assert "Elevated resting heart rate" in headlines

    def test_steps_above_avg_fires(self):
        engine = RuleEngine()
        metrics = PrivacySafeDailyMetrics(
            mode=SummaryMode.EVENING,
            steps=12000,
            steps_vs_7d_avg=30.0,
        )
        insights = engine.evaluate_daily(metrics)
        headlines = [i.headline for i in insights]
        assert any("above average" in h.lower() for h in headlines)

    def test_steps_below_avg_fires(self):
        engine = RuleEngine()
        metrics = PrivacySafeDailyMetrics(
            mode=SummaryMode.EVENING,
            steps=3000,
            steps_vs_7d_avg=-40.0,
        )
        insights = engine.evaluate_daily(metrics)
        headlines = [i.headline for i in insights]
        assert "Lower activity today" in headlines

    def test_workout_completed_fires(self):
        engine = RuleEngine()
        metrics = PrivacySafeDailyMetrics(
            mode=SummaryMode.EVENING,
            workout_summaries=["Running: 30min, 280cal"],
        )
        insights = engine.evaluate_daily(metrics)
        headlines = [i.headline for i in insights]
        assert "Workout completed" in headlines

    def test_max_insights_respected(self):
        engine = RuleEngine()
        # Trigger multiple rules
        metrics = PrivacySafeDailyMetrics(
            mode=SummaryMode.MORNING,
            sleep_duration_min=300.0,  # poor sleep
            sleep_deep_min=20.0,  # low deep sleep
            resting_hr=80.0,  # elevated HR
            hrv_vs_7d_avg=-20.0,  # low HRV
            workout_summaries=["Running: 30min"],  # workout
        )
        insights = engine.evaluate_daily(metrics, max_insights=2)
        assert len(insights) <= 2

    def test_empty_metrics_no_crash(self):
        engine = RuleEngine()
        metrics = PrivacySafeDailyMetrics(mode=SummaryMode.MORNING)
        insights = engine.evaluate_daily(metrics)
        # Should not crash and should return empty or minimal list
        assert isinstance(insights, list)


# --- TestDailyReportGenerator ---


class TestDailyReportGenerator:
    """Tests for DailyReportGenerator with mocked InfluxDB."""

    async def test_generate_morning_summary(self):
        """Test that generate_summary produces a formatted string."""
        from health_ingest.reports.daily import DailyReportGenerator

        # Mock settings
        influx_settings = MagicMock()
        influx_settings.url = "http://localhost:8086"
        influx_settings.token = "test-token"
        influx_settings.org = "test-org"
        influx_settings.bucket = "test-bucket"

        generator = DailyReportGenerator(
            influxdb_settings=influx_settings,
            anthropic_settings=MagicMock(api_key=None),
            openai_settings=MagicMock(api_key=None),
            grok_settings=MagicMock(api_key=None),
            ai_provider="anthropic",
        )

        # Mock the influx client
        mock_query_api = MagicMock()
        mock_query_api.query = AsyncMock(return_value=[])

        mock_client = MagicMock()
        mock_client.query_api.return_value = mock_query_api
        generator._influx_client = mock_client

        # Mock get_settings for InsightEngine
        mock_settings = MagicMock()
        mock_settings.insight.prefer_ai = False
        mock_settings.insight.max_insights = 3
        mock_settings.insight.ai_provider = "anthropic"
        mock_settings.insight.ai_timeout_seconds = 30.0

        with patch("health_ingest.reports.daily.get_settings", return_value=mock_settings):
            ref_time = datetime(2025, 1, 15, 9, 0, 0, tzinfo=TZ_TBILISI)
            report = await generator.generate_summary(SummaryMode.MORNING, ref_time)

        assert isinstance(report, str)
        assert len(report) > 0
        assert "Good Morning" in report

    async def test_generate_evening_summary(self):
        """Test that generate_summary produces an evening-formatted string."""
        from health_ingest.reports.daily import DailyReportGenerator

        influx_settings = MagicMock()
        influx_settings.url = "http://localhost:8086"
        influx_settings.token = "test-token"
        influx_settings.org = "test-org"
        influx_settings.bucket = "test-bucket"

        generator = DailyReportGenerator(
            influxdb_settings=influx_settings,
            anthropic_settings=MagicMock(api_key=None),
            openai_settings=MagicMock(api_key=None),
            grok_settings=MagicMock(api_key=None),
            ai_provider="anthropic",
        )

        mock_query_api = MagicMock()
        mock_query_api.query = AsyncMock(return_value=[])

        mock_client = MagicMock()
        mock_client.query_api.return_value = mock_query_api
        generator._influx_client = mock_client

        mock_settings = MagicMock()
        mock_settings.insight.prefer_ai = False
        mock_settings.insight.max_insights = 3
        mock_settings.insight.ai_provider = "anthropic"
        mock_settings.insight.ai_timeout_seconds = 30.0

        with patch("health_ingest.reports.daily.get_settings", return_value=mock_settings):
            ref_time = datetime(2025, 1, 15, 23, 0, 0, tzinfo=TZ_TBILISI)
            report = await generator.generate_summary(SummaryMode.EVENING, ref_time)

        assert isinstance(report, str)
        assert "Evening Recap" in report

    async def test_to_privacy_safe_morning(self):
        """Test conversion of raw metrics to privacy-safe format."""
        from health_ingest.reports.daily import DailyReportGenerator

        generator = DailyReportGenerator(
            influxdb_settings=MagicMock(),
            anthropic_settings=MagicMock(),
            openai_settings=MagicMock(),
            grok_settings=MagicMock(),
            ai_provider="anthropic",
        )

        raw = DailyMetrics(
            sleep_duration_min=420.0,
            sleep_deep_min=60.0,
            sleep_rem_min=80.0,
            resting_hr=60.0,
            hrv_ms=50.0,
            steps=9000,
            active_calories=400,
            exercise_min=45,
            avg_7d_steps=8000.0,
            avg_7d_exercise_min=40.0,
            avg_7d_hrv_ms=48.0,
            workouts=[
                {"type": "Running", "duration_min": 30, "active_calories": 280, "distance_m": 4200}
            ],
        )

        result = generator._to_privacy_safe(raw, SummaryMode.MORNING)

        assert result.mode == SummaryMode.MORNING
        assert result.sleep_duration_min == 420.0
        assert result.steps == 9000
        assert result.steps_vs_7d_avg == pytest.approx(12.5)
        assert result.exercise_vs_7d_avg == pytest.approx(12.5)
        assert result.hrv_vs_7d_avg is not None
        assert len(result.workout_summaries) == 1
        assert "Running" in result.workout_summaries[0]
