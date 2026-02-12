"""Tests for weekly SVG infographic rendering."""

from datetime import datetime

from health_ingest.reports.analysis_contract import AnalysisProvenance
from health_ingest.reports.models import (
    InsightResult,
    PrivacySafeDailyMetrics,
    PrivacySafeMetrics,
    SummaryMode,
)
from health_ingest.reports.visualization import DailyInfographicRenderer, WeeklyInfographicRenderer


def _sample_metrics() -> PrivacySafeMetrics:
    return PrivacySafeMetrics(
        avg_daily_steps=10400,
        total_exercise_min=190,
        avg_duration_hours=7.4,
        avg_hrv=46.0,
    )


def _sample_insights() -> list[InsightResult]:
    return [
        InsightResult(
            category="activity",
            headline="Activity stayed above target",
            reasoning="Average steps remained above ten thousand per day.",
            recommendation="Keep two dedicated walk blocks in your weekday schedule.",
            confidence=0.9,
            source="rule",
        ),
        InsightResult(
            category="sleep",
            headline="Sleep was consistent",
            reasoning="Nightly duration stayed near seven and a half hours.",
            recommendation="Protect the same bedtime on weekends for recovery.",
            confidence=0.85,
            source="rule",
        ),
    ]


def _sample_provenance() -> AnalysisProvenance:
    return AnalysisProvenance(
        request_type="weekly_summary",
        source="rule",
        provider="rule",
        model="rule-engine.v1",
        dataset_version="sha256:1234567890abcdef",
        prompt_id="weekly_insight",
        prompt_version="v1",
        prompt_hash="abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
    )


def test_weekly_infographic_render_contains_key_sections():
    renderer = WeeklyInfographicRenderer()
    svg = renderer.render(
        metrics=_sample_metrics(),
        insights=_sample_insights(),
        week_start=datetime(2025, 1, 1),
        week_end=datetime(2025, 1, 8),
        analysis_provenance=_sample_provenance(),
    )

    assert svg.startswith("<svg")
    assert "WEEKLY HEALTH INFOGRAPHIC" in svg
    assert "Goal Progress" in svg
    assert "Facts and Recommendations" in svg
    assert "trace: req=weekly_summary" in svg
    assert "10,400" in svg


def test_weekly_infographic_render_handles_no_insights():
    renderer = WeeklyInfographicRenderer()
    svg = renderer.render(
        metrics=_sample_metrics(),
        insights=[],
        week_start=datetime(2025, 1, 1),
        week_end=datetime(2025, 1, 8),
    )
    assert "No notable patterns this week." in svg


def test_write_svg_creates_file(tmp_path):
    renderer = WeeklyInfographicRenderer()
    svg = renderer.render(
        metrics=_sample_metrics(),
        insights=_sample_insights(),
        week_start=datetime(2025, 1, 1),
        week_end=datetime(2025, 1, 8),
    )

    output_path = renderer.write_svg(svg, tmp_path / "weekly.svg")
    assert output_path.exists()
    assert output_path.read_text(encoding="utf-8").startswith("<svg")


def _sample_daily_metrics(mode: SummaryMode) -> PrivacySafeDailyMetrics:
    return PrivacySafeDailyMetrics(
        mode=mode,
        sleep_duration_min=440.0,
        sleep_quality_score=86.0,
        hrv_ms=49.0,
        steps=9800,
        active_calories=430,
        exercise_min=52,
        stand_hours=11,
        hrv_vs_7d_avg=6.0,
    )


def test_daily_infographic_render_morning():
    renderer = DailyInfographicRenderer()
    svg = renderer.render(
        metrics=_sample_daily_metrics(SummaryMode.MORNING),
        insights=_sample_insights(),
        reference_time=datetime(2025, 1, 9, 8, 0, 0),
        analysis_provenance=_sample_provenance(),
    )
    assert svg.startswith("<svg")
    assert "Morning Readiness" in svg
    assert "Morning Insights" in svg
    assert "Movement (Steps)" in svg
    assert "trace: req=weekly_summary" in svg


def test_daily_infographic_render_evening_no_insights():
    renderer = DailyInfographicRenderer()
    svg = renderer.render(
        metrics=_sample_daily_metrics(SummaryMode.EVENING),
        insights=[],
        reference_time=datetime(2025, 1, 9, 21, 0, 0),
    )
    assert "Evening Recovery" in svg
    assert "Evening Insights" in svg
    assert "No notable patterns in today's summary." in svg


def test_daily_infographic_write_svg(tmp_path):
    renderer = DailyInfographicRenderer()
    svg = renderer.render(
        metrics=_sample_daily_metrics(SummaryMode.MORNING),
        insights=_sample_insights(),
        reference_time=datetime(2025, 1, 9, 8, 0, 0),
    )
    output_path = renderer.write_svg(svg, tmp_path / "daily.svg")
    assert output_path.exists()
    assert output_path.read_text(encoding="utf-8").startswith("<svg")
