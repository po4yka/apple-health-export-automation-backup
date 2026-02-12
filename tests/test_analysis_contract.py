"""Tests for analysis request/prompt version contracts."""

import pytest

from health_ingest.config import AnthropicSettings, InsightSettings
from health_ingest.reports.analysis_contract import (
    PROMPT_SPECS,
    AnalysisRequestType,
    dataset_version_for_text,
    load_prompt_template,
)
from health_ingest.reports.insights import InsightEngine
from health_ingest.reports.models import PrivacySafeDailyMetrics, PrivacySafeMetrics, SummaryMode


def test_all_prompt_specs_load_and_have_stable_hashes():
    """Each registered prompt must load and expose digest metadata."""
    for prompt_id in PROMPT_SPECS:
        prompt = load_prompt_template(prompt_id)
        assert prompt.prompt_id == prompt_id
        assert prompt.version.startswith("v")
        assert prompt.path.exists()
        assert len(prompt.sha256) == 64
        assert prompt.text


def test_dataset_version_normalizes_trailing_whitespace():
    text_a = "A: 1\nB: 2\n"
    text_b = "A: 1   \nB: 2\n\n"
    assert dataset_version_for_text(text_a) == dataset_version_for_text(text_b)


@pytest.mark.asyncio
async def test_insight_engine_sets_provenance_for_weekly_rule_fallback():
    engine = InsightEngine(
        anthropic_settings=AnthropicSettings(_env_file=None, api_key=None),
        insight_settings=InsightSettings(_env_file=None, prefer_ai=True),
    )
    metrics = PrivacySafeMetrics(avg_daily_steps=9000, total_exercise_min=120)

    await engine.generate(metrics)

    assert engine.last_provenance is not None
    assert engine.last_provenance.request_type == AnalysisRequestType.WEEKLY_SUMMARY.value
    assert engine.last_provenance.source == "rule"
    assert engine.last_provenance.prompt_id == "weekly_insight"
    assert engine.last_provenance.dataset_version.startswith("sha256:")


@pytest.mark.asyncio
async def test_insight_engine_uses_daily_request_profile_prompt():
    engine = InsightEngine(
        anthropic_settings=AnthropicSettings(_env_file=None, api_key=None),
        insight_settings=InsightSettings(_env_file=None, prefer_ai=True),
    )
    metrics = PrivacySafeDailyMetrics(
        mode=SummaryMode.MORNING,
        sleep_duration_min=420.0,
        steps=7000,
    )

    await engine.generate(metrics, request_type=AnalysisRequestType.DAILY_MORNING_BRIEF)

    assert engine.last_provenance is not None
    assert engine.last_provenance.request_type == AnalysisRequestType.DAILY_MORNING_BRIEF.value
    assert engine.last_provenance.prompt_id == "daily_morning"
