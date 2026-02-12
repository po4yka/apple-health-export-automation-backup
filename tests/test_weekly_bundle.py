"""Regression tests for weekly report bundle generation."""

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from health_ingest.reports import weekly


@pytest.mark.asyncio
async def test_generate_weekly_report_bundle_uses_supported_generator_signature(monkeypatch):
    settings = SimpleNamespace(
        influxdb=SimpleNamespace(),
        anthropic=SimpleNamespace(),
        openai=SimpleNamespace(),
        grok=SimpleNamespace(),
        insight=SimpleNamespace(ai_provider="anthropic", ai_timeout_seconds=10.0),
    )

    class _FakeGenerator:
        def __init__(
            self,
            influxdb_settings,
            anthropic_settings,
            openai_settings,
            grok_settings,
            ai_provider,
            ai_timeout_seconds=30.0,
        ):
            self._args = (
                influxdb_settings,
                anthropic_settings,
                openai_settings,
                grok_settings,
                ai_provider,
                ai_timeout_seconds,
            )

        async def connect(self):
            return None

        async def disconnect(self):
            return None

        async def _fetch_weekly_metrics(self, start_date, end_date):
            return {"start": start_date, "end": end_date}

        def _calculate_changes(self, current, previous):
            return current

    class _FakeInsightEngine:
        def __init__(self, **kwargs):
            self.last_provenance = None

        async def generate(self, metrics, request_type):
            return [SimpleNamespace(source="rule")]

    class _FakeFormatter:
        def format(self, **kwargs):
            return "weekly report"

    monkeypatch.setattr(weekly, "get_settings", lambda: settings)
    monkeypatch.setattr(weekly, "WeeklyReportGenerator", _FakeGenerator)
    monkeypatch.setattr(weekly, "InsightEngine", _FakeInsightEngine)
    monkeypatch.setattr(weekly, "TelegramFormatter", _FakeFormatter)
    monkeypatch.setattr(weekly, "convert_to_privacy_safe", lambda current, previous: object())

    end_date = datetime(2026, 2, 12, tzinfo=UTC)
    bundle = await weekly.generate_weekly_report_bundle(end_date=end_date)

    assert bundle.report == "weekly report"
    assert bundle.week_end == end_date
    assert bundle.insight_source == "rule"
