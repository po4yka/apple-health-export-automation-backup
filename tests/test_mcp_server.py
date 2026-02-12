"""Tests for MCP server tool functions."""

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from health_ingest.mcp_server import (
    _parse_iso_datetime,
    _parse_mode,
    analysis_contracts,
    generate_daily_report,
    generate_weekly_report,
    health_pipeline_status,
    inspect_dlq,
    send_weekly_report,
)
from health_ingest.reports.daily import DailyReportBundle
from health_ingest.reports.models import SummaryMode
from health_ingest.reports.weekly import WeeklyReportBundle


def test_parse_iso_datetime_supports_z_suffix():
    parsed = _parse_iso_datetime("2026-02-12T10:00:00Z")
    assert parsed is not None
    assert parsed.tzinfo is not None
    assert parsed.year == 2026


def test_parse_mode_rejects_invalid():
    with pytest.raises(ValueError):
        _parse_mode("bad-mode")


def test_analysis_contracts_exposes_entries():
    result = analysis_contracts()
    assert "contracts" in result
    assert result["contracts"]
    assert any(row["request_type"] == "weekly_summary" for row in result["contracts"])


@pytest.mark.asyncio
async def test_generate_weekly_report_tool(monkeypatch):
    async def _fake_bundle(end_date=None, infographic_out=None):
        return WeeklyReportBundle(
            report="weekly text",
            week_start=datetime(2026, 2, 1, tzinfo=UTC),
            week_end=datetime(2026, 2, 8, tzinfo=UTC),
            insight_count=3,
            insight_source="rule",
            infographic_path=infographic_out,
        )

    monkeypatch.setattr("health_ingest.mcp_server.generate_weekly_report_bundle", _fake_bundle)

    result = await generate_weekly_report(infographic_out="/tmp/weekly.svg")
    assert result["report"] == "weekly text"
    assert result["insight_count"] == 3
    assert result["infographic_path"] == "/tmp/weekly.svg"


@pytest.mark.asyncio
async def test_generate_daily_report_tool(monkeypatch):
    async def _fake_bundle(mode, reference_time=None, infographic_out=None):
        return DailyReportBundle(
            mode=SummaryMode.MORNING,
            report="daily text",
            reference_time=datetime(2026, 2, 12, 8, 0, tzinfo=UTC),
            insight_count=2,
            insight_source="ai",
            infographic_path=infographic_out,
        )

    monkeypatch.setattr("health_ingest.mcp_server.generate_daily_report_bundle", _fake_bundle)

    result = await generate_daily_report(
        mode="morning",
        reference_time_iso="2026-02-12T08:00:00Z",
        infographic_out="/tmp/daily.svg",
    )
    assert result["mode"] == "morning"
    assert result["insight_source"] == "ai"
    assert result["infographic_path"] == "/tmp/daily.svg"


@pytest.mark.asyncio
async def test_send_weekly_report_returns_not_configured(monkeypatch):
    async def _fake_bundle(end_date=None, infographic_out=None):
        return WeeklyReportBundle(
            report="weekly text",
            week_start=datetime(2026, 2, 1, tzinfo=UTC),
            week_end=datetime(2026, 2, 8, tzinfo=UTC),
            insight_count=3,
            insight_source="rule",
            infographic_path=None,
        )

    settings = SimpleNamespace(
        openclaw=SimpleNamespace(enabled=False, hooks_token=None),
    )
    monkeypatch.setattr("health_ingest.mcp_server.get_settings", lambda: settings)
    monkeypatch.setattr("health_ingest.mcp_server.generate_weekly_report_bundle", _fake_bundle)

    result = await send_weekly_report()
    assert result["success"] is False
    assert "not configured" in result["error"].lower()


@pytest.mark.asyncio
async def test_inspect_dlq_disabled(monkeypatch):
    settings = SimpleNamespace(
        dlq=SimpleNamespace(
            enabled=False,
            db_path="/tmp/none.db",
            max_entries=100,
            retention_days=30,
            max_retries=3,
        )
    )
    monkeypatch.setattr("health_ingest.mcp_server.get_settings", lambda: settings)
    result = await inspect_dlq()
    assert result["enabled"] is False
    assert result["entries"] == []


@pytest.mark.asyncio
async def test_health_pipeline_status_success(monkeypatch):
    class _FakeInflux:
        async def ping(self):
            return True

        async def close(self):
            return None

    settings = SimpleNamespace(
        influxdb=SimpleNamespace(url="http://localhost:8086", token="x", org="health"),
        openclaw=SimpleNamespace(enabled=False, hooks_token=None),
        http=SimpleNamespace(enabled=True, port=8080, allow_unauthenticated=False),
    )

    monkeypatch.setattr("health_ingest.mcp_server.get_settings", lambda: settings)
    monkeypatch.setattr("health_ingest.mcp_server.InfluxDBClientAsync", lambda **_: _FakeInflux())

    result = await health_pipeline_status(check_openclaw=False)
    assert result["service"] == "healthy"
    assert result["influxdb"]["ok"] is True
