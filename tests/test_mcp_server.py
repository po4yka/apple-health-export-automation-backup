"""Tests for MCP server tool functions."""

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from health_ingest.mcp_server import (
    _parse_iso_datetime,
    _parse_mode,
    analysis_contracts,
    archive_stats,
    build_analysis_prompt,
    dlq_stats,
    generate_daily_report,
    generate_weekly_report,
    health_pipeline_status,
    inspect_dlq,
    metric_catalog,
    preview_ingest_payload,
    query_metric_timeseries,
    replay_dlq,
    run_flux_query,
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


def test_metric_catalog_contains_expected_sections():
    result = metric_catalog()
    assert "measurements" in result
    assert "heart" in result["measurements"]
    assert "valid_ranges" in result
    assert "valid_aggregations" in result


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


@pytest.mark.asyncio
async def test_query_metric_timeseries(monkeypatch):
    settings = SimpleNamespace(
        influxdb=SimpleNamespace(bucket="apple_health"),
    )

    async def _fake_run_query(_settings, flux):
        assert "_measurement == \"activity\"" in flux
        return [
            {"value": 1000, "field": "steps", "time": "2026-02-12T00:00:00Z"},
            {"value": 2000, "field": "steps", "time": "2026-02-12T01:00:00Z"},
        ]

    monkeypatch.setattr("health_ingest.mcp_server.get_settings", lambda: settings)
    monkeypatch.setattr("health_ingest.mcp_server._run_query", _fake_run_query)

    result = await query_metric_timeseries(
        measurement="activity",
        field="steps",
        range_str="24h",
        agg="sum",
        window="1h",
        limit=10,
    )

    assert result["count"] == 2
    assert result["summary"]["count"] == 2
    assert result["summary"]["max"] == 2000.0


@pytest.mark.asyncio
async def test_query_metric_timeseries_rejects_invalid_field():
    with pytest.raises(ValueError):
        await query_metric_timeseries(
            measurement="heart",
            field="steps",
        )


@pytest.mark.asyncio
async def test_run_flux_query_rejects_mutating_flux():
    with pytest.raises(ValueError):
        await run_flux_query('from(bucket: "x") |> to(bucket: "y")')


@pytest.mark.asyncio
async def test_run_flux_query_truncates_results(monkeypatch):
    settings = SimpleNamespace(
        influxdb=SimpleNamespace(url="http://localhost", token="x", org="health"),
    )

    async def _fake_run_query(_settings, _flux):
        return [{"value": n} for n in range(5)]

    monkeypatch.setattr("health_ingest.mcp_server.get_settings", lambda: settings)
    monkeypatch.setattr("health_ingest.mcp_server._run_query", _fake_run_query)

    result = await run_flux_query('from(bucket: "x") |> range(start: -1h)', limit=2)
    assert result["count"] == 2
    assert result["total_matches"] == 5
    assert result["truncated"] is True


@pytest.mark.asyncio
async def test_archive_stats_disabled(monkeypatch):
    settings = SimpleNamespace(
        archive=SimpleNamespace(enabled=False),
    )
    monkeypatch.setattr("health_ingest.mcp_server.get_settings", lambda: settings)
    result = await archive_stats()
    assert result["enabled"] is False


@pytest.mark.asyncio
async def test_archive_stats_enabled(monkeypatch):
    settings = SimpleNamespace(
        archive=SimpleNamespace(
            enabled=True,
            dir="/tmp/archive",
            rotation="daily",
            max_age_days=30,
            compress_after_days=7,
        )
    )

    class _FakeArchiver:
        def __init__(self, archive_dir, rotation, max_age_days, compress_after_days):
            assert archive_dir == "/tmp/archive"
            assert rotation == "daily"
            assert max_age_days == 30
            assert compress_after_days == 7

        async def get_stats(self):
            return {"jsonl_files": 2, "compressed_files": 1, "total_size_bytes": 2048}

    monkeypatch.setattr("health_ingest.mcp_server.get_settings", lambda: settings)
    monkeypatch.setattr("health_ingest.mcp_server.RawArchiver", _FakeArchiver)

    result = await archive_stats()
    assert result["enabled"] is True
    assert result["jsonl_files"] == 2
    assert result["compressed_files"] == 1


@pytest.mark.asyncio
async def test_dlq_stats_disabled(monkeypatch):
    settings = SimpleNamespace(
        dlq=SimpleNamespace(enabled=False),
    )
    monkeypatch.setattr("health_ingest.mcp_server.get_settings", lambda: settings)
    result = await dlq_stats()
    assert result["enabled"] is False


@pytest.mark.asyncio
async def test_dlq_stats_enabled(monkeypatch):
    settings = SimpleNamespace(
        dlq=SimpleNamespace(
            enabled=True,
            db_path="/tmp/dlq.db",
            max_entries=100,
            retention_days=30,
            max_retries=3,
        )
    )

    class _FakeDLQ:
        def __init__(self, db_path, max_entries, retention_days, max_retries):
            assert db_path == "/tmp/dlq.db"
            assert max_entries == 100
            assert retention_days == 30
            assert max_retries == 3

        async def get_stats(self):
            return {"total_entries": 7, "by_category": {"write_error": 7}}

    monkeypatch.setattr("health_ingest.mcp_server.get_settings", lambda: settings)
    monkeypatch.setattr("health_ingest.mcp_server.DeadLetterQueue", _FakeDLQ)

    result = await dlq_stats()
    assert result["enabled"] is True
    assert result["total_entries"] == 7


@pytest.mark.asyncio
async def test_replay_dlq_preview_category(monkeypatch):
    settings = SimpleNamespace(
        dlq=SimpleNamespace(
            enabled=True,
            db_path="/tmp/dlq.db",
            max_entries=100,
            retention_days=30,
            max_retries=3,
        ),
        app=SimpleNamespace(default_source="health_auto_export"),
    )

    class _FakeDLQ:
        def __init__(self, db_path, max_entries, retention_days, max_retries):
            return None

        async def get_entries(self, category=None, limit=100):
            return [SimpleNamespace(id="a1"), SimpleNamespace(id="a2")]

    monkeypatch.setattr("health_ingest.mcp_server.get_settings", lambda: settings)
    monkeypatch.setattr("health_ingest.mcp_server.DeadLetterQueue", _FakeDLQ)

    result = await replay_dlq(mode="category", category="write_error", execute=False, limit=10)
    assert result["executed"] is False
    assert result["count"] == 2
    assert result["entry_ids"] == ["a1", "a2"]


@pytest.mark.asyncio
async def test_replay_dlq_execute_entry(monkeypatch):
    settings = SimpleNamespace(
        dlq=SimpleNamespace(
            enabled=True,
            db_path="/tmp/dlq.db",
            max_entries=100,
            retention_days=30,
            max_retries=3,
        ),
        app=SimpleNamespace(default_source="health_auto_export"),
        influxdb=SimpleNamespace(
            url="http://localhost",
            token="x",
            org="health",
            bucket="apple_health",
        ),
    )

    class _FakeDLQ:
        def __init__(self, db_path, max_entries, retention_days, max_retries):
            return None

        async def replay_entry(self, entry_id, callback):
            assert entry_id == "abc123"
            return True

    class _FakeWriter:
        def __init__(self, influxdb_settings):
            self.connected = False

        async def connect(self):
            self.connected = True

        async def disconnect(self):
            self.connected = False

        async def write(self, points):
            return None

    class _FakeRegistry:
        def __init__(self, default_source):
            return None

        def transform(self, payload):
            return []

    monkeypatch.setattr("health_ingest.mcp_server.get_settings", lambda: settings)
    monkeypatch.setattr("health_ingest.mcp_server.DeadLetterQueue", _FakeDLQ)
    monkeypatch.setattr("health_ingest.mcp_server.InfluxWriter", _FakeWriter)
    monkeypatch.setattr("health_ingest.mcp_server.TransformerRegistry", _FakeRegistry)

    result = await replay_dlq(mode="entry", entry_id="abc123", execute=True)
    assert result["executed"] is True
    assert result["success"] == 1
    assert result["failure"] == 0


@pytest.mark.asyncio
async def test_replay_dlq_requires_category():
    with pytest.raises(ValueError):
        await replay_dlq(mode="category", category=None, execute=False)


def test_preview_ingest_payload(monkeypatch):
    settings = SimpleNamespace(
        app=SimpleNamespace(default_source="health_auto_export"),
    )

    class _FakePoint:
        def __init__(self, name, lp):
            self._name = name
            self._lp = lp

        def to_line_protocol(self):
            return self._lp

    class _FakeRegistry:
        def __init__(self, default_source):
            return None

        def _normalize_payload(self, payload):
            return [{"name": "heart_rate"}, {"name": "steps"}]

        def transform(self, payload):
            return [
                _FakePoint("heart", "heart bpm=70i"),
                _FakePoint("activity", "activity steps=1000i"),
            ]

    class _FakeValidator:
        def validate_items(self, items):
            failure = SimpleNamespace(
                schema="base",
                item={"name": "bad_metric"},
                error="invalid value",
            )
            return items[:1], [failure]

    monkeypatch.setattr("health_ingest.mcp_server.get_settings", lambda: settings)
    monkeypatch.setattr("health_ingest.mcp_server.TransformerRegistry", _FakeRegistry)
    monkeypatch.setattr("health_ingest.mcp_server.get_metric_validator", lambda: _FakeValidator())

    result = preview_ingest_payload(payload={"data": []}, max_points=1)
    assert result["input_items"] == 2
    assert result["valid_items"] == 1
    assert result["validation_failure_count"] == 1
    assert result["points_generated"] == 2
    assert result["measurement_counts"]["heart"] == 1
    assert len(result["sample_points"]) == 1


def test_build_analysis_prompt_weekly():
    result = build_analysis_prompt(
        request_type="weekly_summary",
        metrics_text="ACTIVITY: steps 10000",
    )
    assert result["request_type"] == "weekly_summary"
    assert result["prompt_id"] == "weekly_insight"
    assert result["dataset_version"].startswith("sha256:")
    assert result["prompt"]


def test_build_analysis_prompt_requires_bot_fields():
    with pytest.raises(ValueError):
        build_analysis_prompt(request_type="bot_command_insight", data_text="x")
