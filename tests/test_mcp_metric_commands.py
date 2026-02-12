"""Tests for MCP metric-focused status commands."""

from typing import Any

import pytest

from health_ingest.mcp_server import (
    activity_status,
    body_status,
    heart_status,
    key_metrics_today,
    metric_pack,
    recovery_status,
    sleep_status,
    top_metric_changes,
    trend_alerts,
)


def _snapshot(
    *,
    label: str,
    direction: str,
    today: float | None,
    delta_7d_pct: float | None,
    delta_28d_pct: float | None = None,
    unit: str = "count",
    history: list[float] | None = None,
) -> dict[str, Any]:
    return {
        "label": label,
        "unit": unit,
        "direction": direction,
        "today": today,
        "baseline_7d": 100.0,
        "baseline_28d": 100.0,
        "delta_7d_pct": delta_7d_pct,
        "delta_28d_pct": delta_28d_pct,
        "history_28d": history or [100.0] * 28,
    }


def _assert_status_shape(result: dict[str, Any]) -> None:
    assert "facts" in result
    assert "interpretation" in result
    assert "priority" in result
    assert "confidence" in result
    assert "observability" in result
    assert "latency_seconds" in result["observability"]
    assert "estimated_cost_usd" in result["observability"]
    assert "quality_score" in result["observability"]


@pytest.mark.asyncio
async def test_key_metrics_today_returns_fixed_schema(monkeypatch):
    async def _fake_collect(keys):
        return {
            key: _snapshot(
                label=key,
                direction="up",
                today=110.0,
                delta_7d_pct=10.0,
                history=[95.0, 100.0, 105.0, 110.0],
            )
            for key in keys
        }

    monkeypatch.setattr("health_ingest.mcp_server._collect_metric_snapshots", _fake_collect)
    result = await key_metrics_today()
    _assert_status_shape(result)
    assert result["priority"] == "low"


@pytest.mark.asyncio
async def test_sleep_status_flags_short_sleep(monkeypatch):
    async def _fake_collect(keys):
        return {
            "sleep_duration_min": _snapshot(
                label="Sleep duration",
                direction="up",
                today=320.0,
                delta_7d_pct=-20.0,
                unit="min",
            ),
            "sleep_quality_score": _snapshot(
                label="Sleep quality",
                direction="up",
                today=65.0,
                delta_7d_pct=-18.0,
                unit="pct",
            ),
            "deep_sleep_min": _snapshot(
                label="Deep sleep",
                direction="up",
                today=40.0,
                delta_7d_pct=-10.0,
                unit="min",
            ),
            "rem_sleep_min": _snapshot(
                label="REM sleep",
                direction="up",
                today=55.0,
                delta_7d_pct=-9.0,
                unit="min",
            ),
        }

    monkeypatch.setattr("health_ingest.mcp_server._collect_metric_snapshots", _fake_collect)
    result = await sleep_status()
    _assert_status_shape(result)
    assert result["priority"] in {"high", "medium"}
    assert any("Sleep duration is under 6h" in row for row in result["interpretation"])


@pytest.mark.asyncio
async def test_activity_status_on_track(monkeypatch):
    async def _fake_collect(keys):
        return {
            key: _snapshot(
                label=key,
                direction="up",
                today=120.0,
                delta_7d_pct=12.0,
            )
            for key in keys
        }

    monkeypatch.setattr("health_ingest.mcp_server._collect_metric_snapshots", _fake_collect)
    result = await activity_status()
    _assert_status_shape(result)
    assert result["priority"] == "low"


@pytest.mark.asyncio
async def test_heart_status_detects_risk(monkeypatch):
    async def _fake_collect(keys):
        return {
            "resting_hr_bpm": _snapshot(
                label="Resting HR",
                direction="down",
                today=62.0,
                delta_7d_pct=12.0,
                unit="bpm",
            ),
            "hrv_ms": _snapshot(
                label="HRV",
                direction="up",
                today=40.0,
                delta_7d_pct=-15.0,
                unit="ms",
            ),
        }

    monkeypatch.setattr("health_ingest.mcp_server._collect_metric_snapshots", _fake_collect)
    result = await heart_status()
    _assert_status_shape(result)
    assert result["priority"] in {"high", "medium"}


@pytest.mark.asyncio
async def test_recovery_status_returns_readiness_score(monkeypatch):
    async def _fake_collect(keys):
        return {
            "sleep_duration_min": _snapshot(
                label="Sleep duration",
                direction="up",
                today=430.0,
                delta_7d_pct=8.0,
                unit="min",
            ),
            "sleep_quality_score": _snapshot(
                label="Sleep quality",
                direction="up",
                today=82.0,
                delta_7d_pct=6.0,
                unit="pct",
            ),
            "resting_hr_bpm": _snapshot(
                label="Resting HR",
                direction="down",
                today=54.0,
                delta_7d_pct=-5.0,
                unit="bpm",
            ),
            "hrv_ms": _snapshot(
                label="HRV",
                direction="up",
                today=55.0,
                delta_7d_pct=9.0,
                unit="ms",
            ),
        }

    monkeypatch.setattr("health_ingest.mcp_server._collect_metric_snapshots", _fake_collect)
    result = await recovery_status()
    _assert_status_shape(result)
    assert "readiness_score" in result["facts"]
    assert 0 <= result["facts"]["readiness_score"] <= 100


@pytest.mark.asyncio
async def test_trend_alerts_detects_sustained_changes(monkeypatch):
    base = [100.0] * 25
    elevated = base + [140.0, 145.0, 150.0]

    async def _fake_collect(keys):
        return {
            "steps": _snapshot(
                label="Steps",
                direction="up",
                today=150.0,
                delta_7d_pct=10.0,
                history=elevated,
            ),
            "exercise_min": _snapshot(
                label="Exercise",
                direction="up",
                today=90.0,
                delta_7d_pct=-2.0,
                history=[100.0] * 28,
            ),
            "sleep_duration_min": _snapshot(
                label="Sleep duration",
                direction="up",
                today=90.0,
                delta_7d_pct=-2.0,
                history=[100.0] * 28,
            ),
            "resting_hr_bpm": _snapshot(
                label="Resting HR",
                direction="down",
                today=100.0,
                delta_7d_pct=0.0,
                history=[100.0] * 28,
            ),
            "hrv_ms": _snapshot(
                label="HRV",
                direction="up",
                today=100.0,
                delta_7d_pct=0.0,
                history=[100.0] * 28,
            ),
        }

    monkeypatch.setattr("health_ingest.mcp_server._collect_metric_snapshots", _fake_collect)
    result = await trend_alerts()
    _assert_status_shape(result)
    assert result["facts"]["alerts"]
    assert result["facts"]["alerts"][0]["metric"] == "steps"


@pytest.mark.asyncio
async def test_top_metric_changes_splits_lists(monkeypatch):
    async def _fake_collect(keys):
        return {
            "sleep_duration_min": _snapshot(
                label="Sleep duration",
                direction="up",
                today=0,
                delta_7d_pct=12,
            ),
            "sleep_quality_score": _snapshot(
                label="Sleep quality",
                direction="up",
                today=0,
                delta_7d_pct=-7,
            ),
            "resting_hr_bpm": _snapshot(
                label="Resting HR",
                direction="down",
                today=0,
                delta_7d_pct=9,
            ),
            "hrv_ms": _snapshot(label="HRV", direction="up", today=0, delta_7d_pct=6),
            "steps": _snapshot(label="Steps", direction="up", today=0, delta_7d_pct=-14),
            "exercise_min": _snapshot(
                label="Exercise",
                direction="up",
                today=0,
                delta_7d_pct=11,
            ),
            "active_calories": _snapshot(
                label="Active calories",
                direction="up",
                today=0,
                delta_7d_pct=-5,
            ),
            "weight_kg": _snapshot(label="Weight", direction="down", today=0, delta_7d_pct=4),
        }

    monkeypatch.setattr("health_ingest.mcp_server._collect_metric_snapshots", _fake_collect)
    result = await top_metric_changes()
    _assert_status_shape(result)
    assert "top_improvements" in result["facts"]
    assert "top_declines" in result["facts"]


@pytest.mark.asyncio
async def test_body_status_returns_weight_metrics(monkeypatch):
    async def _fake_collect(keys):
        return {
            "weight_kg": _snapshot(
                label="Weight",
                direction="down",
                today=76.0,
                delta_7d_pct=2.0,
                delta_28d_pct=1.0,
                unit="kg",
            )
        }

    monkeypatch.setattr("health_ingest.mcp_server._collect_metric_snapshots", _fake_collect)
    result = await body_status()
    _assert_status_shape(result)
    assert "weight_kg" in result["facts"]["metrics"]


@pytest.mark.asyncio
async def test_metric_pack_routes(monkeypatch):
    async def _fake_recovery():
        return {"facts": {}, "interpretation": [], "priority": "low", "confidence": 1.0}

    monkeypatch.setattr("health_ingest.mcp_server.recovery_status", _fake_recovery)
    result = await metric_pack(pack="recovery")
    assert result["pack"] == "recovery"
    assert result["priority"] == "low"
