"""MCP server exposing first-class tools for the health pipeline."""

import asyncio
from collections import Counter
from datetime import UTC, datetime
from statistics import fmean
from time import perf_counter
from typing import Any, Literal

from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from mcp.server.fastmcp import FastMCP

from .archive import RawArchiver
from .config import get_settings
from .dlq import DeadLetterQueue, DLQCategory
from .influx_writer import InfluxWriter
from .metrics import (
    MCP_COMMAND_COST_USD,
    MCP_COMMAND_LATENCY_SECONDS,
    MCP_COMMAND_QUALITY_SCORE,
    MCP_COMMAND_RUNS,
)
from .query import (
    AUTO_WINDOW,
    MEASUREMENT_FIELDS,
    VALID_AGGS,
    VALID_RANGES,
    VALID_WINDOWS,
    _build_flux_query,
    _compute_summary,
    _run_query,
)
from .reports.analysis_contract import (
    ANALYSIS_PROFILES,
    AnalysisRequestType,
    dataset_version_for_text,
    get_analysis_profile,
    load_prompt_template,
)
from .reports.daily import generate_daily_report_bundle
from .reports.delivery import OpenClawDelivery
from .reports.models import SummaryMode
from .reports.weekly import generate_weekly_report_bundle
from .schema_validation import get_metric_validator
from .transformers import TransformerRegistry

DLQ_CATEGORY_NAME = Literal[
    "json_parse_error",
    "unicode_decode_error",
    "validation_error",
    "transform_error",
    "write_error",
    "unknown_error",
]

QUERY_RANGE = Literal["1h", "6h", "12h", "24h", "3d", "7d", "14d", "30d", "90d"]
QUERY_AGG = Literal["mean", "sum", "min", "max", "last", "count", "none"]
QUERY_WINDOW = Literal["5m", "15m", "30m", "1h", "6h", "12h", "1d", "7d"]
QUERY_MEASUREMENT = Literal["heart", "activity", "sleep", "workout", "body", "vitals"]
METRIC_PACK_NAME = Literal["recovery", "activity", "sleep", "heart", "body"]

mcp = FastMCP(
    "health-pipeline",
    instructions=(
        "Use these tools to inspect health pipeline status, generate daily/weekly reports, "
        "query health metrics, inspect/replay DLQ entries, preview ingest transformations, "
        "and deliver reports via OpenClaw."
    ),
)


def _parse_iso_datetime(value: str | None) -> datetime | None:
    """Parse ISO datetime input with `Z` support."""
    if value is None:
        return None
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _parse_mode(mode: Literal["morning", "evening"] | str) -> SummaryMode:
    """Parse summary mode safely for tool input."""
    try:
        return SummaryMode(str(mode).lower())
    except ValueError as exc:
        raise ValueError("mode must be 'morning' or 'evening'") from exc


def _validate_read_only_flux(flux: str) -> None:
    """Reject obvious write/mutation Flux operations."""
    normalized = " ".join(flux.lower().split())
    forbidden = ("|> to(", "experimental.to(", "influxdb.wide_to(", "v1.delete(")
    if any(token in normalized for token in forbidden):
        raise ValueError("Only read-only Flux queries are allowed")


IMPORTANT_METRIC_SPECS: dict[str, dict[str, str]] = {
    "sleep_duration_min": {
        "measurement": "sleep",
        "field": "duration_min",
        "agg": "max",
        "unit": "min",
        "direction": "up",
        "label": "Sleep duration",
    },
    "sleep_quality_score": {
        "measurement": "sleep",
        "field": "quality_score",
        "agg": "mean",
        "unit": "pct",
        "direction": "up",
        "label": "Sleep quality",
    },
    "deep_sleep_min": {
        "measurement": "sleep",
        "field": "deep_min",
        "agg": "max",
        "unit": "min",
        "direction": "up",
        "label": "Deep sleep",
    },
    "rem_sleep_min": {
        "measurement": "sleep",
        "field": "rem_min",
        "agg": "max",
        "unit": "min",
        "direction": "up",
        "label": "REM sleep",
    },
    "resting_hr_bpm": {
        "measurement": "heart",
        "field": "resting_bpm",
        "agg": "mean",
        "unit": "bpm",
        "direction": "down",
        "label": "Resting HR",
    },
    "hrv_ms": {
        "measurement": "heart",
        "field": "hrv_ms",
        "agg": "mean",
        "unit": "ms",
        "direction": "up",
        "label": "HRV",
    },
    "steps": {
        "measurement": "activity",
        "field": "steps",
        "agg": "sum",
        "unit": "count",
        "direction": "up",
        "label": "Steps",
    },
    "exercise_min": {
        "measurement": "activity",
        "field": "exercise_min",
        "agg": "sum",
        "unit": "min",
        "direction": "up",
        "label": "Exercise",
    },
    "active_calories": {
        "measurement": "activity",
        "field": "active_calories",
        "agg": "sum",
        "unit": "kcal",
        "direction": "up",
        "label": "Active calories",
    },
    "stand_hours": {
        "measurement": "activity",
        "field": "stand_hours",
        "agg": "sum",
        "unit": "h",
        "direction": "up",
        "label": "Stand hours",
    },
    "distance_m": {
        "measurement": "activity",
        "field": "distance_m",
        "agg": "sum",
        "unit": "m",
        "direction": "up",
        "label": "Distance",
    },
    "weight_kg": {
        "measurement": "body",
        "field": "weight_kg",
        "agg": "last",
        "unit": "kg",
        "direction": "down",
        "label": "Weight",
    },
}


def _average(values: list[float]) -> float | None:
    if not values:
        return None
    return float(fmean(values))


def _round_opt(value: float | None, ndigits: int = 2) -> float | None:
    if value is None:
        return None
    return round(value, ndigits)


def _pct_change(current: float | None, baseline: float | None) -> float | None:
    if current is None or baseline is None or baseline == 0:
        return None
    return ((current - baseline) / baseline) * 100.0


def _baseline(values: list[float], window_days: int) -> float | None:
    if len(values) < 2:
        return None
    start = max(0, len(values) - window_days - 1)
    segment = values[start:-1]
    return _average(segment)


def _directional_change(delta_pct: float | None, direction: str) -> float | None:
    if delta_pct is None:
        return None
    return delta_pct if direction == "up" else -delta_pct


def _metric_confidence(snapshots: dict[str, dict[str, Any]]) -> float:
    if not snapshots:
        return 0.0
    available = sum(1 for item in snapshots.values() if item["today"] is not None)
    return round(available / len(snapshots), 3)


def _record_mcp_observation(
    *,
    command: str,
    status: str,
    latency_seconds: float,
    estimated_cost_usd: float,
    quality_score: float,
) -> None:
    MCP_COMMAND_RUNS.labels(command=command, status=status).inc()
    MCP_COMMAND_LATENCY_SECONDS.labels(command=command).observe(max(latency_seconds, 0.0))
    MCP_COMMAND_COST_USD.labels(command=command).observe(max(estimated_cost_usd, 0.0))
    MCP_COMMAND_QUALITY_SCORE.labels(command=command).observe(
        min(max(quality_score, 0.0), 1.0)
    )


def _build_status_response(
    *,
    command: str,
    started_at: float,
    facts: dict[str, Any],
    interpretation: list[str],
    priority: Literal["high", "medium", "low"],
    confidence: float,
) -> dict[str, Any]:
    latency = perf_counter() - started_at
    quality = min(max(confidence, 0.0), 1.0)
    status = "success" if quality > 0 else "no_data"
    _record_mcp_observation(
        command=command,
        status=status,
        latency_seconds=latency,
        estimated_cost_usd=0.0,
        quality_score=quality,
    )
    return {
        "facts": facts,
        "interpretation": interpretation,
        "priority": priority,
        "confidence": round(quality, 3),
        "observability": {
            "latency_seconds": round(latency, 4),
            "estimated_cost_usd": 0.0,
            "quality_score": round(quality, 3),
        },
    }


async def _query_metric_daily_values(
    *,
    measurement: str,
    field: str,
    agg: str,
    days: int = 35,
) -> list[float]:
    settings = get_settings()
    flux = _build_flux_query(
        settings.influxdb.bucket,
        measurement,
        field,
        f"{days}d",
        agg,
        "1d",
        days + 7,
    )
    records = await _run_query(settings.influxdb, flux)
    return [
        float(record["value"])
        for record in records
        if isinstance(record["value"], int | float)
    ]


async def _collect_metric_snapshots(metric_keys: list[str]) -> dict[str, dict[str, Any]]:
    async def _build_single(metric_key: str) -> tuple[str, dict[str, Any]]:
        spec = IMPORTANT_METRIC_SPECS[metric_key]
        history = await _query_metric_daily_values(
            measurement=spec["measurement"],
            field=spec["field"],
            agg=spec["agg"],
        )
        today = history[-1] if history else None
        baseline_7d = _baseline(history, 7)
        baseline_28d = _baseline(history, 28)
        return metric_key, {
            "label": spec["label"],
            "unit": spec["unit"],
            "direction": spec["direction"],
            "today": _round_opt(today),
            "baseline_7d": _round_opt(baseline_7d),
            "baseline_28d": _round_opt(baseline_28d),
            "delta_7d_pct": _round_opt(_pct_change(today, baseline_7d)),
            "delta_28d_pct": _round_opt(_pct_change(today, baseline_28d)),
            "history_28d": [_round_opt(value) for value in history[-28:]],
        }

    results = await asyncio.gather(*[_build_single(key) for key in metric_keys])
    return dict(results)


def _deviation_summary(
    snapshots: dict[str, dict[str, Any]],
    *,
    limit: int = 3,
) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    for metric_key, item in snapshots.items():
        delta = item.get("delta_7d_pct")
        if delta is None:
            continue
        directional = _directional_change(delta, str(item["direction"]))
        if directional is None:
            continue
        ranked.append(
            {
                "metric": metric_key,
                "label": item["label"],
                "delta_7d_pct": delta,
                "directional_score": round(directional, 2),
            }
        )
    ranked.sort(key=lambda entry: abs(float(entry["directional_score"])), reverse=True)
    return ranked[:limit]


@mcp.tool(
    description=(
        "Check pipeline health including InfluxDB connectivity "
        "and optional OpenClaw gateway status."
    )
)
async def health_pipeline_status(check_openclaw: bool = True) -> dict[str, Any]:
    """Return runtime health snapshot for MCP clients."""
    settings = get_settings()
    influx_ok = False
    influx_error: str | None = None
    client = InfluxDBClientAsync(
        url=settings.influxdb.url,
        token=settings.influxdb.token,
        org=settings.influxdb.org,
    )
    try:
        influx_ok = bool(await client.ping())
    except Exception as exc:
        influx_error = str(exc)
    finally:
        await client.close()

    openclaw_ok: bool | None = None
    if check_openclaw and settings.openclaw.enabled and settings.openclaw.hooks_token:
        delivery = OpenClawDelivery(settings.openclaw)
        openclaw_ok = await delivery.health_check()

    return {
        "service": "healthy" if influx_ok else "degraded",
        "influxdb": {
            "ok": influx_ok,
            "url": settings.influxdb.url,
            "error": influx_error,
        },
        "openclaw": {
            "enabled": settings.openclaw.enabled,
            "checked": openclaw_ok is not None,
            "ok": openclaw_ok,
        },
        "http": {
            "enabled": settings.http.enabled,
            "port": settings.http.port,
            "allow_unauthenticated": settings.http.allow_unauthenticated,
        },
    }


@mcp.tool(
    description=(
        "Generate weekly report text and optional SVG infographic. "
        "Use `end_date_iso` (ISO 8601) to control week window."
    )
)
async def generate_weekly_report(
    end_date_iso: str | None = None,
    infographic_out: str | None = None,
) -> dict[str, Any]:
    """Generate weekly report bundle for MCP clients."""
    bundle = await generate_weekly_report_bundle(
        end_date=_parse_iso_datetime(end_date_iso),
        infographic_out=infographic_out,
    )
    return {
        "report": bundle.report,
        "week_start": bundle.week_start.isoformat(),
        "week_end": bundle.week_end.isoformat(),
        "insight_count": bundle.insight_count,
        "insight_source": bundle.insight_source,
        "infographic_path": bundle.infographic_path,
    }


@mcp.tool(
    description=(
        "Generate and deliver weekly report through OpenClaw. "
        "Returns delivery metadata and generated report."
    )
)
async def send_weekly_report(
    end_date_iso: str | None = None,
    infographic_out: str | None = None,
) -> dict[str, Any]:
    """Generate then send weekly report via OpenClaw."""
    settings = get_settings()
    bundle = await generate_weekly_report_bundle(
        end_date=_parse_iso_datetime(end_date_iso),
        infographic_out=infographic_out,
    )
    if not (settings.openclaw.enabled and settings.openclaw.hooks_token):
        return {
            "success": False,
            "error": "OpenClaw is not configured",
            "report": bundle.report,
            "infographic_path": bundle.infographic_path,
        }

    delivery = OpenClawDelivery(settings.openclaw)
    week_id = bundle.week_start.strftime("%Y-W%W")
    result = await delivery.send_report(bundle.report, week_id=week_id)
    return {
        "success": result.success,
        "attempt": result.attempt,
        "run_id": result.run_id,
        "error": result.error,
        "week_id": week_id,
        "report": bundle.report,
        "infographic_path": bundle.infographic_path,
    }


@mcp.tool(
    description=(
        "Generate daily report text and optional SVG infographic. "
        "Mode must be `morning` or `evening`."
    )
)
async def generate_daily_report(
    mode: Literal["morning", "evening"],
    reference_time_iso: str | None = None,
    infographic_out: str | None = None,
) -> dict[str, Any]:
    """Generate daily report bundle for MCP clients."""
    bundle = await generate_daily_report_bundle(
        mode=_parse_mode(mode),
        reference_time=_parse_iso_datetime(reference_time_iso),
        infographic_out=infographic_out,
    )
    return {
        "mode": bundle.mode.value,
        "reference_time": bundle.reference_time.isoformat(),
        "report": bundle.report,
        "insight_count": bundle.insight_count,
        "insight_source": bundle.insight_source,
        "infographic_path": bundle.infographic_path,
    }


@mcp.tool(
    description=(
        "Generate and deliver daily report through OpenClaw. "
        "Mode must be `morning` or `evening`."
    )
)
async def send_daily_report(
    mode: Literal["morning", "evening"],
    reference_time_iso: str | None = None,
    infographic_out: str | None = None,
) -> dict[str, Any]:
    """Generate then send daily report via OpenClaw."""
    settings = get_settings()
    summary_mode = _parse_mode(mode)
    bundle = await generate_daily_report_bundle(
        mode=summary_mode,
        reference_time=_parse_iso_datetime(reference_time_iso),
        infographic_out=infographic_out,
    )
    if not (settings.openclaw.enabled and settings.openclaw.hooks_token):
        return {
            "success": False,
            "error": "OpenClaw is not configured",
            "report": bundle.report,
            "infographic_path": bundle.infographic_path,
        }

    delivery = OpenClawDelivery(settings.openclaw)
    date_str = bundle.reference_time.strftime("%Y-%m-%d")
    session_key = f"health-daily-{summary_mode.value}:{date_str}"
    delivery_name = (
        "Morning Health Summary"
        if summary_mode == SummaryMode.MORNING
        else "Evening Health Recap"
    )
    payload = {
        "message": bundle.report,
        "channel": "telegram",
        "to": str(settings.openclaw.telegram_user_id),
        "deliver": True,
        "name": delivery_name,
        "sessionKey": session_key,
    }
    result = await delivery._send_with_retries(payload)
    return {
        "success": result.success,
        "attempt": result.attempt,
        "run_id": result.run_id,
        "error": result.error,
        "mode": summary_mode.value,
        "reference_time": bundle.reference_time.isoformat(),
        "report": bundle.report,
        "infographic_path": bundle.infographic_path,
    }


@mcp.tool(
    description="Inspect recent dead-letter queue entries with optional category filter."
)
async def inspect_dlq(
    limit: int = 20,
    category: (
        DLQ_CATEGORY_NAME
        | None
    ) = None,
) -> dict[str, Any]:
    """Inspect DLQ contents from MCP."""
    settings = get_settings()
    if not settings.dlq.enabled:
        return {"enabled": False, "count": 0, "entries": []}

    capped_limit = max(1, min(limit, 200))
    queue = DeadLetterQueue(
        db_path=settings.dlq.db_path,
        max_entries=settings.dlq.max_entries,
        retention_days=settings.dlq.retention_days,
        max_retries=settings.dlq.max_retries,
    )
    category_enum = DLQCategory(category) if category else None
    entries = await queue.get_entries(category=category_enum, limit=capped_limit)
    return {
        "enabled": True,
        "count": len(entries),
        "entries": [entry.to_dict() for entry in entries],
    }


@mcp.tool(
    description="Show DLQ aggregate stats (counts, retries, per-category distribution)."
)
async def dlq_stats() -> dict[str, Any]:
    """Return DLQ statistics from persistent storage."""
    settings = get_settings()
    if not settings.dlq.enabled:
        return {"enabled": False}

    queue = DeadLetterQueue(
        db_path=settings.dlq.db_path,
        max_entries=settings.dlq.max_entries,
        retention_days=settings.dlq.retention_days,
        max_retries=settings.dlq.max_retries,
    )
    stats = await queue.get_stats()
    return {"enabled": True, **stats}


@mcp.tool(
    description=(
        "Preview or execute DLQ replay. Use execute=false (default) to inspect impact first."
    )
)
async def replay_dlq(
    mode: Literal["entry", "category", "all"] = "category",
    entry_id: str | None = None,
    category: DLQ_CATEGORY_NAME | None = None,
    limit: int = 100,
    execute: bool = False,
) -> dict[str, Any]:
    """Replay DLQ entries with a safe preview-first default."""
    settings = get_settings()
    if not settings.dlq.enabled:
        return {"enabled": False, "executed": False, "error": "DLQ is disabled"}

    capped_limit = max(1, min(limit, 500))
    queue = DeadLetterQueue(
        db_path=settings.dlq.db_path,
        max_entries=settings.dlq.max_entries,
        retention_days=settings.dlq.retention_days,
        max_retries=settings.dlq.max_retries,
    )

    if mode == "entry" and not entry_id:
        raise ValueError("entry_id is required when mode='entry'")
    if mode == "category" and not category:
        raise ValueError("category is required when mode='category'")

    if not execute:
        if mode == "entry":
            entry = await queue.get_entry(entry_id or "")
            return {
                "enabled": True,
                "executed": False,
                "mode": mode,
                "found": entry is not None,
                "entry": entry.to_dict() if entry else None,
            }

        if mode == "category":
            cat = DLQCategory(category or "")
            entries = await queue.get_entries(category=cat, limit=capped_limit)
            return {
                "enabled": True,
                "executed": False,
                "mode": mode,
                "category": cat.value,
                "count": len(entries),
                "entry_ids": [entry.id for entry in entries],
            }

        by_category: dict[str, int] = {}
        for cat in DLQCategory:
            entries = await queue.get_entries(category=cat, limit=capped_limit)
            by_category[cat.value] = len(entries)
        return {
            "enabled": True,
            "executed": False,
            "mode": mode,
            "limit_per_category": capped_limit,
            "counts": by_category,
            "total": sum(by_category.values()),
        }

    writer = InfluxWriter(settings.influxdb)
    registry = TransformerRegistry(default_source=settings.app.default_source)

    async def process_message(topic: str, payload: dict[str, Any]) -> None:
        points = registry.transform(payload)
        if points:
            await writer.write(points)

    await writer.connect()
    try:
        if mode == "entry":
            success = await queue.replay_entry(entry_id or "", process_message)
            return {
                "enabled": True,
                "executed": True,
                "mode": mode,
                "entry_id": entry_id,
                "success": 1 if success else 0,
                "failure": 0 if success else 1,
            }

        if mode == "category":
            cat = DLQCategory(category or "")
            success, failure = await queue.replay_category(
                cat,
                process_message,
                limit=capped_limit,
            )
            return {
                "enabled": True,
                "executed": True,
                "mode": mode,
                "category": cat.value,
                "success": success,
                "failure": failure,
            }

        per_category: dict[str, dict[str, int]] = {}
        total_success = 0
        total_failure = 0
        for cat in DLQCategory:
            success, failure = await queue.replay_category(
                cat,
                process_message,
                limit=capped_limit,
            )
            per_category[cat.value] = {"success": success, "failure": failure}
            total_success += success
            total_failure += failure

        return {
            "enabled": True,
            "executed": True,
            "mode": mode,
            "limit_per_category": capped_limit,
            "success": total_success,
            "failure": total_failure,
            "by_category": per_category,
        }
    finally:
        await writer.disconnect()


@mcp.tool(
    description="Show archive storage stats (file counts/sizes) from configured archive directory."
)
async def archive_stats() -> dict[str, Any]:
    """Return archive stats and retention/compression config."""
    settings = get_settings()
    if not settings.archive.enabled:
        return {"enabled": False}

    archiver = RawArchiver(
        archive_dir=settings.archive.dir,
        rotation=settings.archive.rotation,
        max_age_days=settings.archive.max_age_days,
        compress_after_days=settings.archive.compress_after_days,
    )
    stats = await archiver.get_stats()
    return {
        "enabled": True,
        "rotation": settings.archive.rotation,
        "max_age_days": settings.archive.max_age_days,
        "compress_after_days": settings.archive.compress_after_days,
        **stats,
    }


@mcp.tool(
    description="List available measurements/fields and valid query parameter options."
)
def metric_catalog() -> dict[str, Any]:
    """Return supported metric schema for MCP-driven querying."""
    return {
        "measurements": MEASUREMENT_FIELDS,
        "valid_ranges": sorted(VALID_RANGES),
        "valid_aggregations": sorted(VALID_AGGS),
        "valid_windows": sorted(VALID_WINDOWS),
        "auto_window": AUTO_WINDOW,
    }


@mcp.tool(
    description=(
        "Run a structured Influx query by measurement/field/range with summary statistics."
    )
)
async def query_metric_timeseries(
    measurement: QUERY_MEASUREMENT,
    field: str | None = None,
    range_str: QUERY_RANGE = "24h",
    agg: QUERY_AGG = "mean",
    window: QUERY_WINDOW | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    """Query time-series data via constrained query inputs."""
    valid_fields = MEASUREMENT_FIELDS[measurement]
    if field and field not in valid_fields:
        raise ValueError(
            f"Unknown field '{field}' for measurement '{measurement}'. "
            f"Valid fields: {', '.join(valid_fields)}"
        )

    capped_limit = max(1, min(limit, 1000))
    settings = get_settings()
    flux = _build_flux_query(
        settings.influxdb.bucket,
        measurement,
        field,
        range_str,
        agg,
        window,
        capped_limit,
    )
    records = await _run_query(settings.influxdb, flux)
    numeric_values = [float(r["value"]) for r in records if isinstance(r["value"], int | float)]
    summary = _compute_summary(numeric_values)
    return {
        "measurement": measurement,
        "field": field,
        "range": range_str,
        "agg": agg,
        "window": window or AUTO_WINDOW.get(range_str, "1h"),
        "limit": capped_limit,
        "count": len(records),
        "summary": summary,
        "flux": flux,
        "records": records,
    }


@mcp.tool(
    description=(
        "Run a custom read-only Flux query. Results are capped to avoid oversized MCP responses."
    )
)
async def run_flux_query(flux: str, limit: int = 200) -> dict[str, Any]:
    """Execute custom Flux query with read-only guardrails."""
    _validate_read_only_flux(flux)
    settings = get_settings()
    capped_limit = max(1, min(limit, 1000))
    records = await _run_query(settings.influxdb, flux)
    truncated = len(records) > capped_limit
    selected = records[:capped_limit]
    numeric_values = [float(r["value"]) for r in records if isinstance(r["value"], int | float)]
    return {
        "count": len(selected),
        "total_matches": len(records),
        "limit": capped_limit,
        "truncated": truncated,
        "summary": _compute_summary(numeric_values),
        "records": selected,
    }


@mcp.tool(
    description=(
        "Validate and transform a sample ingestion payload and return point generation preview."
    )
)
def preview_ingest_payload(
    payload: dict[str, Any],
    include_line_protocol: bool = True,
    max_points: int = 20,
) -> dict[str, Any]:
    """Preview ingestion transform output for payload debugging."""
    settings = get_settings()
    registry = TransformerRegistry(default_source=settings.app.default_source)
    validator = get_metric_validator()

    items = registry._normalize_payload(payload)
    valid_items, failures = validator.validate_items(items)
    points = registry.transform(payload)

    measurement_counts = Counter((point._name or "unknown") for point in points)
    capped_points = max(1, min(max_points, 200))

    return {
        "input_items": len(items),
        "valid_items": len(valid_items),
        "validation_failure_count": len(failures),
        "validation_failures": [
            {
                "schema": failure.schema,
                "metric_name": str(failure.item.get("name", "")),
                "error": failure.error,
            }
            for failure in failures[:20]
        ],
        "points_generated": len(points),
        "measurement_counts": dict(measurement_counts),
        "sample_points": (
            [point.to_line_protocol() for point in points[:capped_points]]
            if include_line_protocol
            else []
        ),
    }


def _priority_from_directional_scores(scores: list[float]) -> Literal["high", "medium", "low"]:
    if not scores:
        return "low"
    worst = min(scores)
    if worst <= -25:
        return "high"
    if worst <= -12:
        return "medium"
    return "low"


@mcp.tool(
    description=(
        "Most important daily metrics in one response, with 7d/28d baselines and ranked deviations."
    )
)
async def key_metrics_today() -> dict[str, Any]:
    """Return key daily metrics with facts-first structure."""
    started = perf_counter()
    metric_keys = [
        "sleep_duration_min",
        "sleep_quality_score",
        "resting_hr_bpm",
        "hrv_ms",
        "steps",
        "exercise_min",
        "active_calories",
        "weight_kg",
    ]
    snapshots = await _collect_metric_snapshots(metric_keys)
    deviations = _deviation_summary(snapshots, limit=5)
    directional_scores = [float(item["directional_score"]) for item in deviations]
    confidence = _metric_confidence(snapshots)
    priority = _priority_from_directional_scores(directional_scores)

    interpretation: list[str] = []
    if deviations:
        top = deviations[0]
        interpretation.append(
            f"Largest change is {top['label']} ({top['delta_7d_pct']}% vs 7d baseline)."
        )
    if priority == "high":
        interpretation.append("At least one core metric is materially below baseline.")
    elif priority == "medium":
        interpretation.append("Some metrics drifted below baseline and need attention today.")
    else:
        interpretation.append("Core metrics are stable versus recent baseline.")

    facts = {
        "generated_at": datetime.now(UTC).isoformat(),
        "metrics": snapshots,
        "largest_deviations": deviations,
    }
    return _build_status_response(
        command="key_metrics_today",
        started_at=started,
        facts=facts,
        interpretation=interpretation,
        priority=priority,
        confidence=confidence,
    )


@mcp.tool(
    description="Sleep-focused status with sleep duration/quality/deep/REM and baseline deviations."
)
async def sleep_status() -> dict[str, Any]:
    """Return sleep-specific facts and recommendations."""
    started = perf_counter()
    snapshots = await _collect_metric_snapshots(
        ["sleep_duration_min", "sleep_quality_score", "deep_sleep_min", "rem_sleep_min"]
    )
    duration = snapshots["sleep_duration_min"]["today"]
    quality = snapshots["sleep_quality_score"]["today"]
    deltas = _deviation_summary(snapshots, limit=4)
    directional_scores = [float(item["directional_score"]) for item in deltas]
    priority = _priority_from_directional_scores(directional_scores)
    confidence = _metric_confidence(snapshots)

    interpretation: list[str] = []
    if duration is not None and duration < 360:
        interpretation.append(
            "Sleep duration is under 6h; prioritize recovery and earlier bedtime."
        )
    if quality is not None and quality < 70:
        interpretation.append(
            "Sleep quality is below 70%; reduce late caffeine and screen exposure."
        )
    if not interpretation:
        interpretation.append("Sleep metrics are near baseline; keep current sleep routine.")

    facts = {
        "generated_at": datetime.now(UTC).isoformat(),
        "metrics": snapshots,
        "deviations": deltas,
    }
    return _build_status_response(
        command="sleep_status",
        started_at=started,
        facts=facts,
        interpretation=interpretation,
        priority=priority,
        confidence=confidence,
    )


@mcp.tool(
    description=(
        "Activity-focused status for steps, exercise, active calories, stand hours, and distance."
    )
)
async def activity_status() -> dict[str, Any]:
    """Return activity-focused facts and next-step recommendations."""
    started = perf_counter()
    snapshots = await _collect_metric_snapshots(
        ["steps", "exercise_min", "active_calories", "stand_hours", "distance_m"]
    )
    deltas = _deviation_summary(snapshots, limit=5)
    directional_scores = [float(item["directional_score"]) for item in deltas]
    priority = _priority_from_directional_scores(directional_scores)
    confidence = _metric_confidence(snapshots)

    interpretation: list[str] = []
    below = [item for item in deltas if float(item["directional_score"]) < -10]
    if below:
        labels = ", ".join(item["label"] for item in below[:2])
        interpretation.append(
            f"Activity is below baseline for {labels}; add a focused session today."
        )
    else:
        interpretation.append("Activity metrics are on track or above baseline.")

    facts = {
        "generated_at": datetime.now(UTC).isoformat(),
        "metrics": snapshots,
        "deviations": deltas,
    }
    return _build_status_response(
        command="activity_status",
        started_at=started,
        facts=facts,
        interpretation=interpretation,
        priority=priority,
        confidence=confidence,
    )


@mcp.tool(
    description=(
        "Heart-focused status for resting HR and HRV with trend direction and risk prioritization."
    )
)
async def heart_status() -> dict[str, Any]:
    """Return heart-related health status."""
    started = perf_counter()
    snapshots = await _collect_metric_snapshots(["resting_hr_bpm", "hrv_ms"])
    deltas = _deviation_summary(snapshots, limit=2)
    directional_scores = [float(item["directional_score"]) for item in deltas]
    priority = _priority_from_directional_scores(directional_scores)
    confidence = _metric_confidence(snapshots)

    interpretation: list[str] = []
    resting_delta = snapshots["resting_hr_bpm"]["delta_7d_pct"]
    hrv_delta = snapshots["hrv_ms"]["delta_7d_pct"]
    if resting_delta is not None and resting_delta > 8:
        interpretation.append(
            "Resting HR is elevated versus baseline; reduce intensity and recover."
        )
    if hrv_delta is not None and hrv_delta < -10:
        interpretation.append("HRV is below baseline; prioritize sleep and lower training load.")
    if not interpretation:
        interpretation.append("Heart metrics are stable relative to baseline.")

    facts = {
        "generated_at": datetime.now(UTC).isoformat(),
        "metrics": snapshots,
        "deviations": deltas,
    }
    return _build_status_response(
        command="heart_status",
        started_at=started,
        facts=facts,
        interpretation=interpretation,
        priority=priority,
        confidence=confidence,
    )


@mcp.tool(
    description="Recovery readiness from sleep and heart signals with clear readiness score."
)
async def recovery_status() -> dict[str, Any]:
    """Return a readiness-oriented recovery status."""
    started = perf_counter()
    snapshots = await _collect_metric_snapshots(
        ["sleep_duration_min", "sleep_quality_score", "resting_hr_bpm", "hrv_ms"]
    )
    directional_components: list[float] = []
    for _metric_key, item in snapshots.items():
        directional = _directional_change(item["delta_7d_pct"], str(item["direction"]))
        if directional is not None:
            directional_components.append(float(directional))

    avg_directional = _average(directional_components)
    readiness_score = (
        50.0 if avg_directional is None else max(0.0, min(100.0, 50 + avg_directional))
    )
    if readiness_score < 40:
        priority: Literal["high", "medium", "low"] = "high"
        interpretation = ["Recovery readiness is low; keep training light and emphasize recovery."]
    elif readiness_score < 55:
        priority = "medium"
        interpretation = ["Recovery readiness is moderate; use controlled training intensity."]
    else:
        priority = "low"
        interpretation = ["Recovery readiness is good; normal training load is reasonable."]

    confidence = _metric_confidence(snapshots)
    facts = {
        "generated_at": datetime.now(UTC).isoformat(),
        "readiness_score": round(readiness_score, 1),
        "metrics": snapshots,
        "component_directional_scores": [round(value, 2) for value in directional_components],
    }
    return _build_status_response(
        command="recovery_status",
        started_at=started,
        facts=facts,
        interpretation=interpretation,
        priority=priority,
        confidence=confidence,
    )


@mcp.tool(
    description=(
        "Detect sustained 3-day deviations above 15% from longer baseline and rank by severity."
    )
)
async def trend_alerts() -> dict[str, Any]:
    """Return sustained trend deviations for key metrics."""
    started = perf_counter()
    snapshots = await _collect_metric_snapshots(
        ["steps", "exercise_min", "sleep_duration_min", "resting_hr_bpm", "hrv_ms"]
    )
    alerts: list[dict[str, Any]] = []

    for metric_key, item in snapshots.items():
        history = [float(value) for value in item["history_28d"] if value is not None]
        if len(history) < 7:
            continue
        recent = history[-3:]
        baseline = history[:-3]
        baseline_avg = _average(baseline)
        recent_avg = _average(recent)
        deviation = _pct_change(recent_avg, baseline_avg)
        if deviation is None or abs(deviation) < 15:
            continue
        directional = _directional_change(deviation, str(item["direction"])) or 0.0
        severity = (
            "high"
            if directional <= -25
            else "medium"
            if directional < 0
            else "low"
        )
        alerts.append(
            {
                "metric": metric_key,
                "label": item["label"],
                "deviation_pct": round(deviation, 2),
                "directional_score": round(directional, 2),
                "recent_3d_avg": _round_opt(recent_avg),
                "baseline_avg": _round_opt(baseline_avg),
                "severity": severity,
            }
        )

    alerts.sort(key=lambda entry: abs(float(entry["deviation_pct"])), reverse=True)
    unfavorable_scores = [
        float(item["directional_score"])
        for item in alerts
        if float(item["directional_score"]) < 0
    ]
    priority = _priority_from_directional_scores(unfavorable_scores)
    confidence = _metric_confidence(snapshots)

    interpretation: list[str] = []
    if not alerts:
        interpretation.append("No sustained 3-day deviations above 15% were detected.")
    else:
        top = alerts[0]
        interpretation.append(
            f"Strongest sustained change is {top['label']} ({top['deviation_pct']}% vs baseline)."
        )
        if priority == "high":
            interpretation.append(
                "At least one sustained negative trend needs immediate attention."
            )

    facts = {
        "generated_at": datetime.now(UTC).isoformat(),
        "alerts": alerts,
        "metrics": snapshots,
    }
    return _build_status_response(
        command="trend_alerts",
        started_at=started,
        facts=facts,
        interpretation=interpretation,
        priority=priority,
        confidence=confidence,
    )


@mcp.tool(
    description=(
        "Rank largest metric changes and split into top improvements and top declines."
    )
)
async def top_metric_changes() -> dict[str, Any]:
    """Return ranked positive and negative changes for important metrics."""
    started = perf_counter()
    snapshots = await _collect_metric_snapshots(
        [
            "sleep_duration_min",
            "sleep_quality_score",
            "resting_hr_bpm",
            "hrv_ms",
            "steps",
            "exercise_min",
            "active_calories",
            "weight_kg",
        ]
    )
    scored: list[dict[str, Any]] = []
    for metric_key, item in snapshots.items():
        delta = item["delta_7d_pct"]
        directional = _directional_change(delta, str(item["direction"]))
        if delta is None or directional is None:
            continue
        scored.append(
            {
                "metric": metric_key,
                "label": item["label"],
                "delta_7d_pct": delta,
                "directional_score": round(directional, 2),
            }
        )

    improvements = sorted(scored, key=lambda row: float(row["directional_score"]), reverse=True)[:3]
    declines = sorted(scored, key=lambda row: float(row["directional_score"]))[:3]
    decline_scores = [float(row["directional_score"]) for row in declines]
    priority = _priority_from_directional_scores(decline_scores)
    confidence = _metric_confidence(snapshots)

    interpretation = [
        "Facts are split into strongest improvements and largest declines for quick prioritization."
    ]
    if priority == "high":
        interpretation.append("Largest decline exceeds 25% directional impact and needs attention.")

    facts = {
        "generated_at": datetime.now(UTC).isoformat(),
        "top_improvements": improvements,
        "top_declines": declines,
        "metrics": snapshots,
    }
    return _build_status_response(
        command="top_metric_changes",
        started_at=started,
        facts=facts,
        interpretation=interpretation,
        priority=priority,
        confidence=confidence,
    )


@mcp.tool(description="Body-focused status centered on weight trend and baseline deltas.")
async def body_status() -> dict[str, Any]:
    """Return body metrics status."""
    started = perf_counter()
    snapshots = await _collect_metric_snapshots(["weight_kg"])
    weight_snapshot = snapshots["weight_kg"]
    delta = weight_snapshot["delta_28d_pct"]
    directional = _directional_change(delta, str(weight_snapshot["direction"]))
    confidence = _metric_confidence(snapshots)

    if directional is not None and directional <= -8:
        priority: Literal["high", "medium", "low"] = "high"
        interpretation = ["Weight trend shifted materially upward relative to 28d baseline."]
    elif directional is not None and directional <= -3:
        priority = "medium"
        interpretation = ["Weight trend is modestly above baseline; monitor over the week."]
    else:
        priority = "low"
        interpretation = ["Weight trend is stable versus baseline."]

    facts = {
        "generated_at": datetime.now(UTC).isoformat(),
        "metrics": snapshots,
    }
    return _build_status_response(
        command="body_status",
        started_at=started,
        facts=facts,
        interpretation=interpretation,
        priority=priority,
        confidence=confidence,
    )


@mcp.tool(
    description=(
        "Convenience router for metric domains: recovery, activity, sleep, heart, body."
    )
)
async def metric_pack(pack: METRIC_PACK_NAME) -> dict[str, Any]:
    """Return a focused metric command result by pack name."""
    if pack == "recovery":
        result = await recovery_status()
    elif pack == "activity":
        result = await activity_status()
    elif pack == "sleep":
        result = await sleep_status()
    elif pack == "heart":
        result = await heart_status()
    else:
        result = await body_status()

    return {"pack": pack, **result}


@mcp.tool(
    description="List analysis request contracts with prompt IDs, versions, and expected outcomes."
)
def analysis_contracts() -> dict[str, Any]:
    """Expose request/analysis contract metadata to MCP clients."""
    rows: list[dict[str, Any]] = []
    for request_type, profile in ANALYSIS_PROFILES.items():
        template = load_prompt_template(profile.prompt_id)
        rows.append(
            {
                "request_type": request_type.value,
                "objective": profile.objective,
                "expected_outcome": profile.expected_outcome,
                "prompt_id": template.prompt_id,
                "prompt_version": template.version,
                "prompt_hash": template.sha256[:12],
                "default_max_insights": profile.default_max_insights,
            }
        )
    return {"contracts": rows}


@mcp.tool(
    description=(
        "Render a versioned analysis prompt for a request type with dataset-version metadata."
    )
)
def build_analysis_prompt(
    request_type: Literal[
        "weekly_summary",
        "daily_morning_brief",
        "daily_evening_recap",
        "bot_command_insight",
    ],
    metrics_text: str | None = None,
    data_text: str | None = None,
    command: str | None = None,
    max_insights: int | None = None,
) -> dict[str, Any]:
    """Render a concrete prompt for prompt review/regression testing."""
    parsed_request_type = AnalysisRequestType(request_type)
    profile = get_analysis_profile(parsed_request_type)
    template = load_prompt_template(profile.prompt_id)
    effective_max = max(1, min(max_insights or profile.default_max_insights, 10))

    if parsed_request_type == AnalysisRequestType.BOT_COMMAND_INSIGHT:
        if not data_text:
            raise ValueError("data_text is required for bot_command_insight")
        if not command:
            raise ValueError("command is required for bot_command_insight")
        dataset_source = data_text
        dataset_version = dataset_version_for_text(dataset_source)
        rendered = template.text.format(
            analysis_objective=profile.objective,
            expected_outcome=profile.expected_outcome,
            dataset_version=dataset_version,
            data_text=data_text,
            command=command,
            max_insights=effective_max,
        )
    else:
        if not metrics_text:
            raise ValueError("metrics_text is required for this request_type")
        dataset_source = metrics_text
        dataset_version = dataset_version_for_text(dataset_source)
        rendered = template.text.format(
            analysis_objective=profile.objective,
            expected_outcome=profile.expected_outcome,
            dataset_version=dataset_version,
            metrics_text=metrics_text,
            max_insights=effective_max,
        )

    return {
        "request_type": parsed_request_type.value,
        "objective": profile.objective,
        "expected_outcome": profile.expected_outcome,
        "prompt_id": template.prompt_id,
        "prompt_version": template.version,
        "prompt_hash": template.short_hash,
        "dataset_version": dataset_version,
        "max_insights": effective_max,
        "prompt": rendered,
    }


def run_mcp_server() -> None:
    """CLI entry point: run MCP server over stdio."""
    mcp.run(transport="stdio")
