"""MCP server exposing first-class tools for the health pipeline."""

from datetime import UTC, datetime
from typing import Any, Literal

from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from mcp.server.fastmcp import FastMCP

from .config import get_settings
from .dlq import DeadLetterQueue, DLQCategory
from .reports.analysis_contract import ANALYSIS_PROFILES, load_prompt_template
from .reports.daily import generate_daily_report_bundle
from .reports.delivery import OpenClawDelivery
from .reports.models import SummaryMode
from .reports.weekly import generate_weekly_report_bundle

mcp = FastMCP(
    "health-pipeline",
    instructions=(
        "Use these tools to inspect health pipeline status, generate daily/weekly reports, "
        "export infographics, and deliver reports via OpenClaw."
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
        Literal[
            "json_parse_error",
            "unicode_decode_error",
            "validation_error",
            "transform_error",
            "write_error",
            "unknown_error",
        ]
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


def run_mcp_server() -> None:
    """CLI entry point: run MCP server over stdio."""
    mcp.run(transport="stdio")
