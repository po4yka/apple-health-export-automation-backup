"""Ad-hoc health data query CLI for InfluxDB."""

import argparse
import asyncio
import csv
import io
import json
import sys
from typing import Any

from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

from .config import InfluxDBSettings, get_settings

# Schema: measurement -> list of valid field names
MEASUREMENT_FIELDS: dict[str, list[str]] = {
    "heart": [
        "bpm",
        "bpm_min",
        "bpm_max",
        "bpm_avg",
        "resting_bpm",
        "hrv_ms",
        "hrv_ms_min",
        "hrv_ms_max",
        "hrv_ms_avg",
    ],
    "activity": [
        "steps",
        "active_calories",
        "basal_calories",
        "distance_m",
        "exercise_min",
        "stand_min",
        "stand_hours",
        "floors_climbed",
    ],
    "sleep": [
        "duration_min",
        "deep_min",
        "rem_min",
        "core_min",
        "awake_min",
        "in_bed_min",
        "quality_score",
    ],
    "workout": [
        "duration_min",
        "calories",
        "distance_m",
        "avg_hr",
        "max_hr",
    ],
    "body": [
        "weight_kg",
        "body_fat_pct",
        "bmi",
        "lean_mass_kg",
        "waist_cm",
        "height_cm",
    ],
    "vitals": [
        "spo2_pct",
        "spo2_pct_min",
        "spo2_pct_max",
        "respiratory_rate",
        "bp_systolic",
        "bp_diastolic",
        "temp_c",
        "vo2max",
    ],
}

VALID_RANGES = {"1h", "6h", "12h", "24h", "3d", "7d", "14d", "30d", "90d"}
VALID_AGGS = {"mean", "sum", "min", "max", "last", "count", "none"}
VALID_WINDOWS = {"5m", "15m", "30m", "1h", "6h", "12h", "1d", "7d"}
VALID_FORMATS = {"text", "json", "csv"}

# Auto-window: range -> default window period
AUTO_WINDOW: dict[str, str] = {
    "1h": "5m",
    "6h": "30m",
    "12h": "1h",
    "24h": "1h",
    "3d": "1d",
    "7d": "1d",
    "14d": "1d",
    "30d": "1d",
    "90d": "7d",
}


def _build_flux_query(
    bucket: str,
    measurement: str,
    field: str | None,
    range_str: str,
    agg: str,
    window: str | None,
    limit: int,
) -> str:
    """Build a Flux query from structured arguments."""
    window = window or AUTO_WINDOW.get(range_str, "1h")

    parts = [
        f'from(bucket: "{bucket}")',
        f"  |> range(start: -{range_str})",
        f'  |> filter(fn: (r) => r._measurement == "{measurement}")',
    ]

    if field:
        parts.append(f'  |> filter(fn: (r) => r._field == "{field}")')

    if agg != "none":
        parts.append(f"  |> aggregateWindow(every: {window}, fn: {agg}, createEmpty: false)")

    parts.append(f"  |> limit(n: {limit})")

    return "\n".join(parts)


def _format_value(value: Any) -> str:
    """Format a numeric value for display."""
    if isinstance(value, float):
        return f"{value:.1f}"
    return str(value)


def _compute_summary(values: list[float]) -> dict[str, Any]:
    """Compute summary statistics for a list of values."""
    if not values:
        return {}
    return {
        "mean": sum(values) / len(values),
        "min": min(values),
        "max": max(values),
        "count": len(values),
    }


async def _run_query(
    settings: InfluxDBSettings,
    flux_query: str,
) -> list[dict[str, Any]]:
    """Execute a Flux query and return records as dicts."""
    client = InfluxDBClientAsync(
        url=settings.url,
        token=settings.token,
        org=settings.org,
    )
    try:
        query_api = client.query_api()
        tables = await query_api.query(flux_query)

        records: list[dict[str, Any]] = []
        for table in tables:
            for record in table.records:
                records.append(
                    {
                        "time": record.get_time().isoformat() if record.get_time() else None,
                        "measurement": record.values.get("_measurement", ""),
                        "field": record.get_field(),
                        "value": record.get_value(),
                        "tags": {
                            k: v
                            for k, v in record.values.items()
                            if k
                            not in (
                                "_time",
                                "_start",
                                "_stop",
                                "_measurement",
                                "_field",
                                "_value",
                                "result",
                                "table",
                            )
                        },
                    }
                )
        return records
    finally:
        await client.close()


def _output_text(
    records: list[dict[str, Any]],
    measurement: str,
    field: str | None,
    range_str: str,
    agg: str,
    window: str | None,
) -> None:
    """Print records in agent-friendly text format."""
    field_label = field or "all"
    window_label = window or AUTO_WINDOW.get(range_str, "1h")
    print(
        f"Query: {measurement}.{field_label} | Range: {range_str}"
        f" | Agg: {agg} | Window: {window_label}"
    )
    print("---")

    if not records:
        print("No data found.")
        return

    numeric_values: list[float] = []
    for rec in records:
        time_str = rec["time"][:16] if rec["time"] else "?"  # trim to minute
        val = rec["value"]
        field_name = rec["field"] or ""
        tag_str = ""
        if rec["tags"]:
            tag_parts = [f"{k}={v}" for k, v in rec["tags"].items() if v]
            if tag_parts:
                tag_str = f" [{', '.join(tag_parts)}]"

        label = f"{field_name}: " if not field else ""
        print(f"{time_str}: {label}{_format_value(val)}{tag_str}")

        if isinstance(val, int | float):
            numeric_values.append(float(val))

    summary = _compute_summary(numeric_values)
    if summary:
        parts = [f"{k}={_format_value(v)}" for k, v in summary.items()]
        print(f"Summary: {' '.join(parts)}")


def _output_json(records: list[dict[str, Any]]) -> None:
    """Print records as JSON."""
    numeric_values = [float(r["value"]) for r in records if isinstance(r["value"], int | float)]
    output = {
        "records": records,
        "count": len(records),
        "summary": _compute_summary(numeric_values),
    }
    print(json.dumps(output, indent=2, default=str))


def _output_csv(records: list[dict[str, Any]]) -> None:
    """Print records as CSV."""
    if not records:
        print("time,measurement,field,value")
        return

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["time", "measurement", "field", "value"])
    for rec in records:
        writer.writerow([rec["time"], rec["measurement"], rec["field"], rec["value"]])
    print(buf.getvalue(), end="")


async def _execute_query(args: argparse.Namespace) -> None:
    """Run the query based on parsed arguments."""
    settings = get_settings()
    influx = settings.influxdb

    # Raw Flux mode
    if args.measurement == "raw":
        if not args.flux:
            print("Error: --flux is required when measurement is 'raw'", file=sys.stderr)
            sys.exit(1)
        records = await _run_query(influx, args.flux)
        fmt = getattr(args, "format", "text")
        if fmt == "json":
            _output_json(records)
        elif fmt == "csv":
            _output_csv(records)
        else:
            # Minimal text output for raw
            for rec in records:
                print(f"{rec['time']}: {rec['field']}={_format_value(rec['value'])}")
        return

    measurement = args.measurement
    field = args.field
    range_str = args.range
    agg = args.agg
    window = args.window
    limit = args.limit
    fmt = args.format

    # Validate field against schema
    if field:
        valid_fields = MEASUREMENT_FIELDS.get(measurement, [])
        if valid_fields and field not in valid_fields:
            print(
                f"Error: unknown field '{field}' for measurement '{measurement}'.\n"
                f"Valid fields: {', '.join(valid_fields)}",
                file=sys.stderr,
            )
            sys.exit(1)

    flux = _build_flux_query(influx.bucket, measurement, field, range_str, agg, window, limit)
    records = await _run_query(influx, flux)

    if fmt == "json":
        _output_json(records)
    elif fmt == "csv":
        _output_csv(records)
    else:
        _output_text(records, measurement, field, range_str, agg, window)


def query_cli() -> None:
    """CLI entry point for ad-hoc health data queries.

    Usage:
        health-query heart --field resting_bpm --range 7d
        health-query activity -f steps -r 24h -a sum --format json
        health-query raw --flux 'from(bucket:"apple_health") |> range(start:-1h) |> limit(n:5)'
    """
    measurements = list(MEASUREMENT_FIELDS.keys()) + ["raw"]

    parser = argparse.ArgumentParser(
        description="Query Apple Health data from InfluxDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  health-query heart -f resting_bpm -r 7d\n"
            "  health-query activity -f steps -r 24h -a sum --format json\n"
            "  health-query sleep -r 30d -a mean --window 1d\n"
            "  health-query raw --flux 'from(bucket:\"apple_health\") |> range(start:-1h)'\n"
        ),
    )
    parser.add_argument(
        "measurement",
        choices=measurements,
        help=f"Measurement to query: {', '.join(measurements)}",
    )
    parser.add_argument(
        "--field",
        "-f",
        default=None,
        help="Specific field to query (default: all fields)",
    )
    parser.add_argument(
        "--range",
        "-r",
        default="24h",
        choices=sorted(VALID_RANGES),
        help="Time range (default: 24h)",
    )
    parser.add_argument(
        "--agg",
        "-a",
        default="mean",
        choices=sorted(VALID_AGGS),
        help="Aggregation function (default: mean)",
    )
    parser.add_argument(
        "--window",
        default=None,
        choices=sorted(VALID_WINDOWS),
        help="Aggregation window period (default: auto based on range)",
    )
    parser.add_argument(
        "--format",
        default="text",
        choices=sorted(VALID_FORMATS),
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum records to return (default: 100)",
    )
    parser.add_argument(
        "--flux",
        default=None,
        help="Raw Flux query (only with measurement=raw)",
    )

    args = parser.parse_args()
    asyncio.run(_execute_query(args))
