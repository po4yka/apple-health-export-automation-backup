---
name: apple-health
description: Query and analyze Apple Health data (heart rate, steps, sleep, workouts, weight, HRV, SpO2). Generate AI-powered weekly reports. Manage DLQ and archives.
metadata: {"openclaw":{"emoji":"🍏","requires":{"bins":["health-query"],"env":["INFLUXDB_TOKEN"]},"install":[{"id":"uv","kind":"uv","package":"health-ingest","bins":["health-query","health-ingest","health-report","health-report-send","health-dlq-inspect","health-dlq-replay","health-archive","health-archive-replay","health-check"],"label":"Install health-ingest (uv)"}]}}
---

# Apple Health Data Skill

Query, analyze, and manage Apple Health data stored in InfluxDB. Data is ingested from Apple Health via REST API or MQTT and stored as time-series measurements.

## Data Schema

| Measurement | Fields | Tags |
|-------------|--------|------|
| `heart` | `bpm`, `bpm_min`, `bpm_max`, `bpm_avg`, `resting_bpm`, `hrv_ms`, `hrv_ms_min`, `hrv_ms_max`, `hrv_ms_avg` | `source` |
| `activity` | `steps`, `active_calories`, `basal_calories`, `distance_m`, `exercise_min`, `stand_min`, `stand_hours`, `floors_climbed` | `source` |
| `sleep` | `duration_min`, `deep_min`, `rem_min`, `core_min`, `awake_min`, `in_bed_min`, `quality_score` | `source` |
| `workout` | `duration_min`, `calories`, `distance_m`, `avg_hr`, `max_hr` | `source`, `workout_type` |
| `body` | `weight_kg`, `body_fat_pct`, `bmi`, `lean_mass_kg`, `waist_cm`, `height_cm` | `source` |
| `vitals` | `spo2_pct`, `spo2_pct_min`, `spo2_pct_max`, `respiratory_rate`, `bp_systolic`, `bp_diastolic`, `temp_c`, `vo2max` | `source` |

Bucket: `apple_health` (configurable via `INFLUXDB_BUCKET`).

## Querying Data

Use `health-query` for ad-hoc queries. It builds Flux queries, validates fields, and outputs agent-friendly text by default.

### Basic Syntax

```
health-query <measurement> [--field F] [--range R] [--agg A] [--window W] [--format F] [--limit N]
health-query raw --flux '<flux query>'
```

### Arguments

| Arg | Short | Default | Values |
|-----|-------|---------|--------|
| `--field` | `-f` | all | Any valid field for the measurement |
| `--range` | `-r` | `24h` | `1h`, `6h`, `12h`, `24h`, `3d`, `7d`, `14d`, `30d`, `90d` |
| `--agg` | `-a` | `mean` | `mean`, `sum`, `min`, `max`, `last`, `count`, `none` |
| `--window` | | auto | `5m`, `15m`, `30m`, `1h`, `6h`, `12h`, `1d`, `7d` |
| `--format` | | `text` | `text`, `json`, `csv` |
| `--limit` | | `100` | max records |
| `--flux` | | | raw Flux query (only with `raw` measurement) |

Auto-window mapping: `1h`->5m, `6h`->30m, `12h/24h`->1h, `3d/7d/14d/30d`->1d, `90d`->7d.

### Common Questions -> Commands

| Question | Command |
|----------|---------|
| What was my resting heart rate this week? | `health-query heart -f resting_bpm -r 7d` |
| How many steps did I take today? | `health-query activity -f steps -r 24h -a sum` |
| What's my sleep trend this month? | `health-query sleep -f duration_min -r 30d -a mean --window 1d` |
| Show my HRV over the last 90 days | `health-query heart -f hrv_ms -r 90d -a mean --window 7d` |
| What workouts did I do this week? | `health-query workout -r 7d -a none` |
| What's my latest weight? | `health-query body -f weight_kg -r 30d -a last` |
| SpO2 readings today? | `health-query vitals -f spo2_pct -r 24h -a none` |
| All heart data as JSON | `health-query heart -r 24h --format json` |
| Raw Flux query | `health-query raw --flux 'from(bucket:"apple_health") \|> range(start:-1h) \|> limit(n:5)'` |

### Output Format (text)

```
Query: heart.resting_bpm | Range: 7d | Agg: mean | Window: 1d
---
2025-01-23T00:00: 52.3
2025-01-24T00:00: 54.1
2025-01-25T00:00: 51.8
Summary: mean=52.7 min=51.8 max=54.1 count=3
```

Use `--format json` for structured output when further processing is needed.

## Reports

### Generate Weekly Report

```bash
health-report          # Print report to stdout
health-report-send     # Generate and send via Telegram
health-report-send --stdout          # Also print to stdout
health-report-send --dry-run         # Generate without sending
health-report-send --stdout --dry-run # Print but don't send
```

Reports include: activity summary, heart rate trends, sleep analysis, workout log, body composition, and AI-powered insights.

### Scheduled Reports

To schedule weekly reports via OpenClaw cron, add to `openclaw.json`:

```json
{
  "hooks": {
    "cron": [{
      "id": "weekly-health-report",
      "schedule": "0 9 * * 1",
      "prompt": "Generate and send the weekly health report using health-report-send --stdout"
    }]
  }
}
```

When user asks to schedule reports, set up a Monday 9 AM cron using the pattern above. Adjust `schedule` as needed (standard cron syntax).

## Operational Commands

### Dead-Letter Queue

Inspect failed messages:
```bash
health-dlq-inspect                           # Show recent failures
health-dlq-inspect --category transform_error  # Filter by category
health-dlq-inspect --json                    # JSON output
health-dlq-inspect --traceback               # Include stack traces
health-dlq-inspect --limit 50               # Show more entries
```

Replay failed messages:
```bash
health-dlq-replay --dry-run                  # Preview what would replay
health-dlq-replay --id <entry-id>            # Replay specific entry
health-dlq-replay --category transform_error  # Replay all in category
health-dlq-replay --category transform_error --limit 50
```

### Archive Management

```bash
health-archive stats                        # Show archive statistics
health-archive compress --older-than 7      # Compress files older than 7 days
health-archive cleanup --older-than 30      # Delete files older than 30 days
```

Replay archived data:
```bash
health-archive-replay --start 2025-01-01 --end 2025-01-15
health-archive-replay --start 2025-01-01 --end 2025-01-15 --dry-run
```

### Health Check

```bash
health-check    # Verify InfluxDB connectivity and config
```

## Environment Variables

### Required

| Variable | Description |
|----------|-------------|
| `INFLUXDB_TOKEN` | InfluxDB API token |

### Optional (with defaults)

| Variable | Default | Description |
|----------|---------|-------------|
| `INFLUXDB_URL` | `http://influxdb:8086` | InfluxDB URL |
| `INFLUXDB_ORG` | `health` | InfluxDB organization |
| `INFLUXDB_BUCKET` | `apple_health` | InfluxDB bucket |
| `ANTHROPIC_API_KEY` | - | For AI insights in reports |
| `ANTHROPIC_MODEL` | `claude-sonnet-4-20250514` | Claude model for insights |
| `CLAWDBOT_ENABLED` | `true` | Enable Telegram delivery |
| `CLAWDBOT_HOOKS_TOKEN` | - | Clawdbot gateway auth token |

## Error Handling

### Empty Results

If `health-query` returns "No data found":
1. Check the time range — data may not exist for the requested period
2. Verify the field name is correct for the measurement (the CLI lists valid fields on error)
3. Confirm InfluxDB is reachable: `health-check`
4. Check if data was ingested: `health-query activity -f steps -r 90d -a count`

### Connection Issues

If InfluxDB is unreachable:
1. Verify `INFLUXDB_URL` and `INFLUXDB_TOKEN` are set
2. Run `health-check` to diagnose
3. Check network connectivity to the InfluxDB host

### DLQ Errors

If ingestion is failing, inspect the dead-letter queue:
```bash
health-dlq-inspect --json | head -20
```
Common categories: `json_parse_error`, `transform_error`, `write_error`.
