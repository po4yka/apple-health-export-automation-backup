# MCP Integration

This project exposes a first-class MCP interface for Claude Code, OpenClaw, and other MCP clients.

## Quick Setup

1. Copy the example config:

```bash
cp .mcp.example.json .mcp.json
```

2. Start using the server from any MCP client that reads `.mcp.json`.

The MCP server command is:

```bash
uv run health-mcp
```

## Exposed MCP Tools

- `health_pipeline_status`: checks InfluxDB connectivity and optional OpenClaw health
- `generate_weekly_report`: generates weekly report text and optional infographic
- `send_weekly_report`: generates and sends weekly report via OpenClaw
- `generate_daily_report`: generates morning/evening daily report and optional infographic
- `send_daily_report`: generates and sends morning/evening report via OpenClaw
- `inspect_dlq`: inspects recent DLQ entries
- `dlq_stats`: returns DLQ aggregate stats and category distribution
- `replay_dlq`: preview or execute DLQ replay (`execute=false` by default)
- `archive_stats`: archive file counts/sizes and retention settings
- `metric_catalog`: lists measurements/fields plus query parameter options
- `query_metric_timeseries`: structured measurement/field/range queries with summaries
- `run_flux_query`: guarded read-only custom Flux query tool (result-capped)
- `preview_ingest_payload`: validates + transforms a sample payload and previews generated points
- `key_metrics_today`: top daily metrics with 7d/28d baselines and ranked changes
- `sleep_status`: sleep-focused facts + recommendations
- `activity_status`: activity-focused facts + recommendations
- `heart_status`: heart-focused facts + recommendations
- `recovery_status`: readiness score from sleep + heart signals
- `trend_alerts`: 3-day sustained deviations above 15% from baseline
- `top_metric_changes`: strongest improvements and declines split explicitly
- `body_status`: body/weight trend status
- `metric_pack`: one-call router for `recovery|activity|sleep|heart|body`
- `analysis_contracts`: lists analysis prompt/request contracts with versions
- `build_analysis_prompt`: renders versioned prompts with dataset hash metadata for review/regression checks

## Security Notes

- `.mcp.json` is intentionally local and ignored by git.
- Avoid hardcoding secrets in `.mcp.json`; prefer environment variable references.
- `run_flux_query` rejects obvious write/mutation operators to keep usage read-only.
- `replay_dlq` defaults to preview mode; set `execute=true` only for intentional reprocessing.
- Status-style tools return a fixed schema: `facts`, `interpretation`, `priority`, `confidence`,
  plus command observability (`latency`, `cost`, `quality`).
- If you add an external GitHub MCP server, use a fine-grained PAT with least privilege:
  - limit to selected repositories
  - read-only scopes by default
  - add write scopes only if you need file/PR/issue mutation tools
