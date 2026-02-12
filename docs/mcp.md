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
- `analysis_contracts`: lists analysis prompt/request contracts with versions

## Security Notes

- `.mcp.json` is intentionally local and ignored by git.
- Avoid hardcoding secrets in `.mcp.json`; prefer environment variable references.
- If you add an external GitHub MCP server, use a fine-grained PAT with least privilege:
  - limit to selected repositories
  - read-only scopes by default
  - add write scopes only if you need file/PR/issue mutation tools

