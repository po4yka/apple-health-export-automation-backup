# Apple Health Backup & Analysis System

A self-hosted system to backup, store, and analyze Apple Health data exported from the [Health Auto Export](https://apps.apple.com/app/health-auto-export-json-csv/id1115567069) iOS app. Data flows via REST API to InfluxDB for time-series storage, with Grafana dashboards for visualization and AI-powered weekly health insights delivered via Telegram.

## Features

- **REST API Ingestion** -- receives health data via authenticated HTTP endpoint with deduplication and dead-letter queue
- **Time-Series Storage** -- InfluxDB 2.x with structured measurements for heart, activity, sleep, workouts, body, vitals, mobility, and audio
- **Rich Dashboards** -- pre-configured Grafana dashboards for all metric types
- **AI-Powered Insights** -- weekly health reports via Anthropic, OpenAI, or Grok
- **MCP Interface** -- first-class MCP tools for health status, report generation, and delivery
- **Telegram Bot** -- on-demand health queries ([details](docs/telegram-bot.md))
- **Docker Deployment** -- single command with Docker Compose
- **Extensible Transformers** -- modular architecture for adding new health metric types

## Architecture

```
Health Auto Export iOS
  --> POST /ingest (Bearer auth)
  --> FastAPI HTTPHandler
    --> RawArchiver        (persist raw payload to disk)
    --> TransformerRegistry (route to metric-specific transformer)
    --> DeduplicationCache  (SHA-256, LRU + SQLite)
    --> InfluxWriter        (async batch writes, circuit breaker)
    --> InfluxDB --> Grafana
```

Failed writes go to a SQLite-backed dead-letter queue (DLQ) for replay.

## Quick Start

### 1. Clone and Configure

```bash
git clone https://github.com/po4yka/apple-health-export-automation-backup.git
cd apple-health-export-automation-backup
cp .env.example .env
```

Edit `.env` with your settings (see [Configuration Reference](docs/configuration.md)):

```bash
# Required
INFLUXDB_TOKEN=$(openssl rand -hex 32)
INFLUXDB_ADMIN_PASSWORD=$(openssl rand -base64 16)
GRAFANA_ADMIN_PASSWORD=$(openssl rand -base64 16)
HTTP_AUTH_TOKEN=$(openssl rand -hex 32)

# Optional: AI weekly reports
ANTHROPIC_API_KEY=sk-ant-...
```

### 2. Create Storage Directories

```bash
sudo mkdir -p /mnt/nvme/health/{influxdb,influxdb-config,grafana,archive,dedup,dlq}
sudo chown -R $USER:$USER /mnt/nvme/health
```

### 3. Start the Stack

```bash
docker compose up -d
```

### 4. Verify

```bash
docker compose ps
curl http://localhost:8084/health
```

### 5. Access Dashboards

- **Grafana**: http://localhost:3001 (admin / your password)
- **InfluxDB**: http://localhost:8087 (admin / your password)

### 6. Configure iOS App

See [iOS App Setup](docs/ios-app-setup.md) for Health Auto Export configuration.

## Services

| Service | Container | Port | Description |
|---------|-----------|------|-------------|
| health-ingest | `health-ingest` | 8084 | REST API ingestion, transforms and writes to InfluxDB |
| InfluxDB | `health-influxdb` | 8087 | Time-series database for health metrics |
| Grafana | `health-grafana` | 3001 | Visualization dashboards |

## CLI Commands

| Command | Description |
|---------|-------------|
| `health-ingest` | Start the REST API ingestion service |
| `health-check` | Verify InfluxDB connectivity and config |
| `health-query` | Ad-hoc InfluxDB queries for health data |
| `health-report` | Generate a weekly health report to stdout |
| `health-report-send` | Generate and send weekly report via Telegram (`--infographic-out ./weekly.svg` exports an SVG infographic) |
| `health-daily` | Generate a daily health report to stdout (`morning/evening`; `--infographic-out ./daily.svg` exports an SVG infographic) |
| `health-daily-send` | Generate and send daily report via Telegram (`--infographic-out ./daily.svg` exports an SVG infographic) |
| `health-mcp` | Run MCP server (`stdio`) for Claude Code/OpenClaw integrations |
| `health-archive` | Manage raw payload archives (stats, compress, cleanup) |
| `health-archive-replay` | Replay archived payloads by date range |
| `health-dlq-inspect` | Inspect dead-letter queue entries |
| `health-dlq-replay` | Replay failed messages from the DLQ |

All commands are run via `uv run <command>`. See [Development](docs/development.md) for full usage examples.

## Data Model

**Organization**: `health` | **Bucket**: `apple_health` | **Retention**: Infinite

See [Data Model](docs/data-model.md) for the complete InfluxDB schema (measurements, tags, and fields).

## Development

```bash
uv sync --group dev          # install with dev dependencies
uv run pytest                # run all tests
uv run ruff check src/ tests/  # lint
uv run ruff format src/ tests/ # format
```

See [Development Guide](docs/development.md) for local API testing, coverage, and detailed CLI usage.

## Further Documentation

| Document | Contents |
|----------|----------|
| [Configuration](docs/configuration.md) | Full environment variable reference |
| [Development](docs/development.md) | Setup, testing, linting, CLI commands |
| [Operations](docs/operations.md) | Troubleshooting, security, backups, Cloudflare Tunnel |
| [iOS App Setup](docs/ios-app-setup.md) | Health Auto Export app configuration |
| [Telegram Bot](docs/telegram-bot.md) | Bot commands, setup, and troubleshooting |
| [Data Model](docs/data-model.md) | InfluxDB schema reference |
| [MCP](docs/mcp.md) | MCP setup, tools, and security guidance |

## License

MIT License -- see [LICENSE](LICENSE) for details.
