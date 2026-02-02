# Development

## Setup

```bash
# Install dependencies
uv sync

# Install with dev dependencies
uv sync --group dev
```

## Testing

```bash
# Run all tests
uv run pytest

# Run with verbose output
uv run pytest -v

# Run specific test file
uv run pytest tests/test_transformers.py

# Run with coverage
uv run pytest --cov=health_ingest
```

## Linting

```bash
# Check for issues
uv run ruff check src/

# Auto-fix issues
uv run ruff check src/ --fix

# Format code
uv run ruff format src/
```

## Local REST API Testing

```bash
# Send test data via REST API
curl -X POST http://localhost:8084/ingest \
  -H "Authorization: Bearer <your-HTTP_AUTH_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"data":[{"name":"heart_rate","date":"2024-01-15T10:30:00+00:00","qty":72,"source":"Apple Watch","units":"bpm"}]}'

# Check HTTP health endpoint
curl http://localhost:8084/health
```

The FastAPI-powered OpenAPI schema is available at `http://localhost:8084/openapi.json`,
and interactive documentation is served at `http://localhost:8084/docs` for SDK generation
workflows.

## CLI Commands

| Command | Description |
|---------|-------------|
| `health-ingest` | Start the REST API ingestion service |
| `health-check` | Verify InfluxDB connectivity and config |
| `health-query` | Ad-hoc InfluxDB queries for health data |
| `health-report` | Generate a weekly health report to stdout |
| `health-report-send` | Generate and send weekly report via Telegram |
| `health-daily` | Generate a daily health report to stdout |
| `health-daily-send` | Generate and send daily report via Telegram |
| `health-archive` | Manage raw payload archives (stats, compress, cleanup) |
| `health-archive-replay` | Replay archived payloads by date range |
| `health-dlq-inspect` | Inspect dead-letter queue entries |
| `health-dlq-replay` | Replay failed messages from the DLQ |

```bash
# Start the ingestion service
uv run health-ingest

# Generate a weekly health report
uv run health-report

# Query recent heart rate data
uv run health-query heart -f resting_bpm -r 7d

# Inspect dead-letter queue
uv run health-dlq-inspect

# Run with custom log level
APP_LOG_LEVEL=DEBUG uv run health-ingest

# Query archived payloads locally with DuckDB
uv run health-duckdb --sql "SELECT topic, COUNT(*) c FROM raw_archive GROUP BY 1"

# Export archived payloads to Parquet
uv run health-duckdb --export-parquet /data/exports/raw_archive.parquet
```
