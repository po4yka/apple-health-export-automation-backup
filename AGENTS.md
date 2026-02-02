# AGENTS.md

> Universal context file for AI coding agents ([agents.md convention](https://agents.md/)).

## Project overview

Self-hosted Apple Health data pipeline. The iOS **Health Auto Export** app sends JSON
payloads via REST API to a **FastAPI** ingestion service that deduplicates, transforms,
and writes health metrics to **InfluxDB 2.x**. **Grafana** dashboards visualise the data.
A weekly report job generates AI-powered health insights (Anthropic / OpenAI / Grok)
delivered via Telegram.

**Stack:** Python 3.13, FastAPI, Pydantic v2 (pydantic-settings), InfluxDB 2.x,
Grafana, Docker Compose, uv (package manager), Ruff (linter/formatter).

## Commands

```bash
# Install (includes dev deps)
uv sync --group dev

# Tests — asyncio_mode=auto, no @pytest.mark.asyncio needed
uv run pytest                              # all tests
uv run pytest tests/test_transformers.py   # single file
uv run pytest -k "test_heart_rate_transform"  # single test

# Lint & format
uv run ruff check src/ tests/             # lint
uv run ruff check src/ tests/ --fix       # auto-fix
uv run ruff format src/ tests/            # format

# Run locally (requires InfluxDB)
uv run health-ingest

# Full stack via Docker
docker compose up -d
```

## Architecture

```
Health Auto Export iOS
  → POST /ingest (Bearer auth)
  → FastAPI HTTPHandler
    → RawArchiver        (persist raw payload to disk)
    → TransformerRegistry (route to metric-specific transformer)
    → DeduplicationCache  (SHA-256, LRU + SQLite)
    → InfluxWriter        (async batch writes, circuit breaker)
    → InfluxDB → Grafana
```

Failed writes go to a SQLite-backed dead-letter queue (DLQ) for replay.

### Key modules (`src/health_ingest/`)

| Module | Role |
|---|---|
| `main.py` | Orchestrator, signal handling, asyncio loop |
| `http_handler.py` | FastAPI routes: `/ingest`, `/health`, `/ready`, `/dlq`, `/reports/weekly`, `/bot/webhook` |
| `config.py` | Pydantic-settings, nested classes, env prefix per subsystem |
| `influx_writer.py` | Batched async InfluxDB writer with retry + circuit breaker |
| `circuit_breaker.py` | CLOSED / OPEN / HALF_OPEN state machine |
| `transformers/` | Priority-ordered registry; subclass `BaseTransformer` |
| `reports/` | Weekly/daily reports: Flux queries, AI insights, Telegram delivery |
| `bot/` | Telegram bot: command dispatcher, query service, formatter |
| `query.py` | CLI query builder for ad-hoc InfluxDB queries |
| `cli.py` | CLI entry points (health-query, health-check, health-archive, etc.) |
| `dedup.py` | LRU cache + SQLite for idempotent ingestion |
| `dlq.py` | Dead-letter queue with categorised failures |
| `archive.py` | Raw payload archiving with rotation/compression |
| `schema_validation.py` | Payload validation before transformation |
| `duckdb_analytics.py` | DuckDB-based archive analytics and Parquet export |
| `metrics.py` | Prometheus metrics instrumentation |
| `tracing.py` | OpenTelemetry tracing setup |

### Payload formats

The registry handles two formats from Health Auto Export:

- **REST API (current):** `{"data": {"metrics": [{"name": "...", "units": "...", "data": [...]}]}}`
- **Flat list (legacy):** `{"data": [{"name": "...", "date": "...", "qty": ...}]}`

## Code style

- **Python 3.13+**. Ruff with `target-version = "py313"`, `line-length = 100`.
- Ruff rule sets: `E, F, I, N, W, UP, B, C4, SIM`.
  - Exceptions: `N815` (camelCase Pydantic aliases), `SIM105` (contextlib.suppress + await).
  - `E501` suppressed in `reports/weekly.py` and `reports/rules.py` (long Flux queries).
- Tests use `pytest-asyncio` with `asyncio_mode = "auto"` — no async markers needed.
- Config: add env vars as fields on the appropriate nested settings class in `config.py`
  with the correct `env_prefix`.
- New metric transformer: subclass `BaseTransformer`, implement `can_transform()` +
  `transform()`, register in `TransformerRegistry.__init__` **before** `GenericTransformer`.

## Git workflow

- Commit messages follow a short imperative style (e.g., "Add CodeQL vulnerability
  scanning workflow", "Fix lint issues", "Improve type safety for ingestion payloads").
- CI (GitHub Actions) runs on push/PR to `main`:
  1. **lint** — `ruff check` + `ruff format --check`
  2. **test** — `pytest --tb=short -q`
  3. **docker** — build image, verify import
- CodeQL scans for vulnerability analysis.

## Boundaries

- **Never** commit `.env` or files containing secrets/tokens.
- **Never** modify `grafana/provisioning/dashboards/*.json` without explicit request
  (these are auto-provisioned).
- **Never** change InfluxDB schema (measurements, tags, fields) without updating
  `docs/data-model.md`.
- **Don't** add dependencies without justification — the project uses `uv` with a
  locked `uv.lock`.
- **Don't** modify CI workflows (`.github/workflows/`) without explicit request.
