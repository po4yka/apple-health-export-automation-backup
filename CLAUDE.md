# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Self-hosted Apple Health data pipeline: iOS Health Auto Export app sends JSON via REST API to a FastAPI ingestion service, which transforms and writes to InfluxDB. Grafana provides dashboards. Weekly AI-powered health reports via Anthropic/OpenAI/Grok.

## Commands

```bash
# Install dependencies
uv sync --group dev

# Run all tests (asyncio_mode=auto, no markers needed on async tests)
uv run pytest

# Run a single test file
uv run pytest tests/test_transformers.py

# Run a single test by name
uv run pytest -k "test_heart_rate_transform"

# Lint and format
uv run ruff check src/ tests/
uv run ruff check src/ tests/ --fix
uv run ruff format src/ tests/

# Start the ingestion service locally (needs InfluxDB running)
uv run health-ingest

# Full stack via Docker
docker compose up -d
```

## Architecture

```
Health Auto Export iOS → POST /ingest (Bearer auth) → FastAPI HTTPHandler
  → RawArchiver (stores payload to disk)
  → TransformerRegistry (routes to specific transformer)
  → DeduplicationCache (SHA256 hash, LRU + SQLite)
  → InfluxWriter (async batch writes, circuit breaker)
  → InfluxDB → Grafana
```

Failed messages go to a SQLite-backed dead-letter queue (DLQ) for replay.

### Source layout

All application code is under `src/health_ingest/`. Key modules:

- **`main.py`** — Service orchestrator, signal handling, asyncio event loop
- **`http_handler.py`** — FastAPI routes (`/ingest`, `/health`, `/ready`, `/dlq`, `/reports/weekly`)
- **`influx_writer.py`** — Async batched InfluxDB writer with retry and circuit breaker
- **`config.py`** — Pydantic-settings with nested settings classes; env prefix per subsystem (e.g. `HTTP_`, `INFLUXDB_`, `ARCHIVE_`). Thread-safe singleton via `get_settings()`
- **`transformers/`** — Priority-ordered registry; each transformer subclasses `BaseTransformer` and implements `can_transform()` + `transform()`. `GenericTransformer` is the catch-all (always last)
- **`reports/`** — Weekly report generation: `weekly.py` (Flux queries), `insights.py` (AI provider dispatch), `rules.py` (rule-based fallback), `formatter.py`, `delivery.py` (Telegram via OpenClaw)
- **`dedup.py`** — LRU cache + SQLite persistence for idempotent ingestion
- **`dlq.py`** — SQLite dead-letter queue with categorized failures
- **`archive.py`** — Raw payload archiving with rotation/compression
- **`circuit_breaker.py`** — CLOSED→OPEN→HALF_OPEN state machine for InfluxDB writes

### Payload formats

The registry handles two formats from Health Auto Export:
- **REST API (current):** `{"data": {"metrics": [{"name": "...", "units": "...", "data": [...]}]}}`
- **Flat list (legacy):** `{"data": [{"name": "...", "date": "...", "qty": ...}]}`

## Code Conventions

- **Python 3.13+** required. Ruff with `target-version = "py313"`, line-length 100.
- Ruff rules: E, F, I, N, W, UP, B, C4, SIM. Exceptions: N815 (camelCase for Pydantic JSON aliases), SIM105 (contextlib.suppress incompatible with await).
- E501 suppressed in `reports/weekly.py` and `reports/rules.py` for long Flux query strings.
- Tests use `pytest-asyncio` with `asyncio_mode = "auto"` — no `@pytest.mark.asyncio` needed.
- Configuration: add new env vars as fields on the appropriate nested settings class in `config.py` with the correct `env_prefix`.
- New health metric types: create a transformer in `transformers/`, subclass `BaseTransformer`, then add it to the list in `TransformerRegistry.__init__` (order matters — more specific before `GenericTransformer`).
