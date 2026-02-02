# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

See [AGENTS.md](AGENTS.md) for project overview, architecture, key modules, payload formats, code style, and boundaries.

## Commands

```bash
uv sync --group dev                        # install with dev deps
uv run pytest                              # all tests (asyncio_mode=auto)
uv run pytest tests/test_transformers.py   # single file
uv run pytest -k "test_heart_rate_transform"  # single test
uv run ruff check src/ tests/              # lint
uv run ruff check src/ tests/ --fix        # auto-fix
uv run ruff format src/ tests/             # format
uv run health-ingest                       # run locally (needs InfluxDB)
docker compose up -d                       # full stack
```
