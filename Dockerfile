# =============================================================================
# Apple Health Backup & Analysis System - Docker Image
# =============================================================================
# Multi-stage build for minimal image size using Python 3.13 and uv.
#
# Build: docker build -t health-ingest .
# Run:   docker run --env-file .env health-ingest
# =============================================================================

# syntax=docker/dockerfile:1
FROM python:3.13-slim AS builder

# Metadata
LABEL org.opencontainers.image.title="health-ingest"
LABEL org.opencontainers.image.description="Apple Health data ingestion from MQTT to InfluxDB"
LABEL org.opencontainers.image.source="https://github.com/po4yka/apple-health-export-automation-backup"
LABEL org.opencontainers.image.licenses="MIT"

# Environment configuration
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_SYSTEM_PYTHON=1 \
    UV_COMPILE_BYTECODE=1

# Install uv package manager
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Set work directory
WORKDIR /app

# Copy dependency files first for better layer caching
COPY pyproject.toml uv.lock* ./

# Install dependencies (without dev dependencies)
RUN uv sync --frozen --no-dev --no-install-project

# Copy source code
COPY src/ ./src/
COPY README.md ./

# Install the project itself
RUN uv sync --frozen --no-dev

# =============================================================================
# Production image
# =============================================================================
FROM python:3.13-slim

# Security: Run as non-root user
RUN useradd --create-home --shell /bin/bash appuser

# Environment
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Copy uv and installed packages from builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY --from=builder /app /app

# Switch to non-root user
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import health_ingest; print('ok')" || exit 1

# Run the service
CMD ["uv", "run", "health-ingest"]
