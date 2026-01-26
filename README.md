# Apple Health Backup & Analysis System

Backup and analyze Apple Health data from Health Auto Export iOS app. Data flows via MQTT, stored in InfluxDB, visualized in Grafana, with AI-powered insights.

## Architecture

```
Health Auto Export (iPhone) --> MQTT --> health-ingest --> InfluxDB --> Grafana
                                                              |
                                                              v
                                                      Weekly AI Reports
```

## Quick Start

1. Copy `.env.example` to `.env` and configure:
   ```bash
   cp .env.example .env
   # Edit .env with your settings
   ```

2. Create storage directories:
   ```bash
   sudo mkdir -p /mnt/nvme/health/{influxdb,influxdb-config,grafana}
   sudo chown -R $USER:$USER /mnt/nvme/health
   ```

3. Start the stack:
   ```bash
   docker compose up -d
   ```

4. Configure Health Auto Export app:
   - Enable MQTT export
   - Set broker to your server IP (port 1883)
   - Set topic prefix to `health/export`

## Services

| Service | Port | Description |
|---------|------|-------------|
| health-ingest | - | MQTT to InfluxDB ingestion |
| InfluxDB | 8087 | Time-series database |
| Grafana | 3001 | Dashboards |

## Development

```bash
# Install dependencies
uv sync

# Run tests
uv run pytest

# Run linting
uv run ruff check src/

# Generate weekly report
uv run health-report
```

## Data Model

| Measurement | Fields |
|-------------|--------|
| heart | bpm, resting_bpm, hrv_ms |
| activity | steps, active_calories, exercise_min, stand_hours |
| sleep | duration_min, deep_min, rem_min, core_min, quality_score |
| workout | duration_min, calories, distance_m, avg_hr, max_hr |
| body | weight_kg, body_fat_pct, bmi |
| vitals | spo2_pct, respiratory_rate, bp_systolic, bp_diastolic |
