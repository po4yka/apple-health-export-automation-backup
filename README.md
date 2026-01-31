# Apple Health Backup & Analysis System

A self-hosted system to backup, store, and analyze Apple Health data exported from the [Health Auto Export](https://apps.apple.com/app/health-auto-export-json-csv/id1115567069) iOS app. Data flows via REST API to InfluxDB for time-series storage, with Grafana dashboards for visualization and AI-powered weekly health insights.

## Features

- **Automated Data Ingestion**: Receives health data via REST API from Health Auto Export app
- **Resilient Ingestion**: Backpressure with bounded queue, raw payload archiving, deduplication, and DLQ handling
- **Time-Series Storage**: Stores all metrics in InfluxDB 2.x with infinite retention
- **Rich Dashboards**: Pre-configured Grafana dashboards for activity, heart rate, sleep, workouts, and vitals
- **AI-Powered Insights**: Weekly health reports with personalized recommendations via Claude API
- **Docker Deployment**: Single command deployment with Docker Compose
- **Extensible Transformers**: Modular architecture for adding new health metric types

## Architecture

```
                          REST API
┌─────────────────────┐  POST /ingest via HTTPS    ┌──────────────────────┐
│  Health Auto Export │ ──────────────────────────▶│  Cloudflare Tunnel   │
│  (iPhone)           │                            │                      │
└─────────────────────┘                            └──────────┬───────────┘
                                                              │
                                                              ▼
                                                   ┌──────────────────────┐
                                                   │  health-ingest       │
                                                   │  HTTP :8084          │
                                                   └──────────┬───────────┘
                                                              │
┌─────────────────────┐     InfluxDB Query                    ▼
│  Grafana            │◀──────────────────────────┌──────────────────────┐
│  (port 3050)        │                           │  InfluxDB 2.x        │
│  health.example.com │                           │  (port 8087)         │
└─────────────────────┘                           └──────────────────────┘
```

## Prerequisites

- **Docker** and **Docker Compose** v2+
- **Python 3.13+** (for local development)
- **[uv](https://github.com/astral-sh/uv)** package manager (for local development)
- **Health Auto Export** iOS app (paid, ~$3)
- **Anthropic API key** (optional, for AI-powered weekly reports)

## Quick Start

### 1. Clone and Configure

```bash
git clone https://github.com/yourusername/apple-health-export-automation-backup.git
cd apple-health-export-automation-backup

# Create environment file
cp .env.example .env
```

Edit `.env` with your settings:

```bash
# Required: Generate a secure token for InfluxDB
INFLUXDB_TOKEN=$(openssl rand -hex 32)
INFLUXDB_ADMIN_PASSWORD=$(openssl rand -base64 16)
GRAFANA_ADMIN_PASSWORD=$(openssl rand -base64 16)

# Optional: For AI weekly reports
ANTHROPIC_API_KEY=sk-ant-...
```

### 2. Create Storage Directories

```bash
sudo mkdir -p /mnt/nvme/health/{influxdb,influxdb-config,grafana}
sudo chown -R $USER:$USER /mnt/nvme/health
```

### 3. Start the Stack

```bash
docker compose up -d
```

### 4. Verify Services

```bash
# Check all services are running
docker compose ps

# View ingestion logs
docker logs -f health-ingest
```

### 5. Access Dashboards

- **Grafana**: http://localhost:3050 (admin / your password)
- **InfluxDB**: http://localhost:8087 (admin / your password)

## Health Auto Export Configuration

### REST API (Recommended)

Configure the iOS app to send data via REST API through Cloudflare Tunnel:

1. Open **Health Auto Export** app on your iPhone
2. Go to **Settings** -> **Automations**
3. Create a new automation:
   - **Trigger**: Daily at 23:00 (or your preferred time)
   - **Export Format**: JSON
   - **Destination**: REST API
4. Configure REST API settings:
   - **URL**: `https://health-api.yourdomain.com/ingest`
   - **Method**: POST
   - **Headers**: `Authorization: Bearer <your-HTTP_AUTH_TOKEN>`
5. Select metrics to export (see list below)

### Recommended Metrics

   - Heart Rate, Resting Heart Rate, HRV
   - Steps, Active Energy, Exercise Time
   - Sleep Analysis
   - Workouts
   - Weight, Body Fat
   - Blood Oxygen, Respiratory Rate

## Services

| Service | Container | Port | Description |
|---------|-----------|------|-------------|
| health-ingest | `health-ingest` | 8084 | REST API ingestion, transforms and writes to InfluxDB |
| InfluxDB | `health-influxdb` | 8087 | Time-series database for health metrics |
| Grafana | `health-grafana` | 3050 | Visualization dashboards |

## Data Model

### InfluxDB Schema

**Organization**: `health` | **Bucket**: `apple_health` | **Retention**: Infinite

| Measurement | Tags | Fields |
|-------------|------|--------|
| `heart` | source | bpm, resting_bpm, hrv_ms |
| `activity` | source | steps, active_calories, basal_calories, exercise_min, stand_hours, floors_climbed |
| `sleep` | source | duration_min, deep_min, rem_min, core_min, awake_min, in_bed_min, quality_score |
| `workout` | source, workout_type | duration_min, calories, distance_m, avg_hr, max_hr |
| `body` | source | weight_kg, body_fat_pct, bmi, lean_mass_kg |
| `vitals` | source | spo2_pct, respiratory_rate, bp_systolic, bp_diastolic, temp_c, vo2max |
| `other` | source, metric_type, unit | value, min, max, avg |

### Supported Workout Types

Running, Walking, Cycling, Swimming, Strength Training, HIIT, Yoga, Pilates, Elliptical, Rowing, Stair Climbing, Core Training, Flexibility, and more.

## CLI Commands

```bash
# Start the ingestion service
uv run health-ingest

# Generate a weekly health report
uv run health-report

# Run with custom log level
APP_LOG_LEVEL=DEBUG uv run health-ingest
```

## Development

### Setup

```bash
# Install dependencies
uv sync

# Install with dev dependencies
uv sync --group dev
```

### Testing

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

### Linting

```bash
# Check for issues
uv run ruff check src/

# Auto-fix issues
uv run ruff check src/ --fix

# Format code
uv run ruff format src/
```

### Local REST API Testing

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

## Project Structure

```
apple-health-export-automation-backup/
├── pyproject.toml              # Project metadata and dependencies
├── uv.lock                     # Locked dependencies
├── Dockerfile                  # Container build instructions
├── docker-compose.yml          # Service orchestration
├── .env.example                # Environment template
├── README.md                   # This file
├── src/
│   └── health_ingest/
│       ├── __init__.py
│       ├── main.py             # Application entry point
│       ├── config.py           # Pydantic settings
│       ├── logging.py          # Structured logging setup
│       ├── http_handler.py     # FastAPI ingestion endpoint + OpenAPI docs
│       ├── influx_writer.py    # Async batch writes to InfluxDB
│       ├── archive.py          # Raw payload archiver
│       ├── dedup.py            # Deduplication cache
│       ├── dlq.py              # Dead-letter queue
│       ├── transformers/       # Data transformation modules
│       │   ├── base.py         # Base classes and Pydantic models
│       │   ├── registry.py     # Transformer routing
│       │   ├── heart.py        # Heart rate, HRV
│       │   ├── activity.py     # Steps, calories, exercise
│       │   ├── sleep.py        # Sleep stages and quality
│       │   ├── workout.py      # Exercise sessions
│       │   ├── body.py         # Weight, body composition
│       │   ├── vitals.py       # SpO2, respiratory, BP
│       │   └── generic.py      # Fallback for unknown metrics
│       └── reports/
│           └── weekly.py       # AI-powered weekly reports
├── tests/
│   ├── conftest.py             # Pytest fixtures
│   ├── test_transformers.py    # Transformer unit tests
│   ├── test_http_handler.py    # HTTP handler tests
│   └── test_influx_writer.py   # Influx writer buffer tests
└── grafana/
    └── provisioning/
        ├── datasources/
        │   └── influxdb.yml    # InfluxDB datasource config
        └── dashboards/
            ├── dashboard.yml   # Dashboard provisioning
            └── health-overview.json  # Main dashboard
```

## Configuration Reference

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `HTTP_ENABLED` | `true` | Enable HTTP REST API ingestion endpoint |
| `HTTP_PORT` | `8080` | HTTP server port (inside container) |
| `HTTP_PORT_EXTERNAL` | `8084` | Host port mapped to HTTP_PORT |
| `HTTP_AUTH_TOKEN` | - | Bearer token for HTTP authentication |
| `HTTP_MAX_REQUEST_SIZE` | `10485760` | Maximum request body size in bytes |
| `INFLUXDB_URL` | `http://influxdb:8086` | InfluxDB connection URL |
| `INFLUXDB_TOKEN` | **required** | InfluxDB API token |
| `INFLUXDB_ORG` | `health` | InfluxDB organization |
| `INFLUXDB_BUCKET` | `apple_health` | InfluxDB bucket name |
| `INFLUXDB_BATCH_SIZE` | `1000` | Points per batch write |
| `INFLUXDB_FLUSH_INTERVAL_MS` | `30000` | Flush interval (ms) |
| `APP_LOG_LEVEL` | `INFO` | Log level (DEBUG, INFO, WARNING, ERROR) |
| `APP_LOG_FORMAT` | `json` | Log format (json, console) |
| `APP_PROMETHEUS_PORT` | `9090` | Prometheus metrics server port |
| `ANTHROPIC_API_KEY` | - | Anthropic API key for weekly reports |

### DLQ Categories

- `json_parse_error`
- `unicode_decode_error`
- `validation_error`
- `transform_error`
- `write_error`
- `unknown_error`

## Troubleshooting

### No data appearing in Grafana

1. Check if health-ingest is receiving messages:
   ```bash
   docker logs -f health-ingest
   ```

2. Test the REST API endpoint:
   ```bash
   curl -s http://localhost:8084/health
   curl -X POST http://localhost:8084/ingest \
     -H "Authorization: Bearer <token>" \
     -H "Content-Type: application/json" \
     -d '{"data":[{"name":"heart_rate","date":"2024-01-15T10:30:00Z","qty":72}]}'
   ```

3. Check InfluxDB has data:
   ```bash
   docker exec -it health-influxdb influx query \
     'from(bucket:"apple_health") |> range(start:-1h) |> limit(n:10)'
   ```

### Health Auto Export not sending data

1. **REST API**: Verify the URL and Bearer token are correct in the iOS app settings
2. Verify automation is enabled and scheduled correctly
3. Try manual export to test connectivity

### InfluxDB connection errors

1. Verify token is correct in `.env`
2. Check InfluxDB container is healthy:
   ```bash
   docker exec health-influxdb influx ping
   ```

### Weekly report errors

1. Verify `ANTHROPIC_API_KEY` is set correctly
2. Check you have sufficient API credits
3. View detailed logs:
   ```bash
   APP_LOG_LEVEL=DEBUG uv run health-report
   ```

## Cloudflare Tunnel Setup (Optional)

To expose services publicly via Cloudflare:

1. Add routes in [Cloudflare Zero Trust Dashboard](https://one.dash.cloudflare.com/):
   - **Hostname**: `health.yourdomain.com` -> `http://localhost:3050` (Grafana)
   - **Hostname**: `health-api.yourdomain.com` -> `http://localhost:8084` (REST API)

2. Configure Grafana root URL in `.env`:
   ```
   GRAFANA_ROOT_URL=https://health.yourdomain.com
   ```

## License

MIT License - see [LICENSE](LICENSE) for details.
