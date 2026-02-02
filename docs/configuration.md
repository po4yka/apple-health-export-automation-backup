# Configuration Reference

## Environment Variables

The full configuration lives in `.env`. Start by copying `.env.example` and filling out the required values.

### HTTP Ingestion

| Variable | Default | Description |
|----------|---------|-------------|
| `HTTP_ENABLED` | `true` | Enable HTTP REST API ingestion endpoint |
| `HTTP_HOST` | `0.0.0.0` | HTTP server bind address |
| `HTTP_PORT` | `8080` | HTTP server port (inside container) |
| `HTTP_PORT_EXTERNAL` | `8084` | Host port mapped to HTTP_PORT |
| `HTTP_AUTH_TOKEN` | - | Bearer token for HTTP authentication |
| `HTTP_MAX_REQUEST_SIZE` | `10485760` | Maximum request body size in bytes |

### InfluxDB

| Variable | Default | Description |
|----------|---------|-------------|
| `INFLUXDB_URL` | `http://influxdb:8086` | InfluxDB connection URL |
| `INFLUXDB_TOKEN` | **required** | InfluxDB API token |
| `INFLUXDB_ORG` | `health` | InfluxDB organization |
| `INFLUXDB_BUCKET` | `apple_health` | InfluxDB bucket name |
| `INFLUXDB_BATCH_SIZE` | `1000` | Points per batch write |
| `INFLUXDB_FLUSH_INTERVAL_MS` | `30000` | Flush interval (ms) |

### Application

| Variable | Default | Description |
|----------|---------|-------------|
| `APP_LOG_LEVEL` | `INFO` | Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL) |
| `APP_LOG_FORMAT` | `json` | Log format (json, console) |
| `APP_DEFAULT_SOURCE` | `health_auto_export` | Default source tag for metrics |
| `APP_PROMETHEUS_PORT` | `9090` | Prometheus metrics server port |
| `INSIGHT_AI_PROVIDER` | `anthropic` | AI provider for weekly reports (`anthropic`, `openai`, `grok`) |
| `ANTHROPIC_API_KEY` | - | Anthropic API key for weekly reports |
| `ANTHROPIC_MODEL` | `claude-sonnet-4-20250514` | Anthropic model for weekly reports |
| `OPENAI_API_KEY` | - | OpenAI API key for weekly reports |
| `OPENAI_MODEL` | `gpt-4o-mini` | OpenAI model for weekly reports |
| `OPENAI_BASE_URL` | `https://api.openai.com/v1` | OpenAI-compatible base URL |
| `GROK_API_KEY` | - | Grok (xAI) API key for weekly reports |
| `GROK_MODEL` | `grok-2-latest` | Grok (xAI) model for weekly reports |
| `GROK_BASE_URL` | `https://api.x.ai/v1` | Grok OpenAI-compatible base URL |
| `INSIGHT_PREFER_AI` | `true` | Prefer AI insights over rule-based |
| `INSIGHT_MAX_INSIGHTS` | `5` | Maximum insights per report |
| `INSIGHT_AI_TIMEOUT_SECONDS` | `30.0` | AI API timeout in seconds |
| `INSIGHT_INCLUDE_REASONING` | `true` | Include reasoning in AI insights |

### Telegram Delivery (Optional)

| Variable | Default | Description |
|----------|---------|-------------|
| `OPENCLAW_ENABLED` | `true` | Enable Telegram delivery via OpenClaw |
| `OPENCLAW_GATEWAY_URL` | `http://openclaw-gateway:18789` | OpenClaw gateway URL |
| `OPENCLAW_HOOKS_TOKEN` | - | Hooks API authentication token |
| `OPENCLAW_TELEGRAM_USER_ID` | `0` | Target Telegram user ID |
| `OPENCLAW_MAX_RETRIES` | `3` | Maximum delivery retries |
| `OPENCLAW_RETRY_DELAY_SECONDS` | `5.0` | Initial retry delay in seconds |

### Telegram Bot

| Variable | Default | Description |
|----------|---------|-------------|
| `BOT_ENABLED` | `false` | Enable Telegram bot webhook endpoint |
| `BOT_WEBHOOK_TOKEN` | - | Bearer token for bot webhook authentication |
| `BOT_RESPONSE_TIMEOUT_SECONDS` | `15.0` | Timeout for bot command processing |

### Archive

| Variable | Default | Description |
|----------|---------|-------------|
| `ARCHIVE_ENABLED` | `true` | Enable raw payload archiving |
| `ARCHIVE_DIR` | `/data/archive` | Archive directory path |
| `ARCHIVE_ROTATION` | `daily` | Rotation strategy (daily, hourly) |
| `ARCHIVE_MAX_AGE_DAYS` | `30` | Delete archives older than N days |
| `ARCHIVE_COMPRESS_AFTER_DAYS` | `7` | Compress archives older than N days |

### Deduplication

| Variable | Default | Description |
|----------|---------|-------------|
| `DEDUP_ENABLED` | `true` | Enable deduplication |
| `DEDUP_MAX_SIZE` | `100000` | Maximum cache entries |
| `DEDUP_TTL_HOURS` | `24` | TTL for cache entries in hours |
| `DEDUP_PERSIST_ENABLED` | `true` | Enable SQLite persistence |
| `DEDUP_PERSIST_PATH` | `/data/dedup/cache.db` | Persistence file path |
| `DEDUP_CHECKPOINT_INTERVAL_SEC` | `300` | Checkpoint interval in seconds |

### Dead-Letter Queue

| Variable | Default | Description |
|----------|---------|-------------|
| `DLQ_ENABLED` | `true` | Enable dead-letter queue |
| `DLQ_DB_PATH` | `/data/dlq/dlq.db` | SQLite database path |
| `DLQ_MAX_ENTRIES` | `10000` | Maximum entries before eviction |
| `DLQ_RETENTION_DAYS` | `30` | Delete entries older than N days |
| `DLQ_MAX_RETRIES` | `3` | Maximum replay attempts |

### Tracing (Optional)

| Variable | Default | Description |
|----------|---------|-------------|
| `OTEL_ENABLED` | `false` | Enable OpenTelemetry tracing |
| `OTEL_SERVICE_NAME` | `health-ingest` | Service name for traces |

### DLQ Categories

- `json_parse_error`
- `unicode_decode_error`
- `validation_error`
- `transform_error`
- `write_error`
- `unknown_error`

## Persistent Storage

The Docker Compose file mounts host directories under `/mnt/nvme/health` by default:

```
/mnt/nvme/health/
├── archive/           # Raw ingestion payloads
├── dedup/             # Deduplication cache
├── dlq/               # Dead-letter queue entries
├── influxdb/          # InfluxDB data
├── influxdb-config/   # InfluxDB config
└── grafana/           # Grafana data
```

If you prefer a different location, update the `volumes:` paths in `docker-compose.yml` and ensure the directories exist with appropriate permissions.
