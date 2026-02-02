# Apple Health OpenClaw Skill

OpenClaw workspace skill for querying and managing Apple Health data.

## Installation

### 1. Install CLI tools

From the repository root:

```bash
uv tool install .
```

Or from GitHub:

```bash
uv tool install git+https://github.com/po4yka/apple-health-export-automation-backup
```

Verify:

```bash
health-query --help
```

### 2. Register the skill

Symlink into the OpenClaw workspace skills directory:

```bash
ln -s "$(pwd)/skills/apple-health" ~/.openclaw/workspace/skills/apple-health
```

Or copy:

```bash
cp -r skills/apple-health ~/.openclaw/workspace/skills/apple-health
```

### 3. Set environment variables

Required:

```bash
export INFLUXDB_TOKEN="your-influxdb-token"
```

Optional (defaults shown):

```bash
export INFLUXDB_URL="http://influxdb:8086"
export INFLUXDB_ORG="health"
export INFLUXDB_BUCKET="apple_health"
export ANTHROPIC_API_KEY="sk-..."       # For AI insights in reports
export OPENCLAW_HOOKS_TOKEN="..."       # For Telegram delivery
```

## Installed Commands

| Command | Description |
|---------|-------------|
| `health-query` | Ad-hoc InfluxDB queries for health data |
| `health-ingest` | Main ingestion service (REST API) |
| `health-check` | Verify InfluxDB connectivity |
| `health-report` | Generate weekly report to stdout |
| `health-report-send` | Generate and send report via Telegram |
| `health-dlq-inspect` | Inspect dead-letter queue |
| `health-dlq-replay` | Replay failed messages |
| `health-archive` | Manage raw payload archives |
| `health-archive-replay` | Replay archived messages |

## Quick Test

```bash
# Check connectivity
health-check

# Query recent heart rate data
health-query heart -f resting_bpm -r 7d

# Query steps as JSON
health-query activity -f steps -r 24h -a sum --format json
```
