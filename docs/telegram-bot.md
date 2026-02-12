# Telegram Bot

On-demand health data queries via Telegram, powered by the OpenClaw gateway.

## Architecture

```
Telegram → OpenClaw Gateway → POST /bot/webhook → BotDispatcher
  → BotQueryService (InfluxDB) → Formatter → OpenClaw → Telegram
```

1. You send a command (e.g. `/health_now`) in your Telegram chat with the OpenClaw bot.
2. OpenClaw forwards the message to the health-ingest service via `POST /bot/webhook`.
3. The dispatcher parses the command, queries InfluxDB, and formats the response.
4. The formatted response is sent back through OpenClaw to your Telegram chat.

## Available Commands

| Command | Description |
|---------|-------------|
| `/health_now` | Quick snapshot: steps, calories, exercise, HR, HRV, weight |
| `/health_heart` | Resting HR, HRV with 7-day comparison |
| `/health_sleep` | Last night: duration, stages, quality score |
| `/health_weight` | Latest weight with 7d and 30d trend |
| `/health_today` | Full today: activity + heart + workouts |
| `/health_yesterday` | Full yesterday summary |
| `/health_week` | This week aggregated summary |
| `/health_steps [7d\|14d\|30d]` | Steps with daily breakdown |
| `/health_workouts [7d\|14d\|30d]` | Recent workout list |
| `/health_trends` | Key metrics this week vs last week |
| `/health_help` | List available commands |

`/health_steps` and `/health_workouts` accept an optional period argument (default: `7d`).

## Setup

### 1. Configure environment variables

Add to your `.env`:

```bash
# Enable the bot webhook
BOT_ENABLED=true
BOT_WEBHOOK_TOKEN=<secret-token-for-webhook-auth>
# Optional for local development only:
# BOT_ALLOW_UNAUTHENTICATED_WEBHOOK=true

# OpenClaw delivery (should already be configured for weekly reports)
OPENCLAW_ENABLED=true
OPENCLAW_HOOKS_TOKEN=<your-openclaw-hooks-token>
OPENCLAW_TELEGRAM_USER_ID=<your-telegram-user-id>
```

### 2. Configure OpenClaw webhook

In your OpenClaw configuration, set up a webhook that forwards Telegram messages matching `/health_*` commands to the health-ingest service:

```
POST http://health-ingest:8080/bot/webhook
Authorization: Bearer <BOT_WEBHOOK_TOKEN>
Content-Type: application/json

{
  "message": "/health_now",
  "user_id": 94225168
}
```

The webhook endpoint returns `202 Accepted` immediately and processes the command asynchronously.

### 3. Register commands with BotFather

Telegram's command autocomplete menu is configured separately via [@BotFather](https://t.me/BotFather). Message BotFather:

```
/setcommands
```

Select your bot, then paste:

```
health_now - Quick snapshot: steps, calories, exercise, HR, HRV, weight
health_heart - Resting HR, HRV with 7-day comparison
health_sleep - Last night: duration, stages, quality score
health_weight - Latest weight with 7d and 30d trend
health_today - Full today: activity + heart + workouts
health_yesterday - Full yesterday summary
health_week - This week aggregated summary
health_steps - Steps with daily breakdown (7d/14d/30d)
health_workouts - Recent workout list (7d/14d/30d)
health_trends - Key metrics this week vs last week
health_help - List available commands
```

This step only affects the autocomplete UI in Telegram. Commands work without it, but registering them provides a better user experience.

### 4. Restart the service

```bash
docker compose build health-ingest
docker compose up -d health-ingest
```

## Troubleshooting

### "No data found" responses

1. Check the service logs for diagnostic output:
   ```bash
   docker compose logs health-ingest --tail=30
   ```
   Each query logs its time range (`bot_*_query`) and result counts (`bot_*_result`).

2. Verify InfluxDB has data for the queried time range:
   ```bash
   health-query activity -f steps -r 24h -a sum
   ```

### "Unable to fetch data" responses

This means an InfluxDB query failed. Check the logs for the `bot_command_error` event:

```bash
docker compose logs health-ingest --tail=30 | grep bot_command_error
```

Common causes: InfluxDB unreachable, invalid token, or network issues.

### Commands not appearing in Telegram autocomplete

Register them with BotFather (see step 3 above). The autocomplete menu is managed by Telegram, not the application.

### Webhook returns 401

Verify `BOT_WEBHOOK_TOKEN` matches between your `.env` and the OpenClaw webhook configuration.

### Webhook returns 503

The bot is disabled. Set `BOT_ENABLED=true` in `.env` and restart the service.
