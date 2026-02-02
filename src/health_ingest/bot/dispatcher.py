"""Bot command dispatcher — routes commands to queries/formatters and delivers responses."""

import asyncio
from datetime import UTC, datetime

import httpx
import structlog

from ..config import (
    AnthropicSettings,
    BotSettings,
    GrokSettings,
    InfluxDBSettings,
    InsightSettings,
    OpenAISettings,
    OpenClawSettings,
)
from . import commands as cmd
from . import formatter as fmt
from .insights import BotInsightEngine
from .queries import BotQueryService

logger = structlog.get_logger(__name__)


class BotDispatcher:
    """Dispatches bot commands: parse → query → format → deliver."""

    def __init__(
        self,
        bot_settings: BotSettings,
        influxdb_settings: InfluxDBSettings,
        openclaw_settings: OpenClawSettings,
        anthropic_settings: AnthropicSettings | None = None,
        openai_settings: OpenAISettings | None = None,
        grok_settings: GrokSettings | None = None,
        insight_settings: InsightSettings | None = None,
    ) -> None:
        self._bot_settings = bot_settings
        self._openclaw_settings = openclaw_settings
        self._query_service = BotQueryService(influxdb_settings)
        self._insight_engine: BotInsightEngine | None = None
        if bot_settings.insights_enabled and insight_settings:
            self._insight_engine = BotInsightEngine(
                anthropic_settings=anthropic_settings or AnthropicSettings(),
                openai_settings=openai_settings or OpenAISettings(),
                grok_settings=grok_settings or GrokSettings(),
                insight_settings=insight_settings,
            )

    async def handle_webhook(self, message_text: str, user_id: int) -> dict:
        """Handle an incoming webhook message.

        Parses the command, executes the query, formats the response,
        and delivers it via OpenClaw. Returns a status dict.
        """
        logger.info("bot_webhook_received", message=message_text, user_id=user_id)

        response_text = await self.process_command(message_text)
        await self._send_response(response_text, user_id)

        parsed = cmd.parse_command(message_text)
        if isinstance(parsed, cmd.ParseError):
            return {"status": "error", "message": parsed.message}
        return {"status": "ok", "command": parsed.command.value}

    async def process_command(self, message_text: str) -> str:
        """Process a command and return formatted response text.

        Parses the command, executes the query, and formats the result.
        Always returns a string (error messages on failure).
        """
        parsed = cmd.parse_command(message_text)

        if isinstance(parsed, cmd.ParseError):
            return fmt.format_error(parsed.message)

        try:
            return await asyncio.wait_for(
                self._execute_command(parsed),
                timeout=self._bot_settings.response_timeout_seconds,
            )
        except TimeoutError:
            logger.warning("bot_command_timeout", command=parsed.command.value)
            return fmt.format_error("Query timed out. Please try again.")
        except Exception as e:
            logger.error("bot_command_error", command=parsed.command.value, error=str(e))
            return fmt.format_error("Unable to fetch data. Please try again later.")

    async def _execute_command(self, parsed: cmd.ParsedCommand) -> str:
        """Execute a parsed command and return formatted response text."""
        match parsed.command:
            case cmd.BotCommand.NOW:
                data = await self._query_service.fetch_snapshot()
                if not data.steps and data.resting_hr is None and data.weight_kg is None:
                    return fmt.format_no_data("snapshot")
                formatted = fmt.format_snapshot(data)

            case cmd.BotCommand.HEART:
                data = await self._query_service.fetch_heart()
                if data.resting_hr is None and data.hrv_ms is None:
                    return fmt.format_no_data("heart")
                formatted = fmt.format_heart(data)

            case cmd.BotCommand.SLEEP:
                data = await self._query_service.fetch_sleep()
                if data.duration_min is None:
                    return fmt.format_no_data("sleep")
                formatted = fmt.format_sleep(data)

            case cmd.BotCommand.WEIGHT:
                data = await self._query_service.fetch_weight()
                if data.latest_kg is None:
                    return fmt.format_no_data("weight")
                formatted = fmt.format_weight(data)

            case cmd.BotCommand.TODAY:
                data = await self._query_service.fetch_day_summary(day_offset=0)
                formatted = fmt.format_day_summary(data, "Today")

            case cmd.BotCommand.YESTERDAY:
                data = await self._query_service.fetch_day_summary(day_offset=-1)
                formatted = fmt.format_day_summary(data, "Yesterday")

            case cmd.BotCommand.WEEK:
                data = await self._query_service.fetch_week_summary()
                formatted = fmt.format_day_summary(data, "This Week (7 days)")

            case cmd.BotCommand.STEPS:
                data = await self._query_service.fetch_steps(parsed.period)
                if not data.total and not data.daily:
                    return fmt.format_no_data("steps")
                formatted = fmt.format_steps(data, parsed.period)

            case cmd.BotCommand.WORKOUTS:
                entries = await self._query_service.fetch_workouts(parsed.period)
                formatted = fmt.format_workouts(entries, parsed.period)

            case cmd.BotCommand.TRENDS:
                data = await self._query_service.fetch_trends()
                formatted = fmt.format_trends(data)

            case cmd.BotCommand.HELP:
                return fmt.format_help()

        if self._insight_engine and parsed.command != cmd.BotCommand.HELP:
            try:
                insights = await asyncio.wait_for(
                    self._insight_engine.generate(formatted, parsed.command.value),
                    timeout=self._bot_settings.insight_timeout_seconds,
                )
                if insights:
                    formatted = fmt.append_insights(formatted, insights)
            except TimeoutError:
                logger.warning("bot_insight_timeout", command=parsed.command.value)
            except Exception as e:
                logger.warning("bot_insight_error", command=parsed.command.value, error=str(e))

        return formatted

    async def _send_response(self, text: str, user_id: int) -> None:
        """Send response text to user via OpenClaw."""
        if not self._openclaw_settings.hooks_token:
            logger.warning("bot_no_openclaw_token")
            return

        payload = {
            "message": text,
            "channel": "telegram",
            "to": str(user_id),
            "deliver": True,
            "name": "Health Bot",
            "sessionKey": f"health-bot:{user_id}:{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}",
        }

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self._openclaw_settings.gateway_url}/hooks/agent",
                    json=payload,
                    headers={
                        "Authorization": f"Bearer {self._openclaw_settings.hooks_token}",
                        "Content-Type": "application/json",
                    },
                    timeout=30.0,
                )
                if response.status_code in (200, 202):
                    logger.info("bot_response_delivered", user_id=user_id)
                else:
                    logger.warning(
                        "bot_response_delivery_failed",
                        status=response.status_code,
                        user_id=user_id,
                    )
        except Exception as e:
            logger.error("bot_response_delivery_error", error=str(e), user_id=user_id)
