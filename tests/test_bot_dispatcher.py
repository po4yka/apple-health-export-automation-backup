"""Tests for bot dispatcher."""

import asyncio
from unittest.mock import AsyncMock, patch

from health_ingest.bot.dispatcher import BotDispatcher
from health_ingest.bot.queries import SnapshotData
from health_ingest.config import BotSettings, InfluxDBSettings, OpenClawSettings


def _make_dispatcher(
    bot_enabled: bool = True,
    webhook_token: str = "test-bot-token",
    response_timeout: float = 5.0,
) -> BotDispatcher:
    bot_settings = BotSettings(
        _env_file=None,
        enabled=bot_enabled,
        webhook_token=webhook_token,
        response_timeout_seconds=response_timeout,
    )
    influxdb_settings = InfluxDBSettings(
        _env_file=None,
        token="test-token",
        url="http://localhost:8086",
    )
    openclaw_settings = OpenClawSettings(
        _env_file=None,
        enabled=True,
        hooks_token="test-hooks-token",
        telegram_user_id=123456,
    )
    return BotDispatcher(
        bot_settings=bot_settings,
        influxdb_settings=influxdb_settings,
        openclaw_settings=openclaw_settings,
    )


class TestBotDispatcher:
    async def test_dispatch_now_command(self):
        dispatcher = _make_dispatcher()
        mock_data = SnapshotData(steps=8000, active_calories=300, exercise_min=30)

        with (
            patch.object(
                dispatcher._query_service,
                "fetch_snapshot",
                new_callable=AsyncMock,
            ) as mock_fetch,
            patch.object(dispatcher, "_send_response", new_callable=AsyncMock) as mock_send,
        ):
            mock_fetch.return_value = mock_data
            result = await dispatcher.handle_webhook("/health_now", 123456)

        assert result["status"] == "ok"
        assert result["command"] == "health_now"
        mock_fetch.assert_awaited_once()
        mock_send.assert_awaited_once()
        sent_text = mock_send.call_args[0][0]
        assert "8,000" in sent_text

    async def test_dispatch_help_command(self):
        dispatcher = _make_dispatcher()

        with patch.object(dispatcher, "_send_response", new_callable=AsyncMock) as mock_send:
            result = await dispatcher.handle_webhook("/health_help", 123456)

        assert result["status"] == "ok"
        assert result["command"] == "health_help"
        mock_send.assert_awaited_once()
        sent_text = mock_send.call_args[0][0]
        assert "/health_now" in sent_text

    async def test_dispatch_unknown_command(self):
        dispatcher = _make_dispatcher()

        with patch.object(dispatcher, "_send_response", new_callable=AsyncMock) as mock_send:
            result = await dispatcher.handle_webhook("/foobar", 123456)

        assert result["status"] == "error"
        mock_send.assert_awaited_once()
        sent_text = mock_send.call_args[0][0]
        assert "Unknown command" in sent_text

    async def test_dispatch_invalid_message(self):
        dispatcher = _make_dispatcher()

        with patch.object(dispatcher, "_send_response", new_callable=AsyncMock) as mock_send:
            result = await dispatcher.handle_webhook("no slash", 123456)

        assert result["status"] == "error"
        mock_send.assert_awaited_once()

    async def test_dispatch_timeout(self):
        dispatcher = _make_dispatcher(response_timeout=0.01)

        async def _slow_fetch():
            await asyncio.sleep(1.0)
            return SnapshotData()

        with (
            patch.object(
                dispatcher._query_service,
                "fetch_snapshot",
                side_effect=_slow_fetch,
            ),
            patch.object(dispatcher, "_send_response", new_callable=AsyncMock) as mock_send,
        ):
            await dispatcher.handle_webhook("/health_now", 123456)

        mock_send.assert_awaited_once()
        sent_text = mock_send.call_args[0][0]
        assert "timed out" in sent_text

    async def test_dispatch_query_error(self):
        dispatcher = _make_dispatcher()

        with (
            patch.object(
                dispatcher._query_service,
                "fetch_snapshot",
                new_callable=AsyncMock,
                side_effect=RuntimeError("connection refused"),
            ),
            patch.object(dispatcher, "_send_response", new_callable=AsyncMock) as mock_send,
        ):
            await dispatcher.handle_webhook("/health_now", 123456)

        mock_send.assert_awaited_once()
        sent_text = mock_send.call_args[0][0]
        assert "Unable to fetch" in sent_text
