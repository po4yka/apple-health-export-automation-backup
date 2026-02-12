"""Tests for bot webhook HTTP endpoint."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

from httpx import ASGITransport, AsyncClient

from health_ingest.config import HTTPSettings
from health_ingest.http_handler import HTTPHandler


def _make_handler(
    auth_token: str = "test-token",
    bot_dispatcher: object | None = None,
    bot_webhook_token: str = "test-bot-token",
    bot_allow_unauthenticated: bool = False,
) -> HTTPHandler:
    settings = HTTPSettings(
        _env_file=None,
        enabled=True,
        host="127.0.0.1",
        port=8080,
        auth_token=auth_token,
    )
    return HTTPHandler(
        settings=settings,
        message_callback=AsyncMock(),
        bot_dispatcher=bot_dispatcher,
        bot_webhook_token=bot_webhook_token,
        bot_allow_unauthenticated=bot_allow_unauthenticated,
    )


async def _client_for(handler: HTTPHandler) -> AsyncClient:
    transport = ASGITransport(app=handler.app)
    return AsyncClient(transport=transport, base_url="http://test")


class TestBotWebhookEndpoint:
    """Tests for POST /bot/webhook endpoint."""

    async def test_valid_request_returns_202(self):
        mock_dispatcher = MagicMock()
        mock_dispatcher.handle_webhook = AsyncMock(return_value={"status": "ok"})
        handler = _make_handler(bot_dispatcher=mock_dispatcher)

        async with await _client_for(handler) as client:
            resp = await client.post(
                "/bot/webhook",
                json={"message": "/health_now", "user_id": 123456},
                headers={"Authorization": "Bearer test-bot-token"},
            )

        assert resp.status_code == 202
        body = resp.json()
        assert body["status"] == "accepted"
        # Give the background task a chance to run
        await asyncio.sleep(0.05)

    async def test_missing_auth_returns_401(self):
        mock_dispatcher = MagicMock()
        handler = _make_handler(bot_dispatcher=mock_dispatcher)

        async with await _client_for(handler) as client:
            resp = await client.post(
                "/bot/webhook",
                json={"message": "/health_now", "user_id": 123456},
            )

        assert resp.status_code == 401

    async def test_wrong_token_returns_401(self):
        mock_dispatcher = MagicMock()
        handler = _make_handler(bot_dispatcher=mock_dispatcher)

        async with await _client_for(handler) as client:
            resp = await client.post(
                "/bot/webhook",
                json={"message": "/health_now", "user_id": 123456},
                headers={"Authorization": "Bearer wrong-token"},
            )

        assert resp.status_code == 401

    async def test_bot_disabled_returns_503(self):
        handler = _make_handler(bot_dispatcher=None)

        async with await _client_for(handler) as client:
            resp = await client.post(
                "/bot/webhook",
                json={"message": "/health_now", "user_id": 123456},
                headers={"Authorization": "Bearer test-bot-token"},
            )

        assert resp.status_code == 503
        body = resp.json()
        assert body["error"] == "Bot unavailable"

    async def test_no_auth_configured_allows_all(self):
        mock_dispatcher = MagicMock()
        mock_dispatcher.handle_webhook = AsyncMock(return_value={"status": "ok"})
        handler = _make_handler(
            bot_dispatcher=mock_dispatcher,
            bot_webhook_token="",
            bot_allow_unauthenticated=True,
        )

        async with await _client_for(handler) as client:
            resp = await client.post(
                "/bot/webhook",
                json={"message": "/health_help", "user_id": 999},
            )

        assert resp.status_code == 202
        await asyncio.sleep(0.05)

    async def test_no_webhook_token_without_allow_flag_returns_401(self):
        mock_dispatcher = MagicMock()
        mock_dispatcher.handle_webhook = AsyncMock(return_value={"status": "ok"})
        handler = _make_handler(bot_dispatcher=mock_dispatcher, bot_webhook_token="")

        async with await _client_for(handler) as client:
            resp = await client.post(
                "/bot/webhook",
                json={"message": "/health_help", "user_id": 999},
            )

        assert resp.status_code == 401

    async def test_missing_required_fields_returns_422(self):
        mock_dispatcher = MagicMock()
        handler = _make_handler(bot_dispatcher=mock_dispatcher)

        async with await _client_for(handler) as client:
            resp = await client.post(
                "/bot/webhook",
                json={"message": "/health_now"},  # missing user_id
                headers={"Authorization": "Bearer test-bot-token"},
            )

        assert resp.status_code == 422

    async def test_fire_and_forget_behavior(self):
        """Webhook returns 202 immediately, not waiting for handle_webhook to complete."""
        mock_dispatcher = MagicMock()

        async def slow_handler(msg, uid):
            await asyncio.sleep(10)
            return {"status": "ok"}

        mock_dispatcher.handle_webhook = slow_handler
        handler = _make_handler(bot_dispatcher=mock_dispatcher)

        async with await _client_for(handler) as client:
            resp = await client.post(
                "/bot/webhook",
                json={"message": "/health_now", "user_id": 123456},
                headers={"Authorization": "Bearer test-bot-token"},
            )

        # Should return immediately, not wait for slow_handler
        assert resp.status_code == 202


class TestBotCommandEndpoint:
    """Tests for POST /bot/command synchronous endpoint."""

    async def test_valid_command_returns_200_with_text(self):
        mock_dispatcher = MagicMock()
        mock_dispatcher.process_command = AsyncMock(return_value="Steps: 8,000")
        handler = _make_handler(bot_dispatcher=mock_dispatcher)

        async with await _client_for(handler) as client:
            resp = await client.post(
                "/bot/command",
                json={"message": "/health_now"},
                headers={"Authorization": "Bearer test-bot-token"},
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["text"] == "Steps: 8,000"
        mock_dispatcher.process_command.assert_awaited_once_with("/health_now")

    async def test_missing_auth_returns_401(self):
        mock_dispatcher = MagicMock()
        handler = _make_handler(bot_dispatcher=mock_dispatcher)

        async with await _client_for(handler) as client:
            resp = await client.post(
                "/bot/command",
                json={"message": "/health_now"},
            )

        assert resp.status_code == 401

    async def test_wrong_token_returns_401(self):
        mock_dispatcher = MagicMock()
        handler = _make_handler(bot_dispatcher=mock_dispatcher)

        async with await _client_for(handler) as client:
            resp = await client.post(
                "/bot/command",
                json={"message": "/health_now"},
                headers={"Authorization": "Bearer wrong-token"},
            )

        assert resp.status_code == 401

    async def test_bot_disabled_returns_503(self):
        handler = _make_handler(bot_dispatcher=None)

        async with await _client_for(handler) as client:
            resp = await client.post(
                "/bot/command",
                json={"message": "/health_now"},
                headers={"Authorization": "Bearer test-bot-token"},
            )

        assert resp.status_code == 503
        body = resp.json()
        assert body["error"] == "Bot unavailable"

    async def test_no_auth_configured_allows_all(self):
        mock_dispatcher = MagicMock()
        mock_dispatcher.process_command = AsyncMock(return_value="OK")
        handler = _make_handler(
            bot_dispatcher=mock_dispatcher,
            bot_webhook_token="",
            bot_allow_unauthenticated=True,
        )

        async with await _client_for(handler) as client:
            resp = await client.post(
                "/bot/command",
                json={"message": "/health_help"},
            )

        assert resp.status_code == 200
        assert resp.json()["text"] == "OK"

    async def test_no_bot_token_without_allow_flag_returns_401(self):
        mock_dispatcher = MagicMock()
        mock_dispatcher.process_command = AsyncMock(return_value="OK")
        handler = _make_handler(bot_dispatcher=mock_dispatcher, bot_webhook_token="")

        async with await _client_for(handler) as client:
            resp = await client.post(
                "/bot/command",
                json={"message": "/health_help"},
            )

        assert resp.status_code == 401

    async def test_dispatcher_exception_returns_500(self):
        mock_dispatcher = MagicMock()
        mock_dispatcher.process_command = AsyncMock(side_effect=RuntimeError("boom"))
        handler = _make_handler(bot_dispatcher=mock_dispatcher)

        async with await _client_for(handler) as client:
            resp = await client.post(
                "/bot/command",
                json={"message": "/health_now"},
                headers={"Authorization": "Bearer test-bot-token"},
            )

        assert resp.status_code == 500
        body = resp.json()
        assert body["error"] == "Command execution failed"

    async def test_missing_message_field_returns_422(self):
        mock_dispatcher = MagicMock()
        handler = _make_handler(bot_dispatcher=mock_dispatcher)

        async with await _client_for(handler) as client:
            resp = await client.post(
                "/bot/command",
                json={},
                headers={"Authorization": "Bearer test-bot-token"},
            )

        assert resp.status_code == 422
