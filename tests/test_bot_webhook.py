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
                json={"message": "/now", "user_id": 123456},
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
                json={"message": "/now", "user_id": 123456},
            )

        assert resp.status_code == 401

    async def test_wrong_token_returns_401(self):
        mock_dispatcher = MagicMock()
        handler = _make_handler(bot_dispatcher=mock_dispatcher)

        async with await _client_for(handler) as client:
            resp = await client.post(
                "/bot/webhook",
                json={"message": "/now", "user_id": 123456},
                headers={"Authorization": "Bearer wrong-token"},
            )

        assert resp.status_code == 401

    async def test_bot_disabled_returns_503(self):
        handler = _make_handler(bot_dispatcher=None)

        async with await _client_for(handler) as client:
            resp = await client.post(
                "/bot/webhook",
                json={"message": "/now", "user_id": 123456},
                headers={"Authorization": "Bearer test-bot-token"},
            )

        assert resp.status_code == 503
        body = resp.json()
        assert body["error"] == "Bot unavailable"

    async def test_no_auth_configured_allows_all(self):
        mock_dispatcher = MagicMock()
        mock_dispatcher.handle_webhook = AsyncMock(return_value={"status": "ok"})
        handler = _make_handler(bot_dispatcher=mock_dispatcher, bot_webhook_token="")

        async with await _client_for(handler) as client:
            resp = await client.post(
                "/bot/webhook",
                json={"message": "/help", "user_id": 999},
            )

        assert resp.status_code == 202
        await asyncio.sleep(0.05)

    async def test_missing_required_fields_returns_422(self):
        mock_dispatcher = MagicMock()
        handler = _make_handler(bot_dispatcher=mock_dispatcher)

        async with await _client_for(handler) as client:
            resp = await client.post(
                "/bot/webhook",
                json={"message": "/now"},  # missing user_id
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
                json={"message": "/now", "user_id": 123456},
                headers={"Authorization": "Bearer test-bot-token"},
            )

        # Should return immediately, not wait for slow_handler
        assert resp.status_code == 202
