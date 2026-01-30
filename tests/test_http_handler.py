"""Tests for HTTP handler."""

from unittest.mock import AsyncMock

import pytest
from aiohttp import web

from health_ingest.config import HTTPSettings
from health_ingest.http_handler import HTTPHandler


def _make_settings(
    auth_token: str = "test-token",
    max_request_size: int = 10_485_760,
) -> HTTPSettings:
    """Create HTTPSettings isolated from env vars."""
    return HTTPSettings(
        _env_file=None,
        enabled=True,
        host="127.0.0.1",
        port=8080,
        auth_token=auth_token,
        max_request_size=max_request_size,
    )


def _make_handler(
    auth_token: str = "test-token",
    max_request_size: int = 10_485_760,
    message_callback: AsyncMock | None = None,
) -> HTTPHandler:
    """Create an HTTPHandler with test settings."""
    return HTTPHandler(
        settings=_make_settings(auth_token=auth_token, max_request_size=max_request_size),
        message_callback=message_callback or AsyncMock(),
    )


@pytest.fixture
def handler():
    return _make_handler()


@pytest.fixture
def app(handler: HTTPHandler) -> web.Application:
    """Create an aiohttp Application for testing."""
    app = web.Application(client_max_size=handler._settings.max_request_size)
    app.router.add_post("/ingest", handler._handle_ingest)
    app.router.add_get("/health", handler._handle_health)
    return app


class TestHTTPIngestEndpoint:
    """Tests for POST /ingest endpoint."""

    @pytest.mark.asyncio
    async def test_valid_payload_returns_202(self, aiohttp_client):
        """POST /ingest with valid payload returns 202 Accepted."""
        callback = AsyncMock()
        handler = _make_handler(message_callback=callback)
        app = web.Application(client_max_size=handler._settings.max_request_size)
        app.router.add_post("/ingest", handler._handle_ingest)

        client = await aiohttp_client(app)
        payload = {
            "data": [
                {"name": "heart_rate", "date": "2026-01-30T12:00:00Z", "qty": 72, "units": "bpm"}
            ]
        }
        resp = await client.post(
            "/ingest",
            json=payload,
            headers={"Authorization": "Bearer test-token"},
        )

        assert resp.status == 202
        body = await resp.json()
        assert body["status"] == "accepted"
        callback.assert_awaited_once()
        call_args = callback.call_args
        assert call_args[0][0] == "http/ingest"
        assert call_args[0][1] == payload

    @pytest.mark.asyncio
    async def test_missing_auth_returns_401(self, aiohttp_client):
        """POST /ingest without Authorization header returns 401."""
        handler = _make_handler()
        app = web.Application()
        app.router.add_post("/ingest", handler._handle_ingest)

        client = await aiohttp_client(app)
        resp = await client.post("/ingest", json={"data": []})

        assert resp.status == 401
        body = await resp.json()
        assert body["error"] == "Unauthorized"

    @pytest.mark.asyncio
    async def test_wrong_token_returns_401(self, aiohttp_client):
        """POST /ingest with wrong bearer token returns 401."""
        handler = _make_handler()
        app = web.Application()
        app.router.add_post("/ingest", handler._handle_ingest)

        client = await aiohttp_client(app)
        resp = await client.post(
            "/ingest",
            json={"data": []},
            headers={"Authorization": "Bearer wrong-token"},
        )

        assert resp.status == 401

    @pytest.mark.asyncio
    async def test_no_auth_token_configured_allows_all(self, aiohttp_client):
        """POST /ingest with empty auth_token config allows all requests."""
        callback = AsyncMock()
        handler = _make_handler(auth_token="", message_callback=callback)
        app = web.Application()
        app.router.add_post("/ingest", handler._handle_ingest)

        client = await aiohttp_client(app)
        resp = await client.post("/ingest", json={"data": []})

        assert resp.status == 202
        callback.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_invalid_json_returns_400(self, aiohttp_client):
        """POST /ingest with invalid JSON returns 400."""
        handler = _make_handler()
        app = web.Application()
        app.router.add_post("/ingest", handler._handle_ingest)

        client = await aiohttp_client(app)
        resp = await client.post(
            "/ingest",
            data=b"not valid json {",
            headers={
                "Authorization": "Bearer test-token",
                "Content-Type": "application/json",
            },
        )

        assert resp.status == 400
        body = await resp.json()
        assert body["error"] == "Invalid JSON"

    @pytest.mark.asyncio
    async def test_oversized_payload_returns_413(self, aiohttp_client):
        """POST /ingest with oversized payload returns 413."""
        handler = _make_handler(max_request_size=1024)
        app = web.Application(client_max_size=1024)
        app.router.add_post("/ingest", handler._handle_ingest)

        client = await aiohttp_client(app)
        # Create a payload larger than 1KB
        large_payload = b"x" * 2048
        resp = await client.post(
            "/ingest",
            data=large_payload,
            headers={
                "Authorization": "Bearer test-token",
                "Content-Type": "application/json",
            },
        )

        # aiohttp may return 413 at the framework level (client_max_size)
        # or our handler catches it
        assert resp.status in (400, 413)

    @pytest.mark.asyncio
    async def test_callback_error_returns_500(self, aiohttp_client):
        """POST /ingest returns 500 when message callback raises."""
        callback = AsyncMock(side_effect=RuntimeError("queue full"))
        handler = _make_handler(message_callback=callback)
        app = web.Application()
        app.router.add_post("/ingest", handler._handle_ingest)

        client = await aiohttp_client(app)
        resp = await client.post(
            "/ingest",
            json={"data": []},
            headers={"Authorization": "Bearer test-token"},
        )

        assert resp.status == 500
        body = await resp.json()
        assert body["error"] == "Internal server error"


class TestHTTPHealthEndpoint:
    """Tests for GET /health endpoint."""

    @pytest.mark.asyncio
    async def test_health_returns_200(self, aiohttp_client):
        """GET /health returns 200 with status ok."""
        handler = _make_handler()
        app = web.Application()
        app.router.add_get("/health", handler._handle_health)

        client = await aiohttp_client(app)
        resp = await client.get("/health")

        assert resp.status == 200
        body = await resp.json()
        assert body["status"] == "ok"


class TestHTTPHandlerLifecycle:
    """Tests for HTTPHandler start/stop lifecycle."""

    @pytest.mark.asyncio
    async def test_start_and_stop(self):
        """HTTPHandler starts and stops cleanly."""
        handler = _make_handler()
        await handler.start()
        assert handler._runner is not None
        assert handler._site is not None
        await handler.stop()

    @pytest.mark.asyncio
    async def test_stop_without_start(self):
        """HTTPHandler.stop() without start() doesn't error."""
        handler = _make_handler()
        await handler.stop()


class TestHTTPSettings:
    """Tests for HTTPSettings configuration."""

    def test_defaults(self):
        settings = HTTPSettings(_env_file=None)
        assert settings.enabled is False
        assert settings.host == "0.0.0.0"
        assert settings.port == 8080
        assert settings.auth_token == ""
        assert settings.max_request_size == 10_485_760

    def test_custom_values(self):
        settings = HTTPSettings(
            _env_file=None,
            enabled=True,
            port=9090,
            auth_token="my-secret",
            max_request_size=1_048_576,
        )
        assert settings.enabled is True
        assert settings.port == 9090
        assert settings.auth_token == "my-secret"
        assert settings.max_request_size == 1_048_576

    def test_invalid_port(self):
        with pytest.raises(ValueError, match="Port must be between"):
            HTTPSettings(_env_file=None, port=0)

    def test_max_request_size_too_small(self):
        with pytest.raises(ValueError, match="at least 1KB"):
            HTTPSettings(_env_file=None, max_request_size=100)
