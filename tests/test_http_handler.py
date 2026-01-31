"""Tests for HTTP handler."""

import asyncio
from collections.abc import Callable
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

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
    status_provider: Callable[[], dict[str, object]] | None = None,
    report_callback: AsyncMock | None = None,
) -> HTTPHandler:
    """Create an HTTPHandler with test settings."""
    return HTTPHandler(
        settings=_make_settings(auth_token=auth_token, max_request_size=max_request_size),
        message_callback=message_callback or AsyncMock(),
        status_provider=status_provider,
        report_callback=report_callback,
    )


@pytest.fixture
def handler() -> HTTPHandler:
    return _make_handler()


async def _client_for(handler: HTTPHandler) -> AsyncClient:
    transport = ASGITransport(app=handler.app)
    return AsyncClient(transport=transport, base_url="http://test")


class TestHTTPIngestEndpoint:
    """Tests for POST /ingest endpoint."""

    @pytest.mark.asyncio
    async def test_valid_payload_returns_202(self):
        """POST /ingest with valid payload returns 202 Accepted."""
        callback = AsyncMock()
        handler = _make_handler(message_callback=callback)
        async with await _client_for(handler) as client:
            payload = {"data": [{"name": "heart_rate", "date": "2026-01-30T12:00:00Z", "qty": 72}]}
            resp = await client.post(
                "/ingest",
                json=payload,
                headers={"Authorization": "Bearer test-token"},
            )

        assert resp.status_code == 202
        body = resp.json()
        assert body["status"] == "accepted"
        callback.assert_awaited_once()
        call_args = callback.call_args
        assert call_args[0][0] == "http/ingest"
        assert call_args[0][1] == payload
        assert isinstance(call_args[0][3], dict)

    @pytest.mark.asyncio
    async def test_missing_auth_returns_401(self):
        """POST /ingest without Authorization header returns 401."""
        handler = _make_handler()
        async with await _client_for(handler) as client:
            resp = await client.post("/ingest", json={"data": []})

        assert resp.status_code == 401
        body = resp.json()
        assert body["error"] == "Unauthorized"

    @pytest.mark.asyncio
    async def test_wrong_token_returns_401(self):
        """POST /ingest with wrong bearer token returns 401."""
        handler = _make_handler()
        async with await _client_for(handler) as client:
            resp = await client.post(
                "/ingest",
                json={"data": []},
                headers={"Authorization": "Bearer wrong-token"},
            )

        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_no_auth_token_configured_allows_all(self):
        """POST /ingest with empty auth_token config allows all requests."""
        callback = AsyncMock()
        handler = _make_handler(auth_token="", message_callback=callback)
        async with await _client_for(handler) as client:
            resp = await client.post("/ingest", json={"data": []})

        assert resp.status_code == 202
        callback.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_invalid_json_returns_400(self):
        """POST /ingest with invalid JSON returns 400."""
        handler = _make_handler()
        async with await _client_for(handler) as client:
            resp = await client.post(
                "/ingest",
                content=b"not valid json {",
                headers={
                    "Authorization": "Bearer test-token",
                    "Content-Type": "application/json",
                },
            )

        assert resp.status_code == 400
        body = resp.json()
        assert body["error"] == "Invalid JSON"

    @pytest.mark.asyncio
    async def test_oversized_payload_returns_413(self):
        """POST /ingest with oversized payload returns 413."""
        handler = _make_handler(max_request_size=1024)
        async with await _client_for(handler) as client:
            large_payload = b"x" * 2048
            resp = await client.post(
                "/ingest",
                content=large_payload,
                headers={
                    "Authorization": "Bearer test-token",
                    "Content-Type": "application/json",
                },
            )

        assert resp.status_code == 413

    @pytest.mark.asyncio
    async def test_callback_error_returns_500(self):
        """POST /ingest returns 500 when message callback raises."""
        callback = AsyncMock(side_effect=RuntimeError("queue full"))
        handler = _make_handler(message_callback=callback)
        async with await _client_for(handler) as client:
            resp = await client.post(
                "/ingest",
                json={"data": []},
                headers={"Authorization": "Bearer test-token"},
            )

        assert resp.status_code == 500
        body = resp.json()
        assert body["error"] == "Internal server error"

    @pytest.mark.asyncio
    async def test_queue_full_returns_429(self):
        """POST /ingest returns 429 when queue is full."""
        callback = AsyncMock(side_effect=asyncio.QueueFull())
        handler = _make_handler(message_callback=callback)
        async with await _client_for(handler) as client:
            resp = await client.post(
                "/ingest",
                json={"data": []},
                headers={"Authorization": "Bearer test-token"},
            )

        assert resp.status_code == 429
        body = resp.json()
        assert body["error"] == "Service overloaded, try again later"

    @pytest.mark.asyncio
    async def test_queue_not_ready_returns_503(self):
        """POST /ingest returns 503 when queue is not ready."""
        callback = AsyncMock(side_effect=RuntimeError("message_queue_not_ready"))
        handler = _make_handler(message_callback=callback)
        async with await _client_for(handler) as client:
            resp = await client.post(
                "/ingest",
                json={"data": []},
                headers={"Authorization": "Bearer test-token"},
            )

        assert resp.status_code == 503
        body = resp.json()
        assert body["error"] == "Service not ready"


class TestHTTPHealthEndpoint:
    """Tests for GET /health endpoint."""

    @pytest.mark.asyncio
    async def test_health_returns_200(self):
        """GET /health returns 200 with status ok."""
        handler = _make_handler()
        async with await _client_for(handler) as client:
            resp = await client.get("/health")

        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"


class TestHTTPStatusEndpoints:
    """Tests for readiness, info, and metrics endpoints."""

    @pytest.mark.asyncio
    async def test_ready_returns_ok(self):
        handler = _make_handler()
        async with await _client_for(handler) as client:
            resp = await client.get("/ready")

        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"

    @pytest.mark.asyncio
    async def test_ready_returns_503_when_not_ready(self):
        handler = _make_handler(status_provider=lambda: {"status": "degraded"})
        async with await _client_for(handler) as client:
            resp = await client.get("/ready")

        assert resp.status_code == 503

    @pytest.mark.asyncio
    async def test_info_returns_version(self):
        handler = _make_handler()
        async with await _client_for(handler) as client:
            resp = await client.get("/info")

        assert resp.status_code == 200
        body = resp.json()
        assert body["name"] == "health-ingest"
        assert "version" in body

    @pytest.mark.asyncio
    async def test_metrics_returns_200(self):
        handler = _make_handler()
        async with await _client_for(handler) as client:
            resp = await client.get("/metrics")

        assert resp.status_code == 200


class TestHTTPReportEndpoint:
    """Tests for weekly report endpoint."""

    @pytest.mark.asyncio
    async def test_report_requires_auth(self):
        handler = _make_handler()
        async with await _client_for(handler) as client:
            resp = await client.post("/reports/weekly", json={})

        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_report_returns_503_without_callback(self):
        handler = _make_handler()
        async with await _client_for(handler) as client:
            resp = await client.post(
                "/reports/weekly",
                json={},
                headers={"Authorization": "Bearer test-token"},
            )

        assert resp.status_code == 503

    @pytest.mark.asyncio
    async def test_report_returns_generated(self):
        callback = AsyncMock(return_value="report-body")
        handler = _make_handler(report_callback=callback)
        async with await _client_for(handler) as client:
            resp = await client.post(
                "/reports/weekly",
                json={},
                headers={"Authorization": "Bearer test-token"},
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "generated"
        assert body["report"] == "report-body"


class TestHTTPHandlerLifecycle:
    """Tests for HTTPHandler start/stop lifecycle."""

    @pytest.mark.asyncio
    async def test_start_and_stop(self, monkeypatch):
        """HTTPHandler starts and stops cleanly."""
        handler = _make_handler()
        server_mock = MagicMock()
        server_mock.serve = AsyncMock()

        def _server_factory(*_args, **_kwargs):
            return server_mock

        monkeypatch.setattr("health_ingest.http_handler.uvicorn.Server", _server_factory)

        await handler.start()
        assert handler._server is server_mock
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
        assert settings.enabled is True
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
