"""Tests for HTTP authentication edge cases."""

from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient

from health_ingest.config import HTTPSettings
from health_ingest.http_handler import HTTPHandler


def _make_settings(
    auth_token: str = "test-token",
    allow_unauthenticated: bool = False,
    max_request_size: int = 10_485_760,
) -> HTTPSettings:
    """Create HTTPSettings isolated from env vars."""
    return HTTPSettings(
        _env_file=None,
        enabled=True,
        host="127.0.0.1",
        port=8080,
        auth_token=auth_token,
        allow_unauthenticated=allow_unauthenticated,
        max_request_size=max_request_size,
    )


def _make_handler(
    auth_token: str = "test-token",
    allow_unauthenticated: bool = False,
    message_callback: AsyncMock | None = None,
) -> HTTPHandler:
    """Create an HTTPHandler with test settings."""
    return HTTPHandler(
        settings=_make_settings(
            auth_token=auth_token,
            allow_unauthenticated=allow_unauthenticated,
        ),
        message_callback=message_callback or AsyncMock(),
    )


async def _client_for(handler: HTTPHandler) -> AsyncClient:
    transport = ASGITransport(app=handler.app)
    return AsyncClient(transport=transport, base_url="http://test")


VALID_PAYLOAD = {"data": [{"name": "heart_rate", "date": "2026-01-30T12:00:00Z", "qty": 72}]}


class TestHTTPAuthEdgeCases:
    """Tests for HTTP authentication edge cases on /ingest."""

    async def test_missing_authorization_header_returns_401(self):
        """POST /ingest without any Authorization header returns 401."""
        handler = _make_handler()
        async with await _client_for(handler) as client:
            resp = await client.post("/ingest", json=VALID_PAYLOAD)

        assert resp.status_code == 401
        assert resp.json()["error"] == "Unauthorized"

    async def test_malformed_bearer_no_prefix_returns_401(self):
        """Authorization header without 'Bearer ' prefix returns 401."""
        handler = _make_handler()
        async with await _client_for(handler) as client:
            resp = await client.post(
                "/ingest",
                json=VALID_PAYLOAD,
                headers={"Authorization": "test-token"},
            )

        assert resp.status_code == 401
        assert resp.json()["error"] == "Unauthorized"

    async def test_malformed_bearer_basic_scheme_returns_401(self):
        """Authorization header with 'Basic' scheme instead of 'Bearer' returns 401."""
        handler = _make_handler()
        async with await _client_for(handler) as client:
            resp = await client.post(
                "/ingest",
                json=VALID_PAYLOAD,
                headers={"Authorization": "Basic test-token"},
            )

        assert resp.status_code == 401
        assert resp.json()["error"] == "Unauthorized"

    async def test_empty_token_in_header_returns_401(self):
        """Authorization header with 'Bearer ' but empty token returns 401."""
        handler = _make_handler()
        async with await _client_for(handler) as client:
            resp = await client.post(
                "/ingest",
                json=VALID_PAYLOAD,
                headers={"Authorization": "Bearer "},
            )

        assert resp.status_code == 401
        assert resp.json()["error"] == "Unauthorized"

    async def test_correct_token_returns_202(self):
        """POST /ingest with correct bearer token returns 202 Accepted."""
        callback = AsyncMock()
        handler = _make_handler(message_callback=callback)
        async with await _client_for(handler) as client:
            resp = await client.post(
                "/ingest",
                json=VALID_PAYLOAD,
                headers={"Authorization": "Bearer test-token"},
            )

        assert resp.status_code == 202
        assert resp.json()["status"] == "accepted"
        callback.assert_awaited_once()

    async def test_wrong_token_returns_401(self):
        """POST /ingest with incorrect bearer token returns 401."""
        handler = _make_handler()
        async with await _client_for(handler) as client:
            resp = await client.post(
                "/ingest",
                json=VALID_PAYLOAD,
                headers={"Authorization": "Bearer wrong-token"},
            )

        assert resp.status_code == 401
        assert resp.json()["error"] == "Unauthorized"

    async def test_token_with_trailing_whitespace_returns_401(self):
        """Token with trailing whitespace does not match (hmac.compare_digest is exact)."""
        handler = _make_handler()
        async with await _client_for(handler) as client:
            resp = await client.post(
                "/ingest",
                json=VALID_PAYLOAD,
                headers={"Authorization": "Bearer test-token "},
            )

        assert resp.status_code == 401

    async def test_token_with_leading_whitespace_returns_401(self):
        """Token with extra leading whitespace after 'Bearer ' does not match."""
        handler = _make_handler()
        async with await _client_for(handler) as client:
            resp = await client.post(
                "/ingest",
                json=VALID_PAYLOAD,
                headers={"Authorization": "Bearer  test-token"},
            )

        # The handler slices at index 7, so " test-token" != "test-token"
        assert resp.status_code == 401

    async def test_no_auth_token_configured_allows_all(self):
        """When auth_token is empty in settings, all requests are allowed."""
        callback = AsyncMock()
        handler = _make_handler(
            auth_token="",
            allow_unauthenticated=True,
            message_callback=callback,
        )
        async with await _client_for(handler) as client:
            resp = await client.post("/ingest", json=VALID_PAYLOAD)

        assert resp.status_code == 202
        callback.assert_awaited_once()

    async def test_no_auth_token_configured_allows_without_header(self):
        """When auth_token is empty, requests with no header are still accepted."""
        callback = AsyncMock()
        handler = _make_handler(
            auth_token="",
            allow_unauthenticated=True,
            message_callback=callback,
        )
        async with await _client_for(handler) as client:
            resp = await client.post(
                "/ingest",
                json=VALID_PAYLOAD,
                # No Authorization header at all
            )

        assert resp.status_code == 202

    async def test_empty_token_without_allow_flag_denies_requests(self):
        """When auth_token is empty without opt-in, settings validation fails."""
        with pytest.raises(ValueError, match="HTTP_AUTH_TOKEN is required"):
            _make_handler(auth_token="", allow_unauthenticated=False)

    async def test_case_sensitive_bearer_prefix(self):
        """'bearer' (lowercase) prefix is not accepted; must be 'Bearer'."""
        handler = _make_handler()
        async with await _client_for(handler) as client:
            resp = await client.post(
                "/ingest",
                json=VALID_PAYLOAD,
                headers={"Authorization": "bearer test-token"},
            )

        assert resp.status_code == 401
