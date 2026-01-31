"""HTTP handler for Health Auto Export data ingestion via REST API."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import TYPE_CHECKING, Any

import structlog
from aiohttp import web

from .config import HTTPSettings
from .metrics import HTTP_REQUESTS_TOTAL

if TYPE_CHECKING:
    from .archive import RawArchiver
    from .dlq import DeadLetterQueue

logger = structlog.get_logger(__name__)


class HTTPHandler:
    """Handles HTTP ingestion endpoint for health data.

    Provides a REST API that accepts JSON payloads from Health Auto Export,
    routing them through the same processing pipeline via the shared message callback.
    """

    def __init__(
        self,
        settings: HTTPSettings,
        message_callback: Callable[[str, dict[str, Any], str | None], Awaitable[None]],
        archiver: RawArchiver | None = None,
        dlq: DeadLetterQueue | None = None,
    ) -> None:
        self._settings = settings
        self._message_callback = message_callback
        self._archiver = archiver
        self._dlq = dlq
        self._app: web.Application | None = None
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None

    def _check_auth(self, request: web.Request) -> bool:
        """Validate Bearer token from Authorization header."""
        if not self._settings.auth_token:
            return True  # No token configured = auth disabled
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return False
        return auth_header[7:] == self._settings.auth_token

    async def _handle_ingest(self, request: web.Request) -> web.Response:
        """Handle POST /ingest -- accepts health data JSON payload."""
        # Auth check
        if not self._check_auth(request):
            HTTP_REQUESTS_TOTAL.labels(method="POST", path="/ingest", status="401").inc()
            return web.json_response({"error": "Unauthorized"}, status=401)

        # Size check
        content_length = request.content_length or 0
        if content_length > self._settings.max_request_size:
            HTTP_REQUESTS_TOTAL.labels(method="POST", path="/ingest", status="413").inc()
            return web.json_response(
                {"error": "Request body too large", "max_bytes": self._settings.max_request_size},
                status=413,
            )

        # Read and parse body
        try:
            raw_body = await request.read()
        except Exception:
            HTTP_REQUESTS_TOTAL.labels(method="POST", path="/ingest", status="400").inc()
            return web.json_response({"error": "Failed to read request body"}, status=400)

        # Double-check actual size (content_length can be missing/wrong)
        if len(raw_body) > self._settings.max_request_size:
            HTTP_REQUESTS_TOTAL.labels(method="POST", path="/ingest", status="413").inc()
            return web.json_response(
                {"error": "Request body too large", "max_bytes": self._settings.max_request_size},
                status=413,
            )

        # Archive raw payload before parsing
        archive_id: str | None = None
        if self._archiver:
            try:
                archive_id = self._archiver.store_sync(
                    topic="http/ingest",
                    payload=raw_body,
                    received_at=datetime.now(),
                )
            except Exception as e:
                logger.error("http_archive_store_failed", error=str(e))

        # Parse JSON
        try:
            payload = json.loads(raw_body)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning("http_payload_parse_error", error=str(e), archive_id=archive_id)
            if self._dlq:
                from .dlq import DLQCategory

                await self._dlq.enqueue(
                    category=DLQCategory.JSON_PARSE_ERROR,
                    topic="http/ingest",
                    payload=raw_body,
                    error=e,
                    archive_id=archive_id,
                )
            HTTP_REQUESTS_TOTAL.labels(method="POST", path="/ingest", status="400").inc()
            return web.json_response({"error": "Invalid JSON"}, status=400)

        # Enqueue for processing
        try:
            await self._message_callback("http/ingest", payload, archive_id)
        except asyncio.QueueFull:
            HTTP_REQUESTS_TOTAL.labels(method="POST", path="/ingest", status="429").inc()
            return web.json_response(
                {"error": "Service overloaded, try again later"},
                status=429,
            )
        except RuntimeError as e:
            if str(e) == "message_queue_not_ready":
                HTTP_REQUESTS_TOTAL.labels(method="POST", path="/ingest", status="503").inc()
                return web.json_response(
                    {"error": "Service not ready"},
                    status=503,
                )
            logger.error("http_enqueue_error", error=str(e), archive_id=archive_id)
            HTTP_REQUESTS_TOTAL.labels(method="POST", path="/ingest", status="500").inc()
            return web.json_response({"error": "Internal server error"}, status=500)
        except Exception as e:
            logger.error("http_enqueue_error", error=str(e), archive_id=archive_id)
            HTTP_REQUESTS_TOTAL.labels(method="POST", path="/ingest", status="500").inc()
            return web.json_response({"error": "Internal server error"}, status=500)

        logger.debug(
            "http_message_accepted",
            payload_size=len(raw_body),
            archive_id=archive_id,
        )
        HTTP_REQUESTS_TOTAL.labels(method="POST", path="/ingest", status="202").inc()
        return web.json_response({"status": "accepted", "archive_id": archive_id}, status=202)

    async def _handle_health(self, request: web.Request) -> web.Response:
        """Handle GET /health -- returns service liveness status."""
        HTTP_REQUESTS_TOTAL.labels(method="GET", path="/health", status="200").inc()
        return web.json_response({"status": "ok"})

    async def start(self) -> None:
        """Start the HTTP server."""
        self._app = web.Application(client_max_size=self._settings.max_request_size)
        self._app.router.add_post("/ingest", self._handle_ingest)
        self._app.router.add_get("/health", self._handle_health)

        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        self._site = web.TCPSite(
            self._runner,
            self._settings.host,
            self._settings.port,
        )
        await self._site.start()
        logger.info(
            "http_server_started",
            host=self._settings.host,
            port=self._settings.port,
        )

    async def stop(self) -> None:
        """Stop the HTTP server."""
        if self._runner:
            await self._runner.cleanup()
            logger.info("http_server_stopped")
