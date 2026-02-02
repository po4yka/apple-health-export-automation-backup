"""HTTP handler for Health Auto Export data ingestion via REST API."""

from __future__ import annotations

import asyncio
import hmac
import json
import time
from collections.abc import Awaitable, Callable
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Literal, cast

import structlog
import uvicorn
from fastapi import FastAPI, Request, Response, status
from fastapi.responses import JSONResponse
from opentelemetry import trace
from opentelemetry.trace import SpanKind
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from . import __version__
from .config import HTTPSettings
from .metrics import HTTP_REQUESTS_TOTAL
from .tracing import extract_trace_context, inject_trace_context
from .types import (
    ErrorDetail,
    JSONObject,
    JSONValue,
    ServiceStatusSnapshot,
    StatusComponents,
    TraceContextCarrier,
)

if TYPE_CHECKING:
    from .archive import RawArchiver
    from .bot.dispatcher import BotDispatcher
    from .dlq import DeadLetterQueue

logger = structlog.get_logger(__name__)
tracer = trace.get_tracer(__name__)


def _log_task_exception(task: asyncio.Task) -> None:
    """Log exceptions from background tasks that would otherwise be silently lost."""
    if not task.cancelled() and task.exception():
        logger.error("background_task_failed", error=str(task.exception()))


class MetricPoint(BaseModel):
    """A single metric data point."""

    model_config = ConfigDict(extra="allow")

    date: datetime | str | None = None
    qty: float | None = None
    min: float | None = None
    max: float | None = None
    avg: float | None = None
    source: str | None = None
    units: str | None = None


class MetricItem(MetricPoint):
    """Metric item with a required name."""

    name: str
    start: datetime | str | None = None
    end: datetime | str | None = None
    duration: float | None = None
    activeEnergy: float | None = None
    distance: float | None = None
    avgHeartRate: float | None = None
    maxHeartRate: float | None = None


class MetricSeries(BaseModel):
    """Metric series for the REST API metrics format."""

    model_config = ConfigDict(extra="allow")

    name: str
    units: str | None = None
    data: list[MetricPoint] = Field(default_factory=list)


class MetricsEnvelope(BaseModel):
    """Container for grouped metrics."""

    metrics: list[MetricSeries] = Field(default_factory=list)


class HealthIngestPayload(BaseModel):
    """Top-level payload for ingestion."""

    model_config = ConfigDict(extra="allow")

    data: list[MetricItem] | MetricsEnvelope | None = None


class IngestAcceptedResponse(BaseModel):
    """Response for accepted ingest requests."""

    status: str = "accepted"
    archive_id: str | None = None


class ErrorResponse(BaseModel):
    """Standard error response."""

    error: str
    max_bytes: int | None = None
    details: list[ErrorDetail] | None = None


class ReadyResponse(BaseModel):
    """Readiness response."""

    status: str
    components: StatusComponents = Field(default_factory=dict)


class InfoResponse(BaseModel):
    """Service info response."""

    name: str
    version: str


class DLQReplayRequest(BaseModel):
    """Request payload for replaying DLQ entries."""

    category: str
    limit: int = 100


class ArchiveReplayRequest(BaseModel):
    """Request payload for replaying archive entries."""

    start_date: date
    end_date: date


class WeeklyReportRequest(BaseModel):
    """Request payload for weekly report generation."""

    end_date: datetime | None = None


class DailyReportRequest(BaseModel):
    """Request payload for daily report generation."""

    mode: Literal["morning", "evening"]
    reference_time: datetime | None = None


class BotWebhookPayload(BaseModel):
    """Request payload for bot webhook."""

    model_config = ConfigDict(extra="allow")

    message: str
    user_id: int
    channel: str = "telegram"


class BotCommandPayload(BaseModel):
    """Request payload for synchronous bot command."""

    message: str


class BotCommandResponse(BaseModel):
    """Response for synchronous bot command."""

    text: str


class ReplayResponse(BaseModel):
    """Response for replay requests."""

    status: str
    processed: int | None = None
    success: int | None = None
    failure: int | None = None


class TokenBucketRateLimiter:
    """Token bucket rate limiter for request throttling."""

    def __init__(self, rate_per_minute: int, burst: int) -> None:
        self._rate = rate_per_minute / 60.0
        self._burst = float(burst)
        self._tokens = float(burst)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.monotonic()
            self._tokens = min(self._burst, self._tokens + (now - self._last_refill) * self._rate)
            self._last_refill = now
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False


def _sanitize_validation_errors(errors: list[dict[str, object]]) -> list[dict[str, str]]:
    """Strip internal field details from validation errors for client responses."""
    sanitized = []
    for err in errors:
        loc_parts = [str(part) for part in err.get("loc", [])]
        sanitized.append(
            {
                "field": ".".join(loc_parts) if loc_parts else "unknown",
                "message": "invalid value",
            }
        )
    return sanitized


class HTTPHandler:
    """Handles HTTP ingestion endpoint for health data.

    Provides a REST API that accepts JSON payloads from Health Auto Export,
    routing them through the same processing pipeline via the shared message callback.
    """

    def __init__(
        self,
        settings: HTTPSettings,
        message_callback: Callable[
            [str, JSONObject, str | None, TraceContextCarrier | None], Awaitable[None]
        ],
        archiver: RawArchiver | None = None,
        dlq: DeadLetterQueue | None = None,
        status_provider: Callable[[], ServiceStatusSnapshot] | None = None,
        report_callback: Callable[[datetime | None], Awaitable[str]] | None = None,
        daily_report_callback: Callable[[str, datetime | None], Awaitable[str]] | None = None,
        bot_dispatcher: BotDispatcher | None = None,
        bot_webhook_token: str = "",
    ) -> None:
        self._settings = settings
        self._message_callback = message_callback
        self._archiver = archiver
        self._dlq = dlq
        self._status_provider = status_provider
        self._report_callback = report_callback
        self._daily_report_callback = daily_report_callback
        self._bot_dispatcher = bot_dispatcher
        self._bot_webhook_token = bot_webhook_token
        self._app: FastAPI | None = None
        self._server: uvicorn.Server | None = None
        self._server_task: asyncio.Task | None = None
        self._rate_limiter: TokenBucketRateLimiter | None = None
        if settings.rate_limit_per_minute > 0:
            self._rate_limiter = TokenBucketRateLimiter(
                rate_per_minute=settings.rate_limit_per_minute,
                burst=settings.rate_limit_burst,
            )

    def _check_auth(self, request: Request) -> bool:
        """Validate Bearer token from Authorization header."""
        if not self._settings.auth_token:
            return True  # No token configured = auth disabled
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return False
        return hmac.compare_digest(auth_header[7:], self._settings.auth_token)

    def _check_bot_auth(self, request: Request) -> bool:
        """Validate Bearer token for bot webhook (separate from ingest auth)."""
        if not self._bot_webhook_token:
            return True  # No token configured = auth disabled
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return False
        return hmac.compare_digest(auth_header[7:], self._bot_webhook_token)

    def _build_app(self) -> FastAPI:
        app = FastAPI(
            title="Health Ingest API",
            version="0.1.0",
            description="Ingestion API for Apple Health Auto Export payloads.",
        )

        def error_response(
            status_code: int,
            error: str,
            max_bytes: int | None = None,
            details: list[ErrorDetail] | None = None,
        ) -> JSONResponse:
            payload: dict[str, JSONValue] = {"error": error}
            if max_bytes is not None:
                payload["max_bytes"] = max_bytes
            if details is not None:
                payload["details"] = details
            return JSONResponse(status_code=status_code, content=payload)

        @app.post(
            "/ingest",
            status_code=status.HTTP_202_ACCEPTED,
            response_model=IngestAcceptedResponse,
            responses={
                400: {"model": ErrorResponse},
                401: {"model": ErrorResponse},
                413: {"model": ErrorResponse},
                422: {"model": ErrorResponse},
                429: {"model": ErrorResponse},
                500: {"model": ErrorResponse},
                503: {"model": ErrorResponse},
            },
            summary="Ingest Apple Health payloads",
        )
        async def ingest(request: Request) -> IngestAcceptedResponse:
            """Handle POST /ingest -- accepts health data JSON payload."""
            request_context = extract_trace_context(dict(request.headers))
            with tracer.start_as_current_span(
                "http.ingest",
                context=request_context,
                kind=SpanKind.SERVER,
            ) as span:
                span.set_attribute("http.method", "POST")
                span.set_attribute("http.route", "/ingest")
                trace_context = span.get_span_context()
                trace_id = (
                    format(trace_context.trace_id, "032x")
                    if trace_context and trace_context.trace_id
                    else None
                )
                logger.info(
                    "http_ingest_received",
                    client_host=request.client.host if request.client else None,
                    user_agent=request.headers.get("user-agent"),
                    content_length=request.headers.get("content-length"),
                    trace_id=trace_id,
                )

                if not self._check_auth(request):
                    HTTP_REQUESTS_TOTAL.labels(method="POST", path="/ingest", status="401").inc()
                    return error_response(status.HTTP_401_UNAUTHORIZED, "Unauthorized")

                if self._rate_limiter and not await self._rate_limiter.acquire():
                    HTTP_REQUESTS_TOTAL.labels(method="POST", path="/ingest", status="429").inc()
                    return error_response(
                        status.HTTP_429_TOO_MANY_REQUESTS,
                        "Rate limit exceeded",
                    )

                content_length = request.headers.get("content-length")
                if content_length and int(content_length) > self._settings.max_request_size:
                    HTTP_REQUESTS_TOTAL.labels(method="POST", path="/ingest", status="413").inc()
                    return error_response(
                        status.HTTP_413_CONTENT_TOO_LARGE,
                        "Request body too large",
                        max_bytes=self._settings.max_request_size,
                    )

                try:
                    raw_body = await request.body()
                except Exception as e:
                    logger.warning("request_body_read_failed", error=str(e))
                    HTTP_REQUESTS_TOTAL.labels(method="POST", path="/ingest", status="400").inc()
                    return error_response(
                        status.HTTP_400_BAD_REQUEST, "Failed to read request body"
                    )

                if len(raw_body) > self._settings.max_request_size:
                    HTTP_REQUESTS_TOTAL.labels(method="POST", path="/ingest", status="413").inc()
                    return error_response(
                        status.HTTP_413_CONTENT_TOO_LARGE,
                        "Request body too large",
                        max_bytes=self._settings.max_request_size,
                    )

                archive_id: str | None = None
                if self._archiver:
                    try:
                        archive_id = self._archiver.store_sync(
                            topic="http/ingest",
                            payload=raw_body,
                            received_at=datetime.now(),
                        )
                    except Exception as exc:
                        logger.error("http_archive_store_failed", error=str(exc))

                try:
                    payload = json.loads(raw_body)
                except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                    logger.warning(
                        "http_payload_parse_error",
                        error=str(exc),
                        archive_id=archive_id,
                    )
                    if self._dlq:
                        from .dlq import DLQCategory

                        await self._dlq.enqueue(
                            category=DLQCategory.JSON_PARSE_ERROR,
                            topic="http/ingest",
                            payload=raw_body,
                            error=exc,
                            archive_id=archive_id,
                        )
                    HTTP_REQUESTS_TOTAL.labels(method="POST", path="/ingest", status="400").inc()
                    return error_response(status.HTTP_400_BAD_REQUEST, "Invalid JSON")

                if not isinstance(payload, dict):
                    HTTP_REQUESTS_TOTAL.labels(method="POST", path="/ingest", status="422").inc()
                    return error_response(
                        status.HTTP_422_UNPROCESSABLE_ENTITY,
                        "Payload must be a JSON object",
                    )

                try:
                    HealthIngestPayload.model_validate(payload)
                except ValidationError as exc:
                    logger.warning(
                        "http_payload_validation_error",
                        errors=exc.errors(),
                        archive_id=archive_id,
                    )
                    HTTP_REQUESTS_TOTAL.labels(method="POST", path="/ingest", status="422").inc()
                    return error_response(
                        status.HTTP_422_UNPROCESSABLE_ENTITY,
                        "Payload validation failed",
                        details=_sanitize_validation_errors(exc.errors()),
                    )

                payload = cast(JSONObject, payload)

                if archive_id:
                    span.set_attribute("archive.id", archive_id)
                span.set_attribute("payload.size", len(raw_body))

                try:
                    trace_context = inject_trace_context()
                    await self._message_callback("http/ingest", payload, archive_id, trace_context)
                except asyncio.QueueFull:
                    HTTP_REQUESTS_TOTAL.labels(method="POST", path="/ingest", status="429").inc()
                    return error_response(
                        status.HTTP_429_TOO_MANY_REQUESTS,
                        "Service overloaded, try again later",
                    )
                except RuntimeError as exc:
                    if str(exc) == "message_queue_not_ready":
                        HTTP_REQUESTS_TOTAL.labels(
                            method="POST",
                            path="/ingest",
                            status="503",
                        ).inc()
                        return error_response(
                            status.HTTP_503_SERVICE_UNAVAILABLE, "Service not ready"
                        )
                    logger.error("http_enqueue_error", error=str(exc), archive_id=archive_id)
                    HTTP_REQUESTS_TOTAL.labels(method="POST", path="/ingest", status="500").inc()
                    return error_response(
                        status.HTTP_500_INTERNAL_SERVER_ERROR,
                        "Internal server error",
                    )
                except Exception as exc:
                    logger.error("http_enqueue_error", error=str(exc), archive_id=archive_id)
                    HTTP_REQUESTS_TOTAL.labels(method="POST", path="/ingest", status="500").inc()
                    return error_response(
                        status.HTTP_500_INTERNAL_SERVER_ERROR,
                        "Internal server error",
                    )

                logger.debug(
                    "http_message_accepted",
                    payload_size=len(raw_body),
                    archive_id=archive_id,
                )
                HTTP_REQUESTS_TOTAL.labels(method="POST", path="/ingest", status="202").inc()
                return IngestAcceptedResponse(status="accepted", archive_id=archive_id)

        @app.get(
            "/health",
            response_model=dict[str, str],
            summary="Health check",
        )
        async def health() -> dict[str, str]:
            """Handle GET /health -- returns service liveness status."""
            HTTP_REQUESTS_TOTAL.labels(method="GET", path="/health", status="200").inc()
            return {"status": "ok"}

        @app.get(
            "/ready",
            response_model=ReadyResponse,
            responses={
                503: {"model": ErrorResponse},
            },
            summary="Readiness check",
        )
        async def ready():
            """Handle GET /ready -- returns readiness of dependencies."""
            if self._status_provider:
                status_payload = self._status_provider()
                readiness_status = status_payload.get("status", "unknown")
                components = status_payload.get("components", {})
            else:
                components = {
                    "archiver": "enabled" if self._archiver else "disabled",
                    "dlq": "enabled" if self._dlq else "disabled",
                }
                readiness_status = "ok"
            if readiness_status != "ok":
                HTTP_REQUESTS_TOTAL.labels(method="GET", path="/ready", status="503").inc()
                return JSONResponse(
                    status_code=503,
                    content={"status": readiness_status, "components": components},
                )
            HTTP_REQUESTS_TOTAL.labels(method="GET", path="/ready", status="200").inc()
            return ReadyResponse(status=readiness_status, components=components)

        @app.get(
            "/info",
            response_model=InfoResponse,
            summary="Service info",
        )
        async def info() -> InfoResponse:
            """Handle GET /info -- returns service metadata."""
            HTTP_REQUESTS_TOTAL.labels(method="GET", path="/info", status="200").inc()
            return InfoResponse(name="health-ingest", version=__version__)

        @app.get(
            "/metrics",
            summary="Prometheus metrics",
        )
        async def metrics() -> Response:
            """Handle GET /metrics -- returns Prometheus metrics."""
            HTTP_REQUESTS_TOTAL.labels(method="GET", path="/metrics", status="200").inc()
            return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

        @app.get(
            "/dlq",
            responses={
                401: {"model": ErrorResponse},
                503: {"model": ErrorResponse},
            },
            summary="List DLQ entries",
        )
        async def list_dlq_entries(
            request: Request,
            category: str | None = None,
            limit: int = 100,
            offset: int = 0,
        ):
            """Handle GET /dlq -- list DLQ entries."""
            if not self._check_auth(request):
                HTTP_REQUESTS_TOTAL.labels(method="GET", path="/dlq", status="401").inc()
                return error_response(status.HTTP_401_UNAUTHORIZED, "Unauthorized")
            if not self._dlq:
                HTTP_REQUESTS_TOTAL.labels(method="GET", path="/dlq", status="503").inc()
                return error_response(status.HTTP_503_SERVICE_UNAVAILABLE, "DLQ unavailable")
            from .dlq import DLQCategory

            try:
                dlq_category = DLQCategory(category) if category else None
            except ValueError:
                HTTP_REQUESTS_TOTAL.labels(method="GET", path="/dlq", status="400").inc()
                return error_response(status.HTTP_400_BAD_REQUEST, "Invalid category")
            entries = await self._dlq.get_entries(
                category=dlq_category,
                limit=limit,
                offset=offset,
            )
            HTTP_REQUESTS_TOTAL.labels(method="GET", path="/dlq", status="200").inc()
            return {"items": [entry.to_dict() for entry in entries]}

        @app.get(
            "/dlq/{entry_id}",
            responses={
                401: {"model": ErrorResponse},
                404: {"model": ErrorResponse},
                503: {"model": ErrorResponse},
            },
            summary="Get DLQ entry",
        )
        async def get_dlq_entry(
            request: Request,
            entry_id: str,
        ):
            """Handle GET /dlq/{entry_id} -- fetch a DLQ entry."""
            if not self._check_auth(request):
                HTTP_REQUESTS_TOTAL.labels(method="GET", path="/dlq/{id}", status="401").inc()
                return error_response(status.HTTP_401_UNAUTHORIZED, "Unauthorized")
            if not self._dlq:
                HTTP_REQUESTS_TOTAL.labels(method="GET", path="/dlq/{id}", status="503").inc()
                return error_response(status.HTTP_503_SERVICE_UNAVAILABLE, "DLQ unavailable")
            entry = await self._dlq.get_entry(entry_id)
            if not entry:
                HTTP_REQUESTS_TOTAL.labels(method="GET", path="/dlq/{id}", status="404").inc()
                return error_response(status.HTTP_404_NOT_FOUND, "Entry not found")
            HTTP_REQUESTS_TOTAL.labels(method="GET", path="/dlq/{id}", status="200").inc()
            return entry.to_dict()

        @app.post(
            "/dlq/{entry_id}/replay",
            response_model=ReplayResponse,
            responses={
                401: {"model": ErrorResponse},
                404: {"model": ErrorResponse},
                503: {"model": ErrorResponse},
            },
            summary="Replay DLQ entry",
        )
        async def replay_dlq_entry(
            request: Request,
            entry_id: str,
        ):
            """Handle POST /dlq/{entry_id}/replay -- replay a DLQ entry."""
            if not self._check_auth(request):
                HTTP_REQUESTS_TOTAL.labels(
                    method="POST", path="/dlq/{id}/replay", status="401"
                ).inc()
                return error_response(status.HTTP_401_UNAUTHORIZED, "Unauthorized")
            if not self._dlq:
                HTTP_REQUESTS_TOTAL.labels(
                    method="POST", path="/dlq/{id}/replay", status="503"
                ).inc()
                return error_response(status.HTTP_503_SERVICE_UNAVAILABLE, "DLQ unavailable")

            async def callback(topic: str, payload: JSONObject) -> None:
                await self._message_callback(topic, payload, None, None)

            success = await self._dlq.replay_entry(entry_id, callback)
            if not success:
                HTTP_REQUESTS_TOTAL.labels(
                    method="POST", path="/dlq/{id}/replay", status="404"
                ).inc()
                return error_response(status.HTTP_404_NOT_FOUND, "Entry not found or failed")
            HTTP_REQUESTS_TOTAL.labels(method="POST", path="/dlq/{id}/replay", status="200").inc()
            return ReplayResponse(status="replayed", success=1, failure=0)

        @app.post(
            "/dlq/replay",
            response_model=ReplayResponse,
            responses={
                401: {"model": ErrorResponse},
                503: {"model": ErrorResponse},
            },
            summary="Replay DLQ category",
        )
        async def replay_dlq_category(
            request: Request,
            payload: DLQReplayRequest,
        ):
            """Handle POST /dlq/replay -- replay DLQ entries by category."""
            if not self._check_auth(request):
                HTTP_REQUESTS_TOTAL.labels(method="POST", path="/dlq/replay", status="401").inc()
                return error_response(status.HTTP_401_UNAUTHORIZED, "Unauthorized")
            if not self._dlq:
                HTTP_REQUESTS_TOTAL.labels(method="POST", path="/dlq/replay", status="503").inc()
                return error_response(status.HTTP_503_SERVICE_UNAVAILABLE, "DLQ unavailable")
            from .dlq import DLQCategory

            try:
                category = DLQCategory(payload.category)
            except ValueError:
                HTTP_REQUESTS_TOTAL.labels(method="POST", path="/dlq/replay", status="400").inc()
                return error_response(status.HTTP_400_BAD_REQUEST, "Invalid category")

            async def callback(topic: str, payload_data: JSONObject) -> None:
                await self._message_callback(topic, payload_data, None, None)

            success, failure = await self._dlq.replay_category(
                category,
                callback,
                limit=payload.limit,
            )
            HTTP_REQUESTS_TOTAL.labels(method="POST", path="/dlq/replay", status="200").inc()
            return ReplayResponse(status="replayed", success=success, failure=failure)

        @app.delete(
            "/dlq/{entry_id}",
            responses={
                401: {"model": ErrorResponse},
                404: {"model": ErrorResponse},
                503: {"model": ErrorResponse},
            },
            summary="Delete DLQ entry",
        )
        async def delete_dlq_entry(
            request: Request,
            entry_id: str,
        ):
            """Handle DELETE /dlq/{entry_id} -- delete a DLQ entry."""
            if not self._check_auth(request):
                HTTP_REQUESTS_TOTAL.labels(method="DELETE", path="/dlq/{id}", status="401").inc()
                return error_response(status.HTTP_401_UNAUTHORIZED, "Unauthorized")
            if not self._dlq:
                HTTP_REQUESTS_TOTAL.labels(method="DELETE", path="/dlq/{id}", status="503").inc()
                return error_response(status.HTTP_503_SERVICE_UNAVAILABLE, "DLQ unavailable")
            deleted = await self._dlq.delete_entry(entry_id)
            if not deleted:
                HTTP_REQUESTS_TOTAL.labels(method="DELETE", path="/dlq/{id}", status="404").inc()
                return error_response(status.HTTP_404_NOT_FOUND, "Entry not found")
            HTTP_REQUESTS_TOTAL.labels(method="DELETE", path="/dlq/{id}", status="200").inc()
            return {"status": "deleted", "id": entry_id}

        @app.post(
            "/archive/replay",
            response_model=ReplayResponse,
            responses={
                400: {"model": ErrorResponse},
                401: {"model": ErrorResponse},
                503: {"model": ErrorResponse},
            },
            summary="Replay archived payloads",
        )
        async def replay_archive(
            request: Request,
            payload: ArchiveReplayRequest,
        ):
            """Handle POST /archive/replay -- replay archived payloads."""
            if not self._check_auth(request):
                HTTP_REQUESTS_TOTAL.labels(
                    method="POST", path="/archive/replay", status="401"
                ).inc()
                return error_response(status.HTTP_401_UNAUTHORIZED, "Unauthorized")
            if not self._archiver:
                HTTP_REQUESTS_TOTAL.labels(
                    method="POST", path="/archive/replay", status="503"
                ).inc()
                return error_response(status.HTTP_503_SERVICE_UNAVAILABLE, "Archive unavailable")
            if payload.end_date < payload.start_date:
                HTTP_REQUESTS_TOTAL.labels(
                    method="POST", path="/archive/replay", status="400"
                ).inc()
                return error_response(
                    status.HTTP_400_BAD_REQUEST,
                    "end_date must be on or after start_date",
                )

            async def callback(topic: str, payload_data: JSONObject, archive_id: str) -> None:
                await self._message_callback(topic, payload_data, archive_id, None)

            processed = await self._archiver.replay(
                payload.start_date,
                payload.end_date,
                callback,
            )
            HTTP_REQUESTS_TOTAL.labels(method="POST", path="/archive/replay", status="200").inc()
            return ReplayResponse(status="replayed", processed=processed)

        @app.post(
            "/reports/weekly",
            response_model=dict[str, Any],
            responses={
                401: {"model": ErrorResponse},
                503: {"model": ErrorResponse},
            },
            summary="Generate weekly report",
        )
        async def generate_weekly_report(
            request: Request,
            payload: WeeklyReportRequest,
        ):
            """Handle POST /reports/weekly -- generate weekly report."""
            if not self._check_auth(request):
                HTTP_REQUESTS_TOTAL.labels(
                    method="POST", path="/reports/weekly", status="401"
                ).inc()
                return error_response(status.HTTP_401_UNAUTHORIZED, "Unauthorized")
            if not self._report_callback:
                HTTP_REQUESTS_TOTAL.labels(
                    method="POST", path="/reports/weekly", status="503"
                ).inc()
                return error_response(
                    status.HTTP_503_SERVICE_UNAVAILABLE, "Report generator unavailable"
                )
            try:
                report = await self._report_callback(payload.end_date)
            except TimeoutError:
                logger.error("weekly_report_timeout")
                HTTP_REQUESTS_TOTAL.labels(
                    method="POST", path="/reports/weekly", status="504"
                ).inc()
                return error_response(504, "Report generation timed out")
            except Exception as exc:
                logger.error("weekly_report_failed", error=str(exc))
                HTTP_REQUESTS_TOTAL.labels(
                    method="POST", path="/reports/weekly", status="500"
                ).inc()
                return error_response(
                    status.HTTP_500_INTERNAL_SERVER_ERROR, "Report generation failed"
                )
            HTTP_REQUESTS_TOTAL.labels(method="POST", path="/reports/weekly", status="200").inc()
            return {"status": "generated", "report": report}

        @app.post(
            "/reports/daily",
            response_model=dict[str, Any],
            responses={
                401: {"model": ErrorResponse},
                503: {"model": ErrorResponse},
            },
            summary="Generate daily report",
        )
        async def generate_daily_report(
            request: Request,
            payload: DailyReportRequest,
        ):
            """Handle POST /reports/daily -- generate daily summary."""
            if not self._check_auth(request):
                HTTP_REQUESTS_TOTAL.labels(method="POST", path="/reports/daily", status="401").inc()
                return error_response(status.HTTP_401_UNAUTHORIZED, "Unauthorized")
            if not self._daily_report_callback:
                HTTP_REQUESTS_TOTAL.labels(method="POST", path="/reports/daily", status="503").inc()
                return error_response(
                    status.HTTP_503_SERVICE_UNAVAILABLE, "Daily report generator unavailable"
                )
            try:
                report = await self._daily_report_callback(payload.mode, payload.reference_time)
            except TimeoutError:
                logger.error("daily_report_timeout")
                HTTP_REQUESTS_TOTAL.labels(method="POST", path="/reports/daily", status="504").inc()
                return error_response(504, "Report generation timed out")
            except Exception as exc:
                logger.error("daily_report_failed", error=str(exc))
                HTTP_REQUESTS_TOTAL.labels(method="POST", path="/reports/daily", status="500").inc()
                return error_response(
                    status.HTTP_500_INTERNAL_SERVER_ERROR, "Daily report generation failed"
                )
            HTTP_REQUESTS_TOTAL.labels(method="POST", path="/reports/daily", status="200").inc()
            return {"status": "generated", "report": report}

        @app.post(
            "/bot/webhook",
            status_code=status.HTTP_202_ACCEPTED,
            responses={
                401: {"model": ErrorResponse},
                503: {"model": ErrorResponse},
            },
            summary="Bot webhook for Telegram commands",
        )
        async def bot_webhook(
            request: Request,
            payload: BotWebhookPayload,
        ):
            """Handle POST /bot/webhook -- receive bot commands from OpenClaw."""
            if not self._check_bot_auth(request):
                HTTP_REQUESTS_TOTAL.labels(method="POST", path="/bot/webhook", status="401").inc()
                return error_response(status.HTTP_401_UNAUTHORIZED, "Unauthorized")
            if not self._bot_dispatcher:
                HTTP_REQUESTS_TOTAL.labels(method="POST", path="/bot/webhook", status="503").inc()
                return error_response(status.HTTP_503_SERVICE_UNAVAILABLE, "Bot unavailable")

            task = asyncio.create_task(
                self._bot_dispatcher.handle_webhook(payload.message, payload.user_id)
            )
            task.add_done_callback(_log_task_exception)

            HTTP_REQUESTS_TOTAL.labels(method="POST", path="/bot/webhook", status="202").inc()
            return {"status": "accepted", "message": payload.message}

        @app.post(
            "/bot/command",
            response_model=BotCommandResponse,
            responses={
                401: {"model": ErrorResponse},
                500: {"model": ErrorResponse},
                503: {"model": ErrorResponse},
            },
            summary="Synchronous bot command execution",
        )
        async def bot_command(
            request: Request,
            payload: BotCommandPayload,
        ):
            """Handle POST /bot/command -- execute a command and return the result."""
            if not self._check_bot_auth(request):
                HTTP_REQUESTS_TOTAL.labels(method="POST", path="/bot/command", status="401").inc()
                return error_response(status.HTTP_401_UNAUTHORIZED, "Unauthorized")
            if not self._bot_dispatcher:
                HTTP_REQUESTS_TOTAL.labels(method="POST", path="/bot/command", status="503").inc()
                return error_response(status.HTTP_503_SERVICE_UNAVAILABLE, "Bot unavailable")

            try:
                text = await self._bot_dispatcher.process_command(payload.message)
            except Exception as exc:
                logger.error("bot_command_error", error=str(exc))
                HTTP_REQUESTS_TOTAL.labels(method="POST", path="/bot/command", status="500").inc()
                return error_response(
                    status.HTTP_500_INTERNAL_SERVER_ERROR, "Command execution failed"
                )

            HTTP_REQUESTS_TOTAL.labels(method="POST", path="/bot/command", status="200").inc()
            return BotCommandResponse(text=text)

        return app

    async def start(self) -> None:
        """Start the HTTP server."""
        self._app = self._build_app()
        config = uvicorn.Config(
            self._app,
            host=self._settings.host,
            port=self._settings.port,
            log_level="info",
        )
        self._server = uvicorn.Server(config)
        self._server_task = asyncio.create_task(self._server.serve())
        self._server_task.add_done_callback(_log_task_exception)
        logger.info(
            "http_server_started",
            host=self._settings.host,
            port=self._settings.port,
        )

    async def stop(self) -> None:
        """Stop the HTTP server."""
        if self._server:
            self._server.should_exit = True
        if self._server_task:
            await self._server_task
            self._server_task = None
        logger.info("http_server_stopped")

    @property
    def app(self) -> FastAPI:
        """Expose the FastAPI app for testing."""
        if not self._app:
            self._app = self._build_app()
        return self._app
