"""Main entry point for the health data ingestion service."""

import asyncio
import signal
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import structlog
from opentelemetry import trace
from opentelemetry.trace import SpanKind
from prometheus_client import start_http_server as start_metrics_server

from .archive import RawArchiver
from .circuit_breaker import CircuitState
from .config import get_settings
from .dedup import DeduplicationCache
from .dlq import DeadLetterQueue, DLQCategory
from .http_handler import HTTPHandler
from .influx_writer import InfluxWriter
from .logging import setup_logging
from .metrics import (
    CIRCUIT_BREAKER_STATE,
    CIRCUIT_BREAKER_TRIPS,
    DEDUP_CACHE_SIZE,
    DUPLICATES_FILTERED,
    MESSAGES_FAILED,
    MESSAGES_PROCESSED,
    QUEUE_DEPTH,
    SERVICE_INFO,
)
from .tracing import extract_trace_context, setup_tracing
from .transformers import TransformerRegistry
from .reports.weekly import WeeklyReportGenerator

logger = structlog.get_logger(__name__)
tracer = trace.get_tracer(__name__)


# Maximum concurrent message processing tasks to prevent DoS
MAX_CONCURRENT_MESSAGES = 100
# Bounded queue size for backpressure (blocks ingestion when full)
MAX_QUEUE_SIZE = 1000


@dataclass(frozen=True)
class QueuedMessage:
    """Message payload with optional trace context."""

    topic: str
    payload: dict[str, Any]
    archive_id: str | None
    trace_context: dict[str, str] | None = None


class HealthIngestService:
    """Main service orchestrating HTTP ingestion and InfluxDB writes."""

    def __init__(self) -> None:
        """Initialize the health ingestion service."""
        self._settings = get_settings()
        self._http_handler: HTTPHandler | None = None
        self._influx_writer: InfluxWriter | None = None
        self._transformer_registry: TransformerRegistry | None = None
        self._archiver: RawArchiver | None = None
        self._dedup_cache: DeduplicationCache | None = None
        self._dlq: DeadLetterQueue | None = None
        self._shutdown_event = asyncio.Event()
        self._message_count = 0
        self._duplicate_count = 0
        self._message_queue: asyncio.Queue[QueuedMessage] | None = None
        self._worker_tasks: list[asyncio.Task] = []
        self._checkpoint_task: asyncio.Task | None = None
        self._watchdog_task: asyncio.Task | None = None

    async def start(self) -> None:
        """Start the ingestion service."""
        setup_tracing(self._settings.tracing)
        logger.info("service_starting", version="0.1.0")
        SERVICE_INFO.info({"version": "0.1.0", "python": "3.13"})

        # Initialize transformer registry
        self._transformer_registry = TransformerRegistry(
            default_source=self._settings.app.default_source
        )

        # Initialize raw archiver
        if self._settings.archive.enabled:
            self._archiver = RawArchiver(
                archive_dir=Path(self._settings.archive.dir),
                rotation=self._settings.archive.rotation,
                max_age_days=self._settings.archive.max_age_days,
                compress_after_days=self._settings.archive.compress_after_days,
            )
            logger.info("archiver_initialized", dir=self._settings.archive.dir)

        # Initialize deduplication cache
        if self._settings.dedup.enabled:
            persist_path = (
                Path(self._settings.dedup.persist_path)
                if self._settings.dedup.persist_enabled
                else None
            )
            self._dedup_cache = DeduplicationCache(
                max_size=self._settings.dedup.max_size,
                persist_path=persist_path,
                ttl_hours=self._settings.dedup.ttl_hours,
            )
            # Restore cache from persistence
            if persist_path:
                restored = await self._dedup_cache.restore()
                logger.info("dedup_cache_restored", entries=restored)

            # Start checkpoint task
            self._checkpoint_task = asyncio.create_task(self._periodic_checkpoint())

        # Initialize dead-letter queue
        if self._settings.dlq.enabled:
            self._dlq = DeadLetterQueue(
                db_path=Path(self._settings.dlq.db_path),
                max_entries=self._settings.dlq.max_entries,
                retention_days=self._settings.dlq.retention_days,
                max_retries=self._settings.dlq.max_retries,
            )
            logger.info("dlq_initialized", path=self._settings.dlq.db_path)

        # Initialize and connect InfluxDB writer
        self._influx_writer = InfluxWriter(self._settings.influxdb)
        await self._influx_writer.connect()

        # Initialize message queue and workers for backpressure
        self._message_queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
        self._worker_tasks = [
            asyncio.create_task(self._worker_loop(worker_id=i))
            for i in range(MAX_CONCURRENT_MESSAGES)
        ]

        # Initialize and start HTTP handler (if enabled)
        if self._settings.http.enabled:
            self._http_handler = HTTPHandler(
                settings=self._settings.http,
                message_callback=self._enqueue_message,
                archiver=self._archiver,
                dlq=self._dlq,
                status_provider=self._status_snapshot,
                report_callback=self._generate_weekly_report,
            )
            await self._http_handler.start()

        # Start internal watchdog for periodic health monitoring
        self._watchdog_task = asyncio.create_task(self._watchdog_loop())

        # Start Prometheus metrics HTTP server (non-blocking)
        start_metrics_server(port=self._settings.app.prometheus_port)
        logger.info(
            "prometheus_metrics_started",
            port=self._settings.app.prometheus_port,
        )

        logger.info("service_started")

    async def stop(self) -> None:
        """Stop the ingestion service gracefully."""
        queue_size = self._message_queue.qsize() if self._message_queue else 0
        logger.info(
            "service_stopping",
            queue_size=queue_size,
            workers=len(self._worker_tasks),
        )

        # Stop watchdog and checkpoint tasks
        for task in (self._watchdog_task, self._checkpoint_task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Stop accepting new messages first
        if self._http_handler:
            await self._http_handler.stop()

        # Drain queue (with timeout), then stop workers
        if self._message_queue:
            try:
                await asyncio.wait_for(self._message_queue.join(), timeout=10.0)
            except TimeoutError:
                logger.warning("queue_drain_timeout", remaining=self._message_queue.qsize())

        if self._worker_tasks:
            for task in self._worker_tasks:
                task.cancel()
            await asyncio.gather(*self._worker_tasks, return_exceptions=True)

        if self._influx_writer:
            await self._influx_writer.disconnect()

        # Final dedup checkpoint
        if self._dedup_cache:
            await self._dedup_cache.checkpoint()
            logger.info("dedup_final_checkpoint_complete")

        logger.info(
            "service_stopped",
            total_messages_processed=self._message_count,
            total_duplicates_filtered=self._duplicate_count,
        )

    def _status_snapshot(self) -> dict[str, Any]:
        """Return a readiness status snapshot for HTTP endpoints."""
        components: dict[str, Any] = {}
        influx_ready = False
        if self._influx_writer:
            components["influxdb"] = self._influx_writer.get_status()
            influx_ready = self._influx_writer.is_ready
        else:
            components["influxdb"] = {"connected": False, "running": False, "circuit_state": "unknown"}

        queue_ready = bool(self._message_queue)
        queue_size = self._message_queue.qsize() if self._message_queue else 0
        components["queue"] = {
            "ready": queue_ready,
            "size": queue_size,
            "max_size": MAX_QUEUE_SIZE,
        }

        if self._settings.archive.enabled:
            components["archiver"] = "enabled" if self._archiver else "missing"
        else:
            components["archiver"] = "disabled"

        if self._settings.dlq.enabled:
            components["dlq"] = "enabled" if self._dlq else "missing"
        else:
            components["dlq"] = "disabled"

        status = "ok" if influx_ready and queue_ready else "degraded"
        return {"status": status, "components": components}

    async def _generate_weekly_report(self, end_date: datetime | None) -> str:
        """Generate a weekly report using configured settings."""
        generator = WeeklyReportGenerator(
            influxdb_settings=self._settings.influxdb,
            anthropic_settings=self._settings.anthropic,
            openai_settings=self._settings.openai,
            grok_settings=self._settings.grok,
            ai_provider=self._settings.insight.ai_provider,
            ai_timeout_seconds=self._settings.insight.ai_timeout_seconds,
        )
        await generator.connect()
        try:
            return await generator.generate_report(end_date=end_date)
        finally:
            await generator.disconnect()

    async def _enqueue_message(
        self,
        topic: str,
        payload: dict[str, Any],
        archive_id: str | None,
        trace_context: dict[str, str] | None,
    ) -> None:
        """Enqueue an incoming message with backpressure."""
        if not self._message_queue:
            logger.warning("message_queue_not_ready")
            raise RuntimeError("message_queue_not_ready")
        try:
            self._message_queue.put_nowait(
                QueuedMessage(
                    topic=topic,
                    payload=payload,
                    archive_id=archive_id,
                    trace_context=trace_context,
                )
            )
            QUEUE_DEPTH.set(self._message_queue.qsize())
        except asyncio.QueueFull:
            QUEUE_DEPTH.set(self._message_queue.qsize())
            raise

    async def _worker_loop(self, worker_id: int) -> None:
        """Worker loop for processing queued messages."""
        if not self._message_queue:
            return

        while True:
            try:
                message = await self._message_queue.get()
            except asyncio.CancelledError:
                break

            try:
                context = extract_trace_context(message.trace_context)
                with tracer.start_as_current_span(
                    "queue.process",
                    context=context,
                    kind=SpanKind.CONSUMER,
                ) as span:
                    span.set_attribute("messaging.system", "asyncio")
                    span.set_attribute("messaging.destination", "health_ingest_queue")
                    span.set_attribute("messaging.destination_kind", "queue")
                    span.set_attribute("health.ingest.topic", message.topic)
                    if message.archive_id:
                        span.set_attribute("archive.id", message.archive_id)
                    await self._process_message(
                        message.topic,
                        message.payload,
                        message.archive_id,
                    )
                MESSAGES_PROCESSED.inc()
            except Exception as e:
                MESSAGES_FAILED.labels(error_type=type(e).__name__).inc()
                logger.exception("worker_error", worker_id=worker_id, error=str(e))
            finally:
                self._message_queue.task_done()
                QUEUE_DEPTH.set(self._message_queue.qsize())

    async def _process_message(
        self, topic: str, payload: dict[str, Any], archive_id: str | None
    ) -> None:
        """Process a health data message.

        Args:
            topic: Message topic.
            payload: Health data payload.
            archive_id: Archive entry ID for correlation.
        """
        import json

        try:
            if not self._transformer_registry or not self._influx_writer:
                logger.warning("service_not_ready")
                return

            # Transform the data
            try:
                points = self._transformer_registry.transform(payload)
            except Exception as e:
                logger.warning(
                    "transform_error",
                    topic=topic,
                    error=str(e),
                    archive_id=archive_id,
                )
                if self._dlq:
                    await self._dlq.enqueue(
                        category=DLQCategory.TRANSFORM_ERROR,
                        topic=topic,
                        payload=json.dumps(payload).encode("utf-8"),
                        error=e,
                        archive_id=archive_id,
                    )
                return

            if not points:
                logger.debug("no_points_generated", topic=topic)
                return

            # Filter duplicates
            if self._dedup_cache:
                original_count = len(points)
                points = self._dedup_cache.filter_duplicates(points)
                filtered = original_count - len(points)
                if filtered > 0:
                    self._duplicate_count += filtered
                    DUPLICATES_FILTERED.inc(filtered)
                    logger.debug(
                        "duplicates_filtered",
                        topic=topic,
                        filtered=filtered,
                        remaining=len(points),
                    )

                if not points:
                    return

            # Write to InfluxDB
            try:
                with tracer.start_as_current_span(
                    "influxdb.write",
                    kind=SpanKind.CLIENT,
                ) as span:
                    span.set_attribute("db.system", "influxdb")
                    span.set_attribute("db.operation", "write")
                    span.set_attribute("influxdb.bucket", self._settings.influxdb.bucket)
                    span.set_attribute("points.count", len(points))
                    await self._influx_writer.write(points)
            except Exception as e:
                logger.warning(
                    "write_error",
                    topic=topic,
                    error=str(e),
                    archive_id=archive_id,
                )
                if self._dlq:
                    await self._dlq.enqueue(
                        category=DLQCategory.WRITE_ERROR,
                        topic=topic,
                        payload=json.dumps(payload).encode("utf-8"),
                        error=e,
                        archive_id=archive_id,
                    )
                return

            # Mark points as processed in dedup cache
            if self._dedup_cache:
                self._dedup_cache.mark_processed_batch(points)

            self._message_count += 1
            logger.debug(
                "message_processed",
                topic=topic,
                points_count=len(points),
                total_processed=self._message_count,
                archive_id=archive_id,
            )

        except Exception as e:
            logger.exception(
                "message_processing_error",
                topic=topic,
                error=str(e),
                archive_id=archive_id,
            )
            if self._dlq:
                await self._dlq.enqueue(
                    category=DLQCategory.UNKNOWN_ERROR,
                    topic=topic,
                    payload=json.dumps(payload).encode("utf-8"),
                    error=e,
                    archive_id=archive_id,
                )

    async def _periodic_checkpoint(self) -> None:
        """Periodically checkpoint dedup cache to SQLite."""
        interval = self._settings.dedup.checkpoint_interval_sec

        while True:
            try:
                await asyncio.sleep(interval)
                if self._dedup_cache:
                    await self._dedup_cache.checkpoint()
                    await self._dedup_cache.cleanup_expired()
                    logger.debug("dedup_checkpoint_complete")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("checkpoint_error", error=str(e))

    async def _watchdog_loop(self) -> None:
        """Periodic internal health monitoring.

        Checks InfluxDB circuit breaker, and queue depth.
        Updates Prometheus gauges for external monitoring.
        """
        while True:
            try:
                await asyncio.sleep(30.0)

                # InfluxDB circuit breaker status
                if self._influx_writer:
                    cb = self._influx_writer.circuit_breaker
                    state = cb.state
                    state_val = {
                        CircuitState.CLOSED: 0,
                        CircuitState.OPEN: 1,
                        CircuitState.HALF_OPEN: 2,
                    }[state]
                    CIRCUIT_BREAKER_STATE.labels(name="influxdb").set(state_val)
                    stats = cb.get_stats()
                    CIRCUIT_BREAKER_TRIPS.labels(name="influxdb").set(stats["total_trips"])
                    if state != CircuitState.CLOSED:
                        logger.warning(
                            "watchdog_influxdb_circuit",
                            state=state.value,
                            failures=stats["failure_count"],
                        )

                # Queue depth
                if self._message_queue:
                    depth = self._message_queue.qsize()
                    QUEUE_DEPTH.set(depth)
                    if depth > MAX_QUEUE_SIZE * 0.8:
                        logger.warning(
                            "watchdog_queue_high",
                            depth=depth,
                            max=MAX_QUEUE_SIZE,
                        )

                # Dedup cache size
                if self._dedup_cache:
                    stats = self._dedup_cache.get_stats()
                    DEDUP_CACHE_SIZE.set(stats["size"])

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("watchdog_error", error=str(e))

    async def run_until_shutdown(self) -> None:
        """Run the service until shutdown signal received."""
        await self._shutdown_event.wait()

    def request_shutdown(self) -> None:
        """Request service shutdown."""
        self._shutdown_event.set()

    async def health_check(self) -> dict[str, Any]:
        """Get service health status.

        Returns:
            Dict with health status of all components.
        """
        result: dict[str, Any] = {
            "service": "healthy",
            "messages_processed": self._message_count,
            "duplicates_filtered": self._duplicate_count,
            "queue_size": self._message_queue.qsize() if self._message_queue else 0,
            "workers": len(self._worker_tasks),
            "max_concurrent": MAX_CONCURRENT_MESSAGES,
        }

        if self._http_handler:
            result["http"] = {"enabled": True}

        if self._influx_writer:
            result["influxdb"] = await self._influx_writer.health_check()

        if self._archiver:
            result["archive"] = await self._archiver.get_stats()

        if self._dedup_cache:
            result["dedup"] = self._dedup_cache.get_stats()

        if self._dlq:
            result["dlq"] = await self._dlq.get_stats()

        return result


async def main() -> None:
    """Main entry point."""
    settings = get_settings()
    setup_logging(settings.app)

    service = HealthIngestService()

    # Set up signal handlers
    loop = asyncio.get_running_loop()

    def signal_handler() -> None:
        logger.info("shutdown_signal_received")
        service.request_shutdown()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    try:
        await service.start()
        await service.run_until_shutdown()
    except Exception as e:
        logger.exception("service_error", error=str(e))
        raise
    finally:
        await service.stop()


def run() -> None:
    """Entry point for the CLI."""
    asyncio.run(main())


def health_check_cli() -> None:
    """Health check CLI for Docker HEALTHCHECK.

    Verifies that the service can:
    1. Load configuration
    2. Connect to InfluxDB

    Exits with code 0 on success, 1 on failure.
    """
    import sys

    from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

    async def check() -> bool:
        try:
            settings = get_settings()

            # Check InfluxDB connection
            client = InfluxDBClientAsync(
                url=settings.influxdb.url,
                token=settings.influxdb.token,
                org=settings.influxdb.org,
            )
            try:
                ready = await asyncio.wait_for(client.ping(), timeout=5.0)
                if not ready:
                    print("InfluxDB ping failed")
                    return False
            finally:
                await client.close()

            print("Health check passed")
            return True
        except Exception as e:
            print(f"Health check failed: {e}")
            return False

    success = asyncio.run(check())
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    run()
