"""Main entry point for the health data ingestion service."""

import asyncio
import signal
from pathlib import Path
from typing import Any

import structlog

from .archive import RawArchiver
from .config import get_settings
from .dedup import DeduplicationCache
from .dlq import DeadLetterQueue, DLQCategory
from .influx_writer import InfluxWriter
from .logging import setup_logging
from .mqtt_handler import MQTTHandler
from .transformers import TransformerRegistry

logger = structlog.get_logger(__name__)


# Maximum concurrent message processing tasks to prevent DoS
MAX_CONCURRENT_MESSAGES = 100


class HealthIngestService:
    """Main service orchestrating MQTT ingestion and InfluxDB writes."""

    def __init__(self) -> None:
        """Initialize the health ingestion service."""
        self._settings = get_settings()
        self._mqtt_handler: MQTTHandler | None = None
        self._influx_writer: InfluxWriter | None = None
        self._transformer_registry: TransformerRegistry | None = None
        self._archiver: RawArchiver | None = None
        self._dedup_cache: DeduplicationCache | None = None
        self._dlq: DeadLetterQueue | None = None
        self._shutdown_event = asyncio.Event()
        self._message_count = 0
        self._duplicate_count = 0
        self._pending_tasks: set[asyncio.Task] = set()
        self._semaphore = asyncio.Semaphore(MAX_CONCURRENT_MESSAGES)
        self._rejected_count = 0  # Messages rejected due to rate limiting
        self._checkpoint_task: asyncio.Task | None = None

    async def start(self) -> None:
        """Start the ingestion service."""
        logger.info("service_starting", version="0.1.0")

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

        # Initialize and connect MQTT handler
        self._mqtt_handler = MQTTHandler(
            settings=self._settings.mqtt,
            message_callback=self._handle_message,
            archiver=self._archiver,
            dlq=self._dlq,
        )
        await self._mqtt_handler.connect()

        logger.info("service_started")

    async def stop(self) -> None:
        """Stop the ingestion service gracefully."""
        logger.info("service_stopping", pending_tasks=len(self._pending_tasks))

        # Stop checkpoint task
        if self._checkpoint_task:
            self._checkpoint_task.cancel()
            try:
                await self._checkpoint_task
            except asyncio.CancelledError:
                pass

        # Stop accepting new messages first
        if self._mqtt_handler:
            await self._mqtt_handler.disconnect()

        # Wait for pending tasks to complete (with timeout)
        if self._pending_tasks:
            logger.info("awaiting_pending_tasks", count=len(self._pending_tasks))
            done, pending = await asyncio.wait(
                self._pending_tasks,
                timeout=10.0,  # 10 second timeout for graceful shutdown
            )
            if pending:
                logger.warning(
                    "cancelling_pending_tasks",
                    count=len(pending),
                    completed=len(done),
                )
                for task in pending:
                    task.cancel()
                # Wait for cancellation to complete
                await asyncio.gather(*pending, return_exceptions=True)

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

    def _handle_message(
        self, topic: str, payload: dict[str, Any], archive_id: str | None
    ) -> None:
        """Handle incoming MQTT message.

        Args:
            topic: MQTT topic the message was received on.
            payload: Parsed JSON payload.
            archive_id: Archive entry ID for correlation.
        """
        # Schedule async processing with task tracking and rate limiting
        task = asyncio.create_task(self._process_with_limit(topic, payload, archive_id))
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)

    async def _process_with_limit(
        self, topic: str, payload: dict[str, Any], archive_id: str | None
    ) -> None:
        """Process message with rate limiting.

        Args:
            topic: MQTT topic.
            payload: Health data payload.
            archive_id: Archive entry ID for correlation.
        """
        # Try to acquire semaphore without blocking
        acquired = self._semaphore.locked() is False
        if not acquired and self._semaphore._value == 0:  # type: ignore[attr-defined]
            # All slots are in use - log and process anyway (with semaphore)
            logger.debug(
                "rate_limit_queued",
                topic=topic,
                pending=len(self._pending_tasks),
            )

        async with self._semaphore:
            await self._process_message(topic, payload, archive_id)

    async def _process_message(
        self, topic: str, payload: dict[str, Any], archive_id: str | None
    ) -> None:
        """Process a health data message.

        Args:
            topic: MQTT topic.
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
            "pending_tasks": len(self._pending_tasks),
            "max_concurrent": MAX_CONCURRENT_MESSAGES,
        }

        if self._mqtt_handler:
            result["mqtt"] = {
                "connected": self._mqtt_handler.is_connected(),
            }

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
    3. (Optionally) connect to MQTT

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
                ready = await client.ping()
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
