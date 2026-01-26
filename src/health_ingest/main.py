"""Main entry point for the health data ingestion service."""

import asyncio
import signal
from typing import Any

import structlog

from .config import get_settings
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
        self._shutdown_event = asyncio.Event()
        self._message_count = 0
        self._pending_tasks: set[asyncio.Task] = set()
        self._semaphore = asyncio.Semaphore(MAX_CONCURRENT_MESSAGES)
        self._rejected_count = 0  # Messages rejected due to rate limiting

    async def start(self) -> None:
        """Start the ingestion service."""
        logger.info("service_starting", version="0.1.0")

        # Initialize transformer registry
        self._transformer_registry = TransformerRegistry(
            default_source=self._settings.app.default_source
        )

        # Initialize and connect InfluxDB writer
        self._influx_writer = InfluxWriter(self._settings.influxdb)
        await self._influx_writer.connect()

        # Initialize and connect MQTT handler
        self._mqtt_handler = MQTTHandler(
            settings=self._settings.mqtt,
            message_callback=self._handle_message,
        )
        await self._mqtt_handler.connect()

        logger.info("service_started")

    async def stop(self) -> None:
        """Stop the ingestion service gracefully."""
        logger.info("service_stopping", pending_tasks=len(self._pending_tasks))

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

        logger.info(
            "service_stopped",
            total_messages_processed=self._message_count,
        )

    def _handle_message(self, topic: str, payload: dict[str, Any]) -> None:
        """Handle incoming MQTT message.

        Args:
            topic: MQTT topic the message was received on.
            payload: Parsed JSON payload.
        """
        # Schedule async processing with task tracking and rate limiting
        task = asyncio.create_task(self._process_with_limit(topic, payload))
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)

    async def _process_with_limit(self, topic: str, payload: dict[str, Any]) -> None:
        """Process message with rate limiting.

        Args:
            topic: MQTT topic.
            payload: Health data payload.
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
            await self._process_message(topic, payload)

    async def _process_message(self, topic: str, payload: dict[str, Any]) -> None:
        """Process a health data message.

        Args:
            topic: MQTT topic.
            payload: Health data payload.
        """
        try:
            if not self._transformer_registry or not self._influx_writer:
                logger.warning("service_not_ready")
                return

            # Transform the data
            points = self._transformer_registry.transform(payload)

            if not points:
                logger.debug("no_points_generated", topic=topic)
                return

            # Write to InfluxDB
            await self._influx_writer.write(points)

            self._message_count += 1
            logger.debug(
                "message_processed",
                topic=topic,
                points_count=len(points),
                total_processed=self._message_count,
            )

        except Exception as e:
            logger.exception("message_processing_error", topic=topic, error=str(e))

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
            "pending_tasks": len(self._pending_tasks),
            "max_concurrent": MAX_CONCURRENT_MESSAGES,
        }

        if self._mqtt_handler:
            result["mqtt"] = {
                "connected": self._mqtt_handler.is_connected(),
            }

        if self._influx_writer:
            result["influxdb"] = await self._influx_writer.health_check()

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
