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
        logger.info("service_stopping")

        if self._mqtt_handler:
            await self._mqtt_handler.disconnect()

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
        # Schedule async processing
        asyncio.create_task(self._process_message(topic, payload))

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


if __name__ == "__main__":
    run()
