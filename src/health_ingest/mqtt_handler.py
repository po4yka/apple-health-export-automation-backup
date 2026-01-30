"""MQTT handler for Health Auto Export data ingestion."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import TYPE_CHECKING, Any

import paho.mqtt.client as mqtt
import structlog

from .config import MQTTSettings

if TYPE_CHECKING:
    from .archive import RawArchiver
    from .dlq import DeadLetterQueue, DLQCategory

logger = structlog.get_logger(__name__)


class MQTTHandler:
    """Handles MQTT subscription and message routing for health data."""

    def __init__(
        self,
        settings: MQTTSettings,
        message_callback: Callable[[str, dict[str, Any], str | None], Awaitable[None]],
        archiver: RawArchiver | None = None,
        dlq: DeadLetterQueue | None = None,
    ) -> None:
        """Initialize MQTT handler.

        Args:
            settings: MQTT connection settings.
            message_callback: Callback function to process messages.
                Takes topic (str), payload (dict), and archive_id (str | None).
            archiver: Optional RawArchiver for persisting payloads.
            dlq: Optional DeadLetterQueue for routing parse errors.
        """
        self._settings = settings
        self._message_callback = message_callback
        self._archiver = archiver
        self._dlq = dlq
        self._client: mqtt.Client | None = None
        self._connected = asyncio.Event()
        self._running = False
        self._loop: asyncio.AbstractEventLoop | None = None

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        """Handle MQTT connection established."""
        if reason_code == mqtt.ReasonCode(mqtt.CONNACK_ACCEPTED):
            logger.info(
                "mqtt_connected",
                host=self._settings.host,
                port=self._settings.port,
            )
            client.subscribe(self._settings.topic)
            logger.info("mqtt_subscribed", topic=self._settings.topic)
            if self._loop:
                self._loop.call_soon_threadsafe(self._connected.set)
        else:
            logger.error("mqtt_connection_failed", reason_code=str(reason_code))

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        disconnect_flags: mqtt.DisconnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        """Handle MQTT disconnection."""
        logger.warning("mqtt_disconnected", reason_code=str(reason_code))
        if self._loop:
            self._loop.call_soon_threadsafe(self._connected.clear)

    def _schedule_dlq_enqueue(
        self,
        category: DLQCategory,
        topic: str,
        payload: bytes,
        error: Exception,
        archive_id: str | None,
    ) -> None:
        """Schedule a DLQ enqueue on the event loop with error logging."""
        future = asyncio.run_coroutine_threadsafe(
            self._dlq.enqueue(
                category=category,
                topic=topic,
                payload=payload,
                error=error,
                archive_id=archive_id,
            ),
            self._loop,
        )
        future.add_done_callback(self._dlq_enqueue_done)

    def _dlq_enqueue_done(self, future: asyncio.Future) -> None:
        """Callback for DLQ enqueue completion."""
        if future.exception():
            logger.error(
                "dlq_enqueue_failed",
                error=str(future.exception()),
            )

    def _on_message(
        self,
        client: mqtt.Client,
        userdata: Any,
        message: mqtt.MQTTMessage,
    ) -> None:
        """Handle incoming MQTT message."""
        topic = message.topic
        raw_payload = message.payload
        archive_id: str | None = None

        # Archive raw payload first (before any parsing)
        if self._archiver:
            try:
                archive_id = self._archiver.store_sync(
                    topic=topic,
                    payload=raw_payload,
                    received_at=datetime.now(),
                )
            except Exception as e:
                logger.error("archive_store_failed", topic=topic, error=str(e))

        try:
            payload = json.loads(raw_payload.decode("utf-8"))
            logger.debug(
                "mqtt_message_received",
                topic=topic,
                payload_size=len(raw_payload),
                archive_id=archive_id,
            )

            # Schedule callback in the asyncio event loop (blocks to apply backpressure)
            if self._loop:
                try:
                    future = asyncio.run_coroutine_threadsafe(
                        self._message_callback(topic, payload, archive_id),
                        self._loop,
                    )
                    future.result()
                except Exception as e:
                    logger.error(
                        "mqtt_message_callback_error",
                        topic=topic,
                        error=str(e),
                        archive_id=archive_id,
                    )
        except UnicodeDecodeError as e:
            logger.error(
                "mqtt_payload_decode_error",
                topic=topic,
                error=str(e),
                archive_id=archive_id,
            )
            if self._dlq and self._loop:
                from .dlq import DLQCategory

                self._schedule_dlq_enqueue(
                    category=DLQCategory.UNICODE_DECODE_ERROR,
                    topic=topic,
                    payload=raw_payload,
                    error=e,
                    archive_id=archive_id,
                )
        except json.JSONDecodeError as e:
            logger.error(
                "mqtt_payload_parse_error",
                topic=topic,
                error=str(e),
                archive_id=archive_id,
            )
            # Route to DLQ
            if self._dlq and self._loop:
                from .dlq import DLQCategory

                self._schedule_dlq_enqueue(
                    category=DLQCategory.JSON_PARSE_ERROR,
                    topic=topic,
                    payload=raw_payload,
                    error=e,
                    archive_id=archive_id,
                )
        except Exception as e:
            logger.exception(
                "mqtt_message_processing_error",
                topic=topic,
                error=str(e),
                archive_id=archive_id,
            )

    async def connect(self) -> None:
        """Connect to MQTT broker."""
        self._loop = asyncio.get_running_loop()
        self._client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=self._settings.client_id,
            clean_session=self._settings.clean_session,
        )

        # Set callbacks
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message

        # Set credentials if provided
        if self._settings.username and self._settings.password:
            self._client.username_pw_set(
                self._settings.username,
                self._settings.password,
            )

        # Configure reconnect backoff
        self._client.reconnect_delay_set(
            min_delay=self._settings.reconnect_delay_min,
            max_delay=self._settings.reconnect_delay_max,
        )

        # Connect
        logger.info(
            "mqtt_connecting",
            host=self._settings.host,
            port=self._settings.port,
        )
        self._client.connect_async(
            self._settings.host,
            self._settings.port,
            self._settings.keepalive,
        )
        self._client.loop_start()

        # Wait for connection
        try:
            await asyncio.wait_for(self._connected.wait(), timeout=30.0)
        except TimeoutError:
            logger.error("mqtt_connection_timeout")
            raise

    async def disconnect(self) -> None:
        """Disconnect from MQTT broker."""
        if self._client:
            self._client.loop_stop()
            self._client.disconnect()
            logger.info("mqtt_disconnected_gracefully")

    async def wait_connected(self) -> None:
        """Wait until connected to MQTT broker."""
        await self._connected.wait()

    def is_connected(self) -> bool:
        """Check if connected to MQTT broker."""
        return self._connected.is_set()
