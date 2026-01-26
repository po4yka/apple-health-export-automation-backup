"""MQTT handler for Health Auto Export data ingestion."""

import asyncio
import json
from collections.abc import Callable
from typing import Any

import paho.mqtt.client as mqtt
import structlog

from .config import MQTTSettings

logger = structlog.get_logger(__name__)


class MQTTHandler:
    """Handles MQTT subscription and message routing for health data."""

    def __init__(
        self,
        settings: MQTTSettings,
        message_callback: Callable[[str, dict[str, Any]], None],
    ) -> None:
        """Initialize MQTT handler.

        Args:
            settings: MQTT connection settings.
            message_callback: Callback function to process messages.
                Takes topic (str) and payload (dict) as arguments.
        """
        self._settings = settings
        self._message_callback = message_callback
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

    def _on_message(
        self,
        client: mqtt.Client,
        userdata: Any,
        message: mqtt.MQTTMessage,
    ) -> None:
        """Handle incoming MQTT message."""
        topic = message.topic
        try:
            payload = json.loads(message.payload.decode("utf-8"))
            logger.debug(
                "mqtt_message_received",
                topic=topic,
                payload_size=len(message.payload),
            )

            # Schedule callback in the asyncio event loop
            if self._loop:
                self._loop.call_soon_threadsafe(
                    lambda: self._message_callback(topic, payload)
                )
        except json.JSONDecodeError as e:
            logger.error(
                "mqtt_payload_parse_error",
                topic=topic,
                error=str(e),
            )
        except Exception as e:
            logger.exception(
                "mqtt_message_processing_error",
                topic=topic,
                error=str(e),
            )

    async def connect(self) -> None:
        """Connect to MQTT broker."""
        self._loop = asyncio.get_running_loop()
        self._client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=self._settings.client_id,
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
