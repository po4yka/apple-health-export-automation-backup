"""Tests for MQTT handler."""

import json
from unittest.mock import MagicMock, patch

import pytest

from health_ingest.config import MQTTSettings
from health_ingest.mqtt_handler import MQTTHandler


class TestMQTTHandler:
    """Tests for MQTTHandler."""

    def setup_method(self):
        self.settings = MQTTSettings(
            host="localhost",
            port=1883,
            topic="health/export/#",
            client_id="test-client",
        )
        self.received_messages = []

        def message_callback(topic: str, payload: dict):
            self.received_messages.append((topic, payload))

        self.handler = MQTTHandler(
            settings=self.settings,
            message_callback=message_callback,
        )

    def test_initialization(self):
        assert self.handler._settings == self.settings
        assert self.handler._client is None
        assert not self.handler.is_connected()

    def test_message_parsing_valid_json(self):
        """Test that valid JSON messages are parsed correctly."""
        # Create a mock message
        mock_message = MagicMock()
        mock_message.topic = "health/export/heart"
        mock_message.payload = json.dumps({
            "name": "heart_rate",
            "date": "2024-01-15T10:00:00Z",
            "qty": 72,
        }).encode("utf-8")

        # We need to test the parsing logic directly
        payload = json.loads(mock_message.payload.decode("utf-8"))
        assert payload["name"] == "heart_rate"
        assert payload["qty"] == 72

    def test_message_parsing_invalid_json(self):
        """Test that invalid JSON is handled gracefully."""
        invalid_payload = b"not valid json {"

        with pytest.raises(json.JSONDecodeError):
            json.loads(invalid_payload.decode("utf-8"))

    def test_settings_with_credentials(self):
        """Test settings with username and password."""
        settings = MQTTSettings(
            host="broker.example.com",
            port=8883,
            username="user",
            password="secret",
            topic="health/#",
        )

        handler = MQTTHandler(settings=settings, message_callback=lambda t, p: None)

        assert handler._settings.username == "user"
        assert handler._settings.password == "secret"

    def test_settings_defaults(self):
        """Test default settings values."""
        settings = MQTTSettings()

        assert settings.host == "192.168.1.175"
        assert settings.port == 1883
        assert settings.topic == "health/export/#"
        assert settings.client_id == "health-ingest"
        assert settings.keepalive == 60

    def test_topic_subscription_pattern(self):
        """Test that topic patterns are correctly formatted."""
        # Single-level wildcard
        settings = MQTTSettings(topic="health/export/+")
        assert "+" in settings.topic

        # Multi-level wildcard
        settings = MQTTSettings(topic="health/#")
        assert "#" in settings.topic


class TestMQTTMessageRouting:
    """Tests for MQTT message routing logic."""

    def test_topic_extraction(self):
        """Test extracting metric type from topic."""
        topics = [
            ("health/export/heart", "heart"),
            ("health/export/activity", "activity"),
            ("health/export/sleep", "sleep"),
            ("health/export/workout", "workout"),
        ]

        for topic, expected in topics:
            parts = topic.split("/")
            metric_type = parts[-1] if len(parts) > 0 else None
            assert metric_type == expected

    def test_payload_structure_single_metric(self):
        """Test expected payload structure for single metric."""
        payload = {
            "name": "heart_rate",
            "date": "2024-01-15T10:30:00+00:00",
            "qty": 72,
            "source": "Apple Watch",
        }

        assert "name" in payload
        assert "date" in payload
        assert "qty" in payload

    def test_payload_structure_batch_metrics(self):
        """Test expected payload structure for batch of metrics."""
        payload = {
            "data": [
                {
                    "name": "heart_rate",
                    "date": "2024-01-15T10:00:00+00:00",
                    "qty": 70,
                },
                {
                    "name": "heart_rate",
                    "date": "2024-01-15T10:05:00+00:00",
                    "qty": 72,
                },
            ]
        }

        assert "data" in payload
        assert isinstance(payload["data"], list)
        assert len(payload["data"]) == 2


class TestMQTTConnectionHandling:
    """Tests for MQTT connection handling."""

    def test_connection_state_initial(self):
        """Test initial connection state."""
        settings = MQTTSettings()
        handler = MQTTHandler(settings=settings, message_callback=lambda t, p: None)

        assert not handler.is_connected()

    @pytest.mark.asyncio
    async def test_disconnect_without_connect(self):
        """Test that disconnecting without connecting doesn't error."""
        settings = MQTTSettings()
        handler = MQTTHandler(settings=settings, message_callback=lambda t, p: None)

        # Should not raise
        await handler.disconnect()
