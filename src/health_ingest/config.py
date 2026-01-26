"""Configuration management using pydantic-settings."""

import threading

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Valid log levels
VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}


class MQTTSettings(BaseSettings):
    """MQTT connection settings."""

    model_config = SettingsConfigDict(env_prefix="MQTT_")

    host: str = Field(default="192.168.1.175", description="MQTT broker host")
    port: int = Field(default=1883, description="MQTT broker port")
    username: str | None = Field(default=None, description="MQTT username")
    password: str | None = Field(default=None, description="MQTT password")
    topic: str = Field(default="health/export/#", description="MQTT topic to subscribe")
    client_id: str = Field(default="health-ingest", description="MQTT client ID")
    keepalive: int = Field(default=60, description="MQTT keepalive interval in seconds")

    @field_validator("port")
    @classmethod
    def validate_port(cls, v: int) -> int:
        """Validate port is in valid range."""
        if not 1 <= v <= 65535:
            raise ValueError(f"Port must be between 1 and 65535, got {v}")
        return v

    @field_validator("keepalive")
    @classmethod
    def validate_keepalive(cls, v: int) -> int:
        """Validate keepalive is positive."""
        if v < 1:
            raise ValueError(f"Keepalive must be at least 1 second, got {v}")
        return v


class InfluxDBSettings(BaseSettings):
    """InfluxDB connection settings."""

    model_config = SettingsConfigDict(env_prefix="INFLUXDB_")

    url: str = Field(default="http://influxdb:8086", description="InfluxDB URL")
    token: str = Field(description="InfluxDB API token")
    org: str = Field(default="health", description="InfluxDB organization")
    bucket: str = Field(default="apple_health", description="InfluxDB bucket")
    batch_size: int = Field(default=1000, description="Batch size for writes")
    flush_interval_ms: int = Field(default=30000, description="Flush interval in milliseconds")

    @field_validator("token")
    @classmethod
    def validate_token(cls, v: str) -> str:
        """Validate token is not empty."""
        if not v or not v.strip():
            raise ValueError("InfluxDB token cannot be empty")
        return v

    @field_validator("batch_size")
    @classmethod
    def validate_batch_size(cls, v: int) -> int:
        """Validate batch size is reasonable."""
        if v < 1:
            raise ValueError(f"Batch size must be at least 1, got {v}")
        if v > 50000:
            raise ValueError(f"Batch size too large (max 50000), got {v}")
        return v

    @field_validator("flush_interval_ms")
    @classmethod
    def validate_flush_interval(cls, v: int) -> int:
        """Validate flush interval is reasonable."""
        if v < 100:
            raise ValueError(f"Flush interval must be at least 100ms, got {v}")
        return v


class AnthropicSettings(BaseSettings):
    """Anthropic API settings for health reports."""

    model_config = SettingsConfigDict(env_prefix="ANTHROPIC_")

    api_key: str | None = Field(default=None, description="Anthropic API key")
    model: str = Field(default="claude-sonnet-4-20250514", description="Claude model to use")


class AppSettings(BaseSettings):
    """Application settings."""

    model_config = SettingsConfigDict(env_prefix="APP_")

    log_level: str = Field(default="INFO", description="Logging level")
    log_format: str = Field(default="json", description="Log format: json or console")
    default_source: str = Field(
        default="health_auto_export", description="Default source tag for metrics"
    )

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level is valid."""
        normalized = v.upper()
        if normalized not in VALID_LOG_LEVELS:
            raise ValueError(
                f"Invalid log level '{v}'. Must be one of: {', '.join(VALID_LOG_LEVELS)}"
            )
        return normalized

    @field_validator("log_format")
    @classmethod
    def validate_log_format(cls, v: str) -> str:
        """Validate log format is valid."""
        normalized = v.lower()
        if normalized not in ("json", "console"):
            raise ValueError(f"Invalid log format '{v}'. Must be 'json' or 'console'")
        return normalized


class Settings(BaseSettings):
    """Combined application settings."""

    mqtt: MQTTSettings = Field(default_factory=MQTTSettings)
    influxdb: InfluxDBSettings = Field(default_factory=InfluxDBSettings)
    anthropic: AnthropicSettings = Field(default_factory=AnthropicSettings)
    app: AppSettings = Field(default_factory=AppSettings)

    @classmethod
    def load(cls) -> "Settings":
        """Load settings from environment variables."""
        return cls(
            mqtt=MQTTSettings(),
            influxdb=InfluxDBSettings(),
            anthropic=AnthropicSettings(),
            app=AppSettings(),
        )


# Global settings instance with thread-safe initialization
_settings: Settings | None = None
_settings_lock = threading.Lock()


def get_settings() -> Settings:
    """Get or create the global settings instance.

    Thread-safe singleton pattern using double-checked locking.
    """
    global _settings
    if _settings is None:
        with _settings_lock:
            # Double-check after acquiring lock
            if _settings is None:
                _settings = Settings.load()
    return _settings
