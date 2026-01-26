"""Configuration management using pydantic-settings."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


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


class InfluxDBSettings(BaseSettings):
    """InfluxDB connection settings."""

    model_config = SettingsConfigDict(env_prefix="INFLUXDB_")

    url: str = Field(default="http://influxdb:8086", description="InfluxDB URL")
    token: str = Field(description="InfluxDB API token")
    org: str = Field(default="health", description="InfluxDB organization")
    bucket: str = Field(default="apple_health", description="InfluxDB bucket")
    batch_size: int = Field(default=1000, description="Batch size for writes")
    flush_interval_ms: int = Field(default=30000, description="Flush interval in milliseconds")


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


# Global settings instance
_settings: Settings | None = None


def get_settings() -> Settings:
    """Get or create the global settings instance."""
    global _settings
    if _settings is None:
        _settings = Settings.load()
    return _settings
