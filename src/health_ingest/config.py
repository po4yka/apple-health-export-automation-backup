"""Configuration management using pydantic-settings."""

import threading
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Valid log levels
VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}


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


class OpenAISettings(BaseSettings):
    """OpenAI API settings for health reports."""

    model_config = SettingsConfigDict(env_prefix="OPENAI_")

    api_key: str | None = Field(default=None, description="OpenAI API key")
    model: str = Field(default="gpt-4o-mini", description="OpenAI model to use")
    base_url: str = Field(
        default="https://api.openai.com/v1",
        description="OpenAI-compatible base URL",
    )


class GrokSettings(BaseSettings):
    """Grok (xAI) API settings for health reports."""

    model_config = SettingsConfigDict(env_prefix="GROK_")

    api_key: str | None = Field(default=None, description="Grok API key")
    model: str = Field(default="grok-2-latest", description="Grok model to use")
    base_url: str = Field(
        default="https://api.x.ai/v1",
        description="Grok OpenAI-compatible base URL",
    )


class ArchiveSettings(BaseSettings):
    """Raw payload archive settings."""

    model_config = SettingsConfigDict(env_prefix="ARCHIVE_")

    enabled: bool = Field(default=True, description="Enable raw payload archiving")
    dir: str = Field(default="/data/archive", description="Archive directory path")
    rotation: str = Field(default="daily", description="Rotation strategy: daily or hourly")
    max_age_days: int = Field(default=30, description="Delete archives older than this")
    compress_after_days: int = Field(default=7, description="Compress archives older than this")

    @field_validator("rotation")
    @classmethod
    def validate_rotation(cls, v: str) -> str:
        """Validate rotation strategy."""
        normalized = v.lower()
        if normalized not in ("daily", "hourly"):
            raise ValueError(f"Invalid rotation '{v}'. Must be 'daily' or 'hourly'")
        return normalized

    @field_validator("max_age_days", "compress_after_days")
    @classmethod
    def validate_positive_days(cls, v: int) -> int:
        """Validate days are positive."""
        if v < 1:
            raise ValueError(f"Days must be at least 1, got {v}")
        return v


class DedupSettings(BaseSettings):
    """Deduplication cache settings."""

    model_config = SettingsConfigDict(env_prefix="DEDUP_")

    enabled: bool = Field(default=True, description="Enable deduplication")
    max_size: int = Field(default=100_000, description="Maximum cache entries")
    ttl_hours: int = Field(default=24, description="TTL for cache entries in hours")
    persist_enabled: bool = Field(default=True, description="Enable SQLite persistence")
    persist_path: str = Field(default="/data/dedup/cache.db", description="Persistence file path")
    checkpoint_interval_sec: int = Field(default=300, description="Checkpoint interval in seconds")

    @field_validator("max_size")
    @classmethod
    def validate_max_size(cls, v: int) -> int:
        """Validate max size is reasonable."""
        if v < 100:
            raise ValueError(f"Max size must be at least 100, got {v}")
        if v > 10_000_000:
            raise ValueError(f"Max size too large (max 10M), got {v}")
        return v

    @field_validator("ttl_hours")
    @classmethod
    def validate_ttl_hours(cls, v: int) -> int:
        """Validate TTL is reasonable."""
        if v < 1:
            raise ValueError(f"TTL must be at least 1 hour, got {v}")
        return v


class DLQSettings(BaseSettings):
    """Dead-letter queue settings."""

    model_config = SettingsConfigDict(env_prefix="DLQ_")

    enabled: bool = Field(default=True, description="Enable dead-letter queue")
    db_path: str = Field(default="/data/dlq/dlq.db", description="SQLite database path")
    max_entries: int = Field(default=10_000, description="Maximum entries before eviction")
    retention_days: int = Field(default=30, description="Delete entries older than this")
    max_retries: int = Field(default=3, description="Maximum replay attempts")

    @field_validator("max_entries")
    @classmethod
    def validate_max_entries(cls, v: int) -> int:
        """Validate max entries is reasonable."""
        if v < 100:
            raise ValueError(f"Max entries must be at least 100, got {v}")
        return v

    @field_validator("max_retries")
    @classmethod
    def validate_max_retries(cls, v: int) -> int:
        """Validate max retries is reasonable."""
        if v < 1:
            raise ValueError(f"Max retries must be at least 1, got {v}")
        if v > 10:
            raise ValueError(f"Max retries too high (max 10), got {v}")
        return v


class OpenClawSettings(BaseSettings):
    """OpenClaw gateway settings for report delivery."""

    model_config = SettingsConfigDict(env_prefix="OPENCLAW_")

    enabled: bool = Field(default=True, description="Enable Telegram delivery via OpenClaw")
    gateway_url: str = Field(
        default="http://openclaw-gateway:18789",
        description="OpenClaw gateway URL",
    )
    hooks_token: str | None = Field(default=None, description="Hooks API authentication token")
    telegram_user_id: int = Field(default=0, description="Target Telegram user ID")
    max_retries: int = Field(default=3, description="Maximum delivery retries")
    retry_delay_seconds: float = Field(default=5.0, description="Initial retry delay in seconds")

    @field_validator("max_retries")
    @classmethod
    def validate_max_retries(cls, v: int) -> int:
        """Validate max retries is reasonable."""
        if v < 1:
            raise ValueError(f"Max retries must be at least 1, got {v}")
        if v > 10:
            raise ValueError(f"Max retries too high (max 10), got {v}")
        return v


class InsightSettings(BaseSettings):
    """AI insight generation settings."""

    model_config = SettingsConfigDict(env_prefix="INSIGHT_")

    prefer_ai: bool = Field(default=True, description="Prefer AI over rules when available")
    ai_provider: Literal["anthropic", "openai", "grok"] = Field(
        default="anthropic",
        description="AI provider to use for insights",
    )
    max_insights: int = Field(default=5, description="Maximum insights to include in report")
    include_reasoning: bool = Field(default=True, description="Include reasoning in insights")
    ai_timeout_seconds: float = Field(default=30.0, description="AI API timeout in seconds")

    @field_validator("max_insights")
    @classmethod
    def validate_max_insights(cls, v: int) -> int:
        """Validate max insights is reasonable."""
        if v < 1:
            raise ValueError(f"Max insights must be at least 1, got {v}")
        if v > 10:
            raise ValueError(f"Max insights too high (max 10), got {v}")
        return v


class HTTPSettings(BaseSettings):
    """HTTP ingestion API settings."""

    model_config = SettingsConfigDict(env_prefix="HTTP_")

    enabled: bool = Field(default=True, description="Enable HTTP ingestion endpoint")
    host: str = Field(default="0.0.0.0", description="HTTP server bind address")
    port: int = Field(default=8080, description="HTTP server port")
    auth_token: str = Field(default="", description="Bearer token for authentication")
    max_request_size: int = Field(
        default=10_485_760, description="Maximum request body size in bytes (10MB)"
    )

    @field_validator("port")
    @classmethod
    def validate_port(cls, v: int) -> int:
        """Validate port is in valid range."""
        if not 1 <= v <= 65535:
            raise ValueError(f"Port must be between 1 and 65535, got {v}")
        return v

    @field_validator("max_request_size")
    @classmethod
    def validate_max_request_size(cls, v: int) -> int:
        """Validate max request size is reasonable."""
        if v < 1024:
            raise ValueError(f"Max request size must be at least 1KB, got {v}")
        if v > 104_857_600:
            raise ValueError(f"Max request size too large (max 100MB), got {v}")
        return v


class AppSettings(BaseSettings):
    """Application settings."""

    model_config = SettingsConfigDict(env_prefix="APP_")

    log_level: str = Field(default="INFO", description="Logging level")
    log_format: str = Field(default="json", description="Log format: json or console")
    default_source: str = Field(
        default="health_auto_export", description="Default source tag for metrics"
    )
    prometheus_port: int = Field(default=9090, description="Port for Prometheus metrics server")

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

    @field_validator("prometheus_port")
    @classmethod
    def validate_prometheus_port(cls, v: int) -> int:
        """Validate Prometheus port is in valid range."""
        if not 1 <= v <= 65535:
            raise ValueError(f"Port must be between 1 and 65535, got {v}")
        return v


class TracingSettings(BaseSettings):
    """OpenTelemetry tracing settings."""

    model_config = SettingsConfigDict(env_prefix="OTEL_")

    enabled: bool = Field(default=False, description="Enable OpenTelemetry tracing")
    service_name: str = Field(default="health-ingest", description="Service name")


class BotSettings(BaseSettings):
    """Telegram bot webhook settings."""

    model_config = SettingsConfigDict(env_prefix="BOT_")

    enabled: bool = Field(default=False, description="Enable Telegram bot webhook")
    webhook_token: str = Field(default="", description="Bearer token for bot webhook auth")
    response_timeout_seconds: float = Field(
        default=15.0, description="Timeout for bot command processing"
    )


class Settings(BaseSettings):
    """Combined application settings."""

    http: HTTPSettings = Field(default_factory=HTTPSettings)
    influxdb: InfluxDBSettings = Field(default_factory=InfluxDBSettings)
    anthropic: AnthropicSettings = Field(default_factory=AnthropicSettings)
    openai: OpenAISettings = Field(default_factory=OpenAISettings)
    grok: GrokSettings = Field(default_factory=GrokSettings)
    app: AppSettings = Field(default_factory=AppSettings)
    archive: ArchiveSettings = Field(default_factory=ArchiveSettings)
    dedup: DedupSettings = Field(default_factory=DedupSettings)
    dlq: DLQSettings = Field(default_factory=DLQSettings)
    openclaw: OpenClawSettings = Field(default_factory=OpenClawSettings)
    insight: InsightSettings = Field(default_factory=InsightSettings)
    tracing: TracingSettings = Field(default_factory=TracingSettings)
    bot: BotSettings = Field(default_factory=BotSettings)

    @classmethod
    def load(cls) -> "Settings":
        """Load settings from environment variables."""
        return cls(
            http=HTTPSettings(),
            influxdb=InfluxDBSettings(),
            anthropic=AnthropicSettings(),
            openai=OpenAISettings(),
            grok=GrokSettings(),
            app=AppSettings(),
            archive=ArchiveSettings(),
            dedup=DedupSettings(),
            dlq=DLQSettings(),
            openclaw=OpenClawSettings(),
            insight=InsightSettings(),
            tracing=TracingSettings(),
            bot=BotSettings(),
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
