"""Tests for configuration validation."""

import pytest

from health_ingest.config import (
    VALID_LOG_LEVELS,
    AppSettings,
    ArchiveSettings,
    HTTPSettings,
    InfluxDBSettings,
)


def test_influxdb_token_validation():
    """InfluxDB token must be non-empty."""
    with pytest.raises(ValueError, match="InfluxDB token cannot be empty"):
        InfluxDBSettings(token="")


def test_influxdb_batch_size_validation():
    """InfluxDB batch size must be within allowed range."""
    with pytest.raises(ValueError, match="Batch size must be at least 1"):
        InfluxDBSettings(token="token", batch_size=0)

    with pytest.raises(ValueError, match="Batch size too large"):
        InfluxDBSettings(token="token", batch_size=100_000)


def test_archive_rotation_normalizes():
    """Archive rotation strategy should normalize to lowercase."""
    settings = ArchiveSettings(rotation="Daily")
    assert settings.rotation == "daily"


def test_app_settings_normalize_log_fields():
    """App settings normalize log format and log level."""
    settings = AppSettings(log_level="debug", log_format="Console")

    assert settings.log_level == "DEBUG"
    assert settings.log_format == "console"
    assert settings.log_level in VALID_LOG_LEVELS


def test_http_settings_validation():
    """HTTP settings enforce valid ranges."""
    with pytest.raises(ValueError, match="Port must be between"):
        HTTPSettings(port=0)

    with pytest.raises(ValueError, match="Max request size must be at least 1KB"):
        HTTPSettings(max_request_size=100)


def test_http_warns_when_auth_token_empty():
    """Warning emitted when HTTP is enabled but auth_token is empty."""
    with pytest.warns(UserWarning, match="HTTP_AUTH_TOKEN is empty"):
        HTTPSettings(_env_file=None, enabled=True, auth_token="")


def test_http_no_warning_when_auth_token_set():
    """No warning when auth_token is set."""
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        HTTPSettings(_env_file=None, enabled=True, auth_token="secret")
