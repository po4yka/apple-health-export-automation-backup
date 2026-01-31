"""Apple Health data ingestion service.

A service that receives health data from the Health Auto Export iOS app via
REST API, transforms it into InfluxDB-compatible format, and writes it to
InfluxDB for storage and visualization in Grafana.

Modules:
    config: Configuration management using pydantic-settings
    http_handler: REST API ingestion endpoint
    influx_writer: Async batch writes to InfluxDB
    transformers: Data transformation for different health metric types
    reports: AI-powered health report generation

Example:
    Run the ingestion service::

        $ uv run health-ingest

    Generate a weekly health report::

        $ uv run health-report
"""

__version__ = "0.1.0"

from .config import Settings, get_settings

__all__ = ["Settings", "get_settings", "__version__"]
