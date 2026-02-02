"""OpenTelemetry tracing utilities."""

from __future__ import annotations

import os
from collections.abc import Mapping

import structlog
from opentelemetry import propagate, trace
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from .config import TracingSettings
from .types import TraceContextCarrier

logger = structlog.get_logger(__name__)


def setup_tracing(settings: TracingSettings) -> bool:
    """Configure OpenTelemetry tracing.

    Returns:
        True if tracing was configured, False otherwise.
    """
    if not settings.enabled:
        logger.info("tracing_disabled")
        return False

    exporter_name = os.getenv("OTEL_TRACES_EXPORTER", "otlp").lower()
    if exporter_name in {"none", ""}:
        logger.info("tracing_exporter_disabled")
        return False

    resource = Resource.create({"service.name": settings.service_name})
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
    logger.info(
        "tracing_configured",
        exporter=exporter_name,
        service_name=settings.service_name,
    )
    return True


def inject_trace_context() -> TraceContextCarrier:
    """Inject the current trace context into a carrier dict."""
    carrier: TraceContextCarrier = {}
    propagate.inject(carrier)
    return carrier


def extract_trace_context(headers: Mapping[str, str] | None) -> Context | None:
    """Extract trace context from headers or carriers."""
    if not headers:
        return None
    return propagate.extract(headers)
