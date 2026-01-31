"""Tests for tracing utilities."""

from health_ingest.config import TracingSettings
from health_ingest.tracing import extract_trace_context, inject_trace_context, setup_tracing


TRACEPARENT_HEADER = (
    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
)


def test_setup_tracing_disabled():
    """Tracing should be disabled when setting is false."""
    assert setup_tracing(TracingSettings(enabled=False)) is False


def test_setup_tracing_exporter_disabled(monkeypatch):
    """Tracing should be disabled when exporter env var is none."""
    monkeypatch.setenv("OTEL_TRACES_EXPORTER", "none")

    settings = TracingSettings(enabled=True, service_name="health")
    assert setup_tracing(settings) is False


def test_trace_context_helpers():
    """Trace context helpers should return consistent types."""
    carrier = inject_trace_context()
    assert isinstance(carrier, dict)

    assert extract_trace_context(None) is None
    assert extract_trace_context({}) is None
    assert extract_trace_context({"traceparent": TRACEPARENT_HEADER}) is not None
