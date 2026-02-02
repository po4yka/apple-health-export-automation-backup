"""Tests for the circuit breaker."""

import time

from health_ingest.circuit_breaker import CircuitBreaker, CircuitState


def test_circuit_breaker_opens_after_threshold():
    """Circuit opens after reaching the failure threshold."""
    breaker = CircuitBreaker("influx", failure_threshold=2, recovery_timeout=5)

    breaker.record_failure()
    assert breaker.is_closed

    breaker.record_failure()

    assert breaker.is_open
    stats = breaker.get_stats()
    assert stats["state"] == CircuitState.OPEN.value
    assert stats["failure_count"] == 2
    assert stats["total_trips"] == 1


def test_circuit_breaker_half_open_after_timeout(monkeypatch):
    """Circuit transitions to half-open after recovery timeout."""
    current_time = 0.0

    def fake_monotonic():
        return current_time

    monkeypatch.setattr(time, "monotonic", fake_monotonic)

    breaker = CircuitBreaker("influx", failure_threshold=1, recovery_timeout=5)
    breaker.record_failure()
    assert breaker.is_open

    current_time = 5.0
    assert breaker.state == CircuitState.HALF_OPEN

    breaker.record_success()
    assert breaker.is_closed
    assert breaker.get_stats()["failure_count"] == 0
