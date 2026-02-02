"""Tests for the circuit breaker."""

import threading
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


def test_circuit_breaker_thread_safety():
    """Circuit breaker handles concurrent access without corruption."""
    breaker = CircuitBreaker("influx", failure_threshold=3, recovery_timeout=60)
    errors: list[Exception] = []

    def hammer():
        try:
            for _ in range(200):
                breaker.record_failure()
                breaker.state  # noqa: B018
                breaker.get_stats()
                breaker.record_success()
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=hammer) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"Thread-safety errors: {errors}"
    # After all threads finish, state should be consistent
    stats = breaker.get_stats()
    assert stats["state"] in ("closed", "open", "half_open")
