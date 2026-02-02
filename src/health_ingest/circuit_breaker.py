"""Async circuit breaker for protecting external service calls."""

import threading
import time
from enum import Enum

import structlog

from .types import JSONObject

logger = structlog.get_logger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Simple async circuit breaker (CLOSED -> OPEN -> HALF_OPEN -> CLOSED).

    When consecutive failures exceed the threshold, the circuit opens and
    all calls fail fast without attempting the operation. After the recovery
    timeout, one probe call is allowed through (half-open). If it succeeds,
    the circuit closes; if it fails, the circuit reopens.
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
    ) -> None:
        self._name = name
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._lock = threading.RLock()
        self._failure_count = 0
        self._state = CircuitState.CLOSED
        self._last_failure_time: float = 0
        self._total_trips = 0

    @property
    def state(self) -> CircuitState:
        """Current circuit state, accounting for recovery timeout."""
        with self._lock:
            elapsed = time.monotonic() - self._last_failure_time
            if self._state == CircuitState.OPEN and elapsed >= self._recovery_timeout:
                self._state = CircuitState.HALF_OPEN
                logger.info(
                    "circuit_half_open",
                    name=self._name,
                    after_seconds=self._recovery_timeout,
                )
            return self._state

    @property
    def is_closed(self) -> bool:
        return self.state == CircuitState.CLOSED

    @property
    def is_open(self) -> bool:
        return self.state == CircuitState.OPEN

    def record_success(self) -> None:
        """Record a successful call. Resets failure count and closes circuit."""
        with self._lock:
            if self._state != CircuitState.CLOSED:
                logger.info("circuit_closed", name=self._name)
            self._failure_count = 0
            self._state = CircuitState.CLOSED

    def record_failure(self) -> None:
        """Record a failed call. May trip the circuit open."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.monotonic()
            if self._failure_count >= self._failure_threshold:
                if self._state != CircuitState.OPEN:
                    self._total_trips += 1
                    logger.warning(
                        "circuit_opened",
                        name=self._name,
                        failures=self._failure_count,
                        recovery_timeout=self._recovery_timeout,
                        total_trips=self._total_trips,
                    )
                self._state = CircuitState.OPEN

    def get_stats(self) -> JSONObject:
        """Get circuit breaker statistics."""
        with self._lock:
            return {
                "name": self._name,
                "state": self._state.value,
                "failure_count": self._failure_count,
                "failure_threshold": self._failure_threshold,
                "recovery_timeout": self._recovery_timeout,
                "total_trips": self._total_trips,
            }


class CircuitOpenError(Exception):
    """Raised when a call is attempted on an open circuit."""

    def __init__(self, name: str) -> None:
        super().__init__(f"Circuit breaker '{name}' is open")
        self.breaker_name = name
