"""InfluxDB writer with async batch writes and retry logic."""

import asyncio
from collections.abc import Sequence
from contextlib import asynccontextmanager
from typing import Any

import structlog
from influxdb_client import Point
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client.client.write_api_async import WriteApiAsync
from influxdb_client.rest import ApiException

from .config import InfluxDBSettings

logger = structlog.get_logger(__name__)

# Maximum buffer size to prevent OOM on persistent failures
MAX_BUFFER_SIZE = 10000

# Exceptions that should NOT trigger retries (permanent failures)
NON_RETRYABLE_EXCEPTIONS = (
    ValueError,  # Invalid data
    TypeError,   # Type errors
)


class InfluxWriter:
    """Async InfluxDB writer with batching and retry logic."""

    def __init__(self, settings: InfluxDBSettings) -> None:
        """Initialize InfluxDB writer.

        Args:
            settings: InfluxDB connection settings.
        """
        self._settings = settings
        self._client: InfluxDBClientAsync | None = None
        self._write_api: WriteApiAsync | None = None
        self._buffer: list[Point] = []
        self._buffer_lock = asyncio.Lock()
        self._flush_task: asyncio.Task | None = None
        self._running = False
        self._max_retries = 3
        self._retry_delay = 1.0  # seconds
        self._dropped_points = 0  # Counter for dropped points due to buffer overflow

    async def connect(self) -> None:
        """Connect to InfluxDB."""
        logger.info(
            "influxdb_connecting",
            url=self._settings.url,
            org=self._settings.org,
            bucket=self._settings.bucket,
        )

        self._client = InfluxDBClientAsync(
            url=self._settings.url,
            token=self._settings.token,
            org=self._settings.org,
        )

        # Verify connection
        ready = await self._client.ping()
        if not ready:
            raise ConnectionError("InfluxDB is not ready")

        self._write_api = self._client.write_api()
        self._running = True

        # Start background flush task
        self._flush_task = asyncio.create_task(self._periodic_flush())

        logger.info("influxdb_connected")

    async def disconnect(self) -> None:
        """Disconnect from InfluxDB, flushing any remaining data."""
        self._running = False

        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass

        # Final flush
        await self._flush()

        if self._client:
            await self._client.close()
            logger.info("influxdb_disconnected")

    async def write(self, points: Point | Sequence[Point]) -> None:
        """Add points to the write buffer.

        Args:
            points: Single point or sequence of points to write.
        """
        if isinstance(points, Point):
            points = [points]

        async with self._buffer_lock:
            self._buffer.extend(points)
            buffer_size = len(self._buffer)

        logger.debug("points_buffered", count=len(points), buffer_size=buffer_size)

        # Flush if buffer exceeds batch size
        if buffer_size >= self._settings.batch_size:
            await self._flush()

    async def _flush(self) -> None:
        """Flush buffered points to InfluxDB."""
        async with self._buffer_lock:
            if not self._buffer:
                return

            points_to_write = self._buffer.copy()
            self._buffer.clear()

        if not points_to_write:
            return

        # Retry logic with error type differentiation
        for attempt in range(self._max_retries):
            try:
                await self._write_batch(points_to_write)
                logger.info("points_written", count=len(points_to_write))
                return
            except NON_RETRYABLE_EXCEPTIONS as e:
                # Permanent failure - don't retry, don't re-add to buffer
                logger.error(
                    "write_failed_non_retryable",
                    count=len(points_to_write),
                    error=str(e),
                    error_type=type(e).__name__,
                )
                self._dropped_points += len(points_to_write)
                return
            except ApiException as e:
                # Check for auth errors (401, 403) - don't retry
                if e.status in (401, 403):
                    logger.error(
                        "write_failed_auth_error",
                        count=len(points_to_write),
                        status=e.status,
                        error=str(e),
                    )
                    self._dropped_points += len(points_to_write)
                    return
                # Other API errors - retry
                logger.warning(
                    "write_failed",
                    attempt=attempt + 1,
                    max_retries=self._max_retries,
                    status=e.status,
                    error=str(e),
                )
                if attempt < self._max_retries - 1:
                    await asyncio.sleep(self._retry_delay * (attempt + 1))
            except Exception as e:
                # Network/transient errors - retry
                logger.warning(
                    "write_failed",
                    attempt=attempt + 1,
                    max_retries=self._max_retries,
                    error=str(e),
                    error_type=type(e).__name__,
                )
                if attempt < self._max_retries - 1:
                    await asyncio.sleep(self._retry_delay * (attempt + 1))

        # All retries exhausted - try to re-add to buffer with overflow protection
        logger.error(
            "write_failed_permanently",
            count=len(points_to_write),
        )
        async with self._buffer_lock:
            new_size = len(self._buffer) + len(points_to_write)
            if new_size <= MAX_BUFFER_SIZE:
                self._buffer = points_to_write + self._buffer
                logger.warning(
                    "points_requeued",
                    count=len(points_to_write),
                    buffer_size=new_size,
                )
            else:
                # Buffer overflow - drop the oldest points
                overflow = new_size - MAX_BUFFER_SIZE
                self._dropped_points += overflow
                logger.error(
                    "buffer_overflow",
                    dropped=overflow,
                    buffer_size=MAX_BUFFER_SIZE,
                    total_dropped=self._dropped_points,
                )
                # Keep only newest points up to max buffer size
                combined = points_to_write + self._buffer
                self._buffer = combined[:MAX_BUFFER_SIZE]

    async def _write_batch(self, points: list[Point]) -> None:
        """Write a batch of points to InfluxDB.

        Args:
            points: List of points to write.
        """
        if not self._write_api:
            raise RuntimeError("InfluxDB client not connected")

        await self._write_api.write(
            bucket=self._settings.bucket,
            org=self._settings.org,
            record=points,
        )

    async def _periodic_flush(self) -> None:
        """Periodically flush the buffer."""
        interval = self._settings.flush_interval_ms / 1000.0

        while self._running:
            try:
                await asyncio.sleep(interval)
                await self._flush()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("periodic_flush_error", error=str(e))

    async def health_check(self) -> dict[str, Any]:
        """Check InfluxDB connection health.

        Returns:
            Dict with health status information.
        """
        if not self._client:
            return {"healthy": False, "error": "Not connected"}

        try:
            ready = await self._client.ping()
            async with self._buffer_lock:
                buffer_size = len(self._buffer)

            return {
                "healthy": ready,
                "buffer_size": buffer_size,
                "max_buffer_size": MAX_BUFFER_SIZE,
                "dropped_points": self._dropped_points,
                "url": self._settings.url,
                "bucket": self._settings.bucket,
            }
        except Exception as e:
            return {"healthy": False, "error": str(e)}


@asynccontextmanager
async def create_writer(settings: InfluxDBSettings):
    """Context manager for creating and managing an InfluxDB writer.

    Args:
        settings: InfluxDB connection settings.

    Yields:
        Connected InfluxWriter instance.
    """
    writer = InfluxWriter(settings)
    await writer.connect()
    try:
        yield writer
    finally:
        await writer.disconnect()
