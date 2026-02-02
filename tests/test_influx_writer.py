"""Tests for InfluxWriter buffering behavior."""

import asyncio

import pytest
from influxdb_client import Point

import health_ingest.influx_writer as influx_writer
from health_ingest.config import InfluxDBSettings
from health_ingest.influx_writer import InfluxWriter


@pytest.mark.asyncio
async def test_overflow_keeps_newest_points(monkeypatch):
    """Ensure buffer overflow retains newest points when requeueing."""
    monkeypatch.setattr(influx_writer, "MAX_BUFFER_SIZE", 3)

    settings = InfluxDBSettings(token="test-token")
    writer = InfluxWriter(settings)
    writer._max_retries = 1
    writer._retry_delay = 0

    points = [Point("m").field("v", i) for i in range(5)]

    async with writer._buffer_lock:
        writer._buffer = points.copy()

    async def fail_write(_points):
        raise RuntimeError("write failed")

    monkeypatch.setattr(writer, "_write_batch", fail_write)

    await writer._flush()

    async with writer._buffer_lock:
        buffered = writer._buffer.copy()

    assert len(buffered) == 3
    assert [p._fields["v"] for p in buffered] == [2, 3, 4]
    assert writer._dropped_points == 2


@pytest.mark.asyncio
async def test_write_timeout_requeues_points(monkeypatch):
    """Points are requeued when write times out."""
    settings = InfluxDBSettings(token="test-token", write_timeout_seconds=0.01)
    writer = InfluxWriter(settings)
    writer._max_retries = 1
    writer._retry_delay = 0

    points = [Point("m").field("v", i) for i in range(3)]

    async with writer._buffer_lock:
        writer._buffer = points.copy()

    async def slow_write(_points):
        await asyncio.sleep(5.0)

    monkeypatch.setattr(writer, "_write_batch", slow_write)

    await writer._flush()

    async with writer._buffer_lock:
        buffered = writer._buffer.copy()

    # Points should be requeued after timeout (not dropped)
    assert len(buffered) == 3


@pytest.mark.asyncio
async def test_non_retryable_drops_points(monkeypatch):
    """Non-retryable errors cause points to be dropped."""
    settings = InfluxDBSettings(token="test-token")
    writer = InfluxWriter(settings)
    writer._max_retries = 3
    writer._retry_delay = 0

    points = [Point("m").field("v", i) for i in range(2)]

    async with writer._buffer_lock:
        writer._buffer = points.copy()

    async def bad_write(_points):
        raise ValueError("invalid data")

    monkeypatch.setattr(writer, "_write_batch", bad_write)

    await writer._flush()

    # Points should be dropped (not requeued)
    async with writer._buffer_lock:
        assert len(writer._buffer) == 0
    assert writer._dropped_points == 2


@pytest.mark.asyncio
async def test_on_drop_callback_invoked_on_non_retryable(monkeypatch):
    """on_drop callback receives points and reason on non-retryable errors."""
    drop_calls: list[tuple] = []

    async def drop_cb(pts, reason):
        drop_calls.append((pts, reason))

    settings = InfluxDBSettings(token="test-token")
    writer = InfluxWriter(settings, on_drop=drop_cb)
    writer._max_retries = 1
    writer._retry_delay = 0

    points = [Point("m").field("v", i) for i in range(2)]

    async with writer._buffer_lock:
        writer._buffer = points.copy()

    async def bad_write(_points):
        raise ValueError("bad data")

    monkeypatch.setattr(writer, "_write_batch", bad_write)

    await writer._flush()

    assert len(drop_calls) == 1
    assert len(drop_calls[0][0]) == 2
    assert "bad data" in drop_calls[0][1]


@pytest.mark.asyncio
async def test_graceful_shutdown_flushes_while_running(monkeypatch):
    """disconnect() flushes while _running is True, then sets it to False."""
    settings = InfluxDBSettings(token="test-token")
    writer = InfluxWriter(settings)
    writer._running = True

    running_during_flush: list[bool] = []

    original_flush = writer._flush

    async def capture_running_flush():
        running_during_flush.append(writer._running)
        await original_flush()

    monkeypatch.setattr(writer, "_flush", capture_running_flush)

    await writer.disconnect()

    assert running_during_flush == [True]
    assert writer._running is False
