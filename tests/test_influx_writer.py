"""Tests for InfluxWriter buffering behavior."""

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
