"""Tests for the dead-letter queue."""

import json
import tempfile
from pathlib import Path

import pytest

from health_ingest.dlq import DeadLetterQueue, DLQCategory


@pytest.fixture
def db_path():
    """Create temporary database path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir) / "dlq.db"


@pytest.fixture
def dlq(db_path):
    """Create a dead-letter queue."""
    return DeadLetterQueue(db_path, max_entries=100, retention_days=30, max_retries=3)


class TestDeadLetterQueue:
    """Tests for DeadLetterQueue class."""

    @pytest.mark.asyncio
    async def test_enqueue_creates_entry(self, dlq):
        """Test that enqueue creates a new entry."""
        topic = "health/export/heart"
        payload = b'{"name": "heart_rate", "qty": 72}'
        error = ValueError("Invalid value")

        entry_id = await dlq.enqueue(
            category=DLQCategory.VALIDATION_ERROR,
            topic=topic,
            payload=payload,
            error=error,
        )

        assert len(entry_id) == 16

        entry = await dlq.get_entry(entry_id)
        assert entry is not None
        assert entry.topic == topic
        assert entry.category == DLQCategory.VALIDATION_ERROR
        assert entry.payload == payload
        assert "Invalid value" in entry.error_message
        assert entry.retry_count == 0

    @pytest.mark.asyncio
    async def test_enqueue_with_archive_id(self, dlq):
        """Test that archive_id is stored with entry."""
        entry_id = await dlq.enqueue(
            category=DLQCategory.JSON_PARSE_ERROR,
            topic="topic",
            payload=b"invalid json",
            error=json.JSONDecodeError("test", "doc", 0),
            archive_id="archive123",
        )

        entry = await dlq.get_entry(entry_id)
        assert entry.archive_id == "archive123"

    @pytest.mark.asyncio
    async def test_get_entries_returns_all(self, dlq):
        """Test getting all entries."""
        for i in range(3):
            await dlq.enqueue(
                category=DLQCategory.TRANSFORM_ERROR,
                topic=f"topic{i}",
                payload=f'{{"id": {i}}}'.encode(),
                error=Exception(f"Error {i}"),
            )

        entries = await dlq.get_entries()

        assert len(entries) == 3

    @pytest.mark.asyncio
    async def test_get_entries_filter_by_category(self, dlq):
        """Test filtering entries by category."""
        await dlq.enqueue(
            category=DLQCategory.JSON_PARSE_ERROR,
            topic="topic1",
            payload=b"{}",
            error=Exception("parse"),
        )
        await dlq.enqueue(
            category=DLQCategory.TRANSFORM_ERROR,
            topic="topic2",
            payload=b"{}",
            error=Exception("transform"),
        )

        parse_entries = await dlq.get_entries(category=DLQCategory.JSON_PARSE_ERROR)
        transform_entries = await dlq.get_entries(category=DLQCategory.TRANSFORM_ERROR)

        assert len(parse_entries) == 1
        assert len(transform_entries) == 1

    @pytest.mark.asyncio
    async def test_get_entries_respects_limit(self, dlq):
        """Test limit parameter."""
        for i in range(10):
            await dlq.enqueue(
                category=DLQCategory.WRITE_ERROR,
                topic=f"topic{i}",
                payload=b"{}",
                error=Exception(f"Error {i}"),
            )

        entries = await dlq.get_entries(limit=5)

        assert len(entries) == 5

    @pytest.mark.asyncio
    async def test_delete_entry(self, dlq):
        """Test deleting an entry."""
        entry_id = await dlq.enqueue(
            category=DLQCategory.UNKNOWN_ERROR,
            topic="topic",
            payload=b"{}",
            error=Exception("test"),
        )

        deleted = await dlq.delete_entry(entry_id)
        assert deleted is True

        entry = await dlq.get_entry(entry_id)
        assert entry is None

    @pytest.mark.asyncio
    async def test_replay_entry_success(self, dlq):
        """Test successful replay of an entry."""
        payload = {"name": "heart_rate", "qty": 72}
        entry_id = await dlq.enqueue(
            category=DLQCategory.WRITE_ERROR,
            topic="topic",
            payload=json.dumps(payload).encode(),
            error=Exception("write failed"),
        )

        processed = []

        async def callback(topic, data):
            processed.append((topic, data))

        success = await dlq.replay_entry(entry_id, callback)

        assert success is True
        assert len(processed) == 1
        assert processed[0] == ("topic", payload)

        # Entry should be deleted after successful replay
        entry = await dlq.get_entry(entry_id)
        assert entry is None

    @pytest.mark.asyncio
    async def test_replay_entry_failure_increments_retry(self, dlq):
        """Test that failed replay increments retry count."""
        entry_id = await dlq.enqueue(
            category=DLQCategory.TRANSFORM_ERROR,
            topic="topic",
            payload=b'{"data": true}',
            error=Exception("transform error"),
        )

        async def failing_callback(topic, data):
            raise Exception("Still failing")

        success = await dlq.replay_entry(entry_id, failing_callback)

        assert success is False

        entry = await dlq.get_entry(entry_id)
        assert entry.retry_count == 1

    @pytest.mark.asyncio
    async def test_replay_entry_max_retries(self, db_path):
        """Test that replay respects max retries."""
        dlq = DeadLetterQueue(db_path, max_retries=2)

        entry_id = await dlq.enqueue(
            category=DLQCategory.WRITE_ERROR,
            topic="topic",
            payload=b'{"data": true}',
            error=Exception("error"),
        )

        async def failing_callback(topic, data):
            raise Exception("Still failing")

        # Exhaust retries
        await dlq.replay_entry(entry_id, failing_callback)
        await dlq.replay_entry(entry_id, failing_callback)

        # Third attempt should be blocked
        success = await dlq.replay_entry(entry_id, failing_callback)
        assert success is False

        entry = await dlq.get_entry(entry_id)
        assert entry.retry_count == 2

    @pytest.mark.asyncio
    async def test_replay_category(self, dlq):
        """Test replaying all entries in a category."""
        for i in range(3):
            await dlq.enqueue(
                category=DLQCategory.WRITE_ERROR,
                topic=f"topic{i}",
                payload=json.dumps({"id": i}).encode(),
                error=Exception("write error"),
            )

        processed = []

        async def callback(topic, data):
            processed.append(data["id"])

        success, failure = await dlq.replay_category(DLQCategory.WRITE_ERROR, callback)

        assert success == 3
        assert failure == 0
        assert sorted(processed) == [0, 1, 2]

    @pytest.mark.asyncio
    async def test_get_stats(self, dlq):
        """Test statistics reporting."""
        await dlq.enqueue(
            category=DLQCategory.JSON_PARSE_ERROR,
            topic="topic1",
            payload=b"bad json",
            error=Exception("parse"),
        )
        await dlq.enqueue(
            category=DLQCategory.JSON_PARSE_ERROR,
            topic="topic2",
            payload=b"bad json 2",
            error=Exception("parse 2"),
        )
        await dlq.enqueue(
            category=DLQCategory.TRANSFORM_ERROR,
            topic="topic3",
            payload=b"{}",
            error=Exception("transform"),
        )

        stats = await dlq.get_stats()

        assert stats["total_entries"] == 3
        assert stats["by_category"]["json_parse_error"] == 2
        assert stats["by_category"]["transform_error"] == 1
        assert stats["total_enqueued"] == 3

    @pytest.mark.asyncio
    async def test_clear(self, dlq):
        """Test clearing all entries."""
        for i in range(5):
            await dlq.enqueue(
                category=DLQCategory.UNKNOWN_ERROR,
                topic=f"topic{i}",
                payload=b"{}",
                error=Exception("error"),
            )

        count = await dlq.clear()

        assert count == 5

        entries = await dlq.get_entries()
        assert len(entries) == 0

    @pytest.mark.asyncio
    async def test_max_entries_eviction(self, db_path):
        """Test that oldest entries are evicted when max is reached."""
        dlq = DeadLetterQueue(db_path, max_entries=5)

        # Create 10 entries
        for i in range(10):
            await dlq.enqueue(
                category=DLQCategory.WRITE_ERROR,
                topic=f"topic{i}",
                payload=b"{}",
                error=Exception(f"error {i}"),
            )

        entries = await dlq.get_entries()

        # Should only have 5 entries (newest)
        assert len(entries) == 5

    @pytest.mark.asyncio
    async def test_entry_to_dict(self, dlq):
        """Test DLQEntry.to_dict()."""
        entry_id = await dlq.enqueue(
            category=DLQCategory.VALIDATION_ERROR,
            topic="test/topic",
            payload=b'{"key": "value"}',
            error=ValueError("test error"),
            archive_id="arch123",
        )

        entry = await dlq.get_entry(entry_id)
        d = entry.to_dict()

        assert d["id"] == entry_id
        assert d["category"] == "validation_error"
        assert d["topic"] == "test/topic"
        assert d["payload_size"] == len(b'{"key": "value"}')
        assert d["archive_id"] == "arch123"
        assert d["retry_count"] == 0

    @pytest.mark.asyncio
    async def test_error_traceback_stored(self, dlq):
        """Test that error traceback is stored."""
        try:

            def inner():
                raise ValueError("inner error")

            inner()
        except ValueError as e:
            entry_id = await dlq.enqueue(
                category=DLQCategory.TRANSFORM_ERROR,
                topic="topic",
                payload=b"{}",
                error=e,
            )

        entry = await dlq.get_entry(entry_id)

        assert entry.error_traceback is not None
        assert "inner error" in entry.error_traceback
        assert "inner()" in entry.error_traceback
