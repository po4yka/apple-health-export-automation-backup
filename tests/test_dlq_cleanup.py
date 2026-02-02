"""Tests for DLQ retention cleanup and max-entries eviction."""

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from health_ingest.dlq import DeadLetterQueue, DLQCategory


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Temporary SQLite database path."""
    return tmp_path / "dlq_cleanup.db"


class TestRetentionCleanup:
    """Tests that entries older than retention_days are cleaned up."""

    async def test_old_entries_cleaned_on_enqueue(self, db_path: Path):
        """Entries older than retention_days are removed when a new entry is enqueued."""
        dlq = DeadLetterQueue(db_path, max_entries=10_000, retention_days=1)

        # Enqueue an entry
        entry_id = await dlq.enqueue(
            category=DLQCategory.WRITE_ERROR,
            topic="topic/old",
            payload=b'{"old": true}',
            error=Exception("old error"),
        )

        # Manually backdate the entry to 2 days ago
        cutoff = (datetime.now() - timedelta(days=2)).isoformat()
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                "UPDATE dlq_entries SET created_at = ? WHERE id = ?",
                (cutoff, entry_id),
            )
            conn.commit()

        # Enqueue a new entry -- this triggers _cleanup_if_needed
        new_id = await dlq.enqueue(
            category=DLQCategory.WRITE_ERROR,
            topic="topic/new",
            payload=b'{"new": true}',
            error=Exception("new error"),
        )

        # The old entry should have been cleaned up
        old_entry = await dlq.get_entry(entry_id)
        assert old_entry is None

        # The new entry should still exist
        new_entry = await dlq.get_entry(new_id)
        assert new_entry is not None

    async def test_entries_within_retention_kept(self, db_path: Path):
        """Entries within the retention period are not removed."""
        dlq = DeadLetterQueue(db_path, max_entries=10_000, retention_days=30)

        entry_id = await dlq.enqueue(
            category=DLQCategory.TRANSFORM_ERROR,
            topic="topic/recent",
            payload=b'{"recent": true}',
            error=Exception("recent error"),
        )

        # Enqueue another entry to trigger cleanup
        await dlq.enqueue(
            category=DLQCategory.TRANSFORM_ERROR,
            topic="topic/trigger",
            payload=b"{}",
            error=Exception("trigger"),
        )

        # The recent entry should still be present
        entry = await dlq.get_entry(entry_id)
        assert entry is not None


class TestMaxEntriesEviction:
    """Tests that oldest entries are evicted when max_entries is exceeded."""

    async def test_eviction_keeps_max_entries(self, db_path: Path):
        """When more than max_entries are added, only the newest max_entries remain."""
        dlq = DeadLetterQueue(db_path, max_entries=5, retention_days=365)

        ids = []
        for i in range(8):
            eid = await dlq.enqueue(
                category=DLQCategory.WRITE_ERROR,
                topic=f"topic/{i}",
                payload=f'{{"seq": {i}}}'.encode(),
                error=Exception(f"error {i}"),
            )
            ids.append(eid)

        entries = await dlq.get_entries(limit=100)
        assert len(entries) == 5

    async def test_eviction_removes_oldest_first(self, db_path: Path):
        """Eviction removes the oldest entries, keeping the newest."""
        dlq = DeadLetterQueue(db_path, max_entries=3, retention_days=365)

        # Create entries with distinct topics for identification
        first_id = await dlq.enqueue(
            category=DLQCategory.WRITE_ERROR,
            topic="topic/first",
            payload=b'{"order": 1}',
            error=Exception("first"),
        )
        await dlq.enqueue(
            category=DLQCategory.WRITE_ERROR,
            topic="topic/second",
            payload=b'{"order": 2}',
            error=Exception("second"),
        )
        await dlq.enqueue(
            category=DLQCategory.WRITE_ERROR,
            topic="topic/third",
            payload=b'{"order": 3}',
            error=Exception("third"),
        )
        # This fourth entry should cause eviction of the first
        await dlq.enqueue(
            category=DLQCategory.WRITE_ERROR,
            topic="topic/fourth",
            payload=b'{"order": 4}',
            error=Exception("fourth"),
        )

        # The first entry should have been evicted
        assert await dlq.get_entry(first_id) is None

        entries = await dlq.get_entries(limit=100)
        assert len(entries) == 3
        topics = {e.topic for e in entries}
        assert "topic/first" not in topics


class TestCategoryFiltering:
    """Tests that DLQ entries can be filtered by category."""

    async def test_filter_by_single_category(self, db_path: Path):
        """get_entries with category filter returns only matching entries."""
        dlq = DeadLetterQueue(db_path, max_entries=100, retention_days=30)

        await dlq.enqueue(
            category=DLQCategory.JSON_PARSE_ERROR,
            topic="topic/json",
            payload=b"bad json",
            error=Exception("parse"),
        )
        await dlq.enqueue(
            category=DLQCategory.TRANSFORM_ERROR,
            topic="topic/transform",
            payload=b"{}",
            error=Exception("transform"),
        )
        await dlq.enqueue(
            category=DLQCategory.JSON_PARSE_ERROR,
            topic="topic/json2",
            payload=b"also bad",
            error=Exception("parse 2"),
        )

        json_errors = await dlq.get_entries(category=DLQCategory.JSON_PARSE_ERROR)
        transform_errors = await dlq.get_entries(category=DLQCategory.TRANSFORM_ERROR)

        assert len(json_errors) == 2
        assert len(transform_errors) == 1
        assert all(e.category == DLQCategory.JSON_PARSE_ERROR for e in json_errors)
        assert all(e.category == DLQCategory.TRANSFORM_ERROR for e in transform_errors)

    async def test_filter_empty_category_returns_nothing(self, db_path: Path):
        """Filtering by a category with no entries returns an empty list."""
        dlq = DeadLetterQueue(db_path, max_entries=100, retention_days=30)

        await dlq.enqueue(
            category=DLQCategory.WRITE_ERROR,
            topic="topic",
            payload=b"{}",
            error=Exception("error"),
        )

        validation_errors = await dlq.get_entries(category=DLQCategory.VALIDATION_ERROR)

        assert len(validation_errors) == 0

    async def test_no_category_filter_returns_all(self, db_path: Path):
        """get_entries without category filter returns all entries."""
        dlq = DeadLetterQueue(db_path, max_entries=100, retention_days=30)

        categories = (
            DLQCategory.JSON_PARSE_ERROR,
            DLQCategory.TRANSFORM_ERROR,
            DLQCategory.WRITE_ERROR,
        )
        for cat in categories:
            await dlq.enqueue(
                category=cat,
                topic="topic",
                payload=b"{}",
                error=Exception("error"),
            )

        all_entries = await dlq.get_entries()

        assert len(all_entries) == 3

    async def test_stats_reflect_category_counts(self, db_path: Path):
        """get_stats returns accurate per-category counts."""
        dlq = DeadLetterQueue(db_path, max_entries=100, retention_days=30)

        for _ in range(3):
            await dlq.enqueue(
                category=DLQCategory.WRITE_ERROR,
                topic="topic",
                payload=b"{}",
                error=Exception("e"),
            )
        for _ in range(2):
            await dlq.enqueue(
                category=DLQCategory.VALIDATION_ERROR,
                topic="topic",
                payload=b"{}",
                error=Exception("e"),
            )

        stats = await dlq.get_stats()

        assert stats["total_entries"] == 5
        assert stats["by_category"]["write_error"] == 3
        assert stats["by_category"]["validation_error"] == 2
