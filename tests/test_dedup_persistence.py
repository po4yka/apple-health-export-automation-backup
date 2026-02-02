"""Tests for DeduplicationCache SQLite persistence: checkpoint, restore, and duplicate detection."""

import time
from pathlib import Path

from influxdb_client import Point

from health_ingest.dedup import DeduplicationCache


def _make_point(measurement: str = "test", source: str = "a", value: float = 1.0) -> Point:
    """Helper to create InfluxDB Point objects for testing."""
    return Point(measurement).tag("source", source).field("value", value)


class TestCheckpointSaves:
    """Tests that checkpoint() persists the in-memory cache to SQLite."""

    async def test_checkpoint_creates_db_file(self, tmp_path: Path):
        """checkpoint() creates the SQLite file at the configured path."""
        db_path = tmp_path / "dedup.db"
        cache = DeduplicationCache(max_size=100, persist_path=db_path)

        cache.mark_processed(_make_point("hr", "watch", 72.0))
        await cache.checkpoint()

        assert db_path.exists()

    async def test_checkpoint_persists_all_entries(self, tmp_path: Path):
        """checkpoint() writes every cache entry to SQLite."""
        db_path = tmp_path / "dedup.db"
        cache = DeduplicationCache(max_size=100, persist_path=db_path)

        points = [_make_point(f"m{i}", "src", float(i)) for i in range(10)]
        for p in points:
            cache.mark_processed(p)

        await cache.checkpoint()

        # Verify by restoring into a fresh cache
        cache2 = DeduplicationCache(max_size=100, persist_path=db_path)
        restored = await cache2.restore()

        assert restored == 10

    async def test_checkpoint_with_no_persist_path_is_noop(self):
        """checkpoint() does nothing when persist_path is None."""
        cache = DeduplicationCache(max_size=100, persist_path=None)
        cache.mark_processed(_make_point())

        # Should not raise
        await cache.checkpoint()

    async def test_checkpoint_overwrites_previous(self, tmp_path: Path):
        """A second checkpoint() replaces the previous contents atomically."""
        db_path = tmp_path / "dedup.db"
        cache = DeduplicationCache(max_size=100, persist_path=db_path)

        # First checkpoint: 3 entries
        for i in range(3):
            cache.mark_processed(_make_point(f"m{i}", "a", float(i)))
        await cache.checkpoint()

        # Add more entries, then checkpoint again
        for i in range(3, 7):
            cache.mark_processed(_make_point(f"m{i}", "a", float(i)))
        await cache.checkpoint()

        # Restore should have all 7
        cache2 = DeduplicationCache(max_size=100, persist_path=db_path)
        restored = await cache2.restore()

        assert restored == 7


class TestRestoreLoads:
    """Tests that restore() loads the cache from SQLite."""

    async def test_restore_populates_cache(self, tmp_path: Path):
        """restore() loads entries from SQLite into the in-memory cache."""
        db_path = tmp_path / "dedup.db"
        cache1 = DeduplicationCache(max_size=100, persist_path=db_path)

        points = [_make_point(f"m{i}", "src", float(i)) for i in range(5)]
        for p in points:
            cache1.mark_processed(p)
        await cache1.checkpoint()

        cache2 = DeduplicationCache(max_size=100, persist_path=db_path)
        restored = await cache2.restore()

        assert restored == 5
        stats = cache2.get_stats()
        assert stats["size"] == 5

    async def test_restore_returns_zero_when_no_file(self, tmp_path: Path):
        """restore() returns 0 when the SQLite file does not exist."""
        db_path = tmp_path / "nonexistent.db"
        cache = DeduplicationCache(max_size=100, persist_path=db_path)

        restored = await cache.restore()

        assert restored == 0

    async def test_restore_returns_zero_without_persist_path(self):
        """restore() returns 0 when persist_path is None."""
        cache = DeduplicationCache(max_size=100, persist_path=None)

        restored = await cache.restore()

        assert restored == 0

    async def test_restore_respects_max_size(self, tmp_path: Path):
        """restore() only loads up to max_size newest entries."""
        db_path = tmp_path / "dedup.db"

        # Save 10 entries
        cache1 = DeduplicationCache(max_size=100, persist_path=db_path)
        for i in range(10):
            cache1.mark_processed(_make_point(f"m{i}", "a", float(i)))
        await cache1.checkpoint()

        # Restore with max_size=5 -- should only load 5 newest
        cache2 = DeduplicationCache(max_size=5, persist_path=db_path)
        restored = await cache2.restore()

        assert restored == 5
        assert cache2.get_stats()["size"] == 5

    async def test_restore_skips_expired_entries(self, tmp_path: Path):
        """restore() does not load entries whose TTL has expired."""
        db_path = tmp_path / "dedup.db"
        cache1 = DeduplicationCache(max_size=100, persist_path=db_path, ttl_hours=0)
        cache1._ttl_seconds = 0.1

        cache1.mark_processed(_make_point("old", "a", 1.0))
        await cache1.checkpoint()

        # Wait for TTL expiration
        time.sleep(0.15)

        cache2 = DeduplicationCache(max_size=100, persist_path=db_path, ttl_hours=0)
        cache2._ttl_seconds = 0.1
        restored = await cache2.restore()

        assert restored == 0


class TestRestoredCacheDuplicateDetection:
    """Tests that a restored cache correctly identifies duplicates."""

    async def test_restored_cache_detects_duplicates(self, tmp_path: Path):
        """Points marked before checkpoint are detected as duplicates after restore."""
        db_path = tmp_path / "dedup.db"
        cache1 = DeduplicationCache(max_size=100, persist_path=db_path)

        p1 = _make_point("heart_rate", "watch", 72.0)
        p2 = _make_point("steps", "phone", 1000.0)
        p3 = _make_point("weight", "scale", 75.5)

        cache1.mark_processed(p1)
        cache1.mark_processed(p2)
        cache1.mark_processed(p3)
        await cache1.checkpoint()

        # New cache instance, restore from disk
        cache2 = DeduplicationCache(max_size=100, persist_path=db_path)
        await cache2.restore()

        # All previously processed points should be detected as duplicates
        assert cache2.is_duplicate(p1) is True
        assert cache2.is_duplicate(p2) is True
        assert cache2.is_duplicate(p3) is True

    async def test_restored_cache_allows_new_points(self, tmp_path: Path):
        """Points not in the checkpointed data are not detected as duplicates."""
        db_path = tmp_path / "dedup.db"
        cache1 = DeduplicationCache(max_size=100, persist_path=db_path)

        cache1.mark_processed(_make_point("heart_rate", "watch", 72.0))
        await cache1.checkpoint()

        cache2 = DeduplicationCache(max_size=100, persist_path=db_path)
        await cache2.restore()

        # A new, different point should not be a duplicate
        new_point = _make_point("heart_rate", "watch", 80.0)
        assert cache2.is_duplicate(new_point) is False

    async def test_filter_duplicates_after_restore(self, tmp_path: Path):
        """filter_duplicates correctly filters using restored cache state."""
        db_path = tmp_path / "dedup.db"
        cache1 = DeduplicationCache(max_size=100, persist_path=db_path)

        existing = _make_point("hr", "watch", 72.0)
        cache1.mark_processed(existing)
        await cache1.checkpoint()

        cache2 = DeduplicationCache(max_size=100, persist_path=db_path)
        await cache2.restore()

        # Mix of known and new points
        new_point = _make_point("hr", "watch", 80.0)
        points = [existing, new_point, existing]

        filtered = cache2.filter_duplicates(points)

        # Only new_point should survive
        assert len(filtered) == 1
        assert cache2.compute_key(filtered[0]) == cache2.compute_key(new_point)

    async def test_checkpoint_restore_roundtrip_preserves_keys(self, tmp_path: Path):
        """compute_key produces the same key before and after a checkpoint/restore cycle."""
        db_path = tmp_path / "dedup.db"
        cache1 = DeduplicationCache(max_size=100, persist_path=db_path)

        point = _make_point("activity", "phone", 5000.0)
        key_before = cache1.compute_key(point)
        cache1.mark_processed(point)
        await cache1.checkpoint()

        cache2 = DeduplicationCache(max_size=100, persist_path=db_path)
        await cache2.restore()

        key_after = cache2.compute_key(point)
        assert key_before == key_after
        assert cache2.is_duplicate(point) is True
