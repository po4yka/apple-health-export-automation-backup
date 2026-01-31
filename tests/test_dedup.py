"""Tests for the deduplication cache."""

import tempfile
import time
from pathlib import Path

import pytest
from influxdb_client import Point

from health_ingest.dedup import DeduplicationCache


@pytest.fixture
def cache():
    """Create a deduplication cache."""
    return DeduplicationCache(max_size=100, ttl_hours=1)


@pytest.fixture
def persist_path():
    """Create temporary persistence path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir) / "cache.db"


def create_point(measurement="test", tag_value="a", field_value=1.0, ts=None):
    """Helper to create test points."""
    point = Point(measurement).tag("source", tag_value).field("value", field_value)
    if ts:
        point.time(ts)
    return point


class TestDeduplicationCache:
    """Tests for DeduplicationCache class."""

    def test_compute_key_deterministic(self, cache):
        """Test that compute_key returns consistent results."""
        point = create_point("heart_rate", "watch", 72.0)

        key1 = cache.compute_key(point)
        key2 = cache.compute_key(point)

        assert key1 == key2
        assert len(key1) == 16  # SHA256[:16]

    def test_compute_key_different_for_different_points(self, cache):
        """Test that different points produce different keys."""
        point1 = create_point("heart_rate", "watch", 72.0)
        point2 = create_point("heart_rate", "watch", 73.0)
        point3 = create_point("heart_rate", "phone", 72.0)

        key1 = cache.compute_key(point1)
        key2 = cache.compute_key(point2)
        key3 = cache.compute_key(point3)

        assert len({key1, key2, key3}) == 3

    def test_is_duplicate_returns_false_for_new(self, cache):
        """Test that new points are not duplicates."""
        point = create_point()

        assert cache.is_duplicate(point) is False

    def test_is_duplicate_returns_true_after_mark(self, cache):
        """Test that marked points are duplicates."""
        point = create_point()

        cache.mark_processed(point)

        assert cache.is_duplicate(point) is True

    def test_mark_processed_batch(self, cache):
        """Test batch marking of points."""
        points = [
            create_point("m1", "a", 1.0),
            create_point("m2", "b", 2.0),
            create_point("m3", "c", 3.0),
        ]

        cache.mark_processed_batch(points)

        for point in points:
            assert cache.is_duplicate(point) is True

    def test_filter_duplicates(self, cache):
        """Test filtering duplicates from a list."""
        points = [
            create_point("m1", "a", 1.0),
            create_point("m2", "b", 2.0),
            create_point("m1", "a", 1.0),  # duplicate of first
        ]

        # Mark first point as already processed
        cache.mark_processed(points[0])

        # After filtering:
        # - points[0] is duplicate (already in cache) - filtered out
        # - points[1] is new - kept
        # - points[2] is also duplicate (same key as points[0]) - filtered out
        filtered = cache.filter_duplicates(points)

        assert len(filtered) == 1
        assert cache.compute_key(filtered[0]) == cache.compute_key(points[1])

    def test_lru_eviction(self):
        """Test LRU eviction when max size is reached."""
        cache = DeduplicationCache(max_size=3)

        points = [create_point(f"m{i}", "a", float(i)) for i in range(5)]

        for point in points:
            cache.mark_processed(point)

        # Only last 3 should be in cache
        assert cache.is_duplicate(points[0]) is False  # evicted
        assert cache.is_duplicate(points[1]) is False  # evicted
        assert cache.is_duplicate(points[2]) is True
        assert cache.is_duplicate(points[3]) is True
        assert cache.is_duplicate(points[4]) is True

    def test_ttl_expiration(self):
        """Test that entries expire after TTL."""
        # Use very short TTL for testing
        cache = DeduplicationCache(max_size=100, ttl_hours=0)
        cache._ttl_seconds = 0.1  # 100ms for testing

        point = create_point()
        cache.mark_processed(point)

        assert cache.is_duplicate(point) is True

        # Wait for expiration
        time.sleep(0.15)

        assert cache.is_duplicate(point) is False

    def test_get_stats(self, cache):
        """Test statistics reporting."""
        point1 = create_point("m1", "a", 1.0)
        point2 = create_point("m2", "b", 2.0)

        cache.mark_processed(point1)
        cache.is_duplicate(point1)  # hit
        cache.is_duplicate(point2)  # miss

        stats = cache.get_stats()

        assert stats["size"] == 1
        assert stats["hits"] == 1
        assert stats["misses"] == 1
        assert stats["hit_rate_pct"] == 50.0

    @pytest.mark.asyncio
    async def test_checkpoint_and_restore(self, persist_path):
        """Test persisting and restoring cache."""
        cache1 = DeduplicationCache(max_size=100, persist_path=persist_path)

        points = [create_point(f"m{i}", "a", float(i)) for i in range(5)]
        for point in points:
            cache1.mark_processed(point)

        await cache1.checkpoint()

        # Create new cache and restore
        cache2 = DeduplicationCache(max_size=100, persist_path=persist_path)
        restored = await cache2.restore()

        assert restored == 5

        # All points should be duplicates
        for point in points:
            assert cache2.is_duplicate(point) is True

    @pytest.mark.asyncio
    async def test_restore_skips_expired(self, persist_path):
        """Test that restore skips expired entries."""
        cache1 = DeduplicationCache(max_size=100, persist_path=persist_path, ttl_hours=0)
        cache1._ttl_seconds = 0.1

        point = create_point()
        cache1.mark_processed(point)
        await cache1.checkpoint()

        time.sleep(0.15)

        cache2 = DeduplicationCache(max_size=100, persist_path=persist_path, ttl_hours=0)
        cache2._ttl_seconds = 0.1
        restored = await cache2.restore()

        assert restored == 0

    @pytest.mark.asyncio
    async def test_restore_respects_max_size(self, persist_path):
        """Test that restore only loads up to max_size newest entries."""
        cache1 = DeduplicationCache(max_size=10, persist_path=persist_path)

        points = [create_point(f"m{i}", "a", float(i)) for i in range(5)]
        for point in points:
            cache1.mark_processed(point)

        await cache1.checkpoint()

        cache2 = DeduplicationCache(max_size=3, persist_path=persist_path)
        restored = await cache2.restore()

        assert restored == 3
        assert cache2.is_duplicate(points[0]) is False
        assert cache2.is_duplicate(points[1]) is False
        assert cache2.is_duplicate(points[2]) is True
        assert cache2.is_duplicate(points[3]) is True
        assert cache2.is_duplicate(points[4]) is True

    @pytest.mark.asyncio
    async def test_cleanup_expired(self):
        """Test cleanup of expired entries."""
        cache = DeduplicationCache(max_size=100, ttl_hours=0)
        cache._ttl_seconds = 0.1

        points = [create_point(f"m{i}", "a", float(i)) for i in range(5)]
        for point in points:
            cache.mark_processed(point)

        time.sleep(0.15)

        removed = await cache.cleanup_expired()

        assert removed == 5
        assert cache.get_stats()["size"] == 0

    def test_clear(self, cache):
        """Test clearing the cache."""
        points = [create_point(f"m{i}", "a", float(i)) for i in range(5)]
        for point in points:
            cache.mark_processed(point)

        cache.clear()

        for point in points:
            assert cache.is_duplicate(point) is False

    def test_concurrent_access(self, cache):
        """Test thread safety of cache operations."""
        import concurrent.futures

        points = [create_point(f"m{i}", "a", float(i)) for i in range(100)]

        def mark_and_check(point):
            cache.mark_processed(point)
            return cache.is_duplicate(point)

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(mark_and_check, points))

        assert all(results)
