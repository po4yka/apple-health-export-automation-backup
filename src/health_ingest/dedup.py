"""Deduplication cache for idempotent InfluxDB writes."""

import asyncio
import hashlib
import sqlite3
import threading
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any

import structlog
from influxdb_client import Point

logger = structlog.get_logger(__name__)


class DeduplicationCache:
    """Prevents duplicate InfluxDB writes using content-based hashing.

    Uses an in-memory LRU cache with optional SQLite persistence
    for restart recovery.
    """

    def __init__(
        self,
        max_size: int = 100_000,
        persist_path: Path | str | None = None,
        ttl_hours: int = 24,
    ) -> None:
        """Initialize deduplication cache.

        Args:
            max_size: Maximum number of entries to keep in cache.
            persist_path: SQLite path for persistence (None to disable).
            ttl_hours: Time-to-live for entries in hours.
        """
        self._max_size = max_size
        self._persist_path = Path(persist_path) if persist_path else None
        self._ttl_seconds = ttl_hours * 3600
        self._cache: OrderedDict[str, float] = OrderedDict()  # key -> timestamp
        self._lock = threading.Lock()
        self._db: sqlite3.Connection | None = None
        self._hits = 0
        self._misses = 0
        self._evictions = 0

    def compute_key(self, point: Point) -> str:
        """Compute deduplication key from an InfluxDB Point.

        Key is SHA256 of: measurement | sorted_tags | timestamp | sorted_fields

        Args:
            point: InfluxDB Point object.

        Returns:
            16-character hex hash as deduplication key.
        """
        # Access Point internals to build key
        # Note: Point stores data in _tags, _fields, _time, _name
        parts = []

        # Measurement name
        parts.append(point._name or "")

        # Sorted tags
        if point._tags:
            tag_str = "|".join(f"{k}={v}" for k, v in sorted(point._tags.items()))
            parts.append(tag_str)

        # Timestamp
        if point._time:
            parts.append(str(point._time))

        # Sorted fields
        if point._fields:
            field_str = "|".join(f"{k}={v}" for k, v in sorted(point._fields.items()))
            parts.append(field_str)

        content = "|".join(parts)
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def is_duplicate(self, point: Point) -> bool:
        """Check if point was already processed.

        Args:
            point: InfluxDB Point to check.

        Returns:
            True if point is a duplicate, False otherwise.
        """
        key = self.compute_key(point)
        now = time.time()

        with self._lock:
            if key in self._cache:
                ts = self._cache[key]
                # Check if entry has expired
                if now - ts < self._ttl_seconds:
                    self._hits += 1
                    # Move to end (most recently used)
                    self._cache.move_to_end(key)
                    return True
                # Expired - remove and treat as new
                del self._cache[key]

            self._misses += 1
            return False

    def mark_processed(self, point: Point) -> None:
        """Add point to cache.

        Args:
            point: InfluxDB Point that was successfully processed.
        """
        key = self.compute_key(point)
        now = time.time()

        with self._lock:
            if key in self._cache:
                # Update timestamp and move to end
                self._cache[key] = now
                self._cache.move_to_end(key)
            else:
                # Add new entry
                self._cache[key] = now

                # Evict oldest if over capacity
                while len(self._cache) > self._max_size:
                    self._cache.popitem(last=False)
                    self._evictions += 1

    def mark_processed_batch(self, points: list[Point]) -> None:
        """Add multiple points to cache efficiently.

        Args:
            points: List of InfluxDB Points that were successfully processed.
        """
        now = time.time()
        keys = [(self.compute_key(p), now) for p in points]

        with self._lock:
            for key, ts in keys:
                if key in self._cache:
                    self._cache[key] = ts
                    self._cache.move_to_end(key)
                else:
                    self._cache[key] = ts

            # Evict oldest if over capacity
            while len(self._cache) > self._max_size:
                self._cache.popitem(last=False)
                self._evictions += 1

    def filter_duplicates(self, points: list[Point]) -> list[Point]:
        """Filter out duplicate points from a list.

        Args:
            points: List of points to filter.

        Returns:
            List of non-duplicate points.
        """
        result = []
        for point in points:
            if not self.is_duplicate(point):
                result.append(point)
        return result

    async def checkpoint(self) -> None:
        """Persist cache to SQLite for restart recovery."""
        if not self._persist_path:
            return

        loop = asyncio.get_running_loop()

        def do_checkpoint() -> None:
            self._persist_path.parent.mkdir(parents=True, exist_ok=True)

            with sqlite3.connect(self._persist_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS dedup_cache (
                        key TEXT PRIMARY KEY,
                        timestamp REAL
                    )
                """)

                # Clear and repopulate
                conn.execute("DELETE FROM dedup_cache")

                with self._lock:
                    entries = list(self._cache.items())

                conn.executemany(
                    "INSERT INTO dedup_cache (key, timestamp) VALUES (?, ?)",
                    entries,
                )
                conn.commit()

            logger.debug("dedup_checkpoint_complete", entries=len(entries))

        await loop.run_in_executor(None, do_checkpoint)

    async def restore(self) -> int:
        """Restore cache from SQLite on startup.

        Returns:
            Number of entries restored.
        """
        if not self._persist_path or not self._persist_path.exists():
            return 0

        loop = asyncio.get_running_loop()

        def do_restore() -> int:
            now = time.time()
            restored = 0

            with sqlite3.connect(self._persist_path) as conn:
                cursor = conn.execute(
                    "SELECT key, timestamp FROM dedup_cache ORDER BY timestamp"
                )

                with self._lock:
                    for key, ts in cursor:
                        # Skip expired entries
                        if now - ts < self._ttl_seconds:
                            self._cache[key] = ts
                            restored += 1

            logger.info("dedup_restored", entries=restored)
            return restored

        return await loop.run_in_executor(None, do_restore)

    async def cleanup_expired(self) -> int:
        """Remove expired entries from cache.

        Returns:
            Number of entries removed.
        """
        now = time.time()
        removed = 0

        with self._lock:
            keys_to_remove = [
                key
                for key, ts in self._cache.items()
                if now - ts >= self._ttl_seconds
            ]
            for key in keys_to_remove:
                del self._cache[key]
                removed += 1

        if removed > 0:
            logger.debug("dedup_cleanup", removed=removed)

        return removed

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dict with cache metrics.
        """
        with self._lock:
            size = len(self._cache)

        total = self._hits + self._misses
        hit_rate = (self._hits / total * 100) if total > 0 else 0.0

        return {
            "size": size,
            "max_size": self._max_size,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate_pct": round(hit_rate, 2),
            "evictions": self._evictions,
            "ttl_hours": self._ttl_seconds / 3600,
            "persist_enabled": self._persist_path is not None,
        }

    def clear(self) -> None:
        """Clear all entries from cache."""
        with self._lock:
            self._cache.clear()
        logger.info("dedup_cache_cleared")
