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
        self._pending: OrderedDict[str, float] = OrderedDict()  # key -> reservation timestamp
        # Keep reservation timeout short to avoid indefinitely blocking keys if a worker dies.
        self._pending_ttl_seconds = min(300.0, max(60.0, self._ttl_seconds))
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
        return self._is_duplicate_key(key)

    def _is_duplicate_key(self, key: str) -> bool:
        """Check duplicate status by precomputed key.

        The lock ensures atomicity of the TTL check and cache update:
        without it, a concurrent thread could read a stale entry between
        the expiry check and the deletion, leading to false positives.
        """
        now = time.time()

        # Lock protects TTL check + cache mutation as an atomic unit.
        with self._lock:
            self._cleanup_pending_locked(now)

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

            if key in self._pending:
                self._hits += 1
                self._pending.move_to_end(key)
                return True

            self._misses += 1
            return False

    def _cleanup_pending_locked(self, now: float) -> int:
        """Drop stale in-flight reservations while holding _lock."""
        removed = 0
        stale_keys = [k for k, ts in self._pending.items() if now - ts >= self._pending_ttl_seconds]
        for key in stale_keys:
            del self._pending[key]
            removed += 1
        return removed

    def reserve_batch(self, points: list[Point]) -> tuple[list[Point], list[str]]:
        """Atomically reserve non-duplicate points for processing.

        Returns:
            Tuple of (points_to_process, reservation_keys).
        """
        now = time.time()
        keyed_points = [(point, self.compute_key(point)) for point in points]
        selected_points: list[Point] = []
        reservation_keys: list[str] = []
        seen_batch: set[str] = set()

        with self._lock:
            self._cleanup_pending_locked(now)

            for point, key in keyed_points:
                if key in seen_batch:
                    self._hits += 1
                    continue
                seen_batch.add(key)

                cache_ts = self._cache.get(key)
                if cache_ts is not None:
                    if now - cache_ts < self._ttl_seconds:
                        self._hits += 1
                        self._cache.move_to_end(key)
                        continue
                    del self._cache[key]

                if key in self._pending:
                    self._hits += 1
                    self._pending.move_to_end(key)
                    continue

                self._pending[key] = now
                selected_points.append(point)
                reservation_keys.append(key)
                self._misses += 1

        return selected_points, reservation_keys

    def commit_batch(self, reservation_keys: list[str]) -> None:
        """Commit successfully processed reservations into the dedup cache."""
        now = time.time()

        with self._lock:
            for key in reservation_keys:
                self._pending.pop(key, None)
                self._cache[key] = now
                self._cache.move_to_end(key)

            while len(self._cache) > self._max_size:
                self._cache.popitem(last=False)
                self._evictions += 1

    def release_batch(self, reservation_keys: list[str]) -> None:
        """Release reservations after a failed processing attempt."""
        if not reservation_keys:
            return
        with self._lock:
            for key in reservation_keys:
                self._pending.pop(key, None)

    def mark_processed(self, point: Point) -> None:
        """Add point to cache.

        Args:
            point: InfluxDB Point that was successfully processed.
        """
        key = self.compute_key(point)
        now = time.time()

        with self._lock:
            self._pending.pop(key, None)
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
                self._pending.pop(key, None)
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
        seen: set[str] = set()
        for point in points:
            key = self.compute_key(point)
            if key in seen:
                continue
            if not self._is_duplicate_key(key):
                seen.add(key)
                result.append(point)
        return result

    async def checkpoint(self) -> None:
        """Persist cache to SQLite for restart recovery."""
        if not self._persist_path:
            return

        # Snapshot under lock on the event loop thread (fast, no I/O)
        with self._lock:
            entries = list(self._cache.items())

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

                # Clear and repopulate atomically (within transaction)
                conn.execute("DELETE FROM dedup_cache")
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
                    "SELECT key, timestamp FROM dedup_cache ORDER BY timestamp DESC LIMIT ?",
                    (self._max_size,),
                )
                rows = list(cursor)

                with self._lock:
                    for key, ts in reversed(rows):
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
        removed_cache = 0

        with self._lock:
            keys_to_remove = [
                key for key, ts in self._cache.items() if now - ts >= self._ttl_seconds
            ]
            for key in keys_to_remove:
                del self._cache[key]
                removed_cache += 1

            removed_pending = self._cleanup_pending_locked(now)

        removed = removed_cache + removed_pending
        if removed > 0:
            logger.debug(
                "dedup_cleanup",
                removed_cache=removed_cache,
                removed_pending=removed_pending,
            )

        return removed

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dict with cache metrics.
        """
        with self._lock:
            size = len(self._cache)
            pending_size = len(self._pending)

        total = self._hits + self._misses
        hit_rate = (self._hits / total * 100) if total > 0 else 0.0

        return {
            "size": size,
            "max_size": self._max_size,
            "pending_size": pending_size,
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
            self._pending.clear()
        logger.info("dedup_cache_cleared")
