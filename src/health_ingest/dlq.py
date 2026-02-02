"""Dead-letter queue for failed message handling."""

import asyncio
import json
import sqlite3
import traceback
import zlib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class DLQCategory(str, Enum):
    """Categories of DLQ entries for error classification."""

    JSON_PARSE_ERROR = "json_parse_error"
    UNICODE_DECODE_ERROR = "unicode_decode_error"
    VALIDATION_ERROR = "validation_error"
    TRANSFORM_ERROR = "transform_error"
    WRITE_ERROR = "write_error"
    UNKNOWN_ERROR = "unknown_error"


@dataclass
class DLQEntry:
    """Represents a dead-letter queue entry."""

    id: str
    category: DLQCategory
    topic: str
    payload: bytes
    error_message: str
    error_traceback: str | None
    archive_id: str | None
    retry_count: int
    created_at: datetime
    last_retry_at: datetime | None

    def to_dict(self) -> dict[str, Any]:
        """Convert entry to dictionary."""
        return {
            "id": self.id,
            "category": self.category.value,
            "topic": self.topic,
            "payload_size": len(self.payload),
            "error_message": self.error_message,
            "archive_id": self.archive_id,
            "retry_count": self.retry_count,
            "created_at": self.created_at.isoformat(),
            "last_retry_at": self.last_retry_at.isoformat() if self.last_retry_at else None,
        }


class DeadLetterQueue:
    """Stores failed messages for inspection and replay.

    Uses SQLite for durable storage with automatic cleanup
    of old entries.
    """

    def __init__(
        self,
        db_path: Path | str,
        max_entries: int = 10_000,
        retention_days: int = 30,
        max_retries: int = 3,
    ) -> None:
        """Initialize dead-letter queue.

        Args:
            db_path: Path to SQLite database file.
            max_entries: Maximum entries before oldest are evicted.
            retention_days: Delete entries older than this.
            max_retries: Maximum retry attempts for replay.
        """
        self._db_path = Path(db_path)
        self._max_entries = max_entries
        self._retention_days = retention_days
        self._max_retries = max_retries
        self._initialized = False
        self._total_enqueued = 0
        self._total_replayed = 0
        self._total_failed_replays = 0

    def _connect(self) -> sqlite3.Connection:
        """Open a SQLite connection with safer concurrency settings."""
        conn = sqlite3.connect(self._db_path, timeout=5.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout = 5000")
        return conn

    @staticmethod
    def _decompress_payload(data: bytes) -> bytes:
        """Decompress a stored payload, falling back to raw bytes for old entries."""
        try:
            return zlib.decompress(data)
        except zlib.error:
            # Pre-compression entry stored as raw bytes -- return as-is
            return data

    async def _ensure_initialized(self) -> None:
        """Ensure database is initialized."""
        if self._initialized:
            return

        loop = asyncio.get_running_loop()

        def init_db() -> None:
            self._db_path.parent.mkdir(parents=True, exist_ok=True)

            with self._connect() as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS dlq_entries (
                        id TEXT PRIMARY KEY,
                        category TEXT NOT NULL,
                        topic TEXT NOT NULL,
                        payload BLOB NOT NULL,
                        error_message TEXT NOT NULL,
                        error_traceback TEXT,
                        archive_id TEXT,
                        retry_count INTEGER DEFAULT 0,
                        created_at TEXT NOT NULL,
                        last_retry_at TEXT
                    )
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_dlq_category
                    ON dlq_entries(category)
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_dlq_created_at
                    ON dlq_entries(created_at)
                """)
                conn.commit()

        await loop.run_in_executor(None, init_db)
        self._initialized = True
        logger.debug("dlq_initialized", path=str(self._db_path))

    async def enqueue(
        self,
        category: DLQCategory,
        topic: str,
        payload: bytes,
        error: Exception,
        archive_id: str | None = None,
    ) -> str:
        """Add a failed message to the dead-letter queue.

        Args:
            category: Error category for classification.
            topic: Topic of the failed message.
            payload: Raw payload bytes.
            error: Exception that caused the failure.
            archive_id: Optional archive ID for correlation.

        Returns:
            DLQ entry ID.
        """
        await self._ensure_initialized()

        import uuid

        entry_id = uuid.uuid4().hex[:16]
        now = datetime.now()
        error_tb = "".join(traceback.format_exception(type(error), error, error.__traceback__))
        compressed_payload = zlib.compress(payload)

        loop = asyncio.get_running_loop()

        def do_enqueue() -> None:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO dlq_entries (
                        id, category, topic, payload, error_message,
                        error_traceback, archive_id, retry_count, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)
                    """,
                    (
                        entry_id,
                        category.value,
                        topic,
                        compressed_payload,
                        str(error),
                        error_tb,
                        archive_id,
                        now.isoformat(),
                    ),
                )
                conn.commit()

        await loop.run_in_executor(None, do_enqueue)
        self._total_enqueued += 1

        logger.warning(
            "dlq_enqueued",
            entry_id=entry_id,
            category=category.value,
            topic=topic,
            error=str(error),
            archive_id=archive_id,
        )

        # Trigger cleanup if needed
        await self._cleanup_if_needed()

        return entry_id

    async def get_entry(self, entry_id: str) -> DLQEntry | None:
        """Get a single DLQ entry by ID.

        Args:
            entry_id: Entry ID to retrieve.

        Returns:
            DLQEntry or None if not found.
        """
        await self._ensure_initialized()

        loop = asyncio.get_running_loop()

        def do_get() -> DLQEntry | None:
            with self._connect() as conn:
                cursor = conn.execute(
                    """
                    SELECT id, category, topic, payload, error_message,
                           error_traceback, archive_id, retry_count,
                           created_at, last_retry_at
                    FROM dlq_entries WHERE id = ?
                    """,
                    (entry_id,),
                )
                row = cursor.fetchone()
                if not row:
                    return None

                return DLQEntry(
                    id=row[0],
                    category=DLQCategory(row[1]),
                    topic=row[2],
                    payload=DeadLetterQueue._decompress_payload(row[3]),
                    error_message=row[4],
                    error_traceback=row[5],
                    archive_id=row[6],
                    retry_count=row[7],
                    created_at=datetime.fromisoformat(row[8]),
                    last_retry_at=datetime.fromisoformat(row[9]) if row[9] else None,
                )

        return await loop.run_in_executor(None, do_get)

    async def get_entries(
        self,
        category: DLQCategory | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[DLQEntry]:
        """Get DLQ entries with optional filtering.

        Args:
            category: Filter by category (None for all).
            limit: Maximum entries to return.
            offset: Number of entries to skip.

        Returns:
            List of DLQEntry objects.
        """
        await self._ensure_initialized()

        loop = asyncio.get_running_loop()

        def do_get() -> list[DLQEntry]:
            with self._connect() as conn:
                if category:
                    cursor = conn.execute(
                        """
                        SELECT id, category, topic, payload, error_message,
                               error_traceback, archive_id, retry_count,
                               created_at, last_retry_at
                        FROM dlq_entries
                        WHERE category = ?
                        ORDER BY created_at DESC
                        LIMIT ? OFFSET ?
                        """,
                        (category.value, limit, offset),
                    )
                else:
                    cursor = conn.execute(
                        """
                        SELECT id, category, topic, payload, error_message,
                               error_traceback, archive_id, retry_count,
                               created_at, last_retry_at
                        FROM dlq_entries
                        ORDER BY created_at DESC
                        LIMIT ? OFFSET ?
                        """,
                        (limit, offset),
                    )

                entries = []
                for row in cursor:
                    entries.append(
                        DLQEntry(
                            id=row[0],
                            category=DLQCategory(row[1]),
                            topic=row[2],
                            payload=DeadLetterQueue._decompress_payload(row[3]),
                            error_message=row[4],
                            error_traceback=row[5],
                            archive_id=row[6],
                            retry_count=row[7],
                            created_at=datetime.fromisoformat(row[8]),
                            last_retry_at=datetime.fromisoformat(row[9]) if row[9] else None,
                        )
                    )
                return entries

        return await loop.run_in_executor(None, do_get)

    async def replay_entry(
        self,
        entry_id: str,
        callback: Callable[[str, dict[str, Any]], Any],
    ) -> bool:
        """Attempt to replay a DLQ entry.

        Args:
            entry_id: Entry ID to replay.
            callback: Async function(topic, payload) to process the message.

        Returns:
            True if replay succeeded, False otherwise.
        """
        entry = await self.get_entry(entry_id)
        if not entry:
            logger.warning("dlq_entry_not_found", entry_id=entry_id)
            return False

        if entry.retry_count >= self._max_retries:
            logger.warning(
                "dlq_max_retries_exceeded",
                entry_id=entry_id,
                retry_count=entry.retry_count,
            )
            return False

        try:
            # Parse payload
            payload = json.loads(entry.payload.decode("utf-8"))

            # Call the processing callback
            await callback(entry.topic, payload)

            # Success - delete entry
            await self.delete_entry(entry_id)
            self._total_replayed += 1

            logger.info("dlq_replay_success", entry_id=entry_id)
            return True

        except Exception as e:
            # Update retry count
            self._total_failed_replays += 1
            await self._increment_retry(entry_id)

            logger.warning(
                "dlq_replay_failed",
                entry_id=entry_id,
                retry_count=entry.retry_count + 1,
                error=str(e),
            )
            return False

    async def replay_category(
        self,
        category: DLQCategory,
        callback: Callable[[str, dict[str, Any]], Any],
        limit: int = 100,
    ) -> tuple[int, int]:
        """Replay all entries in a category.

        Args:
            category: Category to replay.
            callback: Processing function.
            limit: Maximum entries to replay.

        Returns:
            Tuple of (success_count, failure_count).
        """
        entries = await self.get_entries(category=category, limit=limit)

        success = 0
        failure = 0

        for entry in entries:
            if await self.replay_entry(entry.id, callback):
                success += 1
            else:
                failure += 1

        logger.info(
            "dlq_category_replay_complete",
            category=category.value,
            success=success,
            failure=failure,
        )

        return success, failure

    async def delete_entry(self, entry_id: str) -> bool:
        """Delete a DLQ entry.

        Args:
            entry_id: Entry ID to delete.

        Returns:
            True if entry was deleted, False if not found.
        """
        await self._ensure_initialized()

        loop = asyncio.get_running_loop()

        def do_delete() -> bool:
            with self._connect() as conn:
                cursor = conn.execute(
                    "DELETE FROM dlq_entries WHERE id = ?",
                    (entry_id,),
                )
                conn.commit()
                return cursor.rowcount > 0

        return await loop.run_in_executor(None, do_delete)

    async def _increment_retry(self, entry_id: str) -> None:
        """Increment retry count for an entry."""
        loop = asyncio.get_running_loop()
        now = datetime.now()

        def do_update() -> None:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE dlq_entries
                    SET retry_count = retry_count + 1, last_retry_at = ?
                    WHERE id = ?
                    """,
                    (now.isoformat(), entry_id),
                )
                conn.commit()

        await loop.run_in_executor(None, do_update)

    async def _cleanup_if_needed(self) -> None:
        """Clean up old entries if over max or past retention."""
        loop = asyncio.get_running_loop()
        cutoff = (datetime.now() - timedelta(days=self._retention_days)).isoformat()

        def do_cleanup() -> tuple[int, int]:
            with self._connect() as conn:
                # Delete old entries
                cursor = conn.execute(
                    "DELETE FROM dlq_entries WHERE created_at < ?",
                    (cutoff,),
                )
                aged_out = cursor.rowcount

                # Check count
                cursor = conn.execute("SELECT COUNT(*) FROM dlq_entries")
                count = cursor.fetchone()[0]

                # Evict oldest if over max
                evicted = 0
                if count > self._max_entries:
                    excess = count - self._max_entries
                    conn.execute(
                        """
                        DELETE FROM dlq_entries WHERE id IN (
                            SELECT id FROM dlq_entries
                            ORDER BY created_at ASC
                            LIMIT ?
                        )
                        """,
                        (excess,),
                    )
                    evicted = excess

                conn.commit()
                return aged_out, evicted

        aged_out, evicted = await loop.run_in_executor(None, do_cleanup)

        if aged_out > 0 or evicted > 0:
            logger.info("dlq_cleanup", aged_out=aged_out, evicted=evicted)

    async def get_stats(self) -> dict[str, Any]:
        """Get DLQ statistics.

        Returns:
            Dict with DLQ metrics and category counts.
        """
        await self._ensure_initialized()

        loop = asyncio.get_running_loop()

        def do_stats() -> dict[str, Any]:
            with self._connect() as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM dlq_entries")
                total = cursor.fetchone()[0]

                cursor = conn.execute("""
                    SELECT category, COUNT(*) FROM dlq_entries GROUP BY category
                """)
                by_category = {row[0]: row[1] for row in cursor}

                cursor = conn.execute("""
                    SELECT AVG(retry_count) FROM dlq_entries
                """)
                avg_retries = cursor.fetchone()[0] or 0

            return {
                "total_entries": total,
                "max_entries": self._max_entries,
                "by_category": by_category,
                "avg_retry_count": round(avg_retries, 2),
                "total_enqueued": self._total_enqueued,
                "total_replayed": self._total_replayed,
                "total_failed_replays": self._total_failed_replays,
                "retention_days": self._retention_days,
                "db_path": str(self._db_path),
            }

        return await loop.run_in_executor(None, do_stats)

    async def clear(self) -> int:
        """Clear all DLQ entries.

        Returns:
            Number of entries deleted.
        """
        await self._ensure_initialized()

        loop = asyncio.get_running_loop()

        def do_clear() -> int:
            with self._connect() as conn:
                cursor = conn.execute("DELETE FROM dlq_entries")
                conn.commit()
                return cursor.rowcount

        count = await loop.run_in_executor(None, do_clear)
        logger.info("dlq_cleared", count=count)
        return count
