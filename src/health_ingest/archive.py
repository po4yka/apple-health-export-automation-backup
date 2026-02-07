"""Raw payload archiver for disaster recovery."""

import asyncio
import functools
import gzip
import json
import os
import threading
import uuid
from collections.abc import AsyncIterator, Callable
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class RawArchiver:
    """Persists raw payloads to JSONL files before processing.

    Provides a durable record of all incoming messages for replay
    in case of processing failures or data recovery needs.
    """

    def __init__(
        self,
        archive_dir: Path | str,
        rotation: str = "daily",
        max_age_days: int = 30,
        compress_after_days: int = 7,
    ) -> None:
        """Initialize the archiver.

        Args:
            archive_dir: Directory to store archive files.
            rotation: Rotation strategy ('daily' or 'hourly').
            max_age_days: Delete archives older than this.
            compress_after_days: Compress archives older than this.
        """
        self._archive_dir = Path(archive_dir).resolve()
        if self._archive_dir.exists() and not self._archive_dir.is_dir():
            raise ValueError(f"archive_dir is not a directory: {self._archive_dir}")
        self._rotation = rotation
        self._max_age_days = max_age_days
        self._compress_after_days = compress_after_days
        self._file_handles: dict[str, Any] = {}
        self._lock = asyncio.Lock()
        self._write_count = 0
        self._write_lock = threading.Lock()

    def _get_file_path(self, ts: datetime) -> Path:
        """Get archive file path for a given timestamp."""
        if self._rotation == "hourly":
            filename = ts.strftime("%Y-%m-%d_%H.jsonl")
        else:
            filename = ts.strftime("%Y-%m-%d.jsonl")
        return self._archive_dir / filename

    def _generate_id(self) -> str:
        """Generate a unique archive entry ID."""
        return uuid.uuid4().hex[:16]

    def store_sync(
        self,
        topic: str,
        payload: bytes,
        received_at: datetime | None = None,
    ) -> str:
        """Synchronously store payload to archive.

        This is designed to be called from ingestion callback threads.
        Uses a simple append to minimize latency.

        Args:
            topic: Message topic.
            payload: Raw payload bytes.
            received_at: Timestamp when message was received.

        Returns:
            Archive entry ID for correlation.
        """
        if received_at is None:
            received_at = datetime.now()

        archive_id = self._generate_id()
        file_path = self._get_file_path(received_at)

        # Ensure directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)

        entry = {
            "id": archive_id,
            "topic": topic,
            "ts": received_at.isoformat(),
            "payload": self._decode_payload(payload),
        }

        try:
            with self._write_lock:
                with open(file_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(entry, ensure_ascii=False) + "\n")
                self._write_count += 1
        except Exception as e:
            logger.error(
                "archive_write_failed",
                archive_id=archive_id,
                error=str(e),
            )

        return archive_id

    async def store(
        self,
        topic: str,
        payload: bytes,
        received_at: datetime | None = None,
    ) -> str:
        """Asynchronously store payload to archive (non-blocking).

        Wraps store_sync in a thread executor to avoid blocking the main event loop
        during file I/O operations.

        Args:
            topic: Message topic.
            payload: Raw payload bytes.
            received_at: Timestamp when message was received.

        Returns:
            Archive entry ID for correlation.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            functools.partial(self.store_sync, topic, payload, received_at),
        )

    def _decode_payload(self, payload: bytes) -> Any:
        """Decode payload, trying JSON first, then base64 for binary."""
        try:
            return json.loads(payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            import base64

            return {"_binary": base64.b64encode(payload).decode("ascii")}

    async def replay(
        self,
        start_date: date,
        end_date: date,
        callback: Callable[[str, dict[str, Any], str], Any],
    ) -> int:
        """Replay archived messages through processing pipeline.

        Args:
            start_date: Start date for replay (inclusive).
            end_date: End date for replay (inclusive).
            callback: Async function(topic, payload, archive_id) to process each message.

        Returns:
            Number of messages replayed.
        """
        count = 0
        current = start_date

        while current <= end_date:
            async for entry in self._read_day(current):
                try:
                    await callback(entry["topic"], entry["payload"], entry["id"])
                    count += 1
                except Exception as e:
                    logger.error(
                        "replay_callback_error",
                        archive_id=entry.get("id"),
                        error=str(e),
                    )
            current += timedelta(days=1)

        logger.info("archive_replay_complete", count=count)
        return count

    async def _read_day(self, day: date) -> AsyncIterator[dict[str, Any]]:
        """Read all entries from a day's archive file."""
        base_name = day.strftime("%Y-%m-%d")
        jsonl_path = self._archive_dir / f"{base_name}.jsonl"
        gz_path = self._archive_dir / f"{base_name}.jsonl.gz"

        # Try uncompressed first, then compressed
        if jsonl_path.exists():
            async for entry in self._read_jsonl(jsonl_path):
                yield entry
        elif gz_path.exists():
            async for entry in self._read_gzip(gz_path):
                yield entry

        # Handle hourly rotation files
        if self._rotation == "hourly":
            for hour in range(24):
                hourly_name = f"{base_name}_{hour:02d}"
                hourly_jsonl = self._archive_dir / f"{hourly_name}.jsonl"
                hourly_gz = self._archive_dir / f"{hourly_name}.jsonl.gz"

                if hourly_jsonl.exists():
                    async for entry in self._read_jsonl(hourly_jsonl):
                        yield entry
                elif hourly_gz.exists():
                    async for entry in self._read_gzip(hourly_gz):
                        yield entry

    async def _read_jsonl(self, path: Path) -> AsyncIterator[dict[str, Any]]:
        """Stream entries from a JSONL file."""
        opener = functools.partial(open, encoding="utf-8")
        async for entry in self._stream_jsonl(path, opener):
            yield entry

    async def _read_gzip(self, path: Path) -> AsyncIterator[dict[str, Any]]:
        """Stream entries from a gzipped JSONL file."""
        opener = functools.partial(gzip.open, mode="rt", encoding="utf-8")
        async for entry in self._stream_jsonl(path, opener):
            yield entry

    async def _stream_jsonl(
        self, path: Path, opener: Callable[[Path], Any]
    ) -> AsyncIterator[dict[str, Any]]:
        """Stream JSONL entries without loading the whole file into memory."""
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue[object] = asyncio.Queue(maxsize=1000)
        sentinel = object()

        def producer() -> None:
            try:
                with opener(path) as f:
                    for line in f:
                        if not line.strip():
                            continue
                        try:
                            entry = json.loads(line)
                        except json.JSONDecodeError as e:
                            logger.warning("archive_parse_error", path=str(path), error=str(e))
                            continue
                        asyncio.run_coroutine_threadsafe(queue.put(entry), loop).result()
            except Exception as e:
                logger.error("archive_stream_error", path=str(path), error=str(e))
            finally:
                asyncio.run_coroutine_threadsafe(queue.put(sentinel), loop).result()

        producer_task = loop.run_in_executor(None, producer)

        while True:
            item = await queue.get()
            if item is sentinel:
                break
            yield item  # type: ignore[misc]

        await producer_task

    async def compress_old_files(self) -> int:
        """Compress archive files older than compress_after_days.

        Returns:
            Number of files compressed.
        """
        cutoff = date.today() - timedelta(days=self._compress_after_days)
        count = 0

        loop = asyncio.get_running_loop()

        def do_compress() -> int:
            compressed = 0
            for path in self._archive_dir.glob("*.jsonl"):
                # Extract date from filename
                try:
                    name = path.stem
                    if "_" in name and self._rotation == "hourly":
                        file_date = datetime.strptime(name.split("_")[0], "%Y-%m-%d").date()
                    else:
                        file_date = datetime.strptime(name, "%Y-%m-%d").date()

                    if file_date < cutoff:
                        gz_path = path.with_suffix(".jsonl.gz")
                        with open(path, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
                            f_out.writelines(f_in)
                        os.remove(path)
                        compressed += 1
                        logger.info("archive_compressed", path=str(path))
                except (ValueError, OSError) as e:
                    logger.warning("compress_failed", path=str(path), error=str(e))
            return compressed

        count = await loop.run_in_executor(None, do_compress)
        return count

    async def cleanup_old_files(self) -> int:
        """Delete archive files older than max_age_days.

        Returns:
            Number of files deleted.
        """
        cutoff = date.today() - timedelta(days=self._max_age_days)
        count = 0

        loop = asyncio.get_running_loop()

        def do_cleanup() -> int:
            deleted = 0
            for pattern in ("*.jsonl", "*.jsonl.gz"):
                for path in self._archive_dir.glob(pattern):
                    try:
                        name = path.stem.replace(".jsonl", "")
                        if "_" in name and self._rotation == "hourly":
                            file_date = datetime.strptime(name.split("_")[0], "%Y-%m-%d").date()
                        else:
                            file_date = datetime.strptime(name, "%Y-%m-%d").date()

                        if file_date < cutoff:
                            os.remove(path)
                            deleted += 1
                            logger.info("archive_deleted", path=str(path))
                    except (ValueError, OSError) as e:
                        logger.warning("cleanup_failed", path=str(path), error=str(e))
            return deleted

        count = await loop.run_in_executor(None, do_cleanup)
        return count

    async def get_stats(self) -> dict[str, Any]:
        """Get archive statistics.

        Returns:
            Dict with file counts and sizes.
        """
        loop = asyncio.get_running_loop()

        def collect_stats() -> dict[str, Any]:
            jsonl_count = 0
            gz_count = 0
            total_size = 0

            if self._archive_dir.exists():
                for path in self._archive_dir.glob("*.jsonl"):
                    jsonl_count += 1
                    total_size += path.stat().st_size
                for path in self._archive_dir.glob("*.jsonl.gz"):
                    gz_count += 1
                    total_size += path.stat().st_size

            return {
                "jsonl_files": jsonl_count,
                "compressed_files": gz_count,
                "total_size_bytes": total_size,
                "write_count": self._write_count,
                "archive_dir": str(self._archive_dir),
            }

        return await loop.run_in_executor(None, collect_stats)
