"""Tests for the raw payload archiver."""

import gzip
import json
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from health_ingest.archive import RawArchiver


@pytest.fixture
def archive_dir():
    """Create temporary archive directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def archiver(archive_dir):
    """Create archiver with temporary directory."""
    return RawArchiver(archive_dir)


class TestRawArchiver:
    """Tests for RawArchiver class."""

    def test_store_sync_creates_file(self, archiver, archive_dir):
        """Test that store_sync creates a JSONL file."""
        topic = "health/export/heart"
        payload = json.dumps({"name": "heart_rate", "qty": 72}).encode()

        archive_id = archiver.store_sync(topic, payload)

        # Check file was created
        today = date.today().strftime("%Y-%m-%d")
        file_path = archive_dir / f"{today}.jsonl"
        assert file_path.exists()

        # Check content
        with open(file_path) as f:
            line = f.readline()
            entry = json.loads(line)

        assert entry["id"] == archive_id
        assert entry["topic"] == topic
        assert entry["payload"]["name"] == "heart_rate"
        assert entry["payload"]["qty"] == 72

    def test_store_sync_appends_to_file(self, archiver, archive_dir):
        """Test that multiple calls append to the same file."""
        payload1 = json.dumps({"name": "heart_rate", "qty": 72}).encode()
        payload2 = json.dumps({"name": "heart_rate", "qty": 75}).encode()

        archiver.store_sync("health/export/heart", payload1)
        archiver.store_sync("health/export/heart", payload2)

        today = date.today().strftime("%Y-%m-%d")
        file_path = archive_dir / f"{today}.jsonl"

        with open(file_path) as f:
            lines = f.readlines()

        assert len(lines) == 2

    def test_store_sync_handles_binary_payload(self, archiver, archive_dir):
        """Test that binary payloads are base64 encoded."""
        binary_payload = b"\x00\x01\x02\xff\xfe"

        archiver.store_sync("health/export/binary", binary_payload)

        today = date.today().strftime("%Y-%m-%d")
        file_path = archive_dir / f"{today}.jsonl"

        with open(file_path) as f:
            entry = json.loads(f.readline())

        assert "_binary" in entry["payload"]

    def test_store_sync_returns_unique_ids(self, archiver):
        """Test that each call returns a unique ID."""
        payload = b'{"name": "test"}'

        id1 = archiver.store_sync("topic", payload)
        id2 = archiver.store_sync("topic", payload)
        id3 = archiver.store_sync("topic", payload)

        assert len({id1, id2, id3}) == 3

    def test_hourly_rotation(self, archive_dir):
        """Test hourly rotation creates hour-specific files."""
        archiver = RawArchiver(archive_dir, rotation="hourly")
        now = datetime.now()

        archiver.store_sync("topic", b'{"test": true}', received_at=now)

        expected_filename = now.strftime("%Y-%m-%d_%H.jsonl")
        assert (archive_dir / expected_filename).exists()

    @pytest.mark.asyncio
    async def test_replay_processes_entries(self, archiver, archive_dir):
        """Test replay iterates through archived entries."""
        # Create some archived entries
        payload1 = json.dumps({"id": 1}).encode()
        payload2 = json.dumps({"id": 2}).encode()

        archiver.store_sync("topic/1", payload1)
        archiver.store_sync("topic/2", payload2)

        # Replay and collect
        processed = []

        async def callback(topic, payload, archive_id):
            processed.append((topic, payload, archive_id))

        count = await archiver.replay(date.today(), date.today(), callback)

        assert count == 2
        assert len(processed) == 2
        assert processed[0][0] == "topic/1"
        assert processed[1][0] == "topic/2"

    @pytest.mark.asyncio
    async def test_replay_handles_date_range(self, archive_dir):
        """Test replay respects date range."""
        archiver = RawArchiver(archive_dir)
        yesterday = datetime.now() - timedelta(days=1)
        today = datetime.now()

        # Create entries for different days
        archiver.store_sync("yesterday", b'{"day": 1}', received_at=yesterday)
        archiver.store_sync("today", b'{"day": 2}', received_at=today)

        processed = []

        async def callback(topic, payload, archive_id):
            processed.append(topic)

        # Only replay today
        await archiver.replay(date.today(), date.today(), callback)

        assert len(processed) == 1
        assert processed[0] == "today"

    @pytest.mark.asyncio
    async def test_compress_old_files(self, archive_dir):
        """Test compression of old archive files."""
        archiver = RawArchiver(archive_dir, compress_after_days=0)

        # Create a file dated in the past
        old_date = date.today() - timedelta(days=1)
        old_file = archive_dir / f"{old_date}.jsonl"
        old_file.write_text('{"test": true}\n')

        count = await archiver.compress_old_files()

        assert count == 1
        assert not old_file.exists()
        assert (archive_dir / f"{old_date}.jsonl.gz").exists()

    @pytest.mark.asyncio
    async def test_cleanup_old_files(self, archive_dir):
        """Test deletion of old archive files."""
        archiver = RawArchiver(archive_dir, max_age_days=0)

        # Create a file dated in the past
        old_date = date.today() - timedelta(days=1)
        old_file = archive_dir / f"{old_date}.jsonl"
        old_file.write_text('{"test": true}\n')

        count = await archiver.cleanup_old_files()

        assert count == 1
        assert not old_file.exists()

    @pytest.mark.asyncio
    async def test_get_stats(self, archiver, archive_dir):
        """Test archive statistics."""
        # Create some files
        archiver.store_sync("topic", b'{"test": true}')
        archiver.store_sync("topic", b'{"test": true}')

        stats = await archiver.get_stats()

        assert stats["jsonl_files"] == 1
        assert stats["compressed_files"] == 0
        assert stats["write_count"] == 2
        assert stats["total_size_bytes"] > 0

    @pytest.mark.asyncio
    async def test_replay_gzipped_files(self, archive_dir):
        """Test replay can read gzipped files."""
        archiver = RawArchiver(archive_dir)

        # Create a gzipped file
        old_date = date.today() - timedelta(days=1)
        gz_path = archive_dir / f"{old_date}.jsonl.gz"
        entry = {"id": "test123", "topic": "test", "ts": "2024-01-15T00:00:00", "payload": {"x": 1}}

        with gzip.open(gz_path, "wt", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")

        processed = []

        async def callback(topic, payload, archive_id):
            processed.append((topic, archive_id))

        await archiver.replay(old_date, old_date, callback)

        assert len(processed) == 1
        assert processed[0] == ("test", "test123")
