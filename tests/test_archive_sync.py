"""Tests for archive store_sync method: file creation, rotation, and path safety."""

import json
from datetime import date, datetime
from pathlib import Path

import pytest

from health_ingest.archive import RawArchiver


class TestStoreSyncFileCreation:
    """Tests for store_sync creating files at the correct path."""

    def test_store_sync_writes_to_daily_file(self, tmp_path: Path):
        """store_sync writes a JSONL entry to the date-stamped file."""
        archiver = RawArchiver(tmp_path)
        topic = "http/ingest"
        payload = json.dumps({"name": "heart_rate", "qty": 72}).encode()

        archive_id = archiver.store_sync(topic, payload)

        today = date.today().strftime("%Y-%m-%d")
        file_path = tmp_path / f"{today}.jsonl"
        assert file_path.exists()

        with open(file_path) as f:
            entry = json.loads(f.readline())

        assert entry["id"] == archive_id
        assert entry["topic"] == topic
        assert entry["payload"]["name"] == "heart_rate"
        assert entry["payload"]["qty"] == 72

    def test_store_sync_appends_multiple_entries(self, tmp_path: Path):
        """Multiple store_sync calls append to the same daily file."""
        archiver = RawArchiver(tmp_path)
        payload1 = json.dumps({"name": "hr", "qty": 70}).encode()
        payload2 = json.dumps({"name": "hr", "qty": 75}).encode()
        payload3 = json.dumps({"name": "hr", "qty": 80}).encode()

        archiver.store_sync("topic", payload1)
        archiver.store_sync("topic", payload2)
        archiver.store_sync("topic", payload3)

        today = date.today().strftime("%Y-%m-%d")
        file_path = tmp_path / f"{today}.jsonl"

        with open(file_path) as f:
            lines = f.readlines()

        assert len(lines) == 3

    def test_store_sync_returns_unique_ids(self, tmp_path: Path):
        """Each store_sync call returns a distinct archive ID."""
        archiver = RawArchiver(tmp_path)
        payload = b'{"test": true}'

        ids = {archiver.store_sync("t", payload) for _ in range(10)}

        assert len(ids) == 10

    def test_store_sync_payload_stored_as_json_when_valid(self, tmp_path: Path):
        """Valid JSON payloads are stored as parsed objects (not base64)."""
        archiver = RawArchiver(tmp_path)
        original = {"metric": "steps", "value": 1234}
        payload = json.dumps(original).encode()

        archiver.store_sync("topic", payload)

        today = date.today().strftime("%Y-%m-%d")
        with open(tmp_path / f"{today}.jsonl") as f:
            entry = json.loads(f.readline())

        assert entry["payload"] == original

    def test_store_sync_binary_payload_base64_encoded(self, tmp_path: Path):
        """Binary payloads that are not valid JSON are stored as base64."""
        archiver = RawArchiver(tmp_path)
        binary = b"\x00\x01\xff\xfe"

        archiver.store_sync("topic", binary)

        today = date.today().strftime("%Y-%m-%d")
        with open(tmp_path / f"{today}.jsonl") as f:
            entry = json.loads(f.readline())

        assert "_binary" in entry["payload"]


class TestStoreSyncRotation:
    """Tests for daily and hourly rotation in store_sync."""

    def test_daily_rotation_uses_date_filename(self, tmp_path: Path):
        """Daily rotation creates files named YYYY-MM-DD.jsonl."""
        archiver = RawArchiver(tmp_path, rotation="daily")
        now = datetime(2025, 6, 15, 14, 30, 0)

        archiver.store_sync("topic", b'{"x": 1}', received_at=now)

        expected = tmp_path / "2025-06-15.jsonl"
        assert expected.exists()

    def test_hourly_rotation_uses_date_hour_filename(self, tmp_path: Path):
        """Hourly rotation creates files named YYYY-MM-DD_HH.jsonl."""
        archiver = RawArchiver(tmp_path, rotation="hourly")
        now = datetime(2025, 6, 15, 14, 30, 0)

        archiver.store_sync("topic", b'{"x": 1}', received_at=now)

        expected = tmp_path / "2025-06-15_14.jsonl"
        assert expected.exists()

    def test_different_hours_create_different_files(self, tmp_path: Path):
        """Hourly rotation separates entries from different hours."""
        archiver = RawArchiver(tmp_path, rotation="hourly")
        morning = datetime(2025, 6, 15, 9, 0, 0)
        afternoon = datetime(2025, 6, 15, 14, 0, 0)

        archiver.store_sync("topic", b'{"period": "am"}', received_at=morning)
        archiver.store_sync("topic", b'{"period": "pm"}', received_at=afternoon)

        assert (tmp_path / "2025-06-15_09.jsonl").exists()
        assert (tmp_path / "2025-06-15_14.jsonl").exists()

    def test_different_days_create_different_files(self, tmp_path: Path):
        """Daily rotation separates entries from different days."""
        archiver = RawArchiver(tmp_path, rotation="daily")
        day1 = datetime(2025, 6, 14, 12, 0, 0)
        day2 = datetime(2025, 6, 15, 12, 0, 0)

        archiver.store_sync("topic", b'{"day": 1}', received_at=day1)
        archiver.store_sync("topic", b'{"day": 2}', received_at=day2)

        assert (tmp_path / "2025-06-14.jsonl").exists()
        assert (tmp_path / "2025-06-15.jsonl").exists()


class TestArchivePathSafety:
    """Tests that the archive directory is resolved and path traversal is prevented."""

    def test_archive_dir_is_resolved(self, tmp_path: Path):
        """RawArchiver resolves the archive_dir to an absolute path."""
        relative_looking = tmp_path / "sub" / ".." / "sub"
        relative_looking.mkdir(parents=True, exist_ok=True)

        archiver = RawArchiver(relative_looking)

        # The internal _archive_dir should be resolved (no ".." components)
        assert ".." not in str(archiver._archive_dir)
        assert archiver._archive_dir == relative_looking.resolve()

    def test_store_sync_creates_parent_dirs(self, tmp_path: Path):
        """store_sync creates the archive directory if it does not exist."""
        archive_dir = tmp_path / "does" / "not" / "exist"
        # Don't create it -- let store_sync handle it
        archiver = RawArchiver(archive_dir)

        archiver.store_sync("topic", b'{"test": true}')

        assert archive_dir.exists()

    def test_archive_dir_not_a_file(self, tmp_path: Path):
        """RawArchiver raises ValueError if archive_dir points to a file."""
        file_path = tmp_path / "somefile.txt"
        file_path.write_text("not a directory")

        with pytest.raises(ValueError, match="not a directory"):
            RawArchiver(file_path)

    def test_store_sync_entry_contains_timestamp(self, tmp_path: Path):
        """Each archived entry includes an ISO timestamp."""
        archiver = RawArchiver(tmp_path)
        now = datetime(2025, 3, 20, 10, 15, 0)

        archiver.store_sync("topic", b'{"x": 1}', received_at=now)

        today_str = now.strftime("%Y-%m-%d")
        with open(tmp_path / f"{today_str}.jsonl") as f:
            entry = json.loads(f.readline())

        assert entry["ts"] == now.isoformat()

    def test_store_sync_raises_when_write_fails(self, tmp_path: Path, monkeypatch):
        """store_sync propagates write errors so callers can react safely."""
        archiver = RawArchiver(tmp_path)

        def fail_open(*args, **kwargs):
            raise OSError("disk full")

        monkeypatch.setattr("builtins.open", fail_open)

        with pytest.raises(OSError, match="disk full"):
            archiver.store_sync("topic", b'{"test": true}')
