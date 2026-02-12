"""Tests for async archive storage and integration."""

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from health_ingest.archive import RawArchiver
from health_ingest.config import HTTPSettings
from health_ingest.http_handler import HTTPHandler


@pytest.fixture
def temp_archive_dir(tmp_path: Path) -> Path:
    return tmp_path


class TestRawArchiverAsync:
    """Tests for the async store method of RawArchiver."""

    @pytest.mark.asyncio
    async def test_store_is_awaitable_and_creates_file(self, temp_archive_dir: Path):
        """Test that store() is awaitable and actually writes the file."""
        archiver = RawArchiver(temp_archive_dir)
        topic = "test/async"
        payload = b'{"async": true}'

        # Should await without error
        archive_id = await archiver.store(topic, payload)

        assert archive_id is not None
        assert isinstance(archive_id, str)

        # Verify file creation
        today = datetime.now().strftime("%Y-%m-%d")
        expected_file = temp_archive_dir / f"{today}.jsonl"
        assert expected_file.exists()

        # Verify content
        with open(expected_file, encoding="utf-8") as f:
            lines = f.readlines()
            assert len(lines) == 1
            entry = json.loads(lines[0])
            assert entry["topic"] == topic
            assert entry["id"] == archive_id
            assert entry["payload"] == {"async": True}

    @pytest.mark.asyncio
    async def test_store_handles_binary_payload(self, temp_archive_dir: Path):
        """Test that store() handles binary payloads correctly."""
        archiver = RawArchiver(temp_archive_dir)
        payload = b"\x80\x01\x02"  # Invalid UTF-8

        await archiver.store("test/binary", payload)

        today = datetime.now().strftime("%Y-%m-%d")
        expected_file = temp_archive_dir / f"{today}.jsonl"

        with open(expected_file, encoding="utf-8") as f:
            entry = json.loads(f.readline())
            assert "_binary" in entry["payload"]


class TestHTTPHandlerWithArchiver:
    """Tests for HTTPHandler integration with RawArchiver."""

    @pytest.mark.asyncio
    async def test_ingest_calls_archiver_store(self, temp_archive_dir: Path):
        """Test that POST /ingest calls archiver.store()."""

        # Create a real archiver
        archiver = RawArchiver(temp_archive_dir)

        # Spy on the store method
        with patch.object(archiver, "store", wraps=archiver.store) as mock_store:
            # Create handler with this archiver
            settings = HTTPSettings(_env_file=None, auth_token="token")
            handler = HTTPHandler(
                settings=settings,
                message_callback=AsyncMock(),
                archiver=archiver,
            )

            transport = ASGITransport(app=handler.app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                payload = {"data": []}
                resp = await client.post(
                    "/ingest",
                    json=payload,
                    headers={"Authorization": "Bearer token"},
                )

            assert resp.status_code == 202

            # Verify store was called (awaited)
            mock_store.assert_awaited_once()

            # Verify returned archive_id matches
            body = resp.json()
            assert "archive_id" in body
            assert body["archive_id"] is not None

            # Verify file actually exists (double check integration)
            today = datetime.now().strftime("%Y-%m-%d")
            expected_file = temp_archive_dir / f"{today}.jsonl"
            assert expected_file.exists()
