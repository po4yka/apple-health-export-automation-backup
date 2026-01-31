"""CLI tools for archive replay, DLQ inspection, and DLQ replay."""

import argparse
import asyncio
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

from .archive import RawArchiver
from .config import get_settings
from .dlq import DeadLetterQueue, DLQCategory
from .influx_writer import InfluxWriter
from .logging import setup_logging
from .transformers import TransformerRegistry


def parse_date(date_str: str) -> date:
    """Parse date string in YYYY-MM-DD format."""
    return datetime.strptime(date_str, "%Y-%m-%d").date()


async def _replay_archive(
    archive_dir: Path,
    start_date: date,
    end_date: date,
    dry_run: bool = False,
) -> None:
    """Replay archived messages through the processing pipeline."""
    settings = get_settings()
    setup_logging(settings.app)

    archiver = RawArchiver(archive_dir)
    registry = TransformerRegistry(default_source=settings.app.default_source)

    if dry_run:
        count = 0

        async def count_callback(topic: str, payload: dict[str, Any], archive_id: str) -> None:
            nonlocal count
            count += 1
            if count <= 10:
                print(f"  [{archive_id}] {topic}: {len(str(payload))} bytes")

        await archiver.replay(start_date, end_date, count_callback)
        print(f"\nDry run: {count} messages would be replayed")
        return

    # Real replay with InfluxDB writes
    writer = InfluxWriter(settings.influxdb)
    await writer.connect()

    processed = 0
    errors = 0

    async def process_callback(topic: str, payload: dict[str, Any], archive_id: str) -> None:
        nonlocal processed, errors
        try:
            points = registry.transform(payload)
            if points:
                await writer.write(points)
            processed += 1
        except Exception as e:
            errors += 1
            print(f"Error processing {archive_id}: {e}", file=sys.stderr)

    try:
        total = await archiver.replay(start_date, end_date, process_callback)
        print(f"\nReplayed {total} messages: {processed} processed, {errors} errors")
    finally:
        await writer.disconnect()


def archive_replay() -> None:
    """CLI entry point for archive replay.

    Usage:
        health-archive-replay --start 2024-01-01 --end 2024-01-15 [--dry-run]
    """
    parser = argparse.ArgumentParser(
        description="Replay archived messages through the processing pipeline"
    )
    parser.add_argument(
        "--start",
        type=parse_date,
        required=True,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        type=parse_date,
        required=True,
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--archive-dir",
        type=Path,
        default=Path("/data/archive"),
        help="Archive directory (default: /data/archive)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Just count messages without processing",
    )

    args = parser.parse_args()

    if args.start > args.end:
        print("Error: start date must be before or equal to end date", file=sys.stderr)
        sys.exit(1)

    asyncio.run(_replay_archive(args.archive_dir, args.start, args.end, args.dry_run))


async def _inspect_dlq(
    db_path: Path,
    category: str | None,
    limit: int,
    show_traceback: bool,
    format_json: bool,
) -> None:
    """Inspect DLQ entries."""
    dlq = DeadLetterQueue(db_path)

    cat = DLQCategory(category) if category else None
    entries = await dlq.get_entries(category=cat, limit=limit)

    if format_json:
        output = [entry.to_dict() for entry in entries]
        print(json.dumps(output, indent=2))
        return

    if not entries:
        print("No DLQ entries found")
        return

    print(f"Found {len(entries)} entries:\n")

    for entry in entries:
        print(f"ID: {entry.id}")
        print(f"  Category:   {entry.category.value}")
        print(f"  Topic:      {entry.topic}")
        print(f"  Error:      {entry.error_message[:100]}...")
        print(f"  Created:    {entry.created_at}")
        print(f"  Retries:    {entry.retry_count}")
        if entry.archive_id:
            print(f"  Archive ID: {entry.archive_id}")
        if show_traceback and entry.error_traceback:
            print(f"  Traceback:\n    {entry.error_traceback[:500]}")
        print()

    # Show stats
    stats = await dlq.get_stats()
    print("\nDLQ Stats:")
    print(f"  Total entries: {stats['total_entries']}")
    print(f"  By category:   {stats['by_category']}")


def dlq_inspect() -> None:
    """CLI entry point for DLQ inspection.

    Usage:
        health-dlq-inspect [--category json_parse_error] [--limit 50] [--traceback]
    """
    parser = argparse.ArgumentParser(description="Inspect dead-letter queue entries")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("/data/dlq/dlq.db"),
        help="DLQ database path (default: /data/dlq/dlq.db)",
    )
    parser.add_argument(
        "--category",
        choices=[c.value for c in DLQCategory],
        help="Filter by category",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum entries to show (default: 20)",
    )
    parser.add_argument(
        "--traceback",
        action="store_true",
        help="Show error tracebacks",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="format_json",
        help="Output as JSON",
    )

    args = parser.parse_args()
    asyncio.run(
        _inspect_dlq(args.db_path, args.category, args.limit, args.traceback, args.format_json)
    )


async def _replay_dlq(
    db_path: Path,
    entry_id: str | None,
    category: str | None,
    limit: int,
    dry_run: bool,
) -> None:
    """Replay DLQ entries."""
    settings = get_settings()
    setup_logging(settings.app)

    dlq = DeadLetterQueue(db_path)
    registry = TransformerRegistry(default_source=settings.app.default_source)

    if dry_run:
        if entry_id:
            entry = await dlq.get_entry(entry_id)
            if entry:
                print(f"Would replay entry {entry_id}:")
                print(f"  Topic: {entry.topic}")
                print(f"  Category: {entry.category.value}")
            else:
                print(f"Entry {entry_id} not found")
            return

        cat = DLQCategory(category) if category else None
        entries = await dlq.get_entries(category=cat, limit=limit)
        print(f"Would replay {len(entries)} entries")
        return

    # Real replay
    writer = InfluxWriter(settings.influxdb)
    await writer.connect()

    async def process_message(topic: str, payload: dict[str, Any]) -> None:
        points = registry.transform(payload)
        if points:
            await writer.write(points)

    try:
        if entry_id:
            success = await dlq.replay_entry(entry_id, process_message)
            print(f"Replay {'succeeded' if success else 'failed'} for {entry_id}")
        else:
            cat = DLQCategory(category) if category else None
            if cat:
                success, failure = await dlq.replay_category(cat, process_message, limit=limit)
                print(f"Replayed category {cat.value}: {success} succeeded, {failure} failed")
            else:
                # Replay all categories
                total_success = 0
                total_failure = 0
                for c in DLQCategory:
                    s, f = await dlq.replay_category(c, process_message, limit=limit)
                    total_success += s
                    total_failure += f
                print(f"Replayed all: {total_success} succeeded, {total_failure} failed")
    finally:
        await writer.disconnect()


def dlq_replay() -> None:
    """CLI entry point for DLQ replay.

    Usage:
        health-dlq-replay --id abc123
        health-dlq-replay --category transform_error [--limit 100]
    """
    parser = argparse.ArgumentParser(description="Replay dead-letter queue entries")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("/data/dlq/dlq.db"),
        help="DLQ database path (default: /data/dlq/dlq.db)",
    )
    parser.add_argument(
        "--id",
        dest="entry_id",
        help="Replay specific entry by ID",
    )
    parser.add_argument(
        "--category",
        choices=[c.value for c in DLQCategory],
        help="Replay all entries in category",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum entries to replay (default: 100)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be replayed without processing",
    )

    args = parser.parse_args()

    if not args.entry_id and not args.category and not args.dry_run:
        print("Error: must specify --id, --category, or use --dry-run to show all", file=sys.stderr)
        sys.exit(1)

    asyncio.run(
        _replay_dlq(args.db_path, args.entry_id, args.category, args.limit, args.dry_run)
    )


async def _archive_stats(archive_dir: Path) -> None:
    """Show archive statistics."""
    archiver = RawArchiver(archive_dir)
    stats = await archiver.get_stats()

    print("Archive Statistics:")
    print(f"  Directory:        {stats['archive_dir']}")
    print(f"  JSONL files:      {stats['jsonl_files']}")
    print(f"  Compressed files: {stats['compressed_files']}")
    print(f"  Total size:       {stats['total_size_bytes'] / 1024 / 1024:.2f} MB")


async def _archive_compress(archive_dir: Path, compress_after_days: int) -> None:
    """Compress old archive files."""
    archiver = RawArchiver(archive_dir, compress_after_days=compress_after_days)
    count = await archiver.compress_old_files()
    print(f"Compressed {count} files")


async def _archive_cleanup(archive_dir: Path, max_age_days: int) -> None:
    """Delete old archive files."""
    archiver = RawArchiver(archive_dir, max_age_days=max_age_days)
    count = await archiver.cleanup_old_files()
    print(f"Deleted {count} files")


def archive_manage() -> None:
    """CLI entry point for archive management.

    Usage:
        health-archive stats
        health-archive compress [--older-than 7]
        health-archive cleanup [--older-than 30]
    """
    parser = argparse.ArgumentParser(description="Manage archive files")
    parser.add_argument(
        "--archive-dir",
        type=Path,
        default=Path("/data/archive"),
        help="Archive directory (default: /data/archive)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("stats", help="Show archive statistics")

    compress_parser = subparsers.add_parser("compress", help="Compress old files")
    compress_parser.add_argument(
        "--older-than",
        type=int,
        default=7,
        help="Compress files older than N days (default: 7)",
    )

    cleanup_parser = subparsers.add_parser("cleanup", help="Delete old files")
    cleanup_parser.add_argument(
        "--older-than",
        type=int,
        default=30,
        help="Delete files older than N days (default: 30)",
    )

    args = parser.parse_args()

    if args.command == "stats":
        asyncio.run(_archive_stats(args.archive_dir))
    elif args.command == "compress":
        asyncio.run(_archive_compress(args.archive_dir, args.older_than))
    elif args.command == "cleanup":
        asyncio.run(_archive_cleanup(args.archive_dir, args.older_than))
