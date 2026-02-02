"""DuckDB-powered local analytics for archived health payloads."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections.abc import Iterable
from pathlib import Path

import duckdb


def _archive_glob(archive_dir: Path) -> str:
    """Return a glob pattern that captures daily and hourly JSONL archives."""
    return str(archive_dir / "*.jsonl*")


def _ensure_parent(path: Path) -> None:
    """Ensure the parent directory exists for a file path."""
    if path.parent and not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)


def _print_table(columns: list[str], rows: Iterable[tuple[object, ...]]) -> None:
    """Print rows in a simple aligned table."""
    rows_list = list(rows)
    widths = [len(col) for col in columns]
    for row in rows_list:
        widths = [max(widths[i], len(str(value))) for i, value in enumerate(row)]

    def format_row(values: Iterable[object]) -> str:
        parts = [str(value).ljust(widths[i]) for i, value in enumerate(values)]
        return " | ".join(parts)

    print(format_row(columns))
    print("-+-".join("-" * width for width in widths))
    for row in rows_list:
        print(format_row(row))


def _print_results(columns: list[str], rows: list[tuple[object, ...]], fmt: str) -> None:
    """Print query results in the requested format."""
    if fmt == "json":
        output = [dict(zip(columns, row, strict=True)) for row in rows]
        print(json.dumps(output, indent=2, default=str))
        return
    if fmt == "csv":
        writer = csv.writer(sys.stdout)
        writer.writerow(columns)
        writer.writerows(rows)
        return

    _print_table(columns, rows)


def _load_archive_table(
    conn: duckdb.DuckDBPyConnection,
    archive_dir: Path,
    table_name: str,
) -> None:
    """Load JSONL archive files into a DuckDB table."""
    archive_glob = _archive_glob(archive_dir)
    if not list(archive_dir.glob("*.jsonl*")):
        raise FileNotFoundError(f"No archive files found in {archive_dir}")

    conn.execute(
        f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT * FROM read_json_auto('{archive_glob}', format='newline_delimited')
        """
    )


def duckdb_cli() -> None:
    """CLI entry point for DuckDB analytics and Parquet exports."""
    parser = argparse.ArgumentParser(
        description="Query archived health payloads with DuckDB and export Parquet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            '  health-duckdb --sql "SELECT topic, COUNT(*) c FROM raw_archive GROUP BY 1"\n'
            '  health-duckdb --sql "SELECT * FROM raw_archive LIMIT 5" --format csv\n'
            "  health-duckdb --export-parquet /data/exports/raw.parquet\n"
        ),
    )
    parser.add_argument(
        "--archive-dir",
        type=Path,
        default=Path("/data/archive"),
        help="Archive directory containing JSONL files (default: /data/archive)",
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=Path("/data/duckdb/health.duckdb"),
        help="DuckDB database file (default: /data/duckdb/health.duckdb)",
    )
    parser.add_argument(
        "--table",
        default="raw_archive",
        help="Table name for loaded archives (default: raw_archive)",
    )
    parser.add_argument(
        "--sql",
        default=None,
        help="SQL query to run against the archive table",
    )
    parser.add_argument(
        "--format",
        choices=("table", "json", "csv"),
        default="table",
        help="Output format for query results (default: table)",
    )
    parser.add_argument(
        "--export-parquet",
        type=Path,
        default=None,
        help="Export query results to a Parquet file",
    )

    args = parser.parse_args()

    db_path = args.database
    _ensure_parent(db_path)

    conn = duckdb.connect(str(db_path))
    try:
        _load_archive_table(conn, args.archive_dir, args.table)
        query = args.sql or f"SELECT * FROM {args.table}"

        if args.export_parquet:
            export_path = args.export_parquet
            _ensure_parent(export_path)
            conn.execute(f"COPY ({query}) TO '{export_path}' (FORMAT PARQUET)")
            print(f"Exported Parquet to {export_path}")
            if args.sql is None:
                return

        cursor = conn.execute(query)
        rows = cursor.fetchall()
        _print_results([col[0] for col in cursor.description], rows, args.format)
    except FileNotFoundError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()
