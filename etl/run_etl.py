"""
Transform and Load Stage -- NIDS ETL Pipeline (Stage 2 of 2).

Reads raw JSON files produced by etl/extract.py from data/raw_json/,
cleans them with pandas, validates with Pydantic, loads into SQLite
(nids_full.db), and exports the deployment database (nids_deploy.db).

This module does NOT call any external API. All network I/O is handled
exclusively by etl/extract.py (Stage 1).

Usage:
    python -m etl.run_etl
    python -m etl.run_etl --json-dir data/raw_json --no-export
"""

import argparse
import json
import sys
import time
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd

from etl.cleaner import clean_dataframe
from etl.schema import validate_records
from etl.loader import get_db_connection, load_records_idempotently, is_offset_loaded
from etl.export_deploy_db import export_deploy_db

MAX_SAFETY_ROWS = 30000

DEFAULT_JSON_DIR = Path(__file__).resolve().parent.parent / "data" / "raw_json"


# ---------------------------------------------------------------------------
# JSON file helpers
# ---------------------------------------------------------------------------

def _offset_from_filename(filename: str) -> int:
    """
    Extracts the offset integer from a filename like rows_offset_000123400.json.
    Returns -1 if the filename does not match the expected pattern.
    """
    stem = Path(filename).stem  # e.g. rows_offset_000123400
    parts = stem.split("_")
    try:
        return int(parts[-1])
    except (ValueError, IndexError):
        return -1


def load_raw_json_files(json_dir: Path) -> List[Dict[str, Any]]:
    """
    Scans json_dir for rows_offset_*.json files and returns them as a sorted
    list of (offset, filepath) tuples, ordered by offset ascending.
    """
    files = sorted(
        json_dir.glob("rows_offset_*.json"),
        key=lambda p: _offset_from_filename(p.name)
    )
    return files


# ---------------------------------------------------------------------------
# Main transform-and-load runner
# ---------------------------------------------------------------------------

def run_etl(
    json_dir: Path = None,
    full_db_path: Path = None,
    deploy_db_path: Path = None,
    do_export: bool = True,
) -> dict:
    """
    Stage 2: Transform and Load.

    Reads every rows_offset_*.json file from json_dir, extracts the rows list
    from the raw HF API payload, cleans with pandas, validates with Pydantic,
    and loads idempotently into SQLite. After all files are processed, exports
    the deployment database.

    Returns a summary statistics dict.
    """
    base_dir = Path(__file__).resolve().parent.parent

    if json_dir is None:
        json_dir = DEFAULT_JSON_DIR
    json_dir = Path(json_dir)

    if full_db_path is None:
        full_db_path = base_dir / "data" / "nids_full.db"
    if deploy_db_path is None:
        deploy_db_path = base_dir / "data" / "nids_deploy.db"

    print("==================================================")
    print("NIDS Transform & Load Stage (Stage 2 of 2)")
    print(f"Input Dir     : {json_dir}")
    print(f"Full DB Path  : {full_db_path}")
    print(f"Deploy DB     : {deploy_db_path}")
    print("==================================================")
    sys.stdout.flush()

    if not json_dir.exists():
        raise FileNotFoundError(
            f"JSON input directory not found: {json_dir}\n"
            "Run etl/extract.py first to populate it."
        )

    json_files = load_raw_json_files(json_dir)
    if not json_files:
        raise FileNotFoundError(
            f"No rows_offset_*.json files found in {json_dir}.\n"
            "Run etl/extract.py first to populate it."
        )

    print(f"\n[Step 1] Found {len(json_files)} JSON file(s) in {json_dir}")
    sys.stdout.flush()

    start_time = time.time()
    conn = get_db_connection(full_db_path)

    total_files_processed = 0
    total_files_skipped = 0
    total_rows_read = 0
    total_cleaned = 0
    total_valid = 0
    total_invalid = 0
    total_inserted = 0
    batch_durations: List[float] = []

    # Pre-populate counts if resuming an existing database
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM etl_offsets;")
        existing_batches = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM network_flows;")
        existing_rows = cur.fetchone()[0]
        if existing_batches > 0:
            print(
                f"  [Resume Detection] Found {existing_batches} existing batches "
                f"and {existing_rows:,} rows already in DB. Skipping those offsets."
            )
            total_inserted = existing_rows
            sys.stdout.flush()
    except Exception:
        pass

    try:
        print(f"\n[Step 2] Processing {len(json_files)} JSON file(s)...\n")
        sys.stdout.flush()

        for i, filepath in enumerate(json_files, 1):
            batch_t0 = time.time()
            offset = _offset_from_filename(filepath.name)

            if offset == -1:
                print(f"  [Warning] Could not parse offset from {filepath.name}. Skipping.")
                continue

            # Idempotency: skip if this offset is already in the database
            if is_offset_loaded(conn, offset):
                total_files_skipped += 1
                if total_files_skipped <= 5 or total_files_skipped % 50 == 0:
                    print(f"  [Skip] Offset {offset} already in DB ({total_files_skipped} skipped so far).")
                    sys.stdout.flush()
                continue

            # Read raw JSON file
            try:
                raw_payload = json.loads(filepath.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError) as e:
                print(f"  [Warning] Failed to read {filepath.name}: {e}. Skipping.")
                continue

            # Extract rows from HF API payload structure: {"rows": [{"row": {...}}, ...]}
            raw_rows_list = raw_payload.get("rows", [])
            rows = [item["row"] for item in raw_rows_list if "row" in item]

            if not rows:
                print(f"  [Warning] {filepath.name} has no rows. Skipping.")
                continue

            total_rows_read += len(rows)

            # Clean with pandas
            df_raw = pd.DataFrame(rows)
            df_cleaned = clean_dataframe(df_raw)
            cleaned_count = len(df_cleaned)
            total_cleaned += cleaned_count

            # Validate with Pydantic
            cleaned_records = df_cleaned.to_dict(orient="records")
            valid_records, invalid_count = validate_records(cleaned_records)
            total_valid += len(valid_records)
            total_invalid += invalid_count

            # Load idempotently into SQLite with offset tracking
            new_inserted = load_records_idempotently(conn, valid_records, offset=offset)
            total_inserted += new_inserted
            total_files_processed += 1

            batch_duration = time.time() - batch_t0
            batch_durations.append(batch_duration)
            avg_batch = sum(batch_durations) / len(batch_durations)

            print(
                f"  [File {i}/{len(json_files)}] "
                f"offset={offset} | "
                f"rows_read={len(rows)} | cleaned={cleaned_count} | "
                f"valid={len(valid_records)} | invalid={invalid_count} | "
                f"new_inserted={new_inserted} | "
                f"total_db={total_inserted:,} | "
                f"time={batch_duration:.2f}s | avg={avg_batch:.2f}s"
            )
            sys.stdout.flush()

    finally:
        conn.close()

    elapsed = round(time.time() - start_time, 2)
    overall_avg = sum(batch_durations) / max(1, len(batch_durations))

    print("\n--------------------------------------------------")
    print(f"Transform & Load Complete in {elapsed:.1f}s ({elapsed/60:.2f} minutes):")
    print(f"  JSON Files Processed   : {total_files_processed}")
    print(f"  JSON Files Skipped     : {total_files_skipped} (already in DB)")
    print(f"  Total Rows Read        : {total_rows_read:,}")
    print(f"  Total Cleaned Rows     : {total_cleaned:,}")
    print(f"  Valid Pydantic Records : {total_valid:,}")
    print(f"  Invalid / Rejected     : {total_invalid:,}")
    print(f"  Total Inserted to DB   : {total_inserted:,}")
    print(f"  Avg Time Per File      : {overall_avg:.2f}s")
    print("--------------------------------------------------")
    sys.stdout.flush()

    export_summary = {}
    if do_export and total_inserted > 0:
        print("\nExporting precomputed EDA metrics & stratified sample to nids_deploy.db...")
        sys.stdout.flush()
        try:
            export_summary = export_deploy_db(full_db_path, deploy_db_path)
            print(f"Export successful: {export_summary}")
        except Exception as e:
            print(f"[Warning] Failed to export deploy db: {e}")
        sys.stdout.flush()

    return {
        "json_files_found": len(json_files),
        "files_processed": total_files_processed,
        "files_skipped": total_files_skipped,
        "total_rows_read": total_rows_read,
        "total_cleaned": total_cleaned,
        "total_valid": total_valid,
        "total_invalid": total_invalid,
        "total_inserted": total_inserted,
        "avg_batch_time": overall_avg,
        "elapsed_seconds": elapsed,
        "export_summary": export_summary,
    }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="NIDS Transform & Load Stage -- read JSON files and ingest into SQLite"
    )
    parser.add_argument(
        "--json-dir", type=str, default=None,
        help="Directory containing rows_offset_*.json files (default: data/raw_json/)"
    )
    parser.add_argument(
        "--full-db", type=str, default=None,
        help="Path to the full SQLite database (default: data/nids_full.db)"
    )
    parser.add_argument(
        "--deploy-db", type=str, default=None,
        help="Path to the deployment SQLite database (default: data/nids_deploy.db)"
    )
    parser.add_argument(
        "--no-export", action="store_true",
        help="Skip exporting nids_deploy.db after ingestion"
    )

    args = parser.parse_args()

    json_dir = Path(args.json_dir) if args.json_dir else None
    full_db = Path(args.full_db) if args.full_db else None
    deploy_db = Path(args.deploy_db) if args.deploy_db else None

    try:
        run_etl(
            json_dir=json_dir,
            full_db_path=full_db,
            deploy_db_path=deploy_db,
            do_export=not args.no_export,
        )
    except Exception as e:
        print(f"\n[FATAL ERROR in ETL]: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
