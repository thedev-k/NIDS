import argparse
import sys
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
import concurrent.futures
import pandas as pd

from etl.client import (
    fetch_row_batch_with_retry,
    verify_dataset_accessibility,
    get_dataset_total_rows,
    DATASET_REPO,
    MAX_ROWS_PER_REQUEST,
    DEFAULT_PAUSE_SECONDS,
)
from etl.cleaner import clean_dataframe
from etl.schema import validate_records
from etl.loader import get_db_connection, load_records_idempotently, is_offset_loaded
from etl.export_deploy_db import export_deploy_db

# Hard limit of 2 worker threads to avoid triggering Hugging Face 429 rate limits
MAX_PARALLEL_WORKERS = 2
MAX_SAFETY_ROWS = 30000


def generate_spread_offsets(total_rows: int, target_rows: int, chunk_size: int = 100) -> List[int]:
    """
    Generates strictly unique, evenly spaced offsets across the full dataset.
    Guarantees no offset appears more than once.
    """
    chunk_size = min(MAX_ROWS_PER_REQUEST, max(1, chunk_size))
    num_requests = (target_rows + chunk_size - 1) // chunk_size
    step = (total_rows - chunk_size) / max(1, num_requests)

    unique_offsets = []
    seen = set()

    for i in range(num_requests):
        off = int(i * step)
        clamped = max(0, min(off, total_rows - chunk_size))
        if clamped not in seen:
            seen.add(clamped)
            unique_offsets.append(clamped)

    # If any duplicates occurred from rounding/clamping, fill gaps with distinct offsets
    fill_candidate = 0
    while len(unique_offsets) < num_requests and fill_candidate < total_rows - chunk_size:
        if fill_candidate not in seen:
            seen.add(fill_candidate)
            unique_offsets.append(fill_candidate)
        fill_candidate += chunk_size

    unique_offsets.sort()
    return unique_offsets[:num_requests]


def fetch_worker_batch(offset: int, chunk_size: int, pause_seconds: float) -> Dict[str, Any]:
    """
    Worker task: fetches a single batch of JSON rows with aggressive retry, backoff,
    and a polite pause between requests on this worker thread.
    """
    t0 = time.time()
    rows = fetch_row_batch_with_retry(offset=offset, length=chunk_size)
    fetch_time = time.time() - t0

    if pause_seconds > 0:
        time.sleep(pause_seconds)

    return {
        "offset": offset,
        "rows": rows,
        "fetch_time": fetch_time,
    }


def run_etl(
    max_rows: int = 30000,
    batch_size: int = 100,
    workers: int = 2,
    pause_seconds: float = DEFAULT_PAUSE_SECONDS,
    full_db_path: Path = None,
    deploy_db_path: Path = None,
    do_export: bool = True
) -> dict:
    """
    Main Multi-Threaded ETL Pipeline:
    - Enforces max 2 parallel worker threads to prevent HF 429 rate limiting.
    - Aggressive retry with exponential backoff on HTTP 429 / 5xx / timeouts.
    - Polite pause between requests per worker.
    - Idempotent offset tracking ensuring the same offset is never processed or inserted twice.
    - Real-time batch timing and progress logging.
    - Automatic deployment database export and class distribution summary.
    """
    if max_rows > MAX_SAFETY_ROWS:
        raise ValueError(
            f"max_rows ({max_rows:,}) exceeds safety threshold of {MAX_SAFETY_ROWS:,}. "
            f"User approval required before running larger extractions."
        )

    # Restrict to maximum 2 worker threads
    active_workers = min(max(1, workers), MAX_PARALLEL_WORKERS)

    base_dir = Path(__file__).resolve().parent.parent
    if full_db_path is None:
        full_db_path = base_dir / "data" / "nids_full.db"
    if deploy_db_path is None:
        deploy_db_path = base_dir / "data" / "nids_deploy.db"

    print("==================================================")
    print("NIDS Multi-Threaded ETL Pipeline Started")
    print(f"Data Source   : Hugging Face Datasets Server REST API")
    print(f"Dataset       : {DATASET_REPO}")
    print(f"Max Rows      : {max_rows:,}")
    print(f"Workers       : {active_workers} parallel threads (max allowed: {MAX_PARALLEL_WORKERS})")
    print(f"Polite Pause  : {pause_seconds}s per worker request")
    print(f"Full DB Path  : {full_db_path}")
    print(f"Deploy DB     : {deploy_db_path}")
    print("==================================================")
    sys.stdout.flush()

    # Step 0: Verify API connectivity
    print("\n[Step 0] Verifying Hugging Face Datasets API connectivity...")
    sys.stdout.flush()
    api_status = verify_dataset_accessibility()
    print(f"  Dataset          : {api_status.get('dataset')}")
    print(f"  Config / Split   : {api_status.get('config')} / {api_status.get('split')}")
    print(f"  Viewer Enabled   : {api_status.get('viewer')}")
    print(f"  Test /rows Status: {api_status.get('test_rows_status')}")
    print(f"  Columns Found    : {len(api_status.get('sample_columns', []))}")
    sys.stdout.flush()

    # Step 1: Generate strictly unique spread offsets
    total_rows = get_dataset_total_rows()
    chunk_size = min(MAX_ROWS_PER_REQUEST, max(1, batch_size))
    offsets = generate_spread_offsets(total_rows, max_rows, chunk_size)
    total_expected_batches = len(offsets)

    print(f"\n[Step 1] Initializing extraction of {max_rows:,} rows across {total_expected_batches} unique offsets...")
    print(f"  Dataset Total Rows: {total_rows:,}")
    print(f"  Unique Offsets    : {len(offsets)} (from {offsets[0]} to {offsets[-1]})")
    print(f"  Parallel Workers  : {active_workers}")
    sys.stdout.flush()

    start_time = time.time()
    conn = get_db_connection(full_db_path)

    total_fetched = 0
    total_cleaned = 0
    total_valid = 0
    total_invalid = 0
    total_inserted = 0
    completed_batches = 0
    batch_durations: List[float] = []
    processed_offsets = set()

    # Pre-populate counts if resuming an existing database
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM etl_offsets;")
        completed_batches = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM network_flows;")
        total_inserted = cur.fetchone()[0]
        if completed_batches > 0:
            print(f"  [Resume Detection] Found {completed_batches} existing batches and {total_inserted:,} flows in DB. Resuming remaining offsets...")
            sys.stdout.flush()
    except Exception:
        pass

    try:
        # Step 2: Concurrently fetch with ThreadPoolExecutor (strictly max 2 workers, controlled queue)
        with concurrent.futures.ThreadPoolExecutor(max_workers=active_workers) as executor:
            future_to_offset = {}
            offset_iter = iter(offsets)

            # Prime executor with active_workers initial tasks
            for _ in range(active_workers):
                try:
                    off = next(offset_iter)
                    while is_offset_loaded(conn, off):
                        print(f"  [Skip] Offset {off} already loaded in database. Skipping duplicate.")
                        off = next(offset_iter)
                    fut = executor.submit(fetch_worker_batch, off, chunk_size, pause_seconds)
                    future_to_offset[fut] = off
                except StopIteration:
                    break

            print(f"  Initialized {len(future_to_offset)} concurrent workers (max inflight: {active_workers}).\n")
            sys.stdout.flush()

            while future_to_offset:
                done, _ = concurrent.futures.wait(
                    future_to_offset.keys(),
                    return_when=concurrent.futures.FIRST_COMPLETED
                )

                for future in done:
                    offset = future_to_offset.pop(future)
                    batch_start_ts = time.time()

                    if offset in processed_offsets or is_offset_loaded(conn, offset):
                        print(f"  [Warning] Offset {offset} was already processed. Skipping duplicate.")
                    else:
                        res = future.result()
                        raw_rows = res["rows"]
                        fetch_time = res["fetch_time"]

                        fetched_count = len(raw_rows)
                        total_fetched += fetched_count

                        # Clean with pandas
                        df_raw = pd.DataFrame(raw_rows)
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
                        processed_offsets.add(offset)

                        completed_batches += 1
                        batch_total_time = (time.time() - batch_start_ts) + fetch_time
                        batch_durations.append(batch_total_time)
                        avg_batch_time = sum(batch_durations) / len(batch_durations)

                        remaining_batches = total_expected_batches - completed_batches
                        eta_seconds = (remaining_batches * avg_batch_time) / active_workers
                        eta_min = eta_seconds / 60.0

                        print(
                            f"  [Batch {completed_batches}/{total_expected_batches}] "
                            f"Offset: {offset} | "
                            f"Time: {batch_total_time:.2f}s (fetch: {fetch_time:.2f}s) | "
                            f"Avg: {avg_batch_time:.2f}s/batch | "
                            f"Fetched: {fetched_count} | "
                            f"Valid: {len(valid_records)} | "
                            f"New: {new_inserted} | "
                            f"Total DB: {total_inserted:,} | "
                            f"ETA: {eta_min:.1f}m"
                        )
                        sys.stdout.flush()

                    # Replenish queue with next unique offset
                    try:
                        next_off = next(offset_iter)
                        while is_offset_loaded(conn, next_off):
                            print(f"  [Skip] Offset {next_off} already in DB. Skipping duplicate.")
                            next_off = next(offset_iter)
                        fut = executor.submit(fetch_worker_batch, next_off, chunk_size, pause_seconds)
                        future_to_offset[fut] = next_off
                    except StopIteration:
                        pass

    finally:
        conn.close()

    total_elapsed = round(time.time() - start_time, 2)
    overall_avg_batch = sum(batch_durations) / max(1, len(batch_durations))

    print("\n--------------------------------------------------")
    print(f"ETL Ingestion Complete in {total_elapsed:.1f}s ({total_elapsed/60:.2f} minutes):")
    print(f"  Completed Batches      : {completed_batches}/{total_expected_batches}")
    print(f"  Average Time Per Batch : {overall_avg_batch:.2f} seconds")
    print(f"  Total Fetched via HTTP : {total_fetched:,}")
    print(f"  Total Cleaned Rows     : {total_cleaned:,}")
    print(f"  Valid Pydantic Records : {total_valid:,}")
    print(f"  Invalid / Rejected     : {total_invalid:,}")
    print(f"  Total Inserted to DB   : {total_inserted:,}")
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
        "total_fetched": total_fetched,
        "total_cleaned": total_cleaned,
        "total_valid": total_valid,
        "total_invalid": total_invalid,
        "total_inserted": total_inserted,
        "completed_batches": completed_batches,
        "avg_batch_time": overall_avg_batch,
        "elapsed_seconds": total_elapsed,
        "export_summary": export_summary
    }


def main():
    parser = argparse.ArgumentParser(description="Run NIDS ETL Pipeline with 2 Worker Threads")
    parser.add_argument("--max-rows", type=int, default=30000, help="Maximum total rows to ingest (max 30,000)")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size per request (max 100 on HF /rows)")
    parser.add_argument("--workers", type=int, default=2, help="Parallel worker threads (max 2 allowed)")
    parser.add_argument("--pause", type=float, default=DEFAULT_PAUSE_SECONDS, help="Polite pause between worker requests (seconds)")
    parser.add_argument("--no-export", action="store_true", help="Skip exporting nids_deploy.db after ingestion")

    args = parser.parse_args()

    if args.max_rows > MAX_SAFETY_ROWS:
        print(f"ERROR: --max-rows ({args.max_rows:,}) exceeds {MAX_SAFETY_ROWS:,} safety threshold. Explicit user approval required.", file=sys.stderr)
        sys.exit(1)

    try:
        run_etl(
            max_rows=args.max_rows,
            batch_size=args.batch_size,
            workers=min(args.workers, MAX_PARALLEL_WORKERS),
            pause_seconds=args.pause,
            do_export=not args.no_export
        )
    except Exception as e:
        print(f"\n[FATAL ERROR in ETL]: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
