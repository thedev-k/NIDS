"""
Extract Stage -- NIDS ETL Pipeline (Stage 1 of 2).

Fetches raw JSON responses from the Hugging Face Datasets Server /rows endpoint
and saves each response exactly as received to data/raw_json/, named by offset
(e.g. rows_offset_000123400.json).

Key design decisions:
- Skips offsets whose file already exists -> fully resumable.
- Max 2 parallel worker threads to stay below HF 429 rate limit.
- Shares the same global rate-limit pacing / cooldown logic from client.py.
- Saves the raw payload (full response JSON, not just the rows list) so the
  transform stage has every field the API returned.
- Hard ceiling of 30,000 rows unless explicitly overridden by the user.

Usage:
    python -m etl.extract --max-rows 30000
    python -m etl.extract --max-rows 30000 --output-dir data/raw_json
"""

import argparse
import concurrent.futures
import json
import sys
import time
from pathlib import Path
from typing import Optional

import requests

from etl.client import (
    DATASET_REPO,
    CONFIG_NAME,
    SPLIT_NAME,
    HF_ROWS_URL,
    MAX_ROWS_PER_REQUEST,
    DEFAULT_PAUSE_SECONDS,
    get_dataset_total_rows,
    pace_request,
    check_and_wait_rate_limit_cooldown,
    trigger_rate_limit_cooldown,
)

MAX_PARALLEL_WORKERS = 2
MAX_SAFETY_ROWS = 30000

DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data" / "raw_json"


# ---------------------------------------------------------------------------
# Offset helpers
# ---------------------------------------------------------------------------

def _offset_filename(offset: int) -> str:
    """Returns the canonical filename for a given offset."""
    return f"rows_offset_{offset:09d}.json"


def _file_exists(output_dir: Path, offset: int) -> bool:
    """Returns True if the JSON file for this offset already exists (non-empty)."""
    p = output_dir / _offset_filename(offset)
    return p.exists() and p.stat().st_size > 0


def generate_spread_offsets(total_rows: int, target_rows: int, chunk_size: int = 100):
    """
    Generates strictly unique, evenly spaced offsets across the full dataset.
    Mirrors the logic in run_etl.py so this stage is self-contained.
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

    # Fill gaps caused by rounding/clamping
    fill_candidate = 0
    while len(unique_offsets) < num_requests and fill_candidate < total_rows - chunk_size:
        if fill_candidate not in seen:
            seen.add(fill_candidate)
            unique_offsets.append(fill_candidate)
        fill_candidate += chunk_size

    unique_offsets.sort()
    return unique_offsets[:num_requests]


# ---------------------------------------------------------------------------
# Raw fetch (saves full response payload, not just the rows list)
# ---------------------------------------------------------------------------

def fetch_and_save_batch(
    offset: int,
    length: int,
    output_dir: Path,
    max_retries: int = 15,
    timeout: int = 30,
    pause_seconds: float = DEFAULT_PAUSE_SECONDS,
) -> dict:
    """
    Fetches a single batch from HF /rows and saves the raw JSON payload to disk.

    Returns a dict with keys: offset, filepath, rows_in_response, fetch_time.
    Raises RuntimeError if all retries are exhausted.
    """
    length = min(length, MAX_ROWS_PER_REQUEST)
    params = {
        "dataset": DATASET_REPO,
        "config": CONFIG_NAME,
        "split": SPLIT_NAME,
        "offset": offset,
        "length": length,
    }

    filepath = output_dir / _offset_filename(offset)
    t0 = time.time()
    last_exception = None

    for attempt in range(1, max_retries + 1):
        check_and_wait_rate_limit_cooldown()
        pace_request()

        try:
            resp = requests.get(HF_ROWS_URL, params=params, timeout=timeout)

            if resp.status_code == 200:
                payload = resp.json()
                # Save raw response exactly as received
                filepath.write_text(
                    json.dumps(payload, ensure_ascii=False, indent=None),
                    encoding="utf-8",
                )
                fetch_time = time.time() - t0

                if pause_seconds > 0:
                    time.sleep(pause_seconds)

                return {
                    "offset": offset,
                    "filepath": str(filepath),
                    "rows_in_response": len(payload.get("rows", [])),
                    "fetch_time": fetch_time,
                }

            elif resp.status_code == 429:
                retry_after_hdr = resp.headers.get("Retry-After")
                if retry_after_hdr:
                    try:
                        backoff = max(float(retry_after_hdr), 20.0)
                    except ValueError:
                        backoff = min(25.0 * (1.5 ** (attempt - 1)), 120.0)
                else:
                    backoff = min(25.0 * (1.5 ** (attempt - 1)), 120.0)

                trigger_rate_limit_cooldown(backoff)
                last_exception = RuntimeError(f"HTTP 429 Rate Limited: {resp.text[:120]}")
                print(
                    f"  [429 Rate Limit] Offset {offset} attempt {attempt}/{max_retries} "
                    f"throttled. Global cooldown: {backoff:.1f}s..."
                )
                sys.stdout.flush()
                time.sleep(backoff)
                continue

            elif resp.status_code in (500, 502, 503, 504):
                last_exception = RuntimeError(f"HTTP {resp.status_code}: {resp.text[:100]}")
            else:
                resp.raise_for_status()

        except (requests.RequestException, RuntimeError) as e:
            last_exception = e

        backoff = min(4.0 * (1.3 ** attempt), 45.0)
        print(
            f"  [HTTP Retry] Offset {offset} attempt {attempt}/{max_retries} "
            f"failed ({last_exception}). Retrying in {backoff:.1f}s..."
        )
        sys.stdout.flush()
        time.sleep(backoff)

    raise RuntimeError(
        f"Failed to fetch offset {offset} after {max_retries} attempts: {last_exception}"
    )


# ---------------------------------------------------------------------------
# Main extract runner
# ---------------------------------------------------------------------------

def run_extract(
    max_rows: int = 30000,
    chunk_size: int = 100,
    workers: int = 2,
    pause_seconds: float = DEFAULT_PAUSE_SECONDS,
    output_dir: Optional[Path] = None,
) -> dict:
    """
    Stage 1: Extract.

    Fetches up to max_rows rows from the HF Datasets Server and saves each raw
    100-row batch as its own JSON file in output_dir. Skips files that already
    exist, enabling clean resume after interruption.

    Returns summary statistics dict.
    """
    if max_rows > MAX_SAFETY_ROWS:
        raise ValueError(
            f"max_rows ({max_rows:,}) exceeds {MAX_SAFETY_ROWS:,} safety ceiling. "
            "Explicit user approval required before running larger extractions."
        )

    active_workers = min(max(1, workers), MAX_PARALLEL_WORKERS)
    chunk_size = min(MAX_ROWS_PER_REQUEST, max(1, chunk_size))

    if output_dir is None:
        output_dir = DEFAULT_OUTPUT_DIR
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("==================================================")
    print("NIDS Extract Stage (Stage 1 of 2)")
    print(f"Data Source   : Hugging Face Datasets Server REST API")
    print(f"Dataset       : {DATASET_REPO}")
    print(f"Max Rows      : {max_rows:,}")
    print(f"Chunk Size    : {chunk_size} rows/request")
    print(f"Workers       : {active_workers} parallel threads (max allowed: {MAX_PARALLEL_WORKERS})")
    print(f"Polite Pause  : {pause_seconds}s per worker request")
    print(f"Output Dir    : {output_dir}")
    print("==================================================")
    sys.stdout.flush()

    total_rows = get_dataset_total_rows()
    offsets = generate_spread_offsets(total_rows, max_rows, chunk_size)
    total_planned = len(offsets)

    # Filter out offsets that already have files on disk
    pending_offsets = [off for off in offsets if not _file_exists(output_dir, off)]
    already_done = total_planned - len(pending_offsets)

    print(f"\n[Offset Plan] {total_planned} total offsets planned")
    print(f"  Already on disk (will skip) : {already_done}")
    print(f"  Pending (to fetch)          : {len(pending_offsets)}")
    if pending_offsets:
        print(f"  Range: offset {pending_offsets[0]} -> {pending_offsets[-1]}")
    sys.stdout.flush()

    if not pending_offsets:
        print("\n[Extract] All offsets already on disk. Nothing to fetch.")
        return {
            "files_written": 0,
            "files_skipped": already_done,
            "total_files_in_dir": total_planned,
            "total_rows_saved": 0,
            "elapsed_seconds": 0.0,
        }

    start_time = time.time()
    files_written = 0
    total_rows_saved = 0
    completed = 0
    batch_durations = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=active_workers) as executor:
        future_to_offset = {}
        offset_iter = iter(pending_offsets)

        # Prime the executor with initial tasks
        for _ in range(active_workers):
            try:
                off = next(offset_iter)
                fut = executor.submit(
                    fetch_and_save_batch, off, chunk_size, output_dir, 15, 30, pause_seconds
                )
                future_to_offset[fut] = off
            except StopIteration:
                break

        print(f"\n  Initialized {len(future_to_offset)} worker(s). Fetching...\n")
        sys.stdout.flush()

        while future_to_offset:
            done, _ = concurrent.futures.wait(
                future_to_offset.keys(),
                return_when=concurrent.futures.FIRST_COMPLETED,
            )

            for future in done:
                offset = future_to_offset.pop(future)
                try:
                    result = future.result()
                    files_written += 1
                    total_rows_saved += result["rows_in_response"]
                    completed += 1
                    batch_durations.append(result["fetch_time"])
                    avg_batch = sum(batch_durations) / len(batch_durations)
                    remaining = len(pending_offsets) - completed
                    eta_sec = (remaining * avg_batch) / active_workers
                    print(
                        f"  [Extract {completed}/{len(pending_offsets)}] "
                        f"offset={offset} | rows={result['rows_in_response']} | "
                        f"fetch={result['fetch_time']:.2f}s | "
                        f"avg={avg_batch:.2f}s | "
                        f"total_saved={total_rows_saved:,} | "
                        f"ETA={eta_sec/60:.1f}m"
                    )
                    sys.stdout.flush()
                except Exception as e:
                    print(f"  [ERROR] Offset {offset} failed permanently: {e}", file=sys.stderr)
                    sys.stdout.flush()

                # Replenish queue
                try:
                    next_off = next(offset_iter)
                    fut = executor.submit(
                        fetch_and_save_batch, next_off, chunk_size, output_dir, 15, 30, pause_seconds
                    )
                    future_to_offset[fut] = next_off
                except StopIteration:
                    pass

    elapsed = round(time.time() - start_time, 2)
    total_files = len(list(output_dir.glob("rows_offset_*.json")))

    print("\n--------------------------------------------------")
    print(f"Extract Stage Complete in {elapsed:.1f}s ({elapsed/60:.2f} minutes)")
    print(f"  Files written this run : {files_written}")
    print(f"  Files skipped (cached) : {already_done}")
    print(f"  Total files in dir     : {total_files}")
    print(f"  Total rows saved       : {total_rows_saved:,}")
    print(f"  Output directory       : {output_dir}")
    print("--------------------------------------------------")
    sys.stdout.flush()

    return {
        "files_written": files_written,
        "files_skipped": already_done,
        "total_files_in_dir": total_files,
        "total_rows_saved": total_rows_saved,
        "elapsed_seconds": elapsed,
    }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="NIDS Extract Stage -- fetch from HF API and dump raw JSON files"
    )
    parser.add_argument(
        "--max-rows", type=int, default=30000,
        help="Maximum total rows to fetch (hard ceiling: 30,000)"
    )
    parser.add_argument(
        "--batch-size", type=int, default=100,
        help="Rows per API request (max 100 enforced by HF)"
    )
    parser.add_argument(
        "--workers", type=int, default=2,
        help="Parallel worker threads (max 2 allowed)"
    )
    parser.add_argument(
        "--pause", type=float, default=DEFAULT_PAUSE_SECONDS,
        help="Polite pause (seconds) between requests per worker"
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Directory to save raw JSON files (default: data/raw_json/)"
    )

    args = parser.parse_args()

    if args.max_rows > MAX_SAFETY_ROWS:
        print(
            f"ERROR: --max-rows ({args.max_rows:,}) exceeds {MAX_SAFETY_ROWS:,} safety ceiling. "
            "Explicit user approval required.",
            file=sys.stderr,
        )
        sys.exit(1)

    output_dir = Path(args.output_dir) if args.output_dir else None

    try:
        run_extract(
            max_rows=args.max_rows,
            chunk_size=args.batch_size,
            workers=min(args.workers, MAX_PARALLEL_WORKERS),
            pause_seconds=args.pause,
            output_dir=output_dir,
        )
    except Exception as e:
        print(f"\n[FATAL ERROR in Extract]: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
