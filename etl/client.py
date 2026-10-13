"""
Hugging Face Datasets Server REST API Client for CIC-IDS2017.

Fetches flow records in JSON format directly from the Hugging Face
Datasets Server `/rows` endpoint (https://datasets-server.huggingface.co/rows).
Uses requests with retry, exponential backoff, and pauses between requests.
Offsets are spread evenly across the dataset to sample diverse attack types.
"""

import sys
import time
import requests
from typing import List, Dict, Any, Optional

import threading

DATASET_REPO = "c01dsnap/CIC-IDS2017"
CONFIG_NAME = "default"
SPLIT_NAME = "train"
HF_ROWS_URL = "https://datasets-server.huggingface.co/rows"
HF_SIZE_URL = "https://datasets-server.huggingface.co/size"
HF_VALID_URL = "https://datasets-server.huggingface.co/is-valid"
HF_SPLITS_URL = "https://datasets-server.huggingface.co/splits"

MAX_ROWS_PER_REQUEST = 100
DEFAULT_PAUSE_SECONDS = 1.0

_rate_limit_lock = threading.Lock()
_rate_limit_cooldown_until = 0.0

_pacer_lock = threading.Lock()
_last_request_time = 0.0
# Hugging Face anonymous rate limit is ~35-40 req/min. Setting 2.2s enforces max ~27 req/min globally.
MIN_SECONDS_BETWEEN_REQUESTS = 2.2


def pace_request():
    """Globally paces outgoing requests across all worker threads to safely stay below 429 limits."""
    global _last_request_time
    with _pacer_lock:
        now = time.time()
        elapsed = now - _last_request_time
        if elapsed < MIN_SECONDS_BETWEEN_REQUESTS:
            time.sleep(MIN_SECONDS_BETWEEN_REQUESTS - elapsed)
        _last_request_time = time.time()


def check_and_wait_rate_limit_cooldown():
    """Ensures worker threads do not send requests during an active 429 cooldown window."""
    global _rate_limit_cooldown_until
    with _rate_limit_lock:
        remaining = _rate_limit_cooldown_until - time.time()
    if remaining > 0:
        print(f"  [Rate Limit Guard] Worker waiting {remaining:.1f}s for global cooldown...")
        sys.stdout.flush()
        time.sleep(remaining + 0.5)


def trigger_rate_limit_cooldown(seconds: float):
    """Sets a global cooldown timestamp shared across all worker threads."""
    global _rate_limit_cooldown_until
    with _rate_limit_lock:
        _rate_limit_cooldown_until = max(_rate_limit_cooldown_until, time.time() + seconds)


def verify_dataset_accessibility() -> Dict[str, Any]:
    """
    Verifies that the dataset is valid and the /rows endpoint returns 200.
    """
    r_valid = requests.get(HF_VALID_URL, params={"dataset": DATASET_REPO}, timeout=20)
    r_valid.raise_for_status()
    valid_data = r_valid.json()

    r_test = requests.get(
        HF_ROWS_URL,
        params={
            "dataset": DATASET_REPO,
            "config": CONFIG_NAME,
            "split": SPLIT_NAME,
            "offset": 0,
            "length": 5,
        },
        timeout=20,
    )
    r_test.raise_for_status()
    test_data = r_test.json()

    return {
        "dataset": DATASET_REPO,
        "config": CONFIG_NAME,
        "split": SPLIT_NAME,
        "viewer": valid_data.get("viewer", False),
        "test_rows_status": r_test.status_code,
        "sample_columns": list(test_data["rows"][0]["row"].keys()) if test_data.get("rows") else [],
    }


def get_dataset_total_rows() -> int:
    """
    Queries the /size endpoint to obtain the total row count for the dataset split.
    """
    try:
        r = requests.get(HF_SIZE_URL, params={"dataset": DATASET_REPO}, timeout=20)
        if r.status_code == 200:
            size_data = r.json()
            splits = size_data.get("size", {}).get("splits", [])
            for s in splits:
                if s.get("config") == CONFIG_NAME and s.get("split") == SPLIT_NAME:
                    return int(s.get("num_rows", 2830743))
            num_rows = size_data.get("size", {}).get("dataset", {}).get("num_rows")
            if num_rows:
                return int(num_rows)
    except Exception as e:
        print(f"  [Warning] Could not fetch total rows from /size: {e}")

    # Fallback to known row count for c01dsnap/CIC-IDS2017
    return 2830743


def fetch_row_batch_with_retry(
    offset: int,
    length: int,
    max_retries: int = 15,
    timeout: int = 30,
) -> List[Dict[str, Any]]:
    """
    Fetches a single batch of JSON rows from the /rows endpoint with aggressive retry and backoff.
    Coordinates across threads with a shared rate-limit cooldown lock.
    """
    length = min(length, MAX_ROWS_PER_REQUEST)
    params = {
        "dataset": DATASET_REPO,
        "config": CONFIG_NAME,
        "split": SPLIT_NAME,
        "offset": offset,
        "length": length,
    }

    last_exception = None
    for attempt in range(1, max_retries + 1):
        # Wait if another worker recently triggered a rate limit cooldown
        check_and_wait_rate_limit_cooldown()
        # Enforce polite spacing between outgoing requests
        pace_request()

        try:
            resp = requests.get(HF_ROWS_URL, params=params, timeout=timeout)
            if resp.status_code == 200:
                payload = resp.json()
                rows = [item["row"] for item in payload.get("rows", [])]
                return rows
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
                print(f"  [429 Rate Limit] Offset {offset} attempt {attempt}/{max_retries} throttled. Global cooldown: {backoff:.1f}s...")
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
        print(f"  [HTTP Retry] Offset {offset} attempt {attempt}/{max_retries} failed ({last_exception}). Retrying in {backoff:.1f}s...")
        sys.stdout.flush()
        time.sleep(backoff)

    raise RuntimeError(f"Failed to fetch rows at offset {offset} after {max_retries} attempts: {last_exception}")


def fetch_rows_spread(
    max_rows: int,
    batch_size: int = 100,
    pause_seconds: float = DEFAULT_PAUSE_SECONDS,
) -> List[List[Dict[str, Any]]]:
    """
    Fetches rows spread evenly across the dataset by requesting chunks from different offsets.
    The Hugging Face Datasets Server allows at most 100 rows per request.

    Returns a list of raw record batches (each batch is a list of dicts).
    """
    if max_rows > 30000:
        raise ValueError("max_rows exceeds 30,000 safety threshold without explicit user approval.")

    total_dataset_rows = get_dataset_total_rows()
    print(f"  [Dataset] Total rows in {DATASET_REPO}: {total_dataset_rows:,}")
    print(f"  [Dataset] Target extraction: {max_rows:,} rows")

    chunk_size = min(MAX_ROWS_PER_REQUEST, max(1, batch_size))
    num_requests = (max_rows + chunk_size - 1) // chunk_size

    # Compute evenly spaced offsets
    step = (total_dataset_rows - chunk_size) / max(1, num_requests)
    offsets = [int(i * step) for i in range(num_requests)]
    # Ensure all offsets are valid and within bounds
    offsets = [min(off, total_dataset_rows - chunk_size) for off in offsets]
    print(f"  [Dataset] Planned {len(offsets)} requests of up to {chunk_size} rows, spread from offset {offsets[0]} to {offsets[-1]}")

    all_batches = []
    total_fetched = 0

    for i, offset in enumerate(offsets, 1):
        remaining = max_rows - total_fetched
        current_len = min(chunk_size, remaining)
        if current_len <= 0:
            break

        rows = fetch_row_batch_with_retry(offset=offset, length=current_len)
        if rows:
            all_batches.append(rows)
            total_fetched += len(rows)

        if i % 10 == 0 or i == len(offsets):
            print(f"  [Progress] {total_fetched:,}/{max_rows:,} rows fetched ({i}/{len(offsets)} requests)")

        if pause_seconds > 0 and i < len(offsets):
            time.sleep(pause_seconds)

    return all_batches
