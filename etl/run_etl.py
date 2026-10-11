import argparse
import sys
import time
from pathlib import Path
import pandas as pd

from etl.client import fetch_network_flows_json
from etl.cleaner import clean_dataframe
from etl.schema import validate_records
from etl.loader import get_db_connection, load_records_idempotently
from etl.export_deploy_db import export_deploy_db


def run_etl(
    api_url: str = "http://127.0.0.1:8000/flows",
    batch_size: int = 5000,
    max_rows: int = None,
    full_db_path: Path = None,
    deploy_db_path: Path = None,
    do_export: bool = True
) -> dict:
    """
    Main ETL execution pipeline:
    1. Fetches batches from REST API over HTTP with requests.
    2. Cleans raw batch data using pandas.
    3. Validates batch records using Pydantic.
    4. Loads records idempotently into full SQLite database.
    5. Exports precomputed summaries and stratified sample to deploy database.
    """
    base_dir = Path(__file__).resolve().parent.parent
    if full_db_path is None:
        full_db_path = base_dir / "data" / "nids_full.db"
    if deploy_db_path is None:
        deploy_db_path = base_dir / "data" / "nids_deploy.db"

    print("==================================================")
    print("NIDS ETL Pipeline Started")
    print(f"Source API URL: {api_url}")
    print(f"Batch Size    : {batch_size}")
    print(f"Max Rows Limit: {max_rows if max_rows else 'Unlimited (Full Run)'}")
    print(f"Full DB Path  : {full_db_path}")
    print(f"Deploy DB Path: {deploy_db_path}")
    print("==================================================")

    conn = get_db_connection(full_db_path)

    offset = 0
    total_fetched = 0
    total_cleaned = 0
    total_valid = 0
    total_invalid = 0
    total_inserted = 0
    batch_num = 0

    start_time = time.time()

    try:
        while True:
            # Respect max_rows limit if specified
            if max_rows is not None and total_fetched >= max_rows:
                print(f"[ETL] Reached max-rows limit ({max_rows}). Stopping extraction.")
                break

            current_limit = batch_size
            if max_rows is not None:
                current_limit = min(batch_size, max_rows - total_fetched)

            batch_num += 1
            print(f"[Batch {batch_num}] Fetching offset={offset}, limit={current_limit}...")

            raw_records = fetch_network_flows_json(api_url, offset=offset, limit=current_limit)
            if not raw_records:
                print(f"[ETL] No more records returned from API at offset {offset}. Completed ingestion.")
                break

            fetched_count = len(raw_records)
            total_fetched += fetched_count

            # 1. Clean with pandas
            df_raw = pd.DataFrame(raw_records)
            df_cleaned = clean_dataframe(df_raw)
            cleaned_count = len(df_cleaned)
            total_cleaned += cleaned_count

            # 2. Validate with Pydantic
            cleaned_records = df_cleaned.to_dict(orient="records")
            valid_records, invalid_count = validate_records(cleaned_records)
            total_valid += len(valid_records)
            total_invalid += invalid_count

            # 3. Load idempotently into SQLite
            new_inserted = load_records_idempotently(conn, valid_records)
            total_inserted += new_inserted

            print(
                f"[Batch {batch_num}] Fetched: {fetched_count} | "
                f"Cleaned: {cleaned_count} | "
                f"Valid: {len(valid_records)} | "
                f"Invalid: {invalid_count} | "
                f"New Inserted: {new_inserted}"
            )

            offset += fetched_count

            if fetched_count < current_limit:
                print("[ETL] Received partial batch. End of stream reached.")
                break

    finally:
        conn.close()

    elapsed = round(time.time() - start_time, 2)
    print("\n--------------------------------------------------")
    print(f"ETL Ingestion Summary (completed in {elapsed}s):")
    print(f"  Total Fetched via HTTP: {total_fetched}")
    print(f"  Total Cleaned Rows    : {total_cleaned}")
    print(f"  Valid Pydantic Records: {total_valid}")
    print(f"  Invalid / Rejected    : {total_invalid}")
    print(f"  New Inserted to DB    : {total_inserted}")
    print("--------------------------------------------------")

    export_summary = {}
    if do_export and total_valid > 0:
        print("\nExporting precomputed EDA metrics & stratified sample to nids_deploy.db...")
        try:
            export_summary = export_deploy_db(full_db_path, deploy_db_path)
            print(f"Export successful: {export_summary}")
        except Exception as e:
            print(f"[Warning] Failed to export deploy db: {e}")

    return {
        "total_fetched": total_fetched,
        "total_cleaned": total_cleaned,
        "total_valid": total_valid,
        "total_invalid": total_invalid,
        "total_inserted": total_inserted,
        "elapsed_seconds": elapsed,
        "export_summary": export_summary
    }


def main():
    parser = argparse.ArgumentParser(description="Run NIDS ETL Pipeline")
    parser.add_argument("--api-url", type=str, default="http://127.0.0.1:8000/flows", help="Source REST API flows endpoint")
    parser.add_argument("--batch-size", type=int, default=5000, help="Batch limit per HTTP request")
    parser.add_argument("--max-rows", type=int, default=None, help="Maximum total rows to ingest for quick testing")
    parser.add_argument("--no-export", action="store_true", help="Skip exporting nids_deploy.db after ingestion")

    args = parser.parse_args()

    try:
        run_etl(
            api_url=args.api_url,
            batch_size=args.batch_size,
            max_rows=args.max_rows,
            do_export=not args.no_export
        )
    except Exception as e:
        print(f"\n[FATAL ERROR in ETL]: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
