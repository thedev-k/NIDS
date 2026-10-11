import os
import glob
from pathlib import Path
from typing import List, Dict, Any, Optional
import numpy as np
import pandas as pd
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="NIDS Source API",
    description="Local source API serving CICIDS2017 flow records in JSON batches for ETL ingestion.",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = BASE_DIR / "data" / "raw"

# Cache for file row counts to enable efficient offset/limit across multiple files
_file_catalog = []
_total_rows = 0
_catalog_initialized = False


def get_raw_csv_files() -> List[Path]:
    """Find all CSV files in data/raw directory."""
    if not RAW_DATA_DIR.exists():
        RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    return sorted(list(RAW_DATA_DIR.glob("*.csv")))


def build_catalog() -> List[Dict[str, Any]]:
    """Scan raw CSV files and build an offset catalog."""
    global _file_catalog, _total_rows, _catalog_initialized
    csv_files = get_raw_csv_files()
    catalog = []
    running_offset = 0

    for f in csv_files:
        try:
            # Quick line count to estimate rows (subtract 1 for header)
            with open(f, "rb") as fp:
                lines = sum(1 for _ in fp)
            row_count = max(0, lines - 1)
        except Exception:
            row_count = 0

        catalog.append({
            "path": f,
            "filename": f.name,
            "start_offset": running_offset,
            "end_offset": running_offset + row_count,
            "row_count": row_count
        })
        running_offset += row_count

    _file_catalog = catalog
    _total_rows = running_offset
    _catalog_initialized = True
    return _file_catalog


@app.on_event("startup")
def startup_event():
    build_catalog()


@app.get("/health")
def health_check():
    return {"status": "ok", "service": "nids-source-api"}


@app.get("/status")
def get_status():
    global _catalog_initialized, _total_rows, _file_catalog
    if not _catalog_initialized:
        build_catalog()
    return {
        "status": "online",
        "raw_data_dir": str(RAW_DATA_DIR),
        "total_files": len(_file_catalog),
        "total_rows": _total_rows,
        "files": [
            {"filename": item["filename"], "row_count": item["row_count"]}
            for item in _file_catalog
        ]
    }


@app.get("/refresh-catalog")
def refresh_catalog():
    build_catalog()
    return {"status": "refreshed", "total_files": len(_file_catalog), "total_rows": _total_rows}


@app.get("/flows", response_model=List[Dict[str, Any]])
def get_flows(
    offset: int = Query(0, ge=0, description="0-indexed row offset across all CSV files"),
    limit: int = Query(5000, ge=1, le=50000, description="Batch size of records to return")
):
    """
    Serves network flow records as a JSON payload in batches.
    Matches PRD 3.2: GET /flows with offset and limit parameters.
    """
    global _catalog_initialized, _file_catalog, _total_rows
    if not _catalog_initialized:
        build_catalog()

    if not _file_catalog or _total_rows == 0:
        return []

    if offset >= _total_rows:
        return []

    records: List[Dict[str, Any]] = []
    remaining_needed = limit
    current_offset = offset

    for entry in _file_catalog:
        if remaining_needed <= 0:
            break

        start = entry["start_offset"]
        end = entry["end_offset"]

        # Check if the requested range overlaps with this file
        if current_offset < end and (current_offset + remaining_needed) > start:
            file_skip = max(0, current_offset - start)
            file_limit = min(remaining_needed, entry["row_count"] - file_skip)

            if file_limit <= 0:
                continue

            try:
                # Read specific slice from the CSV.
                # skiprows=range(1, file_skip + 1) skips the pre-offset data rows while preserving row 0 header.
                skip_rows = range(1, file_skip + 1) if file_skip > 0 else None
                df_chunk = pd.read_csv(
                    entry["path"],
                    skiprows=skip_rows,
                    nrows=file_limit,
                    low_memory=False,
                    encoding="latin-1"
                )

                # Raw CICIDS2017 features may contain inf/NaN; replace with None for standard JSON serialization
                chunk_records = df_chunk.replace([np.inf, -np.inf], None).where(pd.notnull(df_chunk), None).to_dict(orient="records")
                records.extend(chunk_records)

                fetched_count = len(chunk_records)
                remaining_needed -= fetched_count
                current_offset += fetched_count
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error reading {entry['filename']}: {str(e)}")

    return records


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("source_api.main:app", host="127.0.0.1", port=8000, reload=True)
