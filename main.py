import json
import sqlite3
from pathlib import Path
from typing import Dict, Any, List
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

# Requirement 4: Open database and static folder using paths based on location of main.py
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "nids_deploy.db"
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(
    title="NIDS Dashboard",
    description="Exploratory Data Analysis Dashboard for AI-Powered Network Intrusion Detection System",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db():
    if not DB_PATH.exists():
        raise HTTPException(
            status_code=503,
            detail="Deployment database (data/nids_deploy.db) not found. Run the ETL pipeline to generate it."
        )
    # Open SQLite in read-only mode for safety and concurrent access
    conn = sqlite3.connect(f"file:{DB_PATH.as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


@app.get("/api/health")
def health():
    return {
        "status": "online",
        "db_exists": DB_PATH.exists(),
        "db_path": str(DB_PATH)
    }


@app.get("/api/kpis")
def get_kpis() -> Dict[str, Any]:
    """Returns top-level metric counters."""
    if not DB_PATH.exists():
        return {
            "total_flows": 0,
            "benign_count": 0,
            "attack_count": 0,
            "attack_percentage": 0.0,
            "unique_attack_types": 0,
            "ready": False
        }

    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM eda_summary_kpis LIMIT 1;")
        row = cursor.fetchone()
        conn.close()
        if row:
            res = dict(row)
            res["ready"] = True
            return res
    except Exception as e:
        pass
    return {"total_flows": 0, "benign_count": 0, "attack_count": 0, "attack_percentage": 0.0, "unique_attack_types": 0, "ready": False}


@app.get("/api/class-distribution")
def get_class_distribution() -> List[Dict[str, Any]]:
    """Returns flow counts and percentages by attack label and binary indicator."""
    if not DB_PATH.exists():
        return []
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT label, is_attack, count, percentage FROM eda_class_distribution ORDER BY count DESC;")
        rows = [dict(r) for r in cursor.fetchall()]
        conn.close()
        return rows
    except Exception:
        return []


@app.get("/api/correlation-matrix")
def get_correlation_matrix() -> Dict[str, Any]:
    """
    Returns precomputed correlation matrix of the 10-15 most correlated features + is_attack.
    Conforms to Requirement 2 for real Plotly.js heatmap rendering.
    """
    if not DB_PATH.exists():
        return {"features": [], "matrix": [], "correlations_with_attack": {}}

    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT data_json FROM eda_metadata WHERE config_key = 'correlation_matrix' LIMIT 1;")
        row = cursor.fetchone()
        conn.close()
        if row:
            return json.loads(row["data_json"])
    except Exception:
        pass
    return {"features": [], "matrix": [], "correlations_with_attack": {}}


@app.get("/api/scatter-sample")
def get_scatter_sample() -> List[Dict[str, Any]]:
    """
    Returns stratified sample points partitioned across attack labels (including DDoS).
    Conforms to Requirement 3 for volumetric profiling.
    """
    if not DB_PATH.exists():
        return []

    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM eda_scatter_sample;")
        rows = [dict(r) for r in cursor.fetchall()]
        conn.close()
        return rows
    except Exception:
        return []


# Mount static assets directory
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
def serve_index():
    index_file = STATIC_DIR / "index.html"
    if index_file.exists():
        return FileResponse(index_file)
    return {"message": "NIDS EDA Dashboard API is running. UI static file not found."}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8050, reload=True)
