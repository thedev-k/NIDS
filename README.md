# NIDS Project

AI-Powered Network Intrusion Detection System (JSON API Edition).

## Project 1 Architecture: Ingestion, ETL & EDA

Project 1 is split into two cleanly decoupled parts:
1. **Local Pipeline (`source_api/` & `etl/`)**:
   - `source_api`: FastAPI service serving CICIDS2017 flow records in JSON batches over HTTP (`GET /flows?offset={int}&limit={int}`).
   - `etl`: Programmatic ingestion via HTTP (`requests`), data cleaning with `pandas` (stripping spaces, snake_case normalization, infinite/NaN handling, deduplication, downcasting), validation with `pydantic` (`NetworkFlow` schema), idempotent loading into `data/nids_full.db` (SQLite), and precomputing summary tables & stratified samples into `data/nids_deploy.db`.
2. **Deployed Dashboard (`main.py` & `static/`)**:
   - Lightweight root FastAPI application reading strictly from the small, read-only `data/nids_deploy.db`.
   - Rich, interactive EDA dashboard with Plotly.js charts (Binary & Multi-class Imbalance, full feature Correlation Heatmap, Volumetric Profiling scatter plot). Zero Streamlit.

---

## Quickstart Guide

### 1. Installation
For local development and running the ETL:
```bash
pip install -r requirements-dev.txt
```
For running the production dashboard only:
```bash
pip install -r requirements.txt
```

### 2. Run Local Source API
Place your CICIDS2017 CSV files into `data/raw/`, then start the source API:
```bash
python -m uvicorn source_api.main:app --host 127.0.0.1 --port 8000
```

### 3. Run ETL Pipeline
Run a quick test ingestion (e.g. 100 rows):
```bash
python -m etl.run_etl --api-url http://127.0.0.1:8000/flows --max-rows 100
```
Run the full ingestion:
```bash
python -m etl.run_etl --api-url http://127.0.0.1:8000/flows
```

### 4. Launch the EDA Dashboard
Start the root dashboard server:
```bash
python main.py
```
Open [http://localhost:8050](http://localhost:8050) in your browser.
