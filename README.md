# NIDS Project

AI-Powered Network Intrusion Detection System (JSON API Edition).

## Project 1 Architecture: Ingestion, ETL & EDA

Project 1 is split into two cleanly decoupled parts:
1. **ETL Pipeline (`etl/`)**:
   - `etl`: Programmatic ingestion via HTTP (`requests`) from Hugging Face Datasets Server REST API (`rdpahalavan/CIC-IDS2017`), data cleaning with `pandas` (stripping spaces, snake_case normalization, infinite/NaN handling, deduplication, downcasting), validation with `pydantic` (`NetworkFlow` schema), idempotent loading into `data/nids_full.db` (SQLite), and precomputing summary tables & stratified samples into `data/nids_deploy.db`.
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

### 2. Run ETL Pipeline
Ingest data programmatically from Hugging Face (e.g. 5,000 rows spread across dataset):
```bash
python -m etl.run_etl --max-rows 5000
```


### 3. Launch the EDA Dashboard
Start the root dashboard server:
```bash
python main.py
```
Open [http://localhost:8050](http://localhost:8050) in your browser.
