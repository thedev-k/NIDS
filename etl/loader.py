import sqlite3
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd


def get_db_connection(db_path: Path) -> sqlite3.Connection:
    """Creates SQLite connection with performance optimizations."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    return conn


def init_schema(conn: sqlite3.Connection, sample_record: Dict[str, Any]) -> None:
    """
    Initializes network_flows table conforming to PRD Section 3.5.
    Dynamically accommodates all CICIDS2017 numeric features.
    """
    cursor = conn.cursor()

    # Core required columns from PRD 3.5
    columns = [
        "flow_id TEXT PRIMARY KEY",
        "destination_port INTEGER NOT NULL",
        "flow_duration INTEGER NOT NULL",
        "total_fwd_packets INTEGER NOT NULL",
        "total_backward_packets INTEGER NOT NULL",
        "packet_length_mean REAL NOT NULL",
        "flow_bytes_s REAL NOT NULL",
        "flow_packets_s REAL NOT NULL",
    ]

    core_names = {
        "flow_id", "destination_port", "flow_duration", "total_fwd_packets",
        "total_backward_packets", "packet_length_mean", "flow_bytes_s",
        "flow_packets_s", "label", "is_attack"
    }

    # Add extra columns from dataset
    for col, val in sample_record.items():
        if col in core_names:
            continue
        if isinstance(val, int):
            col_type = "INTEGER"
        elif isinstance(val, float):
            col_type = "REAL"
        else:
            col_type = "TEXT"
        columns.append(f'"{col}" {col_type}')

    columns.append("label TEXT NOT NULL")
    columns.append("is_attack INTEGER NOT NULL")

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS network_flows (
        {', '.join(columns)}
    );
    """
    cursor.execute(create_table_sql)

    # Indices
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_attack_flag ON network_flows(is_attack);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_label ON network_flows(label);")
    conn.commit()


def load_records_idempotently(conn: sqlite3.Connection, records: List[Dict[str, Any]]) -> int:
    """
    Loads validated records into network_flows table idempotently using INSERT OR IGNORE.
    Returns the count of new rows inserted.
    """
    if not records:
        return 0

    # Ensure table exists
    init_schema(conn, records[0])

    # Get existing columns in table
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(network_flows);")
    table_cols = [row[1] for row in cursor.fetchall()]

    # Filter record keys to those in table
    clean_records = []
    for r in records:
        filtered = {k: r.get(k) for k in table_cols if k in r}
        clean_records.append(filtered)

    df_batch = pd.DataFrame(clean_records)
    if df_batch.empty:
        return 0

    cols = df_batch.columns.tolist()
    placeholders = ", ".join(["?"] * len(cols))
    col_str = ", ".join([f'"{c}"' for c in cols])
    insert_sql = f"INSERT OR IGNORE INTO network_flows ({col_str}) VALUES ({placeholders})"

    initial_changes = conn.total_changes
    cursor.executemany(insert_sql, df_batch.values.tolist())
    conn.commit()
    new_rows = conn.total_changes - initial_changes

    return new_rows
