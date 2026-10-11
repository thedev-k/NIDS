import json
import sqlite3
from pathlib import Path
from typing import Dict, Any, List
import numpy as np
import pandas as pd


def export_deploy_db(
    full_db_path: Path,
    deploy_db_path: Path,
    scatter_sample_size: int = 5000,
    top_corr_features_count: int = 12
) -> Dict[str, Any]:
    """
    Exports precomputed EDA summary tables and a stratified sample from nids_full.db
    into a lightweight, deployable database: nids_deploy.db.

    Satisfies:
    - Real precomputed correlation matrix of the 10-15 most correlated features + is_attack.
    - Stratified scatter sample by original attack label (preserving DDoS and rare attacks).
    - Class imbalance analysis.
    """
    if not full_db_path.exists():
        raise FileNotFoundError(f"Full database not found at {full_db_path}")

    deploy_db_path.parent.mkdir(parents=True, exist_ok=True)
    if deploy_db_path.exists():
        deploy_db_path.unlink()

    full_conn = sqlite3.connect(str(full_db_path))
    deploy_conn = sqlite3.connect(str(deploy_db_path))

    # 1. Total counts & Class Imbalance
    class_df = pd.read_sql_query(
        """
        SELECT label, is_attack, COUNT(*) as count
        FROM network_flows
        GROUP BY label, is_attack
        ORDER BY count DESC;
        """,
        full_conn
    )

    total_flows = int(class_df["count"].sum())
    if total_flows == 0:
        raise ValueError("network_flows table is empty. Nothing to export.")

    class_df["percentage"] = (class_df["count"] / total_flows * 100).round(3)
    class_df.to_sql("eda_class_distribution", deploy_conn, if_exists="replace", index=False)

    benign_count = int(class_df[class_df["is_attack"] == 0]["count"].sum())
    attack_count = int(class_df[class_df["is_attack"] == 1]["count"].sum())
    attack_percentage = round((attack_count / total_flows * 100), 2)
    attack_types_count = int(class_df[class_df["is_attack"] == 1]["label"].nunique())

    # Summary KPIs table
    kpis = {
        "total_flows": total_flows,
        "benign_count": benign_count,
        "attack_count": attack_count,
        "attack_percentage": attack_percentage,
        "unique_attack_types": attack_types_count
    }
    pd.DataFrame([kpis]).to_sql("eda_summary_kpis", deploy_conn, if_exists="replace", index=False)

    # 2. Correlation Matrix of top 10-15 most correlated features + is_attack
    # Read numeric columns in chunks or sample if very large, but full DB compute
    # Let's inspect column names first
    cursor = full_conn.cursor()
    cursor.execute("PRAGMA table_info(network_flows);")
    columns_info = cursor.fetchall()
    numeric_cols = [
        col[1] for col in columns_info
        if col[2] in ["INTEGER", "REAL"] and col[1] not in ["flow_id"]
    ]

    # Pull numeric columns to compute correlations
    # For large datasets (2.8M rows), we can select columns
    col_selectors = ", ".join([f'"{c}"' for c in numeric_cols])
    numeric_df = pd.read_sql_query(f"SELECT {col_selectors} FROM network_flows", full_conn)

    # Compute correlation with is_attack
    corrs = {}
    for col in numeric_cols:
        if col == "is_attack":
            continue
        try:
            # Check if non-constant
            if numeric_df[col].std() > 1e-9:
                c = numeric_df[col].corr(numeric_df["is_attack"])
                if not np.isnan(c):
                    corrs[col] = float(c)
        except Exception:
            continue

    # Sort by absolute correlation and pick top N features
    sorted_corrs = sorted(corrs.items(), key=lambda x: abs(x[1]), reverse=True)
    top_feature_names = [f[0] for f in sorted_corrs[:top_corr_features_count]]

    # Form the matrix features list: top features + is_attack
    matrix_features = top_feature_names + ["is_attack"]
    corr_matrix_df = numeric_df[matrix_features].corr().fillna(0.0)

    # Store full correlation matrix in database
    # Format: feature names JSON list and matrix 2D values
    matrix_json = {
        "features": matrix_features,
        "matrix": corr_matrix_df.values.tolist(),
        "correlations_with_attack": {f: corrs.get(f, 0.0) for f in top_feature_names}
    }

    pd.DataFrame([{
        "config_key": "correlation_matrix",
        "data_json": json.dumps(matrix_json)
    }]).to_sql("eda_metadata", deploy_conn, if_exists="replace", index=False)

    # Also store relational version for easy SQL querying
    corr_unstacked = corr_matrix_df.reset_index().melt(id_vars="index")
    corr_unstacked.columns = ["feature_x", "feature_y", "corr_value"]
    corr_unstacked.to_sql("eda_correlation_matrix", deploy_conn, if_exists="replace", index=False)

    # 3. Stratified Scatter Sample by original attack label
    # "Stratify the scatter sample by the original attack label, not only the binary flag, so DDoS is well represented."
    unique_labels = class_df["label"].tolist()
    sampled_frames = []

    # Columns needed for volumetric profiling and analysis
    scatter_cols = [
        "flow_id", "destination_port", "flow_duration", "flow_packets_s",
        "flow_bytes_s", "packet_length_mean", "total_fwd_packets", "label", "is_attack"
    ]
    # Ensure existing in table
    existing_scatter_cols = [c for c in scatter_cols if c in [col[1] for col in columns_info]]
    select_scatter_str = ", ".join([f'"{c}"' for c in existing_scatter_cols])

    # Allocate target sample per class
    # Goal: 50% Benign, 50% Attacks partitioned across attack categories
    # guaranteeing DDoS has strong representation
    total_samples_target = min(scatter_sample_size, total_flows)
    benign_target = min(benign_count, int(total_samples_target * 0.45))
    attack_budget = total_samples_target - benign_target

    attack_labels = [l for l in unique_labels if l.upper() != "BENIGN"]
    num_attack_types = len(attack_labels)

    for label in unique_labels:
        label_count = int(class_df[class_df["label"] == label]["count"].iloc[0])
        if label.upper() == "BENIGN":
            n_sample = benign_target
        else:
            if num_attack_types > 0:
                # Proportional allocation with minimum guarantee of at least 50 (or all if < 50)
                prop = label_count / max(1, attack_count)
                n_sample = max(min(label_count, 100), int(attack_budget * prop))
                n_sample = min(n_sample, label_count)
            else:
                n_sample = label_count

        if n_sample <= 0:
            continue

        query = f"""
        SELECT {select_scatter_str}
        FROM network_flows
        WHERE label = ?
        ORDER BY RANDOM()
        LIMIT {n_sample};
        """
        sub_df = pd.read_sql_query(query, full_conn, params=(label,))
        sampled_frames.append(sub_df)

    if sampled_frames:
        scatter_sample_df = pd.concat(sampled_frames, ignore_index=True)
    else:
        scatter_sample_df = pd.DataFrame(columns=existing_scatter_cols)

    scatter_sample_df.to_sql("eda_scatter_sample", deploy_conn, if_exists="replace", index=False)

    full_conn.close()
    deploy_conn.close()

    return {
        "status": "success",
        "total_flows": total_flows,
        "classes_exported": len(unique_labels),
        "scatter_samples": len(scatter_sample_df),
        "corr_features": len(matrix_features)
    }


if __name__ == "__main__":
    base = Path(__file__).resolve().parent.parent
    full_db = base / "data" / "nids_full.db"
    deploy_db = base / "data" / "nids_deploy.db"
    res = export_deploy_db(full_db, deploy_db)
    print("Export completed:", res)
