import re
import hashlib
from typing import List, Dict, Any, Tuple
import numpy as np
import pandas as pd


def normalize_column_name(col: str) -> str:
    """
    Normalizes raw column names:
    - Strips leading/trailing whitespace
    - Lowercases
    - Converts spaces, slashes, dashes, and dots to single underscores
    """
    name = col.strip().lower()
    name = re.sub(r"[\s\/\-\.]+", "_", name)
    name = re.sub(r"[^\w]", "", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def clean_dataframe(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms raw flow records DataFrame according to PRD Section 3.3 & 3.5:
    1. Strips whitespace from column names and normalizes to snake_case.
    2. Maps label column to standard 'label' and computes binary 'is_attack'.
    3. Replaces +/- inf with NaN, then imputes/cleans missing values.
    4. Drops exact duplicate rows.
    5. Downcasts numeric types to optimize memory.
    6. Generates a deterministic flow_id for idempotent storage.
    """
    if df_raw.empty:
        return df_raw

    df = df_raw.copy()

    # 1. Normalize column names
    col_mapping = {col: normalize_column_name(col) for col in df.columns}
    df.rename(columns=col_mapping, inplace=True)

    # Standardize label column name if it appears under variant names
    for possible_label in ["label", "class", "attack_cat"]:
        if possible_label in df.columns and possible_label != "label":
            df.rename(columns={possible_label: "label"}, inplace=True)
            break

    if "label" not in df.columns:
        df["label"] = "BENIGN"

    # Clean label text
    df["label"] = df["label"].astype(str).str.strip()

    # 2. Binary target variable: 0 for BENIGN, 1 for ATTACK
    df["is_attack"] = (df["label"].str.upper() != "BENIGN").astype(int)

    # 3. Replace infinite values with NaN
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    # Impute missing values for rate and numeric columns
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    for col in numeric_cols:
        if df[col].isnull().any():
            # Fill missing rate and packet features with 0.0
            df[col] = df[col].fillna(0.0)

    # Also handle non-numeric columns that should be numeric
    for col in ["flow_bytes_s", "flow_packets_s", "packet_length_mean"]:
        if col in df.columns and not pd.api.types.is_numeric_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    for col in ["destination_port", "flow_duration", "total_fwd_packets", "total_backward_packets"]:
        if col in df.columns and not pd.api.types.is_numeric_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # 4. Drop exact duplicates across feature columns
    feature_cols = [c for c in df.columns if c not in ["flow_id"]]
    df.drop_duplicates(subset=feature_cols, inplace=True)

    # 5. Downcast numeric dtypes
    for col in df.select_dtypes(include=["float64"]).columns:
        df[col] = pd.to_numeric(df[col], downcast="float")

    for col in df.select_dtypes(include=["int64"]).columns:
        if col != "is_attack":
            df[col] = pd.to_numeric(df[col], downcast="integer")

    # 6. Generate deterministic flow_id for idempotency
    def generate_flow_hash(row: pd.Series) -> str:
        # Construct fingerprint from port, duration, packets, label, and key features
        key_fields = [
            str(row.get("destination_port", "")),
            str(row.get("flow_duration", "")),
            str(row.get("total_fwd_packets", "")),
            str(row.get("total_backward_packets", "")),
            str(row.get("flow_bytes_s", "")),
            str(row.get("flow_packets_s", "")),
            str(row.get("label", ""))
        ]
        raw_str = "|".join(key_fields)
        return hashlib.sha256(raw_str.encode("utf-8")).hexdigest()[:16]

    if "flow_id" not in df.columns:
        # Create unique IDs combining hash and index to guarantee uniqueness even for edge collisions
        hashes = [generate_flow_hash(row) for _, row in df.iterrows()]
        df["flow_id"] = hashes

        # If any duplicate flow_id within batch, disambiguate with counter
        if df["flow_id"].duplicated().any():
            counts = {}
            new_ids = []
            for fid in df["flow_id"]:
                counts[fid] = counts.get(fid, 0) + 1
                if counts[fid] > 1:
                    new_ids.append(f"{fid}_{counts[fid]}")
                else:
                    new_ids.append(fid)
            df["flow_id"] = new_ids

    return df
