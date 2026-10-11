"""
Generates a representative sample CICIDS2017 CSV file in data/raw/
for testing and verification of the NIDS ETL and EDA pipeline.
Includes realistic features, whitespace column names, duplicates, and inf values.
"""
import random
from pathlib import Path
import numpy as np
import pandas as pd


def generate_sample_cicids(output_path: Path, n_records: int = 1500) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    random.seed(42)
    np.random.seed(42)

    data = []
    # Mix of classes: 65% BENIGN, 20% DDoS, 10% PortScan, 5% Bot
    classes = ["BENIGN"] * 65 + ["DDoS"] * 20 + ["PortScan"] * 10 + ["Bot"] * 5

    for i in range(n_records):
        lbl = random.choice(classes)
        if lbl == "BENIGN":
            port = random.choice([80, 443, 53, 8080, 22])
            duration = int(np.random.exponential(scale=500000) + 100)
            fwd_pkts = int(np.random.poisson(lam=12) + 1)
            bwd_pkts = int(np.random.poisson(lam=15) + 1)
            pkt_len_mean = round(float(np.random.normal(loc=450, scale=120)), 2)
            pkt_rate = round(float((fwd_pkts + bwd_pkts) / (max(1, duration) / 1e6)), 2)
            byte_rate = round(float(pkt_rate * max(40, pkt_len_mean)), 2)
        elif lbl == "DDoS":
            port = 80
            # High packet rate, short-to-medium duration
            duration = int(np.random.uniform(5000, 80000))
            fwd_pkts = int(np.random.uniform(80, 500))
            bwd_pkts = int(np.random.uniform(0, 10))
            pkt_len_mean = round(float(np.random.normal(loc=120, scale=20)), 2)
            pkt_rate = round(float((fwd_pkts + bwd_pkts) / (duration / 1e6)), 2)
            byte_rate = round(float(pkt_rate * pkt_len_mean), 2)
        elif lbl == "PortScan":
            port = random.randint(1, 1024)
            duration = int(np.random.uniform(10, 5000))
            fwd_pkts = int(np.random.choice([1, 2]))
            bwd_pkts = 0
            pkt_len_mean = 44.0
            pkt_rate = round(float(fwd_pkts / (max(1, duration) / 1e6)), 2)
            byte_rate = round(float(pkt_rate * 44), 2)
        else: # Bot
            port = random.choice([8080, 6667, 4444])
            duration = int(np.random.uniform(200000, 1500000))
            fwd_pkts = int(np.random.poisson(lam=25) + 5)
            bwd_pkts = int(np.random.poisson(lam=20) + 5)
            pkt_len_mean = round(float(np.random.normal(loc=300, scale=80)), 2)
            pkt_rate = round(float((fwd_pkts + bwd_pkts) / (duration / 1e6)), 2)
            byte_rate = round(float(pkt_rate * pkt_len_mean), 2)

        # Inject realistic CICIDS2017 quirks: inf values and NaNs occasionally in rate columns
        if i % 150 == 0:
            byte_rate = np.inf
        elif i % 250 == 0:
            pkt_rate = np.nan

        row = {
            " Destination Port": port,
            " Flow Duration": max(1, duration),
            " Total Fwd Packets": max(1, fwd_pkts),
            " Total Backward Packets": max(0, bwd_pkts),
            "Total Length of Fwd Packets": int(fwd_pkts * pkt_len_mean * 0.8),
            " Fwd Packet Length Max": int(pkt_len_mean * 1.5),
            " Fwd Packet Length Min": int(pkt_len_mean * 0.2),
            " Fwd Packet Length Mean": pkt_len_mean,
            "Flow Bytes/s": byte_rate,
            " Flow Packets/s": pkt_rate,
            " Flow IAT Mean": float(duration / max(1, fwd_pkts + bwd_pkts)),
            " Flow IAT Std": float(duration * 0.1),
            " Fwd IAT Total": float(duration * 0.8),
            " Fwd IAT Mean": float(duration * 0.4),
            " Packet Length Mean": pkt_len_mean,
            " Packet Length Std": round(float(pkt_len_mean * 0.25), 2),
            " Average Packet Size": pkt_len_mean,
            " Label": lbl
        }
        data.append(row)

    # Add duplicate rows deliberately to test deduplication
    data.extend(data[:25])

    df = pd.DataFrame(data)
    df.to_csv(output_path, index=False)
    print(f"Generated sample CICIDS2017 dataset at {output_path} ({len(df)} rows)")
    return output_path


if __name__ == "__main__":
    base = Path(__file__).resolve().parent.parent
    sample_file = base / "data" / "raw" / "sample_cicids2017.csv"
    generate_sample_cicids(sample_file)
