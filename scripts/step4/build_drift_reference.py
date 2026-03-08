#!/usr/bin/env python3
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

DATASET_PATH = Path("data/processed/ml_rsi_dataset_plus.csv")
OUT_PATH = Path("models/drift_reference.json")

FEATURES = [
    "ret_1h",
    "ret_3h",
    "ret_6h",
    "rsi_14",
    "trend_24h",
    "vol_24h",
    "rsi_slope_6h",
]

def _safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

def main():
    if not DATASET_PATH.exists():
        raise SystemExit(f"Dataset not found: {DATASET_PATH}")

    df = pd.read_csv(DATASET_PATH)

    # open_time range (period)
    if "open_time" in df.columns:
        ot = pd.to_datetime(df["open_time"], utc=True, errors="coerce")
        period_start = ot.min()
        period_end = ot.max()
    else:
        period_start = None
        period_end = None

    # Stats sur features (dropna)
    feat_df = df[FEATURES].apply(pd.to_numeric, errors="coerce").dropna()
    stats = {}
    for col in FEATURES:
        s = feat_df[col]
        stats[col] = {
            "mean": _safe_float(s.mean()),
            "std": _safe_float(s.std(ddof=0)),  # std population (stable)
            "min": _safe_float(s.min()),
            "max": _safe_float(s.max()),
            "n": int(s.shape[0]),
        }

    payload = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "dataset": str(DATASET_PATH),
        "features": FEATURES,
        "period_start": period_start.isoformat() if period_start is not None else None,
        "period_end": period_end.isoformat() if period_end is not None else None,
        "stats": stats,
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"[OK] wrote: {OUT_PATH} | rows_stats={feat_df.shape[0]}")

if __name__ == "__main__":
    main()

    