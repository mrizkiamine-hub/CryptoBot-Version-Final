#!/usr/bin/env python3
import json
import os
from datetime import datetime
import joblib
import pandas as pd
from sklearn.linear_model import LogisticRegression

DATASET = "data/processed/ml_rsi_dataset_plus.csv"
MODEL_OUT = "models/logreg_rsi_plus.joblib"
META_OUT = "models/metadata.json"

FEATURES = ["ret_1h","ret_3h","ret_6h","rsi_14","trend_24h","vol_24h","rsi_slope_6h"]
THRESHOLD = 0.56

def main():
    os.makedirs("models", exist_ok=True)

    df = pd.read_csv(DATASET)
    df["open_time"] = pd.to_datetime(df["open_time"], utc=True)

    df = df.dropna(subset=FEATURES + ["y_buy"]).sort_values("open_time").reset_index(drop=True)

    X = df[FEATURES].astype(float)
    y = df["y_buy"].astype(int)

    clf = LogisticRegression(max_iter=4000, class_weight="balanced")
    clf.fit(X, y)

    joblib.dump(clf, MODEL_OUT)

    meta = {
        "created_at_utc": datetime.utcnow().isoformat(),
        "dataset": DATASET,
        "rows": int(len(df)),
        "period_start": df["open_time"].min().isoformat(),
        "period_end": df["open_time"].max().isoformat(),
        "model": "LogisticRegression(class_weight=balanced)",
        "features": FEATURES,
        "threshold": THRESHOLD,
    }

    with open(META_OUT, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    print(f"[OK] saved model: {MODEL_OUT}")
    print(f"[OK] saved metadata: {META_OUT}")

if __name__ == "__main__":
    main()