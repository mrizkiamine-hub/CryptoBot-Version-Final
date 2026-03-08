#!/usr/bin/env python3
import pandas as pd
import numpy as np

IN_PATH = "data/processed/ml_rsi_dataset.csv"
OUT_PATH = "data/processed/ml_rsi_dataset_plus.csv"

def main():
    df = pd.read_csv(IN_PATH)
    df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
    df = df.sort_values("open_time").reset_index(drop=True)

    # SMA 24h on close
    sma24 = df["close"].rolling(24, min_periods=24).mean()
    df["trend_24h"] = np.log(df["close"] / sma24)

    # vol 24h (std of ret_1h)
    df["vol_24h"] = df["ret_1h"].rolling(24, min_periods=24).std()

    # RSI slope 6h
    df["rsi_slope_6h"] = df["rsi_14"] - df["rsi_14"].shift(6)

    # drop rows with NaNs introduced by rolling/shift
    df2 = df.dropna(subset=["trend_24h", "vol_24h", "rsi_slope_6h"]).copy()

    df2.to_csv(OUT_PATH, index=False)
    print(f"[OK] saved: {OUT_PATH} | rows: {len(df2)} (from {len(df)})")
    print("Columns:", list(df2.columns))

if __name__ == "__main__":
    main()