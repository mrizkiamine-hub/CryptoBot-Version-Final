import numpy as np
import pandas as pd

def _wilder_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)

    avg_gain = gain.rolling(period, min_periods=period).mean()
    avg_loss = loss.rolling(period, min_periods=period).mean()

    rsi = pd.Series(index=close.index, dtype=float)

    # init
    for i in range(len(close)):
        if i == period:
            ag = avg_gain.iloc[i]
            al = avg_loss.iloc[i]
            rs = np.inf if al == 0 else ag / al
            rsi.iloc[i] = 100 - (100 / (1 + rs))
            break

    # smoothing
    for i in range(period + 1, len(close)):
        ag_prev = avg_gain.iloc[i - 1] if not np.isnan(avg_gain.iloc[i - 1]) else 0.0
        al_prev = avg_loss.iloc[i - 1] if not np.isnan(avg_loss.iloc[i - 1]) else 0.0
        ag = (ag_prev * (period - 1) + gain.iloc[i]) / period
        al = (al_prev * (period - 1) + loss.iloc[i]) / period
        avg_gain.iloc[i] = ag
        avg_loss.iloc[i] = al
        rs = np.inf if al == 0 else ag / al
        rsi.iloc[i] = 100 - (100 / (1 + rs))

    return rsi

def compute_features_rsi_plus(df: pd.DataFrame) -> dict:
    # df must have open_time, close, sorted ascending
    df = df.copy()
    close = df["close"].astype(float)

    df["ret_1h"] = np.log(close / close.shift(1))
    df["ret_3h"] = np.log(close / close.shift(3))
    df["ret_6h"] = np.log(close / close.shift(6))

    df["rsi_14"] = _wilder_rsi(close, period=14)

    df["trend_24h"] = np.log(close / close.shift(24))
    df["vol_24h"] = df["ret_1h"].rolling(24, min_periods=24).std()

    df["rsi_slope_6h"] = df["rsi_14"] - df["rsi_14"].shift(6)

    last = df.iloc[-1]

    feat = {
        "ret_1h": float(last["ret_1h"]),
        "ret_3h": float(last["ret_3h"]),
        "ret_6h": float(last["ret_6h"]),
        "rsi_14": float(last["rsi_14"]),
        "trend_24h": float(last["trend_24h"]),
        "vol_24h": float(last["vol_24h"]),
        "rsi_slope_6h": float(last["rsi_slope_6h"]),
    }

    if any(np.isnan(v) for v in feat.values()):
        raise ValueError("Not enough history to compute features (NaN). Increase LOOKBACK_ROWS.")

    return feat
