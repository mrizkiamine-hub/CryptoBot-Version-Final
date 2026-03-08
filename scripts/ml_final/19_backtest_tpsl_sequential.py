#!/usr/bin/env python3
import argparse
import pandas as pd
from sqlalchemy import create_engine
from sklearn.linear_model import LogisticRegression

PG_URI = "postgresql+psycopg2://daniel:datascientest@localhost:5432/dst_db"

FEATURES = ["ret_1h", "ret_3h", "ret_6h", "rsi_14", "trend_24h", "vol_24h", "rsi_slope_6h"]

SQL_OHLCV = """
SELECT f.open_time, f.open, f.high, f.low, f.close
FROM cryptobot.fact_market_price f
JOIN cryptobot.dim_market m ON m.id=f.market_id
WHERE m.symbol='BTCUSDT' AND f.interval_code='1h'
ORDER BY f.open_time;
"""

def month_floor(ts: pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(year=ts.year, month=ts.month, day=1, tz="UTC")

def add_months(ts: pd.Timestamp, n: int) -> pd.Timestamp:
    return (ts + pd.DateOffset(months=n)).tz_convert("UTC")

def apply_cooldown(signals: pd.DataFrame, cooldown_hours: int):
    if signals.empty:
        return signals
    signals = signals.sort_values("open_time").copy()
    keep = []
    next_allowed = signals["open_time"].iloc[0] - pd.Timedelta(days=9999)
    cd = pd.Timedelta(hours=cooldown_hours)
    for _, row in signals.iterrows():
        t = row["open_time"]
        if t >= next_allowed:
            keep.append(True)
            next_allowed = t + cd
        else:
            keep.append(False)
    return signals.loc[keep].copy()

def simulate_trade_seq(entry_close: float, future_rows: pd.DataFrame, tp: float, sl: float, fees: float, conservative_same_candle=True):
    """future_rows must be ordered t+1..t+6 with columns high, low, close."""
    tp_px = entry_close * (1.0 + tp)
    sl_px = entry_close * (1.0 - sl)

    for _, r in future_rows.iterrows():
        hit_tp = r["high"] >= tp_px
        hit_sl = r["low"] <= sl_px

        if hit_tp and hit_sl:
            # intrabar order unknown
            gross = -sl if conservative_same_candle else tp
            return gross - fees, "both"
        elif hit_sl:
            return (-sl) - fees, "sl"
        elif hit_tp:
            return (tp) - fees, "tp"

    # none hit => exit at horizon close (t+6 close)
    exit_close = future_rows.iloc[-1]["close"]
    gross = (exit_close / entry_close) - 1.0
    return gross - fees, "none"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", type=str, default="data/processed/ml_rsi_dataset_plus.csv")
    ap.add_argument("--thr", type=float, default=0.54)
    ap.add_argument("--fees", type=float, default=0.002)
    ap.add_argument("--tp", type=float, default=0.006)
    ap.add_argument("--sl", type=float, default=0.004)
    ap.add_argument("--train_months", type=int, default=12)
    ap.add_argument("--cooldown_hours", type=int, default=6)
    ap.add_argument("--conservative", action="store_true", help="If set: both-hit candle counts as SL (worst-case)")
    args = ap.parse_args()

    df = pd.read_csv(args.csv)
    df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
    df = df.sort_values("open_time").reset_index(drop=True)

    engine = create_engine(PG_URI)
    ohlcv = pd.read_sql(SQL_OHLCV, engine)
    ohlcv["open_time"] = pd.to_datetime(ohlcv["open_time"], utc=True)
    ohlcv = ohlcv.sort_values("open_time").reset_index(drop=True)

    # map open_time -> index for fast slicing
    idx_map = {t: i for i, t in enumerate(ohlcv["open_time"])}

    start = month_floor(df["open_time"].min())
    end = month_floor(df["open_time"].max())
    cur_test_start = add_months(start, args.train_months)

    results = []
    outcome_counts = {"tp":0, "sl":0, "both":0, "none":0}

    while cur_test_start <= end:
        test_start = cur_test_start
        test_end = add_months(test_start, 1)
        train_start = add_months(test_start, -args.train_months)
        train_end = test_start - pd.Timedelta(hours=1)

        df_train = df[(df["open_time"] >= train_start) & (df["open_time"] <= train_end)].copy()
        df_test = df[(df["open_time"] >= test_start) & (df["open_time"] < test_end)].copy()

        if len(df_train) < 1000 or len(df_test) < 100:
            cur_test_start = add_months(cur_test_start, 1)
            continue

        clf = LogisticRegression(max_iter=4000, class_weight="balanced")
        clf.fit(df_train[FEATURES], df_train["y_buy"].astype(int))

        df_test["proba_buy"] = clf.predict_proba(df_test[FEATURES])[:, 1]
        df_test["signal_buy"] = (df_test["proba_buy"] >= args.thr).astype(int)

        sig = df_test[df_test["signal_buy"] == 1].copy()
        sig_cd = apply_cooldown(sig, cooldown_hours=args.cooldown_hours)

        pnls = []
        for t in sig_cd["open_time"]:
            j = idx_map.get(t)
            if j is None or j + 6 >= len(ohlcv):
                continue
            entry_close = float(ohlcv.loc[j, "close"])
            future_rows = ohlcv.loc[j+1:j+6, ["high","low","close"]]
            pnl, outcome = simulate_trade_seq(
                entry_close, future_rows, args.tp, args.sl, args.fees,
                conservative_same_candle=args.conservative
            )
            pnls.append(pnl)
            outcome_counts[outcome] += 1

        nb = len(pnls)
        avg_net = float(pd.Series(pnls).mean()) if nb else 0.0
        cum_sum = float(pd.Series(pnls).sum()) if nb else 0.0
        cum_prod = float((1.0 + pd.Series(pnls)).prod() - 1.0) if nb else 0.0

        results.append({
            "test_month": test_start.strftime("%Y-%m"),
            "trades_cd": nb,
            "avg_net": avg_net,
            "cum_sum": cum_sum,
            "cum_prod": cum_prod
        })

        cur_test_start = add_months(cur_test_start, 1)

    res = pd.DataFrame(results)
    print(res.to_string(index=False))

    print("\n=== Summary (Sequential TP/SL) ===")
    print(f"thr={args.thr} fees={args.fees} tp={args.tp} sl={args.sl} train_months={args.train_months} cd_h={args.cooldown_hours} conservative={args.conservative}")
    print(f"Total trades: {res['trades_cd'].sum()} | Sum cum_sum: {res['cum_sum'].sum():.4f}")
    print("Outcomes:", outcome_counts)

if __name__ == "__main__":
    main()