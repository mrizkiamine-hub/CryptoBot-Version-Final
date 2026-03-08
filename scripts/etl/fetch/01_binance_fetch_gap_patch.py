import os
import json
import time
from datetime import datetime, timezone
import requests

BINANCE_URL = "https://api.binance.com/api/v3/klines"
INTERVAL_MS = 60 * 60 * 1000
LIMIT = 1000

def to_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

def ms_to_datestr(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y%m%d")

def fetch_klines(symbol: str, interval: str, start_ms: int, end_ms: int) -> list:
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": LIMIT,
    }
    r = requests.get(BINANCE_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def main():
    symbol = "BTCUSDT"
    interval = "1h"
    out_dir = "data/raw/binance"
    os.makedirs(out_dir, exist_ok=True)

    # fenêtre autour du gap : 2023-03-24 10:00 -> 2023-03-24 17:00 UTC
    start = datetime(2023, 3, 24, 10, 0, 0, tzinfo=timezone.utc)
    end   = datetime(2023, 3, 24, 17, 0, 0, tzinfo=timezone.utc)

    start_ms = to_ms(start)
    end_ms = to_ms(end)

    file_name = f"binance_{symbol}_{interval}_PATCH_GAP_{ms_to_datestr(start_ms)}_{ms_to_datestr(end_ms)}_{start.strftime('%Y%m%dT%H%M')}_{end.strftime('%Y%m%dT%H%M')}.json"
    path = os.path.join(out_dir, file_name)

    if os.path.exists(path):
        print(f"[SKIP] {path} already exists")
        return

    print(f"[FETCH] {symbol} {interval} {start.isoformat()} -> {end.isoformat()}")
    klines = fetch_klines(symbol, interval, start_ms, end_ms)

    payload = {
        "source_system": "binance",
        "market_symbol": symbol,
        "interval_code": interval,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "klines": klines,
        "note": "patch for missing 2023-03-24 13:00 UTC candle",
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)

    print(f"[OK] wrote {len(klines)} klines -> {path}")

if __name__ == "__main__":
    main()