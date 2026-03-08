import os
import json
import time
from datetime import datetime, timezone, timedelta
import requests

BINANCE_URL = "https://api.binance.com/api/v3/klines"
INTERVAL_MS = 60 * 60 * 1000  # 1h
LIMIT = 1000                  # max Binance
CHUNK_HOURS = 1000            # 1000 bougies = ~41.6 jours


def to_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def ms_to_datestr(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y%m%d")


def fetch_klines(symbol: str, interval: str, start_ms: int, end_ms: int) -> list:
    out = []
    cur = start_ms

    while cur < end_ms:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": cur,
            "endTime": end_ms,
            "limit": LIMIT,
        }
        r = requests.get(BINANCE_URL, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        if not data:
            break

        out.extend(data)

        last_open = data[-1][0]
        next_cur = last_open + INTERVAL_MS

        # sécurité : éviter boucle infinie
        if next_cur <= cur:
            break

        cur = next_cur
        time.sleep(0.15)  # soft rate-limit

    return out


def main():
    symbol = "BTCUSDT"
    interval = "1h"
    out_dir = "data/raw/binance"
    os.makedirs(out_dir, exist_ok=True)

    # Période: depuis 2022-01-01 (UTC) jusqu'à la dernière heure clôturée (UTC)
    start = datetime(2022, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    now_utc = datetime.now(timezone.utc)
    end = now_utc.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)

    # garde-fou
    if end <= start:
        raise ValueError(f"End ({end.isoformat()}) must be after start ({start.isoformat()}).")

    start_ms = to_ms(start)
    end_ms = to_ms(end)

    cur = start_ms
    while cur < end_ms:
        chunk_end = min(cur + CHUNK_HOURS * INTERVAL_MS, end_ms)

        file_name = f"binance_{symbol}_{interval}_{ms_to_datestr(cur)}_{ms_to_datestr(chunk_end)}.json"
        path = os.path.join(out_dir, file_name)

        # reproductible + anti-doublon : si le fichier existe, on ne refetch pas
        if os.path.exists(path):
            print(f"[SKIP] {path} already exists")
            cur = chunk_end
            continue

        print(
            f"[FETCH] {symbol} {interval} "
            f"{datetime.fromtimestamp(cur/1000, tz=timezone.utc)} -> {datetime.fromtimestamp(chunk_end/1000, tz=timezone.utc)}"
        )

        klines = fetch_klines(symbol, interval, cur, chunk_end)

        payload = {
            "source_system": "binance",
            "market_symbol": symbol,
            "interval_code": interval,
            "start_ms": cur,
            "end_ms": chunk_end,
            "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
            "klines": klines,
        }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)

        print(f"[OK] wrote {len(klines)} klines -> {path}")
        cur = chunk_end


if __name__ == "__main__":
    main()