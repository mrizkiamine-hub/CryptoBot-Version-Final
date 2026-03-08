#!/usr/bin/env python3
"""
02_coingecko_fetch_history.py
Fetch CoinGecko market_chart history (daily) and save snapshots into data/raw/.

Examples:
  python scripts/etl/fetch/02_coingecko_fetch_history.py --symbols BTC ETH SOL --days 365 --vs usd eur
  python scripts/etl/fetch/02_coingecko_fetch_history.py --symbols SOL --days 90 --vs usd --sleep 2
"""

import argparse
import json
import time
from pathlib import Path

import requests


# --------- Config (DST-friendly: simple constants) ----------
BASE_DIR = Path(__file__).resolve().parents[2]
DEFAULT_OUT_DIR = BASE_DIR / "data" / "raw"

BASE_URL = "https://api.coingecko.com/api/v3/coins/{id}/market_chart"

# mapping symbol -> coingecko coin id
COINGECKO_IDS = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "BNB": "binancecoin",
    "SOL": "solana",
    "XRP": "ripple",
    "ADA": "cardano",
    "DOGE": "dogecoin",
    "AVAX": "avalanche-2",
    "DOT": "polkadot",
    "LINK": "chainlink",
    "LTC": "litecoin",
    "MATIC": "polygon",
    "ATOM": "cosmos",
    "TRX": "tron",
    "USDC": "usd-coin",
    "DAI": "dai",
}

DEFAULT_SLEEP_SEC = 3.0
DEFAULT_MAX_RETRIES = 6


def fetch_market_chart(session: requests.Session, coin_id: str, vs_currency: str, days: int,
                       max_retries: int = DEFAULT_MAX_RETRIES) -> dict:
    url = BASE_URL.format(id=coin_id)
    params = {"vs_currency": vs_currency, "days": days, "interval": "daily"}

    wait = 5  # seconds
    for attempt in range(1, max_retries + 1):
        r = session.get(url, params=params, timeout=25)

        # Rate limit: backoff
        if r.status_code == 429:
            print(f"[WARN] 429 rate limit for {coin_id} {vs_currency}. Retry {attempt}/{max_retries} in {wait}s...")
            time.sleep(wait)
            wait = min(wait * 2, 120)
            continue

        # Other errors
        if r.status_code >= 400:
            # show minimal info (no huge dump)
            print(f"[ERROR] HTTP {r.status_code} for {coin_id} {vs_currency}: {r.text[:200]}")
            r.raise_for_status()

        data = r.json()

        # Basic validation (CoinGecko market_chart should have these keys)
        if not isinstance(data, dict) or "prices" not in data or "market_caps" not in data:
            raise ValueError(f"Unexpected response structure for {coin_id} {vs_currency}: keys={list(data)[:10]}")

        return data

    raise RuntimeError(f"Rate limit persists after {max_retries} retries for {coin_id} {vs_currency}")


def save_json(obj: dict, out_dir: Path, filename: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / filename
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    print(f"[OK] saved -> {path}")
    return path


def parse_args():
    parser = argparse.ArgumentParser(description="Fetch CoinGecko market_chart history and save JSON snapshots.")
    parser.add_argument("--symbols", nargs="+", required=True, help="Symbols list (ex: BTC ETH SOL)")
    parser.add_argument("--days", type=int, default=365, help="History depth in days (default: 365)")
    parser.add_argument("--vs", nargs="+", default=["usd", "eur"], help="Currencies (default: usd eur)")
    parser.add_argument("--sleep", type=float, default=DEFAULT_SLEEP_SEC, help="Sleep seconds between calls (default: 3)")
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help="Output directory (default: data/raw)")
    return parser.parse_args()


def main():
    args = parse_args()

    symbols = [s.upper() for s in args.symbols]
    vs_list = [v.lower() for v in args.vs]
    days = args.days
    sleep_sec = args.sleep
    out_dir = Path(args.out_dir).expanduser().resolve()

    # Validate symbols
    unknown = [s for s in symbols if s not in COINGECKO_IDS]
    if unknown:
        raise ValueError(f"Unknown symbols in COINGECKO_IDS: {unknown}. Add them to the mapping if needed.")

    session = requests.Session()
    session.headers.update({
        "Accept": "application/json",
        "User-Agent": "cryptobot-dst/1.0 (+https://github.com/your-repo)"  # keep simple
    })

    for sym in symbols:
        coin_id = COINGECKO_IDS[sym]

        for vs_currency in vs_list:
            data = fetch_market_chart(session, coin_id, vs_currency, days)
            filename = f"coingecko_{sym}_{vs_currency}_{days}d.json"
            save_json(data, out_dir, filename)
            time.sleep(sleep_sec)

    print("[DONE] CoinGecko snapshots fetched.")


if __name__ == "__main__":
    main()
