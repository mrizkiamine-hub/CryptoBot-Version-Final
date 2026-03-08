#!/usr/bin/env python3
"""
02_raw_to_fact_macro_price.py
Load CoinGecko macro daily data from RAW (cryptobot.raw_market_data) into FACT (cryptobot.fact_macro_price).

Assumptions / conventions (aligned with 01_file_json_to_raw.py patch):
- RAW rows for CoinGecko use:
    source_system = 'COINGECKO'
    market_symbol = asset symbol (BTC/ETH/SOL/...)
    interval_code = vs_currency ('usd' or 'eur')
    source_file   = 'coingecko_<SYM>_<vs>_<days>d.json'

Upsert strategy:
- Unique key in FACT: (asset_id, date)
- Upsert updates price_usd, price_eur, market_cap
  (market_cap taken from USD payload when available)

Usage:
  python scripts/etl/load/02_raw_to_fact_macro_price.py --symbols BTC ETH SOL --days 365
"""

import argparse
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple, Optional

import psycopg2
from psycopg2.extras import execute_values


PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "dst_db",
    "user": "daniel",
    "password": "datascientest",
}


def ms_to_date(ms: int) -> str:
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt.date().isoformat()


def build_daily_map(data: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
    """
    Build:
      date -> {"price": x, "market_cap": y}
    from CoinGecko market_chart keys.
    """
    out: Dict[str, Dict[str, float]] = {}

    for ts, val in data.get("prices", []):
        if val is None:
            continue
        d = ms_to_date(int(ts))
        out.setdefault(d, {})["price"] = float(val)

    for ts, val in data.get("market_caps", []):
        if val is None:
            continue
        d = ms_to_date(int(ts))
        out.setdefault(d, {})["market_cap"] = float(val)

    return out


def get_asset_id(cur, symbol: str) -> int:
    cur.execute("SELECT id FROM cryptobot.dim_asset WHERE symbol = %s", (symbol,))
    row = cur.fetchone()
    if not row:
        raise ValueError(f"Asset not found in dim_asset: {symbol}")
    return int(row[0])


def fetch_raw_payload(cur, symbol: str, vs_currency: str, days: int) -> Optional[Dict[str, Any]]:
    """
    Fetch payload jsonb from RAW for a given snapshot filename.
    """
    source_file = f"coingecko_{symbol}_{vs_currency}_{days}d.json"
    cur.execute(
        """
        SELECT payload
        FROM cryptobot.raw_market_data
        WHERE source_system = 'COINGECKO'
          AND market_symbol = %s
          AND interval_code = %s
          AND source_file = %s
        ORDER BY ingest_ts DESC
        LIMIT 1
        """,
        (symbol, vs_currency, source_file),
    )
    row = cur.fetchone()
    return row[0] if row else None


def upsert_macro_for_symbol(conn, symbol: str, days: int) -> int:
    """
    Upsert macro rows for one symbol from RAW payloads (usd+eur) into FACT.
    """
    symbol = symbol.upper()

    with conn.cursor() as cur:
        asset_id = get_asset_id(cur, symbol)

        payload_usd = fetch_raw_payload(cur, symbol, "usd", days)
        payload_eur = fetch_raw_payload(cur, symbol, "eur", days)

        if payload_usd is None and payload_eur is None:
            raise FileNotFoundError(
                f"No RAW CoinGecko payload found for {symbol} ({days}d). "
                f"Load RAW snapshots first using 01_file_json_to_raw.py."
            )

        usd_map = build_daily_map(payload_usd) if payload_usd else {}
        eur_map = build_daily_map(payload_eur) if payload_eur else {}

        all_dates = sorted(set(usd_map.keys()) | set(eur_map.keys()))


        if not all_dates:
            raise ValueError(f"Empty series for {symbol} ({days}d) in RAW payloads.")

        rows: List[Tuple] = []
        for d in all_dates:
            price_usd = usd_map.get(d, {}).get("price")
            price_eur = eur_map.get(d, {}).get("price")
            market_cap = usd_map.get(d, {}).get("market_cap")  # market_cap in USD (if USD payload exists)
            rows.append((asset_id, d, price_usd, price_eur, market_cap))

        sql = """
            INSERT INTO cryptobot.fact_macro_price (asset_id, date, price_usd, price_eur, market_cap)
            VALUES %s
            ON CONFLICT (asset_id, date) DO UPDATE
            SET price_usd  = EXCLUDED.price_usd,
                price_eur  = EXCLUDED.price_eur,
                market_cap = EXCLUDED.market_cap
        """
        execute_values(cur, sql, rows, page_size=2000)

    return len(all_dates)


def main():
    ap = argparse.ArgumentParser(description="Load CoinGecko macro from RAW to FACT")
    ap.add_argument("--symbols", nargs="+", required=True, help="e.g. BTC ETH SOL")
    ap.add_argument("--days", type=int, default=365)
    args = ap.parse_args()

    symbols = [s.upper() for s in args.symbols]
    days = int(args.days)

    total = 0
    with psycopg2.connect(**PG_CONFIG) as conn:
        conn.autocommit = False
        for sym in symbols:
            n = upsert_macro_for_symbol(conn, sym, days)
            total += n
            print(f"[OK] {sym}: upserted {n} days")
        conn.commit()

    print(f"\n=== Done: {total} lignes upsertées dans fact_macro_price ===")


if __name__ == "__main__":
    main()
