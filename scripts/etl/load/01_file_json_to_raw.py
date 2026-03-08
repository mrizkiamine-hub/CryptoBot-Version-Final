#!/usr/bin/env python3
"""
01_file_json_to_raw.py
Load a JSON snapshot file (Binance klines) into PostgreSQL RAW table.

Anti-duplicate strategy (Option B):
- Use (source_system, market_symbol, interval_code, source_file) as a unique key
- If the same file is loaded again, it is ignored (ON CONFLICT DO NOTHING)

Usage:
  python scripts/etl/load/01_file_json_to_raw.py --path data/raw/binance_BTCUSDT_1h_2025-09-01_to_2025-12-01.json
"""

import argparse
import json
import os
from pathlib import Path

import psycopg2


# DST-friendly config (simple constants)
PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "dst_db",
    "user": "daniel",
    "password": "datascientest",
}


def parse_meta_from_filename(file_path: Path):
    """
    Supported filename formats:

    Binance (OHLCV klines):
      binance_BTCUSDT_1h_2025-09-01_to_2025-12-01.json
      binance_BTCUSDT_1h_5.json

    CoinGecko (macro market_chart):
      coingecko_BTC_usd_365d.json
      coingecko_ETH_eur_90d.json

    Returns: (source_system, market_symbol, interval_code)

    Convention (to reuse RAW schema):
    - For CoinGecko:
        market_symbol = asset symbol (BTC/ETH/SOL/...)
        interval_code = vs_currency (usd/eur/...)
    """
    name = file_path.name
    if not name.endswith(".json"):
        raise ValueError(f"Expected .json file: {name}")

    parts = name.replace(".json", "").split("_")
    if len(parts) < 3:
        raise ValueError(f"Unexpected filename format: {name}")

    src = parts[0].lower()

    # CoinGecko snapshots
    if src == "coingecko":
        # coingecko_BTC_usd_365d.json -> ["coingecko","BTC","usd","365d"]
        if len(parts) < 4:
            raise ValueError(f"Unexpected CoinGecko filename format: {name}")

        source_system = "COINGECKO"
        market_symbol = parts[1].upper()   # BTC
        interval_code = parts[2].lower()   # usd / eur (vs_currency)
        return source_system, market_symbol, interval_code

    # Default: Binance (or other exchange snapshots)
    source_system = parts[0].upper()  # BINANCE
    market_symbol = parts[1]          # BTCUSDT
    interval_code = parts[2]          # 1h / 4h / 1d
    return source_system, market_symbol, interval_code


def load_json(json_path: Path):
    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)

def insert_raw(conn, source_system: str, market_symbol: str, interval_code: str, source_file: str, payload):
    """
    Insert one RAW snapshot row.
    If same (source_system, market_symbol, interval_code, source_file) already exists => ignore.
    """
    sql = """
        INSERT INTO cryptobot.raw_market_data
            (source_system, market_symbol, interval_code, source_file, payload)
        VALUES
            (%s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (source_system, market_symbol, interval_code, source_file)
        DO NOTHING;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (source_system, market_symbol, interval_code, source_file, json.dumps(payload)))
        # rowcount = 1 if inserted, 0 if ignored
        return cur.rowcount


def main():
    parser = argparse.ArgumentParser(description="Load JSON file into RAW table (cryptobot.raw_market_data)")
    parser.add_argument("--path", required=True, help="Path to JSON file (data/raw/...)")
    args = parser.parse_args()

    json_path = Path(args.path).expanduser().resolve()
    if not json_path.exists():
        raise FileNotFoundError(f"File not found: {json_path}")

    source_file = json_path.name
    source_system, market_symbol, interval_code = parse_meta_from_filename(json_path)
    payload = load_json(json_path)

    payload_type = type(payload).__name__
    payload_len = len(payload) if hasattr(payload, "__len__") else None

    with psycopg2.connect(**PG_CONFIG) as conn:
        conn.autocommit = True  # simple & DST-friendly
        inserted = insert_raw(conn, source_system, market_symbol, interval_code, source_file, payload)

    if inserted == 1:
        print("OK - inserted RAW snapshot")
    else:
        print("OK - RAW snapshot already exists (ignored)")

    print(f"  file          : {source_file}")
    print(f"  source_system : {source_system}")
    print(f"  market_symbol : {market_symbol}")
    print(f"  interval_code : {interval_code}")
    print(f"  payload_type  : {payload_type}")
    print(f"  payload_len   : {payload_len}")


if __name__ == "__main__":
    main()
