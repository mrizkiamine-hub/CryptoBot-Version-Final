#!/usr/bin/env bash
set -euo pipefail

# ---- config ----
PG_URI="${PG_URI:-postgresql+psycopg2://daniel:datascientest@127.0.0.1:5432/dst_db}"
RAW_DIR="${RAW_DIR:-data/raw/binance}"

LOG_DIR="${LOG_DIR:-./logs}"
LOG_FILE="${LOG_FILE:-$LOG_DIR/cron_etl.log}"

mkdir -p "$LOG_DIR"

ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

echo "[$(ts)] ETL start" | tee -a "$LOG_FILE"

# 1) Fetch latest Binance chunks (script is idempotent: SKIP existing files)
echo "[$(ts)] Fetch Binance klines..." | tee -a "$LOG_FILE"
python scripts/etl/fetch/01_binance_fetch_klines_chunks.py >> "$LOG_FILE" 2>&1 || true

# 2) Load only new JSON files into RAW (skip files already in raw_market_data.source_file)
echo "[$(ts)] Load new RAW snapshots..." | tee -a "$LOG_FILE"

# list candidate json files (excluding PATCH_GAP if you want; keep it anyway)
mapfile -t files < <(ls -1 "$RAW_DIR"/*.json 2>/dev/null | sort || true)

if [ "${#files[@]}" -eq 0 ]; then
  echo "[$(ts)] No JSON files found in $RAW_DIR" | tee -a "$LOG_FILE"
else
  # get already-loaded source_file names from DB
  export PGPASSWORD="${PGPASSWORD:-datascientest}"
  loaded_tmp="$(mktemp)"
  psql "host=127.0.0.1 port=5432 dbname=dst_db user=daniel sslmode=disable" -Atc \
    "SELECT source_file FROM cryptobot.raw_market_data WHERE source_system='BINANCE';" \
    > "$loaded_tmp" || true

  # convert to grep set
  loaded_count="$(wc -l < "$loaded_tmp" || echo 0)"
  echo "[$(ts)] Already loaded BINANCE source_file count=$loaded_count" | tee -a "$LOG_FILE"

  new_count=0
  for f in "${files[@]}"; do
    base="$(basename "$f")"
    # load only if not already present
    if ! grep -qx "$base" "$loaded_tmp"; then
      echo "[$(ts)] LOADING $base" | tee -a "$LOG_FILE"
      PG_URI="$PG_URI" python scripts/etl/load/01_file_json_to_raw.py --path "$f" >> "$LOG_FILE" 2>&1
      new_count=$((new_count+1))
    fi
  done
  rm -f "$loaded_tmp"

  echo "[$(ts)] Loaded new files: $new_count" | tee -a "$LOG_FILE"
fi

# 3) Refresh STG/CLEAN/FACT
echo "[$(ts)] Refresh STG/CLEAN/FACT..." | tee -a "$LOG_FILE"
docker compose exec -T postgres psql -U daniel -d dst_db < sql/03_raw_to_stg.sql >> "$LOG_FILE" 2>&1
docker compose exec -T postgres psql -U daniel -d dst_db < sql/04_stg_to_clean.sql >> "$LOG_FILE" 2>&1
docker compose exec -T postgres psql -U daniel -d dst_db < sql/06_clean_to_fact_market_price.sql >> "$LOG_FILE" 2>&1

# 4) Summary
max_t="$(docker compose exec -T postgres psql -U daniel -d dst_db -Atc "SELECT MAX(open_time) FROM cryptobot.fact_market_price;")"
n_rows="$(docker compose exec -T postgres psql -U daniel -d dst_db -Atc "SELECT COUNT(*) FROM cryptobot.fact_market_price;")"

echo "[$(ts)] ETL done | n_rows=$n_rows | max_open_time=$max_t" | tee -a "$LOG_FILE"
