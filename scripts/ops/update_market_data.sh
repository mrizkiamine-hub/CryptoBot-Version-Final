#!/usr/bin/env bash
set -euo pipefail

cd /home/ubuntu/cryptobot

# ---- local environment ----
if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

: "${POSTGRES_USER:?POSTGRES_USER must be set in .env}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD must be set in .env}"
: "${POSTGRES_DB:?POSTGRES_DB must be set in .env}"

PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-5432}"
PG_URI_HOST="${PG_URI_HOST:-postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${PGHOST}:${PGPORT}/${POSTGRES_DB}}"

RAW_DIR="${RAW_DIR:-data/raw/binance}"
LOG_DIR="${LOG_DIR:-./logs}"
LOG_FILE="${LOG_FILE:-$LOG_DIR/cron_etl.log}"

mkdir -p "$LOG_DIR"

ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

echo "[$(ts)] ETL start" | tee -a "$LOG_FILE"

# 1) Fetch latest Binance chunks
echo "[$(ts)] Fetch Binance klines..." | tee -a "$LOG_FILE"
/home/ubuntu/cryptobot/.venv/bin/python \
  scripts/etl/fetch/01_binance_fetch_klines_chunks.py \
  >> "$LOG_FILE" 2>&1 || true

# 2) Load only new JSON files into RAW
echo "[$(ts)] Load new RAW snapshots..." | tee -a "$LOG_FILE"

mapfile -t files < <(ls -1 "$RAW_DIR"/*.json 2>/dev/null | sort || true)

if [ "${#files[@]}" -eq 0 ]; then
  echo "[$(ts)] No JSON files found in $RAW_DIR" | tee -a "$LOG_FILE"
else
  export PGPASSWORD="$POSTGRES_PASSWORD"
  export PGHOST
  export PGPORT
  export POSTGRES_USER
  export POSTGRES_PASSWORD
  export POSTGRES_DB

  loaded_tmp="$(mktemp)"

  if ! psql \
    "host=$PGHOST port=$PGPORT dbname=$POSTGRES_DB user=$POSTGRES_USER sslmode=disable" \
    -Atc "SELECT source_file
          FROM cryptobot.raw_market_data
          WHERE source_system='BINANCE';" \
    > "$loaded_tmp"
  then
    echo "[$(ts)] ERROR: unable to read loaded BINANCE source files from PostgreSQL" \
      | tee -a "$LOG_FILE"
    rm -f "$loaded_tmp"
    exit 1
  fi

  loaded_count="$(wc -l < "$loaded_tmp")"
  echo "[$(ts)] Already loaded BINANCE source_file count=$loaded_count" \
    | tee -a "$LOG_FILE"

  new_count=0

  for f in "${files[@]}"; do
    base="$(basename "$f")"

    if ! grep -qx "$base" "$loaded_tmp"; then
      echo "[$(ts)] LOADING $base" | tee -a "$LOG_FILE"

      PG_URI_HOST="$PG_URI_HOST" \
        /home/ubuntu/cryptobot/.venv/bin/python \
        scripts/etl/load/01_file_json_to_raw.py \
        --path "$f" \
        >> "$LOG_FILE" 2>&1

      new_count=$((new_count+1))
    fi
  done

  rm -f "$loaded_tmp"

  echo "[$(ts)] Loaded new files: $new_count" | tee -a "$LOG_FILE"
fi

# 3) Refresh STG / CLEAN / FACT
echo "[$(ts)] Refresh STG/CLEAN/FACT..." | tee -a "$LOG_FILE"

docker compose exec -T postgres \
  psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  < sql/03_raw_to_stg.sql >> "$LOG_FILE" 2>&1

docker compose exec -T postgres \
  psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  < sql/04_stg_to_clean.sql >> "$LOG_FILE" 2>&1

docker compose exec -T postgres \
  psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  < sql/06_clean_to_fact_market_price.sql >> "$LOG_FILE" 2>&1

# 4) Summary
max_t="$(
  docker compose exec -T postgres \
    psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc \
    "SELECT MAX(open_time) FROM cryptobot.fact_market_price;"
)"

n_rows="$(
  docker compose exec -T postgres \
    psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc \
    "SELECT COUNT(*) FROM cryptobot.fact_market_price;"
)"

echo "[$(ts)] ETL done | n_rows=$n_rows | max_open_time=$max_t" \
  | tee -a "$LOG_FILE"
