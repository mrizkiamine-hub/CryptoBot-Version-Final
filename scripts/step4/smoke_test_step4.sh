#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"

echo "== Smoke test Step4 =="

wait_http() {
  local url="$1"
  local max_sec="${2:-60}"
  local sleep_sec="${3:-2}"

  echo "Waiting for $url (timeout ${max_sec}s)..."
  local start
  start="$(date +%s)"

  while true; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      echo "OK: $url"
      return 0
    fi

    local now
    now="$(date +%s)"

    if (( now - start >= max_sec )); then
      echo "FAILED: timeout waiting for $url"
      return 1
    fi

    sleep "$sleep_sec"
  done
}

wait_postgres() {
  local max_sec="${1:-90}"
  local sleep_sec="${2:-2}"

  echo "Waiting for postgres to be ready (timeout ${max_sec}s)..."
  local start
  start="$(date +%s)"

  while true; do
    if docker compose exec -T postgres \
      pg_isready -U daniel -d dst_db >/dev/null 2>&1; then
      echo "OK: postgres ready"
      return 0
    fi

    local now
    now="$(date +%s)"

    if (( now - start >= max_sec )); then
      echo "FAILED: timeout waiting for postgres"
      return 1
    fi

    sleep "$sleep_sec"
  done
}

# --- Attente des services ---
wait_http "${BASE_URL}/health" 60 2
wait_postgres 90 2

echo "[1] health"
curl -fsS "${BASE_URL}/health"
echo

echo "[2] model info (short)"
curl -fsS "${BASE_URL}/model/info" | head -c 300
echo -e "\n"

echo "[3] predict"
curl -fsS -X POST "${BASE_URL}/predict" \
  -H "Content-Type: application/json" \
  -d '{"ret_1h":0.001,"ret_3h":0.002,"ret_6h":0.003,"rsi_14":55,"trend_24h":0.01,"vol_24h":0.005,"rsi_slope_6h":1.2}'
echo

echo "[4] signal/latest (DB up)"
curl -fsS -X POST "${BASE_URL}/signal/latest" | head -c 500
echo -e "\n"

echo "[5] DB down => expect 503 on signal/latest"
docker compose stop postgres >/dev/null

code=$(curl -s -o /dev/null -w "%{http_code}" \
  -X POST "${BASE_URL}/signal/latest" || true)

echo "signal/latest http_code=$code (expected 503)"

if [ "$code" != "503" ]; then
  echo "FAILED: expected 503 when postgres is stopped"
  docker compose start postgres >/dev/null
  exit 1
fi

docker compose start postgres >/dev/null
wait_postgres 90 2

echo "✅ Smoke test PASSED"
