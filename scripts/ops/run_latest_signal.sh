#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://127.0.0.1:8000}"
TS="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

OUT_DIR="${OUT_DIR:-./logs}"
OUT_FILE="${OUT_FILE:-$OUT_DIR/signal_latest.jsonl}"
mkdir -p "$OUT_DIR"


# --- log rotation (5MB) ---
MAX_BYTES=$((5*1024*1024))
if [ -f "$OUT_FILE" ]; then
  size=$(wc -c < "$OUT_FILE" || echo 0)
  if [ "$size" -ge "$MAX_BYTES" ]; then
    mv "$OUT_FILE" "${OUT_FILE%.jsonl}_$(date -u +%Y%m%dT%H%M%SZ).jsonl"
  fi
fi



tmp_body="$(mktemp)"
http_code="000"
status="ok"
err_msg=""

# Call API: capture body + http status
if ! http_code="$(curl -sS -o "$tmp_body" -w "%{http_code}" -X POST "$API_URL/signal/latest" -H "Content-Type: application/json")"; then
  status="error"
  err_msg="curl_failed"
fi

body="$(cat "$tmp_body" 2>/dev/null || echo "")"
rm -f "$tmp_body"

# Any non-200 is considered error
if [[ "$http_code" != "200" ]]; then
  status="error"
  err_msg="${err_msg:-http_${http_code}}"
fi

# Write JSONL
if [[ "$status" == "ok" ]]; then
  echo "{\"run_ts_utc\":\"$TS\",\"api_url\":\"$API_URL\",\"status\":\"ok\",\"http_code\":$http_code,\"response\":$body}" >> "$OUT_FILE"
else
  safe_body="$(echo "$body" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')"
  echo "{\"run_ts_utc\":\"$TS\",\"api_url\":\"$API_URL\",\"status\":\"error\",\"http_code\":$http_code,\"error\":\"$err_msg\",\"raw_body\":$safe_body}" >> "$OUT_FILE"
fi

echo "OK $TS status=$status http=$http_code -> appended to $OUT_FILE"
