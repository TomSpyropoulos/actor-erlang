#!/usr/bin/env bash
set -euo pipefail

SCENARIO="${1:?Usage: $0 <scenario-file>}"
[[ -f "$SCENARIO" ]] || { echo "scenario file not found: $SCENARIO" >&2; exit 1; }

REPO="$(cd "$(dirname "$0")" && pwd)"
set -a; source "$SCENARIO"; set +a

PUBLISHER_COUNT="${PUBLISHER_COUNT:-1}"
METRICS_INTERVAL="${METRICS_INTERVAL:-10}"

cleanup() {
    echo ""
    echo "Stopping stack..."
    docker compose -f "$REPO/docker-compose.yaml" down -v > /dev/null 2>&1
    exit 0
}
trap cleanup INT TERM

echo "=== Benchmark: $(basename "$SCENARIO") ==="
echo "  Publishers  : $PUBLISHER_COUNT  (~$((PUBLISHER_COUNT * 1000)) msg/s)"
echo "  Batch       : ${BATCH_ENABLED:-false}  (size=${BATCH_SIZE:-100}, timeout=${BATCH_TIMEOUT_MS:-1000}ms)"
echo "  DB pool     : ${DB_POOL_SIZE:-20} workers"
echo "  Backend     : ${DB_BACKEND:-timescaledb}"
echo ""

echo "Spinning up stack..."
docker compose -f "$REPO/docker-compose.yaml" --env-file "$SCENARIO" up --build -d \
    --scale publisher="$PUBLISHER_COUNT" > /dev/null 2>&1

echo "Waiting for subscriber metrics endpoint..."
until curl -sf http://localhost:8081/metrics > /dev/null 2>&1; do sleep 1; done
echo "Ready. Polling every ${METRICS_INTERVAL}s. Press Ctrl+C to stop."
echo ""
printf "%-10s  %-10s  %-10s  %-10s  %-10s  %-10s\n" \
    "time" "total" "rate/s" "e2e p50" "e2e p99" "db p50"
printf '%s\n' "----------  ----------  ----------  ----------  ----------  ----------"

prev_total=0
while true; do
    sleep "$METRICS_INTERVAL"
    raw=$(curl -s http://localhost:8081/metrics 2>/dev/null) || continue

    # Handle both integer (Erlang) and float (Scala) counter formats
    total=$(printf '%s\n' "$raw" | grep '^subscriber_requests_total' \
            | awk '{printf "%d", $2}')
    e2e_p50=$(printf '%s\n' "$raw" \
            | grep 'e2e_latency_milliseconds{quantile="0.5"' \
            | awk '{printf "%.1f ms", $2}')
    e2e_p99=$(printf '%s\n' "$raw" \
            | grep 'e2e_latency_milliseconds{quantile="0.99"' \
            | awk '{printf "%.1f ms", $2}')
    db_p50=$(printf '%s\n' "$raw" \
            | grep 'db_write_latency_milliseconds{quantile="0.5"' \
            | awk '{printf "%.1f ms", $2}')

    rate=$(( (total - prev_total) / METRICS_INTERVAL ))
    prev_total=$total

    printf "%-10s  %-10s  %-10s  %-10s  %-10s  %-10s\n" \
        "$(date +%H:%M:%S)" "$total" "$rate" \
        "${e2e_p50:-?}" "${e2e_p99:-?}" "${db_p50:-?}"
done
