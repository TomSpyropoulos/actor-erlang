#!/usr/bin/env bash
set -euo pipefail

REPO="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="$REPO/benchmarking"
VENV_DIR="$BENCH_DIR/.venv"
COMPOSE=(docker compose -f "$REPO/docker-compose.yaml")

MODE="${1:?Usage: $0 <scenario-file>|all}"

# Create the monitor's venv and install its dependencies on first run only.
ensure_venv() {
    if [[ ! -x "$VENV_DIR/bin/python" ]]; then
        echo "Setting up Python environment..."
        python3 -m venv "$VENV_DIR"
        "$VENV_DIR/bin/pip" install --quiet -r "$BENCH_DIR/requirements.txt"
    fi
}

# Tear down the docker stack (containers + volumes) so the next run starts cold.
# Also wired to the EXIT trap so it runs on every exit path (success, error, Ctrl+C).
teardown() {
    echo ""
    echo "Stopping stack..."
    "${COMPOSE[@]}" down -v > /dev/null 2>&1 || true
}
trap teardown EXIT

# Run a single scenario end-to-end: bring the stack up, wait for the subscriber's
# metrics endpoint, then hand off to monitor.py. Runs in a subshell so the scenario's
# sourced variables don't leak into the next iteration (which would let a stale value
# like PAYLOAD_PADDING_BYTES override the next scenario's compose interpolation).
# Args: <scenario-file> <build|nobuild>
run_one() (
    local scenario="$1" build="$2"
    set -a; source "$scenario"; set +a

    local pub="${PUBLISHER_COUNT:-1}"
    # Fixed-duration runs sample faster (5s) than open-ended interactive runs (10s).
    local default_interval; default_interval=$([[ -n "${RUN_DURATION:-}" ]] && echo 5 || echo 10)
    local interval="${METRICS_INTERVAL:-$default_interval}"

    # Only pass --duration when RUN_DURATION is set; without it monitor.py stays
    # interactive and waits for Ctrl+C (the original single-run behaviour).
    local duration_arg=()
    [[ -n "${RUN_DURATION:-}" ]] && duration_arg=(--duration "$RUN_DURATION")

    echo "=== Benchmark: $(basename "$scenario") ==="
    echo "  Publishers  : $pub  (~$((pub * 1000)) msg/s)"
    echo "  Batch       : ${BATCH_ENABLED:-false}  (size=${BATCH_SIZE:-100}, timeout=${BATCH_TIMEOUT_MS:-1000}ms)"
    echo "  DB pool     : ${DB_POOL_SIZE:-20} workers"
    echo "  Backend     : ${DB_BACKEND:-timescaledb}"
    [[ -n "${RUN_DURATION:-}" ]] && echo "  Duration    : ${RUN_DURATION}s (interval ${interval}s)"
    echo ""

    echo "Spinning up stack..."
    local up_args=(--env-file "$scenario" up -d --scale publisher="$pub")
    [[ "$build" == build ]] && up_args+=(--build)
    "${COMPOSE[@]}" "${up_args[@]}" > /dev/null 2>&1

    echo "Waiting for subscriber metrics endpoint..."
    until curl -sf http://localhost:8081/metrics > /dev/null 2>&1; do sleep 1; done

    ensure_venv

    echo "Ready. Launching metrics view..."
    echo ""
    # Foreground so Ctrl+C reaches monitor.py directly in interactive mode; `|| true`
    # keeps `set -e` from short-circuiting before teardown runs when it returns.
    "$VENV_DIR/bin/python" "$BENCH_DIR/monitor.py" \
        --prometheus-url "http://localhost:9090" \
        --interval "$interval" \
        --scenario-name "$(basename "$scenario")" \
        --output-dir "$BENCH_DIR/output" \
        "${duration_arg[@]}" || true
)

if [[ "$MODE" == all ]]; then
    # Batch mode: fixed-duration run of every scenario, unattended. Build the images
    # once up front so the per-scenario `up` never pays a rebuild across the sweep.
    : "${RUN_DURATION:=60}"; export RUN_DURATION
    shopt -s nullglob
    scenarios=("$BENCH_DIR"/scenarios/*.env)
    shopt -u nullglob
    [[ ${#scenarios[@]} -gt 0 ]] || { echo "no scenario files in $BENCH_DIR/scenarios" >&2; exit 1; }

    echo "Building images once before the sweep..."
    "${COMPOSE[@]}" build > /dev/null 2>&1

    i=0
    for scenario in "${scenarios[@]}"; do
        i=$((i + 1))
        echo ""
        echo "########## [$i/${#scenarios[@]}] $(basename "$scenario") ##########"
        run_one "$scenario" nobuild
        teardown
    done
    echo ""
    echo "Sweep complete: ${#scenarios[@]} scenarios. Results in $BENCH_DIR/output/"
else
    # Single-scenario mode. Interactive (Ctrl+C) unless RUN_DURATION is set, in which
    # case it runs unattended for that many seconds. Rebuilds to pick up code changes.
    [[ -f "$MODE" ]] || { echo "scenario file not found: $MODE" >&2; exit 1; }
    run_one "$MODE" build
fi
