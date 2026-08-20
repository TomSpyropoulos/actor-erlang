#!/usr/bin/env bash
set -euo pipefail

REPO="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="$REPO/benchmarking"
VENV_DIR="$BENCH_DIR/.venv"
COMPOSE=(docker compose -f "$REPO/docker-compose.yaml")

# How long a rep may take to become measurable before it is abandoned. Startup covers image start
# plus the database's initdb; ingestion covers broker connect and the first messages arriving.
STARTUP_TIMEOUT="${STARTUP_TIMEOUT:-180}"
INGEST_TIMEOUT="${INGEST_TIMEOUT:-60}"
SKIPPED_REPS=0

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

# Current value of the subscriber's ingest counter, or 0 before the metric exists. Erlang's
# prometheus.erl prints counters as plain integers and Scala's simpleclient prints them as floats,
# so callers must compare these with awk rather than bash's integer test.
subscriber_requests() {
    curl -s http://localhost:8081/metrics 2>/dev/null \
        | awk '/^subscriber_requests_total /{v=$2; found=1} END{print (found ? v : 0)}'
}

# Run a single scenario end-to-end: bring the stack up, wait until it is actually ingesting,
# then hand off to monitor.py. Returns non-zero if the stack never became measurable. Runs in a subshell so the scenario's
# sourced variables don't leak into the next iteration (which would let a stale value
# like PAYLOAD_PADDING_BYTES override the next scenario's compose interpolation).
# Args: <scenario-file> <build|nobuild> <rep>
run_one() (
    local scenario="$1" build="$2" rep="$3"
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
    local waited=0
    until curl -sf http://localhost:8081/metrics > /dev/null 2>&1; do
        sleep 1; waited=$((waited + 1))
        if [[ $waited -ge $STARTUP_TIMEOUT ]]; then
            echo "ERROR: metrics endpoint did not come up within ${STARTUP_TIMEOUT}s" >&2
            "${COMPOSE[@]}" logs --tail 40 subscriber >&2 || true
            return 1
        fi
    done

    # The endpoint binds before the subscriber has necessarily reached the broker or the database,
    # so a subscriber that died during startup still serves a scrapeable /metrics with every counter
    # sitting at zero. Require the ingest counter to actually advance: without this gate a dead
    # stack is measured for the full RUN_DURATION and its zeros are averaged into the report.
    echo "Waiting for ingestion to start..."
    local before after ingest_waited=0
    before=$(subscriber_requests)
    until after=$(subscriber_requests); awk -v a="$before" -v b="$after" 'BEGIN{exit !(b > a)}'; do
        sleep 1; ingest_waited=$((ingest_waited + 1))
        if [[ $ingest_waited -ge $INGEST_TIMEOUT ]]; then
            echo "ERROR: subscriber is up but ingested nothing in ${INGEST_TIMEOUT}s (counter stuck at ${after})" >&2
            "${COMPOSE[@]}" logs --tail 40 subscriber >&2 || true
            return 1
        fi
    done

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
        --rep "$rep" \
        "${duration_arg[@]}" || true
)

# Run one scenario REPS times, tearing the stack down between reps so each starts cold and
# its run-to-run spread is real (not warm-cache carryover). The first rep honours the caller's
# build flag; later reps reuse those images (nobuild) so a multi-rep run rebuilds only once.
run_reps() {
    local scenario="$1" build="$2"
    local reps="${REPS:-1}"
    for rep in $(seq 1 "$reps"); do
        [[ "$reps" -gt 1 ]] && echo "----- rep $rep/$reps -----"
        # A rep that never becomes measurable is skipped rather than fatal: it writes no JSON, so
        # report.py cannot average a dead run into a cell, and the rest of the sweep still runs.
        if ! run_one "$scenario" "$build" "$rep"; then
            SKIPPED_REPS=$((SKIPPED_REPS + 1))
            echo "!!! SKIPPED $(basename "$scenario") rep $rep - no data recorded" >&2
        fi
        teardown
        build=nobuild
    done
}

if [[ "$MODE" == all ]]; then
    # Batch mode: fixed-duration run of every scenario, unattended, each repeated REPS times.
    # RUN_DURATION and REPS both default here since they only make sense for an unattended sweep
    # (K=3 reps let run-to-run noise be told apart from a real runtime difference). Build the
    # images once up front so no per-scenario/per-rep `up` ever pays a rebuild across the sweep.
    : "${RUN_DURATION:=60}"; export RUN_DURATION
    : "${REPS:=3}"; export REPS
    shopt -s nullglob
    scenarios=("$BENCH_DIR"/scenarios/*.env)
    shopt -u nullglob
    [[ ${#scenarios[@]} -gt 0 ]] || { echo "no scenario files in $BENCH_DIR/scenarios" >&2; exit 1; }

    # Start each sweep from a clean slate so the report reflects only this run's data, not stale
    # JSON left by earlier sweeps. -f keeps a non-matching glob from erroring when output/ is empty.
    echo "Clearing previous run JSON from $BENCH_DIR/output/..."
    rm -f "$BENCH_DIR"/output/*.json

    echo "Building images once before the sweep..."
    "${COMPOSE[@]}" build > /dev/null 2>&1

    i=0
    for scenario in "${scenarios[@]}"; do
        i=$((i + 1))
        echo ""
        echo "########## [$i/${#scenarios[@]}] $(basename "$scenario") (x${REPS}) ##########"
        run_reps "$scenario" nobuild
    done
    echo ""
    echo "Sweep complete: ${#scenarios[@]} scenarios x ${REPS} reps, ${SKIPPED_REPS} reps skipped. Results in $BENCH_DIR/output/"

    # Aggregate every per-rep JSON into a CSV + Markdown report (stdlib only, so the venv
    # built during the sweep already has what it needs).
    echo "Generating report..."
    "$VENV_DIR/bin/python" "$BENCH_DIR/report.py" --output-dir "$BENCH_DIR/output" || true
else
    # Single-scenario mode. Interactive (Ctrl+C) unless RUN_DURATION is set, in which
    # case it runs unattended for that many seconds. Rebuilds to pick up code changes.
    # REPS defaults to 1; set it (with RUN_DURATION) to repeat an unattended single scenario.
    [[ -f "$MODE" ]] || { echo "scenario file not found: $MODE" >&2; exit 1; }
    run_reps "$MODE" build
fi
