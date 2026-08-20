# IoT Data Pipeline (Erlang/OTP)

A high-performance, containerized IoT data pipeline implemented using **Erlang/OTP** and **GenServers**. This project demonstrates how to build a scalable messaging system that handles real-time sensor data using the actor model's lightweight processes and fault-tolerant principles.

## 🏗️ System Architecture

The system consists of the following components:

1.  **[Service Publisher](service_publisher/)**: An Erlang application that simulates IoT sensors. Each instance generates sensor readings (JSON) and publishes them to an MQTT broker.
2.  **Mosquitto MQTT Broker**: Acts as the central messaging hub, facilitating communication between publishers and subscribers.
3.  **[Service Subscriber](service_subscriber/)**: An Erlang application that consumes messages from the `sensors/#` wildcard topic. It dynamically creates a dedicated **Worker Actor** (GenServer) for each unique sensor topic to maintain state (running sum and last timestamp) while tracking metrics. Features an ultra-fast lock-free ETS-based dynamic actor routing table and a parallel DB connection pool. The write path is pluggable: the active backend is selected at runtime via `DB_BACKEND`.
4.  **Prometheus**: Scrapes metrics from the containers and the host system.
5.  **Grafana**: Provides a visual dashboard for monitoring container resource usage (CPU/RAM) and application latency.
6.  **TimescaleDB**: A PostgreSQL extension for high-performance time-series data storage.

## 🚀 Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

### Running the Pipeline

To start the entire stack with 3 simulated sensors (publisher instances):

```bash
docker compose up -d --build --scale publisher=3
```

### Monitoring & Logs

- **Grafana**: Accessible at [http://localhost:3000](http://localhost:3000) without authentication.
    - Pre-provisioned with Prometheus and a "Container Monitoring" dashboard.
- **Subscriber Metrics**: Raw Prometheus endpoint at [http://localhost:8081/metrics](http://localhost:8081/metrics).
    - `subscriber_requests_total` — throughput counter.
    - `subscriber_request_latency_milliseconds` — publisher→subscriber latency (p50/p95/p99/p999).
    - `subscriber_e2e_latency_milliseconds` — publisher→DB latency (p50/p95/p99/p999).
    - `subscriber_db_write_latency_milliseconds` — subscriber→DB write latency (p50/p95/p99/p999).
    - `subscriber_sensor_up{device="<name>"}` — per-sensor liveness gauge (1 = ALIVE, 0 = MISSING).
- **Subscriber Logs**:
    ```bash
    docker logs -f subscriber
    ```

## 🔬 Benchmarking

The subscriber's DB write path is decoupled from any specific database through a pluggable backend (`db_backend` behaviour). The active backend is selected at startup via the `DB_BACKEND` environment variable, with no recompilation required.

### How `bench.sh` works

`bench.sh` takes a scenario file, sources it as environment variables, starts the full Docker Compose stack with the configured number of publisher instances, and waits for the subscriber's Prometheus endpoint to come up. It then sets up a Python virtual environment under `benchmarking/.venv` (installing `benchmarking/requirements.txt` automatically on first run — no manual setup needed) and hands off to `benchmarking/monitor.py`.

`monitor.py` queries Prometheus directly for the same panels shown on the "Container Monitoring" Grafana dashboard (ingest rate, committed-rows rate, the three latency summaries, sensor liveness, container CPU/memory) every `METRICS_INTERVAL` seconds (default: 10) and renders them as a live-updating view: a throughput/resource table plus a latency matrix (stage × quantile). Press `Ctrl+C` to stop: it prints an avg/max summary of the run and saves every raw, timestamped sample to a JSON file under `benchmarking/output/` — so you can revisit or replot a run's data later without re-running the (slow) benchmark. `bench.sh` then runs a clean `docker compose down -v`.

```
=== Benchmark: timescale_batch.env ===
  Publishers  : 5  (~5000 msg/s)
  Batch       : true  (size=100, timeout=1000ms)
  DB pool     : 5 workers
  Backend     : timescaledb

                      Live benchmark metrics
   time      msgs/s   committed/s   sensors up/down   cpu (cores)   mem (MB)
  14:22:11    2445        2441           3 / 0            0.07        184.9

                    Latencies (ms)
  stage             p50     p95     p99    p999
  req (pub->sub)   12.3    28.4    40.1    58.2
  e2e (pub->db)    27.9    44.6    53.8    77.5
  db  (sub->db)    26.8    43.2    52.4    76.1
```

`msgs/s` counts messages **ingested** off MQTT, while `committed/s` counts rows **actually written to the database**. They track each other while the DB keeps up; a sustained gap between them is the clearest signal that the write path — not the pipeline — is the bottleneck.

**Single vs. batch mode.** `bench.sh <scenario>` runs one scenario interactively (stop with `Ctrl+C`). Set `RUN_DURATION` to run it unattended for a fixed number of seconds instead. `bench.sh all` sweeps **every** scenario in `benchmarking/scenarios/` back-to-back: it builds the images once up front, runs each for `RUN_DURATION` seconds (default `60`), writes one JSON per rep to `benchmarking/output/`, and tears the stack down (`docker compose down -v`) between reps so each starts cold.

**Repetitions.** Each scenario is run `REPS` times (default `1`, but `3` in `bench.sh all`), tearing the stack down between reps so run-to-run noise can be told apart from a real runtime difference. Each rep's JSON is tagged `..._repN_...json`. Set `REPS` on a single scenario too (with `RUN_DURATION`) to repeat it unattended.

**Post-run report.** After a full `bench.sh all` sweep, `benchmarking/report.py` aggregates every per-rep JSON into [`benchmarking/output/report.csv`](benchmarking/output/report.csv) (one row per scenario) and [`benchmarking/output/report.md`](benchmarking/output/report.md) (one Markdown table per OFAT group, each cell `mean ± stdev` across reps). Every table reports `msgs_s` (ingested) alongside `committed_s` (committed to the DB), and p50/p95/p99/p999 for each of the three latencies. `bench.sh all` first clears `benchmarking/output/*.json` so the report covers only that sweep; run a single scenario and invoke `report.py` yourself if you'd rather accumulate runs across sweeps. Run it standalone at any time against an existing `output/` directory:

```bash
benchmarking/.venv/bin/python benchmarking/report.py
```

To override the poll interval, set `METRICS_INTERVAL` in your scenario file or environment (defaults: `5`s in duration mode, `10`s interactive).

### Running a scenario

```bash
chmod +x bench.sh

# One scenario, interactively (Ctrl+C to stop):
./bench.sh benchmarking/scenarios/timescale_load_p20.env

# One scenario, fixed 60s unattended run:
RUN_DURATION=60 ./bench.sh benchmarking/scenarios/timescale_load_p20.env

# The full OFAT sweep (all 26 scenarios, 60s each, unattended):
./bench.sh all
```

### Available scenarios

All 26 scenarios follow a **one-factor-at-a-time (OFAT)** design: every file changes exactly one variable from a shared **anchor** (`20` publishers ≈ 20k msg/s, pool `20`, batching on at size `50` / timeout `200`ms, no payload padding), so any measured effect is attributable to that one factor.

The anchor batches because the single-row write path cannot sustain 20k msg/s: rows queue ahead of the database and end-to-end latency climbs for as long as the run lasts, which makes the measured percentiles a function of `RUN_DURATION` rather than of the runtime under test. The `Batching` group keeps one `BATCH_ENABLED=false` run as the reference point showing that.

Since there is now a single anchor, six files are identical to it (`load_p20`, `pool_20`, `payload_0`, `batchsize_050`, `batchto_0200`, `batching_on`). That redundancy is deliberate: at `REPS=3` a sweep measures the anchor 18 times, and the spread across those runs is the noise floor that differences elsewhere in the report should be judged against.

| Group | File pattern | Factor swept | Values (**bold** = anchor) |
|-------|--------------|--------------|----------------------------|
| Load | `timescale_load_p{NN}.env` | `PUBLISHER_COUNT` | 4, 8, 16, **20**, 32, 48, 64 |
| Pool | `timescale_pool_{NN}.env` | `DB_POOL_SIZE` | 5, 10, **20**, 50 |
| Payload | `timescale_payload_{N}.env` | `PAYLOAD_PADDING_BYTES` | **0**, 256, 1KB, 10KB |
| Batch size | `timescale_batchsize_{NNN}.env` | `BATCH_SIZE` | 20, **50**, 100, 200, 500 |
| Batch timeout | `timescale_batchto_{NNNN}.env` | `BATCH_TIMEOUT_MS` | 50, **200**, 500, 1000 |
| Batching | `timescale_batching_{off,on}.env` | `BATCH_ENABLED` | off, **on** |

The two batch factors are not independent: a buffer flushes on whichever trigger fires first, so the
**effective batch size** is roughly `min(BATCH_SIZE, per-writer rate × BATCH_TIMEOUT_MS)`. Rows are routed
round-robin across `DB_POOL_SIZE` writers, so at the anchor each writer sees ≈ 19k/20 ≈ 950 rows/s and a
50-row buffer fills in ≈ 53ms — well under the 200ms timeout, which makes `BATCH_SIZE` the binding trigger
under load and leaves the timeout as the latency guard for low-rate periods. Sweeping the timeout below the
fill time therefore does not measure the timeout so much as silently shrink the effective batch, and cells
where the two triggers are comparable (e.g. `batchto_0050` at the anchor's 50-row buffer) tend to alternate
between them and show correspondingly wide run-to-run spread.

The no-batch-vs-batch comparison is the `Batching` group: `timescale_batching_off.env` versus
`timescale_batching_on.env` (the latter identical to the anchor).

### Scenario variables reference

| Variable | Default | Description |
|----------|---------|-------------|
| `PUBLISHER_COUNT` | `1` | Number of publisher containers (`--scale publisher=N`) |
| `DB_BACKEND` | `timescaledb` | Backend module to use |
| `DB_POOL_SIZE` | `20` | Number of DB pool workers |
| `BATCH_ENABLED` | `false` | Enable row buffering |
| `BATCH_SIZE` | `100` | Flush when buffer reaches this many rows |
| `BATCH_TIMEOUT_MS` | `1000` | Flush after this many ms even if buffer is not full |
| `PAYLOAD_PADDING_BYTES` | `0` | Extra filler bytes added as a `"padding"` field in each publisher's JSON payload, for payload-size benchmarks |
| `RUN_DURATION` | _(unset)_ | Fixed measurement window in seconds. Set it (or use `bench.sh all`, which defaults it to `60`) for an unattended run; leave unset for an interactive `Ctrl+C` run |
| `REPS` | `1` | Times each scenario is repeated, with a full teardown between reps (`bench.sh all` defaults it to `3`). The report collapses reps to mean ± stdev |
| `METRICS_INTERVAL` | `5` / `10` | Seconds between metric snapshots (defaults to `5` in duration mode, `10` interactive) |

### Supported `DB_BACKEND` values

| Value | Module | Description |
|-------|--------|-------------|
| `timescaledb` (default) | `db_backend_timescaledb` | PostgreSQL/TimescaleDB via epgsql, async writes |

### TODO / Planned

- **Warm-up analysis** — runs currently measure from `t=0` including startup. Retain the per-interval time series and analyse the startup transient (latency-vs-time, time-to-steady-state — the JVM ramp vs. BEAM's flat start) separately from the steady-state plateau, rather than folding both into one aggregate. **Prerequisite:** the latency metrics must first move from Prometheus quantile **summaries** to **histograms**. A summary exposes a pre-computed quantile over a fixed sliding window, so it can't be re-sliced after the fact; a histogram keeps the raw bucket counts, letting `histogram_quantile()` recompute a quantile over just the post-warm-up window. Until that migration lands, a steady-state cutoff can't be applied to the latency percentiles.

## 🧠 Deep Dive: Erlang Concurrency

### Lightweight Processes vs. OS Threads

The project leverages Erlang's unique concurrency model, which is built on **Lightweight Processes** rather than OS threads.

#### Erlang Processes
Unlike OS threads, Erlang processes are managed by the Erlang Runtime System (ERTS).
- **Lightweight**: Each process starts with only a few kilobytes of memory, allowing for millions of concurrent processes.
- **Preemptive**: The Erlang Scheduler ensures that no single process can hog the CPU, providing fair distribution of execution time.
- **Pros**: Extremely efficient for massive concurrency and fault-tolerant systems.
- **Cons**: Requires a different mental model (message passing) compared to shared-state concurrency.

#### Fault Tolerance (Let It Crash)
Erlang's "Let It Crash" philosophy is implemented through supervisors and process monitoring.
- **Behavior**: Instead of defensive programming with complex try-catch blocks, Erlang encourages failing fast and allowing a supervisor to restart the process to a known good state.
- **Advantage**: Creates highly resilient systems where localized failures do not bring down the entire application.

The current implementation uses **GenServers** to encapsulate state and logic, managed by a supervisor tree defined in `service_subscriber_sup.erl`.

## 🛠️ Tech Stack

- **Language**: Erlang/OTP
- **Concurrency**: GenServers & Lightweight Processes
- **Messaging**: MQTT (Mosquitto/via emqtt)
- **JSON**: Built-in json (OTP 27+)
- **Observability**: Prometheus & Grafana
- **Database**: Pluggable backends (TimescaleDB default)
- **Deployment**: Docker & Docker Compose
