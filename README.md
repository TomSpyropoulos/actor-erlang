# IoT Data Pipeline (Erlang/OTP)

A high-performance, containerized IoT data pipeline implemented using **Erlang/OTP** and **GenServers**. This project demonstrates how to build a scalable messaging system that handles real-time sensor data using the actor model's lightweight processes and fault-tolerant principles.

## 🏗️ System Architecture

The system consists of the following components:

1.  **Service Publisher**: An Erlang application that simulates IoT sensors. Each instance generates sensor readings (JSON) and publishes them to an MQTT broker. See [service_publisher/publisher.md](service_publisher/publisher.md) for more details.
2.  **Mosquitto MQTT Broker**: Acts as the central messaging hub, facilitating communication between publishers and subscribers.
3.  **Service Subscriber**: An Erlang application that consumes messages from the `sensors/#` wildcard topic. It dynamically creates a dedicated **Worker Actor** (GenServer) for each unique sensor topic to maintain state (running sum and last timestamp) while tracking metrics. Features an ultra-fast lock-free ETS-based dynamic actor routing table and a parallel DB connection pool. The write path is pluggable: the active backend is selected at runtime via `DB_BACKEND`. See [service_subscriber/](service_subscriber/) for more details.
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

`monitor.py` queries Prometheus directly for the same panels shown on the "Container Monitoring" Grafana dashboard (message rate, the three latency summaries, sensor liveness, container CPU/memory) every `METRICS_INTERVAL` seconds (default: 10) and renders them as a live-updating table. Press `Ctrl+C` to stop: it prints an avg/max summary of the run and saves every raw, timestamped sample to a JSON file under `benchmarking/output/` — so you can revisit or replot a run's data later without re-running the (slow) benchmark. `bench.sh` then runs a clean `docker compose down -v`.

```
=== Benchmark: timescale_batch.env ===
  Publishers  : 5  (~5000 msg/s)
  Batch       : true  (size=100, timeout=1000ms)
  DB pool     : 5 workers
  Backend     : timescaledb

                     Live benchmark metrics
  time      msgs/s  req p50  req p99  e2e p50  e2e p99  db p50  db p99  sensors up/down  cpu (cores)  mem (MB)
 14:22:01    1245    12.1 ms  39.8 ms  28.3 ms  54.1 ms  27.1 ms  32.9 ms      3 / 0          0.06        180.4
 14:22:11    2445    12.3 ms  40.1 ms  27.9 ms  53.8 ms  26.8 ms  33.1 ms      3 / 0          0.07        184.9
```

**Single vs. batch mode.** `bench.sh <scenario>` runs one scenario interactively (stop with `Ctrl+C`). Set `RUN_DURATION` to run it unattended for a fixed number of seconds instead. `bench.sh all` sweeps **every** scenario in `benchmarking/scenarios/` back-to-back: it builds the images once up front, runs each for `RUN_DURATION` seconds (default `60`), writes one JSON per scenario to `benchmarking/output/`, and tears the stack down (`docker compose down -v`) between runs so each starts cold.

To override the poll interval, set `METRICS_INTERVAL` in your scenario file or environment (defaults: `5`s in duration mode, `10`s interactive).

### Running a scenario

```bash
chmod +x bench.sh

# One scenario, interactively (Ctrl+C to stop):
./bench.sh benchmarking/scenarios/timescale_load_p20.env

# One scenario, fixed 60s unattended run:
RUN_DURATION=60 ./bench.sh benchmarking/scenarios/timescale_load_p20.env

# The full OFAT sweep (all 24 scenarios, 60s each, unattended):
./bench.sh all
```

### Available scenarios

All 24 scenarios follow a **one-factor-at-a-time (OFAT)** design: every file changes exactly one variable from a shared **anchor** (`20` publishers ≈ 20k msg/s, pool `20`, batching off, no payload padding), so any measured effect is attributable to that one factor. The batch scenarios share a **batch sub-anchor** (batching on, size `100`, timeout `500`ms) that differs from the anchor only by enabling batching.

| Group | File pattern | Factor swept | Values (**bold** = anchor) |
|-------|--------------|--------------|----------------------------|
| Load | `timescale_load_p{NN}.env` | `PUBLISHER_COUNT` | 4, 8, 16, **20**, 32, 48, 64 |
| Pool | `timescale_pool_{NN}.env` | `DB_POOL_SIZE` | 5, 10, **20**, 50 |
| Payload | `timescale_payload_{N}.env` | `PAYLOAD_PADDING_BYTES` | **0**, 256, 1KB, 10KB |
| Batch size | `timescale_batchsize_{NNN}.env` | `BATCH_SIZE` (batch on) | 20, 50, **100**, 200, 500 |
| Batch timeout | `timescale_batchto_{NNNN}.env` | `BATCH_TIMEOUT_MS` (batch on) | 50, 200, **500**, 1000 |

The no-batch-vs-batch comparison is the anchor (`timescale_load_p20.env`) versus the batch sub-anchor (`timescale_batchsize_100.env`, identical to `timescale_batchto_0500.env`).

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
| `METRICS_INTERVAL` | `5` / `10` | Seconds between metric snapshots (defaults to `5` in duration mode, `10` interactive) |

### Supported `DB_BACKEND` values

| Value | Module | Description |
|-------|--------|-------------|
| `timescaledb` (default) | `db_backend_timescaledb` | PostgreSQL/TimescaleDB via epgsql, async writes |

### TODO / Planned

- **Warm-up analysis** — runs currently measure from `t=0` including startup. Retain the per-interval time series and analyse the startup transient (latency-vs-time, time-to-steady-state — the JVM ramp vs. BEAM's flat start) separately from the steady-state plateau, rather than folding both into one aggregate.
- **Repetitions (K=3)** — run each scenario 3× with a full teardown between reps and report mean ± stdev, so a runtime difference can be told apart from run-to-run noise. Pilot the spread on one scenario first to confirm K. (Needs a rep tag in `monitor.py`'s output filename and a rep loop in `bench.sh`.)
- **Post-run report** — after a full sweep, aggregate the per-scenario JSON into a **CSV** (one row per runtime × scenario) and write a **Markdown** report (load-saturation curves, OFAT plots, batch crossover, interpretation) for lifting into the thesis. Excel only as an optional throwaway export.

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
