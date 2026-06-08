# IoT Data Pipeline (Erlang/OTP)

A high-performance, containerized IoT data pipeline implemented using **Erlang/OTP** and **GenServers**. This project demonstrates how to build a scalable messaging system that handles real-time sensor data using the actor model's lightweight processes and fault-tolerant principles.

## 🏗️ System Architecture

The system consists of the following components:

1.  **Service Publisher**: An Erlang application that simulates IoT sensors. Each instance generates sensor readings (JSON) and publishes them to an MQTT broker. See [service_publisher/publisher.md](service_publisher/publisher.md) for more details.
2.  **Mosquitto MQTT Broker**: Acts as the central messaging hub, facilitating communication between publishers and subscribers.
3.  **Service Subscriber**: An Erlang application that consumes messages from the `sensors/#` wildcard topic. It dynamically creates a dedicated **Worker Actor** (GenServer) for each unique sensor topic to maintain state (running sum and last timestamp) while tracking metrics. Features an ultra-fast lock-free ETS-based dynamic actor routing table and a parallel DB connection pool. The write path is pluggable: the active backend is selected at runtime via `DB_BACKEND`. See [service_subscriber/](service_subscriber/) for more details.
4.  **Prometheus**: Scrapes metrics from the containers and the host system.
5.  **Grafana**: Provides a visual dashboard for monitoring container resource usage (CPU/RAM) and application latency.

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

To override the poll interval, set `METRICS_INTERVAL=5` in your scenario file.

### Running a scenario

```bash
chmod +x bench.sh
./bench.sh benchmarking/scenarios/timescale.env
```

Press `Ctrl+C` to stop the stack when done.

### Available scenarios

| File | Publishers | Batch | DB pool | Load |
|------|-----------|-------|---------|------|
| `benchmarking/scenarios/timescale.env` | 5 | off | 20 | ~5k msg/s baseline |
| `benchmarking/scenarios/timescale_batch.env` | 5 | on (100 rows, 1s) | 5 | ~5k msg/s with batching |
| `benchmarking/scenarios/timescale_stress.env` | 20 | off | 20 | ~20k msg/s stress |
| `benchmarking/scenarios/timescale_batch_stress.env` | 20 | on (100 rows, 0.5s) | 5 | ~20k msg/s with batching |

### Scenario variables reference

| Variable | Default | Description |
|----------|---------|-------------|
| `PUBLISHER_COUNT` | `1` | Number of publisher containers (`--scale publisher=N`) |
| `DB_BACKEND` | `timescaledb` | Backend module to use |
| `DB_POOL_SIZE` | `20` | Number of DB pool workers |
| `BATCH_ENABLED` | `false` | Enable row buffering |
| `BATCH_SIZE` | `100` | Flush when buffer reaches this many rows |
| `BATCH_TIMEOUT_MS` | `1000` | Flush after this many ms even if buffer is not full |
| `METRICS_INTERVAL` | `10` | Seconds between metric snapshots in the bench output |

### Supported `DB_BACKEND` values

| Value | Module | Description |
|-------|--------|-------------|
| `timescaledb` (default) | `db_backend_timescaledb` | PostgreSQL/TimescaleDB via epgsql, async writes |

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
