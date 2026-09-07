# IoT Data Pipeline (Erlang/OTP)

A high-performance, containerized IoT data pipeline implemented using **Erlang/OTP** and **GenServers**. This project demonstrates how to build a scalable messaging system that handles real-time sensor data using the actor model's lightweight processes and fault-tolerant principles.

## 🏗️ System Architecture

The system consists of the following components:

1.  **[Service Publisher](service_publisher/)**: An Erlang application that simulates IoT sensors. Each instance generates sensor readings (JSON) and publishes them to an MQTT broker.
2.  **Mosquitto MQTT Broker**: Acts as the central messaging hub, facilitating communication between publishers and subscribers.
3.  **[Service Subscriber](service_subscriber/)**: An Erlang application that consumes messages from the `sensors/#` wildcard topic. It dynamically creates a dedicated **Worker Actor** (GenServer) for each unique sensor topic to maintain state (running sum and last timestamp) while tracking metrics. Features an ultra-fast lock-free ETS-based dynamic actor routing table and a parallel DB connection pool. The write path is pluggable: the active backend is selected at runtime via `DB_BACKEND`.
4.  **Prometheus**: Scrapes metrics from the containers and the host system.
5.  **Grafana**: Provides a visual dashboard for monitoring container resource usage (CPU/RAM) and application latency.
6.  **The database**: selected by `DB_BACKEND` — TimescaleDB (default), MySQL or InfluxDB.

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
    - `subscriber_request_latency_milliseconds` — publisher→subscriber latency, histogram.
    - `subscriber_e2e_latency_milliseconds` — publisher→DB latency, histogram.
    - `subscriber_db_write_latency_milliseconds` — subscriber→DB write latency, histogram.
    - `subscriber_reads_total` — completed read queries, counter. Driven by the subscriber's own
      readers rather than by ingest traffic, so it stays at 0 unless `READS_PER_SEC` is set.
    - `subscriber_read_latency_milliseconds` — read query latency, histogram.
    - All four share one bucket list with the other repo, so quantiles are comparable across arms
      by construction; `histogram_quantile()` computes them at query time over any window.
    - `subscriber_sensor_up{device="<name>"}` — per-sensor liveness gauge (1 = ALIVE, 0 = MISSING).
- **Subscriber Logs**:
    ```bash
    docker logs -f subscriber
    ```

## 🔬 Benchmarking

The subscriber's DB write path is decoupled from any specific database through a pluggable backend (`db_backend` behaviour). The active backend is selected at startup via the `DB_BACKEND` environment variable, with no recompilation required.

### How `bench.sh` works

`bench.sh` takes a scenario file, sources it as environment variables, starts the full Docker Compose stack with the configured number of publisher instances, and waits until the stack is actually ingesting — first for the subscriber's Prometheus endpoint to answer, then for `subscriber_requests_total` to start advancing. The second check matters because the metrics server binds before the subscriber has necessarily reached the broker or the database, so a subscriber that died during startup still serves a scrapeable `/metrics` with every counter at zero. A rep that does not begin ingesting within `INGEST_TIMEOUT` seconds (default `60`) is **skipped**: its container logs are dumped, no JSON is written, and the sweep moves on, so a dead run can never be averaged into a report cell. `bench.sh all` reports the number of skipped reps in its closing summary. It then sets up a Python virtual environment under `benchmarking/.venv` (installing `benchmarking/requirements.txt` automatically on first run — no manual setup needed) and hands off to `benchmarking/monitor.py`.

`monitor.py` queries Prometheus directly for the same panels shown on the "Container Monitoring" Grafana dashboard (ingest rate, committed-rows rate, the four latency histograms, sensor liveness, container CPU/memory) every `METRICS_INTERVAL` seconds (default: 10) and renders them as a live-updating view: a throughput/resource table plus a latency matrix (stage × quantile, with an exact mean beside them). It also collects the raw cumulative histogram buckets, which have no dashboard panel and exist purely so a run can be re-sliced offline. Press `Ctrl+C` to stop: it prints an avg/max summary and saves every raw, timestamped sample to a JSON file under `benchmarking/output/<backend>/`.

Aggregates cover **steady state only** — the first `WARMUP_SECONDS` (default `30`) are excluded, since throughput ramps toward steady state while publishers connect against a cold DB, and averaging that in understates throughput and inflates every latency figure. `RUN_DURATION` defaults to `90` in a sweep so a full minute of steady state remains after the trim and the 15s rate window. Every sample is still stored whole, so the trim is a reporting parameter: a run can be re-sliced at a different boundary without re-collecting it. `bench.sh` then runs a clean `docker compose down -v`.

The live view, with illustrative values — a shape, not a measurement:

```
=== Benchmark: <scenario>.env ===
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

**Single vs. batch mode.** `bench.sh <scenario>` runs one scenario interactively (stop with `Ctrl+C`). Set `RUN_DURATION` to run it unattended for a fixed number of seconds instead. `bench.sh all` sweeps **every** scenario in `benchmarking/scenarios/` back-to-back: it builds the images once up front, runs each for `RUN_DURATION` seconds (default `90`), writes one JSON per rep to `benchmarking/output/<backend>/`, and tears the stack down (`docker compose down -v`) between reps so each starts cold.

**Repetitions.** Each scenario is run `REPS` times (default `1`, but `3` in `bench.sh all`), tearing the stack down between reps so run-to-run noise can be told apart from a real runtime difference. Each rep's JSON is tagged `..._repN_...json`. Set `REPS` on a single scenario too (with `RUN_DURATION`) to repeat it unattended.

**Post-run report.** After a full `bench.sh all` sweep, `benchmarking/report.py` aggregates every per-rep JSON into [`benchmarking/output/timescaledb/report.csv`](benchmarking/output/timescaledb/report.csv) (one row per scenario) and [`benchmarking/output/timescaledb/report.md`](benchmarking/output/timescaledb/report.md) — one such pair per backend. The Markdown report has three parts: one **headline** table per OFAT group, a **startup** table, and a **provenance** block recording the trim, run duration, bucket-list fingerprint and rep counts needed to reproduce or re-slice the figures.

Headline tables report `msgs_s` (ingested) alongside `committed_s` (committed to the DB), and p50/p95/p99/p999 plus an exact mean for each of the four latencies — all over steady state only. Throughput and resource cells are `mean ± stdev` across reps. Latency cells are a **pooled quantile**: the reps' steady-state histogram buckets are summed and one quantile taken over the total, with the per-rep min–max shown where the reps disagreed by more than 5%. Averaging per-rep quantiles instead can report a value no rep ever observed — three reps whose p99s are 10 ms, 10 ms and 300 ms average to 107 ms, while the p99 of the same 3000 observations is 300 ms. The per-rep spread is what shows when the reps were not interchangeable.

The startup table carries one row per scenario: how long the stack took to serve metrics and then to ingest its first message, the measured time-to-steady-state, and the warm-up p99 — turning the startup transient from a contaminant into the JVM-ramp-vs-BEAM-flat-start comparison it should be. Time-to-steady-state is reported, never used to trim: a per-rep trim would give the two arms windows of different length and phase. `bench.sh all` first clears `benchmarking/output/<backend>/*.json` so the report covers only that sweep, and only for the backend being swept; run a single scenario and invoke `report.py` yourself if you'd rather accumulate runs across sweeps. Run it standalone at any time against an existing `output/` directory:

```bash
benchmarking/.venv/bin/python benchmarking/report.py
```

To override the poll interval, set `METRICS_INTERVAL` in your scenario file or environment (defaults: `5`s in duration mode, `10`s interactive).

### Running a scenario

```bash
chmod +x bench.sh

# One scenario, interactively (Ctrl+C to stop):
./bench.sh benchmarking/scenarios/load_p20.env

# One scenario, fixed 90s unattended run:
RUN_DURATION=90 ./bench.sh benchmarking/scenarios/load_p20.env

# The full OFAT sweep (all 30 scenarios, 90s each, unattended):
./bench.sh all

# Any of the above against a different database:
DB_BACKEND=<backend> ./bench.sh all
```

**Choosing the database.** `DB_BACKEND` is a launch-time choice, not part of a scenario: it selects
which `docker-compose.<backend>.yaml` fragment is loaded alongside the base compose file, so only
that database's container ever starts. It defaults to `timescaledb`, and `bench.sh` fails
immediately with the list of available backends if no such fragment exists. Scenario files
therefore carry only the factors they vary — no `DB_BACKEND`, no connection settings — which is
what lets one set of 30 describe every database and stay byte-identical to the other repo's set.

Each backend's runs and report land in their own directory, `benchmarking/output/<backend>/`, so
sweeping a second database can never overwrite the first's results.

### Available scenarios

All 30 scenarios follow a **one-factor-at-a-time (OFAT)** design: every file changes exactly one variable from a shared **anchor** (`20` publishers ≈ 20k msg/s, pool `20`, batching on at size `50` / timeout `200`ms, no payload padding), so any measured effect is attributable to that one factor.

The anchor batches because the single-row write path cannot sustain 20k msg/s: rows queue ahead of the database and end-to-end latency climbs for as long as the run lasts, which makes the measured percentiles a function of `RUN_DURATION` rather than of the runtime under test. The `Batching` group keeps one `BATCH_ENABLED=false` run as the reference point showing that.

Seven files are configuration-identical to the anchor (`load_p20`, `pool_20`, `payload_0`, `batchsize_050`, `batchto_0200`, `batching_on`, `reads_0000`). That redundancy is deliberate: at `REPS=3` a sweep measures the anchor 21 times, and the spread across those runs is the noise floor that differences elsewhere in the report should be judged against.

| Group | File pattern | Factor swept | Values (**bold** = anchor) |
|-------|--------------|--------------|----------------------------|
| Load | `load_p{NN}.env` | `PUBLISHER_COUNT` | 4, 8, 16, **20**, 32, 48 |
| Pool | `pool_{NN}.env` | `DB_POOL_SIZE` | 5, 10, **20**, 50 |
| Payload | `payload_{N}.env` | `PAYLOAD_PADDING_BYTES` | **0**, 256, 1KB, 10KB |
| Batch size | `batchsize_{NNN}.env` | `BATCH_SIZE` | 20, **50**, 100, 200, 500 |
| Batch timeout | `batchto_{NNNN}.env` | `BATCH_TIMEOUT_MS` | 50, **200**, 500, 1000 |
| Batching | `batching_{off,on}.env` | `BATCH_ENABLED` | off, **on** |
| DB reads | `reads_{NNNN}.env` | `READS_PER_SEC` | **0**, 20, 50, 80, 100 |

The two batch factors are not independent: a buffer flushes on whichever trigger fires first, so the
**effective batch size** is roughly `min(BATCH_SIZE, per-writer rate × BATCH_TIMEOUT_MS)`. Rows are routed
round-robin across `DB_POOL_SIZE` writers, and at the anchor a buffer fills well before the timeout expires,
which makes `BATCH_SIZE` the binding trigger under load and leaves the timeout as the latency guard for
low-rate periods. Sweeping the timeout below the fill time therefore does not measure the timeout so much as
silently shrink the effective batch. Where the timeout is the shorter of the two (`batchto_0050` at the
anchor) the timer wins every cycle rather than racing the buffer, because it is armed on the first row of
each new buffer instead of free-running — so the flush period is fixed and the resulting tail is *tighter*
than a size-triggered one, not noisier.

**The read group is deliberately artificial load.** `READ_POOL_SIZE` reader actors inside the subscriber
each run one query at a time — `SELECT avg(Value), count(*) FROM Data WHERE Timestamp > now() - interval
'5 seconds'`, fixed text and byte-identical across arms — so the point is concurrent query pressure on the
database and on the runtime's schedulers, not a realistic query mix. Reads live in the subscriber rather
than a separate container precisely because a separate container would contend for the database but not for
either runtime's schedulers or dispatchers, which is the only part that can distinguish the two arms.

A reader arms its next read only once the previous one has *returned*, so at most one query per reader is
ever in flight and `READS_PER_SEC` can never build a backlog. The delay is computed against a **fixed
deadline** that advances by exactly one period per cycle, not as period-minus-query-time: the latter leaves
each cycle carrying whatever the runtime spends outside the measured read, and that overhead differed enough
between the two arms that they ran measurably different read loads at the same setting. Deadline pacing
absorbs a constant lateness entirely.

The target is still a ceiling rather than a guarantee, so **report the achieved rate
(`subscriber_reads_total`), never the configured one** — an unreachable target shows up as an achieved rate
below it rather than as an error, at any setting.

Reads also change the connection budget: total database connections are `DB_POOL_SIZE + READ_POOL_SIZE`
while reads are on and `DB_POOL_SIZE` when `READS_PER_SEC=0`, identically in both arms. `READ_POOL_SIZE` is
documented but never swept — holding it fixed is what keeps the group's connection count constant and
independent of `PUBLISHER_COUNT`, so the read curve is not confounded by a moving connection count.

The no-batch-vs-batch comparison is the `Batching` group: `batching_off.env` versus
`batching_on.env` (the latter identical to the anchor).

### Scenario variables reference

| Variable | Default | Description |
|----------|---------|-------------|
| `PUBLISHER_COUNT` | `1` | Number of publisher containers (`--scale publisher=N`) |
| `DB_BACKEND` | `timescaledb` | Which database to run against. Set on the command line, **not** in a scenario file; selects the `docker-compose.<backend>.yaml` fragment |
| `DB_HOST` | `timescaledb` | Database host (`mysql` or `influxdb` under those backends). Supplied by the backend's compose fragment; identical key in both arms |
| `DB_PORT` | `5432` | Database port (`3306` MySQL, `8086` InfluxDB). Supplied by the backend's compose fragment; identical key in both arms |
| `DB_NAME` | `epu` | Database name, and the bucket name under `DB_BACKEND=influxdb`. Supplied by the backend's compose fragment; identical key in both arms |
| `DB_USER` / `DB_PASSWORD` | `postgres` | Database credentials (`root` / `mysql` under MySQL). Supplied by the backend's compose fragment. Under `DB_BACKEND=influxdb` these seed the initial user only — the API authenticates with `DB_TOKEN` — and InfluxDB enforces an 8-character minimum password |
| `DB_ORG` / `DB_TOKEN` | `epu` / `epu-benchmark-token` | InfluxDB only: the organisation and API token. The two keys beyond the five every backend reads; supplied by the compose fragment and read identically in both arms |
| `DB_POOL_SIZE` | `20` | Number of DB pool workers, and the total database connection count (one per worker). Status writes round-robin over the same pool. Under `DB_BACKEND=influxdb` it caps concurrent HTTP requests instead of counting pooled connections; see finding J in `../audit.md` |
| `BATCH_ENABLED` | `false` | Enable row buffering |
| `BATCH_SIZE` | `100` | Flush when buffer reaches this many rows |
| `BATCH_TIMEOUT_MS` | `1000` | Flush after this many ms even if buffer is not full |
| `PAYLOAD_PADDING_BYTES` | `0` | Extra filler bytes added as a trailing `"padding"` field on top of the shared base payload (identical in both arms), for payload-size benchmarks |
| `READS_PER_SEC` | `0` | Aggregate target read rate across all readers. `0` starts no readers at all — no actors, no connections, no timers. Achieved rate falls below this once the target exceeds what the readers can sustain |
| `READ_POOL_SIZE` | `4` | Reader actors, each holding one database connection, so total connections are `DB_POOL_SIZE + READ_POOL_SIZE` while reads are on. Documented but not swept |
| `RUN_DURATION` | _(unset)_ | Fixed measurement window in seconds. Set it (or use `bench.sh all`, which defaults it to `90`) for an unattended run; leave unset for an interactive `Ctrl+C` run |
| `WARMUP_SECONDS` | `30` | Startup transient excluded from every reported figure. Must be at least the 15s rate window, or the first samples kept are still contaminated by the ramp |
| `STARTUP_TIMEOUT` | `180` | Seconds a rep may take to bring up a scrapeable metrics endpoint before it is skipped |
| `INGEST_TIMEOUT` | `60` | Seconds a rep may take to start ingesting, once its endpoint answers, before it is skipped |
| `REPS` | `1` | Times each scenario is repeated, with a full teardown between reps (`bench.sh all` defaults it to `3`). The report collapses reps to mean ± stdev, and pools their histogram buckets for latency |
| `METRICS_INTERVAL` | `5` / `10` | Seconds between metric snapshots (defaults to `5` in duration mode, `10` interactive) |

### Supported `DB_BACKEND` values

| Value | Modules | Description |
|-------|---------|-------------|
| `timescaledb` (default) | `db_backend_timescaledb`, `db_read_backend_timescaledb` | PostgreSQL/TimescaleDB via epgsql, async writes |
| `mysql` | `db_backend_mysql`, `db_read_backend_mysql` | MySQL 8.4 via mysql-otp. The driver is synchronous, so writes are handed to a short-lived spawned process and the ack is messaged back — the backend still satisfies the behaviour's `{async, State}` contract |
| `influxdb` | `db_backend_influxdb`, `db_read_backend_influxdb` | InfluxDB 2.7 over its HTTP API via `httpc`, no driver dependency. Writes are line protocol posted asynchronously, so the ack reaches the worker's mailbox and the behaviour's `{async, State}` contract is satisfied without a helper process |

Each SQL value needs its own `<backend>/init/init.sql`, written in that database's own dialect, so
the schemas are equivalent rather than identical. `influxdb` has none: it is schema-on-write, the
image creates the bucket, and what the columns are is decided by the backend's line-protocol
builders. See findings H and L in `../audit.md`.

Adding a backend is three things in this repo: the modules, their one-line registrations, and a
`docker-compose.<backend>.yaml` fragment carrying that database's service, volume and connection
variables, plus a schema if that database needs one. Nothing in `benchmarking/` changes — the
scenario files are backend-agnostic.

> **MySQL cannot pipeline.** One connection carries one query at a time, so in-flight writes are
> capped at `DB_POOL_SIZE` where epgsql can hold several per connection. With `BATCH_ENABLED=false`
> that cap is binding at the swept load and the subscriber builds an unbounded backlog; with
> batching on it keeps up comfortably. See finding G in `../audit.md` — `pool_*` and `batching_*`
> results are not comparable across databases.

> **InfluxDB is HTTP, which changes three things.** `DB_POOL_SIZE` caps concurrent requests rather
> than counting connections; a write is an upsert keyed by timestamp rather than an append; and one
> Flux read costs far more than its SQL equivalent, so the `reads_*` ladder is already at its ceiling
> by `reads_0050`. See findings I through O in `../audit.md` before comparing this backend's
> `pool_*`, `batching_*` or `reads_*` numbers against the SQL ones.

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
- **Database**: Pluggable backends — TimescaleDB (default), MySQL, InfluxDB
- **Deployment**: Docker & Docker Compose
