# IoT Data Pipeline (Erlang/OTP)

A containerized IoT data pipeline in Erlang/OTP. Publishers simulate sensors, a subscriber
consumes their readings over MQTT, and a pluggable backend writes them to a database. This
repository is one arm of a benchmark. An arm is one of two implementations of the same workload,
and the other arm uses Scala and Pekko.

## System Architecture

The system has six components:

1.  **[Service Publisher](service_publisher/)**: An Erlang application that simulates IoT sensors. Each instance generates sensor readings as JSON and publishes them to an MQTT broker.
2.  **Mosquitto MQTT Broker**: The central message hub between the publishers and the subscriber.
3.  **[Service Subscriber](service_subscriber/)**: An Erlang application that consumes messages from the `sensors/#` wildcard topic. It creates one worker actor (a GenServer) for each sensor topic. Each worker keeps the running sum and the last timestamp of its sensor, and records metrics. A lock-free routing table in ETS (the in-memory term store of Erlang) maps each topic to its worker. A pool of database workers writes the rows in parallel. `DB_BACKEND` selects the write path at startup.
4.  **Prometheus**: Scrapes metrics from the containers and the host system.
5.  **Grafana**: Shows dashboards for the CPU and memory use of each container, and for application latency.
6.  **The database**: `DB_BACKEND` selects it. The choices are TimescaleDB (default), MySQL, InfluxDB, SQLite and MongoDB. SQLite is a library, so it runs inside the subscriber and not in a container of its own.

## Getting Started

### Prerequisites

You need [Docker](https://docs.docker.com/get-docker/) and
[Docker Compose](https://docs.docker.com/compose/install/). Install both before you start.

### Running the Pipeline

Start the stack with 3 simulated sensors, which are 3 publisher instances:

```bash
docker compose up -d --build --scale publisher=3
```

### Monitoring and Logs

Grafana runs at [http://localhost:3000](http://localhost:3000) and needs no authentication. It
comes provisioned with Prometheus as its data source and with a "Container Monitoring" dashboard.

The subscriber serves raw Prometheus metrics at
[http://localhost:8081/metrics](http://localhost:8081/metrics):

- `subscriber_requests_total`: throughput counter.
- `subscriber_request_latency_milliseconds`: publisher to subscriber latency, histogram.
- `subscriber_e2e_latency_milliseconds`: publisher to database latency, histogram.
- `subscriber_db_write_latency_milliseconds`: subscriber to database write latency, histogram.
- `subscriber_reads_total`: completed read queries, counter. The readers of the subscriber drive it, and ingest traffic does not, so it stays at 0 unless you set `READS_PER_SEC`.
- `subscriber_read_latency_milliseconds`: read query latency, histogram.
- `subscriber_sensor_up{device="<name>"}`: per-sensor liveness gauge, where 1 is ALIVE and 0 is MISSING.

The four latency histograms share one bucket list with the other arm, so quantiles are comparable
between the arms by construction. `histogram_quantile()` computes a quantile at query time over
any window.

Follow the logs of the subscriber:

```bash
docker logs -f subscriber
```

## Benchmarking

The write path of the subscriber does not depend on any one database. A pluggable backend, the
`db_backend` behaviour, carries it. `DB_BACKEND` selects the active backend at startup, and you
do not have to recompile.

### How `bench.sh` works

`bench.sh` runs one benchmark. It takes a scenario file and reads it as environment variables. It
then starts the Docker Compose stack with the configured number of publisher instances.

`bench.sh` next waits until the stack really ingests messages. It first waits for the Prometheus
endpoint of the subscriber to answer. It then waits for `subscriber_requests_total` to advance.
The second check matters because the metrics server binds before the subscriber reaches the
broker or the database. A subscriber that died during startup still serves a scrapeable
`/metrics` with every counter at zero.

A rep is one measured run of one scenario. If a rep does not start to ingest within
`INGEST_TIMEOUT` seconds (default `60`), `bench.sh` skips it. It dumps the container logs, writes
no JSON file, and moves on, so a dead run can never enter a report cell. `bench.sh all` reports
the number of skipped reps in its closing summary.

`bench.sh` then creates a Python virtual environment under `benchmarking/.venv`. It installs
`benchmarking/requirements.txt` on the first run, so you do not have to install anything by hand.
It then starts `benchmarking/monitor.py`.

`monitor.py` queries Prometheus for the same panels that the "Container Monitoring" dashboard in
Grafana shows. Those panels are the ingest rate, the committed-rows rate, the four latency
histograms, sensor liveness, and the CPU and memory use of each container. It polls every
`METRICS_INTERVAL` seconds (default `10`) and renders a live view. The view holds a table of
throughput and resources, plus a latency matrix of stage against quantile with an exact mean
beside it.

`monitor.py` also collects the raw cumulative histogram buckets. No dashboard panel shows them,
and they exist so that you can re-slice a run offline. Press `Ctrl+C` to stop. `monitor.py` then
prints a summary of averages and maxima, and saves every raw timestamped sample to a JSON file
under `benchmarking/output/<backend>/`.

Every reported figure covers steady state only, which is the part of a run after the startup
transient. `WARMUP_SECONDS` (default `30`) sets how much of the start each report excludes, and
this excluded part is the trim. Throughput ramps toward steady state while the publishers connect
against a cold database. An average over that ramp understates throughput and inflates every
latency figure. `RUN_DURATION` defaults to `90` in a sweep, so a full minute of steady state
remains after the trim and the 15s rate window.

The trim is a reporting parameter and not a collection parameter. `monitor.py` stores every
sample whole, so you can re-slice a run at a different boundary without collecting it again.
`bench.sh` then runs a clean `docker compose down -v`.

The live view looks like this. The values are illustrative and show the shape of the view, not a
measurement.

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

`msgs/s` counts the messages that the subscriber ingests from MQTT. `committed/s` counts the rows
that the database really writes. The two track each other while the database keeps up. A
sustained gap between them is the clearest signal that the write path is the bottleneck, not the
pipeline.

### Single runs and sweeps

`bench.sh <scenario>` runs one scenario interactively, and `Ctrl+C` stops it. Set `RUN_DURATION`
to run one scenario unattended for a fixed number of seconds instead.

`bench.sh all` sweeps every scenario in `benchmarking/scenarios/` one after another. It builds the
images first. It runs each scenario for `RUN_DURATION` seconds (default `90`). It writes one JSON
file per rep to `benchmarking/output/<backend>/`. It runs `docker compose down -v` between reps,
so every rep starts cold.

### Repetitions

`bench.sh` runs each scenario `REPS` times. `REPS` defaults to `1`, and `bench.sh all` sets it to
`3`. The stack goes down between reps, so you can distinguish run-to-run noise from a real
difference between the runtimes. Each rep writes a JSON file tagged `..._repN_...json`. You can
set `REPS` on a single scenario too, together with `RUN_DURATION`, to repeat it unattended.

### Post-run report

After a full `bench.sh all` sweep, `benchmarking/report.py` aggregates every per-rep JSON file.
It writes one pair of files per backend:
[`benchmarking/output/timescaledb/report.csv`](benchmarking/output/timescaledb/report.csv), with
one row per scenario, and
[`benchmarking/output/timescaledb/report.md`](benchmarking/output/timescaledb/report.md).

The Markdown report has three parts. The first is one headline table per scenario group, and the
section "Available scenarios" below describes the groups. The second is a startup table. The
third is a provenance block, which records the trim, the run duration, the fingerprint of the
bucket list and the rep counts. Those four values are what you need to reproduce or re-slice the
figures.

A headline table reports `msgs_s`, which is ingested, beside `committed_s`, which is committed to
the database. It also reports p50, p95, p99 and p999, plus an exact mean, for each of the four
latencies. Every cell covers steady state only. Throughput and resource cells are a mean and a
standard deviation across the reps.

A latency cell is a pooled quantile. `report.py` sums the steady-state histogram buckets of all
reps and takes one quantile over the total. It shows the per-rep minimum and maximum where the
reps disagreed by more than 5%. An average of per-rep quantiles can report a value that no rep
observed. Three reps with p99 values of 10 ms, 10 ms and 300 ms average to 107 ms. The p99 of the
same 3000 observations is 300 ms. The per-rep spread is what shows you that the reps were not
interchangeable.

The startup table carries one row per scenario. The row gives four figures. The first two are how long
the stack took to serve metrics and how long it then took to ingest its first message. The other
two are the measured time to steady state and the warm-up p99. Those figures turn the startup
transient from a contaminant into a measurement of its own. They compare the ramp of the JVM
against the flat start of the BEAM.

`report.py` reports the time to steady state but never trims by it. A per-rep trim gives the two
arms windows of different length and phase.

`bench.sh all` first deletes `benchmarking/output/<backend>/*.json`, so the report covers only
that sweep and only the backend under test. If you want to accumulate runs across sweeps, run a
single scenario and call `report.py` yourself. You can run `report.py` at any time against an
existing `output/` directory:

```bash
benchmarking/.venv/bin/python benchmarking/report.py
```

To change the poll interval, set `METRICS_INTERVAL` in your scenario file or in the environment.
It defaults to `5` seconds in duration mode and to `10` seconds in interactive mode.

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

### Choosing the database

`DB_BACKEND` is a launch-time choice and not part of a scenario. It selects which
`docker-compose.<backend>.yaml` fragment loads beside the base compose file, so only that
database ever starts. It defaults to `timescaledb`. If no such fragment exists, `bench.sh` stops
at once and prints the list of available backends.

Scenario files therefore carry only the factors that they vary. They carry no `DB_BACKEND` and no
connection variables. That is what lets one set of 30 files describe every database and stay
byte-identical to the set in the other arm.

Each backend writes its runs and its report to its own directory,
`benchmarking/output/<backend>/`. A sweep of a second database can therefore never overwrite the
results of the first.

### Available scenarios

All 30 scenarios follow a one-factor-at-a-time (OFAT) design. Every file changes exactly one
variable from a shared anchor, so any measured effect belongs to that one factor. The anchor is
20 publishers (about 20k msg/s), a pool of 20, batching on at size `50` and timeout `200`ms, and
no payload padding.

The anchor batches because the single-row write path cannot sustain 20k msg/s. Rows queue ahead
of the database, and end-to-end latency climbs for as long as the run lasts. The measured
percentiles then depend on `RUN_DURATION` rather than on the runtime under test. The `Batching`
group keeps one `BATCH_ENABLED=false` run as the reference point that shows this.

Seven files are identical in configuration to the anchor: `load_p20`, `pool_20`, `payload_0`,
`batchsize_050`, `batchto_0200`, `batching_on` and `reads_0000`. That redundancy is deliberate. At
`REPS=3` a sweep measures the anchor 21 times. The spread across those runs is the noise floor,
and you must judge every other difference in the report against it.

| Group | File pattern | Factor swept | Values (**bold** = anchor) |
|-------|--------------|--------------|----------------------------|
| Load | `load_p{NN}.env` | `PUBLISHER_COUNT` | 4, 8, 16, **20**, 32, 48 |
| Pool | `pool_{NN}.env` | `DB_POOL_SIZE` | 5, 10, **20**, 50 |
| Payload | `payload_{N}.env` | `PAYLOAD_PADDING_BYTES` | **0**, 256, 1KB, 10KB |
| Batch size | `batchsize_{NNN}.env` | `BATCH_SIZE` | 20, **50**, 100, 200, 500 |
| Batch timeout | `batchto_{NNNN}.env` | `BATCH_TIMEOUT_MS` | 50, **200**, 500, 1000 |
| Batching | `batching_{off,on}.env` | `BATCH_ENABLED` | off, **on** |
| DB reads | `reads_{NNNN}.env` | `READS_PER_SEC` | **0**, 20, 50, 80, 100 |

The two batch factors are not independent. A buffer flushes on whichever trigger fires first, so
the effective batch size is roughly `min(BATCH_SIZE, per-writer rate × BATCH_TIMEOUT_MS)`. The
dispatcher routes rows round-robin across `DB_POOL_SIZE` writers. At the anchor a buffer fills
well before the timeout expires, so `BATCH_SIZE` is the binding trigger under load. The timeout
guards latency in low-rate periods. A sweep of the timeout below the fill time therefore
does not measure the timeout. It shrinks the effective batch instead.

Where the timeout is the shorter of the two, as in `batchto_0050` at the anchor, the timer wins
every cycle instead of racing the buffer. The writer arms the timer on the first row of each new
buffer and does not let it free-run. The flush period is therefore fixed, and the resulting tail
is tighter than a size-triggered tail, not noisier.

The read group is artificial load, by design. `READ_POOL_SIZE` reader actors inside the
subscriber each run one query at a time. The query is `SELECT avg(Value), count(*) FROM Data
WHERE Timestamp > now() - interval '5 seconds'`, with fixed text that is byte-identical in both
arms. The point is concurrent query pressure on the database and on the schedulers of the
runtime, not a realistic query mix.

The readers live inside the subscriber and not in a separate container. A separate container
contends for the database but not for the schedulers or dispatchers of either runtime. That
contention is the only part that can distinguish the two arms.

A reader arms its next read only after the previous read returns. At most one query per reader is
ever in flight, so `READS_PER_SEC` can never build a backlog. The reader computes the delay
against a fixed deadline. That deadline advances by exactly one period per cycle, and not by the
period minus the query time. Period-minus-query-time leaves each cycle carrying whatever the runtime
spends outside the measured read. That overhead differed enough between the two arms that they
ran measurably different read loads at the same setting. Deadline pacing absorbs a constant
lateness in full.

The target is a ceiling and not a guarantee. Report the achieved rate from
`subscriber_reads_total`, never the configured one. At any setting, a target that the readers
cannot reach appears as an achieved rate below the target and not as an error.

Reads also change the connection budget. Total database connections are `DB_POOL_SIZE +
READ_POOL_SIZE` while reads are on, and `DB_POOL_SIZE` when `READS_PER_SEC=0`. Both arms behave
identically here. This README documents `READ_POOL_SIZE`, but no scenario sweeps it. A fixed
`READ_POOL_SIZE` keeps the connection count of the group constant and independent of
`PUBLISHER_COUNT`, so a moving connection count cannot confound the read curve.

The `Batching` group holds the comparison between no batching and batching: `batching_off.env`
against `batching_on.env`, and the second is identical to the anchor.

### Scenario variables reference

| Variable | Default | Description |
|----------|---------|-------------|
| `PUBLISHER_COUNT` | `1` | Number of publisher containers (`--scale publisher=N`) |
| `DB_BACKEND` | `timescaledb` | Which database to run against. Set it on the command line and not in a scenario file. It selects the `docker-compose.<backend>.yaml` fragment |
| `DB_HOST` | `timescaledb` | Database host (`mysql`, `influxdb` or `mongodb` under those backends). The compose fragment of the backend supplies it, and both arms read the same key |
| `DB_PORT` | `5432` | Database port (`3306` MySQL, `8086` InfluxDB, `27017` MongoDB). The compose fragment of the backend supplies it, and both arms read the same key |
| `DB_NAME` | `epu` | Database name, and the bucket name under `DB_BACKEND=influxdb`. The compose fragment of the backend supplies it, and both arms read the same key |
| `DB_USER` / `DB_PASSWORD` | `postgres` | Database credentials (`root` / `mysql` under MySQL, and `root` / `mongo` under MongoDB, which authenticates against its `admin` database). The compose fragment supplies them. Under `DB_BACKEND=influxdb` they seed the initial user only, because the API authenticates with `DB_TOKEN`, and InfluxDB enforces a minimum password length of 8 characters |
| `DB_ORG` / `DB_TOKEN` | `epu` / `epu-benchmark-token` | InfluxDB only: the organization and the API token. These are the two keys beyond the five that every backend reads. The compose fragment supplies them, and both arms read them identically |
| `DB_PATH` / `DB_INIT_DIR` | `/var/lib/sqlite/epu.db` / `/sqlite/init` | SQLite only: the database file on the `sqlite-storage` volume, and the mounted directory that holds `init.sql` and `connection.sql`. This backend does not use the five network keys above. The compose fragment supplies them, and both arms read them identically |
| `DB_POOL_SIZE` | `20` | Number of database pool workers, and the total connection count, because each worker holds one connection. Status writes round-robin over the same pool. Under `DB_BACKEND=influxdb` it caps concurrent HTTP requests instead of counting pooled connections, so the connection count is an upper bound only. Under `DB_BACKEND=sqlite` it is the number of connections that queue on one write lock. Under `DB_BACKEND=mongodb` each connection carries several pipelined writes at once |
| `BATCH_ENABLED` | `false` | Enable row buffering |
| `BATCH_SIZE` | `100` | Flush when the buffer reaches this many rows |
| `BATCH_TIMEOUT_MS` | `1000` | Flush after this many milliseconds, even if the buffer is not full |
| `PAYLOAD_PADDING_BYTES` | `0` | Extra filler bytes, added as a trailing `"padding"` field on top of the shared base payload (identical in both arms), for payload-size benchmarks |
| `READS_PER_SEC` | `0` | Aggregate target read rate across all readers. `0` starts no readers at all: no actors, no connections, no timers. The achieved rate falls below this target once the target exceeds what the readers can sustain |
| `READ_POOL_SIZE` | `4` | Reader actors. Each holds one database connection, so total connections are `DB_POOL_SIZE + READ_POOL_SIZE` while reads are on. Documented but not swept |
| `RUN_DURATION` | _(unset)_ | Fixed measurement window in seconds. Set it, or use `bench.sh all`, which defaults it to `90`, for an unattended run. Leave it unset for an interactive run that you stop with `Ctrl+C` |
| `WARMUP_SECONDS` | `30` | Startup transient excluded from every reported figure. It must be at least the 15s rate window, or the first samples kept still carry the ramp |
| `STARTUP_TIMEOUT` | `180` | Seconds a rep can take to serve a scrapeable metrics endpoint before `bench.sh` skips it |
| `INGEST_TIMEOUT` | `60` | Seconds a rep can take to start ingesting, once its endpoint answers, before `bench.sh` skips it |
| `REPS` | `1` | Times `bench.sh` repeats each scenario, with a full teardown between reps (`bench.sh all` defaults it to `3`). The report collapses the reps to a mean and a standard deviation, and pools their histogram buckets for latency |
| `METRICS_INTERVAL` | `5` / `10` | Seconds between metric snapshots (`5` in duration mode, `10` interactive) |

### Supported `DB_BACKEND` values

| Value | Modules | Description |
|-------|---------|-------------|
| `timescaledb` (default) | `db_backend_timescaledb`, `db_read_backend_timescaledb` | PostgreSQL and TimescaleDB through epgsql, with async writes |
| `mysql` | `db_backend_mysql`, `db_read_backend_mysql` | MySQL 8.4 through mysql-otp. The driver is synchronous, so the backend hands each write to a short-lived spawned process, which messages the ack back. The backend still satisfies the `{async, State}` contract of the behaviour |
| `influxdb` | `db_backend_influxdb`, `db_read_backend_influxdb` | InfluxDB 2.7 over its HTTP API through `httpc`, with no driver dependency. The backend posts line protocol asynchronously, so the ack reaches the mailbox of the worker. It satisfies the `{async, State}` contract of the behaviour without a helper process |
| `sqlite` | `db_backend_sqlite`, `db_read_backend_sqlite`, `db_backend_sqlite_lock` | SQLite embedded through the esqlite NIF (a natively implemented function), so there is no database container. Two processes cannot share an esqlite handle, so each write runs inline in the pool worker and returns the `{sync, ...}` form of the behaviour. Every write holds one node-wide write lock |
| `mongodb` | `db_backend_mongodb`, `db_read_backend_mongodb` | MongoDB 8.0 through the `mc_worker_api` of mongodb-erlang. The calls of the driver block, so the backend hands each write to a short-lived spawned process, which messages the ack back, as with `mysql`. Every write waits for the journal (`j: true`), and a batch is one `insert` command |

Each SQL value needs its own `<backend>/init/init.sql`, written in the dialect of that database,
so the two schemas are equivalent rather than identical.

`influxdb` has no schema file. It is schema-on-write, the image creates the bucket, and the
line-protocol builders of the backend decide what the columns are. No file therefore pins the
schema across the two arms. A missing `precision=us` on the write URL then fails silently and
lands every point in 1970.

`sqlite` has no server to run a schema, so `sqlite/init/` holds `init.sql` plus a
`connection.sql` for the settings that SQLite keeps per connection. The subscriber applies both
every time it opens a connection. `mongodb` has `mongodb/init/init.js`, which the image runs to
create a time-series `Data` collection and a `sensor_status` collection whose validator stands in
for the SQL `CHECK`.

Adding a backend is three things in this repo. The first two are the modules and their one-line
registrations. The third is a `docker-compose.<backend>.yaml` fragment that carries the service,
the volume and the connection variables of that database. For SQLite the fragment carries only a volume and two variables on
the subscriber. Add a schema as well if that database needs one. Nothing in `benchmarking/`
changes, because the scenario files do not depend on the backend.

### What each backend changes

#### MySQL cannot pipeline

One connection carries one query at a time, so in-flight writes are capped at `DB_POOL_SIZE`,
where epgsql can hold several per connection. With `BATCH_ENABLED=false` that cap binds at the
swept load, and the subscriber builds an unbounded backlog. With batching on it keeps up. The
`pool_*` and `batching_*` results are therefore not comparable across databases.

#### InfluxDB speaks HTTP

HTTP changes three things. `DB_POOL_SIZE` caps concurrent requests instead of counting
connections. A write is an upsert keyed by timestamp instead of an append. One Flux read costs
far more than its SQL equivalent, so the `reads_*` scenarios are already at their ceiling by
`reads_0050`.

HTTP also connects lazily, so both backends probe for readiness at startup. Without that probe a
rep can ingest while it commits nothing. The `pool_*`, `batching_*` and `reads_*` numbers of this
backend are not comparable with the SQL ones.

#### SQLite runs inside the subscriber

SQLite allows one writer for the whole database, so every write takes an in-process lock first.
`pool_*` therefore measures how many writers queue on that lock, rather than how many write at
once. Every commit fsyncs, so `batching_off` builds a backlog, and `batchsize_020` and the
higher-load `load_*` scenarios are expected to build one as well.

esqlite runs on the dirty IO schedulers that the readers share with the writers, so `reads_*`
latency can include a wait for one of them. No read crosses a network. The CPU and memory of
the subscriber include the database, and no database container starts.

#### MongoDB stores readings in a time-series collection

The time field of the collection is a BSON Date, so stored timestamps keep milliseconds and not
microseconds. The latency metrics come from the payload, so this does not affect them. Every
write waits for the journal, so `batching_off` builds a backlog at the swept load.
`batchsize_020` and the higher-load `load_*` scenarios can build one as well. The read window
scans every bucket, and buckets accumulate as the backend writes rows, so `reads_*` latency can
grow with run length.

The drivers also differ between the arms. The driver of this arm pipelines several writes on each
connection, while the Java driver sends one at a time. The `pool_*` and `batching_off` numbers of
this backend therefore compare the drivers as much as the runtimes.

## Deep Dive: Erlang Concurrency

### Lightweight Processes against OS Threads

This project uses the concurrency model of Erlang, which builds on lightweight processes instead
of OS threads.

#### Erlang processes

The Erlang Runtime System (ERTS) manages Erlang processes, and the operating system does not.

- Each process starts with a few kilobytes of memory, so one node can run millions of processes at the same time.
- The Erlang scheduler is preemptive. No single process can hold the CPU, and every process gets a fair share of execution time.
- The model is efficient for large-scale concurrency and for fault-tolerant systems.
- The model needs a different way of thinking. Processes pass messages, and they do not share state.

#### Fault tolerance, or let it crash

Erlang implements the "let it crash" philosophy through supervisors and process monitoring. A
process does not defend itself with complex try-catch blocks. It fails fast, and a supervisor
restarts it into a known good state. A local failure therefore does not bring down the whole
application.

This implementation uses GenServers to hold state and logic. The supervisor tree in
`service_subscriber_sup.erl` manages them.

## Tech Stack

- Language: Erlang/OTP
- Concurrency: GenServers and lightweight processes
- Messaging: MQTT, through Mosquitto and emqtt
- JSON: the built-in json module (OTP 27 and later)
- Observability: Prometheus and Grafana
- Database: pluggable backends, which are TimescaleDB (default), MySQL, InfluxDB, SQLite (embedded) and MongoDB
- Deployment: Docker and Docker Compose

## License

MIT. See [`LICENSE`](LICENSE).
