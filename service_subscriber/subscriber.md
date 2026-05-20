# Service Subscriber

The **Service Subscriber** is the core processing component of the IoT data pipeline. It is an Erlang-based application responsible for ingesting, processing, and storing the real-time stream of telemetry data produced by the publishers.

## ⚙️ Core Functionality

1. **Wildcard Ingestion:**
   The Subscriber connects to the Mosquitto MQTT Broker and subscribes to the wildcard topic `sensors/#`. This allows a single subscriber instance to receive telemetry data from every publisher on the network.

2. **Dynamic Actor Creation:**
   Instead of processing all messages in a single bottleneck process, the Subscriber utilizes the Actor Model. For every unique sensor topic it receives a message from, it spawns a dedicated **Worker Actor** (a GenServer). This worker maintains the localized state for that specific sensor (e.g., the running sum of values, last seen timestamp).

3. **Data Transformation & Storage:**
   The worker actor parses the incoming JSON payload, extracts the `timestamp` and `value`, and transforms them into native Erlang types. It then persists this data into **TimescaleDB** (a PostgreSQL extension optimized for time-series data) for long-term storage and analytical querying.

4. **Metrics and Observability:**
   The worker calculates the end-to-end latency of the message by comparing the payload's original timestamp with the current system time. It records this latency, along with request counts, using the Erlang `prometheus.erl` library.

## 🏗️ Architecture

The application is built around an OTP **Supervision Tree** optimized for massive parallelism and zero blocking:

- **Root Supervisor (`service_subscriber_sup`)**: The core supervisor that monitors the metrics server, database connection pool, dynamic worker supervisor, and MQTT receiver.
- **MQTT Client (`service_subscriber_mqtt`)**: A GenServer managing the connection to the Mosquitto broker. It performs extremely fast, lock-free lookups in a shared ETS routing table to route payloads. If a sensor topic is new, it delegates startup asynchronously to avoid blocking the main MQTT loop.
- **Dynamic Worker Supervisor (`service_subscriber_worker_sup`)**: A supervisor managing the lifecycle of dynamically spawned sensor workers.
- **Worker Actors (`service_subscriber_worker`)**: Dynamically spawned processes handling JSON decoding, database insert calls, and metrics reporting. They register themselves in the shared ETS table upon initialization.
- **Database Connection Pool (`service_subscriber_db`)**: A pool of 5 parallel processes holding independent connections to TimescaleDB. Writes are dispatched asynchronously via `gen_server:cast` and routed using a hash of the `DeviceName` to guarantee chronological write order in the database for each sensor while running in parallel.
- **Metrics Server (`service_subscriber_metrics`)**: A centralized service for declaring and exposing Prometheus metrics.

## 📊 Metrics Tracking

The Subscriber heavily utilizes Prometheus for observability. 

One of the critical metrics tracked is the end-to-end latency. This is tracked using a `prometheus_quantile_summary`, which accurately calculates the P50, P95, P99, and P999 latency percentiles dynamically on the client-side. The library expects time measurements to be supplied in Erlang's native time unit (when the metric name ends in a time duration suffix like `_milliseconds`), automatically converting it to milliseconds for the Grafana dashboard.

## 🔍 Missing Sensor Detection

The Subscriber implements a heartbeat-based missing sensor detection system:

- Every **5 seconds**, the MQTT client broadcasts a `heartbeat` message to all active worker actors.
- The MQTT client uses a lock-free `ets:foldl/3` traversal over the shared worker routing table to send heartbeats, preventing any single process map traversal bottleneck.
- Each worker compares its `lastSeen` timestamp (captured using the subscriber's local clock) against the current time.
- If a sensor has not sent data within the last second, its status transitions to `MISSING`; otherwise it is `ALIVE`.
- State changes are logged (`io:format`) and persisted to the `sensor_status` table in TimescaleDB, but only when the status actually changes to avoid unnecessary writes.

## 📡 MQTT Quality of Service

The subscriber connects with **QoS 0 (At Most Once)**. This provides fire-and-forget delivery with no acknowledgment overhead, prioritizing throughput and low latency over guaranteed delivery.

## 📝 Performance & Bottlenecks Status

The architectural bottlenecks limiting subscriber performance have been addressed in the `perf_fix` branch:

1. **[SOLVED] Single Synchronous DB Connection**
   `service_subscriber_db` is now structured as a connection pool of 5 parallel connections. All writes are beautifully load-balanced across the pool.

2. **[SOLVED] Workers Block on DB Calls**
   Database insertions are now asynchronous via `gen_server:cast`. Sensor worker mailboxes never block waiting for disk/network I/O from TimescaleDB, allowing them to instantly digest subsequent MQTT packets.

3. **[SOLVED] Single MQTT Message Handler Blocking**
   Lookups use a lock-free named ETS table (`service_subscriber_workers`) with `{read_concurrency, true}`. New dynamic topic spawns are delegated asynchronously to `service_subscriber_worker_sup` via a spawned task. The `service_subscriber_mqtt` gateway process never blocks for a single millisecond.

4. **[SOLVED] Expensive Timestamp Parsing**
   Workers parse UTC RFC3339 timestamps using high-speed binary pattern-matching (`fast_parse_timestamp/1`) with zero string/list allocations and optimized gregorian calculations, dropping CPU usage and GC runs down to a fraction of before.

5. **[OPEN] Quantile Summary Metrics Overhead**
   `prometheus_quantile_summary:observe/2` uses a streaming algorithm backed by ETS. Under high concurrency from many workers, it remains a potential lock-contention point that could be replaced with a `prometheus_histogram` in the future.