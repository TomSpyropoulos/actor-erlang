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
- **Database Connection Pool (`service_subscriber_db`)**: A pool of **20** parallel processes holding independent connections to TimescaleDB. Writes are dispatched asynchronously via `gen_server:cast` to the pool using a hash of the `DeviceName` to guarantee chronological write order per sensor. The database workers then execute these writes in a **fully asynchronous, non-blocking manner using `epgsqla:prepared_query/3`** with SQL queries prepared once at startup. This enables query pipelining and guarantees that DB worker processes never block on database network socket I/O.
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
- State changes are logged via `logger` and persisted to the `sensor_status` table in TimescaleDB, but only when the status actually changes to avoid unnecessary writes.

## 📡 MQTT Quality of Service

The subscriber connects with **QoS 0 (At Most Once)**. This provides fire-and-forget delivery with no acknowledgment overhead, prioritizing throughput and low latency over guaranteed delivery.

## 📝 Performance & Bottlenecks Status

1. **[TODO] Quantile Summary Metrics Overhead**
   `prometheus_quantile_summary:observe/2` uses a streaming algorithm backed by ETS. Under high concurrency from many workers, it remains a potential lock-contention point that could be replaced with a `prometheus_histogram` in the future.

2. **[TODO] Write Batching**
   Each message currently triggers an individual `INSERT`. Buffering rows and flushing as a multi-row `INSERT ... VALUES (...), (...), ...` every N rows or T milliseconds would significantly increase DB throughput.

3. **[TODO] Investigate Ingestion**
   Right now the backend can handle 10k requests per second but unexpectedly it needs 20 publishers instead of 10, as it seems to cap at half the messages is should be processing (10 subscribers should produce 10k messages/sec but the subscriber stops at 5).
   Also latencies are too low? p999 for 20 publishers is about 5ms. Is this correct? Are latencies reported correctly in prometheus?
