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

The application is built around an OTP **Supervision Tree**:

- **Supervisor (`service_subscriber_sup`)**: The root supervisor that monitors the core services, restarting them if they crash (embracing Erlang's "Let It Crash" philosophy).
- **MQTT Client (`service_subscriber_mqtt`)**: A GenServer that manages the persistent connection to the Mosquitto broker. When a message arrives, it delegates the payload to the appropriate worker actor, spawning a new one if it's the first time seeing that sensor.
- **Worker Actors (`service_subscriber_worker`)**: Dynamically spawned processes that handle the actual decoding, database insertion, and metrics recording. They isolate state and processing logic per sensor.
- **Metrics Server (`service_subscriber_metrics`)**: A centralized service for declaring and exposing Prometheus metrics (like counters and quantile summaries). It exposes an HTTP endpoint (usually on port 8081) for Prometheus to scrape.

## 📊 Metrics Tracking

The Subscriber heavily utilizes Prometheus for observability. 

One of the critical metrics tracked is the end-to-end latency. This is tracked using a `prometheus_quantile_summary`, which accurately calculates the P50, P95, P99, and P999 latency percentiles dynamically on the client-side. The library expects time measurements to be supplied in Erlang's native time unit (when the metric name ends in a time duration suffix like `_milliseconds`), automatically converting it to milliseconds for the Grafana dashboard.