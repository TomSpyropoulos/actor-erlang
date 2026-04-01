# IoT Data Pipeline (Erlang/OTP)

A high-performance, containerized IoT data pipeline implemented using **Erlang/OTP** and **GenServers**. This project demonstrates how to build a scalable messaging system that handles real-time sensor data using the actor model's lightweight processes and fault-tolerant principles.

## 🏗️ System Architecture

The system consists of the following components:

1.  **Service Publisher**: An Erlang application that simulates IoT sensors. Each instance generates sensor readings (JSON) and publishes them to an MQTT broker. See [publisher.md](publisher.md) for more details.
2.  **Mosquitto MQTT Broker**: Acts as the central messaging hub, facilitating communication between publishers and subscribers.
3.  **Service Subscriber**: An Erlang application that consumes messages from the `sensors/#` wildcard topic. It dynamically creates a dedicated **Worker Actor** (GenServer) for each unique sensor topic to maintain state (running sum and last timestamp). See [subscriber.md](subscriber.md) for more details.
4.  **Prometheus**: Scrapes metrics from the containers and the host system.
5.  **Grafana**: Provides a visual dashboard for monitoring container resource usage (CPU/RAM) and application latency.

## ✨ Recent Changes

*   **Latency Metrics Update:** Switched the latency tracking from a standard `Histogram` to a `Quantile Summary` using `prometheus_quantile_summary` from `prometheus.erl`.
*   **Accurate Quantiles:** The Prometheus client now natively tracks and reports accurate P50, P95, P99, and P999 quantiles directly instead of relying on server-side `histogram_quantile` estimations.
*   **Native Time Unit Fix:** Fixed a bug where latency values were unnaturally small by correctly converting the latency to Erlang's *native time unit*. The `prometheus.erl` client expects native time when a metric name ends in a time unit suffix (e.g., `_milliseconds`), automatically handling the final conversion.
*   **Dashboard Updates:** The Grafana container monitoring dashboard was updated to display the direct quantile outputs instead of the legacy bucket-based histogram queries.
*   **Documentation:** Added detailed documentation about the roles of the `publisher` and `subscriber` services.

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

- **Grafana**: Accessible at [http://localhost:3000](http://localhost:3000). 
    - *Credentials*: `admin` / `admin` (default).
    - Pre-provisioned with Prometheus and a "Container Monitoring" dashboard.
- **Subscriber Logs**: View the aggregated state for each sensor:
    ```bash
    docker logs -f subscriber
    ```

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
- **Messaging**: MQTT (via emqtt)
- **JSON**: jsx
- **Observability**: Prometheus & Grafana
- **Database**: TimescaleDB
- **Deployment**: Docker & Docker Compose
