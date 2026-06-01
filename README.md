# IoT Data Pipeline (Erlang/OTP)

A high-performance, containerized IoT data pipeline implemented using **Erlang/OTP** and **GenServers**. This project demonstrates how to build a scalable messaging system that handles real-time sensor data using the actor model's lightweight processes and fault-tolerant principles.

## 🏗️ System Architecture

The system consists of the following components:

1.  **Service Publisher**: An Erlang application that simulates IoT sensors. Each instance generates sensor readings (JSON) and publishes them to an MQTT broker. See [service_publisher/publisher.md](service_publisher/publisher.md) for more details.
2.  **Mosquitto MQTT Broker**: Acts as the central messaging hub, facilitating communication between publishers and subscribers.
3.  **Service Subscriber**: An Erlang application that consumes messages from the `sensors/#` wildcard topic. It dynamically creates a dedicated **Worker Actor** (GenServer) for each unique sensor topic to maintain state (running sum and last timestamp) while tracking metrics. Features an ultra-fast lock-free ETS-based dynamic actor routing table and a parallel TimescaleDB connection pool with asynchronous writes for extreme throughput. See [service_subscriber/subscriber.md](service_subscriber/subscriber.md) for more details.
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
    - Query `subscriber_requests_total` for throughput.
    - Query `subscriber_request_latency_milliseconds` for p50/p95/p99/p999 latencies.
    - Query `subscriber_sensor_up` (per-device gauge, 1 = ALIVE, 0 = MISSING) for sensor liveness.
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
- **Messaging**: MQTT (Mosquitto/via emqtt)
- **JSON**: Built-in json (OTP 27+)
- **Observability**: Prometheus & Grafana
- **Database**: TimescaleDB
- **Deployment**: Docker & Docker Compose
