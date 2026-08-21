# Service Publisher

The **Service Publisher** is an Erlang-based application designed to simulate the behavior of IoT sensors. It acts as the primary data generator for the IoT data pipeline, producing a steady stream of sensor readings and dispatching them to the central messaging hub.

## ⚙️ Core Functionality

1. **Simulation of IoT Devices:** 
   When a Publisher instance starts, it generates a unique sensor identifier based on the container's hostname. It then continuously simulates telemetry data, behaving exactly like a hardware sensor deployed in the field.

2. **Data Generation:**
   The service generates a random data point **every 1 millisecond** (1000 readings/second per instance) using a wall-clock timer that fires regardless of how long each publish takes. The payload is the benchmark's **shared wire format** — byte-for-byte identical to what the Scala arm publishes for the same reading, so `PAYLOAD_PADDING_BYTES` is added to the same base payload in both:

   ```json
   {"device_name":"sensor<hostname>","timestamp":"2026-08-21T12:34:56.123456Z","value":7}
   ```

   - `device_name`: The unique identifier of the sensor, derived from the container hostname (e.g., `sensorabc123def4`).
   - `timestamp`: RFC 3339 UTC with **exactly six fractional digits**, taken from `os:system_time(microsecond)` — the same clock the subscriber stamps arrival with. Both the resolution and the fixed width matter: measuring against a coarser send stamp biases the latency, and a variable-width fraction would vary the payload size.
   - `value`: A random integer in 1–10, published as a **JSON number** — not a quoted string.
   - `padding`: Present only when `PAYLOAD_PADDING_BYTES > 0`; the letter `x` repeated that many times, always the last field.

   The JSON is assembled directly as an iolist rather than through `json:encode/1`, which emits map keys in Erlang term order and would place `padding` *before* `timestamp`.

3. **MQTT Publishing:**
   The generated JSON payload is published to the central **Mosquitto MQTT Broker**. It publishes to a topic specific to the device (e.g., `sensors/sensor-abcd1234`), allowing subscribers to filter or aggregate topics using wildcard subscriptions like `sensors/#`.

## 🏗️ Architecture

The Publisher utilizes Erlang/OTP's **GenServer** behavior. Upon initialization, it:
- Connects to the MQTT broker using the `emqtt` library.
- Starts a periodic timer (`publish_tick`).
- When the timer fires, the `handle_info` callback is invoked, which handles the generation of the random value, creates the JSON payload, and publishes it via MQTT.

## 🚀 Scaling

Because the publisher generates its `device_name` dynamically using the system hostname, it is completely stateless and inherently scalable. You can easily simulate a massive fleet of sensors by scaling the service via Docker Compose:

```bash
docker compose up -d --scale publisher=100
```

This will spin up 100 isolated sensor instances, all feeding data concurrently into the pipeline.