# Service Publisher

The Service Publisher is an Erlang application that simulates an IoT sensor. It generates the data
for the pipeline: a steady stream of sensor readings, published to the message broker.

## Core Functionality

1. **Simulation of an IoT device.**
   When a publisher instance starts, it derives a sensor identifier from the hostname of its
   container. It then produces telemetry the way a hardware sensor in the field does.

2. **Data generation.**
   The service generates one random reading every millisecond, which is 1000 readings per second
   per instance. A wall-clock timer fires that tick, and it fires whether or not the previous
   publish has finished. The payload is the shared wire format of the benchmark. It is
   byte-for-byte identical to what the Scala arm publishes for the same reading.
   `PAYLOAD_PADDING_BYTES` therefore adds its filler to the same base payload in both arms:

   ```json
   {"device_name":"sensor<hostname>","timestamp":"2026-08-21T12:34:56.123456Z","value":7}
   ```

   - `device_name`: The identifier of the sensor, derived from the container hostname, for example `sensorabc123def4`.
   - `timestamp`: RFC 3339 in UTC with exactly six fractional digits, read from `os:system_time(microsecond)`, the same clock the subscriber stamps arrival with. Both the resolution and the fixed width matter. A coarser send stamp biases the latency, and a variable-width fraction changes the payload size from message to message.
   - `value`: A random integer from 1 to 10, published as a JSON number and not as a quoted string.
   - `padding`: Present only when `PAYLOAD_PADDING_BYTES` is above 0. It is the letter `x` repeated that many times, and it is always the last field.

   The code assembles the JSON directly as an iolist rather than through `json:encode/1`. That
   function emits map keys in Erlang term order and places `padding` before `timestamp`.

3. **MQTT publishing.**
   The publisher sends each JSON payload to the Mosquitto MQTT broker. It publishes to a topic of
   its own device, such as `sensors/sensorabc123def4`. A subscriber can therefore filter or
   aggregate topics through a wildcard subscription such as `sensors/#`.

## Architecture

The publisher is one OTP GenServer. At initialization it connects to the MQTT broker through the
`emqtt` library and starts the periodic `publish_tick` timer. When that timer fires, the
`handle_info` callback generates the random value, builds the JSON payload and publishes it over
MQTT.

## Scaling

The publisher derives its `device_name` from the system hostname, so it holds no state of its own
and scales by replication. Simulate a fleet of sensors by scaling the service with Docker Compose:

```bash
docker compose up -d --scale publisher=100
```

That starts 100 isolated sensor instances, all feeding the pipeline at the same time.
