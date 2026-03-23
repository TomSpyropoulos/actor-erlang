# Actor Erlang: Containarized IoT Data Pipeline

This project consists of a containarized data pipeline which uses docker compose do setup a data pipeline using an actor for each "sensor".
The subscriber subscribes to the wildcard topic sensor, and for each different topic (sensor) in the wildcard, it produces a new actor that keeps the last state of the sensor that was written on the queue.

## Usage
Run:
```bash 
docker compose up -d --build --scale publisher=3
```
to run the service with 3 simulated sensors.

Grafana is accessible at `localhost:3000` and is preprovisioned with Prometheus as a data source and an example dashboard containing CPU and RAM usage of each container

To see the latest timestamp for each sensor and the total sum of the published messages run:
```bash
docker logs subscriber
```
