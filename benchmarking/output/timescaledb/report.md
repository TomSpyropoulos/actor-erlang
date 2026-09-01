# Benchmark report

Source: `output/timescaledb` — 4 runs across 3 scenarios.

Latencies in ms, `msgs_s` (ingested) and `committed_s` (DB-committed) in msg/s, `cpu_cores` in cores, `mem_mb` in MB.

All figures cover **steady state only** — the first 30s of each run is excluded. Throughput and CPU therefore read higher than in pre-trim reports, which averaged the startup ramp in; that is a definition change, not an improvement.

Throughput/resource cells are mean ± stdev across reps. Latency cells are a **pooled quantile**: the reps' steady-state histogram buckets are summed and one quantile taken over the total, with the per-rep min–max in parentheses where the reps disagreed by more than 5%. A `mean` column accompanies each stage — it is exact, unquantized and never clamped, so it stays readable where a quantile lands in `+Inf`.

## Batching

| value | reps | msgs_s | committed_s | reads_s | cpu_cores | mem_mb | req_p50 | req_p95 | req_p99 | req_p999 | e2e_p50 | e2e_p95 | e2e_p99 | e2e_p999 | db_p50 | db_p95 | db_p99 | db_p999 | read_p50 | read_p95 | read_p99 | read_p999 | req_mean | e2e_mean | db_mean | read_mean |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| off | 1 | 20000.2 ± 0.0 | 3278.1 ± 0.0 | - | 6.5 ± 0.0 | 4321.4 ± 0.0 | 0.5 | 1.2 | 1.7 | 4.7 | 47284.4 | 60000.0 | 60000.0 | 60000.0 | 47284.1 | 60000.0 | 60000.0 | 60000.0 | - | - | - | - | 0.6 | 43947.3 | 43946.7 | - |

## Load

| value | reps | msgs_s | committed_s | reads_s | cpu_cores | mem_mb | req_p50 | req_p95 | req_p99 | req_p999 | e2e_p50 | e2e_p95 | e2e_p99 | e2e_p999 | db_p50 | db_p95 | db_p99 | db_p999 | read_p50 | read_p95 | read_p99 | read_p999 | req_mean | e2e_mean | db_mean | read_mean |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| p20 | 1 | 20000.0 ± 0.0 | 20000.0 ± 0.0 | 0.0 ± 0.0 | 4.9 ± 0.0 | 3124.6 ± 0.0 | 0.5 | 1.0 | 1.4 | 2.5 | 28.1 | 54.5 | 70.9 | 74.6 | 27.6 | 50.3 | 70.1 | 74.5 | - | - | - | - | 0.5 | 28.1 | 27.5 | - |

## DB reads

| value | reps | msgs_s | committed_s | reads_s | cpu_cores | mem_mb | req_p50 | req_p95 | req_p99 | req_p999 | e2e_p50 | e2e_p95 | e2e_p99 | e2e_p999 | db_p50 | db_p95 | db_p99 | db_p999 | read_p50 | read_p95 | read_p99 | read_p999 | req_mean | e2e_mean | db_mean | read_mean |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0020 | 2 (1 hist) | 20000.0 ± 0.3 | 20000.0 ± 0.0 | 20.0 ± 0.0 | 5.6 ± 0.1 | 3214.8 ± 93.0 | 0.5 | 1.0 | 1.5 | 6.2 | 29.1 | 59.6 | 72.0 | 74.7 | 28.6 | 57.1 | 71.4 | 74.7 | 27.0 | 38.9 | 45.0 | 49.5 | 0.5 | 29.1 | 28.5 | 28.7 |

## Startup

`gate_startup_s` is the wait for the subscriber's metrics endpoint, `gate_ingest_s` the further wait until the first message was ingested — together, the cost of getting the runtime to the point where it is measurable at all. `ttss_s` is seconds from first message until throughput settles within 5% of its steady mean.

| scenario | gate_startup_s | gate_ingest_s | ttss_s | reps over warm-up | warmup_e2e_p99 |
|---|---|---|---|---|---|
| load_p20.env | 0.0 | 0.0 | 15.0 | 0 | 112.4 |
| timescale_batching_off.env | 0.0 | 0.0 | 15.0 | 0 | 19774.9 |
| timescale_reads_0020.env | 0.0 | 0.0 | 20.0 | 0 | 107.2 |

## Provenance

| key | value |
|---|---|
| warm-up trim | 30 s |
| run duration | 91 s |
| sample interval | 5 s |
| histogram buckets | 39 finite, fingerprint `af67de93` |
| reps included | 4 |
| reps contributing histogram buckets | 3 |
| reps excluded (counter reset) | 0 |
| reps not settled by trim boundary | 0 |
| latency estimator | pooled histogram quantiles |
