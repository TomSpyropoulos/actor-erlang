# Benchmark report

Source: `/home/tom/Code/diploma/actor-erlang/benchmarking/output` — 6 runs across 4 scenarios.

Latencies in ms, `msgs_s` in msg/s, `cpu_cores` in cores, `mem_mb` in MB. Cells are mean ± stdev across reps.

## Load

| value | reps | msgs_s | req_p50 | req_p99 | e2e_p50 | e2e_p99 | db_p50 | db_p99 | cpu_cores | mem_mb |
|---|---|---|---|---|---|---|---|---|---|---|
| p04 | 2 | 3078.3 ± 3.2 | 2.2 ± 0.0 | 4.2 ± 0.0 | 1718.5 ± 95.9 | 4101.7 ± 1120.9 | 1715.4 ± 99.1 | 4094.0 ± 1113.1 | 1.9 ± 0.1 | 316.9 ± 43.7 |
| p20 | 1 | 19461.0 ± 0.0 | 0.9 ± 0.0 | 2.7 ± 0.0 | 6448.6 ± 0.0 | 42893.7 ± 0.0 | 6448.6 ± 0.0 | 42893.7 ± 0.0 | 4.7 ± 0.0 | 685.8 ± 0.0 |

## Pool

| value | reps | msgs_s | req_p50 | req_p99 | e2e_p50 | e2e_p99 | db_p50 | db_p99 | cpu_cores | mem_mb |
|---|---|---|---|---|---|---|---|---|---|---|
| 05 | 2 | 16183.1 ± 6.3 | 1.0 ± 0.0 | 7.8 ± 3.1 | 2985.9 ± 146.9 | 13643.3 ± 521.6 | 2978.5 ± 139.6 | 13643.3 ± 521.6 | 4.2 ± 0.0 | 637.3 ± 11.7 |

## Timescale

| value | reps | msgs_s | req_p50 | req_p99 | e2e_p50 | e2e_p99 | db_p50 | db_p99 | cpu_cores | mem_mb |
|---|---|---|---|---|---|---|---|---|---|---|
| - | 1 | 982.6 ± 0.0 | 1.5 ± 0.0 | 3.8 ± 0.0 | 2333.6 ± 0.0 | 6343.5 ± 0.0 | 2333.6 ± 0.0 | 6343.5 ± 0.0 | 0.7 ± 0.0 | 93.5 ± 0.0 |
