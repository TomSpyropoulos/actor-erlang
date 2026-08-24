#!/usr/bin/env python3
"""Live benchmark monitor: mirrors the Grafana "Container Monitoring" dashboard by
querying Prometheus directly, shows a live table, and on Ctrl+C prints aggregate
stats and saves every raw sample to JSON for later analysis/plotting."""

import argparse
import json
import math
import time
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

from rich.console import Console, Group
from rich.live import Live
from rich.table import Table
from rich.text import Text

console = Console()

# The three latency stages, as (panel base name, Prometheus metric name). Each expands into five
# PANELS entries below. Both arms record these as histograms over one shared bucket list, so a
# quantile is computed server-side and can be recomputed over any window after the fact.
LATENCY_STAGES = [
    ("publisher_subscriber_latency_ms", "subscriber_request_latency_milliseconds"),
    ("publisher_db_e2e_latency_ms", "subscriber_e2e_latency_milliseconds"),
    ("db_write_latency_ms", "subscriber_db_write_latency_milliseconds"),
]
QUANTILES = (0.5, 0.95, 0.99, 0.999)
RATE_WINDOW = "15s"


def quantile_union(metric):
    """PromQL unioning the four histogram_quantile levels into one vector, each re-labelled with the
    `quantile` label the summary exposition used to carry natively. histogram_quantile returns an
    unlabelled vector, so without label_replace all four levels would collapse to one series key."""
    return " or ".join(
        f'label_replace(histogram_quantile({q}, sum by (le) '
        f'(rate({metric}_bucket[{RATE_WINDOW}]))), "quantile", "{q}", "", "")'
        for q in QUANTILES
    )


# Named PromQL queries mirroring the panels in grafana/example/container_monitoring.json,
# paired with the one label each panel's Grafana legend keys its series by (`{{name}}`,
# `P{{quantile}}`, or a fixed legend for label-less scalar results -> None). Mirroring the
# legend, not just the raw label set, is what keeps each series identifiable without
# dragging along cAdvisor's ~30 incidental docker-compose labels. This list is a *superset* of
# the dashboard: the `_buckets`/`_sum`/`_count` entries are collection-only and deliberately have
# no panel, existing to be re-sliced offline rather than looked at. It is still the single source
# of truth for what gets watched, displayed, and saved — update it here if the dashboard changes. In the rate-based panels Grafana's `$__rate_interval` is
# replaced with a fixed `15s` rate window: at Prometheus' 2s scrape interval that spans ~7
# samples (stable) yet reaches a full window in 15s, so throughput/CPU settle far sooner than
# a `1m` window's 60s. Memory is deliberately NOT one of them: it is an instant gauge read.
# container_memory_usage_bytes is a gauge, so rate() over it reports its per-second rate of
# change — noise around zero for a steady-state process, not footprint. working_set excludes
# inactive page cache, so the subscriber is not credited with TimescaleDB's file cache.
PANELS = [
    ("container_cpu_usage", 'rate(container_cpu_usage_seconds_total{name!=""}[15s])', "name"),
    ("container_memory_usage_mb", 'container_memory_working_set_bytes{name!=""} / 1024 / 1024', "name"),
    ("messages_per_second", "sum(rate(subscriber_requests_total[15s]))", None),
    ("committed_per_second", "sum(rate(subscriber_committed_total[15s]))", None),
    ("sensors_alive", "sum(subscriber_sensor_up)", None),
    ("sensors_missing", "count(subscriber_sensor_up) - sum(subscriber_sensor_up)", None),
]
for _panel, _metric in LATENCY_STAGES:
    PANELS += [
        (_panel, quantile_union(_metric), "quantile"),
        # Exact windowed mean: not quantized to a bucket width and never clamped, so it stays
        # readable in saturated scenarios where a quantile lands in +Inf and reads as the highest
        # finite bound. Free with the histogram, and a cross-check on every quantile beside it.
        (f"{_panel}_mean",
         f"rate({_metric}_sum[{RATE_WINDOW}]) / rate({_metric}_count[{RATE_WINDOW}])", None),
        # Collection-only: raw cumulative counters, NOT rates. report.py subtracts two snapshots for
        # the exact observations in a window, which is what makes the warm-up trim a reporting
        # parameter changeable offline rather than a choice frozen at collection time.
        (f"{_panel}_buckets", f"sum by (le) ({_metric}_bucket)", "le"),
        (f"{_panel}_sum", f"sum({_metric}_sum)", None),
        (f"{_panel}_count", f"sum({_metric}_count)", None),
    ]

# Cumulative-counter panels, excluded from aggregates: an avg/max over a monotonically growing
# counter is meaningless. report.py reads these out of `samples` as window deltas instead.
RAW_PANELS = {f"{panel}_{suffix}" for panel, _ in LATENCY_STAGES
              for suffix in ("buckets", "sum", "count")}


def query_prometheus(base_url, promql):
    """Run an instant PromQL query against Prometheus and return its result vector."""
    url = f"{base_url}/api/v1/query?{urlencode({'query': promql})}"
    with urllib.request.urlopen(url, timeout=10) as response:
        payload = json.load(response)
    if payload.get("status") != "success":
        raise RuntimeError(f"query failed: {payload}")
    return payload["data"]["result"]


def flatten_vector(result, label_key):
    """Turn one query's result vector into {series_key: numeric_value}, keyed the same way
    Grafana's legend distinguishes series (e.g. `name="subscriber"`, `quantile="0.99""`);
    label-less scalar results (legend is a fixed string in the dashboard) collapse to "value"."""
    flattened = {}
    for entry in result:
        key = "value" if label_key is None else f'{label_key}="{entry["metric"].get(label_key, "")}"'
        flattened[key] = float(entry["value"][1])
    return flattened


def collect_sample(base_url, started_at):
    """Run every panel query and assemble one timestamped record of flattened results."""
    now = datetime.now(timezone.utc)
    panels = {
        name: flatten_vector(query_prometheus(base_url, promql), label_key)
        for name, promql, label_key in PANELS
    }
    return {
        "timestamp": now.isoformat(),
        "elapsed_seconds": round((now - started_at).total_seconds()),
        "panels": panels,
    }


def poll_loop(base_url, interval, started_at):
    """Yield one collected sample every `interval` seconds until the caller stops iterating."""
    while True:
        time.sleep(interval)
        try:
            yield collect_sample(base_url, started_at)
        except Exception as exc:  # transient network/query hiccups shouldn't abort a long run
            console.print(f"[yellow]warning:[/] metrics query failed: {exc}")
            continue


def steady_state(samples, warmup_seconds):
    """Drop the warm-up samples. Throughput ramps ~11k -> ~18k -> 20k over the first ten seconds
    while publishers connect against a cold DB; averaging that in understates throughput and
    inflates every latency figure. `samples` is still stored whole so any run can be re-sliced."""
    return [s for s in samples if s["elapsed_seconds"] >= warmup_seconds]


def detect_counter_reset(samples):
    """True if any histogram observation counter went backwards. Cumulative counters only ever grow,
    so a decrease means the subscriber restarted mid-run — which makes the snapshot deltas report.py
    windows over invalid, so the rep must be excluded rather than quietly mis-reported."""
    for panel in (f"{base}_count" for base, _ in LATENCY_STAGES):
        previous = None
        for sample in samples:
            values = [v for v in sample["panels"].get(panel, {}).values() if not math.isnan(v)]
            if not values:
                continue
            total = sum(values)
            if previous is not None and total < previous:
                return True
            previous = total
    return False


def compute_aggregates(samples):
    """Compute avg/max over the given samples for every distinct panel+series combination, skipping
    NaN samples (a histogram quantile reads NaN before its first observation, e.g. during pipeline
    warm-up — including them would poison sum()/max() for the whole run with NaN). Callers pass
    steady-state samples only; RAW_PANELS are excluded as meaningless to average."""
    series_values = defaultdict(list)
    # Group every sample's non-NaN numeric values by which panel and label-series they belong to.
    for sample in samples:
        for panel_name, series in sample["panels"].items():
            if panel_name in RAW_PANELS:
                continue
            for series_label, value in series.items():
                if not math.isnan(value):
                    series_values[f"{panel_name} / {series_label}"].append(value)
    # Reduce each group's collected values down to its avg/max; series with no real
    # observations yet (still all-NaN) are omitted rather than reported as NaN/NaN.
    return {
        key: {"avg": sum(values) / len(values), "max": max(values)}
        for key, values in series_values.items() if values
    }


def format_elapsed(seconds):
    """Format a duration in seconds as HH:MM:SS for the live elapsed-time display."""
    hours, remainder = divmod(int(seconds), 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _series_value(panels, panel_name, series_label):
    """Look up a single labeled series' value within a panel's flattened results, or None if absent."""
    return panels.get(panel_name, {}).get(series_label)


def _series_sum(panels, panel_name):
    """Sum every labeled series of a panel (e.g. per-container CPU/memory) into one total."""
    series = panels.get(panel_name, {})
    return sum(series.values()) if series else None


def render_table(sample):
    """Build the live view: a throughput/resource table plus a latency matrix (stage x quantile),
    stacked vertically so no column has to be truncated to fit the terminal width."""
    # Render a numeric value with an optional unit suffix, or "?" while data is missing —
    # either the series doesn't exist yet (None) or it's a summary with no observations
    # yet (NaN, e.g. before the first DB write completes during pipeline warm-up).
    def fmt(value, suffix=""):
        return "?" if value is None or math.isnan(value) else f"{value:.1f}{suffix}"

    top = Table(title="Live benchmark metrics")
    for column in ("time", "msgs/s", "committed/s", "sensors up/down", "cpu (cores)", "mem (MB)"):
        top.add_column(column, justify="right")

    # Latencies as a matrix: one row per pipeline stage, one column per quantile, so all four
    # quantiles fit without the wide single-row layout that truncated cells to "...".
    lat = Table(title="Latencies (ms)")
    lat.add_column("stage", justify="left")
    for column in ("p50", "p95", "p99", "p999", "mean"):
        lat.add_column(column, justify="right")

    if sample is not None:
        panels = sample["panels"]
        top.add_row(
            datetime.fromisoformat(sample["timestamp"]).strftime("%H:%M:%S"),
            fmt(_series_value(panels, "messages_per_second", "value")),
            fmt(_series_value(panels, "committed_per_second", "value")),
            f"{fmt(_series_value(panels, 'sensors_alive', 'value'))} / {fmt(_series_value(panels, 'sensors_missing', 'value'))}",
            fmt(_series_sum(panels, "container_cpu_usage")),
            fmt(_series_sum(panels, "container_memory_usage_mb")),
        )
        for label, panel in (("req (pub->sub)", "publisher_subscriber_latency_ms"),
                             ("e2e (pub->db)", "publisher_db_e2e_latency_ms"),
                             ("db  (sub->db)", "db_write_latency_ms")):
            lat.add_row(
                label,
                fmt(_series_value(panels, panel, 'quantile="0.5"')),
                fmt(_series_value(panels, panel, 'quantile="0.95"')),
                fmt(_series_value(panels, panel, 'quantile="0.99"')),
                fmt(_series_value(panels, panel, 'quantile="0.999"')),
                fmt(_series_value(panels, f"{panel}_mean", "value")),
            )

    return Group(top, lat)


def render_view(sample, started_at):
    """Combine the running elapsed-time line and the latest metrics table for the live display."""
    elapsed = format_elapsed((datetime.now(timezone.utc) - started_at).total_seconds())
    return Group(Text(f"Elapsed: {elapsed}", style="bold cyan"), render_table(sample))


def write_output(output_dir, scenario_name, started_at, interval, samples, aggregates, rep=None,
                 warmup_seconds=0, gate_startup_seconds=None, gate_ingest_seconds=None,
                 counter_reset=False):
    """Serialize the run's raw samples and aggregates to a timestamped JSON file under output_dir.
    When rep is given (a repetition index), tag the filename and payload so the post-run report
    can group the reps of one scenario; without it the original single-run naming is preserved.
    The gate timings come from bench.sh: it waits for ingestion before launching this script, so
    without them the cost of a slow runtime boot would never appear in the data at all."""
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = Path(scenario_name).stem
    rep_tag = f"_rep{rep}" if rep is not None else ""
    path = output_dir / f"{stem}{rep_tag}_{started_at.strftime('%Y%m%d-%H%M%S')}.json"
    payload = {
        "scenario": scenario_name,
        "rep": rep,
        "started_at": started_at.isoformat(),
        "interval_seconds": interval,
        "warmup_seconds": warmup_seconds,
        "gate_startup_seconds": gate_startup_seconds,
        "gate_ingest_seconds": gate_ingest_seconds,
        "counter_reset": counter_reset,
        "samples": samples,
        "aggregates": aggregates,
    }
    path.write_text(json.dumps(payload, indent=2))
    return path


def print_summary(aggregates):
    """Pretty-print the avg/max of every tracked series once polling stops."""
    table = Table(title="Run summary (avg / max)")
    table.add_column("series")
    table.add_column("avg", justify="right")
    table.add_column("max", justify="right")
    for key in sorted(aggregates):
        stats = aggregates[key]
        table.add_row(key, f"{stats['avg']:.2f}", f"{stats['max']:.2f}")
    console.print(table)


def main():
    """Poll Prometheus and render a live view until Ctrl+C (or --duration seconds), then print and save."""
    parser = argparse.ArgumentParser(description="Live benchmark metrics monitor")
    parser.add_argument("--prometheus-url", required=True, help="Base URL of the Prometheus server")
    parser.add_argument("--interval", type=int, required=True, help="Seconds between polls")
    parser.add_argument("--scenario-name", required=True, help="Scenario file name, embedded in the output")
    parser.add_argument("--output-dir", required=True, type=Path, help="Directory JSON results are written to")
    parser.add_argument("--duration", type=int, default=None,
                        help="Run for N seconds then stop (default: run until Ctrl+C)")
    parser.add_argument("--rep", type=int, default=None,
                        help="Repetition index, tagged into the output filename/payload for grouping")
    parser.add_argument("--warmup", type=int, default=30,
                        help="Seconds of startup transient excluded from the aggregates (default: 30)")
    parser.add_argument("--gate-startup-seconds", type=int, default=None,
                        help="Seconds bench.sh waited for the metrics endpoint, recorded as startup cost")
    parser.add_argument("--gate-ingest-seconds", type=int, default=None,
                        help="Seconds bench.sh waited for ingestion to begin, recorded as startup cost")
    args = parser.parse_args()

    started_at = datetime.now(timezone.utc)
    samples = []

    if args.duration is not None:
        console.print(f"Polling {args.prometheus_url} every {args.interval}s for {args.duration}s "
                      f"(first {args.warmup}s treated as warm-up).\n")
    else:
        console.print(f"Polling {args.prometheus_url} every {args.interval}s. Press Ctrl+C to stop.\n")
    try:
        # Run the poll loop inside a live-updating view; it stops on Ctrl+C, or once
        # --duration seconds have elapsed when a fixed-duration run was requested.
        with Live(render_view(None, started_at), refresh_per_second=4, console=console) as live:
            for sample in poll_loop(args.prometheus_url, args.interval, started_at):
                samples.append(sample)
                live.update(render_view(sample, started_at))
                if args.duration is not None and sample["elapsed_seconds"] >= args.duration:
                    break
    except KeyboardInterrupt:
        pass  # user requested stop — fall through to summarizing and saving
    finally:
        # Always compute, print, and persist results — even on an unexpected exception —
        # so a long benchmark run's data isn't lost.
        # Aggregate over steady state only, but persist every sample: the trim is a reporting
        # parameter, so a run must stay re-sliceable at a different boundary without re-collecting.
        measured = steady_state(samples, args.warmup)
        if samples and not measured:
            console.print(f"[yellow]warning:[/] no samples past the {args.warmup}s warm-up; "
                          "aggregating the whole run instead")
            measured = samples
        counter_reset = detect_counter_reset(samples)
        if counter_reset:
            console.print("[red]warning:[/] observation counters went backwards — the subscriber "
                          "restarted mid-run; this rep will be excluded from the report")
        aggregates = compute_aggregates(measured)
        console.print()
        print_summary(aggregates)
        path = write_output(args.output_dir, args.scenario_name, started_at, args.interval, samples,
                            aggregates, args.rep, args.warmup, args.gate_startup_seconds,
                            args.gate_ingest_seconds, counter_reset)
        console.print(f"\nSaved {len(samples)} samples ({len(measured)} steady-state) to [bold]{path}[/]")


if __name__ == "__main__":
    main()
