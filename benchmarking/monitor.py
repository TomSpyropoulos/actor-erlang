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

# Named PromQL queries mirroring the panels in grafana/example/container_monitoring.json,
# paired with the one label each panel's Grafana legend keys its series by (`{{name}}`,
# `P{{quantile}}`, or a fixed legend for label-less scalar results -> None). Mirroring the
# legend, not just the raw label set, is what keeps each series identifiable without
# dragging along cAdvisor's ~30 incidental docker-compose labels. This list is the single
# source of truth for what gets watched, displayed, and saved — update it here if the
# dashboard's panels ever change. Grafana's `$__rate_interval` is replaced with a fixed
# `15s` rate window: at Prometheus' 2s scrape interval that spans ~7 samples (stable) yet
# reaches a full window in 15s, so throughput/CPU settle far sooner than a `1m` window's 60s.
PANELS = [
    ("container_cpu_usage", 'rate(container_cpu_usage_seconds_total{name!=""}[15s])', "name"),
    ("container_memory_usage_mb", 'rate(container_memory_usage_bytes{name!=""}[15s]) / 1024 / 1024', "name"),
    ("messages_per_second", "sum(rate(subscriber_requests_total[15s]))", None),
    ("publisher_subscriber_latency_ms", "subscriber_request_latency_milliseconds", "quantile"),
    ("publisher_db_e2e_latency_ms", "subscriber_e2e_latency_milliseconds", "quantile"),
    ("sensors_alive", "sum(subscriber_sensor_up)", None),
    ("sensors_missing", "count(subscriber_sensor_up) - sum(subscriber_sensor_up)", None),
    ("db_write_latency_ms", "subscriber_db_write_latency_milliseconds", "quantile"),
]


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


def compute_aggregates(samples):
    """Compute avg/max across the run for every distinct panel+series combination, skipping
    NaN samples (Prometheus summaries report NaN before their first observation, e.g. during
    pipeline warm-up — including them would poison sum()/max() for the whole run with NaN)."""
    series_values = defaultdict(list)
    # Group every sample's non-NaN numeric values by which panel and label-series they belong to.
    for sample in samples:
        for panel_name, series in sample["panels"].items():
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
    """Build a fresh table of the latest sample's headline values, mirroring the Grafana dashboard."""
    table = Table(title="Live benchmark metrics")
    for column in ("time", "msgs/s", "req p50", "req p99", "e2e p50", "e2e p99",
                   "db p50", "db p99", "sensors up/down", "cpu (cores)", "mem (MB)"):
        table.add_column(column, justify="right")

    if sample is None:
        return table

    panels = sample["panels"]

    # Render a numeric value with an optional unit suffix, or "?" while data is missing —
    # either the series doesn't exist yet (None) or it's a summary with no observations
    # yet (NaN, e.g. before the first DB write completes during pipeline warm-up).
    def fmt(value, suffix=""):
        return "?" if value is None or math.isnan(value) else f"{value:.1f}{suffix}"

    table.add_row(
        datetime.fromisoformat(sample["timestamp"]).strftime("%H:%M:%S"),
        fmt(_series_value(panels, "messages_per_second", "value")),
        fmt(_series_value(panels, "publisher_subscriber_latency_ms", 'quantile="0.5"'), " ms"),
        fmt(_series_value(panels, "publisher_subscriber_latency_ms", 'quantile="0.99"'), " ms"),
        fmt(_series_value(panels, "publisher_db_e2e_latency_ms", 'quantile="0.5"'), " ms"),
        fmt(_series_value(panels, "publisher_db_e2e_latency_ms", 'quantile="0.99"'), " ms"),
        fmt(_series_value(panels, "db_write_latency_ms", 'quantile="0.5"'), " ms"),
        fmt(_series_value(panels, "db_write_latency_ms", 'quantile="0.99"'), " ms"),
        f"{fmt(_series_value(panels, 'sensors_alive', 'value'))} / {fmt(_series_value(panels, 'sensors_missing', 'value'))}",
        fmt(_series_sum(panels, "container_cpu_usage")),
        fmt(_series_sum(panels, "container_memory_usage_mb")),
    )
    return table


def render_view(sample, started_at):
    """Combine the running elapsed-time line and the latest metrics table for the live display."""
    elapsed = format_elapsed((datetime.now(timezone.utc) - started_at).total_seconds())
    return Group(Text(f"Elapsed: {elapsed}", style="bold cyan"), render_table(sample))


def write_output(output_dir, scenario_name, started_at, interval, samples, aggregates):
    """Serialize the run's raw samples and aggregates to a timestamped JSON file under output_dir."""
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = Path(scenario_name).stem
    path = output_dir / f"{stem}_{started_at.strftime('%Y%m%d-%H%M%S')}.json"
    payload = {
        "scenario": scenario_name,
        "started_at": started_at.isoformat(),
        "interval_seconds": interval,
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
    args = parser.parse_args()

    started_at = datetime.now(timezone.utc)
    samples = []

    if args.duration is not None:
        console.print(f"Polling {args.prometheus_url} every {args.interval}s for {args.duration}s.\n")
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
        aggregates = compute_aggregates(samples)
        console.print()
        print_summary(aggregates)
        path = write_output(args.output_dir, args.scenario_name, started_at, args.interval, samples, aggregates)
        console.print(f"\nSaved {len(samples)} samples to [bold]{path}[/]")


if __name__ == "__main__":
    main()
