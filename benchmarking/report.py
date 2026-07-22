#!/usr/bin/env python3
"""Post-run report: aggregate every per-rep benchmark JSON under output/ into a CSV (one row
per scenario) and a Markdown report (one headed table per OFAT group). Reps of a scenario are
collapsed to mean ± stdev per metric so run-to-run noise is visible next to each value. Reads
only the `aggregates` block each run already stores, so nothing about the metrics is re-derived.
Stdlib only — no third-party dependencies."""

import argparse
import csv
import json
import re
import statistics
from collections import defaultdict
from pathlib import Path

# The headline metrics lifted from each run's `aggregates` block, in report column order. Each
# entry maps a short column name to how its per-run value is pulled from the aggregates dict
# (keyed "<panel> / <series>", value {"avg","max"}): a fixed key for scalar/quantile series, or
# a panel prefix summed across its series (CPU/memory total across containers, as the live table
# does). We take each series' run-average (`avg`), matching monitor.py's live view.
SERIES_METRICS = [
    ("msgs_s", "messages_per_second / value"),
    ("committed_s", "committed_per_second / value"),
    ("req_p50", 'publisher_subscriber_latency_ms / quantile="0.5"'),
    ("req_p95", 'publisher_subscriber_latency_ms / quantile="0.95"'),
    ("req_p99", 'publisher_subscriber_latency_ms / quantile="0.99"'),
    ("req_p999", 'publisher_subscriber_latency_ms / quantile="0.999"'),
    ("e2e_p50", 'publisher_db_e2e_latency_ms / quantile="0.5"'),
    ("e2e_p95", 'publisher_db_e2e_latency_ms / quantile="0.95"'),
    ("e2e_p99", 'publisher_db_e2e_latency_ms / quantile="0.99"'),
    ("e2e_p999", 'publisher_db_e2e_latency_ms / quantile="0.999"'),
    ("db_p50", 'db_write_latency_ms / quantile="0.5"'),
    ("db_p95", 'db_write_latency_ms / quantile="0.95"'),
    ("db_p99", 'db_write_latency_ms / quantile="0.99"'),
    ("db_p999", 'db_write_latency_ms / quantile="0.999"'),
]
SUM_METRICS = [
    ("cpu_cores", "container_cpu_usage / "),
    ("mem_mb", "container_memory_usage_mb / "),
]
METRIC_NAMES = [name for name, _ in SERIES_METRICS] + [name for name, _ in SUM_METRICS]

# Pretty section titles for the OFAT groups parsed out of scenario stems; unknown groups fall
# back to a title-cased name.
GROUP_TITLES = {
    "load": "Load", "pool": "Pool", "payload": "Payload",
    "batchsize": "Batch size", "batchto": "Batch timeout",
}


# Pull one run's headline metrics out of its stored aggregates: each series metric is that
# series' run-average, each sum metric totals every series under its panel prefix. Missing
# series (e.g. a summary that never observed) yield None so they can be skipped in the mean.
def extract_metrics(aggregates):
    metrics = {}
    for name, key in SERIES_METRICS:
        entry = aggregates.get(key)
        metrics[name] = entry["avg"] if entry else None
    for name, prefix in SUM_METRICS:
        values = [v["avg"] for k, v in aggregates.items() if k.startswith(prefix)]
        metrics[name] = sum(values) if values else None
    return metrics


# Split a scenario file name ("timescale_load_p20.env") into its OFAT group and factor value
# ("load", "p20") for report sectioning; names that don't fit collapse to (stem, "-").
def parse_group_value(scenario):
    stem = Path(scenario).stem
    stem = stem[len("timescale_"):] if stem.startswith("timescale_") else stem
    group, _, value = stem.partition("_")
    return group, (value or "-")


# Order factor values by magnitude across mixed unit suffixes (p04<p64, 256b<1kb<10kb) by
# scaling the first digit run by a k/m suffix, so payload byte sizes sort semantically.
def value_sort_key(value):
    match = re.search(r"\d+", value)
    if not match:
        return (0, value)
    num = int(match.group())
    unit = value[match.end():].lower()
    factor = 1024 if unit.startswith("k") else 1024 * 1024 if unit.startswith("m") else 1
    return (num * factor, value)


# Load every *.json under output_dir (skipping any report artifacts), grouping runs by scenario.
def load_runs(output_dir):
    by_scenario = defaultdict(list)
    for path in sorted(output_dir.glob("*.json")):
        try:
            data = json.loads(path.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        scenario = data.get("scenario")
        if not scenario or "aggregates" not in data:
            continue
        by_scenario[scenario].append(extract_metrics(data["aggregates"]))
    return by_scenario


# Reduce a scenario's per-rep metric dicts to {metric: (mean, stdev)} over the reps that had a
# value; a metric absent from every rep becomes (None, None). stdev is population stdev, so a
# single rep reports 0 rather than erroring.
def summarize(runs):
    summary = {}
    for name in METRIC_NAMES:
        values = [run[name] for run in runs if run.get(name) is not None]
        if values:
            summary[name] = (statistics.mean(values), statistics.pstdev(values))
        else:
            summary[name] = (None, None)
    return summary


def fmt_cell(mean, std):
    """Render a metric as 'mean ± std' to one decimal, or '-' when no rep observed it."""
    return "-" if mean is None else f"{mean:.1f} ± {std:.1f}"


# Write one CSV row per scenario: identity columns then mean/std pairs for every metric.
def write_csv(path, rows):
    header = ["scenario", "group", "value", "reps"]
    for name in METRIC_NAMES:
        header += [f"{name}_mean", f"{name}_std"]
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        for row in rows:
            record = [row["scenario"], row["group"], row["value"], row["reps"]]
            for name in METRIC_NAMES:
                mean, std = row["summary"][name]
                record += ["" if mean is None else f"{mean:.4f}",
                           "" if std is None else f"{std:.4f}"]
            writer.writerow(record)


# Write the Markdown report: one table per OFAT group, rows sorted by factor magnitude, cells
# 'mean ± std' across reps.
def write_markdown(path, rows, output_dir, run_count):
    by_group = defaultdict(list)
    for row in rows:
        by_group[row["group"]].append(row)

    lines = [
        "# Benchmark report",
        "",
        f"Source: `{output_dir}` — {run_count} runs across {len(rows)} scenarios.",
        "",
        "Latencies in ms, `msgs_s` (ingested) and `committed_s` (DB-committed) in msg/s, "
        "`cpu_cores` in cores, `mem_mb` in MB. Cells are mean ± stdev across reps.",
        "",
    ]
    header = "| value | reps | " + " | ".join(METRIC_NAMES) + " |"
    divider = "|" + "---|" * (len(METRIC_NAMES) + 2)
    for group in sorted(by_group):
        lines.append(f"## {GROUP_TITLES.get(group, group.title())}")
        lines.append("")
        lines.append(header)
        lines.append(divider)
        for row in sorted(by_group[group], key=lambda r: value_sort_key(r["value"])):
            cells = " | ".join(fmt_cell(*row["summary"][name]) for name in METRIC_NAMES)
            lines.append(f"| {row['value']} | {row['reps']} | {cells} |")
        lines.append("")
    path.write_text("\n".join(lines))


def main():
    """Aggregate output JSONs into a per-scenario CSV and a grouped Markdown report."""
    parser = argparse.ArgumentParser(description="Aggregate benchmark runs into CSV + Markdown")
    parser.add_argument("--output-dir", type=Path, default=Path(__file__).parent / "output",
                        help="Directory holding the per-run JSON files (default: ./output)")
    parser.add_argument("--csv", type=Path, default=None, help="CSV path (default: <output-dir>/report.csv)")
    parser.add_argument("--md", type=Path, default=None, help="Markdown path (default: <output-dir>/report.md)")
    args = parser.parse_args()

    csv_path = args.csv or args.output_dir / "report.csv"
    md_path = args.md or args.output_dir / "report.md"

    by_scenario = load_runs(args.output_dir)
    if not by_scenario:
        print(f"No benchmark JSON found in {args.output_dir}")
        return

    rows = []
    run_count = 0
    for scenario in sorted(by_scenario):
        runs = by_scenario[scenario]
        run_count += len(runs)
        group, value = parse_group_value(scenario)
        rows.append({
            "scenario": scenario, "group": group, "value": value,
            "reps": len(runs), "summary": summarize(runs),
        })

    write_csv(csv_path, rows)
    write_markdown(md_path, rows, args.output_dir, run_count)
    print(f"Wrote {csv_path} and {md_path} ({len(rows)} scenarios, {run_count} runs).")


if __name__ == "__main__":
    main()
