#!/usr/bin/env python3
"""Post-run report: aggregate every per-rep benchmark JSON under output/ into a CSV (one row
per scenario) and a Markdown report (one headed table per OFAT group, plus a startup table and a
provenance block). Throughput/resource figures are collapsed to mean ± stdev across reps; latency
figures are *pooled* — the reps' steady-state histogram buckets are summed and one quantile taken
over the total, because the mean of three reps' p99s is not a quantile of anything.
Stdlib only — no third-party dependencies."""

import argparse
import csv
import hashlib
import json
import math
import re
import statistics
from collections import defaultdict
from pathlib import Path

# The headline scalar metrics lifted from each run's `aggregates` block, in report column order.
# Each entry maps a short column name to how its per-run value is pulled from the aggregates dict
# (keyed "<panel> / <series>", value {"avg","max"}): a fixed key for scalar series, or a panel
# prefix summed across its series (CPU/memory total across containers, as the live table does).
# monitor.py now computes `aggregates` over steady-state samples only, so these are steady-state
# figures — they read higher than pre-migration runs, which averaged the startup ramp in.
SERIES_METRICS = [
    ("msgs_s", "messages_per_second / value"),
    ("committed_s", "committed_per_second / value"),
    ("reads_s", "reads_per_second / value"),
]
SUM_METRICS = [
    ("cpu_cores", "container_cpu_usage / "),
    ("mem_mb", "container_memory_usage_mb / "),
]

# The four latency stages, keyed by the monitor.py panel base name that carries their raw buckets.
# Column prefixes match the pre-migration report so a reader's muscle memory still works.
LATENCY_STAGES = [
    ("req", "publisher_subscriber_latency_ms"),
    ("e2e", "publisher_db_e2e_latency_ms"),
    ("db", "db_write_latency_ms"),
    ("read", "db_read_latency_ms"),
]
QUANTILES = [("p50", 0.5), ("p95", 0.95), ("p99", 0.99), ("p999", 0.999)]

SCALAR_NAMES = [name for name, _ in SERIES_METRICS] + [name for name, _ in SUM_METRICS]
LATENCY_NAMES = [f"{prefix}_{q}" for prefix, _ in LATENCY_STAGES for q, _ in QUANTILES] + \
                [f"{prefix}_mean" for prefix, _ in LATENCY_STAGES]
METRIC_NAMES = SCALAR_NAMES + LATENCY_NAMES

# Pretty section titles for the OFAT groups parsed out of scenario stems; unknown groups fall
# back to a title-cased name.
GROUP_TITLES = {
    "load": "Load", "pool": "Pool", "payload": "Payload",
    "batchsize": "Batch size", "batchto": "Batch timeout", "batching": "Batching",
    "reads": "DB reads",
}


# Parse an `le="..."` series key into its numeric bucket bound; "+Inf" becomes infinity so the
# bounds sort into the order histogram_quantile expects.
def parse_bound(series_label):
    value = series_label.split('=', 1)[1].strip('"') if '=' in series_label else series_label
    return math.inf if value in ("+Inf", "Inf", "inf") else float(value)


# Cumulative bucket counts at one sample, as {bound: count}. Missing/NaN entries are dropped rather
# than defaulted, so a panel that has not observed yet yields {} instead of a bogus all-zero row.
def buckets_at(sample, panel):
    series = sample["panels"].get(f"{panel}_buckets", {})
    return {parse_bound(k): v for k, v in series.items() if not math.isnan(v)}


# Single scalar value of a label-less panel at one sample, or None when absent/NaN.
def scalar_at(sample, panel):
    value = sample["panels"].get(panel, {}).get("value")
    return None if value is None or math.isnan(value) else value


# Subtract two cumulative bucket snapshots to get the observations that fell in the window between
# them. This is what makes the warm-up trim a reporting parameter: any pair of samples can be
# differenced offline, so re-slicing a run never needs re-collecting it.
def bucket_delta(start, end):
    if not start or not end:
        return {}
    return {bound: max(0.0, end.get(bound, 0.0) - start.get(bound, 0.0)) for bound in end}


# Sum bucket vectors elementwise. Pooling reps in the histogram domain and taking one quantile over
# the total is the whole point: averaging per-rep quantiles can report a value no rep ever saw.
def pool_buckets(vectors):
    pooled = defaultdict(float)
    for vector in vectors:
        for bound, count in vector.items():
            pooled[bound] += count
    return dict(pooled)


# Quantile of a cumulative-bucket histogram, following Prometheus' histogram_quantile semantics:
# linear interpolation inside the bucket the rank falls in, and a rank landing in the +Inf bucket
# clamps to the highest finite bound. Accuracy is therefore bounded by the bucket width — in
# milliseconds, unlike the rank error of the CKMS summary this replaced.
def histogram_quantile(quantile, buckets):
    if not buckets:
        return None
    bounds = sorted(buckets)
    total = buckets[bounds[-1]]
    if total <= 0:
        return None
    rank = quantile * total
    index = next((i for i, b in enumerate(bounds) if buckets[b] >= rank), len(bounds) - 1)
    if index == len(bounds) - 1 and math.isinf(bounds[index]):
        return bounds[index - 1] if len(bounds) > 1 else None
    upper = bounds[index]
    lower = bounds[index - 1] if index > 0 else 0.0
    count = buckets[upper] - (buckets[bounds[index - 1]] if index > 0 else 0.0)
    rank -= (buckets[bounds[index - 1]] if index > 0 else 0.0)
    return upper if count <= 0 else lower + (upper - lower) * (rank / count)


# One rep's steady-state and warm-up histogram material per stage, or None when the run predates
# the histogram migration (no *_buckets panels) and can only be read through its stored quantiles.
def extract_histograms(run):
    samples = run.get("samples") or []
    warmup = run.get("warmup_seconds", 0)
    steady = [s for s in samples if s["elapsed_seconds"] >= warmup]
    if len(steady) < 2:
        return None
    stages = {}
    for prefix, panel in LATENCY_STAGES:
        first, last = buckets_at(steady[0], panel), buckets_at(steady[-1], panel)
        # A stage with no buckets is skipped, not treated as a failed run: the read stage is absent
        # from every run collected before the read group existed, and failing the run over it would
        # silently drop those runs' write-path histograms too and re-rate the whole archived dataset.
        if not first or not last:
            continue
        stages[prefix] = {
            "steady": bucket_delta(first, last),
            "sum": (scalar_at(steady[-1], f"{panel}_sum") or 0.0)
                   - (scalar_at(steady[0], f"{panel}_sum") or 0.0),
            "count": (scalar_at(steady[-1], f"{panel}_count") or 0.0)
                     - (scalar_at(steady[0], f"{panel}_count") or 0.0),
            # Warm-up is read as the absolute cumulative counts at the trim boundary, not a delta:
            # the counters start at zero when the subscriber boots, so this covers the entire
            # transient including what happened before monitor.py's first sample.
            "warmup": buckets_at(steady[0], panel),
        }
    # No stage at all means a pre-migration run with no bucket panels; the legacy estimator reads it.
    return stages or None


# Seconds until throughput first settles within 5% of its steady mean and stays there. Reported as
# a measurement, never used to trim: a per-rep trim would give the two arms windows of different
# length and phase, which is the same class of uncontrolled asymmetry the migration removes.
def time_to_steady_state(run):
    samples = run.get("samples") or []
    warmup = run.get("warmup_seconds", 0)
    steady = [scalar_at(s, "messages_per_second") for s in samples if s["elapsed_seconds"] >= warmup]
    steady = [v for v in steady if v is not None]
    if not steady:
        return None
    target = statistics.mean(steady)
    if target <= 0:
        return None
    settled = None
    for sample in samples:
        value = scalar_at(sample, "messages_per_second")
        if value is None or abs(value - target) / target > 0.05:
            settled = None
        elif settled is None:
            settled = sample["elapsed_seconds"]
    return settled


# Pull one run's scalar metrics out of its stored aggregates: each series metric is that series'
# steady-state average, each sum metric totals every series under its panel prefix. Missing series
# yield None so they can be skipped in the mean.
def extract_scalars(aggregates):
    metrics = {}
    for name, key in SERIES_METRICS:
        entry = aggregates.get(key)
        metrics[name] = entry["avg"] if entry else None
    for name, prefix in SUM_METRICS:
        values = [v["avg"] for k, v in aggregates.items() if k.startswith(prefix)]
        metrics[name] = sum(values) if values else None
    return metrics


# Pre-migration fallback: runs with no bucket data can only be read through the per-rep quantiles
# their summaries exposed, which is the averaged-quantile estimator this report replaced. Kept so
# the archived pre-fix dataset stays loadable, and flagged in the report so it is never mistaken
# for a pooled figure.
LEGACY_KEYS = {
    f"{prefix}_{q}": f'{panel} / quantile="{level}"'
    for prefix, panel in LATENCY_STAGES for q, level in QUANTILES
}


def extract_legacy_latencies(aggregates):
    latencies = {}
    for name, key in LEGACY_KEYS.items():
        entry = aggregates.get(key)
        latencies[name] = entry["avg"] if entry else None
    for prefix, panel in LATENCY_STAGES:
        entry = aggregates.get(f"{panel}_mean / value")
        latencies[f"{prefix}_mean"] = entry["avg"] if entry else None
    return latencies


# Split a scenario file name ("timescale_load_p20.env") into its OFAT group and factor value
# ("load", "p20") for report sectioning; names that don't fit collapse to (stem, "-").
def parse_group_value(scenario):
    stem = Path(scenario).stem
    stem = stem[len("timescale_"):] if stem.startswith("timescale_") else stem
    group, _, value = stem.partition("_")
    return group, (value or "-")


# Order factor values by magnitude across mixed unit suffixes (p04<p48, 256b<1kb<10kb) by
# scaling the first digit run by a k/m suffix, so payload byte sizes sort semantically.
def value_sort_key(value):
    match = re.search(r"\d+", value)
    if not match:
        return (0, value)
    num = int(match.group())
    unit = value[match.end():].lower()
    factor = 1024 if unit.startswith("k") else 1024 * 1024 if unit.startswith("m") else 1
    return (num * factor, value)


# Load every *.json under output_dir (skipping any report artifacts), grouping whole payloads by
# scenario. Reps whose observation counters went backwards are dropped: the subscriber restarted
# mid-run, so their bucket deltas are not a window over anything.
def load_runs(output_dir):
    by_scenario = defaultdict(list)
    excluded = defaultdict(int)
    for path in sorted(output_dir.glob("*.json")):
        try:
            data = json.loads(path.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        scenario = data.get("scenario")
        if not scenario or "aggregates" not in data:
            continue
        if data.get("counter_reset"):
            excluded[scenario] += 1
            continue
        by_scenario[scenario].append(data)
    return by_scenario, excluded


# Reduce a scenario's reps to one row: scalars as mean ± stdev, latencies as a pooled quantile over
# the summed steady-state buckets with the per-rep min/max kept beside it as the dispersion signal
# (pooling assumes the reps are exchangeable; the spread is what shows when they were not).
def summarize(runs):
    summary = {}
    for name in SCALAR_NAMES:
        values = [extract_scalars(r["aggregates"])[name] for r in runs]
        values = [v for v in values if v is not None]
        summary[name] = {"value": statistics.mean(values) if values else None,
                         "spread": statistics.pstdev(values) if values else None,
                         "kind": "mean_stdev"}

    per_rep = [extract_histograms(r) for r in runs]
    pooled_reps = [h for h in per_rep if h is not None]
    legacy = not pooled_reps

    for prefix, _ in LATENCY_STAGES:
        if legacy:
            for name in [f"{prefix}_{q}" for q, _ in QUANTILES] + [f"{prefix}_mean"]:
                values = [extract_legacy_latencies(r["aggregates"])[name] for r in runs]
                values = [v for v in values if v is not None]
                summary[name] = {"value": statistics.mean(values) if values else None,
                                 "spread": statistics.pstdev(values) if values else None,
                                 "kind": "legacy"}
            continue

        # Only the reps that carry this stage. A stage every rep lacks leaves empty cells rather
        # than a zero, which would read as a measured latency of zero rather than as "not measured".
        stage_reps = [h for h in pooled_reps if prefix in h]
        if not stage_reps:
            for name in [f"{prefix}_{q}" for q, _ in QUANTILES] + [f"{prefix}_mean"]:
                summary[name] = {"value": None, "spread": None, "kind": "pooled"}
            continue

        pooled = pool_buckets([h[prefix]["steady"] for h in stage_reps])
        for label, level in QUANTILES:
            rep_values = [histogram_quantile(level, h[prefix]["steady"]) for h in stage_reps]
            rep_values = [v for v in rep_values if v is not None]
            summary[f"{prefix}_{label}"] = {
                "value": histogram_quantile(level, pooled),
                "spread": (min(rep_values), max(rep_values)) if rep_values else None,
                "kind": "pooled",
            }
        total_sum = sum(h[prefix]["sum"] for h in stage_reps)
        total_count = sum(h[prefix]["count"] for h in stage_reps)
        summary[f"{prefix}_mean"] = {
            "value": total_sum / total_count if total_count > 0 else None,
            "spread": None, "kind": "pooled",
        }

    summary["_legacy"] = legacy
    # How many reps actually contributed buckets. Less than the rep count when a scenario mixes
    # pre- and post-migration runs: the scalar columns use every rep, the latency cells only these.
    summary["_pooled_reps"] = len(pooled_reps)
    return summary


# Startup figures per scenario: what the stack cost before it was measurable, and how long it took
# to settle once it was. bench.sh gates on ingestion before monitor.py starts, so without the gate
# waits a slower runtime boot would leave no trace in the data at all.
def summarize_startup(runs):
    def mean_of(key):
        values = [r.get(key) for r in runs]
        values = [v for v in values if v is not None]
        return statistics.mean(values) if values else None

    # Only runs that recorded a trim boundary can be judged against one; a pre-migration run has
    # no warmup_seconds and would otherwise count as "not settled" for free.
    settled = [(time_to_steady_state(r), r.get("warmup_seconds")) for r in runs]
    ttss = [v for v, _ in settled if v is not None]
    over = sum(1 for v, w in settled if v is not None and w is not None and v > w)
    warmup_p99 = []
    for run in runs:
        stages = extract_histograms(run)
        if stages and "e2e" in stages:
            value = histogram_quantile(0.99, stages["e2e"]["warmup"])
            if value is not None:
                warmup_p99.append(value)
    return {
        "gate_startup_s": mean_of("gate_startup_seconds"),
        "gate_ingest_s": mean_of("gate_ingest_seconds"),
        "ttss_s": statistics.mean(ttss) if ttss else None,
        # A rep that had not settled by the trim boundary contaminated its own steady-state window.
        "ttss_over_warmup": over,
        "warmup_e2e_p99": statistics.mean(warmup_p99) if warmup_p99 else None,
    }


def fmt_cell(entry):
    """Render one metric: 'mean ± stdev' for scalars, and for a pooled p99 the pooled value with the
    per-rep min–max beside it, so an unstable configuration cannot hide inside a plausible middle."""
    if entry is None or entry["value"] is None:
        return "-"
    value, spread = entry["value"], entry["spread"]
    if entry["kind"] == "pooled":
        if isinstance(spread, tuple) and abs(spread[1] - spread[0]) > 0.05 * max(abs(value), 1e-9):
            return f"{value:.1f} ({spread[0]:.1f}–{spread[1]:.1f})"
        return f"{value:.1f}"
    return f"{value:.1f} ± {spread:.1f}" if spread is not None else f"{value:.1f}"


# Write one CSV row per scenario: identity columns, then value/low/high triples for every metric so
# the full per-rep dispersion is available even where the Markdown table shows the pooled value only.
def write_csv(path, rows):
    header = ["scenario", "group", "value", "reps", "excluded_reps", "estimator"]
    for name in METRIC_NAMES:
        header += [f"{name}", f"{name}_low", f"{name}_high"]
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        for row in rows:
            estimator = "legacy_averaged" if row["summary"].get("_legacy") else "pooled"
            record = [row["scenario"], row["group"], row["value"], row["reps"],
                      row["excluded"], estimator]
            for name in METRIC_NAMES:
                entry = row["summary"].get(name)
                if entry is None or entry["value"] is None:
                    record += ["", "", ""]
                    continue
                spread = entry["spread"]
                if isinstance(spread, tuple):
                    low, high = spread
                elif spread is None:
                    low = high = entry["value"]
                else:
                    low, high = entry["value"] - spread, entry["value"] + spread
                record += [f"{entry['value']:.4f}", f"{low:.4f}", f"{high:.4f}"]
            writer.writerow(record)


# Write the Markdown report: one headline table per OFAT group, then the startup table, then a
# provenance block recording every parameter needed to reproduce or re-slice the numbers.
def write_markdown(path, rows, output_dir, run_count, provenance):
    by_group = defaultdict(list)
    for row in rows:
        by_group[row["group"]].append(row)

    lines = [
        "# Benchmark report",
        "",
        f"Source: `{output_dir}` — {run_count} runs across {len(rows)} scenarios.",
        "",
        "Latencies in ms, `msgs_s` (ingested) and `committed_s` (DB-committed) in msg/s, "
        "`cpu_cores` in cores, `mem_mb` in MB.",
        "",
        f"All figures cover **steady state only** — the first {provenance['warmup_seconds']}s of "
        "each run is excluded. Throughput and CPU therefore read higher than in pre-trim reports, "
        "which averaged the startup ramp in; that is a definition change, not an improvement.",
        "",
        "Throughput/resource cells are mean ± stdev across reps. Latency cells are a **pooled "
        "quantile**: the reps' steady-state histogram buckets are summed and one quantile taken "
        "over the total, with the per-rep min–max in parentheses where the reps disagreed by more "
        "than 5%. A `mean` column accompanies each stage — it is exact, unquantized and never "
        "clamped, so it stays readable where a quantile lands in `+Inf`.",
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
            cells = " | ".join(fmt_cell(row["summary"].get(name)) for name in METRIC_NAMES)
            pooled = row["summary"].get("_pooled_reps", 0)
            reps = f"{row['reps']}" if pooled in (0, row["reps"]) else f"{row['reps']} ({pooled} hist)"
            lines.append(f"| {row['value']} | {reps} | {cells} |")
        lines.append("")

    lines += [
        "## Startup",
        "",
        "`gate_startup_s` is the wait for the subscriber's metrics endpoint, `gate_ingest_s` the "
        "further wait until the first message was ingested — together, the cost of getting the "
        "runtime to the point where it is measurable at all. `ttss_s` is seconds from first "
        "message until throughput settles within 5% of its steady mean.",
        "",
        "| scenario | gate_startup_s | gate_ingest_s | ttss_s | reps over warm-up | warmup_e2e_p99 |",
        "|---|---|---|---|---|---|",
    ]
    for row in sorted(rows, key=lambda r: r["scenario"]):
        s = row["startup"]
        def num(v):
            return "-" if v is None else f"{v:.1f}"
        lines.append(f"| {row['scenario']} | {num(s['gate_startup_s'])} | {num(s['gate_ingest_s'])} "
                     f"| {num(s['ttss_s'])} | {s['ttss_over_warmup']} | {num(s['warmup_e2e_p99'])} |")
    lines.append("")

    lines += [
        "## Provenance",
        "",
        "| key | value |",
        "|---|---|",
        f"| warm-up trim | {provenance['warmup_seconds']} s |",
        f"| run duration | {provenance['run_duration']} |",
        f"| sample interval | {provenance['interval']} s |",
        f"| histogram buckets | {provenance['bucket_count']} finite, "
        f"fingerprint `{provenance['bucket_fingerprint']}` |",
        f"| reps included | {provenance['reps_included']} |",
        f"| reps contributing histogram buckets | {provenance['reps_pooled']} |",
        f"| reps excluded (counter reset) | {provenance['reps_excluded']} |",
        f"| reps not settled by trim boundary | {provenance['reps_unsettled']} |",
        f"| latency estimator | {provenance['estimator']} |",
        "",
    ]
    path.write_text("\n".join(lines))


# Collect the run-level parameters the report has to disclose. The bucket fingerprint exists so a
# report can be told apart from one collected under a different bucket list: changing the bounds
# invalidates cross-run latency comparison exactly as the summary→histogram migration did.
def build_provenance(runs, rows, excluded_total):
    # Take run-level parameters from a run that actually recorded them: a pre-migration run
    # sorting first would otherwise report a 0s trim for a dataset that was properly trimmed.
    first = next((r for r in runs if r.get("warmup_seconds") is not None), runs[0] if runs else {})
    # Read the bounds from the first run that actually has them: a legacy run sorting first would
    # otherwise report "no buckets" for a dataset that does have them.
    finite = []
    for run in runs:
        for sample in run.get("samples") or []:
            bounds = sorted(buckets_at(sample, "publisher_db_e2e_latency_ms"))
            finite = [b for b in bounds if not math.isinf(b)]
            if finite:
                break
        if finite:
            break
    # sha1, not hash(): the built-in is salted per process, so the fingerprint has to be stable
    # across runs for it to identify a bucket list at all.
    fingerprint = "none"
    if finite:
        joined = ",".join(f"{b:g}" for b in finite)
        fingerprint = hashlib.sha1(joined.encode()).hexdigest()[:8]
    durations = [s["elapsed_seconds"] for r in runs for s in (r.get("samples") or [])]
    legacy = any(row["summary"].get("_legacy") for row in rows)
    return {
        "warmup_seconds": first.get("warmup_seconds", 0),
        "run_duration": f"{max(durations)} s" if durations else "-",
        "interval": first.get("interval_seconds", "-"),
        "bucket_count": len(finite),
        "bucket_fingerprint": fingerprint,
        "reps_included": sum(row["reps"] for row in rows),
        "reps_pooled": sum(row["summary"].get("_pooled_reps", 0) for row in rows),
        "reps_excluded": excluded_total,
        "reps_unsettled": sum(row["startup"]["ttss_over_warmup"] for row in rows),
        "estimator": "legacy averaged quantiles (pre-histogram data present)" if legacy
                     else "pooled histogram quantiles",
    }


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

    by_scenario, excluded = load_runs(args.output_dir)
    if not by_scenario:
        print(f"No benchmark JSON found in {args.output_dir}")
        return

    rows = []
    run_count = 0
    all_runs = []
    for scenario in sorted(by_scenario):
        runs = by_scenario[scenario]
        run_count += len(runs)
        all_runs += runs
        group, value = parse_group_value(scenario)
        rows.append({
            "scenario": scenario, "group": group, "value": value,
            "reps": len(runs), "excluded": excluded.get(scenario, 0),
            "summary": summarize(runs), "startup": summarize_startup(runs),
        })

    provenance = build_provenance(all_runs, rows, sum(excluded.values()))
    write_csv(csv_path, rows)
    write_markdown(md_path, rows, args.output_dir, run_count, provenance)
    mixed = [r["scenario"] for r in rows
             if 0 < r["summary"].get("_pooled_reps", 0) < r["reps"]]
    if mixed:
        print("WARNING: these scenarios mix pre- and post-histogram reps, so their latency cells "
              "are pooled over fewer reps than the scalar columns: " + ", ".join(mixed))
    if provenance["estimator"].startswith("legacy"):
        print("WARNING: some runs predate the histogram migration; their latency cells are averaged "
              "per-rep quantiles, not pooled — do not compare them against pooled cells.")
    print(f"Wrote {csv_path} and {md_path} ({len(rows)} scenarios, {run_count} runs).")


if __name__ == "__main__":
    main()
