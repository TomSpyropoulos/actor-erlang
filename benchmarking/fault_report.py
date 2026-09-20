#!/usr/bin/env python3
"""Post-run report for fault-injection runs: read every per-run JSON under output/faults/<backend>/
and describe what each runtime did while a dependency it needs was gone. One summary row per run,
then a per-run event list and phase breakdown, plus a long-format CSV of every sample for plotting.

Every figure is derived from the raw cumulative counters rather than the rate panels: a 15s rate
window turns an abrupt stop into a 15s slide, which is exactly the edge these runs are about.
Stdlib only — no third-party dependencies."""

import argparse
import csv
import json
import math
from pathlib import Path

# The two counters every column below is read from. Both are collection-only in monitor.py: they
# have no dashboard panel and exist to be differenced offline.
INGEST_PANEL = "requests_total"
COMMIT_PANEL = "committed_total"

SUMMARY_COLUMNS = [
    "scenario", "target", "outage_s", "rep", "sub_restarted", "restarts", "dark_s",
    "ingest_stop_s", "ingest_resume_s", "commit_stop_s", "commit_resume_s", "final_gap",
]


# One counter's value at one sample, or None when the series was absent or NaN. Absent is not zero
# and must not be read as one: Prometheus drops a series as soon as its target stops answering, so
# None here means the subscriber was unreachable at that sample, which is itself a measurement.
def counter_at(sample, panel):
    value = sample["panels"].get(panel, {}).get("value")
    return None if value is None or math.isnan(value) else value


# Consecutive (previous, current) sample pairs where both sides carry a reading, so a dark stretch
# splits the run into segments rather than producing a bogus jump across it.
def counter_pairs(samples, panel):
    previous = None
    for sample in samples:
        current = counter_at(sample, panel)
        if current is not None and previous is not None:
            yield previous, current, sample
        previous = current


# Total observations in a stretch of samples, summing only the positive steps. A restart sets the
# counter back to zero, so a plain last-minus-first would report a negative count for exactly the
# runs this report exists to describe.
def advance_sum(samples, panel):
    return sum(max(0.0, current - previous) for previous, current, _ in counter_pairs(samples, panel))


# Elapsed second at which a counter first stops advancing at or after `since`. A sample where the
# subscriber is unreachable counts as stopped: from the pipeline's point of view there is no
# difference between a subscriber that ingests nothing and one that is not there.
def first_stop(samples, panel, since):
    previous = None
    for sample in samples:
        current = counter_at(sample, panel)
        if sample["elapsed_seconds"] >= since and previous is not None:
            if current is None or current <= previous:
                return sample["elapsed_seconds"]
        previous = current
    return None


# Elapsed second at which a counter first advances again at or after `since`. A restarted subscriber
# counts from zero, so the comparison is against the previous sample rather than the pre-fault peak.
def first_resume(samples, panel, since):
    for previous, current, sample in counter_pairs(samples, panel):
        if sample["elapsed_seconds"] >= since and current > previous:
            return sample["elapsed_seconds"]
    return None


# Which phase of the run a sample falls in. The baseline is the reference the other two are read
# against, which is why bench.sh places the stop after the warm-up trim rather than at the start.
def phase_of(elapsed, injected_at, healed_at):
    if elapsed < injected_at:
        return "baseline"
    return "outage" if elapsed < healed_at else "recovery"


# Reduce one run to the summary row and the per-phase breakdown behind it. `injected_at`/`healed_at`
# come from the events monitor.py recorded as it issued each command, not from the requested
# schedule, so a command that landed a sample late is described where it actually landed.
def analyse(run):
    fault = run.get("fault") or {}
    samples = run.get("samples") or []
    events = fault.get("events") or []
    interval = run.get("interval_seconds") or 1
    injected_at = events[0]["elapsed_seconds"] if events else fault.get("fault_at_seconds", 0)
    healed_at = events[1]["elapsed_seconds"] if len(events) > 1 else injected_at + fault.get("outage_seconds", 0)

    dark = [s for s in samples if s["elapsed_seconds"] >= injected_at
            and counter_at(s, INGEST_PANEL) is None]
    last_lit = next((s for s in reversed(samples) if counter_at(s, INGEST_PANEL) is not None), None)
    gap = None
    if last_lit is not None:
        ingested, committed = counter_at(last_lit, INGEST_PANEL), counter_at(last_lit, COMMIT_PANEL)
        gap = None if committed is None else ingested - committed

    def since(value, base):
        return None if value is None else value - base

    # A counter that never stopped has nothing to resume, so its resume column is not applicable
    # rather than zero: a 0 there reads as "recovered instantly", which inverts the finding.
    def resume_after(panel, stopped_at):
        if stopped_at is None:
            return "n/a"
        return since(first_resume(samples, panel, healed_at), healed_at)

    ingest_stop = first_stop(samples, INGEST_PANEL, injected_at)
    commit_stop = first_stop(samples, COMMIT_PANEL, injected_at)

    phases = []
    for name in ("baseline", "outage", "recovery"):
        window = [s for s in samples if phase_of(s["elapsed_seconds"], injected_at, healed_at) == name]
        if not window:
            continue
        seconds = max(interval, len(window) * interval)
        phases.append({
            "phase": name,
            "samples": len(window),
            "unreachable": sum(1 for s in window if counter_at(s, INGEST_PANEL) is None),
            "ingested": advance_sum(window, INGEST_PANEL),
            "committed": advance_sum(window, COMMIT_PANEL),
            "ingest_rate": advance_sum(window, INGEST_PANEL) / seconds,
            "commit_rate": advance_sum(window, COMMIT_PANEL) / seconds,
        })

    return {
        "scenario": run.get("scenario", "-"),
        "target": fault.get("target", "-"),
        "outage_s": fault.get("outage_seconds"),
        "rep": run.get("rep"),
        "sub_restarted": bool(run.get("counter_reset")),
        "restarts": fault.get("restart_counts") or {},
        "dark_s": len(dark) * interval,
        "ingest_stop_s": since(ingest_stop, injected_at),
        "ingest_resume_s": resume_after(INGEST_PANEL, ingest_stop),
        "commit_stop_s": since(commit_stop, injected_at),
        "commit_resume_s": resume_after(COMMIT_PANEL, commit_stop),
        "final_gap": gap,
        "injected_at": injected_at,
        "healed_at": healed_at,
        "interval": interval,
        "events": events,
        "phases": phases,
        "samples": samples,
    }


# Load every fault run under output_dir. A JSON with no `fault` block is skipped rather than
# reported: it is an ordinary benchmark run that happens to share a directory, and nothing here
# would mean anything for it.
def load_runs(output_dir):
    runs = []
    for path in sorted(output_dir.glob("*.json")):
        try:
            data = json.loads(path.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        if data.get("fault"):
            runs.append((path, data))
    return runs


# Render a derived second count. None means the thing never happened within the run, which for a
# resume column is the whole finding and must never be shown as a blank or a zero. "n/a" means it
# could not happen, because the counter it belongs to never stopped in the first place.
def fmt_seconds(value):
    if isinstance(value, str):
        return value
    return "never" if value is None else f"{value:g}"


# Restart counts come from Docker's own counter, read once after the run. An explicit stop/start
# does not increment it, so the dependency the fault stopped never appears here and everything that
# does is a restart its container's restart policy performed.
def fmt_restarts(restarts):
    if not restarts:
        return "none"
    return ", ".join(f"{name} x{count}" for name, count in sorted(restarts.items()))


# The source directory as the READMEs write it, relative to the repo root. bench.sh passes an
# absolute --output-dir, and printing that verbatim bakes one machine's home directory into a file
# meant to be read, and committed, somewhere else. A directory outside the repo is printed as given.
def source_label(output_dir):
    try:
        return output_dir.resolve().relative_to(Path(__file__).resolve().parent.parent)
    except ValueError:
        return output_dir


# Write the Markdown report: the summary table, then each run's events and phase breakdown, then a
# provenance block. The resolution caveat is stated rather than implied: every instant below is one
# sample interval plus one Prometheus scrape away from when it really happened.
def write_markdown(path, results, output_dir):
    lines = [
        "# Fault report",
        "",
        f"Source: `{source_label(output_dir)}` — {len(results)} fault "
        f"run{'s' if len(results) != 1 else ''}.",
        "",
        "Each run holds a dependency down for a fixed outage and watches what the pipeline does. "
        "Every column is derived from the subscriber's raw cumulative counters, not from a rate "
        "panel, because a 15s rate window smears the abrupt stop these runs are about.",
        "",
        "A sample where the subscriber served no metrics at all counts as stopped: from the "
        "pipeline's point of view a subscriber that ingests nothing and a subscriber that is not "
        "there are the same outcome. `dark_s` is how much of the run was in that state.",
        "",
        "`never` in a resume column means the pipeline had not recovered by the end of the run. "
        "That is a result, not missing data. `n/a` means that counter never stopped, so there was "
        "nothing to resume. The `restarts` column is Docker's own restart counter, which an "
        "explicit stop and start does not increment, so it shows blast radius and never the fault "
        "itself.",
        "",
        "| " + " | ".join(SUMMARY_COLUMNS) + " |",
        "|" + "---|" * len(SUMMARY_COLUMNS),
    ]
    for r in results:
        lines.append("| " + " | ".join([
            r["scenario"], r["target"], f"{r['outage_s']}", f"{r['rep'] if r['rep'] is not None else '-'}",
            "yes" if r["sub_restarted"] else "no",
            fmt_restarts(r["restarts"]),
            f"{r['dark_s']:g}",
            fmt_seconds(r["ingest_stop_s"]), fmt_seconds(r["ingest_resume_s"]),
            fmt_seconds(r["commit_stop_s"]), fmt_seconds(r["commit_resume_s"]),
            "-" if r["final_gap"] is None else f"{r['final_gap']:,.0f}",
        ]) + " |")

    lines += [
        "",
        "`final_gap` is ingested minus committed at the last sample. A gap that stays open is a "
        "subscriber that kept accepting messages it can no longer write. Where the subscriber "
        "restarted, both counters restarted with it, so the gap covers only the time since.",
        "",
        "The `sensor_status` blast radius is not derived here, because reading it needs a client "
        "for whichever database is loaded. `sub_restarted` already carries the signal it stood "
        "for: a restart loses every per-sensor actor's in-memory state.",
        "",
    ]

    for r in results:
        lines += [
            f"## {r['scenario']} — {r['target']}, {r['outage_s']}s outage"
            + (f" (rep {r['rep']})" if r["rep"] is not None else ""),
            "",
        ]
        for event in r["events"]:
            lines.append(f"- `{event['elapsed_seconds']}s` — `docker compose "
                         f"{event['action']} {event['service']}`")
        if not r["events"]:
            lines.append("- no fault events recorded")
        lines += [
            "",
            "| phase | samples | unreachable | ingested | committed | ingest/s | commit/s |",
            "|---|---|---|---|---|---|---|",
        ]
        for phase in r["phases"]:
            lines.append(f"| {phase['phase']} | {phase['samples']} | {phase['unreachable']} "
                         f"| {phase['ingested']:,.0f} | {phase['committed']:,.0f} "
                         f"| {phase['ingest_rate']:,.0f} | {phase['commit_rate']:,.0f} |")
        lines.append("")

    intervals = sorted({r["interval"] for r in results})
    lines += [
        "## Provenance",
        "",
        "| key | value |",
        "|---|---|",
        f"| runs | {len(results)} |",
        f"| sample interval | {', '.join(f'{i}s' for i in intervals)} |",
        f"| fault injected at | {', '.join(str(i) for i in sorted({r['injected_at'] for r in results}))} s elapsed |",
        f"| outage lengths | {', '.join(str(o) for o in sorted({r['outage_s'] for r in results}))} s |",
        "| timing resolution | one sample interval plus one 2s Prometheus scrape |",
        "",
        "Timings are recorded when the stop or start command was issued, which is not quite when "
        "the dependency became unreachable: `docker compose stop` sends SIGTERM and waits out the "
        "container's grace period. Comparing two runtimes against the same command is the point; "
        "reading an absolute number off one of them is not.",
        "",
    ]
    path.write_text("\n".join(lines))


# Write one CSV row per sample per run, long format. The Markdown collapses a run to a handful of
# derived numbers; this keeps the shape of every counter over time, which is what a plot needs.
def write_csv(path, results):
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["scenario", "target", "outage_s", "rep", "elapsed_s", "phase",
                         "reachable", "requests_total", "committed_total",
                         "msgs_per_s", "committed_per_s"])
        for r in results:
            for sample in r["samples"]:
                ingested = counter_at(sample, INGEST_PANEL)
                committed = counter_at(sample, COMMIT_PANEL)
                writer.writerow([
                    r["scenario"], r["target"], r["outage_s"], r["rep"],
                    sample["elapsed_seconds"],
                    phase_of(sample["elapsed_seconds"], r["injected_at"], r["healed_at"]),
                    int(ingested is not None),
                    "" if ingested is None else f"{ingested:.0f}",
                    "" if committed is None else f"{committed:.0f}",
                    "" if (v := counter_at(sample, "messages_per_second")) is None else f"{v:.2f}",
                    "" if (v := counter_at(sample, "committed_per_second")) is None else f"{v:.2f}",
                ])


def main():
    """Aggregate fault-run JSONs into a summary Markdown report and a per-sample CSV."""
    parser = argparse.ArgumentParser(description="Report on fault-injection benchmark runs")
    parser.add_argument("--output-dir", type=Path, default=Path(__file__).parent / "output/faults",
                        help="Directory holding the per-run fault JSON files")
    parser.add_argument("--csv", type=Path, default=None,
                        help="CSV path (default: <output-dir>/fault_report.csv)")
    parser.add_argument("--md", type=Path, default=None,
                        help="Markdown path (default: <output-dir>/fault_report.md)")
    args = parser.parse_args()

    runs = load_runs(args.output_dir)
    if not runs:
        print(f"No fault-run JSON found in {args.output_dir}")
        return

    results = [analyse(data) for _, data in runs]
    results.sort(key=lambda r: (r["scenario"], r["target"], r["outage_s"] or 0, r["rep"] or 0))

    csv_path = args.csv or args.output_dir / "fault_report.csv"
    md_path = args.md or args.output_dir / "fault_report.md"
    write_csv(csv_path, results)
    write_markdown(md_path, results, args.output_dir)

    unknown = [r for r in results if not r["restarts"] and r["sub_restarted"]]
    if unknown:
        print("NOTE: some runs restarted the subscriber but reported no container restarts. A "
              "process that restarts inside a container leaves the container itself untouched, so "
              "the two columns are answering different questions.")
    print(f"Wrote {csv_path} and {md_path} ({len(results)} fault runs).")


if __name__ == "__main__":
    main()
