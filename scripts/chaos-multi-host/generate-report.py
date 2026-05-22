#!/usr/bin/env python3
"""Generate a morning report from a multi-host chaos run.

Reads logs collected by the coordinator into
``target/chaos-multi-host/<run_id>/`` and emits a markdown
summary suitable for committing to ``dist/chaos-reports/``.

Sections:
  1. Run metadata (start, end, duration, hosts)
  2. Workload throughput per DC (total + per-class)
  3. Failure summary per DC (per (class, exception) tuple)
  4. Chaos events per DC (kinds + first/last timestamp)
  5. Dynomited / redis log signals (ERROR / WARN counts)
  6. Lessons / observations (auto-generated boilerplate the
     operator fills in)
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import sys
from pathlib import Path


def aggregate_workload(path: Path):
    counts = collections.Counter()
    fails = collections.Counter()
    snapshots = []
    if not path.exists():
        return counts, fails, snapshots
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except json.JSONDecodeError:
                continue
            for k, v in d.get("counts", {}).items():
                counts[k] += v
            for k, v in d.get("failures", {}).items():
                fails[k] += v
            snapshots.append(d)
    return counts, fails, snapshots


def aggregate_chaos_events(path: Path):
    kinds = collections.Counter()
    events = []
    if not path.exists():
        return kinds, events
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except json.JSONDecodeError:
                continue
            kinds[d.get("kind", "?")] += 1
            events.append(d)
    return kinds, events


def count_log_signals(path: Path):
    if not path.exists():
        return 0, 0
    errs = warns = 0
    try:
        for line in path.read_text(errors="replace").splitlines():
            if "ERROR" in line:
                errs += 1
            elif "WARN" in line:
                warns += 1
    except OSError:
        pass
    return errs, warns


def by_class(counts):
    """Aggregate (class/op) counter into a per-class dict."""
    by = collections.Counter()
    for k, v in counts.items():
        cls = k.split("/", 1)[0]
        by[cls] += v
    return by


def section_header(title, level=2):
    return "\n" + "#" * level + " " + title + "\n\n"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("run_dir", help="Path to target/chaos-multi-host/<run_id>")
    p.add_argument("--out", default=None,
                   help="Output markdown path; default <run_dir>/report.md")
    args = p.parse_args()

    run_dir = Path(args.run_dir).resolve()
    if not run_dir.is_dir():
        print(f"run dir not found: {run_dir}", file=sys.stderr)
        sys.exit(1)

    run_id = run_dir.name

    # Discover hosts present
    host_dirs = {}
    for sub in run_dir.iterdir():
        if sub.is_dir() and sub.name.endswith("-logs"):
            label = "dc-" + sub.name[:-len("-logs")]
            host_dirs[label] = sub

    out_path = Path(args.out) if args.out else run_dir / "report.md"
    lines = []

    lines.append(f"# Multi-host chaos run: `{run_id}`\n")

    # 1. Run metadata
    lines.append(section_header("Run metadata"))
    coord_log = run_dir / "coordinator.log"
    if coord_log.exists():
        coord_text = coord_log.read_text(errors="replace").splitlines()
        first = coord_text[0] if coord_text else ""
        last = coord_text[-1] if coord_text else ""
        lines.append(f"- Coordinator log: `{coord_log.relative_to(run_dir)}`")
        lines.append(f"- First line: `{first}`")
        lines.append(f"- Last line: `{last}`")
        # Find duration line
        for line in coord_text:
            if "duration:" in line:
                lines.append(f"- {line.split(']', 1)[1].strip() if ']' in line else line.strip()}")
                break
    lines.append(f"- Hosts observed: {sorted(host_dirs.keys())}")
    lines.append("")

    # 2. Workload throughput per DC
    lines.append(section_header("Workload throughput per DC"))
    lines.append("| DC | total ok | total fail | EVAL ok | PING ok | classes covered |")
    lines.append("|---|---|---|---|---|---|")
    grand_ok = grand_fail = 0
    per_dc_data = {}
    for label in sorted(host_dirs):
        wl = host_dirs[label] / f"workload-{label}.ndjson"
        counts, fails, snaps = aggregate_workload(wl)
        per_dc_data[label] = (counts, fails, snaps)
        total_ok = sum(counts.values())
        total_fail = sum(fails.values())
        grand_ok += total_ok
        grand_fail += total_fail
        eval_ok = counts.get("scripting/EVAL", 0)
        ping_ok = counts.get("scripting/PING", 0)
        classes = sorted(by_class(counts).keys())
        lines.append(
            f"| {label} | {total_ok} | {total_fail} | {eval_ok} | {ping_ok} | "
            f"{len(classes)} ({', '.join(classes)}) |"
        )
    lines.append(f"| **total** | **{grand_ok}** | **{grand_fail}** | | | |")
    lines.append("")

    # 2a. Per-class breakdown
    lines.append(section_header("Workload class breakdown", 3))
    for label in sorted(host_dirs):
        counts, fails, _ = per_dc_data[label]
        lines.append(f"\n**{label}**:")
        bc = by_class(counts).most_common()
        for cls, n in bc:
            cls_fail = sum(v for k, v in fails.items() if k.startswith(cls + "/"))
            lines.append(f"  - {cls}: ok={n} fail={cls_fail}")

    # 3. Failure summary
    lines.append(section_header("Failure summary"))
    any_fail = False
    for label in sorted(host_dirs):
        _, fails, _ = per_dc_data[label]
        if fails:
            any_fail = True
            lines.append(f"\n**{label}**:")
            for k, v in fails.most_common():
                lines.append(f"  - `{k}`: {v}")
    if not any_fail:
        lines.append("**No client-visible failures across all DCs.**")

    # 4. Chaos events
    lines.append(section_header("Chaos events fired"))
    lines.append("| DC | pause cycles | kills | redis bounces | other |")
    lines.append("|---|---|---|---|---|")
    total_paused = total_killed = total_bounced = 0
    for label in sorted(host_dirs):
        ev_path = host_dirs[label] / f"chaos-events-{label}.ndjson"
        kinds, events = aggregate_chaos_events(ev_path)
        # Filter to events from THIS run only by start timestamp
        # The chaos-injector log path was shared across smoke runs;
        # take only events whose ISO timestamp is >= run start.
        # Heuristic: take events that occurred after coordinator
        # log creation time.
        if coord_log.exists():
            run_start = coord_log.stat().st_mtime
            # Parse ts strings and filter
            import datetime as _dt
            kept = collections.Counter()
            for e in events:
                ts_s = e.get("ts", "")
                try:
                    ts = _dt.datetime.fromisoformat(ts_s.replace("Z", "+00:00")).timestamp()
                except (ValueError, TypeError):
                    continue
                if ts >= run_start:
                    kept[e["kind"]] += 1
            kinds = kept
        pauses = kinds.get("pause_end", 0)
        kills = kinds.get("kill", 0)
        bounces = kinds.get("redis_bounce", 0)
        other = sum(v for k, v in kinds.items()
                    if k not in {"pause_start", "pause_end", "kill",
                                 "redis_bounce", "injector_start",
                                 "injector_exit", "restart"})
        total_paused += pauses
        total_killed += kills
        total_bounced += bounces
        lines.append(f"| {label} | {pauses} | {kills} | {bounces} | {other} |")
    lines.append(f"| **total** | **{total_paused}** | **{total_killed}** | "
                 f"**{total_bounced}** | |")
    lines.append("")

    # 5. Log signals
    lines.append(section_header("Dynomited / redis log signals"))
    lines.append("| DC | dynomited ERROR | dynomited WARN | redis log present |")
    lines.append("|---|---|---|---|")
    for label in sorted(host_dirs):
        dlog = host_dirs[label] / f"dynomited-{label}.log"
        rlog = host_dirs[label] / f"redis-{label}.log"
        errs, warns = count_log_signals(dlog)
        lines.append(f"| {label} | {errs} | {warns} | "
                     f"{'yes' if rlog.exists() else 'no'} |")
    lines.append("")

    # 6. Throughput over time (per-window snapshot summary)
    lines.append(section_header("Throughput over time (10s windows)"))
    lines.append("| DC | windows | ops/win mean | ops/win min | ops/win max | fail/win mean |")
    lines.append("|---|---|---|---|---|---|")
    for label in sorted(host_dirs):
        _, _, snaps = per_dc_data[label]
        if not snaps:
            continue
        per_win_ok = [sum(s.get("counts", {}).values()) for s in snaps]
        per_win_fail = [sum(s.get("failures", {}).values()) for s in snaps]
        if per_win_ok:
            mean = sum(per_win_ok) / len(per_win_ok)
            mn = min(per_win_ok)
            mx = max(per_win_ok)
            fmean = sum(per_win_fail) / len(per_win_fail)
            lines.append(f"| {label} | {len(snaps)} | {mean:.0f} | {mn} | {mx} | "
                         f"{fmean:.1f} |")
    lines.append("")

    # 7. Observations
    lines.append(section_header("Observations"))
    lines.append("(filled in by operator review)")
    lines.append("")

    out_path.write_text("\n".join(lines))
    print(f"wrote {out_path} ({len(lines)} lines)")


if __name__ == "__main__":
    main()
