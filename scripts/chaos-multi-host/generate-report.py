#!/usr/bin/env python3
"""Generate a per-mode chaos report from a multi-host chaos run.

Reads logs collected by the coordinator into
``target/chaos-multi-host/<run_id>/`` and emits a markdown report
that mirrors the hand-curated reports under
``dist/chaos-reports/v0.1.0/``.

The generator is a pure function from a run-dir to markdown. It
does NOT modify the run-dir in any way.

Sections produced:

  1. Run summary       (run-id, mode, hosts, duration, timestamps)
  2. Workload totals   (per-host + aggregate; ok / fail / rate)
  3. Top failure reasons (top 10 across all hosts)
  4. Chaos events by kind (per-host histogram + aggregate)
  5. Per-host stability indicators
                       (restart_failed / recovery_restart /
                        redis_bounce counts)
  6. Failure-cause metrics snapshot
                       (parsed from <run_dir>/metrics-*.json
                        snapshots if present; skipped silently
                        otherwise -- see P3-2.5)
  7. Notable timeline events
                       (first three restart_failed events with a
                        ``tail`` payload, if any)
  8. Provenance        (dynomited git sha if recorded; otherwise
                        a clear "not recorded" note)

CLI:

  python3 generate-report.py [--run-id <id>] [--out <path>]
                             [--all-runs]

  ``--run-id``   defaults to the most recent ``pass*-...Z`` or
                 ``prod-...Z`` directory under
                 ``target/chaos-multi-host/``.
  ``--out``      defaults to
                 ``dist/chaos-reports/v0.1.0/multi-host-pass-N-<mode>.md``
                 when the run-id encodes a pass number and a
                 mode; otherwise
                 ``dist/chaos-reports/v0.1.0/multi-host-<run-id>.md``.
  ``--all-runs`` regenerates a report for every run-dir found.
"""

from __future__ import annotations

import argparse
import collections
import datetime as dt
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RUNS_DIR = REPO_ROOT / "target" / "chaos-multi-host"
DEFAULT_OUT_DIR = REPO_ROOT / "dist" / "chaos-reports" / "v0.1.0"

RUN_ID_PATTERN = re.compile(
    r"^pass(?P<pass_num>\d+)-(?P<mode>redis|memcache|riak)-"
    r"(?P<stamp>\d{8}-\d{6}Z)$"
)
SIMPLE_RUN_ID_PATTERN = re.compile(
    r"^pass(?P<pass_num>\d+)-(?P<stamp>\d{8}-\d{6}Z)$"
)


# ---------- run-id parsing ----------


def parse_run_id(run_id: str):
    """Extract (pass_num, mode) from a run-id string.

    Returns a dict with optional ``pass_num`` and ``mode`` keys
    when the run-id matches one of the known shapes.
    """
    m = RUN_ID_PATTERN.match(run_id)
    if m:
        return {"pass_num": int(m.group("pass_num")), "mode": m.group("mode")}
    m = SIMPLE_RUN_ID_PATTERN.match(run_id)
    if m:
        return {"pass_num": int(m.group("pass_num")), "mode": None}
    return {"pass_num": None, "mode": None}


def default_out_path(run_id: str, out_dir: Path = DEFAULT_OUT_DIR) -> Path:
    info = parse_run_id(run_id)
    if info["pass_num"] is not None and info["mode"]:
        return out_dir / f"multi-host-pass-{info['pass_num']}-{info['mode']}.md"
    if info["pass_num"] is not None:
        return out_dir / f"multi-host-pass-{info['pass_num']}.md"
    return out_dir / f"multi-host-{run_id}.md"


# ---------- run-dir discovery ----------


def discover_host_dirs(run_dir: Path) -> dict:
    """Map dc-label -> host-logs directory.

    A host's logs live in ``<host>-logs/`` next to ``coordinator.log``.
    The DC label is reconstructed from the workload ndjson filename
    (``workload-dc-<label>.ndjson``) so renamed host dirs do not
    desync the report.
    """
    hosts = {}
    for sub in sorted(run_dir.iterdir()):
        if not (sub.is_dir() and sub.name.endswith("-logs")):
            continue
        label = None
        for entry in sub.iterdir():
            if entry.name.startswith("workload-dc-") and entry.suffix == ".ndjson":
                label = entry.stem[len("workload-"):]
                break
        if label is None:
            label = "dc-" + sub.name[: -len("-logs")]
        hosts[label] = sub
    return hosts


# ---------- ndjson aggregation ----------


def parse_workload_ndjson(path: Path):
    """Return (counts, failures, snapshots, first_ts, last_ts).

    ``counts`` and ``failures`` are aggregate Counters across all
    snapshots; ``snapshots`` is the raw list (in order); the two
    timestamps are the first and last ``ts`` field in the file.
    Missing files yield empty Counters and ``None`` timestamps.
    """
    counts = collections.Counter()
    failures = collections.Counter()
    snapshots = []
    first_ts = last_ts = None
    if not path.exists():
        return counts, failures, snapshots, first_ts, last_ts
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
                failures[k] += v
            snapshots.append(d)
            ts = d.get("ts")
            if isinstance(ts, (int, float)):
                if first_ts is None:
                    first_ts = ts
                last_ts = ts
    return counts, failures, snapshots, first_ts, last_ts


def parse_chaos_events(path: Path):
    """Return (kinds_counter, events_list)."""
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


# ---------- coordinator metadata ----------


def parse_coordinator_log(path: Path):
    """Pull a few useful fields from ``coordinator.log``.

    Returns a dict with optional ``planned_duration`` (seconds),
    ``coord_start`` (HH:MM:SS), ``coord_end`` (HH:MM:SS),
    ``hosts`` (list[str]), and ``mode`` (str).
    """
    out = {
        "planned_duration": None,
        "coord_start": None,
        "coord_end": None,
        "hosts": [],
        "mode": None,
    }
    if not path.exists():
        return out
    text = path.read_text(errors="replace").splitlines()
    for line in text:
        if "duration:" in line and out["planned_duration"] is None:
            m = re.search(r"duration:\s*(\d+)\s*s", line)
            if m:
                out["planned_duration"] = int(m.group(1))
        if "hosts:" in line and not out["hosts"]:
            after = line.split("hosts:", 1)[1].strip()
            out["hosts"] = after.split()
        if "mode:" in line and out["mode"] is None:
            after = line.split("mode:", 1)[1].strip()
            if after:
                out["mode"] = after.split()[0]
    bracketed = [ln for ln in text if ln.startswith("[")]
    if bracketed:
        m_first = re.match(r"\[(\d{2}:\d{2}:\d{2})\]", bracketed[0])
        m_last = re.match(r"\[(\d{2}:\d{2}:\d{2})\]", bracketed[-1])
        if m_first:
            out["coord_start"] = m_first.group(1)
        if m_last:
            out["coord_end"] = m_last.group(1)
    return out


def detect_git_sha(run_dir: Path):
    """Look for a recorded dynomited git sha.

    Checks (in order):
      1. ``<run_dir>/git-sha`` -- single-line file
      2. ``<run_dir>/start-args`` -- ``GIT_SHA=...`` line
      3. ``<run_dir>/launcher.log`` and ``coordinator.log`` for a
         line containing ``git sha`` (case-insensitive)

    Returns ``None`` if nothing matches.
    """
    sha_file = run_dir / "git-sha"
    if sha_file.exists():
        text = sha_file.read_text(errors="replace").strip()
        if text:
            return text.split()[0]
    start_args = run_dir / "start-args"
    if start_args.exists():
        for line in start_args.read_text(errors="replace").splitlines():
            m = re.match(r"GIT_SHA\s*=\s*['\"]?([0-9a-fA-F]{7,40})", line)
            if m:
                return m.group(1)
    for name in ("launcher.log", "coordinator.log"):
        p = run_dir / name
        if not p.exists():
            continue
        for line in p.read_text(errors="replace").splitlines():
            m = re.search(r"git sha[: ]\s*([0-9a-fA-F]{7,40})", line, re.I)
            if m:
                return m.group(1)
    return None


# ---------- metrics snapshot ----------


def parse_metrics_snapshot(run_dir: Path):
    """Look for a P3-2.5 failure-cause metrics snapshot.

    The convention this generator supports is one or more
    ``metrics-*.json`` files at the top of the run-dir, each a
    JSON object with at least the cause-counter keys (e.g.
    ``dispatch_no_targets_total``). The newest snapshot wins.

    Returns ``None`` if no snapshot is present.
    """
    candidates = sorted(run_dir.glob("metrics-*.json"))
    if not candidates:
        return None
    latest = candidates[-1]
    try:
        return json.loads(latest.read_text(errors="replace"))
    except (OSError, json.JSONDecodeError):
        return None


# ---------- helpers ----------


def fmt_int(n: int) -> str:
    return f"{n:,}"


def fmt_pct(num: float, denom: float) -> str:
    if denom <= 0:
        return "n/a"
    return f"{(num / denom) * 100:.2f}%"


def fmt_duration(seconds) -> str:
    if seconds is None:
        return "unknown"
    seconds = int(round(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m:02d}m {s:02d}s"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"


def fmt_iso(ts) -> str:
    if ts is None:
        return "unknown"
    return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


# ---------- report rendering ----------


def render_report(run_dir: Path) -> str:
    """Pure function: run-dir -> markdown string."""
    run_id = run_dir.name
    info = parse_run_id(run_id)
    coord = parse_coordinator_log(run_dir / "coordinator.log")
    mode = info["mode"] or coord["mode"] or "unknown"
    pass_num = info["pass_num"]

    host_dirs = discover_host_dirs(run_dir)
    host_labels = sorted(host_dirs.keys())

    # Aggregate per-host workload + chaos data once.
    per_host = {}
    grand_first_ts = None
    grand_last_ts = None
    for label in host_labels:
        wpath = host_dirs[label] / f"workload-{label}.ndjson"
        cpath = host_dirs[label] / f"chaos-events-{label}.ndjson"
        counts, failures, snapshots, first_ts, last_ts = parse_workload_ndjson(wpath)
        kinds, events = parse_chaos_events(cpath)
        per_host[label] = {
            "counts": counts,
            "failures": failures,
            "snapshots": snapshots,
            "first_ts": first_ts,
            "last_ts": last_ts,
            "kinds": kinds,
            "events": events,
        }
        if first_ts is not None:
            if grand_first_ts is None or first_ts < grand_first_ts:
                grand_first_ts = first_ts
        if last_ts is not None:
            if grand_last_ts is None or last_ts > grand_last_ts:
                grand_last_ts = last_ts

    actual_duration = None
    if grand_first_ts is not None and grand_last_ts is not None:
        actual_duration = grand_last_ts - grand_first_ts

    lines = []

    # ---- title ----
    title_pass = f"pass-{pass_num}" if pass_num is not None else run_id
    title_mode = f" ({mode} mode)" if info["mode"] else ""
    lines.append(f"# Multi-host chaos report: {title_pass}{title_mode}")
    lines.append("")
    lines.append(f"_Auto-generated by `scripts/chaos-multi-host/generate-report.py`._")
    lines.append("")

    # ---- 1. run summary ----
    lines.append("## Run summary")
    lines.append("")
    lines.append("| field | value |")
    lines.append("|---|---|")
    lines.append(f"| run id | `{run_id}` |")
    lines.append(f"| mode | `{mode}` |")
    lines.append(f"| hosts | {', '.join(host_labels) if host_labels else '(none)'} |")
    planned = coord["planned_duration"]
    lines.append(
        f"| planned duration | {fmt_duration(planned)} |"
    )
    lines.append(f"| actual duration (workload window) | {fmt_duration(actual_duration)} |")
    lines.append(f"| workload start (earliest snapshot) | {fmt_iso(grand_first_ts)} |")
    lines.append(f"| workload end (latest snapshot) | {fmt_iso(grand_last_ts)} |")
    if coord["coord_start"] and coord["coord_end"]:
        lines.append(
            f"| coordinator log span | {coord['coord_start']} - {coord['coord_end']} (UTC, HH:MM:SS) |"
        )
    lines.append("")

    # ---- 2. workload totals ----
    lines.append("## Workload totals")
    lines.append("")
    lines.append("| host | ok | fail | total | success rate |")
    lines.append("|---|---:|---:|---:|---:|")
    grand_ok = grand_fail = 0
    for label in host_labels:
        ok = sum(per_host[label]["counts"].values())
        fail = sum(per_host[label]["failures"].values())
        total = ok + fail
        grand_ok += ok
        grand_fail += fail
        lines.append(
            f"| `{label}` | {fmt_int(ok)} | {fmt_int(fail)} | {fmt_int(total)} | "
            f"{fmt_pct(ok, total)} |"
        )
    grand_total = grand_ok + grand_fail
    lines.append(
        f"| **aggregate** | **{fmt_int(grand_ok)}** | **{fmt_int(grand_fail)}** | "
        f"**{fmt_int(grand_total)}** | **{fmt_pct(grand_ok, grand_total)}** |"
    )
    lines.append("")

    # ---- 3. top failure reasons ----
    lines.append("## Top failure reasons")
    lines.append("")
    combined_failures = collections.Counter()
    for label in host_labels:
        combined_failures.update(per_host[label]["failures"])
    if combined_failures:
        lines.append("| failure | count |")
        lines.append("|---|---:|")
        for name, count in combined_failures.most_common(10):
            lines.append(f"| `{name}` | {fmt_int(count)} |")
    else:
        lines.append("No client-visible failures across any host.")
    lines.append("")

    # ---- 4. chaos events by kind ----
    lines.append("## Chaos events by kind")
    lines.append("")
    all_kinds = sorted({k for label in host_labels for k in per_host[label]["kinds"]})
    if not all_kinds and host_labels:
        all_kinds = []
    if host_labels and all_kinds:
        header = "| kind | " + " | ".join(f"`{label}`" for label in host_labels) + " | aggregate |"
        sep = "|---|" + "---:|" * (len(host_labels) + 1)
        lines.append(header)
        lines.append(sep)
        for kind in all_kinds:
            row = [f"`{kind}`"]
            agg = 0
            for label in host_labels:
                v = per_host[label]["kinds"].get(kind, 0)
                row.append(fmt_int(v))
                agg += v
            row.append(f"**{fmt_int(agg)}**")
            lines.append("| " + " | ".join(row) + " |")
    else:
        if host_labels:
            header = "| kind | " + " | ".join(f"`{label}`" for label in host_labels) + " | aggregate |"
            sep = "|---|" + "---:|" * (len(host_labels) + 1)
            lines.append(header)
            lines.append(sep)
            zeros = " | ".join(["0"] * len(host_labels))
            lines.append(f"| _(no events)_ | {zeros} | **0** |")
        else:
            lines.append("No hosts observed; no chaos events to report.")
    lines.append("")

    # ---- 5. per-host stability indicators ----
    lines.append("## Per-host stability indicators")
    lines.append("")
    lines.append(
        "| host | restart_failed | recovery_restart | redis_bounce | "
        "kill | restart |"
    )
    lines.append("|---|---:|---:|---:|---:|---:|")
    for label in host_labels:
        kinds = per_host[label]["kinds"]
        lines.append(
            f"| `{label}` | {fmt_int(kinds.get('restart_failed', 0))} | "
            f"{fmt_int(kinds.get('recovery_restart', 0))} | "
            f"{fmt_int(kinds.get('redis_bounce', 0))} | "
            f"{fmt_int(kinds.get('kill', 0))} | "
            f"{fmt_int(kinds.get('restart', 0))} |"
        )
    lines.append("")

    # ---- 6. P3-2.5 metrics snapshot ----
    metrics = parse_metrics_snapshot(run_dir)
    if metrics is not None:
        lines.append("## Failure-cause metrics snapshot")
        lines.append("")
        lines.append("Latest `metrics-*.json` snapshot found in run-dir.")
        lines.append("")
        lines.append("| counter | value |")
        lines.append("|---|---:|")
        # Render every numeric leaf, sorted for determinism. Object
        # values are stringified to a compact JSON blob so that
        # labelled counters (e.g. per-peer maps) stay legible.
        for key in sorted(metrics.keys()):
            value = metrics[key]
            if isinstance(value, (int, float)):
                lines.append(f"| `{key}` | {fmt_int(int(value))} |")
            else:
                blob = json.dumps(value, sort_keys=True, separators=(",", ":"))
                lines.append(f"| `{key}` | `{blob}` |")
        lines.append("")

    # ---- 7. notable timeline events ----
    lines.append("## Notable timeline events")
    lines.append("")
    notable = []
    for label in host_labels:
        for ev in per_host[label]["events"]:
            if ev.get("kind") != "restart_failed":
                continue
            detail = ev.get("detail", {})
            if not isinstance(detail, dict):
                continue
            if "tail" in detail:
                notable.append((ev.get("ts", ""), label, detail))
            if len(notable) >= 3:
                break
        if len(notable) >= 3:
            break
    if notable:
        lines.append("First three `restart_failed` events with captured `tail`:")
        lines.append("")
        for ts, label, detail in notable[:3]:
            lines.append(f"- `{ts}` on `{label}`")
            rc = detail.get("rc", "?")
            reason = detail.get("reason", "?")
            tail = detail.get("tail", "")
            lines.append(f"    - reason=`{reason}` rc=`{rc}`")
            lines.append("    - tail:")
            lines.append("      ```")
            for chunk in str(tail).split("\\n"):
                lines.append(f"      {chunk}")
            lines.append("      ```")
    else:
        lines.append("No `restart_failed` events with captured `tail` payloads.")
    lines.append("")

    # ---- 8. provenance ----
    lines.append("## Provenance")
    lines.append("")
    git_sha = detect_git_sha(run_dir)
    if git_sha:
        lines.append(f"- dynomited git sha at run-start: `{git_sha}`")
    else:
        lines.append(
            "- dynomited git sha at run-start: not recorded "
            "(no `git-sha`, `start-args:GIT_SHA=...`, or `git sha:` "
            "marker found in run-dir)."
        )
    lines.append(f"- run-dir: `{run_dir}`")
    lines.append(
        f"- generated at: {dt.datetime.now(tz=dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}"
    )
    lines.append("")

    return "\n".join(lines)


# ---------- discovery for default --run-id ----------


def discover_run_dirs(runs_dir: Path):
    """Return all run-dir paths under ``runs_dir`` matching the
    standard naming.
    """
    if not runs_dir.is_dir():
        return []
    out = []
    for sub in runs_dir.iterdir():
        if not sub.is_dir():
            continue
        info = parse_run_id(sub.name)
        if info["pass_num"] is not None or sub.name.startswith("prod-"):
            out.append(sub)
    return sorted(out)


def latest_run_dir(runs_dir: Path):
    runs = discover_run_dirs(runs_dir)
    if not runs:
        return None
    # Latest by mtime; ties broken by name.
    return max(runs, key=lambda p: (p.stat().st_mtime, p.name))


# ---------- CLI ----------


def write_report(run_dir: Path, out_path: Path) -> str:
    md = render_report(run_dir)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(md)
    return md


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="Generate a multi-host chaos report from a run-dir."
    )
    p.add_argument(
        "--run-id",
        default=None,
        help="Run id under target/chaos-multi-host/; defaults to the latest.",
    )
    p.add_argument(
        "--runs-dir",
        default=str(DEFAULT_RUNS_DIR),
        help="Override target/chaos-multi-host base.",
    )
    p.add_argument(
        "--out",
        default=None,
        help=(
            "Output markdown path; defaults to "
            "dist/chaos-reports/v0.1.0/multi-host-pass-N-<mode>.md."
        ),
    )
    p.add_argument(
        "--all-runs",
        action="store_true",
        help="Regenerate a report for every run-dir found.",
    )
    args = p.parse_args(argv)

    runs_dir = Path(args.runs_dir).resolve()

    if args.all_runs:
        runs = discover_run_dirs(runs_dir)
        if not runs:
            print(f"no runs found under {runs_dir}", file=sys.stderr)
            return 1
        for r in runs:
            out = default_out_path(r.name)
            write_report(r, out)
            print(f"wrote {out}")
        return 0

    if args.run_id:
        run_dir = runs_dir / args.run_id
    else:
        run_dir = latest_run_dir(runs_dir)
        if run_dir is None:
            print(f"no runs found under {runs_dir}", file=sys.stderr)
            return 1

    if not run_dir.is_dir():
        print(f"run dir not found: {run_dir}", file=sys.stderr)
        return 1

    out_path = Path(args.out) if args.out else default_out_path(run_dir.name)
    write_report(run_dir, out_path)
    print(f"wrote {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
