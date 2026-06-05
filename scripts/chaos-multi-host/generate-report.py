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
    r"^pass(?P<pass_num>\d+)-(?P<mode>redis|memcache|riak|combined)-"
    r"(?P<stamp>\d{8}-\d{6}Z)$"
)
SIMPLE_RUN_ID_PATTERN = re.compile(
    r"^pass(?P<pass_num>\d+)-(?P<stamp>\d{8}-\d{6}Z)$"
)

# Workload-driver API suffixes. MODE=combined launches one driver
# per co-located dynomited instance (redis, memcache, riak),
# writing ``workload-<label>-<api>.ndjson`` per instance.
# Single-driver modes keep the legacy unsuffixed
# ``workload-<label>.ndjson``. The report sums across all of a
# host's driver files for the per-host total and breaks the load
# down per API. ``redis`` covers RESP + the RediSearch FT.* /
# FT.SUG* surface (the ``ft`` / ``ftsug`` op classes); ``memcache``
# is the memcache ASCII driver; ``riak`` is the Riak PBC driver.
WORKLOAD_API_SUFFIXES = ("redis", "riak", "memcache")


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


def _split_workload_label(stem: str):
    """Split a ``workload-...`` filename stem into (label, api).

    ``stem`` is the filename without the ``.ndjson`` suffix, e.g.
    ``workload-dc-floki`` or ``workload-dc-floki-redis``. Returns
    ``(label, api)`` where ``api`` is ``None`` for the legacy
    single-driver shape or one of ``WORKLOAD_API_SUFFIXES`` for a
    per-instance combined driver file. Labels themselves contain
    hyphens (``dc-floki``), so we strip only a trailing
    ``-<known-api>`` rather than splitting on the last hyphen.
    """
    base = stem[len("workload-"):]
    for api in WORKLOAD_API_SUFFIXES:
        if base.endswith("-" + api):
            return base[: -(len(api) + 1)], api
    return base, None


def discover_host_dirs(run_dir: Path) -> dict:
    """Map dc-label -> host-logs directory.

    A host's logs live in ``<host>-logs/`` next to ``coordinator.log``.
    The DC label is reconstructed from the workload ndjson filename
    (``workload-dc-<label>.ndjson`` or, for combined runs, a
    per-instance ``workload-dc-<label>-<api>.ndjson``) so renamed
    host dirs do not desync the report.
    """
    hosts = {}
    for sub in sorted(run_dir.iterdir()):
        if not (sub.is_dir() and sub.name.endswith("-logs")):
            continue
        label = None
        for entry in sorted(sub.iterdir()):
            if entry.name.startswith("workload-dc-") and entry.suffix == ".ndjson":
                label, _api = _split_workload_label(entry.stem)
                break
        if label is None:
            label = "dc-" + sub.name[: -len("-logs")]
        hosts[label] = sub
    return hosts


def discover_workload_files(host_dir: Path, label: str):
    """Return ``[(api, path), ...]`` for a host's workload files.

    ``api`` is ``None`` for the legacy single-driver
    ``workload-<label>.ndjson`` file, or an API name for each
    per-instance combined driver file present
    (``workload-<label>-<api>.ndjson``). Only existing files are
    returned. A non-combined run yields a single ``(None, path)``
    entry, preserving backward compatibility.
    """
    out = []
    plain = host_dir / f"workload-{label}.ndjson"
    if plain.exists():
        out.append((None, plain))
    for api in WORKLOAD_API_SUFFIXES:
        p = host_dir / f"workload-{label}-{api}.ndjson"
        if p.exists():
            out.append((api, p))
    return out


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
    retries = collections.Counter()
    snapshots = []
    first_ts = last_ts = None
    if not path.exists():
        return counts, failures, retries, snapshots, first_ts, last_ts
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
            for k, v in d.get("retries", {}).items():
                retries[k] += v
            snapshots.append(d)
            ts = d.get("ts")
            if isinstance(ts, (int, float)):
                if first_ts is None:
                    first_ts = ts
                last_ts = ts
    return counts, failures, retries, snapshots, first_ts, last_ts


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


# ---------- restart_failed classification (P3-1.3) ----------

# Regex set for classifying captured restart_failed_detail tails.
# The classifier walks the matchers in declaration order; the
# first hit wins. Each regex is anchored on a single line and
# applied with re.IGNORECASE plus re.MULTILINE so it can hit
# anywhere in the multi-line tail.
#
# port-collision        - dynomited or its backend tried to bind
#                         a port already held by another
#                         process. Common during chaos because
#                         the kernel can lag in reaping a
#                         SIGKILL'd dynomited and its TCP
#                         listener.
# backend-down          - the redis or memcached backend that
#                         dynomited's data_store points at was
#                         not reachable when start-host.sh
#                         probed it. The chaos-injector's
#                         redis-bounce racy with the next start.
# crash-mid-startup     - dynomited spawned, opened files,
#                         emitted a fatal log line, and exited
#                         non-zero. start-host.sh ALSO emits
#                         the literal phrase "CRASHED" in this
#                         case.
# unknown               - everything else; pinpoints residual
#                         categories the operator may need to
#                         add to this list.
RESTART_FAILED_CLASS_PATTERNS = [
    ("port-collision", re.compile(
        r"(?:address (?:already )?in use"
        r"|already in use"
        r"|EADDRINUSE"
        r"|bind:\s*Address already in use"
        r"|cannot bind to\s*[^\n]+already)",
        re.IGNORECASE | re.MULTILINE,
    )),
    ("backend-down", re.compile(
        r"(?:backend on \d+ did not respond"
        r"|connection refused"
        r"|ECONNREFUSED"
        r"|protocol probe within"
        r"|backend probe failed"
        r"|redis-server.*not on PATH"
        r"|memcached.*not on PATH)",
        re.IGNORECASE | re.MULTILINE,
    )),
    ("crash-mid-startup", re.compile(
        r"(?:CRASHED mid-startup"
        r"|panicked at"
        r"|fatal runtime error"
        r"|thread '[^']*' panicked"
        r"|RUST_BACKTRACE"
        r"|stack backtrace:"
        r"|signal: 11, SIGSEGV"
        r"|fatal:\s*[^\n]+\bdynomited\b)",
        re.IGNORECASE | re.MULTILINE,
    )),
]


def chaos_restart_failed_class(stderr_tail: str, log_tail: str) -> str:
    """Classify a restart_failed_detail event into one of:

    ``port-collision``, ``backend-down``, ``crash-mid-startup``,
    or ``unknown``.

    Walks ``RESTART_FAILED_CLASS_PATTERNS`` in order against the
    concatenated stderr+log tail. The first regex that matches
    decides the class; ``unknown`` is the residual bucket.
    Inputs may be ``None`` or the empty string; both are handled
    as if no diagnostic was captured.
    """
    blob = "\n".join(s for s in (stderr_tail, log_tail) if s)
    if not blob:
        return "unknown"
    for label, pat in RESTART_FAILED_CLASS_PATTERNS:
        if pat.search(blob):
            return label
    return "unknown"


def _b64_decode_safe(blob) -> str:
    """Best-effort base64 decode.

    The chaos-injector emits base64-encoded tails; corrupt or
    missing inputs decode to the empty string so the classifier
    routes them to ``unknown`` rather than blowing up the report.
    """
    if not isinstance(blob, str) or not blob:
        return ""
    try:
        import base64
        return base64.b64decode(blob, validate=False).decode(
            "utf-8", errors="replace"
        )
    except Exception:
        return ""


def extract_restart_failed_classes(events):
    """Iterate ``restart_failed_detail`` events and return a Counter
    of class labels. Events without the new shape are ignored.
    """
    counter = collections.Counter()
    for ev in events:
        if not isinstance(ev, dict):
            continue
        ev_type = ev.get("event") or ev.get("kind")
        if ev_type != "restart_failed_detail":
            continue
        stderr_tail = _b64_decode_safe(ev.get("stderr_tail", ""))
        log_tail = _b64_decode_safe(ev.get("log_tail", ""))
        counter[chaos_restart_failed_class(stderr_tail, log_tail)] += 1
    return counter


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

    # Aggregate per-host workload + chaos data once. Each host may
    # have one workload file (legacy single-driver modes) or
    # several (MODE=combined: one per co-located dynomited
    # instance -- redis, memcache, riak). We sum counts/failures
    # across all of a host's driver files for the per-host total
    # and keep a per-API breakdown for the combined section.
    per_host = {}
    grand_first_ts = None
    grand_last_ts = None
    for label in host_labels:
        cpath = host_dirs[label] / f"chaos-events-{label}.ndjson"
        counts = collections.Counter()
        failures = collections.Counter()
        retries = collections.Counter()
        snapshots = []
        first_ts = last_ts = None
        by_api = {}
        for api, wpath in discover_workload_files(host_dirs[label], label):
            (a_counts, a_failures, a_retries, a_snapshots,
             a_first, a_last) = parse_workload_ndjson(wpath)
            counts.update(a_counts)
            failures.update(a_failures)
            retries.update(a_retries)
            snapshots.extend(a_snapshots)
            if a_first is not None:
                if first_ts is None or a_first < first_ts:
                    first_ts = a_first
            if a_last is not None:
                if last_ts is None or a_last > last_ts:
                    last_ts = a_last
            by_api[api] = {
                "counts": a_counts,
                "failures": a_failures,
                "retries": a_retries,
            }
        kinds, events = parse_chaos_events(cpath)
        per_host[label] = {
            "counts": counts,
            "failures": failures,
            "retries": retries,
            "snapshots": snapshots,
            "first_ts": first_ts,
            "last_ts": last_ts,
            "kinds": kinds,
            "events": events,
            "by_api": by_api,
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

    # ---- 2b. per-API breakdown (combined runs) ----
    # MODE=combined runs three independent dynomited instances per
    # host (redis, memcache, riak), each on its own port band and
    # driven by its own workload. When per-API driver files are
    # present we break the per-host load down by instance so the
    # operator can see, e.g., that the Redis, memcache, and Riak
    # PBC surfaces all stayed healthy. The ft / ftsug columns
    # surface the RediSearch FT.* / FT.SUG* sub-classes that
    # already live inside the redis driver's counts. The section
    # is omitted entirely for non-combined runs (only the legacy
    # unsuffixed file present).
    any_api = any(
        any(api is not None for api in per_host[label]["by_api"])
        for label in host_labels
    )
    if any_api:
        def _ok(ctr):
            return sum(ctr.values())

        def _fail(ctr):
            return sum(ctr.values())

        def _class_ok(ctr, cls):
            # counts keys are "<class>/<op>"; match the class head.
            return sum(
                v for k, v in ctr.items() if k.split("/", 1)[0] == cls
            )

        lines.append("## Per-API breakdown")
        lines.append("")
        lines.append(
            "Per-host load split by co-located dynomited instance "
            "(redis / memcache / riak), each on its own port band. "
            "`ft` / `ftsug` are the RediSearch sub-classes inside the "
            "redis driver's counts."
        )
        lines.append("")
        lines.append(
            "| host | redis ok | redis fail | memcache ok | memcache fail | "
            "riak ok | riak fail | ft ok | ftsug ok |"
        )
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
        agg = {
            "redis_ok": 0, "redis_fail": 0, "memcache_ok": 0,
            "memcache_fail": 0, "riak_ok": 0, "riak_fail": 0,
            "ft_ok": 0, "ftsug_ok": 0,
        }
        empty_api = {"counts": collections.Counter(),
                     "failures": collections.Counter()}
        for label in host_labels:
            by_api = per_host[label]["by_api"]
            redis = by_api.get("redis", empty_api)
            memcache = by_api.get("memcache", empty_api)
            riak = by_api.get("riak", empty_api)
            redis_ok = _ok(redis["counts"])
            redis_fail = _fail(redis["failures"])
            memcache_ok = _ok(memcache["counts"])
            memcache_fail = _fail(memcache["failures"])
            riak_ok = _ok(riak["counts"])
            riak_fail = _fail(riak["failures"])
            ft_ok = _class_ok(redis["counts"], "ft")
            ftsug_ok = _class_ok(redis["counts"], "ftsug")
            agg["redis_ok"] += redis_ok
            agg["redis_fail"] += redis_fail
            agg["memcache_ok"] += memcache_ok
            agg["memcache_fail"] += memcache_fail
            agg["riak_ok"] += riak_ok
            agg["riak_fail"] += riak_fail
            agg["ft_ok"] += ft_ok
            agg["ftsug_ok"] += ftsug_ok
            lines.append(
                f"| `{label}` | {fmt_int(redis_ok)} | {fmt_int(redis_fail)} | "
                f"{fmt_int(memcache_ok)} | {fmt_int(memcache_fail)} | "
                f"{fmt_int(riak_ok)} | {fmt_int(riak_fail)} | "
                f"{fmt_int(ft_ok)} | {fmt_int(ftsug_ok)} |"
            )
        lines.append(
            f"| **aggregate** | **{fmt_int(agg['redis_ok'])}** | "
            f"**{fmt_int(agg['redis_fail'])}** | **{fmt_int(agg['memcache_ok'])}** | "
            f"**{fmt_int(agg['memcache_fail'])}** | **{fmt_int(agg['riak_ok'])}** | "
            f"**{fmt_int(agg['riak_fail'])}** | **{fmt_int(agg['ft_ok'])}** | "
            f"**{fmt_int(agg['ftsug_ok'])}** |"
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
    # P3-3.9 phase 5: the chaos-injector's process faults can
    # now hit BOTH the Rust dynomited and the C `dynomite`
    # reference proxy in lockstep when MODE=differential is
    # active. Paired `_c` events (`fault_kill_c`,
    # `recovery_restart_c`, `fault_pause_start_c`, and
    # `fault_pause_end_c`) record which proxy was hit. The
    # per-host stability table below adds two columns next to
    # the existing `kill` and `recovery_restart` counters so
    # the report attributes faults to the correct binary.
    # The columns are zero on non-differential runs.
    lines.append(
        "| host | restart_failed | recovery_restart | recovery_restart_c | "
        "redis_bounce | kill | fault_kill_c | restart |"
    )
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for label in host_labels:
        kinds = per_host[label]["kinds"]
        lines.append(
            f"| `{label}` | {fmt_int(kinds.get('restart_failed', 0))} | "
            f"{fmt_int(kinds.get('recovery_restart', 0))} | "
            f"{fmt_int(kinds.get('recovery_restart_c', 0))} | "
            f"{fmt_int(kinds.get('redis_bounce', 0))} | "
            f"{fmt_int(kinds.get('kill', 0))} | "
            f"{fmt_int(kinds.get('fault_kill_c', 0))} | "
            f"{fmt_int(kinds.get('restart', 0))} |"
        )
    lines.append("")

    # ---- 5b. P3-1.3 restart_failed_detail per-class breakdown ----
    classes_by_host = {}
    grand_classes = collections.Counter()
    any_classified = False
    for label in host_labels:
        ctr = extract_restart_failed_classes(per_host[label]["events"])
        classes_by_host[label] = ctr
        grand_classes.update(ctr)
        if sum(ctr.values()) > 0:
            any_classified = True
    if any_classified:
        lines.append("### Restart-failed class breakdown (P3-1.3)")
        lines.append("")
        lines.append(
            "Classes are derived from the `restart_failed_detail` "
            "events' base64-encoded `stderr_tail` + `log_tail` via "
            "`chaos_restart_failed_class`. See the regex block in "
            "`scripts/chaos-multi-host/generate-report.py` for the "
            "matchers."
        )
        lines.append("")
        class_columns = [
            "port-collision", "backend-down", "crash-mid-startup", "unknown",
        ]
        header = "| host | " + " | ".join(class_columns) + " | total |"
        sep = "|---|" + "---:|" * (len(class_columns) + 1)
        lines.append(header)
        lines.append(sep)
        for label in host_labels:
            ctr = classes_by_host[label]
            row = [f"`{label}`"]
            for c in class_columns:
                row.append(fmt_int(ctr.get(c, 0)))
            row.append(fmt_int(sum(ctr.values())))
            lines.append("| " + " | ".join(row) + " |")
        agg_row = ["**aggregate**"]
        for c in class_columns:
            agg_row.append(f"**{fmt_int(grand_classes.get(c, 0))}**")
        agg_row.append(f"**{fmt_int(sum(grand_classes.values()))}**")
        lines.append("| " + " | ".join(agg_row) + " |")
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
