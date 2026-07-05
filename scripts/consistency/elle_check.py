#!/usr/bin/env python3
"""Elle-style consistency checker for list-append histories.

Consumes the JSON-lines history emitted by
`scripts/consistency/txn_history_workload.py` (one object per line:
`{"index","process","type":"invoke|ok|fail|info","time_ns","value"}`
where each micro-op in `value` is `["append", k, v]` or
`["r", k, list-or-null]`).

It reconstructs, per key, the total order of appended values that
committed writes imply, then checks the anomaly classes it covers.
This is a deliberately-scoped subset of Jepsen's Elle (no mature
Rust Elle exists; see docs/journal/2026-06-19-consistency-
verification.md); the classes below are the ones a list-append
history over an AP store must satisfy, and the checker reports
exactly which it verified.

Anomaly classes covered (per key, over `:ok` transactions only --
`:fail` did not commit, `:info` is indeterminate and skipped):

  * DUP  (duplicate-append): a committed write claims to append a
    value that already appears -- appends are globally unique, so a
    duplicate means a lost/duplicated write. VIOLATION.
  * G1a  (aborted read / dirty read): a read observes a value whose
    only writer's transaction did NOT commit (:fail). VIOLATION.
  * NONMONO (per-key non-monotonic read within a process): a process
    reads a key's list, later reads the same key, and the later read
    is not a prefix-consistent extension (a value disappeared).
    VIOLATION under the per-key total-order the appends define.
  * CYCLE (write-write / write-read dependency cycle): build the
    dependency graph over transactions from the observed per-key
    append orders (ww: earlier-append -> later-append; wr:
    writer -> reader-that-saw-it) and detect a cycle, which implies
    a non-serializable schedule. Reported (G-single / G2-ish).

Exit non-zero if any covered anomaly is found.
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict


def load(path):
    recs = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if line:
                recs.append(json.loads(line))
    return recs


def check(recs):
    anomalies = []
    # value -> writer txn index (the committed :ok that appended it)
    value_writer = {}
    # value -> did its writing txn commit?
    value_committed = {}
    # per key: list of (txn_index, appended_value) in commit order seen
    # per process: last observed list per key (for monotonicity)
    proc_last_read = defaultdict(dict)   # process -> {key: observed_list}
    # dependency edges txn -> set(txn) for cycle detection
    edges = defaultdict(set)

    # First pass: catalogue every appended value and whether it committed.
    for r in recs:
        typ = r["type"]
        for op in r["value"]:
            f = op[0]
            if f == "append":
                _, k, v = op
                if typ in ("invoke", "ok"):
                    value_writer[v] = r["index"]
                if typ == "ok":
                    value_committed[v] = True
                elif typ == "fail":
                    value_committed.setdefault(v, False)

    seen_values_per_key = defaultdict(set)

    # Second pass: check reads + build dependency edges over committed txns.
    for r in recs:
        if r["type"] != "ok":
            continue
        txn = r["index"]
        proc = r["process"]
        for op in r["value"]:
            f = op[0]
            if f == "append":
                _, k, v = op
                # DUP: unique value appended twice.
                if v in seen_values_per_key[k]:
                    anomalies.append(("DUP", f"value {v} appended twice on key {k}"))
                seen_values_per_key[k].add(v)
            elif f == "r":
                _, k, observed = op
                if observed is None:
                    continue
                # DUP within a read.
                if len(observed) != len(set(observed)):
                    anomalies.append(("DUP", f"read of key {k} in txn {txn} has duplicate values: {observed}"))
                for v in observed:
                    # G1a: read a value whose writer aborted.
                    if value_committed.get(v) is False:
                        anomalies.append(("G1a", f"txn {txn} read value {v} on key {k} from an aborted write"))
                    # wr edge: writer(v) -> this reader.
                    w = value_writer.get(v)
                    if w is not None and w != txn:
                        edges[w].add(txn)
                # ww edges: consecutive observed values are append-ordered.
                for a, b in zip(observed, observed[1:]):
                    wa, wb = value_writer.get(a), value_writer.get(b)
                    if wa is not None and wb is not None and wa != wb:
                        edges[wa].add(wb)
                # NONMONO: per-process, a later read of the same key must
                # be a monotonic extension of the earlier one -- values only
                # ever get appended, so within a single process (its own
                # session) a value it already observed must not disappear,
                # and the earlier list must be a prefix of the later.
                prev = proc_last_read[proc].get(k)
                if prev is not None:
                    if len(observed) < len(prev) or observed[: len(prev)] != prev:
                        anomalies.append(("NONMONO", f"process {proc} key {k}: {prev} then {observed} (read is not a monotonic extension)"))
                # Track the longest list this process has observed for the
                # key so a later shorter read is caught even if reads
                # interleave out of length order.
                if prev is None or len(observed) >= len(prev):
                    proc_last_read[proc][k] = observed

    # CYCLE: DFS cycle detection over the dependency graph.
    WHITE, GRAY, BLACK = 0, 1, 2
    color = defaultdict(int)
    cyc = []

    def dfs(u, stack):
        color[u] = GRAY
        stack.append(u)
        for v in edges[u]:
            if color[v] == GRAY:
                i = stack.index(v)
                cyc.append(stack[i:] + [v])
                return True
            if color[v] == WHITE and dfs(v, stack):
                return True
        stack.pop()
        color[u] = BLACK
        return False

    for node in list(edges.keys()):
        if color[node] == WHITE:
            if dfs(node, []):
                break
    if cyc:
        anomalies.append(("CYCLE", f"dependency cycle over txns: {cyc[0][:8]}"))

    return anomalies


def main():
    if len(sys.argv) < 2:
        print("usage: elle_check.py <history.jsonl> [more.jsonl ...]", file=sys.stderr)
        return 2
    all_recs = []
    for p in sys.argv[1:]:
        all_recs.extend(load(p))
    # merge multi-process histories: re-index by time so the per-key
    # order reflects real commit time across processes.
    all_recs.sort(key=lambda r: r.get("time_ns", 0))
    for i, r in enumerate(all_recs):
        r["index"] = i
    anomalies = check(all_recs)
    total = len(all_recs)
    oks = sum(1 for r in all_recs if r["type"] == "ok")
    print(f"elle-check: {total} history records ({oks} committed txns) "
          f"across {len(sys.argv)-1} file(s)")
    print("classes checked: DUP, G1a (aborted read), NONMONO (per-process "
          "monotonic read), CYCLE (ww/wr dependency cycle)")
    if not anomalies:
        print("elle-check: PASS -- no anomalies of the covered classes")
        return 0
    print(f"elle-check: FAIL -- {len(anomalies)} anomaly/anomalies:")
    seen = set()
    for cls, detail in anomalies:
        key = (cls, detail[:60])
        if key in seen:
            continue
        seen.add(key)
        print(f"  [{cls}] {detail}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
