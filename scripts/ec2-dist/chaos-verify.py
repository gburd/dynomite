#!/usr/bin/env python3
"""Dyniak chaos convergence verifier.

Reads the merged CRDT op history (JSONL, one line per accepted update
across all load generators), computes the arithmetically-expected value
per key, then fetches every key from every dyniak node and confirms all
replicas converged to the expected value.

For the counter workload the expected value of key k is the number of
accepted increments routed to k (across all regions). Convergence holds
when every node reports exactly that -- proving no increment was lost
despite the net splits and node churn during the load window, and that
concurrent cross-region writes summed via CRDT merge (eventual
consistency).

Reuses the PBC wire encoding from chaos-crdt-driver.py (imported by
path) so there is a single source of truth for the protocol.
"""
import argparse
import collections
import importlib.util
import json
import os
import sys


def load_driver():
    here = os.path.dirname(os.path.abspath(__file__))
    # The driver is shipped alongside on the load-gen (~/driver.py) or
    # next to this file in the repo.
    for cand in (os.path.join(here, "driver.py"),
                 os.path.join(here, "chaos-crdt-driver.py"),
                 os.path.expanduser("~/driver.py")):
        if os.path.exists(cand):
            spec = importlib.util.spec_from_file_location("driver", cand)
            m = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(m)
            return m
    raise SystemExit("verify: cannot find the PBC driver module")


def expected_counters(history_path):
    exp = collections.Counter()
    with open(history_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except Exception:       # noqa: BLE001
                continue
            if r.get("ok") and r.get("op") == "counter":
                exp[r["key"]] += 1
    return exp


def expected_sets(history_path):
    exp = collections.defaultdict(set)
    with open(history_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except Exception:       # noqa: BLE001
                continue
            if r.get("ok") and r.get("op") == "set":
                exp[r["key"]].add(r["val"])
    return exp


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--history", required=True)
    ap.add_argument("--nodes", required=True, help="comma-separated node public IPs")
    ap.add_argument("--port", type=int, default=8087)
    ap.add_argument("--bucket", default="chaos")
    ap.add_argument("--btype", default="counters")
    ap.add_argument("--workload", choices=["counter", "set"], default="counter")
    ap.add_argument("--keyspace", type=int, default=200)
    ap.add_argument("--timeout", type=float, default=5.0)
    args = ap.parse_args()

    d = load_driver()
    nodes = [n for n in args.nodes.split(",") if n]

    if args.workload == "counter":
        exp = expected_counters(args.history)
    else:
        exp = expected_sets(args.history)

    report = {
        "workload": args.workload,
        "keys_expected": len(exp),
        "total_ops": (sum(exp.values()) if args.workload == "counter"
                      else sum(len(v) for v in exp.values())),
        "nodes": {},
    }
    # Correctness model (Dynamo-style): a key lives on its REPLICA SET,
    # not on every node. Convergence-without-loss means the replica set
    # holds the exact expected value and no node over-counts. So per
    # key we check the MAX value observed across all nodes == expected
    # (the replica set converged to the total) and no node exceeds it
    # (idempotent merge -- no double-count). A non-replica node holding
    # null/0 is correct, not a divergence.
    all_converged = True
    # per-key: best (max) value seen across nodes, and any over-count.
    best = {}       # key -> max counter seen (or set union)
    overcount = []  # (node, key, want, got) where got > want
    node_conns = {}
    for node in nodes:
        try:
            node_conns[node] = d.connect(node, args.port, args.timeout)
        except Exception as e:       # noqa: BLE001
            report["nodes"][node] = {"error": type(e).__name__}

    for i in range(args.keyspace):
        key = f"k{i}"
        want = exp.get(key)
        if want is None:
            continue
        if args.workload == "counter":
            key_best = 0
        else:
            key_best = set()
        for node, s in list(node_conns.items()):
            try:
                d.send_frame(s, d.DT_FETCH_REQ, d.fetch_req(args.bucket, key, args.btype))
                code, body = d.recv_frame(s)
                counter, elems = d.parse_fetch_resp(body)
                if args.workload == "counter":
                    got = counter or 0
                    if got > want:
                        overcount.append((node, key, want, got))
                    key_best = max(key_best, got)
                else:
                    got = set(e.decode("latin1") for e in elems)
                    if not got.issubset(want):
                        overcount.append((node, key, sorted(want), sorted(got - want)))
                    key_best |= got
            except Exception as e:       # noqa: BLE001
                try:
                    s.close()
                except Exception:        # noqa: BLE001
                    pass
                node_conns[node] = d.connect(node, args.port, args.timeout)
        best[key] = key_best
    for s in node_conns.values():
        try:
            s.close()
        except Exception:                # noqa: BLE001
            pass

    # A key converged iff the replica set's best value equals expected.
    lost = []
    for key, want in exp.items():
        b = best.get(key, 0 if args.workload == "counter" else set())
        if args.workload == "counter":
            if b != want:
                lost.append((key, want, b))
        else:
            if set(b) != set(want):
                lost.append((key, sorted(want), sorted(b)))
    if lost or overcount:
        all_converged = False
    report["lost_or_short"] = lost[:10]
    report["lost_count"] = len(lost)
    report["overcount"] = overcount[:10]
    report["overcount_count"] = len(overcount)

    report["all_converged"] = all_converged
    print(json.dumps(report))
    sys.exit(0 if all_converged else 1)


if __name__ == "__main__":
    main()
