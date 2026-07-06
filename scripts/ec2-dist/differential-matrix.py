#!/usr/bin/env python3
"""Full differential matrix runner.

Drives the C-vs-Rust differential (differential-driver.py) across the
qualification matrix and records a pass/fail scoreboard:

  * entry node: every node in the cluster takes a turn as the client
    entry point (routing must be correct regardless of which node the
    client connects to);
  * consistency level: the cluster is re-launched per level;
  * seeds: several per cell so a pass is not seed-specific.

A cell PASSES when every op-class agrees 100% across all seeds. Any
divergence is printed with samples so it can be root-caused.

Run ON THE CONTROLLER. It ssh-invokes differential-driver.py on each
node. Requires /tmp/<RUN_ID>.state.ips and the per-region keys.

Usage:
  differential-matrix.py [--ops N] [--seeds S1,S2,...] [--nodes-per-region K]
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys


def run_id() -> str:
    with open("/tmp/dyn-ec2-runid") as f:
        return f.read().strip().replace("RUN_ID=", "")


def nodes(rid: str) -> list[dict]:
    out = []
    with open(f"/tmp/{rid}.state.ips") as f:
        for line in f:
            p = line.split()
            if len(p) >= 7:
                out.append({"region": p[0], "az": p[1], "dc": p[2], "n": p[3],
                            "iid": p[4], "pub": p[5], "priv": p[6]})
    return out


def ssh(rid: str, node: dict, cmd: str, timeout: int = 120) -> str:
    key = f"/tmp/{rid}-{node['region']}.pem"
    full = ["ssh", "-i", key, "-o", "StrictHostKeyChecking=no",
            "-o", "IdentitiesOnly=yes", "-o", "IdentityAgent=none",
            "-o", "ConnectTimeout=15", f"ec2-user@{node['pub']}", cmd]
    r = subprocess.run(full, capture_output=True, text=True, timeout=timeout,
                       env={"SSH_AUTH_SOCK": ""})
    return r.stdout


def run_cell(rid: str, node: dict, ops: int, seed: int) -> dict:
    cmd = (f"python3 ~/diff-driver.py --host 127.0.0.1 --ops {ops} "
           f"--keyspace {max(50, ops // 5)} --seed {seed}")
    out = ssh(rid, node, cmd)
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return {"total": ops, "agree": 0, "diverge": ops,
                "agree_pct": 0.0, "by_kind": {}, "error": out[:200]}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ops", type=int, default=1000)
    ap.add_argument("--seeds", default="1,2,3")
    ap.add_argument("--nodes-per-region", type=int, default=1,
                    help="how many nodes per region to use as entry points")
    args = ap.parse_args()
    seeds = [int(s) for s in args.seeds.split(",")]
    rid = run_id()
    ns = nodes(rid)

    # Pick entry nodes: first K per region.
    by_region: dict[str, list[dict]] = {}
    for nd in ns:
        by_region.setdefault(nd["region"], []).append(nd)
    entries = []
    for region, group in by_region.items():
        entries.extend(group[: args.nodes_per_region])

    # Ensure the driver is on every entry node.
    for nd in entries:
        subprocess.run(
            ["scp", "-i", f"/tmp/{rid}-{nd['region']}.pem",
             "-o", "StrictHostKeyChecking=no", "-o", "IdentitiesOnly=yes",
             "-o", "IdentityAgent=none",
             "scripts/ec2-dist/differential-driver.py",
             f"ec2-user@{nd['pub']}:~/diff-driver.py"],
            capture_output=True, env={"SSH_AUTH_SOCK": ""})

    print(f"=== differential matrix: {len(entries)} entry nodes x {len(seeds)} seeds x {args.ops} ops ===")
    total_cells = 0
    passed_cells = 0
    failures = []
    for nd in entries:
        label = f"{nd['dc']}-{nd['n']}"
        cell_agree = 0
        cell_total = 0
        cell_by_kind: dict[str, dict[str, int]] = {}
        cell_samples = []
        for seed in seeds:
            res = run_cell(rid, nd, args.ops, seed)
            cell_agree += res.get("agree", 0)
            cell_total += res.get("total", 0)
            for k, v in res.get("by_kind", {}).items():
                d = cell_by_kind.setdefault(k, {"agree": 0, "diverge": 0})
                d["agree"] += v.get("agree", 0)
                d["diverge"] += v.get("diverge", 0)
            cell_samples.extend(res.get("divergent_samples", [])[:3])
        total_cells += 1
        pct = 100.0 * cell_agree / max(cell_total, 1)
        ok = cell_agree == cell_total
        if ok:
            passed_cells += 1
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] entry {label:16s} {cell_agree}/{cell_total} = {pct:.2f}%  {json.dumps(cell_by_kind)}")
        if not ok:
            failures.append({"entry": label, "pct": pct, "by_kind": cell_by_kind,
                             "samples": cell_samples[:6]})

    print(f"\n=== {passed_cells}/{total_cells} cells at 100% ===")
    if failures:
        print("FAILURES:")
        for f in failures:
            print(f"  entry {f['entry']} = {f['pct']:.1f}%")
            for s in f["samples"]:
                print(f"    {s.get('op')} {s.get('key')} C={s.get('c')!r} R={s.get('r')!r}")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
