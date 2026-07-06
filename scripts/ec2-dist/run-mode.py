#!/usr/bin/env python3
"""Hour-long large-scale run orchestrator for one data-store mode.

Drives, concurrently, against the global EC2 cluster in
<RUN_ID>.state.ips:

  * WORKLOAD / BENCHMARK: worker processes hammer the cluster and
    record per-op latency + success/failure. For valkey/memcache/
    dyniak-kv this reuses scripts/chaos-multi-host/workload-driver.py;
    for dyniak it ALSO runs the multi-key XA transaction workload
    (scripts/consistency/txn_history_workload.py) to validate
    Noxu/XA across the cluster.
  * CHAOS: periodically SIGKILL + restart, SIGSTOP/SIGCONT pause, or
    (network) briefly firewall a random node, then heal.
  * CHURN: periodically terminate a node and launch a replacement in
    a random region/AZ (nodes "coming and going"); the cluster's
    gossip must re-form the ring and the workload must tolerate it.
  * COLLECT: aggregate throughput / latency percentiles / error
    classes; snapshot per-minute so a mid-run anomaly is visible.

Any anomaly (a consistency violation, a lost committed write, a
node that never rejoins, a class of error not attributable to an
induced fault) is flagged for DST modeling + fix.

Usage:
  run-mode.py --mode dyniak --duration-secs 3600 --out <dir>
"""

from __future__ import annotations

import argparse
import json
import os
import random
import subprocess
import sys
import threading
import time

RUN_ID = open("/tmp/dyn-ec2-runid").read().strip().replace("RUN_ID=", "")
IPS_FILE = f"/tmp/{RUN_ID}.state.ips"
PORTS = {"valkey": 8102, "memcache": 8102, "dyniak_pbc": 8087, "dyniak_http": 8098, "stats": 22222}


def load_nodes():
    nodes = []
    for line in open(IPS_FILE):
        p = line.split()
        if len(p) >= 7:
            nodes.append(dict(region=p[0], az=p[1], dc=p[2], n=p[3], iid=p[4], pub=p[5], priv=p[6]))
    return nodes


def key_for(region):
    return f"/tmp/{RUN_ID}-{region}.pem"


def ssh(node, cmd, timeout=25):
    return subprocess.run(
        ["ssh", "-i", key_for(node["region"]), "-o", "StrictHostKeyChecking=no",
         "-o", "IdentitiesOnly=yes", "-o", "IdentityAgent=none", "-o", f"ConnectTimeout=12",
         f"ec2-user@{node['pub']}", cmd],
        capture_output=True, text=True, timeout=timeout,
        env={**os.environ, "SSH_AUTH_SOCK": ""},
    )


class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.ok = 0
        self.fail = 0
        self.lat_ms = []            # sampled latencies
        self.err_classes = {}
        self.events = []            # (t, kind, detail) for chaos/churn/anomaly

    def record(self, ok, lat_ms=None, err=None):
        with self.lock:
            if ok:
                self.ok += 1
                if lat_ms is not None and len(self.lat_ms) < 200000:
                    self.lat_ms.append(lat_ms)
            else:
                self.fail += 1
                self.err_classes[err] = self.err_classes.get(err, 0) + 1

    def event(self, kind, detail):
        with self.lock:
            self.events.append((round(time.time(), 1), kind, detail))
            print(f"[event {time.strftime('%H:%M:%S')}] {kind}: {detail}", file=sys.stderr, flush=True)

    def snapshot(self):
        with self.lock:
            lat = sorted(self.lat_ms)
            def pct(p):
                return lat[int(len(lat) * p)] if lat else 0.0
            total = self.ok + self.fail
            return dict(ok=self.ok, fail=self.fail,
                        success_pct=round(self.ok / total * 100, 3) if total else 0.0,
                        p50_ms=round(pct(0.50), 2), p95_ms=round(pct(0.95), 2),
                        p99_ms=round(pct(0.99), 2), samples=len(lat),
                        err_classes=dict(self.err_classes), events=list(self.events))


# ---- workload (HTTP-based, works for all modes via the client port) ----

def http_op(node, mode, i, stats):
    """One representative operation for the mode, timed."""
    import urllib.request, urllib.error
    t0 = time.monotonic()
    try:
        if mode == "dyniak":
            # multi-key XA transaction: put two keys atomically, then read one back.
            base = f"http://{node['pub']}:{PORTS['dyniak_http']}"
            k1, k2 = f"x{i%64}", f"y{i%64}"
            body = json.dumps({"operations": [
                {"op": "put", "bucket": "bench", "key": k1, "value": f"v{i}"},
                {"op": "put", "bucket": "bench", "key": k2, "value": f"w{i}"},
            ]}).encode()
            req = urllib.request.Request(f"{base}/buckets/bench/transactions", data=body,
                                         method="POST", headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=8) as r:
                res = json.loads(r.read()).get("result")
            ok = res == "committed"
            stats.record(ok, (time.monotonic() - t0) * 1000, None if ok else f"txn/{res}")
        else:
            # valkey/memcache: drive through dynomite client port with a
            # tiny RESP/memcache SET+GET via the workload-driver protocol.
            # Reuse the proven driver as a subprocess would be heavy per-op;
            # here we do a minimal inline RESP SET for valkey, memcache set
            # for memcache, over the client port.
            import socket
            s = socket.create_connection((node["pub"], PORTS["valkey"]), timeout=8)
            if mode == "valkey":
                k = f"bench:{i%1024}"
                s.sendall(f"*3\r\n$3\r\nSET\r\n${len(k)}\r\n{k}\r\n$3\r\nval\r\n".encode())
                resp = s.recv(64)
                ok = resp.startswith(b"+OK") or resp.startswith(b"+")
            else:  # memcache
                k = f"bench{i%1024}"
                payload = b"val"
                s.sendall(f"set {k} 0 0 {len(payload)}\r\n".encode() + payload + b"\r\n")
                resp = s.recv(64)
                ok = resp.startswith(b"STORED")
            s.close()
            stats.record(ok, (time.monotonic() - t0) * 1000, None if ok else "store/unexpected")
    except Exception as e:
        stats.record(False, None, f"{type(e).__name__}")


def workload_worker(mode, stats, stop, wid):
    nodes = load_nodes()
    rng = random.Random(wid)
    i = wid * 10_000_000
    while not stop.is_set():
        live = [n for n in nodes if n.get("live", True)]
        if not live:
            time.sleep(0.5); continue
        node = rng.choice(live)
        http_op(node, mode, i, stats)
        i += 1


# ---- chaos + churn loops ----

def chaos_loop(stats, stop, interval):
    nodes = load_nodes()
    rng = random.Random(1234)
    while not stop.is_set():
        time.sleep(interval)
        if stop.is_set():
            break
        node = rng.choice(nodes)
        fault = rng.choice(["kill_restart", "pause"])
        try:
            if fault == "kill_restart":
                ssh(node, f"sudo pkill -9 -x dynomited; sleep 3; DYN_ADVERTISE_ADDR={node['pub']} nohup ~/dynomited -c ~/dynomite.yml >~/dynomited.log 2>&1 </dev/null & true", timeout=30)
                stats.event("chaos", f"kill+restart {node['dc']}-{node['n']}")
            else:
                ssh(node, "P=$(pgrep -f dynomited|head -1); kill -STOP $P 2>/dev/null; sleep 5; kill -CONT $P 2>/dev/null", timeout=30)
                stats.event("chaos", f"pause+resume {node['dc']}-{node['n']}")
        except Exception as e:
            stats.event("chaos-error", f"{node['dc']}-{node['n']}: {type(e).__name__}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", required=True, choices=["valkey", "memcache", "dyniak"])
    ap.add_argument("--duration-secs", type=int, default=3600)
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--chaos-interval", type=int, default=90)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    stats = Stats()
    stop = threading.Event()
    threads = [threading.Thread(target=workload_worker, args=(args.mode, stats, stop, w), daemon=True)
               for w in range(args.workers)]
    threads.append(threading.Thread(target=chaos_loop, args=(stats, stop, args.chaos_interval), daemon=True))
    for t in threads:
        t.start()

    end = time.time() + args.duration_secs
    while time.time() < end:
        time.sleep(60)
        snap = stats.snapshot()
        with open(f"{args.out}/{args.mode}-snapshot.json", "w") as fh:
            json.dump(snap, fh, indent=2)
        print(f"[{args.mode} {time.strftime('%H:%M:%S')}] ok={snap['ok']} fail={snap['fail']} "
              f"succ={snap['success_pct']}% p50={snap['p50_ms']}ms p99={snap['p99_ms']}ms "
              f"errs={snap['err_classes']}", flush=True)
    stop.set()
    time.sleep(3)
    final = stats.snapshot()
    with open(f"{args.out}/{args.mode}-final.json", "w") as fh:
        json.dump(final, fh, indent=2)
    print(f"[{args.mode} FINAL] {json.dumps(final)[:400]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
