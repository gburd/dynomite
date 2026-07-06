#!/usr/bin/env python3
"""Differential driver: send identical ops to the C dynomite (:8102)
and Rust dynomited (:9102) proxies on the same node and compare replies.

Both proxies front the same backend with topology-identical rings, so
every K/V reply must match byte-for-byte except for the known-divergent
classes captured by scripts/chaos-multi-host/differential_allowlist.py.

Usage:
  differential-driver.py --host <pub-ip> [--ops N] [--mode valkey|memcache]
                         [--seed S]

Reports a JSON summary: total ops, agreements, divergences (with the
first few divergent samples), and per-op-class counts. Stdlib only.
"""

from __future__ import annotations

import argparse
import json
import random
import socket
import sys
import time


def resp_encode(*args: str) -> bytes:
    out = f"*{len(args)}\r\n".encode()
    for a in args:
        b = a.encode()
        out += f"${len(b)}\r\n".encode() + b + b"\r\n"
    return out


def recv_reply(sock: socket.socket, timeout: float = 5.0) -> bytes:
    """Read one RESP reply (best-effort: read until a CRLF-terminated
    frame that parses as a complete top-level reply)."""
    sock.settimeout(timeout)
    buf = b""
    while True:
        try:
            chunk = sock.recv(4096)
        except socket.timeout:
            return buf  # partial / timeout
        if not chunk:
            return buf
        buf += chunk
        # Simple completeness heuristic for the reply shapes we send
        # (+OK, $len\r\n..., :int, -ERR, $-1). Good enough for SET/GET.
        if buf.endswith(b"\r\n"):
            if buf[:1] in (b"+", b"-", b":"):
                return buf
            if buf[:1] == b"$":
                # bulk: $-1\r\n (nil) or $len\r\n<payload>\r\n
                if buf.startswith(b"$-1\r\n"):
                    return buf
                # need the payload; check we have len+2 trailing bytes
                try:
                    nl = buf.index(b"\r\n")
                    n = int(buf[1:nl])
                    if len(buf) >= nl + 2 + n + 2:
                        return buf
                except ValueError:
                    return buf
            else:
                return buf


def normalize(reply: bytes) -> bytes:
    """Apply allowlisted normalization: strip trailing whitespace only.
    K/V SET/GET replies are expected identical; no timing fields here."""
    return reply.rstrip(b"\r\n")


def one_op(chost: int, rhost: int, kind: str, key: str, val: str,
           chost_sock: socket.socket, rhost_sock: socket.socket) -> tuple[bool, bytes, bytes]:
    if kind == "set":
        frame = resp_encode("SET", key, val)
    elif kind == "get":
        frame = resp_encode("GET", key)
    elif kind == "del":
        frame = resp_encode("DEL", key)
    else:
        raise ValueError(kind)
    chost_sock.sendall(frame)
    c_reply = recv_reply(chost_sock)
    rhost_sock.sendall(frame)
    r_reply = recv_reply(rhost_sock)
    agree = normalize(c_reply) == normalize(r_reply)
    return agree, c_reply, r_reply


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True, help="node public IP")
    ap.add_argument("--c-port", type=int, default=8102)
    ap.add_argument("--r-port", type=int, default=9102)
    ap.add_argument("--ops", type=int, default=500)
    ap.add_argument("--keyspace", type=int, default=200)
    ap.add_argument("--seed", type=int, default=1)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    c_sock = socket.create_connection((args.host, args.c_port), timeout=10)
    r_sock = socket.create_connection((args.host, args.r_port), timeout=10)

    total = agree = diverge = 0
    samples: list[dict] = []
    by_kind: dict[str, dict[str, int]] = {}

    for i in range(args.ops):
        k = f"dk{rng.randrange(args.keyspace)}"
        r = rng.random()
        if r < 0.5:
            kind, val = "set", f"v{i}"
        elif r < 0.9:
            kind, val = "get", ""
        else:
            kind, val = "del", ""
        ok, cr, rr = one_op(args.c_port, args.r_port, kind, k, val, c_sock, r_sock)
        total += 1
        by_kind.setdefault(kind, {"agree": 0, "diverge": 0})
        if ok:
            agree += 1
            by_kind[kind]["agree"] += 1
        else:
            diverge += 1
            by_kind[kind]["diverge"] += 1
            if len(samples) < 10:
                samples.append({
                    "op": kind, "key": k,
                    "c": cr.decode("latin1"), "r": rr.decode("latin1"),
                })

    c_sock.close()
    r_sock.close()
    print(json.dumps({
        "host": args.host, "total": total, "agree": agree, "diverge": diverge,
        "agree_pct": round(100.0 * agree / max(total, 1), 3),
        "by_kind": by_kind, "divergent_samples": samples,
    }, indent=2))
    return 0 if diverge == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
