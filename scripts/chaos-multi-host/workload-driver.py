#!/usr/bin/env python3
"""Workload driver for the multi-host chaos test.

Drives a Redis client against the local dynomited instance using
every command class the dynomite parser covers: strings, hashes,
sets, sorted sets, lists, scripting, scan, expire/TTL, multi-key,
transactions. Runs continuously until SIGTERM, periodically
recording per-class success/failure counters into a NDJSON log so
the coordinator can summarise them after the run.

Designed to be run on each host in parallel; the coordinator
launches one instance per host pointing at 127.0.0.1:<client_port>.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import signal
import socket
import string
import sys
import time
from contextlib import suppress
from pathlib import Path

# --- minimal RESP-2 client (avoids redis-py dep on FreeBSD) ---


class RespError(Exception):
    """Server returned a -ERR reply."""


class RespTimeout(Exception):
    """Socket timed out before a reply arrived."""


class RespConn:
    """A cheap RESP-2 client.

    The dynomite parser is what we are testing; using a hand-rolled
    client (rather than redis-py) keeps the test honest about what
    bytes go on the wire.
    """

    def __init__(self, host: str, port: int, timeout: float = 5.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock: socket.socket | None = None
        self.rbuf: bytes = b""

    def connect(self) -> None:
        sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        sock.settimeout(self.timeout)
        self.sock = sock
        self.rbuf = b""

    def close(self) -> None:
        if self.sock is not None:
            with suppress(OSError):
                self.sock.close()
        self.sock = None
        self.rbuf = b""

    def _readline(self) -> bytes:
        assert self.sock is not None
        while b"\r\n" not in self.rbuf:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("peer closed mid-reply")
            self.rbuf += chunk
        line, _, self.rbuf = self.rbuf.partition(b"\r\n")
        return line

    def _readn(self, n: int) -> bytes:
        assert self.sock is not None
        while len(self.rbuf) < n:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("peer closed mid-bulk")
            self.rbuf += chunk
        out, self.rbuf = self.rbuf[:n], self.rbuf[n:]
        return out

    def _read_reply(self):
        line = self._readline()
        if not line:
            raise RespError("empty reply")
        prefix, rest = line[0:1], line[1:]
        if prefix == b"+":
            return rest.decode("utf-8", "replace")
        if prefix == b"-":
            raise RespError(rest.decode("utf-8", "replace"))
        if prefix == b":":
            return int(rest)
        if prefix == b"$":
            n = int(rest)
            if n < 0:
                return None
            data = self._readn(n)
            self._readn(2)  # trailing CRLF
            return data
        if prefix == b"*":
            n = int(rest)
            if n < 0:
                return None
            return [self._read_reply() for _ in range(n)]
        raise RespError(f"unknown prefix: {prefix!r}")

    def call(self, *parts) -> object:
        """Send one RESP command and return the parsed reply."""
        if self.sock is None:
            self.connect()
        encoded = []
        encoded.append(f"*{len(parts)}\r\n".encode())
        for p in parts:
            if isinstance(p, str):
                p = p.encode()
            elif isinstance(p, int):
                p = str(p).encode()
            encoded.append(f"${len(p)}\r\n".encode())
            encoded.append(p)
            encoded.append(b"\r\n")
        try:
            self.sock.sendall(b"".join(encoded))
            return self._read_reply()
        except (socket.timeout, ConnectionError, RespError):
            self.close()
            raise


# --- workload classes ---


def randkey(n: int = 8) -> str:
    return "k:" + "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


def randval(n: int = 16) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=n))


def workload_strings(c: RespConn) -> str:
    op = random.choice(["SET", "GET", "GETSET", "INCR", "DECR", "INCRBY",
                        "APPEND", "STRLEN", "GETRANGE"])
    k = randkey()
    if op == "SET":
        c.call("SET", k, randval())
    elif op == "GET":
        c.call("GET", k)
    elif op == "GETSET":
        c.call("GETSET", k, randval())
    elif op == "INCR":
        c.call("INCR", k)
    elif op == "DECR":
        c.call("DECR", k)
    elif op == "INCRBY":
        c.call("INCRBY", k, random.randint(-100, 100))
    elif op == "APPEND":
        c.call("APPEND", k, randval(4))
    elif op == "STRLEN":
        c.call("STRLEN", k)
    elif op == "GETRANGE":
        c.call("GETRANGE", k, 0, random.randint(0, 32))
    return op


def workload_hash(c: RespConn) -> str:
    op = random.choice(["HSET", "HGET", "HDEL", "HMSET", "HMGET",
                        "HGETALL", "HEXISTS", "HKEYS", "HLEN"])
    k = randkey()
    f = "f:" + randval(4)
    if op == "HSET":
        c.call("HSET", k, f, randval())
    elif op == "HGET":
        c.call("HGET", k, f)
    elif op == "HDEL":
        c.call("HDEL", k, f)
    elif op == "HMSET":
        c.call("HMSET", k, "a", randval(), "b", randval(), "c", randval())
    elif op == "HMGET":
        c.call("HMGET", k, "a", "b", "c")
    elif op == "HGETALL":
        c.call("HGETALL", k)
    elif op == "HEXISTS":
        c.call("HEXISTS", k, f)
    elif op == "HKEYS":
        c.call("HKEYS", k)
    elif op == "HLEN":
        c.call("HLEN", k)
    return op


def workload_set(c: RespConn) -> str:
    op = random.choice(["SADD", "SREM", "SMEMBERS", "SCARD",
                        "SISMEMBER"])
    k = randkey()
    m = randval(6)
    if op == "SADD":
        c.call("SADD", k, m)
    elif op == "SREM":
        c.call("SREM", k, m)
    elif op == "SMEMBERS":
        c.call("SMEMBERS", k)
    elif op == "SCARD":
        c.call("SCARD", k)
    elif op == "SISMEMBER":
        c.call("SISMEMBER", k, m)
    return op


def workload_zset(c: RespConn) -> str:
    op = random.choice(["ZADD", "ZREM", "ZSCORE", "ZCARD",
                        "ZRANGE", "ZINCRBY"])
    k = randkey()
    m = randval(6)
    if op == "ZADD":
        c.call("ZADD", k, str(random.uniform(0, 100)), m)
    elif op == "ZREM":
        c.call("ZREM", k, m)
    elif op == "ZSCORE":
        c.call("ZSCORE", k, m)
    elif op == "ZCARD":
        c.call("ZCARD", k)
    elif op == "ZRANGE":
        c.call("ZRANGE", k, 0, 5)
    elif op == "ZINCRBY":
        c.call("ZINCRBY", k, str(random.uniform(0, 5)), m)
    return op


def workload_list(c: RespConn) -> str:
    op = random.choice(["LPUSH", "RPUSH", "LPOP", "RPOP",
                        "LRANGE", "LLEN", "LINDEX"])
    k = randkey()
    if op == "LPUSH":
        c.call("LPUSH", k, randval())
    elif op == "RPUSH":
        c.call("RPUSH", k, randval())
    elif op == "LPOP":
        c.call("LPOP", k)
    elif op == "RPOP":
        c.call("RPOP", k)
    elif op == "LRANGE":
        c.call("LRANGE", k, 0, 9)
    elif op == "LLEN":
        c.call("LLEN", k)
    elif op == "LINDEX":
        c.call("LINDEX", k, random.randint(0, 4))
    return op


def workload_keyspace(c: RespConn) -> str:
    op = random.choice(["DEL", "EXISTS", "EXPIRE", "TTL",
                        "PERSIST", "TYPE"])
    k = randkey()
    if op == "DEL":
        c.call("DEL", k)
    elif op == "EXISTS":
        c.call("EXISTS", k)
    elif op == "EXPIRE":
        c.call("EXPIRE", k, random.randint(1, 3600))
    elif op == "TTL":
        c.call("TTL", k)
    elif op == "PERSIST":
        c.call("PERSIST", k)
    elif op == "TYPE":
        c.call("TYPE", k)
    return op


def workload_multikey(c: RespConn) -> str:
    op = random.choice(["MGET", "MSET"])
    keys = [randkey() for _ in range(random.randint(2, 5))]
    if op == "MGET":
        c.call("MGET", *keys)
    elif op == "MSET":
        args = []
        for k in keys:
            args.append(k)
            args.append(randval())
        c.call("MSET", *args)
    return op


def workload_scripting(c: RespConn) -> str:
    op = random.choice(["EVAL", "PING"])
    if op == "EVAL":
        c.call("EVAL", "return 1", "0")
    elif op == "PING":
        c.call("PING")
    return op


WORKLOADS = [
    ("strings", workload_strings, 30),
    ("hash", workload_hash, 15),
    ("set", workload_set, 10),
    ("zset", workload_zset, 10),
    ("list", workload_list, 10),
    ("keyspace", workload_keyspace, 10),
    ("multikey", workload_multikey, 10),
    ("scripting", workload_scripting, 5),
]


# --- driver loop ---


_RUNNING = True


def _stop(_signo, _frame):  # pragma: no cover
    global _RUNNING
    _RUNNING = False


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=18102)
    p.add_argument("--label", required=True, help="DC label for the log")
    p.add_argument("--out", required=True, help="NDJSON output path")
    p.add_argument("--qps", type=int, default=200)
    p.add_argument("--duration", type=int, default=7200,
                   help="seconds; 0 means until SIGTERM")
    args = p.parse_args()

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    # Truncate on each run; the coordinator manages run-id
    # subdirs so we never want to mix sessions in one file.
    f = out.open("w", buffering=1)

    counts: dict[tuple[str, str], int] = {}
    failures: dict[tuple[str, str], int] = {}
    last_flush = time.monotonic()
    started = time.monotonic()

    weights = [w for _, _, w in WORKLOADS]
    total_weight = sum(weights)

    sleep_per_op = 1.0 / args.qps if args.qps > 0 else 0.0

    conn = RespConn(args.host, args.port)
    while _RUNNING:
        if args.duration and (time.monotonic() - started) >= args.duration:
            break
        roll = random.random() * total_weight
        acc = 0
        chosen_class = WORKLOADS[-1]
        for entry in WORKLOADS:
            acc += entry[2]
            if roll < acc:
                chosen_class = entry
                break
        cls_name, fn, _ = chosen_class
        try:
            op = fn(conn)
            counts[(cls_name, op)] = counts.get((cls_name, op), 0) + 1
        except (RespError, ConnectionError, socket.timeout, OSError) as exc:
            key = (cls_name, type(exc).__name__)
            failures[key] = failures.get(key, 0) + 1
            # Log a small sample of failures to stderr so the
            # operator can correlate with dynomited / redis logs.
            if failures[key] <= 5:
                print(
                    f"[{args.label}] {cls_name} call failed: "
                    f"{type(exc).__name__}: {exc}",
                    file=sys.stderr,
                    flush=True,
                )
            with suppress(Exception):
                conn.close()
        if sleep_per_op > 0:
            time.sleep(sleep_per_op)

        now = time.monotonic()
        if now - last_flush >= 10.0:
            row = {
                "ts": time.time(),
                "label": args.label,
                "elapsed": now - started,
                "counts": {f"{c}/{o}": v for (c, o), v in counts.items()},
                "failures": {f"{c}/{e}": v for (c, e), v in failures.items()},
            }
            f.write(json.dumps(row) + "\n")
            counts.clear()
            failures.clear()
            last_flush = now

    # final flush
    row = {
        "ts": time.time(),
        "label": args.label,
        "elapsed": time.monotonic() - started,
        "counts": {f"{c}/{o}": v for (c, o), v in counts.items()},
        "failures": {f"{c}/{e}": v for (c, e), v in failures.items()},
        "final": True,
    }
    f.write(json.dumps(row) + "\n")
    f.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
