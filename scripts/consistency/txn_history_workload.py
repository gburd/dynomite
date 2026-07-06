#!/usr/bin/env python3
"""Consistency-workload driver for dyniak multi-key transactions.

Drives the REAL dyniak HTTP transaction endpoint
(`POST /buckets/<bucket>/transactions`) on a running `dynomited`
instance and records a per-operation history in the shape an
Elle-style transactional checker consumes.

This is the foundation (workstream W1) of the consistency-
verification initiative documented in
`docs/journal/2026-06-19-consistency-verification.md`. The chaos
workload driver records only per-op-class counts and failures,
which catches errors and crashes but cannot detect a consistency
anomaly (a read that returns an impossible value while every op
"succeeds"). This driver records the ordered history a checker
needs.

Model: Elle list-append.
  * The keyspace is a small set of integer keys, each holding a
    list of integers (stored as a comma-joined string value).
  * Each transaction is a sequence of micro-ops:
      - append k v : read the current list at key k, append the
        unique value v, write the new list back -- all inside one
        `POST /transactions` batch.
      - read k     : read the current list at key k.
    Appends are globally unique, so the per-key order of appended
    values is a total order the checker can reconstruct, and a
    transaction that observes a value implies a dependency edge.

Because dyniak's transaction endpoint applies a batch of put/delete
ops atomically (read-modify-write is done client-side within one
batch: we GET the current lists, compute the new lists, and PUT
them in a single atomic batch), each recorded transaction is
exactly one `POST /transactions` call. A committed batch is an
`:ok`; an aborted or failed batch is an `:fail` (the writes did
not land) or `:info` (indeterminate -- e.g. a timeout where the
outcome is unknown).

History record (one JSON object per line; the checker consumes
the stream):

  {"index": N, "process": P, "type": "invoke"|"ok"|"fail"|"info",
   "time_ns": T, "value": [[f, k, v_or_null], ...]}

where each micro-op in `value` is:
  ["append", k, v]            on invoke and ok
  ["r", k, null]              on invoke
  ["r", k, [v1, v2, ...]]     on ok (the observed list)

This mirrors the Jepsen/Elle list-append history shape closely
enough that the W2 Rust checker and the W4 Jepsen+Elle harness can
both read it.
"""

from __future__ import annotations

import argparse
import json
import random
import socket
import sys
import time
import urllib.error
import urllib.request


class TxnClient:
    """Minimal HTTP client for the dyniak transaction + object API.

    Talks to one dynomited instance's `riak.http_listen` address.
    Object reads use `GET /buckets/<b>/keys/<k>`; the atomic
    multi-key write uses `POST /buckets/<b>/transactions`.
    """

    def __init__(self, host: str, port: int, bucket: str, timeout: float = 5.0):
        self.base = f"http://{host}:{port}"
        self.bucket = bucket
        self.timeout = timeout

    def read_key(self, key: str) -> list[int] | None:
        """Return the parsed list at `key`, or None if absent.

        The object GET path returns the stored value wrapped in a
        JSON `HttpObject` envelope (`{"value": [bytes...], ...}`),
        so we decode the byte array back into the UTF-8 list string
        the workload stores.
        """
        url = f"{self.base}/buckets/{self.bucket}/keys/{key}"
        req = urllib.request.Request(
            url, method="GET", headers={"Accept": "application/json"}
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            raise
        envelope = json.loads(raw)
        value_bytes = bytes(envelope.get("value", []))
        return _parse_list(value_bytes.decode("utf-8", "replace"))

    def commit_batch(self, puts: dict[str, list[int]]) -> bool:
        """Atomically write every (key -> list) in `puts`.

        Returns True on a committed batch, False on a clean abort.
        Raises on a transport error / indeterminate outcome so the
        caller can record `:info`.
        """
        ops = [
            {
                "op": "put",
                "bucket": self.bucket,
                "key": k,
                "value": _format_list(vs),
            }
            for k, vs in puts.items()
        ]
        payload = json.dumps({"operations": ops}).encode("utf-8")
        url = f"{self.base}/buckets/{self.bucket}/transactions"
        req = urllib.request.Request(
            url,
            data=payload,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            body = json.loads(resp.read().decode("utf-8", "replace"))
        return body.get("result") == "committed"

    def ramp_read(self, keys: list[str]) -> dict[str, list[int]]:
        """RAMP-Fast atomic read of `keys` via `POST /ramp/read`.

        Returns a fracture-free `key -> list` snapshot (keys with no
        committed RAMP write are absent). Read-atomic isolation
        guarantees the snapshot never mixes one RAMP transaction's
        writes with a stale sibling.
        """
        payload = json.dumps({"keys": keys}).encode("utf-8")
        url = f"{self.base}/ramp/read"
        req = urllib.request.Request(
            url, data=payload, method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            body = json.loads(resp.read().decode("utf-8", "replace"))
        snap = body.get("snapshot", {})
        return {k: _parse_list(v) for k, v in snap.items()}

    def ramp_commit(self, puts: dict[str, list[int]]) -> bool:
        """Atomically RAMP-write every (key -> list) via
        `POST /ramp/transactions`. Returns True on commit.
        """
        writes = [{"key": k, "value": _format_list(vs)} for k, vs in puts.items()]
        payload = json.dumps({"writes": writes}).encode("utf-8")
        url = f"{self.base}/ramp/transactions"
        req = urllib.request.Request(
            url, data=payload, method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            body = json.loads(resp.read().decode("utf-8", "replace"))
        return body.get("result") == "committed"


def _parse_list(s: str) -> list[int]:
    s = s.strip()
    if not s:
        return []
    return [int(x) for x in s.split(",") if x != ""]


def _format_list(vs: list[int]) -> str:
    return ",".join(str(v) for v in vs)


class History:
    """Append-only history writer (one JSON object per line)."""

    def __init__(self, path: str):
        self.fh = open(path, "w", buffering=1)
        self.index = 0

    def _emit(self, process: int, typ: str, value: list) -> None:
        rec = {
            "index": self.index,
            "process": process,
            "type": typ,
            "time_ns": time.monotonic_ns(),
            "value": value,
        }
        self.fh.write(json.dumps(rec) + "\n")
        self.index += 1

    def invoke(self, process: int, value: list) -> None:
        self._emit(process, "invoke", value)

    def ok(self, process: int, value: list) -> None:
        self._emit(process, "ok", value)

    def fail(self, process: int, value: list) -> None:
        self._emit(process, "fail", value)

    def info(self, process: int, value: list) -> None:
        self._emit(process, "info", value)

    def close(self) -> None:
        self.fh.flush()
        self.fh.close()


def run_transaction(
    client: TxnClient,
    history: History,
    process: int,
    keys: list[str],
    next_value,
    rng: random.Random,
) -> None:
    """Generate, execute, and record one list-append transaction.

    The micro-op list mixes appends and reads across a random
    subset of `keys`. Appends are realized as a client-side
    read-modify-write folded into one atomic put-batch, so the
    whole transaction commits or aborts as a unit.
    """
    n = rng.randint(1, min(4, len(keys)))
    chosen = rng.sample(keys, n)
    micro: list[list] = []
    appends: dict[str, int] = {}
    reads: list[str] = []
    for k in chosen:
        if rng.random() < 0.6:
            v = next_value()
            appends[k] = v
            micro.append(["append", int(k), v])
        else:
            reads.append(k)
            micro.append(["r", int(k), None])

    history.invoke(process, micro)

    try:
        # Read the current lists for every key the transaction
        # touches (reads to report, appends to extend).
        observed: dict[str, list[int]] = {}
        for k in set(list(appends.keys()) + reads):
            observed[k] = client.read_key(k) or []

        if appends:
            puts = {k: observed[k] + [v] for k, v in appends.items()}
            committed = client.commit_batch(puts)
        else:
            # Read-only transaction: no batch to commit.
            committed = True

        if not committed:
            history.fail(process, micro)
            return

        # Build the :ok value: appends keep their value; reads
        # report the observed list (the post-read state, which for
        # a read-only or pre-append snapshot is what was visible).
        ok_micro: list[list] = []
        for f, k, v in micro:
            ks = str(k)
            if f == "append":
                ok_micro.append(["append", k, v])
            else:
                ok_micro.append(["r", k, list(observed.get(ks, []))])
        history.ok(process, ok_micro)
    except (urllib.error.URLError, socket.timeout, ConnectionError, OSError):
        # Indeterminate: the batch may or may not have landed.
        history.info(process, micro)


def run_ramp_transaction(
    client: TxnClient,
    history: History,
    process: int,
    keys: list[str],
    next_value,
    rng: random.Random,
) -> None:
    """Generate, execute, and record one RAMP list-append transaction.

    Uses the RAMP endpoints (`POST /ramp/read`, `POST /ramp/transactions`)
    instead of the XA-style `POST /transactions`. The recorded history
    has the identical shape, so the same Elle-style checker validates
    it -- except RAMP guarantees read-atomic isolation, so the reads
    used to compute each append see a fracture-free snapshot.
    """
    n = rng.randint(1, min(4, len(keys)))
    chosen = rng.sample(keys, n)
    micro: list[list] = []
    appends: dict[str, int] = {}
    reads: list[str] = []
    for k in chosen:
        if rng.random() < 0.6:
            v = next_value()
            appends[k] = v
            micro.append(["append", int(k), v])
        else:
            reads.append(k)
            micro.append(["r", int(k), None])

    history.invoke(process, micro)
    try:
        touched = sorted(set(list(appends.keys()) + reads))
        # One RAMP read gives an atomic snapshot of every touched key.
        observed = client.ramp_read(touched) if touched else {}
        for k in touched:
            observed.setdefault(k, [])

        if appends:
            puts = {k: observed[k] + [v] for k, v in appends.items()}
            committed = client.ramp_commit(puts)
        else:
            committed = True

        if not committed:
            history.fail(process, micro)
            return

        ok_micro: list[list] = []
        for f, k, v in micro:
            if f == "append":
                ok_micro.append(["append", k, v])
            else:
                ok_micro.append(["r", k, list(observed.get(str(k), []))])
        history.ok(process, ok_micro)
    except (urllib.error.URLError, socket.timeout, ConnectionError, OSError):
        history.info(process, micro)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--http-port", type=int, required=True,
                    help="dyniak riak.http_listen port")
    ap.add_argument("--bucket", default="cc")
    ap.add_argument("--keys", type=int, default=8,
                    help="size of the integer keyspace")
    ap.add_argument("--process", type=int, default=0,
                    help="logical process id for this worker")
    ap.add_argument("--duration-secs", type=float, default=30.0)
    ap.add_argument("--out", required=True, help="history output path")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--ramp", action="store_true",
                    help="drive the RAMP endpoints instead of XA transactions")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    keys = [str(i) for i in range(args.keys)]
    client = TxnClient(args.host, args.http_port, args.bucket)
    history = History(args.out)

    # Globally-unique append values: high bits = process, low bits
    # = counter, so two processes never mint the same value.
    counter = {"n": 0}

    def next_value() -> int:
        counter["n"] += 1
        return args.process * 10_000_000 + counter["n"]

    end = time.monotonic() + args.duration_secs
    runner = run_ramp_transaction if args.ramp else run_transaction
    try:
        while time.monotonic() < end:
            runner(client, history, args.process, keys, next_value, rng)
    finally:
        history.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
