#!/usr/bin/env python3
"""Dyniak CRDT chaos load driver + convergence verifier (PBC over TCP).

Speaks Riak's Protocol Buffers wire format directly so it can run on a
bare load-generator instance with only python3. Drives verifiable CRDT
workloads and checks that replicas converge to the arithmetically
expected value -- the property that must hold across net splits and
ring churn because single-key CRDT updates are always accepted and
merge commutatively.

Workloads:
  counter : DtUpdateReq(CounterOp{increment:+1}); expected counter[k]
            == number of increments routed to k.
  set     : DtUpdateReq(SetOp{adds:[elem]}); expected set[k] == all
            elements added to k.

Every op is appended to a JSONL history (op,key,val,ok,err,latency) so
the coordinator can compute availability and the expected per-key value
and compare against a final fetch.

PBC framing: 4-byte big-endian length (code byte + body), then code,
then protobuf body. Codes: DtFetchReq=80 DtFetchResp=81 DtUpdateReq=82
DtUpdateResp=83.
"""
import argparse
import json
import random
import socket
import struct
import time

DT_FETCH_REQ = 80
DT_FETCH_RESP = 81
DT_UPDATE_REQ = 82
DT_UPDATE_RESP = 83


def _varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _tag(field, wire):
    return _varint((field << 3) | wire)


def _field_len(field, data):
    return _tag(field, 2) + _varint(len(data)) + data


def _field_sint64(field, value):
    z = ((value << 1) ^ (value >> 63)) & 0xFFFFFFFFFFFFFFFF
    return _tag(field, 0) + _varint(z)


def _read_varint(buf, i):
    shift = 0
    result = 0
    while True:
        b = buf[i]
        i += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result, i
        shift += 7


def _decode_sint64(v):
    return (v >> 1) ^ -(v & 1)


def counter_update(bucket, key, btype, delta):
    counter_op = _field_sint64(1, delta)
    dt_op = _field_len(1, counter_op)
    body = _field_len(1, bucket.encode())
    body += _field_len(2, key.encode())
    body += _field_len(3, btype.encode())
    body += _field_len(5, dt_op)
    return body


def set_update(bucket, key, btype, add_elem):
    set_op = _field_len(1, add_elem.encode())
    dt_op = _field_len(2, set_op)
    body = _field_len(1, bucket.encode())
    body += _field_len(2, key.encode())
    body += _field_len(3, btype.encode())
    body += _field_len(5, dt_op)
    return body


def fetch_req(bucket, key, btype):
    body = _field_len(1, bucket.encode())
    body += _field_len(2, key.encode())
    body += _field_len(3, btype.encode())
    return body


def parse_fetch_resp(body):
    i = 0
    counter = None
    elems = []
    n = len(body)
    while i < n:
        key, i = _read_varint(body, i)
        field = key >> 3
        wire = key & 7
        if wire == 2:
            ln, i = _read_varint(body, i)
            chunk = body[i:i + ln]
            i += ln
            if field == 3:                 # DtFetchResp.value == DtValue
                counter, elems = _parse_dtvalue(chunk)
        elif wire == 0:
            _, i = _read_varint(body, i)
        else:
            break
    return counter, elems


def _parse_dtvalue(body):
    i = 0
    counter = None
    elems = []
    n = len(body)
    while i < n:
        key, i = _read_varint(body, i)
        field = key >> 3
        wire = key & 7
        if wire == 0:
            v, i = _read_varint(body, i)
            if field == 1:                 # counter_value (sint64, zigzag)
                counter = _decode_sint64(v)
        elif wire == 2:
            ln, i = _read_varint(body, i)
            chunk = body[i:i + ln]
            i += ln
            if field == 2:                 # set_value (repeated bytes)
                elems.append(chunk)
        else:
            break
    return counter, elems


def send_frame(sock, code, body):
    sock.sendall(struct.pack(">I", len(body) + 1) + bytes([code]) + body)


def recv_frame(sock):
    hdr = _recv_exact(sock, 4)
    (ln,) = struct.unpack(">I", hdr)
    payload = _recv_exact(sock, ln)
    return payload[0], payload[1:]


def _recv_exact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("peer closed")
        buf.extend(chunk)
    return bytes(buf)


def connect(host, port, timeout):
    s = socket.create_connection((host, port), timeout=timeout)
    s.settimeout(timeout)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return s


def run_load(args):
    rng = random.Random(args.seed)
    hist = open(args.history, "a", buffering=1)
    deadline = time.time() + args.duration
    # Failover host list: the driver prefers its local-region node but
    # rotates to another node when its current one is unreachable, so a
    # single node being down (churned) does not make the CLIENT
    # unavailable -- this measures CLUSTER availability, the property
    # under test, the way a topology-aware client (Riak's, Dyno) does.
    hosts = [h for h in args.hosts.split(",") if h] or [args.host]
    hi = 0
    sock = None
    n_ok = 0
    n_err = 0
    lat = []
    while time.time() < deadline:
        key = f"k{rng.randrange(args.keyspace)}"
        if args.workload == "counter":
            body = counter_update(args.bucket, key, args.btype, 1)
            val = 1
        else:
            elem = f"e{rng.randrange(args.set_universe)}"
            body = set_update(args.bucket, key, args.btype, elem)
            val = elem
        t0 = time.time()
        ok = False
        err = ""
        try:
            if sock is None:
                sock = connect(hosts[hi % len(hosts)], args.port, args.timeout)
            send_frame(sock, DT_UPDATE_REQ, body)
            code, _ = recv_frame(sock)
            ok = code == DT_UPDATE_RESP
            if not ok:
                err = f"code={code}"
        except Exception as e:       # noqa: BLE001
            err = type(e).__name__
            try:
                if sock:
                    sock.close()
            except Exception:        # noqa: BLE001
                pass
            sock = None
            # Fail over to the next host for the next attempt.
            hi += 1
        dt = time.time() - t0
        lat.append(dt)
        if ok:
            n_ok += 1
        else:
            n_err += 1
        hist.write(json.dumps({
            "t": t0, "op": args.workload, "key": key, "val": val,
            "ok": ok, "err": err, "lat_ms": round(dt * 1000, 3),
            "gen": args.gen_id,
        }) + "\n")
        if args.rate:
            time.sleep(max(0.0, (1.0 / args.rate) - dt))
    hist.close()
    lat.sort()
    def pct(q):
        return round(lat[min(len(lat) - 1, int(len(lat) * q))] * 1000, 2) if lat else 0
    print(json.dumps({
        "gen": args.gen_id, "ok": n_ok, "err": n_err,
        "avail_pct": round(100.0 * n_ok / max(1, n_ok + n_err), 3),
        "p50_ms": pct(0.5), "p99_ms": pct(0.99), "p999_ms": pct(0.999),
        "max_ms": round(lat[-1] * 1000, 2) if lat else 0,
    }))


def run_fetch(args):
    sock = connect(args.host, args.port, args.timeout)
    out = {}
    for i in range(args.keyspace):
        key = f"k{i}"
        try:
            send_frame(sock, DT_FETCH_REQ, fetch_req(args.bucket, key, args.btype))
            code, body = recv_frame(sock)
            if code == DT_FETCH_RESP:
                counter, elems = parse_fetch_resp(body)
                out[key] = counter if args.workload == "counter" else \
                    sorted(e.decode("latin1") for e in elems)
        except Exception as e:       # noqa: BLE001
            out[key] = {"error": type(e).__name__}
            try:
                sock.close()
            except Exception:        # noqa: BLE001
                pass
            sock = connect(args.host, args.port, args.timeout)
    print(json.dumps(out))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["load", "fetch"])
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--hosts", default="",
                    help="comma-separated failover host list (load mode); overrides --host")
    ap.add_argument("--port", type=int, default=8087)
    ap.add_argument("--bucket", default="chaos")
    ap.add_argument("--btype", default="counters")
    ap.add_argument("--workload", choices=["counter", "set"], default="counter")
    ap.add_argument("--keyspace", type=int, default=200)
    ap.add_argument("--set-universe", type=int, default=50)
    ap.add_argument("--duration", type=float, default=60.0)
    ap.add_argument("--rate", type=float, default=0.0)
    ap.add_argument("--timeout", type=float, default=2.0)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--gen-id", default="gen0")
    ap.add_argument("--history", default="/mnt/data/chaos-history.jsonl")
    args = ap.parse_args()
    if args.mode == "load":
        run_load(args)
    else:
        run_fetch(args)


if __name__ == "__main__":
    main()
