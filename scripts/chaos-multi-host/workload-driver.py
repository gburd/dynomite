#!/usr/bin/env python3
"""Workload driver for the multi-host chaos test.

Drives a Redis, memcache, or Riak PBC client against the local
dynomited instance. The selected mode determines:

  * ``--mode redis``    -- RESP-2 over TCP, ``data_store: 0``
  * ``--mode memcache`` -- memcache ASCII over TCP, ``data_store: 1``
  * ``--mode riak``     -- Riak PBC over TCP at the engine's
                           ``riak.pbc_listen`` address; the
                           upstream ``data_store`` is irrelevant
                           because the request flows through
                           dyn-riak's MemoryDatastore (or whatever
                           Datastore the binary was wired with)
                           rather than the Redis/memcache
                           dispatcher.

In every mode the driver runs continuously until SIGTERM,
periodically recording per-class success/failure counters into a
NDJSON log so the coordinator can summarise them after the run.

Designed to be run on each host in parallel; the coordinator
launches one instance per host pointing at
127.0.0.1:<client_port> (or 127.0.0.1:<riak_pbc_port> in Riak
mode).

The Riak PBC encoder is hand-rolled on top of the stdlib
``struct`` module so the driver has no third-party Python
dependencies. Only the four operations Ping / Get / Put / Del are
supported; that is enough to drive load against dyn-riak and
exercise the framer, codec, and dispatcher under chaos.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import signal
import socket
import string
import struct
import sys
import time
import unittest
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
        # On FreeBSD (and occasionally on Linux too) the kernel can
        # pick an ephemeral source port for a 127.0.0.1 connect()
        # that happens to equal the destination port, producing a
        # "self-connection" 127.0.0.1:N -> 127.0.0.1:N that blocks
        # any future bind on N. Avoid that by binding the source
        # to port 0 in a high range BEFORE connecting; if the
        # kernel still hands us a colliding port we close and
        # retry up to a few times.
        for _ in range(5):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                # Hint the kernel toward a source port well above
                # the dynomite/redis ports we're targeting (which
                # live in the 17000-22000 range). 50000-65535 is
                # the standard ephemeral range and never overlaps
                # with our service ports.
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
                sock.bind(("127.0.0.1", 0))
                sock.settimeout(self.timeout)
                sock.connect((self.host, self.port))
                local_port = sock.getsockname()[1]
                if local_port == self.port:
                    # Loopback self-connection. Close and try
                    # again; the kernel will pick a different
                    # ephemeral the next time.
                    sock.close()
                    continue
                self.sock = sock
                self.rbuf = b""
                return
            except OSError:
                with suppress(Exception):
                    sock.close()
                raise
        raise ConnectionError(
            "could not establish a non-self-loop connection after 5 attempts"
        )

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


# --- memcache ASCII protocol client ---


class MemcacheError(Exception):
    """Server returned an ERROR / CLIENT_ERROR / SERVER_ERROR reply."""


class MemcacheConn:
    """A small memcache ASCII-protocol client.

    Mirrors the loopback self-connection avoidance trick used by
    ``RespConn``. The dynomite memcache parser is the system
    under test; this hand-rolled client keeps full control over
    the bytes that go on the wire.
    """

    def __init__(self, host: str, port: int, timeout: float = 5.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock: socket.socket | None = None
        self.rbuf: bytes = b""

    def connect(self) -> None:
        for _ in range(5):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
                sock.bind(("127.0.0.1", 0))
                sock.settimeout(self.timeout)
                sock.connect((self.host, self.port))
                local_port = sock.getsockname()[1]
                if local_port == self.port:
                    sock.close()
                    continue
                self.sock = sock
                self.rbuf = b""
                return
            except OSError:
                with suppress(Exception):
                    sock.close()
                raise
        raise ConnectionError(
            "could not establish a non-self-loop memcache connection"
        )

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
                raise ConnectionError("peer closed mid-line")
            self.rbuf += chunk
        line, _, self.rbuf = self.rbuf.partition(b"\r\n")
        return line

    def _readn(self, n: int) -> bytes:
        assert self.sock is not None
        while len(self.rbuf) < n:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("peer closed mid-data")
            self.rbuf += chunk
        out, self.rbuf = self.rbuf[:n], self.rbuf[n:]
        return out

    def _send(self, payload: bytes) -> None:
        if self.sock is None:
            self.connect()
        try:
            self.sock.sendall(payload)
        except (socket.timeout, ConnectionError, OSError):
            self.close()
            raise

    def _read_storage_reply(self) -> bytes:
        line = self._readline()
        if line in (b"STORED", b"NOT_STORED", b"EXISTS", b"NOT_FOUND"):
            return line
        if line.startswith(b"CLIENT_ERROR") or line.startswith(b"SERVER_ERROR") \
                or line == b"ERROR":
            raise MemcacheError(line.decode("utf-8", "replace"))
        # dynomite may surface its own error frames; treat anything
        # else as protocol-level.
        raise MemcacheError(
            "unexpected storage reply: " + line.decode("utf-8", "replace"))

    def _read_retrieval_reply(self) -> dict:
        out = {}
        while True:
            line = self._readline()
            if line == b"END":
                return out
            if line.startswith(b"VALUE "):
                # VALUE <key> <flags> <bytes>[ <cas>]
                parts = line.split(b" ")
                if len(parts) < 4:
                    raise MemcacheError(
                        "malformed VALUE: " + line.decode("utf-8", "replace"))
                key = parts[1].decode("utf-8", "replace")
                nbytes = int(parts[3])
                data = self._readn(nbytes)
                self._readn(2)  # trailing CRLF
                out[key] = data
                continue
            if line.startswith(b"CLIENT_ERROR") or line.startswith(b"SERVER_ERROR") \
                    or line == b"ERROR":
                raise MemcacheError(line.decode("utf-8", "replace"))
            raise MemcacheError(
                "unexpected retrieval reply: " + line.decode("utf-8", "replace"))

    def _read_arith_reply(self) -> object:
        line = self._readline()
        if line == b"NOT_FOUND":
            return None
        if line.startswith(b"CLIENT_ERROR") or line.startswith(b"SERVER_ERROR") \
                or line == b"ERROR":
            raise MemcacheError(line.decode("utf-8", "replace"))
        # numeric reply
        try:
            return int(line)
        except ValueError:
            raise MemcacheError(
                "unexpected arith reply: " + line.decode("utf-8", "replace"))

    def _read_delete_reply(self) -> bytes:
        line = self._readline()
        if line in (b"DELETED", b"NOT_FOUND"):
            return line
        if line.startswith(b"CLIENT_ERROR") or line.startswith(b"SERVER_ERROR") \
                or line == b"ERROR":
            raise MemcacheError(line.decode("utf-8", "replace"))
        raise MemcacheError(
            "unexpected delete reply: " + line.decode("utf-8", "replace"))

    # ---- public surface ----

    def store(self, op: str, key: str, value: bytes,
              flags: int = 0, exptime: int = 0) -> bytes:
        if isinstance(value, str):
            value = value.encode()
        head = f"{op} {key} {flags} {exptime} {len(value)}\r\n".encode()
        self._send(head + value + b"\r\n")
        return self._read_storage_reply()

    def get(self, *keys: str) -> dict:
        cmd = "get " + " ".join(keys) + "\r\n"
        self._send(cmd.encode())
        return self._read_retrieval_reply()

    def gets(self, *keys: str) -> dict:
        cmd = "gets " + " ".join(keys) + "\r\n"
        self._send(cmd.encode())
        return self._read_retrieval_reply()

    def delete(self, key: str) -> bytes:
        self._send(f"delete {key}\r\n".encode())
        return self._read_delete_reply()

    def incr(self, key: str, delta: int) -> object:
        self._send(f"incr {key} {delta}\r\n".encode())
        return self._read_arith_reply()

    def decr(self, key: str, delta: int) -> object:
        self._send(f"decr {key} {delta}\r\n".encode())
        return self._read_arith_reply()


def workload_memcache_set(c: MemcacheConn) -> str:
    op = random.choice(["set", "add", "replace", "append", "prepend"])
    k = randkey()
    c.store(op, k, randval())
    return op


def workload_memcache_get(c: MemcacheConn) -> str:
    op = random.choice(["get", "gets"])
    if op == "get":
        c.get(randkey())
    else:
        c.gets(randkey())
    return op


def workload_memcache_arith(c: MemcacheConn) -> str:
    op = random.choice(["incr", "decr"])
    k = randkey()
    # Seed the counter so incr/decr have a chance of hitting a
    # numeric value rather than always observing NOT_FOUND.
    if random.random() < 0.3:
        with suppress(MemcacheError, ConnectionError, socket.timeout, OSError):
            c.store("set", k, str(random.randint(0, 1000)))
    if op == "incr":
        c.incr(k, random.randint(1, 100))
    else:
        c.decr(k, random.randint(1, 100))
    return op


def workload_memcache_delete(c: MemcacheConn) -> str:
    c.delete(randkey())
    return "delete"


MEMCACHE_WORKLOADS = [
    ("set", workload_memcache_set, 35),
    ("get", workload_memcache_get, 35),
    ("arith", workload_memcache_arith, 20),
    ("delete", workload_memcache_delete, 10),
]


# --- Riak PBC client ---
#
# The Riak Protocol Buffers Client (PBC) wire shape is:
#   * 4-byte big-endian length (covers the message-code byte plus
#     the protobuf body)
#   * 1-byte message code
#   * N bytes of protobuf body
#
# Each protobuf field is preceded by a varint tag where
#   tag = (field_number << 3) | wire_type
# wire_type 0 is varint (uint32, bool); wire_type 2 is
# length-delimited (bytes, string, embedded message). For the
# four operations the driver supports, only wire types 0 and 2
# are needed.
#
# Field tags below match the dyn-riak crate's
# ``proto::pb::messages`` schema:
#   RpbGetReq:   bucket=1, key=2          (both bytes)
#   RpbPutReq:   bucket=1, key=2, value=4 (all bytes)
#                The dyn-riak v0 surface flattens the canonical
#                Riak ``RpbContent.value`` (nested at upstream
#                tag 3) to a top-level ``value`` at tag 4. That
#                is the wire shape the server decodes; matching
#                it keeps the driver compatible with the
#                MemoryDatastore the binary spins up by default.
#   RpbDelReq:   bucket=1, key=2          (both bytes)
#   RpbErrorResp: errmsg=1 (bytes), errcode=2 (varint)

RIAK_CODE_ERROR_RESP = 0
RIAK_CODE_PING_REQ = 1
RIAK_CODE_PING_RESP = 2
RIAK_CODE_GET_REQ = 9
RIAK_CODE_GET_RESP = 10
RIAK_CODE_PUT_REQ = 11
RIAK_CODE_PUT_RESP = 12
RIAK_CODE_DEL_REQ = 13
RIAK_CODE_DEL_RESP = 14

_PB_WIRE_VARINT = 0
_PB_WIRE_LENGTH_DELIMITED = 2


def _pb_encode_varint(n: int) -> bytes:
    """Encode a non-negative integer as a protobuf varint."""
    if n < 0:
        raise ValueError("varint must be non-negative")
    out = bytearray()
    while n > 0x7F:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out.append(n & 0x7F)
    return bytes(out)


def _pb_decode_varint(buf: bytes, pos: int) -> tuple[int, int]:
    """Decode one varint from ``buf[pos:]``; return (value, new_pos)."""
    n = 0
    shift = 0
    start = pos
    while True:
        if pos >= len(buf):
            raise ValueError("truncated varint at offset %d" % start)
        b = buf[pos]
        pos += 1
        n |= (b & 0x7F) << shift
        if b < 0x80:
            return n, pos
        shift += 7
        if shift > 63:
            raise ValueError("varint too long at offset %d" % start)


def _pb_encode_tag(field: int, wire_type: int) -> bytes:
    return _pb_encode_varint((field << 3) | wire_type)


def _pb_encode_bytes_field(field: int, val: bytes) -> bytes:
    if isinstance(val, str):
        val = val.encode()
    return (
        _pb_encode_tag(field, _PB_WIRE_LENGTH_DELIMITED)
        + _pb_encode_varint(len(val))
        + val
    )


def encode_rpb_ping_req() -> bytes:
    """RpbPingReq has an empty body."""
    return b""


def encode_rpb_get_req(bucket: bytes, key: bytes) -> bytes:
    return _pb_encode_bytes_field(1, bucket) + _pb_encode_bytes_field(2, key)


def encode_rpb_put_req(bucket: bytes, key: bytes, value: bytes) -> bytes:
    # Field tags: bucket=1, key=2, value=4. See the module
    # comment above for the v0-surface rationale.
    return (
        _pb_encode_bytes_field(1, bucket)
        + _pb_encode_bytes_field(2, key)
        + _pb_encode_bytes_field(4, value)
    )


def encode_rpb_del_req(bucket: bytes, key: bytes) -> bytes:
    return _pb_encode_bytes_field(1, bucket) + _pb_encode_bytes_field(2, key)


def decode_rpb_error_resp(body: bytes) -> tuple[bytes, int]:
    """Decode an RpbErrorResp body into ``(errmsg, errcode)``.

    Tags not in {1, 2} are skipped; this lets the decoder tolerate
    future schema additions without raising.
    """
    errmsg = b""
    errcode = 0
    pos = 0
    while pos < len(body):
        tag, pos = _pb_decode_varint(body, pos)
        field = tag >> 3
        wire = tag & 0x07
        if wire == _PB_WIRE_LENGTH_DELIMITED:
            ln, pos = _pb_decode_varint(body, pos)
            chunk = body[pos:pos + ln]
            if len(chunk) != ln:
                raise ValueError("truncated bytes field")
            pos += ln
            if field == 1:
                errmsg = chunk
        elif wire == _PB_WIRE_VARINT:
            val, pos = _pb_decode_varint(body, pos)
            if field == 2:
                errcode = val
        else:
            raise ValueError("unsupported wire type %d" % wire)
    return errmsg, errcode


class RiakPbcError(Exception):
    """Server returned an RpbErrorResp (code 0)."""


class RiakPbcConn:
    """A minimal Riak PBC client.

    Frames a single (code, body) pair on demand and reads back
    one frame. Mirrors the loopback self-connection avoidance
    trick used by the other clients.
    """

    def __init__(self, host: str, port: int, timeout: float = 5.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock: socket.socket | None = None
        self.rbuf: bytes = b""

    def connect(self) -> None:
        for _ in range(5):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
                sock.bind(("127.0.0.1", 0))
                sock.settimeout(self.timeout)
                sock.connect((self.host, self.port))
                local_port = sock.getsockname()[1]
                if local_port == self.port:
                    sock.close()
                    continue
                self.sock = sock
                self.rbuf = b""
                return
            except OSError:
                with suppress(Exception):
                    sock.close()
                raise
        raise ConnectionError(
            "could not establish a non-self-loop riak PBC connection"
        )

    def close(self) -> None:
        if self.sock is not None:
            with suppress(OSError):
                self.sock.close()
        self.sock = None
        self.rbuf = b""

    def _readn(self, n: int) -> bytes:
        assert self.sock is not None
        while len(self.rbuf) < n:
            chunk = self.sock.recv(8192)
            if not chunk:
                raise ConnectionError("peer closed mid-frame")
            self.rbuf += chunk
        out, self.rbuf = self.rbuf[:n], self.rbuf[n:]
        return out

    def call(self, code: int, body: bytes) -> tuple[int, bytes]:
        """Send one PBC request frame and return the response.

        Returns ``(reply_code, reply_body)``. Raises
        :class:`RiakPbcError` if the server returns an
        RpbErrorResp (code 0).
        """
        if self.sock is None:
            self.connect()
        # Length covers the code byte + body bytes.
        frame = struct.pack(">I", 1 + len(body)) + bytes([code]) + body
        try:
            self.sock.sendall(frame)
            length = struct.unpack(">I", self._readn(4))[0]
            if length < 1:
                raise ConnectionError("riak PBC announced zero-length frame")
            head = self._readn(1)
            reply_code = head[0]
            reply_body = self._readn(length - 1) if length > 1 else b""
        except (socket.timeout, ConnectionError, OSError):
            self.close()
            raise
        if reply_code == RIAK_CODE_ERROR_RESP:
            errmsg, errcode = decode_rpb_error_resp(reply_body)
            raise RiakPbcError(
                "riak error %d: %s" % (errcode, errmsg.decode("utf-8", "replace"))
            )
        return reply_code, reply_body


RIAK_BUCKET = b"chaos"


def riak_randkey() -> bytes:
    return (
        "k" + "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
    ).encode()


def riak_randval() -> bytes:
    return (
        "".join(random.choices(string.ascii_letters + string.digits, k=32))
    ).encode()


# Track recently-put keys so the Get workload has a >0 hit rate
# without requiring cross-call coordination. Bounded to keep
# memory flat on long soak runs.
_RIAK_RECENT_KEYS: list[bytes] = []
_RIAK_RECENT_CAP = 1024


def _remember_key(k: bytes) -> None:
    _RIAK_RECENT_KEYS.append(k)
    if len(_RIAK_RECENT_KEYS) > _RIAK_RECENT_CAP:
        # Drop the oldest quarter to amortise the trim cost.
        del _RIAK_RECENT_KEYS[: _RIAK_RECENT_CAP // 4]


def workload_riak_ping(c: RiakPbcConn) -> str:
    code, _ = c.call(RIAK_CODE_PING_REQ, encode_rpb_ping_req())
    if code != RIAK_CODE_PING_RESP:
        raise RiakPbcError("unexpected ping reply code %d" % code)
    return "Ping"


def workload_riak_put(c: RiakPbcConn) -> str:
    k = riak_randkey()
    v = riak_randval()
    code, _ = c.call(RIAK_CODE_PUT_REQ, encode_rpb_put_req(RIAK_BUCKET, k, v))
    if code != RIAK_CODE_PUT_RESP:
        raise RiakPbcError("unexpected put reply code %d" % code)
    _remember_key(k)
    return "Put"


def workload_riak_get(c: RiakPbcConn) -> str:
    # 50/50 split between a key we recently put and a fresh
    # random key, so the workload exercises both hit and miss
    # paths through the Datastore.
    if _RIAK_RECENT_KEYS and random.random() < 0.5:
        k = random.choice(_RIAK_RECENT_KEYS)
    else:
        k = riak_randkey()
    code, _ = c.call(RIAK_CODE_GET_REQ, encode_rpb_get_req(RIAK_BUCKET, k))
    if code != RIAK_CODE_GET_RESP:
        raise RiakPbcError("unexpected get reply code %d" % code)
    return "Get"


def workload_riak_del(c: RiakPbcConn) -> str:
    if _RIAK_RECENT_KEYS and random.random() < 0.5:
        k = random.choice(_RIAK_RECENT_KEYS)
    else:
        k = riak_randkey()
    code, _ = c.call(RIAK_CODE_DEL_REQ, encode_rpb_del_req(RIAK_BUCKET, k))
    if code != RIAK_CODE_DEL_RESP:
        raise RiakPbcError("unexpected del reply code %d" % code)
    return "Del"


RIAK_WORKLOADS = [
    ("riak", workload_riak_ping, 30),
    ("riak", workload_riak_put, 30),
    ("riak", workload_riak_get, 30),
    ("riak", workload_riak_del, 10),
]


# --- error classification + retry policy ---
#
# The driver classifies every raised exception into one of a
# small set of semantic error classes and consults a per-class
# retry budget before recording a failure. This matches what an
# operator-typical Dynomite client SDK does and lets the chaos
# reports separate transient gossip churn (NoTargets that clears
# in milliseconds) from genuine data unavailability (NoTargets
# that persists across retries).
#
# Classes:
#   NoTargets       -- dispatcher refused the request (no replica
#                      could be selected). Surfaced as
#                      ``-DYNOMITE: ...`` in RESP, ``SERVER_ERROR
#                      ... no quorum`` in memcache, or an
#                      ``RpbErrorResp`` whose errmsg matches
#                      "NoTargets" / "no quorum" in Riak.
#   Timeout         -- ``socket.timeout`` from a recv (read
#                      timeout) or a Riak errmsg containing
#                      "timeout".
#   Closed          -- peer reset / EOF mid-reply. Surfaced as a
#                      ``ConnectionError`` from the hand-rolled
#                      readers, or an ``OSError`` like
#                      ECONNRESET.
#   WrongConnection -- recoverable handshake-level rejection
#                      that clears after reconnecting (Redis
#                      ``-NOAUTH``).
#   Unknown         -- anything else (protocol-level, unmapped
#                      server errors). Never retried; always
#                      counted as a failure.

RETRY_DEFAULT = "NoTargets:1,Timeout:0"

RECOVERABLE_CLASSES = ("NoTargets", "Timeout", "Closed", "WrongConnection")


def parse_retry_policy(spec: str) -> dict:
    """Parse a ``--retry-on`` spec into a {class: budget} dict.

    The spec is a comma-separated list of ``Class[:N]`` entries.
    A missing ``:N`` defaults to ``:1``. An empty spec disables
    every retry. Unknown class names are rejected so a typo does
    not silently turn off retries the operator expected.
    """
    out: dict = {}
    if not spec:
        return out
    for raw in spec.split(","):
        entry = raw.strip()
        if not entry:
            continue
        if ":" in entry:
            cls, n_str = entry.split(":", 1)
            cls = cls.strip()
            n_str = n_str.strip()
            if not n_str:
                raise ValueError(
                    "missing budget after ':' in retry spec entry: %r" % entry
                )
            try:
                n = int(n_str)
            except ValueError:
                raise ValueError(
                    "non-integer retry budget in entry: %r" % entry
                )
            if n < 0:
                raise ValueError(
                    "negative retry budget in entry: %r" % entry
                )
        else:
            cls = entry
            n = 1
        if cls not in RECOVERABLE_CLASSES:
            raise ValueError(
                "unknown error class %r in retry spec; valid classes: %s"
                % (cls, ",".join(RECOVERABLE_CLASSES))
            )
        out[cls] = n
    return out


def classify_error(exc: BaseException, mode: str) -> str:
    """Map a raised exception to one of the semantic error classes.

    ``mode`` selects the protocol-specific server-error parser
    (``redis``, ``memcache``, or ``riak``). Anything that does
    not match a known recoverable shape is reported as
    ``"Unknown"`` and the caller treats it as a non-retryable
    failure.
    """
    # ``socket.timeout`` is a subclass of ``OSError`` on Python 3,
    # so check it before the OSError fallback.
    if isinstance(exc, socket.timeout):
        return "Timeout"
    if isinstance(exc, ConnectionError):
        # Our hand-rolled readers raise ``ConnectionError`` with
        # a "peer closed" / "could not establish" message on EOF
        # mid-reply or self-loop avoidance. Both are recoverable
        # by reconnecting.
        return "Closed"
    msg = ""
    if exc.args:
        first = exc.args[0]
        msg = first if isinstance(first, str) else str(first)
    if mode == "redis" and isinstance(exc, RespError):
        # The dispatcher prepends ``-DYNOMITE: `` (or a
        # capitalised ``-Dynomite: ``) to every operational
        # error. The leading minus is stripped by the RESP
        # reader; we see the bare prefix.
        # Native Redis errors like ``NOAUTH Authentication required``
        # separate the prefix from the body with a space rather
        # than a colon, so split on whichever appears first.
        token = msg
        for _sep in (":", " "):
            _i = token.find(_sep)
            if _i >= 0:
                token = token[:_i]
        head = token.strip().upper()
        if head == "DYNOMITE":
            return "NoTargets"
        if head == "NOAUTH":
            return "WrongConnection"
        return "Unknown"
    if mode == "memcache" and isinstance(exc, MemcacheError):
        low = msg.lower()
        if "server_error" in low and ("no quorum" in low or "notargets" in low):
            return "NoTargets"
        return "Unknown"
    if mode == "riak" and isinstance(exc, RiakPbcError):
        low = msg.lower()
        if "notargets" in low or "no targets" in low or "no quorum" in low:
            return "NoTargets"
        if "timeout" in low:
            return "Timeout"
        return "Unknown"
    if isinstance(exc, OSError):
        # ECONNRESET / EPIPE / similar; the connection is gone
        # and can be reopened. Treat as Closed for retry
        # purposes.
        return "Closed"
    return "Unknown"


def run_with_retry(
    workload_fn,
    conn,
    mode: str,
    policy: dict,
    retries: dict,
    cls_name: str,
) -> tuple:
    """Execute one workload op, applying the configured retry policy.

    Returns ``(op, err_class)``:

    * On success: ``(op_name, None)``. ``op_name`` is whatever
      ``workload_fn`` returned.
    * On final failure: ``(None, err_class)``. ``err_class`` is
      the semantic class of the LAST attempt's exception (which
      matches the first attempt's class for any retried request,
      because a class only enters the policy if it is
      recoverable).

    Per-attempt retry consumption is recorded into ``retries``
    keyed by ``"<cls_name>/<err_class>"`` so the per-second
    NDJSON window picks them up. Each retry consumes 1 from the
    per-class budget; budgets are reset per call (i.e. each
    workload op gets a full fresh budget).
    """
    remaining = dict(policy)
    while True:
        try:
            op = workload_fn(conn)
            return op, None
        except BaseException as exc:  # noqa: BLE001
            err_class = classify_error(exc, mode)
            # Always close the connection on error so the next
            # attempt forces a reconnect. This mirrors what the
            # earlier monolithic loop did and keeps the retry
            # honest about reproducing what a real client SDK
            # observes.
            with suppress(Exception):
                conn.close()
            budget = remaining.get(err_class, 0)
            if err_class in RECOVERABLE_CLASSES and budget > 0:
                remaining[err_class] = budget - 1
                key = cls_name + "/" + err_class
                retries[key] = retries.get(key, 0) + 1
                continue
            return None, err_class


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
    p.add_argument("--mode", default="redis",
                   choices=("redis", "memcache", "riak"),
                   help="protocol to drive")
    p.add_argument("--riak-pbc-port", type=int, default=21800,
                   help="Riak PBC listener port (only used when "
                        "--mode riak); defaults to 21800")
    p.add_argument("--retry-on", default=RETRY_DEFAULT,
                   help="Comma-separated list of recoverable error "
                        "classes with optional :N retry budget, e.g. "
                        "'NoTargets:1,Timeout:0'. Empty string disables "
                        "all retries (matches the pre-2026-05-25 "
                        "behaviour where every error counted as a "
                        "failure). Valid classes: "
                        + ",".join(RECOVERABLE_CLASSES)
                        + ". Default: " + RETRY_DEFAULT)
    args = p.parse_args()

    try:
        retry_policy = parse_retry_policy(args.retry_on)
    except ValueError as exc:
        print("invalid --retry-on: %s" % exc, file=sys.stderr)
        return 2

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    # Truncate on each run; the coordinator manages run-id
    # subdirs so we never want to mix sessions in one file.
    f = out.open("w", buffering=1)

    counts: dict[tuple[str, str], int] = {}
    failures: dict[tuple[str, str], int] = {}
    retries: dict[str, int] = {}
    last_flush = time.monotonic()
    started = time.monotonic()

    effective_mode = args.mode

    if effective_mode == "memcache":
        workloads = MEMCACHE_WORKLOADS
        conn = MemcacheConn(args.host, args.port)
        net_errors = (MemcacheError, ConnectionError, socket.timeout, OSError)
    elif effective_mode == "riak":
        workloads = RIAK_WORKLOADS
        # Riak PBC binds to its own port (riak.pbc_listen),
        # not the engine's client_listen, so we ignore --port
        # and dial --riak-pbc-port instead.
        conn = RiakPbcConn(args.host, args.riak_pbc_port)
        net_errors = (RiakPbcError, ConnectionError, socket.timeout, OSError)
    else:
        workloads = WORKLOADS
        conn = RespConn(args.host, args.port)
        net_errors = (RespError, ConnectionError, socket.timeout, OSError)

    weights = [w for _, _, w in workloads]
    total_weight = sum(weights)

    sleep_per_op = 1.0 / args.qps if args.qps > 0 else 0.0

    while _RUNNING:
        if args.duration and (time.monotonic() - started) >= args.duration:
            break
        roll = random.random() * total_weight
        acc = 0
        chosen_class = workloads[-1]
        for entry in workloads:
            acc += entry[2]
            if roll < acc:
                chosen_class = entry
                break
        cls_name, fn, _ = chosen_class
        try:
            op, err_class = run_with_retry(
                fn, conn, effective_mode, retry_policy, retries, cls_name
            )
        except net_errors as exc:
            # ``run_with_retry`` traps every exception itself, but
            # we keep this branch as a last-resort net so an
            # unexpected error type cannot kill the driver.
            err_class = classify_error(exc, effective_mode)
            op = None
        if op is not None:
            counts[(cls_name, op)] = counts.get((cls_name, op), 0) + 1
        else:
            key = (cls_name, err_class)
            failures[key] = failures.get(key, 0) + 1
            # Log a small sample of failures to stderr so the
            # operator can correlate with dynomited / backend logs.
            if failures[key] <= 5:
                print(
                    f"[{args.label}] {cls_name} call failed: "
                    f"{err_class}",
                    file=sys.stderr,
                    flush=True,
                )
        if sleep_per_op > 0:
            time.sleep(sleep_per_op)

        now = time.monotonic()
        if now - last_flush >= 10.0:
            row = {
                "ts": time.time(),
                "label": args.label,
                "mode": effective_mode,
                "elapsed": now - started,
                "counts": {f"{c}/{o}": v for (c, o), v in counts.items()},
                "failures": {f"{c}/{e}": v for (c, e), v in failures.items()},
                "retries": dict(retries),
            }
            f.write(json.dumps(row) + "\n")
            counts.clear()
            failures.clear()
            retries.clear()
            last_flush = now

    # final flush
    row = {
        "ts": time.time(),
        "label": args.label,
        "mode": effective_mode,
        "elapsed": time.monotonic() - started,
        "counts": {f"{c}/{o}": v for (c, o), v in counts.items()},
        "failures": {f"{c}/{e}": v for (c, e), v in failures.items()},
        "retries": dict(retries),
        "final": True,
    }
    f.write(json.dumps(row) + "\n")
    f.close()
    return 0


# --- unit tests ---
#
# Run with ``python3 workload-driver.py --self-test``. The chaos
# coordinator never invokes this path; CI does, before promoting
# a build to a chaos host.


class _RiakPbcEncodingTests(unittest.TestCase):
    def test_varint_round_trip(self) -> None:
        for n in [0, 1, 127, 128, 255, 256, 16383, 16384, 1 << 20, 1 << 32]:
            buf = _pb_encode_varint(n)
            decoded, pos = _pb_decode_varint(buf, 0)
            self.assertEqual(decoded, n)
            self.assertEqual(pos, len(buf))

    def test_ping_req_is_empty(self) -> None:
        body = encode_rpb_ping_req()
        self.assertEqual(body, b"")
        # Frame layout for ping: length=1 (just the code), code=1.
        # We assemble the frame the way RiakPbcConn.call would.
        frame = struct.pack(">I", 1 + len(body)) + bytes([RIAK_CODE_PING_REQ]) + body
        self.assertEqual(frame, b"\x00\x00\x00\x01\x01")

    def test_put_req_wire_bytes(self) -> None:
        # bucket=foo (3 bytes), key=bar (3 bytes), value=baz (3 bytes)
        # at field tags 1, 2, 4 respectively. Each field is
        # tag-byte (single-byte varint for these field numbers)
        # then length-byte then the bytes.
        #   field 1 bytes: tag=0x0a, len=0x03, b"foo"
        #   field 2 bytes: tag=0x12, len=0x03, b"bar"
        #   field 4 bytes: tag=0x22, len=0x03, b"baz"
        body = encode_rpb_put_req(b"foo", b"bar", b"baz")
        expected = (
            b"\x0a\x03foo"
            b"\x12\x03bar"
            b"\x22\x03baz"
        )
        self.assertEqual(body, expected)

    def test_get_req_wire_bytes(self) -> None:
        body = encode_rpb_get_req(b"chaos", b"k01234567")
        expected = (
            b"\x0a\x05chaos"
            b"\x12\x09k01234567"
        )
        self.assertEqual(body, expected)

    def test_del_req_wire_bytes(self) -> None:
        body = encode_rpb_del_req(b"chaos", b"k01234567")
        expected = (
            b"\x0a\x05chaos"
            b"\x12\x09k01234567"
        )
        self.assertEqual(body, expected)

    def test_error_resp_round_trips(self) -> None:
        # Build an RpbErrorResp(errmsg="boom", errcode=42) by hand
        # and confirm decode_rpb_error_resp recovers both fields.
        errmsg = b"boom"
        errcode = 42
        body = (
            _pb_encode_bytes_field(1, errmsg)
            + _pb_encode_tag(2, _PB_WIRE_VARINT)
            + _pb_encode_varint(errcode)
        )
        got_msg, got_code = decode_rpb_error_resp(body)
        self.assertEqual(got_msg, errmsg)
        self.assertEqual(got_code, errcode)

    def test_error_resp_skips_unknown_fields(self) -> None:
        # An RpbErrorResp that also carries a hypothetical field 3
        # varint should still decode the known fields.
        body = (
            _pb_encode_bytes_field(1, b"oops")
            + _pb_encode_tag(3, _PB_WIRE_VARINT)
            + _pb_encode_varint(99)
            + _pb_encode_tag(2, _PB_WIRE_VARINT)
            + _pb_encode_varint(7)
        )
        got_msg, got_code = decode_rpb_error_resp(body)
        self.assertEqual(got_msg, b"oops")
        self.assertEqual(got_code, 7)

    def test_long_value_uses_multi_byte_length(self) -> None:
        # A 200-byte value forces the length varint to use two
        # bytes (200 = 0xc8 = 0b11001000 -> 0xc8 0x01).
        big = b"x" * 200
        body = encode_rpb_put_req(b"b", b"k", big)
        # field 4 tag=0x22, len varint = 0xc8 0x01, then 200 bytes
        idx = body.index(b"\x22\xc8\x01")
        self.assertEqual(body[idx + 3:idx + 3 + 200], big)


class _RetryPolicyParseTests(unittest.TestCase):
    def test_empty_spec_returns_empty_dict(self) -> None:
        self.assertEqual(parse_retry_policy(""), {})

    def test_default_spec_parses(self) -> None:
        got = parse_retry_policy(RETRY_DEFAULT)
        self.assertEqual(got, {"NoTargets": 1, "Timeout": 0})

    def test_missing_budget_defaults_to_one(self) -> None:
        got = parse_retry_policy("NoTargets,Timeout:2")
        self.assertEqual(got, {"NoTargets": 1, "Timeout": 2})

    def test_full_policy_parses(self) -> None:
        got = parse_retry_policy("NoTargets:3,Timeout:1,Closed:0,WrongConnection:2")
        self.assertEqual(
            got,
            {"NoTargets": 3, "Timeout": 1, "Closed": 0, "WrongConnection": 2},
        )

    def test_unknown_class_rejected(self) -> None:
        with self.assertRaises(ValueError):
            parse_retry_policy("BogusClass:1")

    def test_negative_budget_rejected(self) -> None:
        with self.assertRaises(ValueError):
            parse_retry_policy("Timeout:-1")

    def test_non_integer_budget_rejected(self) -> None:
        with self.assertRaises(ValueError):
            parse_retry_policy("Timeout:nope")

    def test_whitespace_tolerant(self) -> None:
        got = parse_retry_policy(" NoTargets : 2 ,  Timeout : 0 ")
        self.assertEqual(got, {"NoTargets": 2, "Timeout": 0})


class _ClassifyErrorTests(unittest.TestCase):
    def test_socket_timeout_is_timeout(self) -> None:
        self.assertEqual(classify_error(socket.timeout("read"), "redis"), "Timeout")
        self.assertEqual(
            classify_error(socket.timeout("read"), "memcache"), "Timeout"
        )
        self.assertEqual(classify_error(socket.timeout("read"), "riak"), "Timeout")

    def test_connection_error_is_closed(self) -> None:
        self.assertEqual(
            classify_error(ConnectionError("peer closed mid-reply"), "redis"),
            "Closed",
        )

    def test_redis_dynomite_prefix_is_no_targets(self) -> None:
        self.assertEqual(
            classify_error(RespError("DYNOMITE: no quorum"), "redis"),
            "NoTargets",
        )
        # Title-cased prefix still matches.
        self.assertEqual(
            classify_error(RespError("Dynomite: no replicas"), "redis"),
            "NoTargets",
        )

    def test_redis_noauth_is_wrong_connection(self) -> None:
        self.assertEqual(
            classify_error(RespError("NOAUTH Authentication required"), "redis"),
            "WrongConnection",
        )

    def test_redis_unknown_resp_error(self) -> None:
        self.assertEqual(
            classify_error(RespError("WRONGTYPE Operation against a wrong key"),
                           "redis"),
            "Unknown",
        )

    def test_memcache_no_quorum_is_no_targets(self) -> None:
        self.assertEqual(
            classify_error(MemcacheError("SERVER_ERROR no quorum"), "memcache"),
            "NoTargets",
        )

    def test_memcache_unrelated_server_error_is_unknown(self) -> None:
        self.assertEqual(
            classify_error(MemcacheError("SERVER_ERROR out of memory"),
                           "memcache"),
            "Unknown",
        )

    def test_riak_errmsg_no_targets(self) -> None:
        self.assertEqual(
            classify_error(RiakPbcError("riak error 1: NoTargets"), "riak"),
            "NoTargets",
        )
        self.assertEqual(
            classify_error(RiakPbcError("riak error 0: no quorum reached"),
                           "riak"),
            "NoTargets",
        )

    def test_riak_errmsg_timeout(self) -> None:
        self.assertEqual(
            classify_error(RiakPbcError("riak error 0: timeout waiting"), "riak"),
            "Timeout",
        )

    def test_riak_errmsg_unknown(self) -> None:
        self.assertEqual(
            classify_error(RiakPbcError("riak error 0: malformed request"),
                           "riak"),
            "Unknown",
        )

    def test_oserror_is_closed(self) -> None:
        self.assertEqual(classify_error(OSError("ECONNRESET"), "redis"), "Closed")


class _FakeConn:
    """Stand-in connection for retry-loop tests.

    Records ``close()`` calls so a test can confirm the retry
    loop actually drops the socket between attempts.
    """

    def __init__(self) -> None:
        self.closed = 0

    def close(self) -> None:
        self.closed += 1


def _scripted_workload(script: list):
    """Build a workload_fn that walks the given script.

    Each script entry is either an exception instance (raised
    on that attempt) or a string (returned as the op name on a
    successful attempt).
    """
    state = {"i": 0}

    def fn(_conn) -> str:
        i = state["i"]
        state["i"] = i + 1
        if i >= len(script):
            raise AssertionError(
                "workload script exhausted at attempt %d" % i
            )
        item = script[i]
        if isinstance(item, BaseException):
            raise item
        return item

    fn.calls = state  # type: ignore[attr-defined]
    return fn


class _RunWithRetryTests(unittest.TestCase):
    def test_first_try_success(self) -> None:
        retries: dict = {}
        fn = _scripted_workload(["SET"])
        op, err = run_with_retry(
            fn, _FakeConn(), "redis",
            parse_retry_policy("NoTargets:1,Timeout:0"),
            retries, "strings",
        )
        self.assertEqual(op, "SET")
        self.assertIsNone(err)
        self.assertEqual(retries, {})
        self.assertEqual(fn.calls["i"], 1)

    def test_no_targets_then_success(self) -> None:
        retries: dict = {}
        fn = _scripted_workload([
            RespError("DYNOMITE: no quorum"),
            "SET",
        ])
        conn = _FakeConn()
        op, err = run_with_retry(
            fn, conn, "redis",
            parse_retry_policy("NoTargets:1,Timeout:0"),
            retries, "strings",
        )
        self.assertEqual(op, "SET")
        self.assertIsNone(err)
        self.assertEqual(retries, {"strings/NoTargets": 1})
        # Connection was closed once between attempts.
        self.assertEqual(conn.closed, 1)
        self.assertEqual(fn.calls["i"], 2)

    def test_no_targets_twice_with_budget_one_fails(self) -> None:
        retries: dict = {}
        fn = _scripted_workload([
            RespError("DYNOMITE: no quorum"),
            RespError("DYNOMITE: no quorum"),
        ])
        op, err = run_with_retry(
            fn, _FakeConn(), "redis",
            parse_retry_policy("NoTargets:1,Timeout:0"),
            retries, "strings",
        )
        self.assertIsNone(op)
        self.assertEqual(err, "NoTargets")
        # Budget=1 means exactly one retry was attempted and
        # consumed; the second NoTargets exhausts the budget
        # and counts as a failure.
        self.assertEqual(retries, {"strings/NoTargets": 1})
        self.assertEqual(fn.calls["i"], 2)

    def test_timeout_with_zero_budget_fails_immediately(self) -> None:
        retries: dict = {}
        fn = _scripted_workload([socket.timeout("read")])
        op, err = run_with_retry(
            fn, _FakeConn(), "redis",
            parse_retry_policy("NoTargets:1,Timeout:0"),
            retries, "hash",
        )
        self.assertIsNone(op)
        self.assertEqual(err, "Timeout")
        self.assertEqual(retries, {})
        self.assertEqual(fn.calls["i"], 1)

    def test_unknown_error_class_is_not_retried(self) -> None:
        retries: dict = {}
        fn = _scripted_workload([
            RespError("WRONGTYPE Operation against a wrong key"),
        ])
        op, err = run_with_retry(
            fn, _FakeConn(), "redis",
            parse_retry_policy("NoTargets:3,Timeout:3"),
            retries, "strings",
        )
        self.assertIsNone(op)
        self.assertEqual(err, "Unknown")
        self.assertEqual(retries, {})
        self.assertEqual(fn.calls["i"], 1)

    def test_empty_policy_disables_all_retries(self) -> None:
        retries: dict = {}
        fn = _scripted_workload([
            RespError("DYNOMITE: no quorum"),
        ])
        op, err = run_with_retry(
            fn, _FakeConn(), "redis",
            parse_retry_policy(""),
            retries, "strings",
        )
        self.assertIsNone(op)
        self.assertEqual(err, "NoTargets")
        self.assertEqual(retries, {})

    def test_retry_budget_resets_per_call(self) -> None:
        # Two independent invocations with budget=1 each: the
        # second invocation should still get its full retry,
        # not inherit a depleted budget from the first.
        policy = parse_retry_policy("NoTargets:1")
        retries: dict = {}

        fn1 = _scripted_workload([
            RespError("DYNOMITE: no quorum"),
            "SET",
        ])
        op1, _ = run_with_retry(
            fn1, _FakeConn(), "redis", policy, retries, "strings",
        )
        self.assertEqual(op1, "SET")

        fn2 = _scripted_workload([
            RespError("DYNOMITE: no quorum"),
            "GET",
        ])
        op2, _ = run_with_retry(
            fn2, _FakeConn(), "redis", policy, retries, "strings",
        )
        self.assertEqual(op2, "GET")
        self.assertEqual(retries, {"strings/NoTargets": 2})

    def test_mixed_classes_consume_separate_budgets(self) -> None:
        # A Timeout followed by a NoTargets with budgets 1/1
        # should retry both and ultimately succeed.
        retries: dict = {}
        fn = _scripted_workload([
            socket.timeout("read"),
            RespError("DYNOMITE: no quorum"),
            "SET",
        ])
        op, err = run_with_retry(
            fn, _FakeConn(), "redis",
            parse_retry_policy("NoTargets:1,Timeout:1"),
            retries, "strings",
        )
        self.assertEqual(op, "SET")
        self.assertIsNone(err)
        self.assertEqual(
            retries,
            {"strings/Timeout": 1, "strings/NoTargets": 1},
        )


if __name__ == "__main__":
    if "--self-test" in sys.argv:
        sys.argv.remove("--self-test")
        unittest.main(argv=sys.argv, verbosity=2)
    else:
        sys.exit(main())
