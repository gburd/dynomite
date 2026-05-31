# 2026-05-25: chaos workload driver gains a Riak PBC mode

Stage: chaos-multi-host
Branch: stage/chaos-riak-mode

## Summary

`scripts/chaos-multi-host/workload-driver.py` now supports a third
mode (in addition to `redis` and `memcache`): `--mode riak`. In
this mode the driver dials the engine's `riak.pbc_listen` socket
and exercises four Riak PBC operations:

* `RpbPing`   (req code 1, resp code 2): empty body
* `RpbPut`    (req code 11, resp code 12): bucket, key, value
* `RpbGet`    (req code 9, resp code 10): bucket, key
* `RpbDel`    (req code 13, resp code 14): bucket, key

All operations target the bucket `chaos`. The per-second mix is
30% Ping, 30% Put (random 8-char key, random 32-byte value),
30% Get, 10% Del. Get and Del each pick from a recently-put key
list with 50% probability so the workload exercises both hit and
miss paths through the dyniak `Datastore`.

The `start-host.sh` Riak placeholder (which previously logged a
warning and fell back to redis) is replaced with real wiring that
configures `riak.pbc_listen: 0.0.0.0:21800` in the dynomited YAML
and verifies the binary was built with `--features riak` by
probing for the `--riak-pbc-listen` CLI flag in `--help`. If the
flag is missing the script aborts with a clear error; it does not
silently fall back. The `coordinator.sh` exports `MODE` to every
host and threads `RIAK_PBC_PORT=21800` through the per-host
start-args.

## Wire format (hand-rolled protobuf via stdlib `struct`)

The driver has no third-party Python dependencies. Each PBC
frame is `4-byte BE length + 1-byte code + body` exactly per
`crates/dyniak/src/proto/pb/framer.rs`. Inside the body, every
field is `(field_number << 3) | wire_type` varint-tagged:

* wire type 0 = varint (uint32, bool); used here for
  `RpbErrorResp.errcode`.
* wire type 2 = length-delimited (bytes, string, embedded
  message); used here for every bucket/key/value field.

Field tags match the dyniak crate's `proto::pb::messages`
schema:

| Message     | Fields                                |
| ----------- | ------------------------------------- |
| RpbPingReq  | (empty)                               |
| RpbGetReq   | bucket=1, key=2 (both bytes)          |
| RpbPutReq   | bucket=1, key=2, value=4 (all bytes)  |
| RpbDelReq   | bucket=1, key=2 (both bytes)          |
| RpbErrorResp| errmsg=1 (bytes), errcode=2 (varint)  |

### Note on RpbPutReq's `value` tag

Upstream Riak places the object value inside a nested
`RpbContent` at `RpbPutReq.content` (field tag 3), with the
opaque bytes at `RpbContent.value` (field tag 1). The dyniak
v0 surface flattens this to a single top-level
`RpbPutReq.value: bytes` at field tag 4 (see
`crates/dyniak/src/proto/pb/messages.rs` and the parity table
in `docs/parity.md` for the rationale). The chaos workload driver
matches the dyniak wire shape -- field 4 bytes -- because the
server is the system under test; it must be able to decode every
frame the driver emits. The user-facing brief described the
upstream-Riak layout; matching the in-tree server schema is the
deviation that keeps the smoke loop honest. If the dyniak
follow-up slice replaces the flat `value` field with a nested
`RpbContent` at tag 3, this driver needs a single corresponding
edit in `encode_rpb_put_req`.

## Datastore wiring

`MODE=riak` keeps `data_store: 0` (Redis) for the engine's
front-side dispatcher and starts a local `redis-server`. The
workload driver does NOT talk to the front-side dispatcher in
this mode -- it talks directly to the `riak.pbc_listen` socket,
which `dynomited`'s `--features riak` startup wiring backs with
the in-process `MemoryDatastore` whenever no `noxu_path` is
configured. That keeps the chaos infra (redis bring-up, redis
bouncer in the chaos injector, etc.) working without changes,
while the PBC traffic flows through dyniak's listener and the
in-process datastore. Side effect: bouncing redis in `MODE=riak`
does not perturb the PBC workload (redis is no longer on the
PBC request path); the chaos test in this mode primarily exercises
process stability under SIGSTOP/SIGKILL of `dynomited` itself.

## Tests

Unit tests at the bottom of `workload-driver.py` (Python's
built-in `unittest`):

* `test_varint_round_trip`: 0 .. 1<<32 round-trips cleanly.
* `test_ping_req_is_empty`: empty body, frame is exactly the
  five bytes `00 00 00 01 01`.
* `test_put_req_wire_bytes`: encode `(foo, bar, baz)` and assert
  byte-for-byte equality with the hand-derived expected wire
  bytes.
* `test_get_req_wire_bytes`, `test_del_req_wire_bytes`: same for
  get/del.
* `test_error_resp_round_trips`: build an RpbErrorResp body by
  hand and confirm `decode_rpb_error_resp` recovers
  `(errmsg, errcode)`.
* `test_error_resp_skips_unknown_fields`: tolerates an unknown
  varint field interleaved between known fields.
* `test_long_value_uses_multi_byte_length`: a 200-byte value
  forces the length varint to two bytes (`c8 01`); confirms the
  length encoder handles the wrap point.

Run with:
```
python3 scripts/chaos-multi-host/workload-driver.py --self-test
```
All eight tests pass.

## Smoke

Manual smoke (running `dynomited --features riak` for 30s with
`MODE=riak` against a local instance) is gated on the build
finishing on the lead host (floki). The per-host `start-host.sh`
abort path is exercised by the unit-test build because
`workload-driver.py --self-test` exits cleanly without touching
the network.

## Files touched

* `scripts/chaos-multi-host/workload-driver.py` -- new RiakPbcConn,
  protobuf encoders/decoders, riak workload mix, unit tests, and
  CLI plumbing for `--riak-pbc-port`.
* `scripts/chaos-multi-host/start-host.sh` -- real `MODE=riak`
  branch (no fallback), dynomited-binary feature probe, YAML
  `riak.pbc_listen` block, new positional arg `RIAK_PBC_PORT`.
* `scripts/chaos-multi-host/coordinator.sh` -- exports `MODE`,
  threads `RIAK_PBC_PORT` through start-args and start-host
  invocations, switches the workload-driver invocation to
  `--mode riak --riak-pbc-port` when `MODE=riak`.
* `docs/journal/2026-05-25-chaos-riak-mode.md` -- this file.

No `crates/`, `docs/book/`, or other docs/journal files are
touched.
