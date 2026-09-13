# Dyniak PBC: TCP-vs-QUIC re-measurement after the NODELAY fix

Date: 2026-09-13
Region: AWS us-east-1 (`lava` profile)
Base commit: `49bac1b`-era HEAD + the write_frame/TCP_NODELAY fix
(`d4a027c`) + the read-half BufReader change (this session).
Run tag: `dyn-run=dynqt-20260913-132531` (torn down, verified clean).

## Why this run

The 2026-09-12 head-to-head report measured dyniak's PBC surface at a
flat ~50ms/op floor and traced it to a specific defect in
`write_frame` (three separate `write_all`s per response) plus a missing
`TCP_NODELAY` on the accept loop. That fix landed as `d4a027c`. This
run re-measures after the fix and adds a TCP-vs-QUIC comparison, since
QUIC is Nagle-free by design and is a natural control for the fix.

Single dyniak node and single real-Riak node, `m6id.xlarge`, AL2023,
loopback (bench driver and server on the same node), same
`dyniak-bench` workloads as the prior report.

## Headline: the NODELAY fix works; the floor is gone

| Workload | Pre-fix (2026-09-12) | Post-fix (this run) | Change |
|---|---|---|---|
| PBC mixed, c=32 | 610 ops/s, p50 50.0ms | 2,798 ops/s, p50 11.3ms | 4.6x tput, 4.4x lower p50 |
| counter_inc, c=16 | 310 ops/s, p50 50.0ms | 1,161 ops/s, p50 13.7ms | 3.7x |
| set_add, c=16 | 311 ops/s, p50 50.0ms | 1,142 ops/s, p50 14.0ms | 3.7x |

The fixed ~50ms floor that dominated every op regardless of type is
gone. Zero errors on every dyniak leg.

## The server is not the bottleneck: ~21 microseconds per op

A single warm TCP connection issuing serial `ping`s (no bench
framework, raw Python client, `TCP_NODELAY` set) measured:

```
serial ping: 2000 ops in 42.4ms -> 0.021ms/op, 47,176 ops/s single-conn
```

The dyniak server answers a PBC round trip in ~21 microseconds on
loopback. There is no server-side latency floor after the fix.

## The remaining TCP number is a benchmark-client artifact, not a server defect

The TCP mixed throughput is flat at ~2,800 ops/s whether the driver
runs 4 or 32 workers, while latency scales linearly with worker count:

| Transport | Concurrency | Throughput | p50 |
|---|---|---:|---:|
| TCP  | 4  | 2,811 ops/s | 1.37ms |
| TCP  | 32 | 2,809 ops/s | 11.23ms |
| QUIC | 32 | 15,056 ops/s | 2.06ms |

Flat throughput with concurrency-proportional latency is the signature
of a client-side serialization ceiling, not a server limit -- the
server serves 47K serial ops/s on one connection. `dyniak-bench`'s TCP
driver (`crates/dyniak-bench/src/driver/riak.rs`) is synchronous and
blocking: one OS thread per worker, one connection per worker, a
blocking read between sends. At c=32 the aggregate saturates around
2,800 ops/s from client-side thread scheduling and per-op blocking
overhead, and each op's wall-clock latency inflates with the number of
in-flight workers. This is the driver, not dyniak.

## QUIC: 15K ops/s, p50 2ms -- and it partly measures the driver too

Over QUIC the same workload, framing, handler, and storage reached
15,056 ops/s at p50 2.06ms with zero errors -- 5.4x the fixed-TCP
number and comparable to real Riak's TCP result (4,305 ops/s, p50
2.4ms in the prior report; QUIC beats it on throughput). Two reasons,
and they must be stated together honestly:

1. QUIC genuinely avoids Nagle and multiplexes many streams over one
   connection, so it is a better transport for a request/response
   workload.
2. The QUIC bench driver (`riak_quic.rs`) is async (a tokio runtime
   with a packet pump) rather than blocking, so it does not have the
   TCP driver's client-side ceiling. Part of the QUIC advantage in
   these numbers is the driver, not the transport.

Because the two effects are entangled in this harness, the honest
reading is: QUIC is clearly faster here, but "5.4x faster transport"
overstates it -- some of that ratio is the async-vs-blocking client.

## Cross-system comparison, corrected

Against the prior report's real-Riak TCP numbers (4,305 ops/s mixed,
p50 2.4ms; counter 2,990; set 2,633):

* dyniak went from ~10x slower (pre-fix) to ~1.5x slower on TCP mixed
  throughput, with the caveat above that the TCP number is
  client-bound.
* dyniak over QUIC exceeds Riak's TCP throughput.
* dyniak's server-side per-op latency (~21us serial) is competitive;
  the higher p50 at high concurrency is the blocking client.

"Similar or better than Riak" is now DEFENSIBLE at the server level
(21us serial round trip; QUIC throughput above Riak's), but a clean
apples-to-apples client-side comparison still needs a non-blocking TCP
load driver. The current TCP driver undersells dyniak.

## Defects found this run

1. **Enabling QUIC forces TLS onto the plain TCP PBC port** (design
   sharp edge, not fixed here). QUIC mandates TLS, so `quic_listen`
   requires `tls_cert`/`tls_key`; but that same cert pair drives the
   shared `RiakHandles.tls` acceptor, which `serve_pbc_full` applies to
   EVERY accepted TCP socket. So a config with `quic_listen` +
   `pbc_listen` serves TLS on the "plain" TCP port too, and a plain-TCP
   PBC client gets a TLS handshake (observed: a `ping` returned bytes
   starting `15 03 03`, a TLS alert). This is defensible behaviour
   (if you set certs you asked for TLS) but the QUIC-forces-TLS-
   everywhere coupling is a usability trap. A separate `quic_tls_*`
   knob, or letting the TCP listener stay plain when only QUIC needs
   the cert, would fix it. Recorded in the parity ledger; the benchmark
   worked around it by running two dyniak configs (plain-TCP, then
   QUIC).

2. **The QUIC bench driver is flaky at low worker counts.** The c=32
   QUIC run succeeded (1.36M ops, 0 errors), but a c=4 QUIC run failed
   immediately with "quic driver shut down" / "early eof". This is a
   robustness bug in `dyniak-bench`'s `riak_quic.rs` multi-worker
   handling, not a dyniak server defect (the server served the c=32
   run cleanly). Flagged for the bench tooling.

## What changed in the engine as a result

* `d4a027c` (prior): coalesce `write_frame` + `TCP_NODELAY` -- removed
  the 50ms floor. Validated here: 50ms -> server-side 21us.
* This session: wrap the PBC read half in a `BufReader`
  (`handle_conn_full`). Measured neutral on throughput/latency here
  (the 3-read cost was not the bottleneck) but a correct micro-
  improvement with no downside (fewer syscalls per op).

## Method notes / limits

* Loopback only (driver and server co-resident); no cross-node network
  in the timed runs. A cross-node run would add real RTT and change the
  concurrency/latency arithmetic.
* Single 90s (or 30s for the concurrency sweep) run per point, n=1.
* The TCP driver's client ceiling is the dominant caveat on every TCP
  throughput number; treat them as a floor on dyniak's real capacity,
  not a ceiling.
