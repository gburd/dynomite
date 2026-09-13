# Dyniak PBC: async-driver re-measurement + TCP-vs-QUIC + Riak head-to-head

Date: 2026-09-13 (second run)
Region: AWS us-east-1 (`lava` profile)
Base commit: HEAD with the async bench driver, Map wire-schema fix,
bucket_type storage-key fold, and the QUIC-only TLS cert split.
Run tag: `dyn-run=dynqt2-20260913-163626` (torn down, verified clean).

## What changed since the first TCP-vs-QUIC run

The first run (2026-09-13, report `dyniak-tcp-vs-quic-2026-09-13.md`)
found that dyniak's TCP throughput was capped by the BLOCKING
`dyniak-bench` TCP driver, not the server. This run re-measures after:

1. The TCP driver was rewritten to be async (per-worker tokio runtime,
   buffered async socket) mirroring the QUIC driver.
2. The Map CRDT wire schema was aligned with upstream `riak_dt.proto`.
3. `bucket_type` is now folded into the storage key.
4. A QUIC-only TLS cert pair (`quic_tls_cert` / `quic_tls_key`) lets the
   QUIC listener terminate TLS while the TCP PBC listener stays
   plaintext.

## Headline: the async driver fixed the gap; dyniak beats Riak

| System | Workload | ops/s | p50 (ms) | p99 (ms) | errors |
|---|---|---:|---:|---:|---:|
| dyniak | PBC mixed (TCP, async) | 18,186 | 1.72 | 3.73 | 0 |
| dyniak | PBC mixed (QUIC) | 13,800 | 2.25 | 4.54 | 0 |
| Riak 2.2.3 | PBC mixed (TCP) | 4,261 | 2.46 | 29.44 | 1 |
| dyniak | counter_inc (TCP) | 7,245 | 2.20 | 4.04 | 0 |
| Riak 2.2.3 | counter_inc (TCP) | 2,943 | 5.37 | 8.80 | 0 |
| dyniak | set_add (TCP) | 5,833 | 2.76 | 5.36 | 0 |
| Riak 2.2.3 | set_add (TCP) | 2,588 | 6.13 | 10.07 | 0 |

## Findings

**The TCP-vs-QUIC discrepancy was a load-driver artifact, now removed.**
The blocking TCP driver capped aggregate throughput at ~2,800 ops/s
regardless of worker count (first run); the async driver reaches 18,186
ops/s at p50 1.72ms -- a 6.5x improvement on the identical server
build. TCP now slightly EXCEEDS QUIC in this harness (18.2K vs 13.8K),
which is expected on loopback: QUIC's userspace congestion/stream
machinery is pure overhead when there is no real network loss or RTT to
amortise it against. On a lossy or high-RTT link QUIC's advantages
(no head-of-line blocking, 0-RTT resumption) would show; this run does
not exercise that.

**"Similar or better than Riak" is now substantiated (single node,
loopback).** dyniak delivers 2.3-4.3x Riak's throughput with lower p50
on every workload and dramatically lower tail latency on the mixed
workload (p99 3.73ms vs Riak's 29.44ms). The counter and set numbers
run through the corrected Map/CRDT wire schema and the bucket_type
storage fold from this session.

**The QUIC-only TLS split works end-to-end.** With `quic_tls_cert` /
`quic_tls_key` set (and no shared `tls_cert` / `tls_key`), a raw PBC
ping over TCP returned `00 00 00 01 02` (plaintext RpbPingResp) while
the QUIC listener served TLS on UDP 8103 -- confirming the TCP listener
is no longer forced to TLS by the QUIC cert requirement.

## Method notes / limits

* Loopback only (driver and server co-resident); no cross-node network.
  A cross-node or lossy-link run would change the TCP-vs-QUIC balance in
  QUIC's favour and is the natural next study.
* Single 90s run per point, n=1.
* Same `dyniak-bench` binary and workloads on both systems; per-type
  CRDT buckets (not the mixed `crdts` bucket) as in the prior report.
* Riak 2.2.3 via the `basho/riak-kv` Docker image, bitcask backend.
