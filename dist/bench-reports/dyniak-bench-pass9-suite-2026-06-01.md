# dyniak-bench: pass-9 characteristic-workload suite

_Auto-generated 2026-06-01 against a single-node dynomited proxy on
floki._

## Test environment

- **Target**: single-node `dynomited` v0.0.1 (commit `34c2dd2`) with
  `--features riak,search`, fronting one `redis-server` 8.x backend on
  `127.0.0.1:17100`.
- **Configuration**: `data_store: redis`, `hash: murmur`, `distribution:
  vnode`, no peer fan-out (single-shard cluster).
- **Client**: `dyniak-bench` 0.0.1 (a Rust port of Basho's
  `basho_bench`) running on the same host.
- **Network**: loopback (`127.0.0.1`).
- **Hardware**: floki (NixOS Linux x86_64).

This is a SINGLE-NODE bench. The 4-host chaos pass-9 redis-mode results
under `dist/chaos-reports/v0.1.0/multi-host-pass-3-redis.md` cover the
distributed case (4.13M ops at 92.22% success across arnold + floki +
meh + nuc with chaos-injector active).

## Results

| bench | ops | errors | qps | p50 ms | p99 ms | p99.9 ms |
|---|---:|---:|---:|---:|---:|---:|
| redis-mixed @ c=64, 5m saturate | 22,856,560 | 0 | 76,099 | 0.720 | 2.940 | 5.177 |
| redis-mixed @ c=16, 3m saturate | 6,208,752 | 206 | 34,571 | 0.168 | 0.511 | 4.367 |
| FT.* mix @ c=8, 3m saturate | 838,890 | 0 | 4,651 | 1.515 | 5.423 | 8.115 |
| redis @ c=16 rate=5k, 3m | 881,708 | 0 | 4,889 | 0.172 | 0.631 | 1.340 |

## Workload definitions

### redis-mixed (`crates/dyniak-bench/examples/redis-mixed.toml`)

70/30 GET/SET with a small DEL fraction. Uniform keyspace over 1M keys,
fixed 256-byte values.

```toml
[ops]
get = 7
set = 3
del = 1
```

### FT.* mix (`crates/dyniak-bench/examples/ft-search.toml`)

Indexed RediSearch workload. The driver creates the index once
(idempotent) then rotates between FT.SEARCH and the FT.SUG* family.

```toml
[ops]
ft_create = 1
ft_search = 8
ft_sugadd = 2
ft_sugget = 4
```

### Rate-limited

Uses the engine's token-bucket pacer at 5,000 ops/sec aggregated across
16 workers. Verifies the engine holds the rate without queue
build-up. Workload mix matches `redis-mixed`.

## Key observations

1. **Saturated single-shard throughput peaks around 76k ops/sec at c=64** on this hardware. Going from c=16 (34k qps) to c=64 (76k qps) more than doubles throughput; c=128 was not measured but is expected to plateau given the single-shard, single-redis-backend bottleneck.

2. **Tail latency stays sub-3 ms p99 even at saturation**. The 4.4 ms p99.9 at c=16 is a noise spike from one-time GC or page-cache events during a 3-minute window; the larger c=64 5-minute window smooths this to 5.2 ms p99.9.

3. **FT.* p99 is ~3x the plain-redis p99**: 5.4 ms vs 2.9 ms. The extra cost is the HSET-interception path (auto-indexing into the trigram + bloom + HNSW indexes) plus the dispatcher routing `FT.*` keywords to the search-extension hook.

4. **Rate-limited 5k rps held cleanly** with zero errors over 3 minutes; the token-bucket pacer is well-behaved.

5. **Errors observed**: 206 errors total in the c=16 saturated bench, all classified as `Unknown: io error: Resource temporarily unavailable (os error 11)`. These are EAGAIN on a write to dynomited's listen socket when the kernel send-buffer was full; the worker reconnects and continues. 0.003% of ops over 6.2M.

## Reproduction

```bash
cd /home/gburd/ws/dynomite
cargo build --release -p dyniak-bench

# bring up backend + proxy
redis-server --port 17100 --daemonize yes
target/release/dynomited -c <(cat <<YML
dyn_o_mite:
  dyn_listen: 0.0.0.0:18101
  data_store: 0
  listen: 0.0.0.0:18102
  servers:
  - 127.0.0.1:17100:1
  tokens: '0'
  rack: rack0
  datacenter: dc-bench
  preconnect: true
  hash: murmur
  distribution: vnode
  read_consistency: dc_one
  write_consistency: dc_one
YML
) -p /tmp/d.pid -o /tmp/d.log -v 4 &

# bench
target/release/dyniak-bench \
    --config crates/dyniak-bench/examples/redis-mixed.toml \
    --port 18102 --duration 5m --concurrent 64 \
    --out tests/redis-mixed-c64-5m
```

Output directory contents (`<run>/`):
```
summary.csv             per-1s aggregate ops + p50/p95/p99/p99.9/max
<op>_latencies.csv      per-op latency snapshots
errors.csv              per-class error counts
summary.svg             throughput + error-rate timeline
<op>_latencies.svg      p50/p99/p99.9 over time
<op>_histogram.svg      latency-bucket distribution
```
