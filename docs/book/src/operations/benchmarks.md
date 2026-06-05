# Benchmarks

Stage 15 ships two bench harnesses:

* **Micro** (`crates/dynomite/benches/`): per-component criterion
  benches. Runs unattended on any developer machine.
* **Macro** (`crates/dynomite/benches/macro_throughput.rs`): live
  three-node cluster driven through a tc/netem impairment matrix.
  Gated behind the `bench-macro` feature because it needs
  `CAP_NET_ADMIN` to install netem qdiscs on the loopback
  interface.

## Micro benches

The micro suite covers seven components:

| File | Coverage |
|---|---|
| `parsers.rs` | Redis (SET/GET/MGET/MSET/HSET/ZADD/EVAL) and Memcache (get/set/cas) parsers at 16/64/256/1024/8192-byte payloads |
| `mbuf.rs` | Cold and recycled allocation, `split_off`, `copy_from_slice` |
| `hashkit.rs` | Every algorithm exposed by `HashType::all` over 16/64/256/1024-byte keys |
| `tokens.rs` | `set_int`, `cmp`, `vnode::dispatch` over 100/1000/10000-server rings |
| `dnode.rs` | DNODE header encode + parse over 64/256/1024/4096-byte payload sizes |
| `crypto.rs` | AES-128-CBC encrypt/decrypt at 16/64/256/1024/4096; RSA OAEP wrap/unwrap; PEM load |
| `quorum.rs` | `ResponseMgr::outcome` + `is_done` over the full max-responses table |

### Running

```
cargo bench --bench parsers -p dynomite
cargo bench --bench mbuf    -p dynomite
cargo bench --bench hashkit -p dynomite
cargo bench --bench tokens  -p dynomite
cargo bench --bench dnode   -p dynomite
cargo bench --bench crypto  -p dynomite
cargo bench --bench quorum  -p dynomite
```

`-- --test` smoke-runs each case once; CI uses this mode.

### Baselines and the regression budget

Every micro bench has a baseline manifest at
`crates/dynomite/benches/baseline/<bench>.json`. The manifest
records the criterion baseline name (`stage-15`), the capture
timestamp, the git sha of the captured run, and the per-case
regression budget (10% by default).

Capture a baseline on a quiescent host:

```
cargo bench --bench parsers -p dynomite -- --save-baseline stage-15
```

Compare a new run against the recorded baseline:

```
cargo bench --bench parsers -p dynomite -- --baseline stage-15
```

The CI gate consumes the criterion `change/` reports under
`target/criterion/<bench>/` and exits non-zero on any case whose
median time regressed by more than the manifest's
`regression_budget_pct`.

## Macro benches

The macro harness exercises a live three-node cluster on
`localhost`, drives `valkey-benchmark` for 30 seconds per
condition, and writes the per-condition latency snapshot to
`target/bench/macro-<git-sha>.json`. Conditions:

* `baseline` (no impairment)
* `delay 5ms`
* `loss 1%`, `loss 5%`, `loss 10%`
* `corrupt 0.1%`
* `reorder 25% 50%`

### Setup

The harness installs `tc qdisc ... netem` on the loopback
interface; this needs `CAP_NET_ADMIN`. Two options:

1. Run as root: `sudo cargo bench --features bench-macro --bench
   macro_throughput`. Not recommended outside CI.
2. Grant `CAP_NET_ADMIN` to a worker namespace:

   ```
   sudo unshare -n bash -c 'ip link set lo up && exec sudo -u $USER cargo bench --features bench-macro --bench macro_throughput'
   ```

   The flake's shellHook documents the same recipe in detail.

The harness clears any qdisc it installed at the end of every
condition. If the harness aborts mid-run, clear leftover state
manually:

```
sudo tc qdisc del dev lo root netem
```

### Output

```
target/bench/macro-<git-sha>.json
```

Each entry is one condition with `ops_per_sec`, `latency_p50_us`,
`latency_p99_us`, `latency_p999_us`, `latency_p9999_us`, and
`wall_seconds`. The Stage 15 commit ships the orchestration
scaffold; the actual workload generator (a valkey-benchmark
spawn) is the operator's responsibility because CI does not
have permission to install netem qdiscs.

## CI integration

`scripts/check.sh` does not run `cargo bench`. The bench gate is
opt-in:

```
scripts/check.sh                   # default; no benches
cargo bench --workspace            # opt-in micro suite
cargo bench --features bench-macro # opt-in macro suite
```

CI for tagged releases additionally runs the micro suite with
`--baseline stage-15` and fails on per-case regression beyond
the manifest's budget.
