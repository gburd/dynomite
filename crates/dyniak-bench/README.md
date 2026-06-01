# dyniak-bench

A small, self-contained load-generation and benchmarking tool for
the dyniak Riak surface and Redis (RESP-2) / RediSearch path. It
takes its design cues from Basho's `basho_bench` (Erlang) but is
written in Rust, ships as a single binary, and has zero runtime
dependencies beyond a Rust toolchain.

## Quickstart

```
# A 30s mixed RESP-2 workload against a local redis-server:
cargo run --release -p dyniak-bench -- \
    --config crates/dyniak-bench/examples/redis-mixed.toml

# CLI-only (no config file):
cargo run --release -p dyniak-bench -- \
    --driver redis --host 127.0.0.1 --port 6379 \
    --ops 'get:4,set:1' --keygen pareto:1000000:1.5 --valgen fixed:256 \
    --rate max --duration 10m --concurrent 32

# Riak Protocol Buffer Client driver (CRDT counter increments):
cargo run --release -p dyniak-bench --features riak -- \
    --config crates/dyniak-bench/examples/riak-crdts.toml
```

Output lands in `tests/<auto-name>/` (or wherever `--out` /
`run.out_dir` points), with these files:

```
tests/<run>/summary.csv             # per-window summary (basho-shaped)
tests/<run>/<op>_latencies.csv      # per-window per-op latencies (us)
tests/<run>/errors.csv              # error class counts
tests/<run>/summary.svg             # ops/sec + error% timeline
tests/<run>/<op>_latencies.svg      # p50 / p99 / p99.9 timeline
tests/<run>/<op>_histogram.svg      # log-bucketed latency histogram
```

> **Note on plot format**: this crate emits SVG, not PNG. Plotters'
> PNG path requires a TrueType font implementation (`ttf` /
> `ab_glyph`) which would pin a system fontconfig + freetype
> dependency this crate does not otherwise need. SVG opens in any
> browser, scales cleanly, and is rendered by the viewer's font
> stack. Convert to PNG with any standard tool (`rsvg-convert`,
> ImageMagick, Inkscape, headless Chrome) if the consumer requires
> raster output.

## Config schema (TOML)

Every CLI flag has a TOML equivalent. CLI flags override the
loaded TOML on a per-field basis.

```toml
[run]
duration = "10m"            # also "30s", "500ms", "2h"
concurrent = 32             # number of worker threads
rate = "max"                # or { rps = 5000 }, or 5000
out_dir = "tests/auto"      # auto-named when "auto"
report_interval = "1s"      # how often to flush a row

[driver]
kind = "redis"              # "redis" | "riak_pbc" | "riak_http"
host = "127.0.0.1"
port = 6379
timeout_ms = 5000
bucket = "bench"            # for the Riak drivers

[ops]
get = 4                     # weighted op selection
set = 1
del = 1

[keygen]
kind = "pareto"             # "uniform" | "sequential" | "pareto" | "normal" | "fixed"
max = 1000000
shape = 1.5                 # pareto exponent
prefix = "k_"

[valgen]
kind = "fixed"              # "fixed" | "uniform" | "exponential"
size = 256
# min = 16; max = 1024      # uniform variant
# mean = 256                # exponential variant
```

Drivers register their op vocabulary at construction; the engine
validates the `[ops]` table against that vocabulary at startup
and rejects unknown ops.

### Driver vocabularies

| Driver       | Ops                                                                                                  |
|--------------|------------------------------------------------------------------------------------------------------|
| `redis`      | `get`, `set`, `incr`, `decr`, `del`, `hset`, `hget`, `sadd`, `sismember`, `zadd`, `zrangebyscore`,    |
|              | `ft_create`, `ft_search`, `ft_sugadd`, `ft_sugget`                                                   |
| `riak_pbc`   | `ping`, `get`, `put`, `del`, `counter_inc`, `set_add`, `map_update`                                   |
| `riak_http`  | `get`, `put`, `del` (feature `http`)                                                                  |

## Features

* `default` -- builds the Redis driver. `riak_pbc` and `riak_http`
  are gated behind feature flags so the binary stays small for
  Redis-only deployments.
* `riak` -- enables the Riak Protocol Buffer Client driver. Adds
  no third-party deps beyond what `default` already pulls.
* `http` -- enables the Riak HTTP driver. Adds `reqwest` (with
  `rustls-tls`).

## Examples

| Path                                            | Description                                  |
|-------------------------------------------------|----------------------------------------------|
| `examples/redis-mixed.toml`                     | 70/30 GET/SET workload, fixed-size values    |
| `examples/riak-pbc.toml`                        | Balanced ping/put/get/del PBC workload       |
| `examples/riak-crdts.toml`                      | Counter / set / map CRDT update mix          |
| `examples/ft-search.toml`                       | RediSearch FT.SEARCH + FT.SUG* mix           |

## Architecture

```
+----------+      +-----------+      +----------+
| KeyGen   |      |  ValGen   |      |  Driver  |
+----+-----+      +-----+-----+      +----+-----+
     v                  v                 v
 +---------------------------------------------+
 |               Worker (per task)             |
 +---------------------------------------------+
                       |
                       v records
 +---------------------------------------------+
 |         StatsAggregator (HdrHistogram)      |
 +---------------------------------------------+
                       |
            every report_interval
                       v
 +---------------------------------------------+
 |   ReportWriter -- summary.csv / *_latencies |
 +---------------------------------------------+
                       |
                  end of run
                       v
 +---------------------------------------------+
 |   plot::render_*_svg -- SVG charts          |
 +---------------------------------------------+
```

* **Workers** are OS threads (`std::thread`); each one owns its
  own `Driver`, KeyGen state, ValGen state, RNG, and per-op
  HdrHistograms.
* **Rate control** is per-worker token bucket: `target_rps /
  concurrent` ops per 1-second window. `rate = "max"` disables
  the bucket.
* **Stats** flush every `report_interval`. The reporter thread
  drains every worker's per-op histograms into a shared global
  view, snapshots the percentiles (p50, p95, p99, p99.9, max),
  and writes one row per file.
* **Errors** are classified into `Closed`, `Timeout`,
  `NoTargets`, and `Unknown`. The first 5 errors per class per
  worker land on stderr; the rest are silently counted.

## CSV schema

`summary.csv`:

```
elapsed_s, window_count, ok_count, err_count,
p50_ms, p95_ms, p99_ms, p99_9_ms, max_ms,
p50_total_ms, p99_total_ms
```

`<op>_latencies.csv`:

```
elapsed_s, count, p50_us, p95_us, p99_us, p99_9_us, max_us, mean_us
```

`errors.csv`:

```
elapsed_s, op, class, count
```

## Acknowledgements

Concept and CSV shape drawn from Basho's `basho_bench`. The
implementation here is independent.
