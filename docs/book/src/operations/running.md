# Running dynomited

<div class="dyn-hero">
Everything you need to run the <code>dynomited</code> server: the config
file format, the command-line flags, validation, daemonization, and
signals. The authoritative flag reference is
<a href="../reference/man-pages.md"><code>dynomited(8)</code></a>.
</div>

## The configuration file

`dynomited` is configured by a YAML file, one top-level key naming the
pool. The smallest working single-node config:

```yaml
dyn_o_mite:
  listen: 127.0.0.1:8102
  dyn_listen: 127.0.0.1:8101
  tokens: '101134286'
  servers:
  - 127.0.0.1:22122:1
  data_store: 0
```

Start it:

```sh
cargo run -p dynomited -- --conf-file my.yml
# or, from an installed binary:
dynomited --conf-file my.yml
```

```admonish tip title="Sample configs live in the test fixtures"
Working configs for single-node, two-node, multi-datacenter, memcache,
and secure setups are in
[`crates/dynomite/tests/fixtures/conf/`](https://codeberg.org/gregburd/dynomite/src/branch/main/crates/dynomite/tests/fixtures/conf).
They are exercised by the test suite, so they are guaranteed to parse.
```

### Key fields

<dl class="dyn-facts">
<dt><code>listen</code></dt>
<dd>The client-facing address (RESP / memcache clients connect here).</dd>
<dt><code>dyn_listen</code></dt>
<dd>The DNODE peer plane address (other nodes connect here). See
<a href="../protocols/dnode.md">DNODE</a>.</dd>
<dt><code>servers</code></dt>
<dd>The backing store endpoint(s), as <code>host:port:weight</code>.</dd>
<dt><code>data_store</code></dt>
<dd><code>0</code> = valkey/redis, <code>1</code> = memcache,
<code>2</code> = dyniak. See <a href="../configuration.md">Configuration</a>.</dd>
<dt><code>tokens</code></dt>
<dd>The token(s) this node owns on the ring.</dd>
<dt><code>datacenter</code> / <code>rack</code></dt>
<dd>This node's placement; drives replication and consistency.</dd>
<dt><code>dyn_seeds</code></dt>
<dd>Peers to gossip with, as
<code>host:port:rack:dc:token</code>.</dd>
</dl>

A two-node config adds placement and a seed pointing at the other node:

```yaml
dyn_o_mite:
  datacenter: dc
  rack: rack
  listen: 127.0.0.1:8102
  dyn_listen: 127.0.0.1:8101
  dyn_seeds:
  - 127.0.0.2:8101:rack:dc:1383429731
  servers:
  - 127.0.0.1:22122:1
  tokens: '12345678'
  data_store: 0
  stats_listen: 0.0.0.0:22222
```

See [Your First Cluster](../getting-started/first-cluster.md) for a
full hands-on walk-through, and [Configuration](../configuration.md) for
every field.

## Validate before you start

`--test-conf` parses and validates the file and exits without binding any
socket. Use it in CI and before a rolling restart:

```sh
dynomited --conf-file my.yml --test-conf && echo ok
```

## Common flags

| Flag | Meaning |
|---|---|
| `-c, --conf-file <F>` | The YAML config to load. |
| `-t, --test-conf` | Validate config and exit. |
| `-d, --daemonize` | Fork twice, become a session leader, redirect stdio. |
| `-v, --verbosity <N>` | Log verbosity, `0..=11` (default 5). |
| `-p, --pid-file <F>` | Write a PID file. |
| `-D, --describe-stats` | Print the stats catalogue and exit. |
| `--log-format <fmt>` | Log output format. |
| `-V, --version` | Version and exit. |

The complete list, with every flag and its default, is in
[`dynomited(8)`](../reference/man-pages.md) and `dynomited --help`.

## Daemonizing and PID files

```sh
dynomited --conf-file my.yml --daemonize --pid-file /run/dynomited.pid
```

`--daemonize` double-forks, becomes a session leader, and redirects
stdio to `/dev/null`. Pair it with `--pid-file` so a supervisor can find
and signal the process.

```admonish note title="Under a service manager"
When running under systemd or another supervisor that manages the process
lifecycle, prefer running in the foreground (no `--daemonize`) and let the
supervisor handle backgrounding, restart, and logging.
```

## Signals

`dynomited` handles the standard termination signals for a clean
shutdown -- it stops accepting new connections, drains in-flight requests,
and closes the backend and peer connections before exiting. Send
`SIGTERM` (or `SIGINT` in the foreground) to trigger it.

```admonish warning title="Fast restarts and the stats port"
On a very fast kill-and-relaunch, a listening socket can briefly linger.
Dynomite sets <code>SO_REUSEADDR</code> on all its listeners, including
the stats port, so a prompt rebind succeeds. If you script restarts, wait
for the process to actually exit before relaunching rather than assuming a
fixed sleep is enough.
```

## Where to go next

* [Metrics](./metrics.md) and [Distributed Tracing](./tracing.md) for
  observability once it is running.
* [Admin CLI (dyn-admin)](./admin.md) for inspecting and operating a live
  cluster.
* [Recommendations](./recommendations.md) for production tuning.
