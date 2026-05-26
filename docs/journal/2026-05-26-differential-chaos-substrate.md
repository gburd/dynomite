# P3-3.9 Differential rig integration with chaos: substrate

Status: phase 1 + phase 2 substrate landed.
Stage branch: `stage/p3-3.9-differential-substrate`.
Scope: build the C `dynomite` reference on each chaos host
(phase 1) and stand up a parallel C cluster alongside the
existing Rust one (phase 2). Phases 3-5 (workload fan-out,
reply comparison, chaos integration) are explicit follow-ups
captured below.

## What landed

### Phase 1: per-host C build

`scripts/chaos-multi-host/build_cref_remote.sh` ports the
local `scripts/build_cref.sh` flow to a per-host SSH harness.

* Single positional argument: the host token. Recognises the
  four known chaos hosts:
  `floki|local` (no SSH), `arnold` (Tailscale direct),
  `nuc` (ProxyJump via arnold to `gburd@nuc`), `meh` (LAN).
  Any other value is passed through to `ssh` verbatim.
* Idempotent against the submodule's git commit hash.
  `/scratch/dynomite-chaos/cref-build/.source-sha` records the
  HEAD that produced the binary; reruns short-circuit on a hit
  and just print the sha256 of the cached binary.
* `--clean` flag wipes both the source mirror and the build
  tree first.
* Per-host prerequisite probe runs once before any rsync. The
  probe asserts `autoreconf`, `automake`, `autoconf`, `libtool`,
  `pkg-config`, `make`, `rsync`, a working sha256 tool, a C
  compiler (`cc`/`gcc`/`clang`), and openssl development
  headers (via `pkg-config openssl` first, with a fallback
  header search across the Linux/FreeBSD/macOS standard paths).
  Missing tools produce a clear log line and exit 1.
* Build relaxes CFLAGS the same way `scripts/build_cref.sh`
  does (`-fcommon -Wno-error -Wno-int-conversion
  -Wno-incompatible-pointer-types -Wno-implicit-function-declaration`).
  We do not modify the submodule.
* On FreeBSD the script automatically appends
  `CPPFLAGS=-I/usr/local/include LDFLAGS=-L/usr/local/lib` when
  the pkgsrc openssl tree is present.
* On a top-level `make` failure the script falls back to the
  per-subdir sequence `scripts/build_cref.sh` uses
  (yaml lib, hashkit, proto, event, entropy, seedsprovider,
  src/dynomite). This works around Nix's hardened-gcc wrapper
  rejecting the upstream yaml test harness without affecting
  the produced binary.

Verified on floki (lead host, NixOS): the build succeeds in
~70s (cold) and short-circuits in ~1s on a cache hit. Produced
binary sha256:
`42912981be2bb5c18add1f3b31bcfd8fb2fad17beb4fb983b27077e4a70a0062`.

### Phase 2: parallel Rust + C cluster bring-up

`scripts/chaos-multi-host/start-host.sh` accepts a new
`MODE=differential` value:

* The Rust dynomited starts on its existing ports
  (`CLIENT_LISTEN_PORT`, `DYN_LISTEN_PORT`, `STATS_LISTEN_PORT`)
  with `data_store: 0`, redis backend, gossip enabled.
* The C dynomite starts on shifted ports
  (Rust + 100 for client/dyn/stats), against the SAME backend
  redis on `DATASTORE_PORT`. The seeds list is rewritten with
  `sed` to retarget `:DYN_LISTEN_PORT:` to `:DYN_LISTEN_PORT+100:`
  so the C cluster forms its own gossip mesh.
* When the seed list is empty (single-host smoke), the C
  config sets `enable_gossip: false`. The C engine's
  `dynomite.c::main` short-circuits the local node to `NORMAL`
  state when gossip is disabled; without that, the proxy stays
  in `JOINING` forever and refuses to route writes. Production
  multi-host differential mode keeps `enable_gossip: true`
  with a populated seed list.
* The C binary is sourced from
  `$ROOT/cref-build/dynomite`. If missing, start-host.sh
  prints a clear error pointing the operator at
  `build_cref_remote.sh`.
* Pidfiles and logs are namespaced (`dynomite-c.pid`,
  `dynomite-c.spawn-pid`, `logs/dynomite-c-$DC_NAME.log`,
  `logs/dynomite-c-$DC_NAME.stderr`) so the existing teardown
  in `coordinator.sh` does not conflict.

`scripts/chaos-multi-host/coordinator.sh` was extended in two
small ways:

* Mode validation accepts `differential` alongside the
  existing `redis|memcache|riak`.
* Before per-host start, when `MODE=differential`, the
  coordinator iterates every active host and invokes
  `build_cref_remote.sh`. A failure on any host marks that
  host failed (via the existing fault-library coordinator
  robustness) and the run continues on the remaining hosts.
* The teardown's `remote_cmd` snippet now also kills
  `/scratch/dynomite-chaos/run/dynomite-c.pid`, alongside the
  existing `dynomited.pid`.

Default behaviour (`MODE` unset / `MODE=redis|memcache|riak`)
is identical to before.

### Phase 2 smoke

`scripts/chaos-multi-host/smoke-differential.sh` (100 lines):
brings up redis + Rust dynomited + C dynomite via a direct
`start-host.sh dc-floki ... MODE=differential` invocation,
then probes both proxy ports (Rust on `CLIENT_LISTEN_PORT`,
C on `+100`) for SET / GET round-trips inside a 60-second
window. Ports are env-overridable so the smoke can run on a
host that already has a production chaos rig holding the
default ports.

## What's next: phase 3-5

These are the explicit follow-ups that complete the original
P3-3.9 brief. They are not implemented here.

### Phase 3: workload-driver dual fan-out (~3 days)

`scripts/chaos-multi-host/workload-driver.py` currently
dials a single `--port`. Phase 3 extends the driver to:

* Accept `--port` AND `--c-port` (or `--differential` toggling
  on `port + 100`). Each generated request is dispatched to
  both ports in parallel via threadpool.
* Capture both replies plus per-request metadata (timestamp,
  command class, key, exact RESP bytes returned).
* Emit a parallel NDJSON stream
  (`workload-<label>-differential.ndjson`) keyed by request
  id, containing
  `{ts, op, key, rust_reply_bytes, c_reply_bytes,
   rust_latency_ms, c_latency_ms, equiv}`.
* The existing per-class success/failure counters still apply
  (Rust proxy is the source of truth for "did the op
  succeed?"); divergence shows up in the `equiv` column.

### Phase 4: reply comparison (~2 days)

A new module `scripts/chaos-multi-host/diff_reply.py`:

* Byte-exact for: SET +OK, GET bulk strings, DEL/EXISTS
  integers, RESP arrays, simple strings, error frames.
* Allowlist for known semantic divergences:
  * `INFO` blob: contains version + uptime; compare keys, not
    values.
  * `TIME`: returns wallclock; compare reply shape only.
  * `DEBUG SLEEP` / scripting timing: skip entirely.
  * `OBJECT IDLETIME`, `OBJECT FREQ`: skip.
  * `CLUSTER NODES`: skip until phase 5 confirms gossip
    parity.
* Generates `target/chaos-multi-host/<run>/diff-summary.md`
  with per-class match-rate and a top-N divergent-bytes
  listing for human review.

### Phase 5: chaos integration (~3 days)

* `chaos-injector.sh` learns to target the C dynomite alongside
  the Rust one. SIGSTOP/SIGKILL/redis-bounce fault classes need
  a parallel pidfile lookup (`dynomite-c.pid`) and a
  parallel restart routine.
* The injector applies faults to BOTH proxies in lockstep so
  the differential workload sees identical failure
  conditions. Network/clock/disk faults are host-level and
  already affect both processes.
* `generate-report.py` adds a "differential" section that
  reads the new NDJSON stream, summarises per-class match
  rate over the run window, and flags any class whose
  divergence exceeds an operator-set threshold (default 0%
  for byte-exact classes, allowlist-controlled for the rest).
* Pre-tag gate: a clean differential pass at the same scale
  as the multi-host pass-4 redis run (4 hosts, 2h, all fault
  modes enabled).

## Per-host build prerequisites

The phase-1 prerequisite probe checks for these tools. The
list is captured here so an operator can pre-flight a host
without running the script.

| tool | reason | Fedora pkg | NixOS attr | FreeBSD pkg |
|---|---|---|---|---|
| `autoconf` | configure.ac | `autoconf` | `autoconf` | `autoconf` |
| `automake` | aclocal | `automake` | `automake` | `automake` |
| `autoreconf` | regen | `autoconf` | `autoconf` | `autoconf` |
| `libtool` | LT_INIT | `libtool` | `libtool` | `libtool` |
| `pkg-config` | openssl probe | `pkgconf-pkg-config` | `pkg-config` | `pkgconf` |
| `make` | build | `make` | `gnumake` | `gmake`/`make` |
| `rsync` | source sync | `rsync` | `rsync` | `rsync` |
| `cc`/`gcc`/`clang` | compile | `gcc` | `gcc`/`clang` | base/`llvm` |
| openssl headers | `dyn_crypto.c` | `openssl-devel` | `openssl.dev` | base or `openssl` |
| `sha256sum`/`sha256`/`shasum` | binary fingerprint | `coreutils` | `coreutils` | base |

A successful pre-flight on a host:

```
nix develop --command \
  bash scripts/chaos-multi-host/build_cref_remote.sh <host>
```

A `--clean` rebuild from scratch:

```
nix develop --command \
  bash scripts/chaos-multi-host/build_cref_remote.sh --clean <host>
```

## Hosts tested

* **floki (NixOS, lead)**: full success. `nix develop`
  shell provides openssl-3.6.1-dev via `PKG_CONFIG_PATH`. Cold
  build ~70s. Cache hit ~1s. Binary sha256 recorded above.
* **arnold (Fedora 44)**: not exercised in this commit.
  Prereqs are documented; the script is expected to work
  unchanged. Operator should run
  `bash scripts/chaos-multi-host/build_cref_remote.sh arnold`
  before the first differential pass.
* **nuc (FreeBSD 15)**: not exercised in this commit. The
  script's FreeBSD branch sets `CPPFLAGS=-I/usr/local/include
  LDFLAGS=-L/usr/local/lib` for pkgsrc openssl. The C tree's
  configure macros (`AC_CHECK_LIB ssl/crypto`) should resolve.
  If the FreeBSD build fails, treat it as a known limitation
  documented under `# Known limitations` below; the
  coordinator's failed-host gate already removes nuc from the
  rest of the run without aborting the pass.
* **meh (NixOS)**: not exercised in this commit. Same flake-
  provided toolchain as floki; expected to work the first
  time.

## Smoke results

Direct end-to-end smoke against the C dynomite was run on
floki using alternate ports (the lead host was concurrently
running the production multi-host pass-4 chaos rig, which
holds the default `/scratch/dynomite-chaos/run/*` paths and
the standard 18102/18101/22222 ports). Results:

* `build_cref_remote.sh floki` cold: PASS, 70s, sha as above.
* `build_cref_remote.sh floki` warm: PASS, ~1s.
* C `dynomite` direct startup with the same YAML the new
  start-host branch generates (`enable_gossip: false`, single
  redis backend): PASS, listens on client+dyn+stats ports.
* RESP-2 `SET foo bar` against the C proxy returns
  `+OK\r\n` (5 bytes). PASS.
* RESP-2 `GET foo` against the C proxy returns
  `$3\r\nbar\r\n` (9 bytes). PASS.

The `smoke-differential.sh` end-to-end harness was NOT run in
this session because the live pass-4 chaos run was holding
`/scratch/dynomite-chaos/run/dynomited.pid` and continually
respawning the Rust proxy under the chaos-injector. The
script's port-override env knobs avoid the port collision but
the start-host machinery still scribbles into the shared
`run/` directory; running the smoke alongside a live chaos
pass would corrupt it. Operators should run the smoke on an
otherwise-idle floki, e.g. between chaos passes.

## Known limitations

* **No multi-host wiring tested in this commit.** The
  coordinator's differential branch and the per-host
  build_cref orchestration are wired but not exercised across
  arnold/nuc/meh. The first cross-host run is expected during
  phase 3 work, when the parallel cluster has actual workload
  to drive.
* **FreeBSD (nuc) is unverified.** The build script's FreeBSD
  branch is best-effort: if pkgsrc openssl is in a different
  prefix, the operator can pass
  `CFLAGS_EXTRA="-I/usr/pkg/include"` and
  `LDFLAGS=-L/usr/pkg/lib` via the env. If the FreeBSD build
  proves intractable, the differential rig still works on a
  3-host (floki/arnold/meh) topology by setting
  `HOSTS_OVERRIDE=floki,arnold,meh` on the coordinator.
* **C config does not advertise the riak PBC listener.** The C
  reference predates the riak-feature work in the Rust port,
  so MODE=differential is meaningful only for redis-mode
  workloads. memcache mode could be added (it is just
  `data_store: 1` on both sides), but the brief scoped phase
  1+2 to redis.
* **No comparison logic yet.** The two clusters run side by
  side but the workload driver still dials only the Rust
  port. Reply comparison is phase 3+4 work.

## Operator runbook addendum

To prepare a chaos host for the first differential pass:

```
# From floki, inside `nix develop`:
for h in floki arnold nuc meh; do
    bash scripts/chaos-multi-host/build_cref_remote.sh "$h" \
        | tee "target/cref-$h.log"
done
```

To run a differential pass once all hosts have a C binary:

```
MODE=differential CHAOS_DURATION_SECS=900 \
    bash scripts/chaos-multi-host/coordinator.sh
```

To smoke just the substrate on the lead host:

```
bash scripts/chaos-multi-host/smoke-differential.sh
```

## Files touched

* `scripts/chaos-multi-host/build_cref_remote.sh` (new)
* `scripts/chaos-multi-host/start-host.sh` (MODE=differential)
* `scripts/chaos-multi-host/coordinator.sh`
  (MODE validation, phase-1 build orchestration,
  teardown extension)
* `scripts/chaos-multi-host/smoke-differential.sh` (new)
* `docs/operations/chaos.md` (new "Differential mode"
  section)
* `docs/post-chaos-queue.md` (P3-3.9 phase 1+2 done; phases
  3-5 still queued with the same effort estimates)
* `docs/journal/2026-05-26-differential-chaos-substrate.md`
  (this file)
