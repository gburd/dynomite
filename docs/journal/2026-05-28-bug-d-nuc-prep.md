# 2026-05-28 - Bug D: nuc operational prep + lamdb v2.3.0

## Bug D recap

Pass-6 memcache + riak modes had nuc dropping out at start with:

- Memcache: `memcached not on PATH and no podman/docker available`
- Riak: `MODE=riak requires a dynomited built with --features riak`

The pass-6 postmortem framed this as an SSH-fallback issue. **That
was wrong.** Re-checked: NUC_SSH already uses ProxyJump for start
operations (`coordinator.sh:148`), and pass-6 redis confirmed nuc
WAS reachable for the workload window. The mid-pass SSH timeout I
observed during my probing was incidental.

The real Bug D was operational state on nuc itself:

- nuc has no native `memcached` (FreeBSD pkg never installed).
- nuc has no container runtime (no podman, no docker on FreeBSD).
- nuc's `dynomited` at `/scratch/dynomite-chaos/build/release/dynomited`
  was an old build (May 21) compiled WITHOUT `--features riak`, so
  it didn't expose `--riak-pbc-listen` and start-host.sh refused
  to start riak mode.

## Fix (operational, no code)

```sh
ssh -J arnold nuc 'sudo pkg install -y memcached'
# memcached 1.6.41 now in /usr/local/bin/memcached
```

For the riak binary, lamdb is at v2.3.0 GA on local; the lamdb
path-dep makes the version transparent. nuc didn't have lamdb at
`/scratch/dynomite-chaos/lamdb/`, so the rebuild failed with
`failed to read .../noxu-db/Cargo.toml`. Pushed lamdb to nuc:

```sh
rsync -az --delete --exclude=target --exclude=.git \
    -e 'ssh -J arnold' \
    ~/ws/lamdb/ \
    nuc:/scratch/dynomite-chaos/lamdb/
```

Then rebuilt natively on nuc:

```sh
ssh -J arnold nuc \
    'cd /scratch/dynomite-chaos/src && \
     PATH=$HOME/.cargo/bin:$PATH \
     cargo build --release -p dynomited --features riak'
# 2m 12s on nuc (FreeBSD 15, amd64)
cp src/target/release/dynomited build/release/dynomited
```

Verified: `dynomited --help` now lists `--riak-pbc-listen`,
`--riak-http-listen`, `--riak-aae-enabled`.

## What this means for pass-7

nuc is now ready to participate in all three modes (redis,
memcache, riak). Combined with:

- Bug A fix (jittered backend reconnect backoff)
- Bug B fix (heredoc / SEEDS multi-line word-split)
- Bug C fix (start-host pre-kills stale containers + protocol
  probe hard-fails)

pass-7 should produce 4-host coverage in every mode.

## Coordinator follow-up (future)

A future revision of the coordinator should detect this kind of
host-state drift before starting a pass:

- Probe each host for the binaries the selected MODE needs
  (`memcached -V`, `redis-server --version`, dynomited
  `--help | grep -q riak`).
- Refuse to start the pass if any active host fails the probe;
  print a clear remediation hint.

That's a 1-2 hour follow-up; deferred so we can run pass-7 first.

## Lamdb v2.3.0 note

While probing nuc, observed lamdb is at `4bd2bd7 merge: v2.3.0
-- Wave 10 polish`. Local ws/lamdb is also at 2.3.0. Path-deps
make this transparent; the workspace Cargo.lock will track the
version label on the next commit that touches it.

dynomite tests under v2.3.0:
- 1591 nextest workspace passing
- All loom tests passing (throttle-core, hashtree, sup,
  loom-tests)
- dyn-riak: 444 tests
- dynomited (riak feature): 90 tests
