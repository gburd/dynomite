# Distribution packaging

This directory holds the OS-level packaging artefacts for the
Rust port of dynomite. Three deployment shapes are supported:

* `dist/systemd/`: systemd unit and environment file for a
  hand-installed binary on a Linux host.
* `dist/docker/`: multi-stage Dockerfile + entrypoint + default
  config for a self-contained image with a bundled
  `redis-server` backend.
* `dist/scripts/`: stub `postinst.sh` / `prerm.sh` hooks for
  deb / rpm packages built with `cargo-deb`, `cargo-rpm`,
  `nfpm`, or any other packager that consumes shell hooks.

The `dist/chaos-reports/` subtree (created the first time
`crates/dynomite/tests/stage_16_chaos.rs` runs in production
mode) is where the lead checks in the report.md artefact for
the v0.1.0 tag. Day-to-day chaos runs land under
`target/chaos/<run-id>/` and are gitignored; only the curated
report for the release tag lives in version control.

## systemd unit

The unit file is `dist/systemd/dynomited.service`. It mirrors
the C reference unit (see `_/dynomite/init/`) and adds a few
sandboxing directives the C engine could not use because it
required glibc threading primitives the sandbox would block.

Install:

```
sudo cp dist/systemd/dynomited.service /etc/systemd/system/
sudo cp dist/systemd/dynomited.env /etc/default/dynomited   # deb
# or
sudo cp dist/systemd/dynomited.env /etc/sysconfig/dynomited  # rpm

sudo install -d -o dynomite -g dynomite /etc/dynomite \
                                     /var/run/dynomite \
                                     /var/lib/dynomite \
                                     /var/log/dynomite

sudo systemctl daemon-reload
sudo systemctl enable --now dynomited.service
```

The `dist/scripts/postinst.sh` hook performs all of the above
when run from a deb / rpm install; the manual sequence is
documented for hand-rolled installs.

## Docker image

The Dockerfile uses two stages:

1. `builder`: `rust:1.90-bookworm` builds the dynomited binary
   with `cargo build --release --locked` and runs `dynomited -h`
   as a smoke test.
2. `runtime`: `debian:bookworm-slim` with `tini`, `ca-certificates`,
   and `redis-server`. The bundled `dist/docker/entrypoint.sh`
   optionally starts a local Redis on 22122 (controlled by
   `DYNOMITE_BACKEND=redis|none`) and then execs dynomited.

Build:

```
docker build -f dist/docker/Dockerfile -t dynomite:0.1.0 .
```

Run:

```
docker run --rm -it \
    -p 8101:8101 -p 8102:8102 -p 22222:22222 \
    dynomite:0.1.0
```

Override the bundled config:

```
docker run --rm -it \
    -v $(pwd)/my.yml:/etc/dynomite/dynomite.yml:ro \
    -p 8102:8102 \
    dynomite:0.1.0
```

Disable the embedded Redis (e.g. when pointing at an external
backend):

```
docker run --rm -it \
    -e DYNOMITE_BACKEND=none \
    -v $(pwd)/external.yml:/etc/dynomite/dynomite.yml:ro \
    dynomite:0.1.0
```

## deb / rpm packaging

We do not ship a `Cargo.toml` `[package.metadata.deb]` block or
a `cargo-rpm` config. Both packagers can consume the hooks
under `dist/scripts/` directly:

```
cargo deb --variant default \
    --maintainer-scripts dist/scripts \
    -- --bin dynomited

# or

nfpm pkg --packager rpm \
    --config dist/nfpm.yaml
```

The hooks are POSIX-shell, idempotent, and exit non-zero only
on a real failure so repeated installs are safe. Operators
deploying via a configuration-management system can call them
directly after staging the binary.
