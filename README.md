# Dynomite (Rust)

A Rust port of [Netflix Dynomite](https://github.com/Netflix/dynomite),
the thin distributed replication layer for Redis and Memcached datastores
inspired by the [Amazon Dynamo paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf).

This project is a from-scratch Rust implementation that aims to be
functionally identical to the original C codebase while being usable
both as:

* a standalone server binary (`dynomited`), and
* a library crate (`dynomite`) that can be embedded directly in another
  Rust program through a stable, documented API.

Network communication between Dynomite nodes can run over TCP (default,
matching the original) or QUIC (via the `quiche` crate). Both transports
support IPv4 and IPv6.

## Status

This is an in-progress port. See `PLAN.md` for the staged roadmap and
`docs/parity.md` for the live C-to-Rust mapping. `AGENTS.md` is the
operating manual for contributors (human or automated).

## Quick start

```
nix develop
cargo build --workspace
cargo nextest run --workspace
```

The Nix flake pins every tool needed to build, test, fuzz, bench, and
package the project.

## Acknowledgements

This Rust implementation is a port of the original
[Dynomite](https://github.com/Netflix/dynomite) project by Netflix, Inc.,
which itself extended Twitter's `twemproxy`. Both projects are licensed
under the Apache License 2.0; their notices are preserved in `NOTICE`.

## License

Apache License 2.0. See `LICENSE`.
