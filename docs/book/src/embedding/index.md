# Embedding API

This chapter is the design contract for the `dynomite::embed` module
landing in Stage 13. It pins the public surface that the implementer
must hit so that callers can plan against it today.

## Two ways to use this crate

Dynomite ships both a binary (`dynomited`) and a library
(`dynomite`).

* As a **server**, `dynomited` parses a YAML file, builds a
  `dynomite::embed::Server`, and runs it under its own
  `#[tokio::main]`. The YAML schema is the same one documented in the
  reference C version; nothing about it is binary-only.
* As a **library**, an embedding program owns a tokio runtime of its
  own and constructs a `Server` directly. The same configuration
  fields are exposed as typed fluent setters. In addition, slots that
  cannot be expressed in YAML (custom transports, custom datastores,
  custom seed providers, custom crypto, custom metrics sinks) are
  available as typed setters.

Both paths share the same core: `dynomited` is a thin wrapper around
the embedding API, not a parallel code path.

## Two-tier API

The surface is deliberately split into a builder and a handle:

1. `Server::builder() -> ServerBuilder` - typed, fluent, validated at
   `build()` time. The builder owns no runtime state.
2. `Server::start() -> ServerHandle` - returns immediately. The
   tokio runtime owns all background work. The caller drives the
   server through the handle.

This split lets configuration errors surface synchronously while the
running server is reduced to an async, observable, controllable
object.

## Component diagram

```
           +---------------------+
           | Embedding program   |
           +----------+----------+
                      |
                      | Server::builder()...build()
                      v
           +---------------------+
           |   ServerBuilder     |  (typed config + hooks)
           +----------+----------+
                      | .start()
                      v
           +---------------------+        +-----------------+
           |      Server         |------->|  tokio runtime  |
           +----------+----------+        +--------+--------+
                      | ServerHandle               |
                      v                            v
           +---------------------+   +-------------------------+
           |  control surface    |   |    background tasks     |
           |  shutdown / reload  |   |  conn pools (TCP/QUIC)  |
           |  stats / events     |   |  proxy + dnode listeners|
           |  inject_request     |   |  gossip + seeds refresh |
           |  peers / ring       |   |  proto parsers          |
           +---------------------+   |  stats aggregator       |
                                     |  entropy / repair       |
                                     +-------------------------+
```

Each block is documented in its own chapter:

* [Server lifecycle](./server.md) - the `ServerBuilder`,
  `ServerHandle`, and `ServerEvent` shapes.
* [Hooks and traits](./hooks.md) - the five pluggable traits, their
  default implementations, and when to write a custom one.
* [Examples](./examples.md) - three end-to-end embedding sketches
  that compose the surface above.

## Stability

Until 0.1 is cut, every public-API change in `dynomite::embed` is
recorded in `docs/journal/` with an `api-change:` entry and a
migration note. 0.1 is cut when the conformance suite is green; 1.0
is cut when SemVer is locked and `cargo public-api` shows no diffs
across two PRs. See `AGENTS.md` Section 13.
