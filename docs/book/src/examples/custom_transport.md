# `embedded_custom_transport_sketch`

<div class="dyn-hero">
The <em>shape</em> of a custom transport plug-in -- how to make an
arbitrary byte stream implement the <code>Transport</code> trait. A
sketch, deliberately not wired into the running engine.
</div>

<p class="dyn-srclink">Source:
<code>crates/dynomite/examples/embedded_custom_transport_sketch.rs</code>
-- run with
<code>cargo run -p dynomite --example embedded_custom_transport_sketch</code></p>

## What it demonstrates

Two things, and it is careful about what it does *not* claim:

1. How to build an `AsyncRead + AsyncWrite + Send + Unpin` type -- here,
   over a `tokio::io::DuplexStream` -- which is the same pattern you use
   for a TLS stream, a Unix-domain socket, or a QUIC stream.
2. How that type implements [`Transport`](../transports/index.md) by
   carrying a `ConnRole` tag and reporting a `peer_addr`.

```rust,no_run
use dynomite::embed::{ConnRole, Transport};

pub struct PipeTransport {
    inner: tokio::io::DuplexStream,
    role: ConnRole,
    peer_addr: SocketAddr,
}
// impl AsyncRead / AsyncWrite by delegating to `inner`;
// impl Transport by returning `role` and `peer_addr`.
```

## Why it is only a sketch

```admonish warning title="Not a runnable plug-in yet"
The example does <strong>not</strong> plug a custom listener into the
embedded engine. Wiring a <code>Box&lt;dyn TransportListener&gt;</code>
into <code>ServerBuilder</code> is deferred work; until it lands, the
embedded engine's sanctioned in-process entry point is
<code>inject_request</code> (see
<a href="./embedded_cluster3.md"><code>embedded_cluster3</code></a>). The
sketch shows the trait shape so that when the setter arrives, swapping
the built-in <code>TcpListener</code> for an embedder-supplied source is
a one-line change.
```

This honesty is itself a documentation decision: the example teaches the
trait contract without pretending a capability exists. It compiles and
runs (it exercises the `PipeTransport` end to end over the duplex
stream), so the trait implementation is real -- only the *listener
integration* is pending.

## Design decisions and trade-offs

<dl class="dyn-facts">
<dt>DuplexStream as the byte source</dt>
<dd>An in-memory pipe needs no OS resources and makes the example
hermetic. The same <code>AsyncRead + AsyncWrite</code> bound is what a
real socket, TLS session, or QUIC stream satisfies, so the pattern
transfers unchanged.</dd>
<dt>ConnRole tag on the transport</dt>
<dd>A transport must say whether it carries client or peer traffic; that
tag is how the engine routes a connection to the right handler.</dd>
</dl>

```admonish note title="Road not taken: a transport-registry plugin system"
Rather than a general plugin registry, Dynomite models a transport as a
single trait an embedder implements and hands to the builder. The trait
is small on purpose -- a byte stream plus a role and an address -- so
that supporting a new transport is writing one adapter, not learning a
framework. TCP and QUIC in the shipped engine are exactly such adapters.
```

## When to use this pattern

When you need Dynomite to accept connections over something other than
TCP or QUIC -- an in-process pipe for testing, a Unix socket, or a bespoke
secure channel -- and you want to see the trait you will implement.

## Where to go next

* [Transports](../transports/index.md) documents the built-in TCP and
  QUIC transports that implement this same trait.
* [Hooks and Traits](../embedding/hooks.md) covers the other extension
  points.
