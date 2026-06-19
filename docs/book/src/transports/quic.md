# QUIC

QUIC is available when the engine is built with the `quic` Cargo
feature, which pulls in the `quiche` crate. It is offered on the
client plane and on the dyniak Riak PBC listener.

```sh
cargo build -p dynomited --features quic
```

## Wire shape

The QUIC transport is intentionally thin. It wraps a single
`quiche::Connection` in a tokio-driven event loop and serves the
configured protocol over one bidirectional stream per accepted
connection (the lowest client-initiated bidirectional stream). The
transport-agnostic connection state machines and protocol parsers run
unchanged on top; only the UDP socket and packet pump are
QUIC-specific. Multi-stream multiplexing is left to future revisions.

## TLS is mandatory

QUIC mandates TLS, so a QUIC listener always needs a certificate and
key.

* **Client plane:** select QUIC with `transport: quic` and supply
  `quic_cert_file:` and `quic_key_file:`. The listener binds a UDP
  socket.
* **dyniak PBC plane:** set `riak.quic_listen:` to a `host:port`. It
  reuses the `riak.tls_cert:` / `riak.tls_key:` pair; setting
  `quic_listen` without both is rejected at validation time.

If a QUIC address is configured but the binary was built without the
`quic` feature, `dynomited` fails fast at startup with a clean
configuration error rather than silently ignoring the directive.

```yaml
dyn_o_mite:
  listen: 127.0.0.1:8102
  dyn_listen: 127.0.0.1:8101
  tokens: '0'
  data_store: 0
  servers:
  - 127.0.0.1:6379:1
  transport: quic
  quic_cert_file: /etc/dynomited/server.crt
  quic_key_file: /etc/dynomited/server.key
```
