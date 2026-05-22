# Distributed tracing

`dynomited` ships with first-class OpenTelemetry support: when the
operator sets a single configuration field, every client request
fans out into a span tree that any OTLP-aware collector
(Jaeger, Tempo, Honeycomb, the OpenTelemetry Collector) can
consume. The exporter is **off by default** - the binary pays no
OTel-SDK cost when the field is unset.

## Span shape

A successful round-trip emits the following tree, anchored at
the per-connection accept span:

```text
client.accept                                 (per accepted client TCP connection)
  client.parse        msg_id, msg_type        (per parsed inbound request)
    dispatch.plan     req_id, plan, targets   (synchronous routing decision)
    backend.send      req_id, bytes           (local datastore write; LocalDatastore plan)
      backend.parse   req_id, bytes           (response parsed off the backend wire)
    peer.send         req_id, bytes           (cross-rack / cross-DC fan-out; Replicas plan)
      peer.parse      req_id, bytes           (DNODE response parsed off the peer wire)
    client.send       req_id, bytes           (response writeback to the client)
```

Plus the supervisor-level long-lived spans created at startup:

```text
server.run            pool, listen, peers
  proxy.run           local
  dnode_proxy.run     local
  backend_supervisor  backend, ds
    run_one_backend_conn
  peer_supervisor.spawn  peer_idx, peer
    peer_supervisor   peer
  stats_server.run    local
```

The cross-task work (`backend.send`, `backend.parse`,
`peer.send`, `peer.parse`, `client.send`) nests under the
originating `client.parse` span because the dispatcher captures
`tracing::Span::current()` on the way out of the dispatch call
and the receiver tasks re-enter that span before doing their
work. The default `Span::none()` is zero-cost, so this
propagation is free when the OTLP exporter is off.

## Configuration

Add an `observability:` block to the pool body:

```yaml
my_pool:
  listen: 0.0.0.0:8102
  dyn_listen: 0.0.0.0:8101
  tokens: '101134286'
  servers:
  - 127.0.0.1:22122:1
  data_store: 0
  observability:
    # Required to enable distributed tracing. OTLP gRPC URL of the
    # collector. Unset (or empty string) disables the exporter
    # entirely.
    otlp_traces_endpoint: "http://localhost:4317"
    # Optional. Overrides the service.name resource attribute
    # attached to every span. Defaults to "dynomited".
    service_name: "dynomited-prod-us-east-1"
    # Optional. Trace sampling ratio in [0.0, 1.0]. Values <1.0
    # apply a TraceIdRatioBased sampler. Defaults to 1.0
    # (record every trace).
    traces_sampling: 0.05
```

When `otlp_traces_endpoint` is unset, the binary uses the same
plain `fmt` subscriber it always has and pays no OTel-SDK cost.

## Sample collector setup

A minimal OpenTelemetry Collector configuration that accepts
spans from `dynomited` over OTLP/gRPC and forwards them to a
local Jaeger:

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317

exporters:
  otlp/jaeger:
    endpoint: jaeger:4317
    tls:
      insecure: true

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [otlp/jaeger]
```

Run that side-by-side with `dynomited` and point the pool's
`otlp_traces_endpoint` at `http://collector:4317`. Every client
GET / SET (or peer-forwarded request) shows up as a span tree in
Jaeger within a few seconds.

## Operator-visible knobs

| Knob | Default | Effect |
|---|---|---|
| `observability.otlp_traces_endpoint` | unset | OTLP/gRPC URL of the collector. Master switch. |
| `observability.service_name` | `"dynomited"` | `service.name` attribute on emitted spans. |
| `observability.traces_sampling` | `1.0` | Per-trace sampling ratio in `[0.0, 1.0]`. |
| `RUST_LOG` | derived from `-v` | Standard tracing env-filter; affects both stderr formatter and OTel layer. |

## Trade-offs

- **Single global subscriber.** `tracing` only allows one global
  subscriber per process. When the OTLP exporter is on, the
  binary installs a layered `EnvFilter` + `fmt` + OTel
  subscriber **before** `dynomite::core::log::log_init`; the
  log subsystem's STATE (used by `SIGHUP` log-reopen) is not
  initialised in that mode. SIGHUP log-reopen is therefore
  unavailable when the OTLP exporter is on. Operators that
  depend on SIGHUP-driven log rotation should run with
  `otlp_traces_endpoint` unset.
- **Performance.** The OTel SDK's batch span processor runs on
  the existing tokio runtime; a 1.0 sampling ratio over a
  multi-thousand-QPS workload routinely doubles per-request
  allocation count vs the no-exporter baseline. Production
  deployments usually run at `traces_sampling: 0.01` or lower.
- **Backpressure.** If the collector is unreachable the batch
  processor drops spans after its internal queue fills (default
  2048 spans). It does not slow the request path.

## Implementation notes

- **Trace context propagation.** The dispatcher hands an
  [`OutboundRequest`](https://docs.rs/dynomite) to either the
  backend supervisor or the per-peer DNODE driver via an `mpsc`
  channel. Both envelopes (`OutboundRequest` and
  `OutboundEnvelope`) carry a `tracing::Span` field that
  captures `tracing::Span::current()` at send time. The
  receiving task re-enters the captured span before doing its
  work so cross-task spans nest correctly under the originating
  client request.
- **Sync-only spans across awaits.** Where a span guard would
  otherwise cross an `.await` boundary (`EnteredSpan` is
  `!Send`), the code uses
  [`Span::in_scope`](https://docs.rs/tracing/latest/tracing/struct.Span.html#method.in_scope)
  for synchronous work and
  [`Instrument`](https://docs.rs/tracing/latest/tracing/instrument/trait.Instrument.html)
  to attach a span to a future. This keeps the spawned futures
  `Send` and tokio-spawn-compatible.
