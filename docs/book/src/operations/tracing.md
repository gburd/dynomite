# Distributed tracing and OTLP logs

`dynomited` ships with first-class OpenTelemetry support: when the
operator sets one or both of the OTLP endpoint fields, every
client request fans out into a span tree and every `tracing`
event is mirrored as an OTLP log record. Any OTLP-aware
collector (Jaeger, Tempo, Honeycomb, the OpenTelemetry
Collector, Loki via the OTLP receiver, ...) can consume the
resulting streams. Both exporters are **off by default** - the
binary pays no OTel-SDK cost when neither field is set.

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
    # Enables distributed tracing. OTLP gRPC URL of the trace
    # collector. Unset (or empty string) disables the trace
    # exporter entirely.
    otlp_traces_endpoint: "http://localhost:4317"
    # Enables the OTLP log appender. OTLP gRPC URL of the log
    # collector (often the same collector as the trace one).
    # Unset (or empty string) disables the log exporter
    # entirely. The fmt layer (stderr or `--output` file) keeps
    # writing in parallel; this knob only adds the OTLP
    # appender.
    otlp_logs_endpoint: "http://localhost:4317"
    # Optional. Overrides the service.name resource attribute
    # attached to every span and log record. Defaults to
    # "dynomited".
    service_name: "dynomited-prod-us-east-1"
    # Optional. Trace sampling ratio in [0.0, 1.0]. Values <1.0
    # apply a TraceIdRatioBased sampler. Defaults to 1.0
    # (record every trace). Does not affect the log appender:
    # every event that passes the global EnvFilter is exported.
    traces_sampling: 0.05
```

When both `otlp_traces_endpoint` and `otlp_logs_endpoint` are
unset, the binary uses the same plain `fmt` subscriber it
always has and pays no OTel-SDK cost. Either knob alone is
fine; the binary installs only the pillar(s) you turn on.

## Sample collector setup

A minimal OpenTelemetry Collector configuration that accepts
spans and log records from `dynomited` over OTLP/gRPC and
forwards them to a local Jaeger and a local file-backed log
store:

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
  file/logs:
    path: /var/log/dynomite/otlp.log

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [otlp/jaeger]
    logs:
      receivers: [otlp]
      exporters: [file/logs]
```

Run that side-by-side with `dynomited` and point the pool's
`otlp_traces_endpoint` and / or `otlp_logs_endpoint` at
`http://collector:4317`. Spans show up in Jaeger and log
records land in `/var/log/dynomite/otlp.log` within a few
seconds.

## Operator-visible knobs

| Knob | Default | Effect |
|---|---|---|
| `observability.otlp_traces_endpoint` | unset | OTLP/gRPC URL of the trace collector. Master switch for distributed tracing. |
| `observability.otlp_logs_endpoint` | unset | OTLP/gRPC URL of the log collector. Master switch for the log appender. |
| `observability.service_name` | `"dynomited"` | `service.name` resource attribute attached to spans and log records. |
| `observability.traces_sampling` | `1.0` | Per-trace sampling ratio in `[0.0, 1.0]`. Trace-only knob. |
| `RUST_LOG` | derived from `-v` | Standard tracing env-filter; affects the fmt layer, the OTel trace layer, and the OTLP log appender uniformly. |

## Trade-offs

- **Single global subscriber.** `tracing` only allows one
  global subscriber per process, so the binary composes the
  fmt layer (with the configured `--log-format` /
  `log_format:` shape and the SIGHUP-reopen handle), the
  `EnvFilter`, the OTel trace layer (when traces are on), and
  the OTLP log appender (when the log endpoint is on) into one
  Registry that is installed exactly once. The fmt layer's
  SIGHUP-reopen state is populated whether OTLP is on or off;
  SIGHUP-driven log rotation works in every mode.
- **Performance.** The OTel SDK's batch processors run on the
  existing tokio runtime; a 1.0 sampling ratio over a
  multi-thousand-QPS workload routinely doubles per-request
  allocation count vs the no-exporter baseline. Production
  deployments usually run at `traces_sampling: 0.01` or lower.
  The log appender does not have a sampling knob; gate volume
  via `RUST_LOG` instead.
- **Backpressure.** If the collector is unreachable the batch
  processors drop spans / log records after their internal
  queues fill (default 2048 each). They do not slow the
  request path.
- **Local logs and OTLP logs are independent.** The fmt layer
  always writes to the configured stderr / `--output` file.
  The OTLP log appender mirrors the same events to the
  collector. Operators see the local stream even if the
  collector is down.

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
- **Log-record body and attributes.** The OTLP log appender is
  the unmodified `opentelemetry-appender-tracing` bridge
  (0.27 release train). The event message lands in the OTLP
  log record body; structured fields land as record
  attributes. The instrumentation scope is fixed at
  `opentelemetry-appender-tracing`. Trace context is
  automatically attached when an event fires inside a span
  that the OTel trace layer also sees, so log records can be
  correlated to spans in the collector.
