# Stats and metrics

Dynomite exposes its runtime counters, gauges, and histogram rollups
on the `stats_listen` HTTP endpoint. The same in-memory snapshot is
served in two formats so operators can pick the one that fits their
stack:

* `GET /` (and the aliases `/info` and `/stats`) returns the legacy
  Netflix Dynomite JSON layout. This is the original wire format and
  remains byte-for-byte stable; existing scrapers, dashboards, and
  scripts that target the legacy schema continue to work unchanged.
* `GET /metrics` returns Prometheus 0.0.4 text exposition. Every
  metric family is annotated with a `# HELP` description and a
  `# TYPE` declaration. This is the recommended path for modern
  observability stacks (Prometheus, VictoriaMetrics, Grafana Agent,
  Mimir, Thanos, OpenTelemetry collectors with the Prometheus
  receiver).

Both endpoints read the same cached `Snapshot` value the aggregator
publishes once per second, so they always agree.

## Metric reference

The table below covers every metric family the Prometheus endpoint
emits.

| Name | Type | Labels | Description |
| --- | --- | --- | --- |
| `dynomite_build_info` | gauge | `version`, `source`, `rack`, `dc` | Identification labels for the running engine. Value is always `1`. |
| `dynomite_uptime_seconds` | gauge | (none) | Seconds elapsed since the engine started. |
| `dynomite_timestamp_seconds` | gauge | (none) | Wall-clock seconds since the UNIX epoch at snapshot time. |
| `dynomite_alloc_msgs` | gauge | (none) | Message structs currently allocated. |
| `dynomite_free_msgs` | gauge | (none) | Message structs on the free list. |
| `dynomite_alloc_mbufs` | gauge | (none) | Mbuf chunks currently allocated. |
| `dynomite_free_mbufs` | gauge | (none) | Mbuf chunks on the free list. |
| `dynomite_memory_bytes` | gauge | (none) | Resident set size of the engine in bytes. |
| `dynomite_pool_<field>_total` | counter | `pool` | One per pool counter (e.g. `client_eof`, `client_read_requests`, `peer_requests`). The set is enumerated by `POOL_CODEC`. |
| `dynomite_pool_<field>` | gauge | `pool` | One per pool gauge or timestamp (e.g. `client_connections`, `peer_ejected_at`). |
| `dynomite_server_<field>_total` | counter | `server` | One per server counter (e.g. `read_requests`, `redis_req_get`). The set is enumerated by `SERVER_CODEC`. |
| `dynomite_server_<field>` | gauge | `server` | One per server gauge or timestamp (e.g. `in_queue`, `server_ejected_at`). |
| `dynomite_peer_state` | gauge | `peer`, `state` | `1` for the active state and `0` for the inactive one. The `state` label is `"up"` or `"down"`. |
| `dynomite_request_latency_microseconds` | gauge | `quantile` | Top-level request latency. The `quantile` label takes the values `mean`, `0.95`, `0.99`, `0.999`, `max`. |
| `dynomite_payload_size_bytes` | gauge | `quantile` | Observed payload sizes. Quantile labels match `dynomite_request_latency_microseconds`. |
| `dynomite_cross_region_latency_microseconds` | gauge | `quantile` | Cross-region peer round-trip latency. |
| `dynomite_cross_zone_latency_microseconds` | gauge | `quantile` | Cross-zone peer latency. |
| `dynomite_server_latency_microseconds` | gauge | `quantile` | Backing-server response latency. |
| `dynomite_cross_region_queue_wait_microseconds` | gauge | `quantile` | Cross-region queue wait time. |
| `dynomite_cross_zone_queue_wait_microseconds` | gauge | `quantile` | Cross-zone queue wait time. |
| `dynomite_server_queue_wait_microseconds` | gauge | `quantile` | Server queue wait time. |
| `dynomite_client_out_queue_p99` | gauge | (none) | 99th percentile of the client outbound queue length. |
| `dynomite_server_in_queue_p99` | gauge | (none) | 99th percentile of the server inbound queue length. |
| `dynomite_server_out_queue_p99` | gauge | (none) | 99th percentile of the server outbound queue length. |
| `dynomite_dnode_client_out_queue_p99` | gauge | (none) | 99th percentile of the dnode client outbound queue length. |
| `dynomite_peer_in_queue_p99` | gauge | (none) | 99th percentile of the local-DC peer inbound queue length. |
| `dynomite_peer_out_queue_p99` | gauge | (none) | 99th percentile of the local-DC peer outbound queue length. |
| `dynomite_remote_peer_in_queue_p99` | gauge | (none) | 99th percentile of the remote-DC peer inbound queue length. |
| `dynomite_remote_peer_out_queue_p99` | gauge | (none) | 99th percentile of the remote-DC peer outbound queue length. |

## Failure-cause counters

The metrics above describe what the engine is doing when traffic is
flowing normally. The counters in this section disambiguate the
different error causes the dispatcher and gossip planes can produce.
All of them initialise to zero and only become meaningful once the
operator wires the dispatcher and gossip handler with a shared
`FailureMetrics` accumulator (the `dynomited` binary does this
automatically; embedders do it via
`ClusterDispatcher::with_failure_metrics(...)` and
`GossipHandler::with_failure_metrics(...)`).

The counters answer questions like "is the cluster losing requests
because peers are flapping in and out of `Down`, or because
perper-peer outbound channels are saturated?" Pre-existing aggregate
error counters (`dynomite_pool_client_err_total`,
`dynomite_pool_client_dropped_requests_total`) report the total but
do not separate the cause; the families below do.

| Name | Type | Labels | Description |
| --- | --- | --- | --- |
| `dispatch_no_targets_total` | counter | `dc`, `rack`, `consistency_level` | Dispatcher returned `NoTargets` because the only routable peer for the hashed token was Down or absent. The `consistency_level` label is one of `DC_ONE`, `DC_QUORUM`, `DC_SAFE_QUORUM`, `DC_EACH_SAFE_QUORUM`. |
| `dispatch_peer_send_full_total` | counter | `peer_idx`, `peer_dc` | The dispatcher's `try_send` to a peer's outbound channel returned `Full`. Sustained values indicate the peer-supervisor task is not draining its inbound queue fast enough. |
| `dispatch_peer_send_closed_total` | counter | `peer_idx`, `peer_dc` | The dispatcher's `try_send` to a peer's outbound channel returned `Closed`. The peer-supervisor task has exited; expect a reconnect-supervised replacement to land soon. |
| `dispatch_backend_send_full_total` | counter | (none) | The dispatcher's `try_send` to the local datastore backend channel returned `Full`. The local backend driver is not keeping up with inbound throughput. |
| `dispatch_backend_send_closed_total` | counter | (none) | The dispatcher's `try_send` to the local datastore backend returned `Closed`. The backend driver task has exited. |
| `dispatch_response_timeout_total` | counter | `consistency_level` | The response coalescer or single-target responder gave up waiting for replies. Currently fires when every per-target sender drops without producing a reply. |
| `peer_state_transitions_total` | counter | `peer_idx`, `from_state`, `to_state` | Number of gossip-driven peer-state transitions. Both labels carry the [`PeerState`] string label (`UNKNOWN`, `JOINING`, `NORMAL`, `STANDBY`, `DOWN`, `RESET`, `LEAVING`). |
| `peer_state_current` | gauge | `peer_idx`, `dc`, `rack` | Current state of each non-local peer as a numeric code: `0=UNKNOWN`, `1=JOINING`, `2=NORMAL`, `3=STANDBY`, `4=DOWN`, `5=RESET`, `6=LEAVING`. |
| `gossip_phi_score_milli` | gauge | `peer_idx`, `dc`, `rack` | Current phi-accrual failure-detector score per peer, scaled by 1000 (i.e. emitted as thousandths). The default suspicion threshold is 8.0 (8000 here); divide by 1000 in PromQL to recover phi. |

The `_milli` suffix on `gossip_phi_score_milli` is deliberate.
Prometheus integer gauges are 64-bit signed; the phi value is a
floating-point number that we widen by 1000 to preserve thousandths
precision while staying within the integer-gauge wire format. A
suspicion threshold of `phi > 8.0` therefore corresponds to
`gossip_phi_score_milli > 8000` in PromQL.

## Active Anti-Entropy (AAE) counters

The AAE worker (Tictac merkle-tree exchange + repair sink) ships
its own family of counters and gauges. They start at zero and only
become meaningful once the embedding wires the AAE handle into
the scheduler and repair scheduler:

* `dyniak::aae::Scheduler::install_metrics(handle)` plus
  `Scheduler::observe_exchange_attempt` /
  `observe_exchange_success` / `observe_divergent_keys` from the
  per-tick hot path.
* `dyniak::aae::RepairScheduler::with_metrics(handle, dc, rack)`
  for the repair-dispatched count.
* `dyniak::aae::metrics::save_snapshot_with_metrics(...)` /
  `load_snapshot_with_metrics(...)` for the snapshot counters.

The families and labels:

| Name | Type | Labels | Description |
| --- | --- | --- | --- |
| `aae_exchange_attempts_total` | counter | `peer_idx`, `dc`, `rack` | One increment per AAE sweep tick that selected this peer, regardless of outcome. |
| `aae_exchange_success_total` | counter | `peer_idx`, `dc`, `rack` | One increment per exchange that completed without a transport error, regardless of whether divergences were found. |
| `aae_exchange_divergent_keys_total` | counter | `peer_idx`, `dc`, `rack` | Cumulative count of divergent keys observed during exchanges with this peer. Sustained growth means the cluster is producing repair traffic. |
| `aae_repair_dispatched_total` | counter | `peer_idx`, `dc`, `rack` | Cumulative count of repair tasks dispatched against this peer (winners + siblings). Outcomes that surfaced `AmbiguousClock` or `PeerUnavailable` do NOT contribute. |
| `aae_tree_segments_dirty_gauge` | gauge | `peer_idx` | Current count of segments needing a rebuild. Values that stay non-zero across sweep cycles indicate a stuck rebuild. |
| `aae_full_sweep_last_completed_seconds_gauge` | gauge | `peer_idx` | Wall-clock seconds since the UNIX epoch when this peer's most recent full sweep completed. Subtract from `time()` to get "seconds since". Zero means "never". |
| `aae_snapshot_save_total` | counter | (none) | Cumulative count of snapshot writes. |
| `aae_snapshot_load_total` | counter | (none) | Cumulative count of snapshot loads at process start. |
| `aae_snapshot_corruption_total` | counter | (none) | Cumulative count of snapshot rejections (`Corrupted`, `VersionSkew`, `BadShape`). A non-zero value is benign on a version bump but otherwise indicates filesystem damage. |

### Sample PromQL queries

Exchange success rate per peer (operators want this near 100%):

```promql
sum by (peer_idx) (rate(aae_exchange_success_total[5m]))
  /
sum by (peer_idx) (rate(aae_exchange_attempts_total[5m]))
```

Divergence rate per DC (a sustained non-zero rate is the signal
that AAE is doing useful work):

```promql
sum by (dc) (rate(aae_exchange_divergent_keys_total[5m]))
```

Seconds since last full sweep on every peer (alert when this
exceeds the configured `full_sweep_interval_seconds`):

```promql
time() - aae_full_sweep_last_completed_seconds_gauge
```

Snapshot health indicator (any non-zero corruption rate that is
NOT immediately after a deploy is a paging condition):

```promql
rate(aae_snapshot_corruption_total[15m])
```


### Sample PromQL queries

Total `NoTargets` rate per consistency level (this is the metric to
watch during chaos runs to confirm peer-state oscillation as the
root cause):

```promql
sum by (consistency_level) (
  rate(dispatch_no_targets_total[1m])
)
```

Per-peer flap count over the last hour (a peer that is flapping will
show a high transition count):

```promql
sum by (peer_idx) (
  increase(peer_state_transitions_total[1h])
)
```

Live phi score per peer, in raw phi units:

```promql
gossip_phi_score_milli / 1000
```

Dispatch error breakdown (one line per cause; useful as a stacked
graph in Grafana to see which cause dominates the error budget):

```promql
sum (rate(dispatch_no_targets_total[1m])) +
sum (rate(dispatch_peer_send_full_total[1m])) +
sum (rate(dispatch_peer_send_closed_total[1m])) +
sum (rate(dispatch_backend_send_full_total[1m])) +
sum (rate(dispatch_backend_send_closed_total[1m])) +
sum (rate(dispatch_response_timeout_total[1m]))
```

The histogram quantile rollups are emitted as gauges, not as
Prometheus histograms, because the engine stores Cassandra-style
estimated histograms whose internal buckets are not the standard
Prometheus `le` ladder. Exposing the pre-computed `mean`, `0.95`,
`0.99`, `0.999`, and `max` rollups keeps the wire payload small while
preserving the same percentiles the JSON endpoint already publishes.

## Sample scrape configuration

A minimal `prometheus.yml` snippet that scrapes a three-node cluster
on port `22222`:

```yaml
scrape_configs:
  - job_name: dynomite
    metrics_path: /metrics
    scrape_interval: 15s
    static_configs:
      - targets:
          - dynomite-0.example.internal:22222
          - dynomite-1.example.internal:22222
          - dynomite-2.example.internal:22222
        labels:
          cluster: prod-east
```

## Sample Grafana panel

A single-stat panel that charts cluster-wide request volume from the
counter `dynomite_pool_client_read_requests_total`:

```json
{
  "type": "timeseries",
  "title": "Pool requests/sec",
  "targets": [
    {
      "expr": "sum by (pool) (rate(dynomite_pool_client_read_requests_total[1m]))",
      "legendFormat": "{{pool}}",
      "refId": "A"
    }
  ],
  "fieldConfig": {
    "defaults": {
      "unit": "reqps"
    }
  }
}
```

Drop the panel into a Grafana dashboard JSON under `panels[]` and
adjust the `datasource` to point at your Prometheus instance.
