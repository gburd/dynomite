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
