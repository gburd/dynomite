//! Static metric descriptors for pool and server stats.
//!
//! The C reference uses `STATS_POOL_CODEC` and `STATS_SERVER_CODEC`
//! X-macros to emit a struct-of-arrays of metric descriptors. We follow
//! the same shape with a small `macro_rules!` so each metric gains a
//! typed handle, an iterable list, and constant metadata.

/// Kind of metric tracked by the stats subsystem.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum StatsMetricType {
    /// Monotonically increasing accumulator.
    Counter,
    /// Non-monotonic gauge that may go up or down.
    Gauge,
    /// Monotonic timestamp in seconds since epoch.
    Timestamp,
}

/// Static descriptor for a metric: how it is interpreted, what its
/// canonical lower-case name is, and a one-line human description.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct MetricSpec {
    /// Lower-case identifier as emitted in JSON.
    pub name: &'static str,
    /// Whether the metric is a counter, gauge, or timestamp.
    pub kind: StatsMetricType,
    /// Free-form description used by the `--describe-stats` CLI flag.
    pub description: &'static str,
}

macro_rules! define_codec {
    (
        $enum_name:ident, $codec_const:ident,
        { $( $variant:ident, $name:literal, $kind:ident, $desc:literal );* $(;)? }
    ) => {
        /// Typed handle for a metric; the `as usize` value of each
        /// variant is also its index into the metric vector.
        #[derive(Copy, Clone, Eq, PartialEq, Debug, Hash)]
        #[repr(usize)]
        pub enum $enum_name {
            $(
                #[doc = $desc]
                $variant
            ),*
        }

        impl $enum_name {
            /// All variants of this metric set, in canonical order.
            pub const ALL: &'static [$enum_name] = &[ $( Self::$variant ),* ];

            /// Lower-case identifier as it appears in the JSON output.
            pub fn name(self) -> &'static str {
                match self { $( Self::$variant => $name ),* }
            }

            /// Whether this metric is a counter, gauge, or timestamp.
            pub fn kind(self) -> StatsMetricType {
                match self { $( Self::$variant => StatsMetricType::$kind ),* }
            }

            /// Human-readable description used by `--describe-stats`.
            pub fn description(self) -> &'static str {
                match self { $( Self::$variant => $desc ),* }
            }

            /// Index of this metric in the corresponding stats vector.
            pub fn index(self) -> usize {
                self as usize
            }
        }

        /// Const slice of every metric descriptor in declaration order.
        pub const $codec_const: &[MetricSpec] = &[
            $(
                MetricSpec {
                    name: $name,
                    kind: StatsMetricType::$kind,
                    description: $desc,
                }
            ),*
        ];
    };
}

define_codec!(PoolField, POOL_CODEC, {
    ClientEof,                "client_eof",                Counter, "# eof on client connections";
    ClientErr,                "client_err",                Counter, "# errors on client connections";
    ClientConnections,        "client_connections",        Gauge,   "# active client connections";
    ClientReadRequests,       "client_read_requests",      Counter, "# client read requests";
    ClientWriteRequests,      "client_write_requests",     Counter, "# client write responses";
    ClientDroppedRequests,    "client_dropped_requests",   Counter, "# client dropped requests";
    ClientNonQuorumWResponses, "client_non_quorum_w_responses", Counter, "# client non quorum write responses";
    ClientNonQuorumRResponses, "client_non_quorum_r_responses", Counter, "# client non quorum read responses";
    ServerEjects,             "server_ejects",             Counter, "# times backend server was ejected";
    DnodeClientEof,           "dnode_client_eof",          Counter, "# eof on dnode client connections";
    DnodeClientErr,           "dnode_client_err",          Counter, "# errors on dnode client connections";
    DnodeClientConnections,   "dnode_client_connections",  Gauge,   "# active dnode client connections";
    DnodeClientInQueue,       "dnode_client_in_queue",     Gauge,   "# dnode client requests in incoming queue";
    DnodeClientInQueueBytes,  "dnode_client_in_queue_bytes", Gauge, "current dnode client request bytes in incoming queue";
    DnodeClientOutQueue,      "dnode_client_out_queue",    Gauge,   "# dnode client requests in outgoing queue";
    DnodeClientOutQueueBytes, "dnode_client_out_queue_bytes", Gauge, "current dnode client request bytes in outgoing queue";
    PeerDroppedRequests,      "peer_dropped_requests",     Counter, "# local dc peer dropped requests";
    PeerTimedoutRequests,     "peer_timedout_requests",    Counter, "# local dc peer timedout requests";
    RemotePeerDroppedRequests,"remote_peer_dropped_requests", Counter, "# remote dc peer dropped requests";
    RemotePeerTimedoutRequests,"remote_peer_timedout_requests", Counter, "# remote dc peer timedout requests";
    RemotePeerFailoverRequests,"remote_peer_failover_requests", Counter, "# remote dc peer failover requests";
    PeerEof,                  "peer_eof",                  Counter, "# eof on peer connections";
    PeerErr,                  "peer_err",                  Counter, "# errors on peer connections";
    PeerTimedout,             "peer_timedout",             Counter, "# timeouts on local dc peer connections";
    RemotePeerTimedout,       "remote_peer_timedout",      Counter, "# timeouts on remote dc peer connections";
    PeerConnections,          "peer_connections",          Gauge,   "# active peer connections";
    PeerForwardError,         "peer_forward_error",        Gauge,   "# times we encountered a peer forwarding error";
    PeerRequests,             "peer_requests",             Counter, "# peer requests";
    PeerRequestBytes,         "peer_request_bytes",        Counter, "total peer request bytes";
    PeerResponses,            "peer_responses",            Counter, "# peer respones";
    PeerResponseBytes,        "peer_response_bytes",       Counter, "total peer response bytes";
    PeerEjectedAt,            "peer_ejected_at",           Timestamp, "timestamp when peer was ejected";
    PeerEjects,               "peer_ejects",               Counter, "# times a peer was ejected";
    PeerInQueue,              "peer_in_queue",             Gauge,   "# local dc peer requests in incoming queue";
    RemotePeerInQueue,        "remote_peer_in_queue",      Gauge,   "# remote dc peer requests in incoming queue";
    PeerInQueueBytes,         "peer_in_queue_bytes",       Gauge,   "current peer request bytes in incoming queue";
    RemotePeerInQueueBytes,   "remote_peer_in_queue_bytes", Gauge,  "current peer request bytes in incoming queue to remote DC";
    PeerOutQueue,             "peer_out_queue",            Gauge,   "# local dc peer requests in outgoing queue";
    RemotePeerOutQueue,       "remote_peer_out_queue",     Gauge,   "# remote dc peer requests in outgoing queue";
    PeerOutQueueBytes,        "peer_out_queue_bytes",      Gauge,   "current peer request bytes in outgoing queue";
    RemotePeerOutQueueBytes,  "remote_peer_out_queue_bytes", Gauge, "current peer request bytes in outgoing queue to remote DC";
    PeerMismatchRequests,     "peer_mismatch_requests",    Counter, "current dnode peer mismatched messages";
    ForwardError,             "forward_error",             Counter, "# times we encountered a forwarding error";
    Fragments,                "fragments",                 Counter, "# fragments created from a multi-vector request";
    StatsCount,               "stats_count",               Counter, "# stats request";
});

define_codec!(ServerField, SERVER_CODEC, {
    ServerEof,                "server_eof",                Counter, "# eof on server connections";
    ServerErr,                "server_err",                Counter, "# errors on server connections";
    ServerTimedout,           "server_timedout",           Counter, "# timeouts on server connections";
    ServerEjectedAt,          "server_ejected_at",         Timestamp, "timestamp when server was ejected in usec since epoch";
    ServerDroppedRequests,    "server_dropped_requests",   Counter, "# server dropped requests";
    ServerTimedoutRequests,   "server_timedout_requests",  Counter, "# server timedout requests";
    ReadRequests,             "read_requests",             Counter, "# read requests";
    ReadRequestBytes,         "read_request_bytes",        Counter, "total read request bytes";
    WriteRequests,            "write_requests",            Counter, "# write requests";
    WriteRequestBytes,        "write_request_bytes",       Counter, "total write request bytes";
    ReadResponses,            "read_responses",            Counter, "# read respones";
    ReadResponseBytes,        "read_response_bytes",       Counter, "total read response bytes";
    WriteResponses,           "write_responses",           Counter, "# write respones";
    WriteResponseBytes,       "write_response_bytes",      Counter, "total write response bytes";
    InQueue,                  "in_queue",                  Gauge,   "# requests in incoming queue";
    InQueueBytes,             "in_queue_bytes",            Gauge,   "current request bytes in incoming queue";
    OutQueue,                 "out_queue",                 Gauge,   "# requests in outgoing queue";
    OutQueueBytes,            "out_queue_bytes",           Gauge,   "current request bytes in outgoing queue";
    RedisReqGet,              "redis_req_get",             Counter, "# Redis get";
    RedisReqSet,              "redis_req_set",             Counter, "# Redis set";
    RedisReqDel,              "redis_req_del",             Counter, "# Redis del";
    RedisReqIncrDecr,         "redis_req_incr_decr",       Counter, "# Redis incr or decr";
    RedisReqKeys,             "redis_req_keys",            Counter, "# Redis keys";
    RedisReqMget,             "redis_req_mget",            Counter, "# Redis mget";
    RedisReqScan,             "redis_req_scan",            Counter, "# Redis scan";
    RedisReqSort,             "redis_req_sort",            Counter, "# Redis sort";
    RedisReqLreqm,            "redis_req_lreqm",           Counter, "# Redis lreqm";
    RedisReqSunion,           "redis_req_sunion",          Counter, "# Redis sunion";
    RedisReqPing,             "redis_req_ping",            Counter, "# Redis ping";
    RedisReqLists,            "redis_req_lists",           Counter, "# Redis lists";
    RedisReqSets,             "redis_req_sets",            Counter, "# Redis sets";
    RedisReqHashes,           "redis_req_hashes",          Counter, "# Redis hashes";
    RedisReqSortedsets,       "redis_req_sortedsets",      Counter, "# Redis sortedsets";
    RedisReqOther,            "redis_req_other",           Counter, "# Redis other";
});

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_codec_indexes_align_with_variants() {
        for (i, variant) in PoolField::ALL.iter().copied().enumerate() {
            assert_eq!(variant.index(), i);
            assert_eq!(variant.name(), POOL_CODEC[i].name);
            assert_eq!(variant.kind(), POOL_CODEC[i].kind);
            assert_eq!(variant.description(), POOL_CODEC[i].description);
        }
    }

    #[test]
    fn server_codec_indexes_align_with_variants() {
        for (i, variant) in ServerField::ALL.iter().copied().enumerate() {
            assert_eq!(variant.index(), i);
            assert_eq!(variant.name(), SERVER_CODEC[i].name);
            assert_eq!(variant.kind(), SERVER_CODEC[i].kind);
            assert_eq!(variant.description(), SERVER_CODEC[i].description);
        }
    }

    #[test]
    fn pool_kinds_match_c_codec() {
        assert_eq!(PoolField::ClientConnections.kind(), StatsMetricType::Gauge);
        assert_eq!(PoolField::ClientEof.kind(), StatsMetricType::Counter);
        assert_eq!(PoolField::PeerEjectedAt.kind(), StatsMetricType::Timestamp);
    }

    #[test]
    fn pool_codec_has_expected_count() {
        assert_eq!(POOL_CODEC.len(), PoolField::ALL.len());
    }

    #[test]
    fn server_codec_has_expected_count() {
        assert_eq!(SERVER_CODEC.len(), ServerField::ALL.len());
    }
}
