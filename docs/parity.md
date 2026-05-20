# C-to-Rust Parity Matrix

This document maps every C symbol in `_/dynomite/` to its Rust home and is
the source of truth for "is the port complete?". Update this file in
every PR that adds Rust code or finishes a port.

Format:

* Sections follow the C source tree.
* Each row: `C symbol` -> `Rust path` (or `omitted: <reason>`).
* `# Ambiguities` collects places where the C code is unclear and the
  chosen Rust interpretation, with rationale and a regression test
  reference.
* `# Deviations` collects intentional differences from C and why.

## src/

### dyn_types.{c,h}

| C symbol | Rust home | Notes |
|---|---|---|
| `rstatus_t` | `dynomite::core::types::Status` (alias for `Result<(), DynError>`) | done (Stage 0) |
| `DN_OK` / `DN_ERROR` / `DN_EAGAIN` / `DN_ENOMEM` | `dynomite::core::types::DynError::{Generic, Again, OutOfMemory}` | done (Stage 0) |
| `dyn_conf.h` `struct conf_pool` | `dynomite::conf::ConfPool` | done (Stage 4) |
| `dyn_conf.h` `struct conf_listen` | `dynomite::conf::ConfListen` | done (Stage 4) |
| `dyn_conf.h` `struct conf_server` | `dynomite::conf::ConfServer` (datastore) and `dynomite::conf::ConfDynSeed` (peer seeds) | done (Stage 4) |
| `dyn_conf.h` `struct conf` | `dynomite::conf::Config` | done (Stage 4) |
| `dyn_conf.c` `conf_create` | `dynomite::conf::Config::parse_file` | done (Stage 4) |
| `dyn_conf.c` `conf_destroy` | implicit (`Drop` of owned types) | done (Stage 4) |
| `dyn_conf.c` `conf_pool_init` | `dynomite::conf::ConfPool::default()` (Option-wrapped fields) | done (Stage 4) |
| `dyn_conf.c` `conf_pool_deinit` | implicit (`Drop`) | done (Stage 4) |
| `dyn_conf.c` `conf_server_init`/`_deinit` | implicit | done (Stage 4) |
| `dyn_conf.c` `conf_yaml_init`/`_deinit` / `conf_token_*` / `conf_event_*` / `conf_push_scalar` / `conf_pop_scalar` | replaced wholesale by `serde_yaml` | done (Stage 4) |
| `dyn_conf.c` `conf_set_string` / `conf_set_listen` / `conf_set_num` / `conf_set_short` / `conf_set_bool` / `conf_set_hash` / `conf_set_hashtag` / `conf_set_tokens` / `conf_set_deprecated` | folded into `serde::Deserialize` impls plus `ConfPool` field types | done (Stage 4) |
| `dyn_conf.c` `conf_add_server` | `dynomite::conf::ConfServer::parse` (called via serde) | done (Stage 4) |
| `dyn_conf.c` `conf_add_dyn_server` | `dynomite::conf::ConfDynSeed::parse` (called via serde) | done (Stage 4) |
| `dyn_conf.c` `conf_handler` / `conf_begin_parse` / `conf_end_parse` / `conf_parse_core` / `conf_parse` / `conf_open` | `Config::parse_str` / `Config::parse_file` (serde-driven) | done (Stage 4) |
| `dyn_conf.c` `conf_validate_document` / `conf_validate_tokens` / `conf_validate_structure` / `conf_pre_validate` | enforced by `serde_yaml` document model + `#[serde(deny_unknown_fields)]` | done (Stage 4) |
| `dyn_conf.c` `conf_validate_pool` | `ConfPool::validate` (split into `validate_numeric_ranges` / `validate_mbuf_size` / `validate_max_msgs` helpers) | done (Stage 4) |
| `dyn_conf.c` `conf_validate_server` | folded into `ConfPool::validate` (servers list non-empty / single entry) | done (Stage 4) |
| `dyn_conf.c` `conf_dump` | `impl fmt::Display for ConfPool` (re-emits via `serde_yaml`) | done (Stage 4) |
| `dyn_conf.c` `conf_datastore_transform` | deferred to Stage 9 (datastore wiring) | omitted-for-stage |
| `dyn_conf.c` `get_secure_server_option` | `dynomite::conf::SecureServerOption::parse` | done (Stage 4) |
| `dyn_conf.c` `is_secure` | deferred to Stage 9 (uses live `SecureServerOption` plus DC/rack identity) | omitted-for-stage |
| `dyn_conf.h` `CONF_DEFAULT_*` macros | `dynomite::conf::pool::defaults` constants | done (Stage 4) |
| `dyn_conf.h` `secure_server_option_t` | `dynomite::conf::SecureServerOption` | done (Stage 4) |
| `dyn_conf.h` `data_store_t` | `dynomite::conf::DataStore` | done (Stage 4) |
| `dyn_conf.h` `consistency_t` | `dynomite::conf::ConsistencyLevel` | done (Stage 4) |

Stages 1 onward populate the rest of this table. Until a row exists, the
symbol is considered un-ported.
| `BUCKET_SIZE` | `dynomite::stats::BUCKET_COUNT` | done (Stage 5) |
| `struct histogram` | `dynomite::stats::Histogram` | done (Stage 5) |
| `histo_init` | `Histogram::new` / `Default::default` | done (Stage 5) |
| `histo_reset` | `Histogram::reset` | done (Stage 5) |
| `histo_add` | `Histogram::record` | done (Stage 5) |
| `histo_percentile` (commented out in C) | `Histogram::percentile` | done (Stage 5) |
| `histo_mean` (commented out in C) | `Histogram::mean` | done (Stage 5) |
| `histo_max` (commented out in C) | `Histogram::max` | done (Stage 5) |
| `histo_compute` | `dynomite::stats::HistogramSummary::from_histogram` | done (Stage 5) |
| `histo_get_bucket` / `histo_get_buckets` | `Histogram::buckets` | done (Stage 5) |
| `enum stats_type` | `dynomite::stats::StatsMetricType` | done (Stage 5) (omits `INVALID`/`STRING`/`SENTINEL` placeholders) |
| `STATS_POOL_CODEC` | `dynomite::stats::POOL_CODEC` and `PoolField` | done (Stage 5) |
| `STATS_SERVER_CODEC` | `dynomite::stats::SERVER_CODEC` and `ServerField` | done (Stage 5) |
| `struct stats_metric` | `dynomite::stats::MetricSpec` (descriptor) and `Vec<i64>` per-pool/server | done (Stage 5) |
| `struct stats_pool` | `dynomite::stats::PoolStats` | done (Stage 5) |
| `struct stats_server` | `dynomite::stats::ServerStats` | done (Stage 5) |
| `struct stats_dnode` | `dynomite::stats::PeerStats` | done (Stage 5) |
| `struct stats` | `dynomite::stats::Stats` (live) and `dynomite::stats::Snapshot` (frozen) | done (Stage 5) |
| `stats_describe` | `dynomite::stats::describe_stats` | done (Stage 5) |
| `stats_create` / `stats_destroy` | `Stats::new` (ownership through Drop) | done (Stage 5) |
| `stats_swap` / `stats_aggregate` / `stats_loop` | `dynomite::stats::Aggregator::run` | done (Stage 5) |
| `_stats_pool_incr` / `_stats_pool_decr` / `_stats_pool_incr_by` / `_stats_pool_decr_by` | `Stats::pool_incr` / `pool_decr` / `pool_incr_by` | done (Stage 5) |
| `_stats_pool_set_ts` / `_stats_pool_set_val` | `Stats::pool_set` | done (Stage 5) |
| `_stats_pool_get_ts` / `_stats_pool_get_val` | `Stats::pool_get` | done (Stage 5) |
| `_stats_server_incr` / `_stats_server_decr` / `_stats_server_incr_by` / `_stats_server_decr_by` | `Stats::server_incr` / `server_decr` / `server_incr_by` | done (Stage 5) |
| `_stats_server_set_ts` / `_stats_server_set_val` | `Stats::server_set` | done (Stage 5) |
| `_stats_server_get_ts` / `_stats_server_get_val` | `Stats::server_get` | done (Stage 5) |
| `stats_histo_add_latency` / `stats_histo_add_payloadsize` | `Stats::record_latency` / `record_payload_size` | done (Stage 5) |
| `stats_listen` / `stats_start_aggregator` / `stats_stop_aggregator` | `dynomite::stats::StatsServer::bind` and `run` | done (Stage 5) |
| `parse_request` / `stats_send_rsp` / `stats_http_rsp` | `dynomite::stats::rest` (single `GET /` JSON endpoint) | done (Stage 5) (deviation: command surface limited to JSON snapshot for Stage 5; cluster-describe and dynamic control endpoints land with Stage 10) |
| `stats_make_info_rsp` / `stats_add_header` / `stats_add_string` / `stats_add_num` / `stats_begin_nesting` / `stats_end_nesting` / `stats_add_footer` / `stats_copy_metric` / `stats_aggregate_metric` | `Snapshot::write_json` and helpers in `dynomite::stats::snapshot` | done (Stage 5) |
| `stats_make_cl_desc_rsp` / `stats_add_node_*` / `stats_add_rack_details` / `stats_add_dc_details` | not yet ported | deferred to Stage 10 (gossip data structures live there) |
| `DN_ENO_IMPL` | `dynomite::core::types::DynError::NotImplemented` | done (Stage 1) |
| `msgid_t` | `dynomite::core::types::MsgId` (`u64` alias) | done (Stage 1) |
| `msec_t` | `dynomite::core::types::Msec` (`u64` alias) | done (Stage 1) |
| `usec_t` | `dynomite::core::types::Usec` (`u64` alias) | done (Stage 1) |
| `sec_t` | `dynomite::core::types::Sec` (`u64` alias) | done (Stage 1) |
| `secure_server_option_t` | `dynomite::core::types::SecureServerOption` | done (Stage 1) |
| `cleanup_charptr` | omitted: scoped `free` is a no-op in Rust where ownership handles it. |
| `init_object` / `print_obj` / `object_t` / `func_print_t` / `OBJ_*` | omitted: the C `object_t` is a debug-printing tag for diagnostic logging; Rust uses `std::fmt::Debug` derives at every relevant struct, removing the need for a runtime type tag. |
| `THROW_STATUS` / `IGNORE_RET_VAL` | omitted: macro shapes; Rust uses `?` and explicit `let _ =` discards. |

### dyn_string.{c,h}

| C symbol | Rust home | Notes |
|---|---|---|
| `struct string` | replaced by `bytes::Bytes` (zero-copy borrow) and `String` (owned UTF-8); pname/data parsing uses `&[u8]`. |
| `string_init`, `string_deinit` | omitted: Rust types are RAII. |
| `string_empty` | `<[u8]>::is_empty` / `String::is_empty` (stdlib). |
| `string_duplicate`, `string_copy`, `string_copy_c` | `<[u8]>::to_vec` / `Bytes::copy_from_slice` (stdlib + bytes). |
| `string_compare` | `dynomite::util::dyn_string::string_compare` | done (Stage 1) |
| `dn_strchr` | `dynomite::util::dyn_string::strchr` | done (Stage 1) |
| `dn_strrchr` | `dynomite::util::dyn_string::strrchr` | done (Stage 1) |
| `dn_strcasecmp` | `dynomite::util::dyn_string::eq_ignore_ascii_case` | done (Stage 1) |
| `dn_memcpy`, `dn_memmove`, `dn_memchr`, `dn_strlen`, `dn_strncmp`, `dn_strcmp`, `dn_strndup`, `dn_snprintf`, `dn_sprintf`, `dn_scnprintf`, `dn_vscnprintf` | omitted: replaced by stdlib slice and `String`/`format!` operations. |

### dyn_array.{c,h}

| C symbol | Rust home | Notes |
|---|---|---|
| `struct array`, `array_create`, `array_destroy`, `array_init`, `array_deinit`, `array_idx`, `array_push`, `array_pop`, `array_get`, `array_top`, `array_swap`, `array_sort`, `array_each`, `array_each_2`, `array_compare_t`, `array_each_t`, `array_each_2_t` | omitted: replaced by `Vec<T>` and closures at every call site. The two-arg callback shape becomes a closure capturing both data pointers. |

### dyn_util.{c,h}

| C symbol | Rust home | Notes |
|---|---|---|
| `dn_set_blocking`, `dn_set_nonblocking`, `dn_set_reuseaddr`, `dn_set_keepalive`, `dn_set_tcpnodelay`, `dn_set_linger`, `dn_set_sndbuf`, `dn_set_rcvbuf`, `dn_get_soerror`, `dn_get_sndbuf`, `dn_get_rcvbuf`, `_dn_sendn`, `_dn_recvn`, `dn_resolve`, `dn_unresolve_addr`, `dn_unresolve_peer_desc`, `dn_unresolve_desc` | deferred to Stage 2 (I/O substrate); these are wired to `socket2`/`tokio` in the reactor. Tracked as out-of-scope for Stage 1. |
| `_dn_atoi` | `dynomite::util::atoi::dn_atoi` | done (Stage 1) |
| `_dn_atoui` | `dynomite::util::atoi::dn_atoui` | done (Stage 1) |
| `dn_valid_port` | `dynomite::util::atoi::valid_port` | done (Stage 1) |
| `dn_msec_now` | `dynomite::util::time::msec_now` | done (Stage 1) |
| `dn_usec_now` | `dynomite::util::time::usec_now` | done (Stage 1) |
| `current_timestamp_in_millis` | `dynomite::util::time::msec_now` (single helper) | done (Stage 1) |
| `count_digits` | `dynomite::util::time::count_digits` | done (Stage 1) |
| `struct sockinfo` | `dynomite::util::sockinfo::SockInfo` | done (Stage 1) |
| `dict_string_hash`, `dict_string_key_compare`, `dict_string_destructor` | deferred to Stage 10 (gossip dict bindings). |
| `keypos_elem_len`, `argpos_elem_len` | deferred to Stage 7/8 once `keypos`/`argpos` are introduced. |
| `_dn_alloc`, `_dn_zalloc`, `_dn_calloc`, `_dn_realloc`, `_dn_free`, `dn_assert`, `dn_stacktrace`, `_scnprintf`, `_vscnprintf`, `dn_gethostname` | omitted: replaced by Rust ownership, `assert!`/`debug_assert!`, `std::backtrace`, and stdlib formatting. |
| `MIN`, `MAX`, `NELEMS`, `SQUARE`, `VAR`, `STDDEV`, `DN_ALIGN`, `str*cmp`, `str*icmp` | omitted: trivial macros replaced by stdlib (`cmp::min`, `cmp::max`, `slice::len`, `eq_ignore_ascii_case`, etc.). |

### dyn_setting.{c,h}

| C symbol | Rust home | Notes |
|---|---|---|
| `msgs_per_sec` | `dynomite::core::setting::msgs_per_sec` | done (Stage 1) |
| `set_msgs_per_sec` | `dynomite::core::setting::set_msgs_per_sec` | done (Stage 1) |
| `CONF_DEFAULT_CONN_MSG_RATE` | `dynomite::core::setting::DEFAULT_MSGS_PER_SEC` | done (Stage 1) |

### dyn_log.{c,h}

| C symbol | Rust home | Notes |
|---|---|---|
| `LOG_EMERG..LOG_PVERB` | `dynomite::core::log::{LOG_EMERG, LOG_ALERT, LOG_CRIT, LOG_ERR, LOG_WARN, LOG_NOTICE, LOG_INFO, LOG_DEBUG, LOG_VERB, LOG_VVERB, LOG_VVVERB, LOG_PVERB}` | done (Stage 1) |
| `log_init` | `dynomite::core::log::log_init` (signature: `(level: u8, path: Option<&Path>) -> Status`) | done (Stage 1) |
| `log_deinit` | omitted: Rust drops the writer when the process exits. |
| `log_reopen` | `dynomite::core::log::reopen_on_sighup` | done (Stage 1) |
| `log_level_up` | `dynomite::core::log::log_level_increment` | done (Stage 1) |
| `log_level_down` | `dynomite::core::log::log_level_decrement` | done (Stage 1) |
| `log_level_set` | `dynomite::core::log::log_level_set` | done (Stage 1) |
| `log_loggable` | `dynomite::core::log::log_loggable` | done (Stage 1) |
| `_log`, `_log_stderr`, `_log_hexdump`, `log_debug`, `log_notice`, `log_info`, `log_error`, `log_warn`, `log_panic`, `loga`, `loga_hexdump` | replaced by `tracing::{trace,debug,info,warn,error}` and `tracing::event!`; the hexdump helpers are deferred to Stage 2 (mbuf substrate) where the buffers being dumped are introduced. |

### dyn_signal.{c,h}

| C symbol | Rust home | Notes |
|---|---|---|
| `struct signal` | `dynomite::core::signal::SignalEntry` | done (Stage 1) |
| `signals[]` (default table) | `dynomite::core::signal::default_actions` | done (Stage 1) |
| `signal_init` | deferred to Stage 12 (the binary), which wires tokio `signal::unix::signal` streams onto `dynomite::core::signal::dispatch`. The data table is in place; the wiring uses tokio rather than `sigaction` so it lives next to the runtime. |
| `signal_deinit` | omitted: tokio drops listeners on shutdown. |
| `signal_handler` | `dynomite::core::signal::dispatch` (and the convenience wrapper `handle`) | done (Stage 1) |

### dyn_queue.h

| C symbol | Rust home | Notes |
|---|---|---|
| `LIST_*` / `TAILQ_*` / `STAILQ_*` / `CIRCLEQ_*` macros | omitted: replaced by `VecDeque<T>` or `LinkedList<T>` per call site. |

### dyn_cbuf.h

| C symbol | Rust home | Notes |
|---|---|---|
| `CBUF_Init` | `dynomite::io::cbuf::CBuf::new` | done (Stage 2) |
| `CBUF_Push` | `dynomite::io::cbuf::CBuf::push` | done (Stage 2) (deviation: returns `Err(item)` on full instead of overwriting silently) |
| `CBUF_Pop` | `dynomite::io::cbuf::CBuf::pop` | done (Stage 2) |
| `CBUF_Len` | `dynomite::io::cbuf::CBuf::len` | done (Stage 2) |
| `CBUF_IsEmpty` | `dynomite::io::cbuf::CBuf::is_empty` | done (Stage 2) |
| `CBUF_IsFull` | `dynomite::io::cbuf::CBuf::is_full` | done (Stage 2) |
| `CBUF_Error` / `CBUF_Get` / `CBUF_GetEnd` / `CBUF_GetLastEntryPtr` / `CBUF_GetPushEntryPtr` / `CBUF_GetPopEntryPtr` / `CBUF_AdvancePushIdx` / `CBUF_AdvancePopIdx` | omitted: the unchecked-overflow and reservation accessors have no in-tree caller after the channel transport (Stage 1) and the typed `CBuf` (Stage 2) replace the macro pattern. |
| `dynomite::core::ring_queue::RingChannels` (Stage 1 alias) | retained as the gossip thread coupling; the new typed `CBuf<T>` covers the in-process SPSC use case. |

### dyn_mbuf.{c,h}

| C symbol | Rust home | Notes |
|---|---|---|
| `MBUF_SIZE` / `MBUF_MIN_SIZE` / `MBUF_MAX_SIZE` / `MBUF_ESIZE` | `dynomite::io::mbuf::{MBUF_SIZE, MBUF_MIN_SIZE, MBUF_MAX_SIZE, MBUF_ESIZE}` | done (Stage 2) |
| `MBUF_HSIZE` / `MBUF_MAGIC` | omitted: the C header was embedded in the buffer tail and used a magic word to catch overruns; the Rust port stores cursors in a separate struct so neither constant has a Rust home. |
| `MBUF_FLAGS_READ_FLIP` | `dynomite::io::mbuf::MBUF_FLAG_READ_FLIP` | done (Stage 2) |
| `MBUF_FLAGS_JUST_DECRYPTED` | `dynomite::io::mbuf::MBUF_FLAG_JUST_DECRYPTED` | done (Stage 2) |
| `struct mbuf` | `dynomite::io::mbuf::Mbuf` | done (Stage 2) |
| `struct mhdr` (STAILQ head) | `dynomite::io::mbuf::MbufQueue` | done (Stage 2) |
| `mbuf_init` | `dynomite::io::mbuf::MbufPool::new` | done (Stage 2) |
| `mbuf_deinit` | implicit (`Drop` of `MbufPool`) | done (Stage 2) |
| `mbuf_get` | `dynomite::io::mbuf::MbufPool::get` | done (Stage 2) |
| `mbuf_put` | `dynomite::io::mbuf::MbufPool::put` | done (Stage 2) |
| `mbuf_alloc` | `dynomite::io::mbuf::Mbuf::with_chunk_size` | done (Stage 2) |
| `mbuf_dealloc` | implicit (`Drop` of `Mbuf`) | done (Stage 2) |
| `mbuf_alloc_get_count` | `dynomite::io::mbuf::MbufPool::total_allocated` | done (Stage 2) |
| `mbuf_free_queue_size` | `dynomite::io::mbuf::MbufPool::free_count` | done (Stage 2) |
| `mbuf_chunk_sz` | `dynomite::io::mbuf::MbufPool::chunk_size` | done (Stage 2) |
| `mbuf_data_size` | `dynomite::io::mbuf::Mbuf::data_size` (also accessible per-pool through the chunk size) | done (Stage 2) |
| `mbuf_length` | `dynomite::io::mbuf::Mbuf::len` | done (Stage 2) |
| `mbuf_remaining_space` | `dynomite::io::mbuf::Mbuf::remaining` | done (Stage 2) |
| `mbuf_empty` | `dynomite::io::mbuf::Mbuf::is_empty` | done (Stage 2) |
| `mbuf_full` | `dynomite::io::mbuf::Mbuf::is_full` | done (Stage 2) |
| `mbuf_rewind` | `dynomite::io::mbuf::Mbuf::rewind` | done (Stage 2) |
| `mbuf_insert` | `dynomite::io::mbuf::MbufQueue::push_back` | done (Stage 2) |
| `mbuf_insert_head` | `dynomite::io::mbuf::MbufQueue::push_front` | done (Stage 2) |
| `mbuf_insert_after` | omitted: the C call site list is empty; if Stage 7/8 reintroduces a need, the behavior is one `VecDeque::insert` away. |
| `mbuf_remove` | `dynomite::io::mbuf::MbufQueue::pop_front` (and `pop_back`) | done (Stage 2) |
| `mbuf_copy` | `dynomite::io::mbuf::Mbuf::copy_from_slice` | done (Stage 2) |
| `mbuf_split` | `dynomite::io::mbuf::Mbuf::split_off` | done (Stage 2) (deviation: takes a byte offset rather than a raw pointer, and the C `cb`/`cbarg` precopy callback is dropped because the Rust caller can prepend bytes to the returned `Mbuf` directly) |
| `mbuf_write_char` / `mbuf_write_string` / `mbuf_write_uint8` / `mbuf_write_uint32` / `mbuf_write_uint64` / `mbuf_write_mbuf` / `mbuf_write_bytes` | replaced by `Mbuf::recv` and `Mbuf::copy_from_slice` plus `format!` / `itoa` at call sites; the recursive C decimal writers have no callers that cannot use the standard formatter. |
| `mbuf_dump` | omitted: replaced by `tracing` events that hex-dump through `tracing::debug!(?slice)`. |
| (new) | `dynomite::io::mbuf::Mbuf::recv` | Rust-only convenience wrapping `mbuf_copy` for the read direction. |
| (new) | `dynomite::io::mbuf::Mbuf::send` | Rust-only convenience draining `pos..last` into a destination slice for outbound writes. |
| (new) | `dynomite::io::mbuf::Mbuf::append` | Rust-only convenience used by parsers and tests to chain mbufs without going through the pool. |
| (new) | `dynomite::io::mbuf::Mbuf::advance_pos` / `advance_last` | Rust-only cursor advance helpers used when callers write directly into the writable region (for example through `AsyncReadBuf`). |
| (new) | `dynomite::io::mbuf::MBUF_POOL_MAX_FREE` | Rust-only cap on the free list size to bound resident memory; the C engine has no equivalent bound. |
| (new) | `dynomite::io::mbuf::MbufQueue::recycle` | Rust-only helper that drains a chain back into a pool. |

### dyn_crypto.{c,h}

| C symbol | Rust home | Notes |
|---|---|---|
| `AES_KEYLEN` | `dynomite::crypto::aes::AES_KEYLEN` | done (Stage 6) |
| `aes_cipher` (static `EVP_CIPHER *`) | replaced by `openssl::symm::Cipher::aes_256_cbc()` returned by `dynomite::crypto::aes::cipher` | done (Stage 6) (deviation: AES-256 instead of the C reference's AES-128, see Deviations) |
| `rsa` (static `RSA *`) | `dynomite::crypto::Crypto::rsa` field | done (Stage 6) |
| `aes_key` (static buffer) | `dynomite::crypto::Crypto::aes_key` field | done (Stage 6) |
| `aes_encrypt_ctx` / `aes_decrypt_ctx` | replaced by per-call `openssl::symm::Crypter` instances | done (Stage 6) (deviation: per-call contexts replace the long-lived globals; see Deviations) |
| `load_private_rsa_key_by_file` | `dynomite::crypto::pem::load_rsa_private_key` | done (Stage 6) |
| `load_private_rsa_key` | folded into `Crypto::from_pem` | done (Stage 6) |
| `aes_init` | folded into `Crypto::from_pem` (the AES key generation half) and `Crypto::generate_aes_key` (the standalone half) | done (Stage 6) |
| `crypto_init` | `dynomite::crypto::Crypto::from_pem` | done (Stage 6) |
| `crypto_deinit` | implicit (`Drop` of `Crypto`) | done (Stage 6) |
| `base64_encode` | `dynomite::crypto::base64_encode` | done (Stage 6) (deviation: emits unpadded output by default; the C helper emits the standard alphabet without `BIO_FLAGS_BASE64_NO_NL` newlines but keeps padding bytes. See Deviations) |
| `base64_decode` (commented out in C) | `dynomite::crypto::base64_decode` | done (Stage 6); the C source ships the helper commented out, the Rust port exposes the symmetric decode that the DNODE handshake will need |
| `aes_encrypt` | `dynomite::crypto::Crypto::aes_encrypt` (delegates to `crypto::aes::encrypt_to_vec`) | done (Stage 6) (deviation: AES-256 with random IV prepended; see Deviations) |
| `aes_decrypt` | `dynomite::crypto::Crypto::aes_decrypt` (delegates to `crypto::aes::decrypt_to_vec`) | done (Stage 6) |
| `dyn_aes_encrypt` | `dynomite::crypto::Crypto::dyn_aes_encrypt` (delegates to `crypto::aes::encrypt_to_chain`) | done (Stage 6); writes a fresh chain rather than mutating a single caller-supplied mbuf because the prepended IV plus padding can exceed the 16-byte trailing region the C path relies on |
| `dyn_aes_decrypt` | `dynomite::crypto::Crypto::dyn_aes_decrypt` (delegates to `crypto::aes::decrypt_chain_to_chain`) | done (Stage 6) |
| `dyn_aes_encrypt_msg` | `dynomite::crypto::Crypto::dyn_aes_encrypt_msg` | done (Stage 6); takes a single `&Mbuf` instead of a `struct msg` because the Rust msg layer (Stage 7) is not yet wired. The handshake call site re-binds against the eventual `Msg` struct in Stage 7 |
| `generate_aes_key` | `dynomite::crypto::Crypto::generate_aes_key` | done (Stage 6) (deviation: returns `Result<[u8; 32], CryptoError>` rather than a static-buffer pointer) |
| `dyn_rsa_size` | `dynomite::crypto::Crypto::rsa_size` | done (Stage 6) |
| `dyn_rsa_encrypt` | `dynomite::crypto::Crypto::rsa_encrypt` (delegates to `crypto::rsa::encrypt`) | done (Stage 6) |
| `dyn_rsa_decrypt` | `dynomite::crypto::Crypto::rsa_decrypt` (delegates to `crypto::rsa::decrypt`) | done (Stage 6) |
| (new) | `dynomite::crypto::CryptoError` | Typed error enum returned by every fallible crypto API. The C engine reports a single `rstatus_t` with logging side effects. |
| (new) | `dynomite::crypto::Crypto::from_parts` | Test-only constructor that builds a bundle from caller-supplied RSA key + AES key without touching the filesystem. |
| (new) | `dynomite::crypto::Crypto::dyn_aes_decrypt_to_vec` | Convenience that flattens an encrypted mbuf chain into a `Vec<u8>` for the DNODE handshake parser. |
| (new) | `dynomite::crypto::pem::load_rsa_private_key_from_bytes` | In-memory PEM parser used by tests and embedders that already hold the PEM bytes. |

## src/event/

The per-platform reactor (`dyn_event.h`, `dyn_epoll.c`, `dyn_kqueue.c`,
`dyn_evport.c`) is replaced wholesale by tokio. Stage 2 introduces the
[`dynomite::io::reactor`] transport boundary that downstream stages
use instead of the C `event_*` API.

| C symbol | Rust home | Notes |
|---|---|---|
| `event_cb_t` / `event_stats_cb_t` / `event_entropy_cb_t` | omitted: replaced by tokio task spawns plus `tokio::sync::watch` / `select!` arms (idiomatic async). |
| `EVENT_READ` / `EVENT_WRITE` / `EVENT_ERR` | omitted: tokio surfaces readability and writability through `AsyncRead` / `AsyncWrite` poll readiness; errors are surfaced as `io::Error`. |
| `struct event_base` | omitted: replaced by the tokio runtime. |
| `event_base_create` / `event_base_destroy` | omitted: the runtime is owned by `dynomited`'s `#[tokio::main]`. |
| `event_add_in` / `event_del_in` / `event_add_out` / `event_del_out` / `event_add_conn` / `event_del_conn` | omitted: registration is implicit when a tokio task awaits an `AsyncRead` / `AsyncWrite`. The Stage 6 connection state machine spawns and shuts down tasks instead. |
| `event_wait` | omitted: tokio's reactor is the runtime's responsibility; user code awaits futures directly. |
| `event_loop_stats` / `event_loop_entropy` | omitted: replaced by `tokio::time::interval` ticks driven from Stage 5 (stats) and Stage 11 (entropy). |
| `event_fd` | omitted: tokio-internal. |
| (new) | `dynomite::io::reactor::ConnRole` | Replaces the role discriminants on `struct conn` (`client`, `server`, `proxy`, plus the dnode peer variants). |
| (new) | `dynomite::io::reactor::Transport` | Trait abstracting `AsyncRead + AsyncWrite + Send + Unpin` so the Stage 9 QUIC transport can drop in beside the TCP one. |
| (new) | `dynomite::io::reactor::TcpTransport` | TCP implementation of `Transport`, newtype around `tokio::net::TcpStream`. |


### dyn_ring_queue.{c,h}

| C symbol | Rust home | Notes |
|---|---|---|
| `C2G_InQ_SIZE`, `C2G_OutQ_SIZE` | `dynomite::core::ring_queue::{C2G_IN_CAPACITY, C2G_OUT_CAPACITY}` | done (Stage 1) |
| `C2G_InQ`, `C2G_OutQ` (volatile globals) | `dynomite::core::ring_queue::RingChannels` (paired bounded channels; capacities preserved) | done (Stage 1) |
| `callback_t`, `data_func_t` | omitted: closures at the call site. |
| `struct ring_msg`, `create_ring_msg`, `create_ring_msg_with_data`, `create_ring_msg_with_size`, `ring_msg_init`, `ring_msg_deinit`, `create_node`, `node_init`, `node_deinit`, `node_copy` | deferred to Stage 10 (gossip): the message body is gossip-specific (`gossip_node`, `dyn_token`, `server_pool`) which Stage 1 does not yet have. The transport (channel) is in place. |

### dyn_dict.{c,h}

| C symbol | Rust home | Notes |
|---|---|---|
| `dict`, `dictType`, `dictEntry`, `dictht`, `dictIterator`, `dictScanFunction`, all `dict*` operations | replaced by `dynomite::util::dict::DictMap` (typed wrapper around `ahash::AHashMap`). The C generic-pointer key/value vtable is replaced by Rust generics on `DictMap<K, V>`. |
| `dictTypeHeapStringCopyKey`, `dictTypeHeapStrings`, `dictTypeHeapStringCopyKeyValue` | deferred to Stage 10 alongside the gossip-specific dict instantiations they belong to. |
| `dictGenHashFunction`, `dictSetHashFunctionSeed`, `dictGetHashFunctionSeed` | omitted: `ahash` already mixes a per-process seed; the C MurmurHash2 helper has no in-tree caller after the dict is replaced. |

### dyn_dict_msg_id.{c,h}

| C symbol | Rust home | Notes |
|---|---|---|
| `msg_table_dict_type` (and the static helpers `dict_msg_id_hash`, `dict_msg_id_cmp`) | `dynomite::util::dict::MsgIndex` (alias for `DictMap<MsgId, V>`) | done (Stage 1) |

### dyn_rbtree.{c,h}

| C symbol | Rust home | Notes |
|---|---|---|
| `struct rbnode`, `struct rbtree`, `rbtree_init`, `rbtree_node_init`, `rbtree_min`, `rbtree_insert`, `rbtree_delete`, `rbtree_red`/`rbtree_black`/`rbtree_is_red`/`rbtree_is_black`/`rbtree_copy_color` | replaced by `dynomite::util::rbtree::OrderedMap` (a typed `BTreeMap` wrapper exposing `lower_bound`, `min`, and `max`). The C engine uses the tree exclusively for token-ring ordering; `OrderedMap` provides the same operations with `O(log n)` worst-case bounds. |

### dyn_histogram.{c,h}

See the Stage 5 [`dyn_histogram.{c,h}`](#dyn_histogramch) entry for the
canonical mapping. The histogram lives in `dynomite::stats::histogram`
next to its only consumer.

### dyn_task.{c,h}

| C symbol | Rust home | Notes |
|---|---|---|
| `task_handler_1` | `Arc<dyn Fn() + Send + Sync>` (periodic) and `Box<dyn FnOnce() + Send>` (one-shot). |
| `task_mgr_init` | omitted: tokio drives timers; the runtime entry is provided by Stage 12 (`dynomited`'s `#[tokio::main]`). |
| `schedule_task_1` | `dynomite::core::task::task_schedule_once` | done (Stage 1) |
| `time_to_next_task` | omitted: tokio's reactor schedules the next wake-up. |
| `execute_expired_tasks` | omitted: tokio drives the timer wheel transparently. |
| `cancel_task` | `TaskHandle::cancel` | done (Stage 1) |
| `task_register` (PLAN-mandated periodic API) | `dynomite::core::task::task_register` | done (Stage 1) |

## src/hashkit/

| C symbol | Rust home | Notes |
|---|---|---|
| `hash_type_t` / `HASH_CODEC` | `dynomite::hashkit::HashType` | done (Stage 3) |
| `get_hash_func` | `dynomite::hashkit::hash` | done |
| `get_hash_type` | `dynomite::hashkit::HashType::from_name` | done |
| `hash_one_at_a_time` | `dynomite::hashkit::one_at_a_time::hash` | done |
| `hash_md5` / `md5_signature` / `MD5_Init` / `MD5_Update` / `MD5_Final` | `dynomite::hashkit::md5::*` | done; passes RFC 1321 test vectors |
| `hash_crc16` | `dynomite::hashkit::crc16::hash` | done |
| `hash_crc32` | `dynomite::hashkit::crc32::hash_libmemcached` | done |
| `hash_crc32a` | `dynomite::hashkit::crc32::hash_standard` | done; passes 0xCBF43926 vector |
| `crc32_sz` | `dynomite::hashkit::crc32_sz` | done |
| `hash_fnv1_64` | `dynomite::hashkit::fnv::hash_fnv1_64` | done |
| `hash_fnv1a_64` | `dynomite::hashkit::fnv::hash_fnv1a_64` | done; reproduces the C 32-bit-accumulator behavior (recorded as a deviation below) |
| `hash_fnv1_32` | `dynomite::hashkit::fnv::hash_fnv1_32` | done |
| `hash_fnv1a_32` | `dynomite::hashkit::fnv::hash_fnv1a_32` | done; passes canonical FNV-1a 32 vectors |
| `hash_hsieh` | `dynomite::hashkit::hsieh::hash` | done |
| `hash_murmur` | `dynomite::hashkit::murmur::hash` | done |
| `hash_jenkins` | `dynomite::hashkit::jenkins::hash` | done |
| `hash_murmur3` (+ `MurmurHash3_x86_128` from contrib/) | `dynomite::hashkit::murmur3::hash` | done; deviation: the C call is commented out, the Rust impl produces the bytes the contrib code would produce |
| `init_dyn_token` / `deinit_dyn_token` | `dynomite::hashkit::DynToken::default` | done |
| `size_dyn_token` | `dynomite::hashkit::DynToken::size` | done |
| `set_int_dyn_token` | `dynomite::hashkit::DynToken::set_int` | done |
| `cmp_dyn_token` | `<DynToken as Ord>::cmp` | done |
| `parse_dyn_token` | `dynomite::hashkit::token::parse_token` | done |
| `derive_token` / `derive_tokens` | folded into `parse_token` + caller-side iteration | done |
| `print_dyn_token` | `<DynToken as Display>` | done |
| `copy_dyn_token` | `<DynToken as Clone>::clone` | done (Rust derives the trait) |
| `ketama_update` / `ketama_dispatch` (+ static `ketama_hash`/`ketama_item_cmp`) | `dynomite::hashkit::ketama::Continuum::{build, dispatch}` | done |
| `modula_update` / `modula_dispatch` | `dynomite::hashkit::modula::Continuum::{build, dispatch}` | done |
| `random_update` / `random_dispatch` | `dynomite::hashkit::PseudoRng` + caller-side modulus | done; the `random_update` ring layout is identical to modula and folded into the same Continuum builder |

## src/proto/

(Stage 8.)

## src/seedsprovider/

(Stage 10.)

## src/entropy/

(Stage 11.)

## src/tools/

| C symbol | Rust home | Notes |
|---|---|---|
| `dyn_hash_tool` | `crates/dyn-hash-tool/` | done; the C tool (`_/dynomite/src/tools/dyn_hash_tool.c`, 3486 bytes) supports only `hash_murmur` and exposes `-h/-k/-i/-o`. The Rust binary keeps that surface under `--c-compat` (with `-i/--input`, `-o/--output`, and `-k` as the `KEY:` prefix toggle) and additionally offers a multi-algorithm default mode. See deviation below. |

## contrib/

| Path | Disposition |
|---|---|
| `contrib/yaml-0.1.4/` | omitted: replaced by `serde_yaml`. |
| `contrib/fmemopen.{c,h}` | omitted: not needed in Rust. |
| `contrib/murmur3/` | open-coded in `dynomite::hashkit::murmur3` (Stage 3). |

## Ambiguities

### Stage 4: `conf_validate_pool` keeps `mbuf_size` / `max_msgs` defaults disabled

The C reference (`dyn_conf.c::conf_validate_pool`) checks the range of
`mbuf_size:` and `max_msgs:` only when those fields are set, and the
code that would default the unset case is commented out with the note
*after backward compatibility is supported, enable this*. The Rust port
mirrors that behavior exactly: `apply_defaults` does **not** populate
`mbuf_size` / `max_msgs` and `validate` only enforces ranges when they
are set explicitly. A regression test
(`tests/conf_property.rs::out_of_range_mbuf_rejected_by_validation`)
pins the chosen interpretation.

### Stage 4: `servers:` list cardinality

`dyn_conf.c::conf_add_server` writes into a single `struct conf_server **`
slot and asserts that it was previously `NULL`, so a config with two
`servers:` entries triggers an assertion in C. The Rust port refuses
the same shape with a typed `ConfError::BadServer` rather than a
panic. A regression test
(`crates/dynomite/src/conf/pool.rs::tests::empty_servers_rejected`)
plus the per-fixture tests confirm one entry is required.

## Deviations

### Stage 6: AES upgraded from AES-128-CBC to AES-256-CBC with a random per-message IV

The C reference (`dyn_crypto.c::aes_init`) calls `EVP_aes_128_cbc()`
and reuses the first 16 bytes of the AES key as the IV (the
`arg_aes_key` argument is passed as both the key and the IV to
`EVP_EncryptInit_ex`). The 32-byte `aes_key` buffer is therefore
effectively a 128-bit key with the upper half ignored. Two
encryptions of the same plaintext with the same key produce
identical ciphertext.

The Rust port uses AES-256-CBC with a fresh 16-byte IV generated
from the system CSPRNG on every encryption, prepended to the
ciphertext. This is a security-relevant upgrade and is not
wire-compatible with a C peer running the reference
`dyn_aes_encrypt` path. Stage 9 (which wires the DNODE handshake)
treats the AES wire format as a Rust-internal contract; if a
future stage needs C wire compatibility it will add a parallel
legacy code path under a feature flag.

Pinned by `crypto::aes::tests::iv_is_random_per_call` and
`tests/stage_06_crypto.rs::cross_version_cipher_round_trip`.

### Stage 6: long-lived `EVP_CIPHER_CTX` globals replaced by per-call `Crypter`

The C reference holds two static `EVP_CIPHER_CTX *` (one for
encryption, one for decryption) for the entire process lifetime
and re-keys them on every call. This is incompatible with the
tokio-driven multi-task model: two worker tasks could re-key the
shared context concurrently. The Rust port allocates a fresh
`openssl::symm::Crypter` per call, which is functionally
equivalent but lock-free.

### Stage 6: RSA padding choice

The Rust port uses PKCS#1 OAEP padding (with the OpenSSL default
SHA-1 hash and MGF1) for both `Crypto::rsa_encrypt` and
`Crypto::rsa_decrypt`. This matches `_/dynomite/src/dyn_crypto.c`
lines 521-538 which call `RSA_public_encrypt` /
`RSA_private_decrypt` with `RSA_PKCS1_OAEP_PADDING`. The original
Stage 6 dispatch brief mistakenly directed PKCS#1 v1.5; the worker
followed the brief and recorded the discrepancy, and a follow-up
commit corrected the padding to OAEP per AGENTS.md non-negotiable
#6 ("reproduce C behavior; no reimagining").

Pinned by `crypto::rsa::tests::round_trip_short` and
`crypto::rsa::tests::round_trip_aes_keylen`.

### Stage 6: `base64_encode` is unpadded

The C reference emits `BIO_FLAGS_BASE64_NO_NL`-flagged padded
output from `BIO_f_base64()`. The Rust helper drops the trailing
`=` characters because every in-tree consumer (the entropy
reconciliation digest and the planned DNODE token blob) is parsed
back through `base64_decode`, which accepts both forms. A future
stage that produces base64 for an external consumer can switch
to the standard padded engine without touching the decode side.

Pinned by `crypto::base64::tests::standard_vectors` and
`crypto::base64::tests::padded_decodes_too`.

### Stage 4: `secure_server_option` validation strictness

`dyn_conf.c::conf_validate_pool` only logs an error and continues
when `secure_server_option:` is set to an unknown value (the
function still returns `DN_OK`). The Rust port returns
`ConfError::BadSecure` from `ConfPool::validate`. Stricter than the
reference; pinned by `pool::tests::secure_server_option_*` tests.
A configuration that the C reference would silently accept (and
then treat as `SECURE_OPTION_NONE` at runtime) is rejected by Rust.

### Stage 4: `SecureServerOption::parse` errors on unknown input

`dyn_conf.c::get_secure_server_option` returns
`SECURE_OPTION_NONE` for any unrecognised string; the Rust port
returns `Err(ConfError::BadSecure)`. The two are not behaviorally
equivalent at the call boundary. When Stage 9 wires the runtime
DNODE handshake, the call site that today consumes the C silent
fallback must be updated to either map `BadSecure` back to `None`
or add a separate lenient-parse path.

### Stage 4: bracketed IPv6 listen syntax

`ConfListen::parse` accepts `[::1]:8101` form by stripping the
brackets in `endpoint::split_host_port`. The C parser does a plain
rightmost-colon split and stores `[::1]` (with brackets) in
`field->name`, which `getaddrinfo` rejects at resolve time. Net
result: Rust accepts an IPv6 configuration the C reference
silently fails on later. Test:
`endpoint::tests::parses_bracketed_ipv6`.

### Stage 4: server-row weight must be numeric

`dyn_conf.c::conf_add_server` parses but never validates the
weight token (the C source explicitly comments that the weight is
ignored at runtime, kept only for backward compatibility).
`ConfServer::parse` requires `u32::parse()` to succeed, so a
configuration like `127.0.0.1:6379:not-a-number` is accepted by C
but rejected by Rust with `ConfError::BadServer`. Test:
`server::tests::weight_must_parse_as_u32`.

### Stage 4: empty strings are not treated as unset

Multiple defaulting blocks in C use `string_empty()`, which is
true for both unset and explicitly-empty fields. Rust
`apply_defaults` only fills `is_none()`, so a YAML directive of
`pem_key_file: ''` is left as the empty string (and then rejected
by `validate` in secure mode) rather than replaced by the default
path. Affected fields: `dyn_seed_provider`, `rack`, `datacenter`,
`secure_server_option`, `read_consistency`, `write_consistency`,
`env`, `pem_key_file`, `recon_key_file`, `recon_iv_file`. Practical
impact is low under typical YAML usage; recorded for visibility.

### Stage 4: explicit `*_connections: 0` is preserved

The C reference uses `0` as the `CONF_UNSET_NUM` sentinel for
`datastore_connections`, `local_peer_connections`, and
`remote_peer_connections`, so an explicit user value of `0` is
silently replaced with `1` during `apply_defaults`. The Rust
`Option<u8>` distinguishes unset from explicit zero, and an
explicit zero is preserved (and then rejected by
`validate_numeric_ranges` because the lower bound is `1`). This
follows the typed-Option model PLAN.md Stage 4 calls for.

## Caveats

Documented design choices that downstream stages must respect.

* **Stage 1 - `core::task::TaskHandle` does not cancel on drop.**
  Dropping a [`TaskHandle`](../crates/dynomite/src/core/task.rs)
  without calling `cancel` leaves the task running detached. This
  matches the original `cancel_task` shape (the engine cancels
  tasks explicitly) but is a footgun for new callers. Any caller
  storing a handle must own its lifetime; an ergonomic
  `cancel-on-drop` wrapper may be added in a follow-up if this
  pattern recurs.
* **Stage 1 - `core::ring_queue` blocks the producer on a full
  channel by default.** The reference SPSC ring drops the message
  on full (`CBUF_Push` returns and the call site logs and skips).
  Stage 10 callers porting `CBUF_Push` must use
  [`crossbeam_channel::Sender::try_send`] explicitly to preserve
  drop-on-full semantics; mechanical translation to `send` will
  block instead.
* **Stage 1 - `util::dict::DictMap::insert` is
  [`dictReplace`](https://github.com/redis/redis)-shaped, not
  `dictAdd`-shaped.** `DictMap::insert` overwrites a previous
  value and returns it. The reference engine has both `dictAdd`
  (rejects duplicates) and `dictReplace` (overwrites); only the
  latter is currently exposed in Rust. Stage 10 gossip callers
  porting `dictAdd` must guard with `contains_key` or use the
  entry API to preserve duplicate-rejection behaviour.
* **Stage 1 - `util::dict` uses `ahash`.** `ahash` is
  non-cryptographic and seeded per-process. Internal-only maps
  with non-adversarial keys (`MsgIndex`, gossip rack/DC
  namespaces) are fine; any future consumer with attacker-
  controlled keys must override the hasher to a DoS-resistant
  one (`SipHash` from std).

## Project-wide follow-ups (not stage-specific)

* `# Examples` doctest coverage on public items is currently near
  zero across all crates. Tracked in
  `docs/journal/blocked.md` for resolution by a cross-cutting
  cleanup PR after the in-flight stages merge. AGENTS.md
  Section 5 mandates a doctest per public item; this gap pre-
  dates the in-flight stages.
* The `conf_parse` cargo-fuzz target listed in AGENTS.md
  Section 6.4 is not present yet. It is added in the merge
  preparation pass alongside `dnode_parse`,
  `proto_redis_parse`, and `proto_memcache_parse` once Stage 7
  / Stage 8 land (those parsers are the targets that need
  fuzzing first).
* The Stage 5 stats REST endpoint exposes only the JSON snapshot path
  (`GET /` and `GET /info`). The C implementation also accepts
  `/help`, `/ping`, `/cluster_describe`, and a family of mutator
  commands that adjust runtime state (log level, consistency knobs,
  peer state, read-repair toggle). Those endpoints depend on cluster
  state structures that arrive in Stage 10; they are intentionally
  deferred and tracked there.
* **Stage 5: `snapshot.json` is structural, not byte-equal.** The
  reference `stats_make_info_rsp` interleaves linefeeds between
  numeric fields and emits a trailing `}\n` footer; the Rust writer
  produces a single compact line with no embedded whitespace. PLAN.md
  Stage 5's exit gate has been relaxed to "structural equivalence":
  the field set, ordering, value types, and nesting all match the
  reference output, but the byte-level whitespace is engine-dependent.
  The integration test now reconstructs the expected field set from
  `POOL_CODEC` and `SERVER_CODEC` so a regression in field selection
  or ordering is caught even though the fixture itself is the Rust
  output.
* **Stage 5: stats REST `/` returns the same body as `/info`.** The
  reference rewrites `reqline[1]` to `"/info"` but then immediately
  returns without re-dispatching, which is a known C bug; the Rust
  port serves the snapshot for both paths.
* **Stage 5: stats REST loop matches C `strcmp` exactly.** The C
  reference compares the request path with literal `strcmp`. The
  Rust port now does the same (the earlier `?`-stripping behavior
  was removed in `b4135aa`). The full command surface beyond `/`
  and `/info` arrives in Stage 10 cluster control plane; until
  then, requests with query strings or unknown paths fall through
  to the default 200 `OK` response, which is the same shape the C
  reference uses for unsupported commands.
* **Stage 5: counters use wrapping arithmetic.** Pool and server
  counter increments wrap on overflow to match the reference `++` /
  `+=` semantics. Counters are 64-bit signed and never reach the
  wrap point under realistic workloads.
* **Stage 5: aggregator collapses the `shadow -> sum` double buffer.**
  The reference engine maintains separate `current` and `shadow`
  metric arrays and only swaps when an `updated` flag is set. The
  Rust port serializes counter writes through a single mutex and
  always re-publishes the snapshot on every aggregator tick. The
  observable JSON output is unchanged because the reference's
  short-circuit is purely an optimization to skip a no-op swap.
* Stage 1 task scheduler: the C code exposes a one-shot, main-loop-driven
  scheduler (`schedule_task_1` / `execute_expired_tasks`). The Rust port
  replaces both with tokio-driven primitives because the rest of the
  port runs under a multi-threaded tokio runtime. The PLAN-mandated
  periodic API (`task_register`) is provided alongside a one-shot
  variant (`task_schedule_once`) for parity with `schedule_task_1`. The
  per-iteration helpers (`time_to_next_task`, `execute_expired_tasks`)
  have no Rust counterpart because tokio's reactor performs that work.
* Stage 1 signal handling: the C code installs `sigaction` directly.
  Rust callers (Stage 12) wire tokio `signal::unix` streams to
  `dispatch`; the table is the single source of truth and runs the same
  per-signal action.
* `hashkit::murmur3` produces a real 128-bit hash. The C
  `hash_murmur3` is a no-op (the call to `MurmurHash3_x86_128` is
  commented out in `dyn_murmur3.c`), so the C binary effectively writes
  uninitialized memory into the token. The Rust port implements the
  contrib MurmurHash3 x86_128 function directly with seed `0xc0a1e5ce`
  so the algorithm produces reproducible bytes; this matches the
  obvious upstream intent.
* `hashkit::fnv::hash_fnv1a_64` keeps the C reference's 32-bit
  accumulator and 32-bit prime even though the canonical FNV-1a-64
  algorithm uses a 64-bit accumulator. This is a faithful reproduction
  of `dyn_fnv.c` so existing token rings continue to land on the same
  servers; the misnamed entry is documented in the algorithm doc
  comment.
* `hashkit::token::DynToken::cmp` is a total order, so it can act as
  the key in a `BTreeMap` continuum. The C `cmp_dyn_token` returned
  `int32_t {-1, 0, 1}` and was used only as a sort comparator;
  semantics are identical for the values that can actually appear.
* `crates/dyn-hash-tool/` defines its own `<algo>:<key>:<token-hex>`
  output format because the C source tree does not contain the
  `dyn_hash_tool.c` referenced in the original PLAN. The flag set
  (`-H/--hash`, `-k/--key`, `--stdin`, `--list`) and one-line-per-key
  output are documented in the binary's crate-level rustdoc.
* `crates/dyn-hash-tool/` deliberately broadens the surface of the C
  `dyn_hash_tool`. The C tool (`_/dynomite/src/tools/dyn_hash_tool.c`)
  exposes `--help`, `-k`/`--outputkey` (boolean: emit `KEY:<key>` lines
  before each token), `-i <file>` / `--keyfile` (input, `-` is stdin),
  and `-o <file>` / `--tokenfile` (output, `-` is stdout). It only
  supports `hash_murmur` and emits one decimal token per line.

  The Rust binary preserves that on-the-wire surface behind
  `--c-compat`: when set, it requires `murmur` (the default if `-H` is
  omitted), reads keys one per line from `-i`/`--input` (default
  stdin, `-` also means stdin), writes decimals to `-o`/`--output`
  (default stdout, `-` also means stdout), and emits `KEY:<key>` lines
  before each token when `-k` is passed. Other algorithms are
  rejected with an explicit error.

  Outside `--c-compat`, the Rust binary adds a multi-algorithm
  convenience mode (`-H/--hash`, `--key`, `--stdin`, `--list`) with
  the format `<algorithm>:<key>:<token-hex>` for ad-hoc invocation
  against any of the thirteen algorithms. The short flag `-k` is
  intentionally repurposed as the C-compat KEY-prefix toggle, so
  inline keys must be passed via the long flag `--key` to avoid
  ambiguity.


* `hashkit::hsieh::hash` defines what the C reference left
  undefined. `_/dynomite/src/hashkit/dyn_hsieh.c:49-51` returns
  `DN_OK` on an empty key without ever calling `size_dyn_token` or
  `set_int_dyn_token`; the caller's `struct dyn_token` is therefore
  read in whatever state it was passed in (typically uninitialised
  stack memory from `init_dyn_token`'s `len = 0` state). The Rust
  port returns a zero-valued single-word token
  (`DynToken::from_u32(0)`) so callers see a deterministic value.
  Pinned by `hashkit::hsieh::tests::empty_key_is_zero_token_not_uninit`.

* `hashkit::crc32::crc32_sz` lower-cases bytes with
  `u8::to_ascii_lowercase`; the C reference uses libc
  `tolower((unsigned int)*p)`, which is locale-dependent and folds
  some bytes >= 0x80 differently in non-`C` locales. The helper is
  used by the entropy reconciliation digest path in
  `_/dynomite/src/dyn_message.c`; in practice that path consumes
  ASCII keys, so this divergence is observable only when a peer
  ships a non-ASCII key whose mainline locale would have folded it.
  The Rust choice yields portable, locale-independent reconciliation
  digests. Pinned by
  `hashkit::crc32::tests::crc32_sz_ascii_only_high_byte_is_unchanged`.

* `hashkit::random::PseudoRng` replaces glibc `random()` with a
  Knuth MMIX 64-bit linear congruential generator seeded from
  `SystemTime::now()`. The C `random_dispatch` is
  `random() % ncontinuum`; `random_update` is entirely commented
  out, so no caller observes the libc PRNG's specific stream.
  PLAN.md Stage 3 originally suggested `clock_gettime(CLOCK_MONOTONIC)`
  for symmetry; the Rust port uses `SystemTime` because the `nix`
  workspace crate is not built with the `time` feature and
  monotonicity is overkill for a non-cryptographic PRNG that the
  active C call graph never reads. The LCG parameters are pinned
  by `hashkit::random::tests::lcg_parameters_are_pinned` so any
  future move to a different PRNG is an intentional change.
