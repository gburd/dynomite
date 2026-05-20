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
| `aes_cipher` (static `EVP_CIPHER *`) | replaced by the `cbc::Encryptor<Aes128>` / `cbc::Decryptor<Aes128>` types constructed per call in `dynomite::crypto::aes` | done (Stage 6) |
| `rsa` (static `RSA *`) | `dynomite::crypto::Crypto::rsa` field | done (Stage 6) |
| `aes_key` (static buffer) | `dynomite::crypto::Crypto::aes_key` field | done (Stage 6) |
| `aes_encrypt_ctx` / `aes_decrypt_ctx` | replaced by per-call `cbc::Encryptor<Aes128>` / `cbc::Decryptor<Aes128>` instances | done (Stage 6) (deviation: per-call contexts replace the long-lived globals; see Deviations) |
| `load_private_rsa_key_by_file` | `dynomite::crypto::pem::load_rsa_private_key` | done (Stage 6) |
| `load_private_rsa_key` | folded into `Crypto::from_pem` | done (Stage 6) |
| `aes_init` | folded into `Crypto::from_pem` (the AES key generation half) and `Crypto::generate_aes_key` (the standalone half) | done (Stage 6) |
| `crypto_init` | `dynomite::crypto::Crypto::from_pem` | done (Stage 6) |
| `crypto_deinit` | implicit (`Drop` of `Crypto`) | done (Stage 6) |
| `base64_encode` | `dynomite::crypto::base64_encode` | done (Stage 6); standard alphabet with trailing `=` padding (matches the C `BIO_f_base64()` + `BIO_FLAGS_BASE64_NO_NL` output, which suppresses newlines but keeps padding) |
| `base64_decode` (commented out in C) | `dynomite::crypto::base64_decode` | done (Stage 6); the C source ships the helper commented out, the Rust port exposes the symmetric decode that the DNODE handshake will need |
| `aes_encrypt` | `dynomite::crypto::Crypto::aes_encrypt` (delegates to `crypto::aes::encrypt_to_vec`) | done (Stage 6); AES-128-CBC with the first 16 bytes of the key reused as the IV, no IV prefix in the output |
| `aes_decrypt` | `dynomite::crypto::Crypto::aes_decrypt` (delegates to `crypto::aes::decrypt_to_vec`) | done (Stage 6) |
| `dyn_aes_encrypt` | `dynomite::crypto::Crypto::dyn_aes_encrypt` (delegates to `crypto::aes::encrypt_to_chain`) | done (Stage 6); writes a fresh chain rather than mutating a single caller-supplied mbuf because the chunk-then-pool model is a closer fit for the tokio reactor than the C in-place `mbuf->end_extra` reservation |
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

### dyn_redis.c

| C symbol | Rust home | Notes |
|---|---|---|
| `redis_argz` | folded into `dynomite::proto::redis::commands::classify` (`CommandClass::Argz`) | done (Stage 8) |
| `redis_arg0` | folded into `commands::classify` (`CommandClass::Arg0`) | done (Stage 8) |
| `redis_arg1` | folded into `commands::classify` (`CommandClass::Arg1`) | done (Stage 8) |
| `redis_arg2` | folded into `commands::classify` (`CommandClass::Arg2`) | done (Stage 8) |
| `redis_arg3` | folded into `commands::classify` (`CommandClass::Arg3`) | done (Stage 8) |
| `redis_argn` | folded into `commands::classify` (`CommandClass::ArgN`) | done (Stage 8) |
| `redis_argx` | folded into `commands::classify` (`CommandClass::ArgX`) | done (Stage 8) |
| `redis_argkvx` | folded into `commands::classify` (`CommandClass::ArgKvX`) | done (Stage 8) |
| `redis_arg_upto1` | folded into `commands::classify` (`CommandClass::ArgUpto1`) | done (Stage 8) |
| `redis_argeval` | folded into `commands::classify` (`CommandClass::ArgEval`) | done (Stage 8) |
| `redis_error` | `dynomite::proto::redis::commands::is_redis_error` | done (Stage 8) |
| `record_arg` | `dynomite::proto::redis::parser::read_bulk_arg` (private; pushes onto `Msg::args`) | done (Stage 8) |
| `redis_rewrite_query` | `dynomite::proto::redis::redis_rewrite_query` | done (Stage 8) (SMEMBERS / DC_SAFE_QUORUM rewrite verbatim) |
| `redis_parse_req` | `dynomite::proto::redis::redis_parse_req` (and `redis_parse_req_with_args`) | done (Stage 8); the cross-mbuf argument recording path is collapsed because the Rust parser flattens the chain before driving the state machine (recorded as ambiguity) |
| `redis_parse_rsp` | `dynomite::proto::redis::redis_parse_rsp` | done (Stage 8) |
| `redis_copy_bulk` | deferred to Stage 9 (mbuf-level chain manipulation in the conn FSM) | omitted-for-stage |
| `redis_pre_coalesce` | `dynomite::proto::redis::redis_pre_coalesce` (response classification) and `dynomite::proto::redis::accumulate_fragment_integer` (parent-running-total accumulation) | done (Stage 8); the C `req->frag_owner->integer += rsp->integer` accumulation is exposed as a separate helper because the Rust `Msg` type does not yet carry the `peer`/`frag_owner` pointer chain (those land with the Stage 9 connection FSM). The dispatcher invokes `accumulate_fragment_integer(parent, rsp)` once both messages are in scope. |
| `redis_post_coalesce_mset` / `redis_post_coalesce_num` / `redis_post_coalesce_mget` / `redis_post_coalesce` | `dynomite::proto::redis::redis_post_coalesce` | done (Stage 8) for data-shape; per-type chain merge deferred to Stage 9 |
| `redis_append_key` | folded into `dynomite::proto::redis::fragment::encode_fragment` | done (Stage 8) |
| `redis_fragment_argx` | folded into `dynomite::proto::redis::redis_fragment` | done (Stage 8) |
| `redis_fragment` | `dynomite::proto::redis::redis_fragment` | done (Stage 8); sharding delegated to a `FragmentDispatcher` trait so the cluster-layer coupling stays out of the proto module |
| `redis_verify_request` | `dynomite::proto::redis::redis_verify_request` | done (Stage 8); takes a `FragmentDispatcher` for parity |
| `redis_is_multikey_request` | `dynomite::proto::redis::redis_is_multikey_request` | done (Stage 8) |
| `consume_numargs_from_response` / `consume_numargs_from_responses` / `redis_append_nargs` / `get_next_response_fragment` / `redis_get_fragment_quorum` / `free_rsp_each` / `redis_reconcile_multikey_responses` | folded into `dynomite::proto::redis::repair::reconcile::redis_reconcile_responses` data-shape arm; the per-fragment chain walker is deferred to Stage 9 because it requires the conn-coupled response-mgr clone path | done (Stage 8) for data-shape; per-fragment quorum walker deferred to Stage 9 |
| `redis_reconcile_responses` | `dynomite::proto::redis::redis_reconcile_responses` | done (Stage 8) |

### dyn_redis_repair.c

| C symbol | Rust home | Notes |
|---|---|---|
| `proto_cmd_info[]` | folded into `dynomite::proto::redis::repair::make::is_repairable` plus `dynomite::proto::redis::repair::clear::redis_clear_repair_md_for_key` predicates | done (Stage 8) |
| `SET_SCRIPT` / `GET_SCRIPT` / `DEL_SCRIPT` / `HSET_SCRIPT` / `HDEL_SCRIPT` / `HGET_SCRIPT` / `ZADD_SCRIPT` / `SADD_SCRIPT` / `CLEANUP_DEL_SCRIPT` / `CLEANUP_HDEL_SCRIPT` | `dynomite::proto::redis::repair::scripts::{SET_SCRIPT, GET_SCRIPT, DEL_SCRIPT, HSET_SCRIPT, HDEL_SCRIPT, HGET_SCRIPT, ZADD_SCRIPT, SADD_SCRIPT, CLEANUP_DEL_SCRIPT, CLEANUP_HDEL_SCRIPT}` | done (Stage 8) byte-for-byte; ten unit tests pin each script's declared `$<n>` length prefix against its actual body length. |
| `CLEANUP_DEL_SCRIPT` / `CLEANUP_HDEL_SCRIPT` | `dynomite::proto::redis::repair::scripts::{CLEANUP_DEL_SCRIPT, CLEANUP_HDEL_SCRIPT}` | done (Stage 8) |
| `ADD_SET_STR` / `REM_SET_STR` | `dynomite::proto::redis::repair::scripts::{ADD_SET_STR, REM_SET_STR}` and `dynomite::proto::redis::verify::{ADD_SET_STR, REM_SET_STR}` | done (Stage 8) |
| `total_tokens_of_type` / `parse_tokens_of_type` / `get_values_from_source` | folded into the post-parse step that lands with Stage 9; the data-shape side (eligibility predicates, command catalogue) is in place. | omitted-for-stage |
| `post_parse_optional_args` / `post_parse_msg` | deferred to Stage 9 (per-command argument layout walker) | omitted-for-stage |
| `obtain_info_from_latest_rsp` | deferred to Stage 9 | omitted-for-stage |
| `update_total_num_tokens` | deferred to Stage 9 (folded into the script-builder step that runs once the post-parse arrays are walkable) | omitted-for-stage |
| `create_redis_prtcl_script` / `finalize_repair_msg` | deferred to Stage 9 | omitted-for-stage |
| `find_most_updated_rsp` / `adjust_rsp_buffers_for_client` | deferred to Stage 9 (per-response argument walker) | omitted-for-stage |
| `redis_make_repair_query` | `dynomite::proto::redis::redis_make_repair_query` | done (Stage 8); data-shape side checks the eligibility predicate (`is_repairable` plus `is_read_repairs_enabled`) and returns `RepairOutcome::NoOp` when the script-builder is not reachable. The script-emit arm depends on Stage 9. |
| `redis_rewrite_query_with_timestamp_md` | `dynomite::proto::redis::redis_rewrite_query_with_timestamp_md` | done (Stage 8) for the eligibility predicate; the Lua-script generation lands once Stage 9 walks the post-parsed argument list. |
| `create_cleanup_script` | folded into `dynomite::proto::redis::repair::clear::redis_clear_repair_md_for_key` (which delegates to the script-builder once Stage 9 lands) | done (Stage 8) for data-shape |
| `redis_clear_repair_md_for_key` | `dynomite::proto::redis::redis_clear_repair_md_for_key` | done (Stage 8) for data-shape |

### dyn_memcache.c

| C symbol | Rust home | Notes |
|---|---|---|
| `MEMCACHE_MAX_KEY_LENGTH` | `dynomite::proto::memcache::parser::MEMCACHE_MAX_KEY_LENGTH` | done (Stage 8) |
| `memcache_storage` / `memcache_cas` / `memcache_retrieval` / `memcache_arithmetic` / `memcache_delete` / `memcache_touch` | `dynomite::proto::memcache::commands::{memcache_storage, memcache_cas, memcache_retrieval, memcache_arithmetic, memcache_delete, memcache_touch}` | done (Stage 8) |
| `memcache_parse_req` | `dynomite::proto::memcache::memcache_parse_req` (and `memcache_parse_req_tagged` for the hash-tag variant) | done (Stage 8) |
| `memcache_parse_rsp` | `dynomite::proto::memcache::memcache_parse_rsp` | done (Stage 8) |
| `memcache_failure` | omitted: trivial `return false;`; the Rust port has no in-tree caller after the conn FSM lands. |
| `memcache_append_key` | folded into `dynomite::proto::memcache::fragment::memcache_fragment` | done (Stage 8) |
| `memcache_fragment_retrieval` / `memcache_fragment` | `dynomite::proto::memcache::memcache_fragment` | done (Stage 8); fragments carry fully-formed `get k1 k2 ...\r\n` wire frames in their mbuf chains. |
| `memcache_verify_request` | `dynomite::proto::memcache::memcache_verify_request` | done (Stage 8) |
| `memcache_pre_coalesce` | `dynomite::proto::memcache::memcache_pre_coalesce` | done (Stage 8) for data-shape; mbuf-level chain mutation deferred to Stage 9 |
| `memcache_copy_bulk` | deferred to Stage 9 (mbuf-level chain manipulation) | omitted-for-stage |
| `memcache_post_coalesce` | `dynomite::proto::memcache::memcache_post_coalesce` | done (Stage 8) for data-shape; per-fragment payload concatenation deferred to Stage 9 |
| `memcache_post_connect` / `memcache_swallow_msg` / `memcache_add_auth` / `memcache_reply` | omitted: the C reference defines these as no-ops or `NOT_REACHED()` stubs. The Rust port has no in-tree caller after the Stage 9 conn FSM lands; matching no-op shape is documented in the conn FSM rather than the protocol module. |
| `memcache_is_multikey_request` | `dynomite::proto::memcache::memcache_is_multikey_request` | done (Stage 8); always returns `false` (matches C source) |
| `memcache_reconcile_responses` | `dynomite::proto::memcache::memcache_reconcile_responses` | done (Stage 8); returns `ReconcileOutcome::PickFirst` under `DC_QUORUM` and `ReconcileOutcome::Error(...)` otherwise |
| `memcache_rewrite_query` / `memcache_rewrite_query_with_timestamp_md` / `memcache_make_repair_query` / `memcache_clear_repair_md_for_key` | `dynomite::proto::memcache::repair::{memcache_rewrite_query, memcache_rewrite_query_with_timestamp_md, memcache_make_repair_query, memcache_clear_repair_md_for_key}` | done (Stage 8); intentional no-ops matching the C source. Documented in module rustdoc. |

## src/seedsprovider/

| C symbol | Rust home | Notes |
|---|---|---|
| `florida_get_seeds` | `dynomite::seeds::florida::FloridaSeedsProvider::fetch` (and the blocking `SeedsProvider::get_seeds` impl which spins up a private current-thread runtime) | done (Stage 10) |
| `dns_get_seeds` | `dynomite::seeds::dns::DnsSeedsProvider::get_seeds` (resolver injected via the [`dynomite::seeds::dns::Resolver`] trait so unit tests stay deterministic) | done (Stage 10) |
| `seeds_check` (per-provider 30s rate limiter) | `dynomite::seeds::SeedsError::NoFreshSeeds` (rate limiting is the responsibility of the gossip task driving the trait; the trait surfaces `NoFreshSeeds` so the gossip task can short-circuit as the C engine does) | done (Stage 10) |
| `hash_seeds` (per-provider de-dup hash on the response body) | folded into the gossip task: the task calls `gossip::add_or_update`, which is idempotent for unchanged nodes (returns `GossipStep::Unchanged`). The dedup-by-hash check is therefore unnecessary in the Rust port. | done (Stage 10) |
| `evalOSVar` (florida env-var lookup) | folded into `FloridaSeedsProvider::new` + `FloridaSeedsProvider::with_request`; the env-var override is the responsibility of the Stage 12 binary which configures the provider before installing it. | done (Stage 10) |
| `create_tcp_socket` (florida) | replaced by `tokio::net::TcpStream::connect`. | done (Stage 10) |
| (new) | `dynomite::seeds::SeedsProvider` trait | the seam Stage 13 will surface as the embedding API for custom seeds backends. |
| (new) | `dynomite::seeds::simple::SimpleSeedsProvider` | static, in-memory provider that returns the YAML-loaded seeds list (the reference engine routes to no provider when `dyn_seed_provider:` is unset and uses the seed list directly; Rust models that branch as `SimpleSeedsProvider`). |

## src/entropy/

### dyn_entropy.h

| C symbol | Rust home | Notes |
|---|---|---|
| `ENTROPY_ADDR` macro (`127.0.0.1`) | `dynomite::entropy::EntropyConfig::listen_addr` (operator-set) | done (Stage 11) |
| `ENTROPY_PORT` macro (`8105`) | `dynomite::entropy::EntropyConfig::listen_addr` (operator-set, the default port appears in module rustdoc examples) | done (Stage 11) |
| `ENCRYPT_FLAG` / `DECRYPT_FLAG` macros | `dynomite::entropy::EntropyConfig::encrypt` (single bool) and `SnapshotHeader::encrypt_flag` on the wire | done (Stage 11) |
| `BUFFER_SIZE` macro | `dynomite::entropy::DEFAULT_BUFFER_SIZE` | done (Stage 11) |
| `CIPHER_SIZE` macro | `dynomite::entropy::DEFAULT_CIPHER_SIZE` | done (Stage 11) |
| `struct entropy` | `dynomite::entropy::EntropyConfig` (operator-visible knobs) plus `EntropySender` / `EntropyReceiver` runtime objects (no single struct holds the live state) | done (Stage 11) |
| `entropy_init` | `EntropyReceiver::bind` (key/iv loaded inside `bind`) -- the C function is marked DEPRECATED in the reference source | done (Stage 11) |
| `entropy_loop` | `EntropyReceiver::accept_loop` | done (Stage 11) |
| `entropy_conn_start` | folded into `EntropyReceiver::run` (binds listener and spawns the accept task) | done (Stage 11) |
| `entropy_conn_destroy` | implicit (`Drop` of `EntropyReceiver` and the spawned `JoinHandle`) | done (Stage 11) |
| `entropy_listen` | folded into `EntropyReceiver::bind` (uses `tokio::net::TcpListener::bind`) | done (Stage 11) |
| `entropy_encrypt` | `dynomite::entropy::send::encrypt_chunk` | done (Stage 11) -- uses PKCS#7 padding (deviation, see below) |
| `entropy_decrypt` | `dynomite::entropy::receive::decrypt_chunk` | done (Stage 11) |
| `entropy_key_iv_load` | `dynomite::entropy::util::load_material` (returns separate validated `EntropyKey` / `EntropyIv` rather than mutating a global) | done (Stage 11) |
| `entropy_snd_start` | `EntropySender::push` (spawned wrapper: `EntropySender::run`) | done (Stage 11) |
| `entropy_rcv_start` | merged into the receiver's per-connection worker plus the user-supplied `SnapshotSink`. See deviation note below. | done (Stage 11) |

### dyn_entropy_util.c

| C symbol | Rust home | Notes |
|---|---|---|
| `MAGIC_NUMBER` macro (`64640001`) | `dynomite::entropy::ENTROPY_MAGIC` | done (Stage 11) |
| `MAX_HEADER_SIZE` macro | `dynomite::entropy::MAX_HEADER_SIZE` | done (Stage 11) |
| `MAX_BUFFER_SIZE` macro | `dynomite::entropy::MAX_BUFFER_SIZE` | done (Stage 11) |
| `MAX_CIPHER_SIZE` macro | `dynomite::entropy::MAX_CIPHER_SIZE` | done (Stage 11) |
| `static unsigned char *theKey` | omitted: the Rust loader always uses the on-disk file. The reference reassigns `theKey` only behind a commented-out line, so the C cipher always runs against the literal `0123456789012345`; recorded under `# Deviations`. | done (Stage 11) |
| `static unsigned char *theIv` | omitted: same rationale as `theKey` | done (Stage 11) |
| `entropy_crypto_init` | omitted: the RustCrypto stack has no global init step | done (Stage 11) |
| `entropy_crypto_deinit` | omitted: same rationale as `entropy_crypto_init` | done (Stage 11) |
| `entropy_decrypt` (util.c body) | `dynomite::entropy::receive::decrypt_chunk` | done (Stage 11) |
| `entropy_encrypt` (util.c body) | `dynomite::entropy::send::encrypt_chunk` | done (Stage 11) |
| `entropy_conn_stop` (static) | implicit (`tokio::net::TcpListener::drop` / connection drop) | done (Stage 11) |
| `entropy_conn_destroy` (util.c body) | implicit (`Drop`) | done (Stage 11) |
| `entropy_listen` (util.c body) | folded into `EntropyReceiver::bind` | done (Stage 11) |
| `entropy_key_iv_load` (util.c body) | `dynomite::entropy::util::load_material` (`load_key_file` + `load_iv_file`) | done (Stage 11) |
| `entropy_init` (util.c body, DEPRECATED) | replaced by `EntropyReceiver::bind` | done (Stage 11) |
| `entropy_callback` (static) | inline body of `EntropyReceiver::handle_one` (per-connection negotiation: read magic, command, header_size, buffer_size, cipher_size, validate ranges, dispatch) | done (Stage 11) |
| `entropy_loop` (util.c body) | `EntropyReceiver::accept_loop` | done (Stage 11) |
| `entropy_conn_start` (util.c body) | folded into `EntropyReceiver::run` | done (Stage 11) |

### dyn_entropy_snd.c

| C symbol | Rust home | Notes |
|---|---|---|
| `LOG_CHUNK_LEVEL` macro | omitted: per-chunk progress logging is left to embedders via tracing spans | done (Stage 11) |
| `THROUGHPUT_THROTTLE` macro | omitted: tokio's cooperative scheduling already throttles long writes; explicit per-chunk usleep dropped (recorded under `# Deviations`) | done (Stage 11) |
| `AOF_TO_SEND` macro (`/mnt/data/nfredis/appendonly.aof`) | default value of `RedisLocalSnapshot::aof_path` (configurable) | done (Stage 11) |
| `entropy_redis_compact_aof` (static) | `RedisLocalSnapshot::bgrewriteaof` (issues RESP `BGREWRITEAOF` over TCP, retries with a configurable pause) | done (Stage 11) -- the reference shells out via `system("redis-cli ... bgrewriteaof")`; the Rust default speaks RESP directly. Recorded under `# Deviations`. |
| `header_send` (static) | private helper `write_snapshot_header` inside `dynomite::entropy::send` -- writes the eight-byte `total_len` + `encrypt_flag` prefix and zero-fills the remainder of the negotiated `header_size`. The reference leaves trailing bytes uninitialized; the Rust port zero-fills (recorded under `# Deviations`). | done (Stage 11) |
| `entropy_snd_stats` (static) | omitted: progress reporting moves to tracing | done (Stage 11) |
| `entropy_snd_start` | `EntropySender::push` (spawned wrapper: `EntropySender::run`) | done (Stage 11) |

### dyn_entropy_rcv.c

| C symbol | Rust home | Notes |
|---|---|---|
| `entropy_redis_connector` (static) | `dynomite::entropy::receive::RedisReplaySink` (default sink that opens a fresh TCP connection to the configured Redis address and streams the decrypted bytes at it) | done (Stage 11) |
| `entropy_rcv_start` | merged into the receiver's per-connection worker (`EntropyReceiver::accept_loop` calls a private `handle_one` that reads the negotiation, snapshot header, and per-chunk frames, decrypts, and hands the plaintext to `SnapshotSink::apply`). The reference engine's per-key handshake (`numberOfKeys` + per-key `keyValueLength` + raw bytes streamed at `redis-server`) is collapsed into a single blob handed to `SnapshotSink::apply`; embedders that need the per-record stream can layer it inside their sink. Recorded under `# Deviations`. | done (Stage 11) |

### Stage 11 new Rust-only items

| Item | Rust home | Notes |
|---|---|---|
| (new) | `dynomite::entropy::SnapshotSource` trait + `dynomite::entropy::SnapshotSink` trait | the seam Stage 13 will re-export through the embedding API for custom snapshot/replay backends; the trait shape is locked here so Stage 13 only adds a forwarding wrapper. |
| (new) | `dynomite::entropy::send::StaticSnapshot` | in-memory snapshot source for tests and embedders that already hold the snapshot in RAM. |
| (new) | `dynomite::entropy::receive::MemorySink` | in-memory snapshot sink for tests. |
| (new) | `dynomite::entropy::send::RedisLocalSnapshot` | default snapshot source: dials a local Redis on the conf-configured port, issues `BGREWRITEAOF`, then reads the AOF file from disk. |
| (new) | `dynomite::entropy::receive::RedisReplaySink` | default snapshot sink: writes the decrypted blob to a fresh Redis TCP connection. |
| (new) | `dynomite::entropy::NegotiationHeader` / `SnapshotHeader` | typed wire-frame helpers for the negotiation and per-snapshot headers. |
| (new) | `dynomite::entropy::EntropyError` | typed error union for the entropy module (I/O, config, key material, protocol, crypto, source, sink). |

## src/dyn_server.{c,h} (Stage 10)

| C symbol | Rust home | Notes |
|---|---|---|
| `struct datastore` (data shape) | `dynomite::cluster::pool::ServerPool::config()` + the per-pool `crate::net::ConnPool` for the backing datastore connection pool | done (Stage 10) for the data shape; per-conn FSM ships in Stage 9. |
| `struct server_pool` | `dynomite::cluster::pool::ServerPool` (peers, datacenters, auto-eject deciders, config) | done (Stage 10) |
| `struct datacenter` | `dynomite::cluster::datacenter::Datacenter` | done (Stage 10) |
| `struct rack` | `dynomite::cluster::datacenter::Rack` | done (Stage 10) |
| `struct continuum` | `dynomite::cluster::datacenter::Continuum` | done (Stage 10) |
| `server_pool_init` | `dynomite::cluster::pool::ServerPool::new` + `PoolConfig::from_conf` | done (Stage 10) |
| `server_pool_deinit` | implicit (`Drop` of owned types) | done (Stage 10) |
| `server_pool_preconnect` / `server_pool_disconnect` | folded into `crate::net::ConnPool` lifecycle; the cluster owns the pool refs and lets the connection layer manage preconnect / disconnect. | done (Stage 9 + Stage 10 wiring) |
| `server_get_dc` / `server_get_rack` / `server_get_rack_by_dc_rack` | `Datacenter::rack` / `Datacenter::upsert_rack` plus `ServerPool::rebuild_ring` for the upsert-on-walk side. | done (Stage 10) |
| `datacenter_destroy` / `rack_destroy` | implicit (`Drop`) | done (Stage 10) |
| `dc_init` / `dc_deinit` / `rack_init` / `rack_deinit` | `Datacenter::new` / `Rack::new` (RAII; no explicit init/deinit). | done (Stage 10) |
| `dc_string_dict_type` (dict typedef) | omitted: replaced by `Vec<Rack>` linear scan + `Datacenter::rack_idx` lookup. The reference engine carries the dict alongside the array but only ever reads from the array; the Rust port drops the redundant dict. | done (Stage 10) |
| `server_init` / `server_connect` / `server_failure` / `server_close` / `server_close_stats` / `server_ack_err` / `server_connected` / `server_ok` / `server_active` | folded into the Stage 9 `crate::net::server::ServerConn` driver + `ConnPool` machinery; the cluster layer never owns these per-conn paths. The pool-side eject decision the C engine bundles into `server_failure` is reimplemented as `crate::net::auto_eject::AutoEject` and called from both [`ConnPool`] and `ServerPool`'s per-peer auto-eject vector. | done (Stage 9 + Stage 10) |
| `datastore_check_autoeject` | `crate::net::auto_eject::AutoEject::record_attempt` | done (Stage 9; integrated by Stage 10 via `ServerPool::auto_eject()`) |
| `get_datastore_conn` | folded into Stage 9's `ConnPool::get`; cluster-side wiring is Stage 12 binary work. | done (Stage 9 + Stage 12 wiring) |
| `init_server_conn` / `server_ops` (server-side conn vtable) | replaced by per-role driver pattern (Stage 9 `ServerConn`); cluster has no role-vtable. | done (Stage 9) |
| `server_timeout` / `dnode_peer_timeout` (per-conn timeout helpers) | `PoolConfig::timeout_ms` + per-target adjustments will land with Stage 12 binary wiring; the data shape is preserved here. | omitted-for-stage (Stage 12) |
| `rsp_recv_next` / `rsp_recv_done` / `req_send_next` / `req_send_done` (server-side) | Stage 9 `ServerConn::run` | done (Stage 9) |
| `_print_datastore` / `_print_node` | replaced by `#[derive(Debug)]`. | done (Stage 10) |
| `req_server_enqueue_imsgq` / `req_server_dequeue_imsgq` / `req_server_enqueue_omsgq` / `req_server_dequeue_omsgq` | Stage 9 `Conn::enqueue_in` / `enqueue_out` | done (Stage 9) |

## src/dyn_dnode_peer.{c,h} (Stage 10)

| C symbol | Rust home | Notes |
|---|---|---|
| `struct node` (peer record) | `dynomite::cluster::peer::Peer` | done (Stage 10) |
| `enum dnode_state` (UNKNOWN..LEAVING) | `dynomite::cluster::peer::PeerState` | done (Stage 10) |
| `dnode_initialize_peers` | `ServerPool::new` (the ctor seeds the local peer at index 0 then appends every `ConfDynSeed`-derived `Peer`). | done (Stage 10) for data shape; the live `tokio` wiring is Stage 12. |
| `dnode_peer_add_local` | folded into `ServerPool::new` + `Peer::new(.., is_local=true, ..)`. | done (Stage 10) |
| `dnode_initialize_peer_each` | folded into the caller's loop in `ServerPool::new`; the `is_secure` flag is computed by the binary at startup using `is_secure(secure_server_option, dc, rack, peer.dc, peer.rack)` (Stage 12). | done (Stage 10) for data shape; secure-flag wiring lands with Stage 12. |
| `dnode_peer_deinit` | implicit (`Drop`). | done (Stage 10) |
| `dnode_peer_pool_run` / `vnode_update` (peer-side caller) | `ServerPool::rebuild_ring` | done (Stage 10) |
| `dnode_peer_pool_update` | the rebuild trigger lives in the gossip task; on every `GossipStep::Added` / `Replaced` outcome, the gossip task calls `ServerPool::rebuild_ring`. The `WAIT_BEFORE_UPDATE_PEERS_IN_MILLIS` (30s) throttle is honored by the gossip task itself. | done (Stage 10) for data shape; throttle wiring is Stage 12. |
| `dnode_peer_pool_server` | folded into `ClusterDispatcher::plan` (which combines the rack lookup, the `vnode_dispatch` call, and the routing-tag check). | done (Stage 10) |
| `dnode_peer_idx_for_key_on_rack` | `crate::cluster::vnode::dispatch` (and `crate::hashkit::hash` for the key->token half). | done (Stage 10) |
| `dnode_peer_for_key_on_rack` | folded into `ClusterDispatcher::plan`. | done (Stage 10) |
| `dnode_peer_get_conn` / `dnode_peer_conn` / `dnode_peer_each_preconnect` / `dnode_peer_each_disconnect` / `dnode_peer_pool_preconnect` / `dnode_peer_pool_disconnect` | folded into Stage 9 `ConnPool::get` / lifecycle; the cluster layer holds Arcs to the per-peer pools. | done (Stage 9 + Stage 12 wiring) |
| `dnode_peer_ref` / `dnode_peer_unref` / `dnode_peer_active` | replaced by Rust ownership (`Conn::new` / drop). | done (Stage 9) |
| `dnode_peer_close` / `dnode_peer_failure` / `dnode_peer_ack_err` / `dnode_peer_close_stats` | Stage 9 `dnode_server::DnodeServerConn::run` (close path) + the gossip-driven failure detector here. | done (Stage 9 + Stage 10) |
| `dnode_peer_connected` / `dnode_peer_ok` | Stage 9 `dnode_server::DnodeServerConn` plus `Peer::record_success`. | done (Stage 9 + Stage 10) |
| `dnode_create_connection_pool` | folded into Stage 12 binary wiring (it consumes `local_peer_connections` / `remote_peer_connections` from the YAML). The data shape (per-peer `ConnPool` ref) lives on `Peer`. | omitted-for-stage (Stage 12) |
| `dnode_peer_forward_state` | the gossip task drives this: it calls `dnode_peer_gossip_forward` on a randomly chosen peer. The encode path lives in `proto::dnode::dmsg_write`; the cluster module exposes `gossip::GossipState` so the encoder has everything it needs. | done (Stage 10) for data shape; live tokio task is Stage 12. |
| `dnode_peer_handshake_announcing` | gossip-driven: emitted when `dyn_state` is `JOINING`. The encoder uses `proto::dnode::dmsg_write` with `DmsgType::GossipSyn`. | done (Stage 10) for data shape; live wiring is Stage 12. |
| `dnode_peer_add` / `dnode_peer_replace` | the gossip task applies `GossipState::add_or_update`, then calls `ServerPool::rebuild_ring`. | done (Stage 10) |
| `dnode_rsp_filter` / `dnode_rsp_swallow` / `dnode_rsp_forward_match` / `dnode_rsp_forward` / `dnode_rsp_recv_done` | Stage 9 `dnode_server::DnodeServerConn::run`. | done (Stage 9) |
| `dnode_req_send_next` (peer-side throttle) | the per-conn `msgs_per_sec()` bucket is left as a Stage 12 wiring item; the rate cap is preserved on `crate::core::setting::msgs_per_sec`. | omitted-for-stage (Stage 12) |
| `dnode_req_peer_enqueue_imsgq` / `dnode_req_peer_dequeue_imsgq` / `_omsgq` variants | Stage 9 `Conn::enqueue_in` / `enqueue_out`. | done (Stage 9) |
| `dnode_peer_ops` (peer-side conn vtable) | replaced by per-role driver. | done (Stage 9) |
| `init_dnode_peer_conn` | folded into Stage 9 `dnode_server::DnodeServerConn::new`. | done (Stage 9) |
| `preselect_remote_rack_for_replication` | `ServerPool::preselect_remote_racks` | done (Stage 10) |
| `rack_name_cmp` | folded into `Datacenter::sort_racks` (`Vec::sort_by`). | done (Stage 10) |

## src/dyn_dnode_proxy.c (Stage 10 portion)

| C symbol | Rust home | Notes |
|---|---|---|
| `dnode_peer_req_forward` | data-shape side handled by `ClusterDispatcher::plan` (the `DispatchPlan::Replicas` arm names the targets); the encode + outbound enqueue is Stage 9 `dnode_server::DnodeServerConn`. | done (Stage 9 + Stage 10) for shape; runtime fan-out is Stage 12. |
| `dnode_peer_gossip_forward` | gossip task drives the encoder using `proto::dnode::dmsg_write` with `DmsgType::GossipSyn`. | done (Stage 10) for shape; tokio task is Stage 12. |
| `dnode_peer_req_forward_stats` | Stage 12 stats wiring. | omitted-for-stage (Stage 12) |

## src/dyn_dnode_request.c (Stage 10 portion)

| C symbol | Rust home | Notes |
|---|---|---|
| `dnode_req_forward_error` | folded into `ClusterDispatcher::dispatch`'s `DispatchPlan::NoTargets` arm, which produces an error response via `crate::msg::response::make_error`. | done (Stage 10) |
| `dnode_peer_req_forward` | data shape: see `ClusterDispatcher::plan`. | done (Stage 10) |

## src/dyn_gossip.{c,h} (Stage 10)

| C symbol | Rust home | Notes |
|---|---|---|
| `struct gossip_node` | `dynomite::cluster::gossip::GossipNode` | done (Stage 10) |
| `struct gossip_rack` / `struct gossip_dc` / `struct gossip_node_pool` (data shape) | folded into `dynomite::cluster::gossip::GossipState` (a `(dc, rack, token)`-keyed map plus a `(dc, rack, host)` map for IP-replacement detection). The C reference duplicates the (dc, rack, node) data into both `gossip_node_pool::datacenters` and `dict_dc`; the Rust port keeps a single map and reconstructs the array on demand. | done (Stage 10) |
| `parse_seeds` | `dynomite::cluster::gossip::parse_seed_node` | done (Stage 10) |
| (multi-entry seeds blob parser, inline in `gossip_update_seeds`) | `dynomite::cluster::gossip::parse_seed_blob` | done (Stage 10) |
| `gossip_dc_init` / `gossip_rack_init` | implicit (`HashMap::insert`) | done (Stage 10) |
| `gossip_add_node_to_rack` / `gossip_add_node` / `gossip_replace_node` / `gossip_update_state` / `gossip_add_node_if_absent` | unified into `GossipState::add_or_update` (returns a `GossipStep` enum that classifies the change so the caller can rebuild the ring on add / replace and skip on unchanged). | done (Stage 10) |
| `gossip_failure_detector` | `GossipState::run_failure_detector` | done (Stage 10) |
| `gossip_forward_state` / `gossip_announce_joining` | data shape preserved; the live tokio task that drives the per-round emission is Stage 12 binary wiring. | done (Stage 10) for shape; live task is Stage 12. |
| `gossip_loop` (the pthread main loop) | the periodic walker is Stage 12 (a tokio interval task built on `crate::core::task`). The state machine it drives lives here in `GossipState`. | omitted-for-stage (Stage 12) |
| `gossip_pool_init` / `gossip_destroy` | folded into `ServerPool::new` (initial state) and `Drop` (destroy). The `gossip_set_seeds_provider` dispatch lives in Stage 12 (the binary picks a `SeedsProvider` impl based on `dyn_seed_provider:`). | done (Stage 10) for shape; provider switch is Stage 12. |
| `gossip_msg_peer_update` | folded into `GossipState::add_or_update`. | done (Stage 10) |
| `gossip_set_seeds_provider` | Stage 12 binary wiring. | omitted-for-stage (Stage 12) |
| `dict_node_hash` / `dict_node_key_compare` / `dict_node_destructor` / `dict_string_*` (dict typedefs) | omitted: replaced by `HashMap` keyed on owned `(dc, rack, token-string)` tuples. | done (Stage 10) |
| `gossip_process_msgs` / `gossip_msg_to_core` / `gossip_ring_msg_to_core` / `string_write_uint32` / `num_len` / `write_char` / `write_number` / `token_to_string` | omitted: ring-message bookkeeping is replaced by Rust ownership; numeric serialisation goes through `format!`. | done (Stage 10) |
| `gossip_debug` | replaced by `tracing::debug!(?GossipState)`. | done (Stage 10) |

## src/dyn_node_snitch.{c,h} (Stage 10)

| C symbol | Rust home | Notes |
|---|---|---|
| `is_aws_env` | `dynomite::cluster::snitch::is_aws_env` | done (Stage 10) |
| `hostname_to_ip` / `hostname_to_private_ip4` | omitted: replaced by `tokio::net::lookup_host` at the call site (the C function is referenced only by `parse_seeds`, which the Rust port forwards through the seeds-provider trait). | done (Stage 10) |
| `get_broadcast_address` | `dynomite::cluster::snitch::broadcast_address` (closure-injected env lookup keeps the function pure) | done (Stage 10) |
| `get_public_hostname` | `dynomite::cluster::snitch::public_hostname` | done (Stage 10) |
| `get_public_ip4` | `dynomite::cluster::snitch::public_ip4` | done (Stage 10) |
| `get_private_ip4` | `dynomite::cluster::snitch::private_ip4` | done (Stage 10) |
| (new, brief-mandated) | `dynomite::cluster::snitch::rack_distance` / `pick_target_rack` / `RackDistance` | the brief asked for DC/rack proximity ordering helpers in `cluster::snitch`. The reference engine's snitch unit does not own them; the proximity decision lives in the peer-side `preselect_remote_rack_for_replication`. We add the helpers here to satisfy the brief and use them from `ClusterDispatcher::plan`; the deviation is recorded in the `# Deviations` section below. |

## src/dyn_vnode.{c,h} (Stage 10)

| C symbol | Rust home | Notes |
|---|---|---|
| `vnode_update` | `ServerPool::rebuild_ring` (which calls `crate::cluster::vnode::rebuild_continuums`) | done (Stage 10) |
| `vnode_dispatch` | `crate::cluster::vnode::dispatch` | done (Stage 10) |
| `vnode_item_cmp` / `vnode_rack_verify_continuum` | folded into `Rack::sort_continuums` (`Vec::sort_by` on `Continuum::token`). | done (Stage 10) |

## src/dyn_setting.{c,h} (Stage 10 wiring)

| C symbol | Rust home | Notes |
|---|---|---|
| `msgs_per_sec` / `set_msgs_per_sec` | `crate::core::setting::msgs_per_sec` / `set_msgs_per_sec` (Stage 1); cluster-side per-conn token bucket integration is Stage 12 binary wiring. | done (Stage 1); cluster integration is Stage 12. |

## Stage-10 new types

| New type | Rust home | Notes |
|---|---|---|
| `dynomite::cluster::peer::Peer` / `PeerState` / `PeerEndpoint` | per-peer record. |
| `dynomite::cluster::datacenter::Datacenter` / `Rack` / `Continuum` | topology. |
| `dynomite::cluster::pool::ServerPool` / `PoolConfig` | cluster owner. |
| `dynomite::cluster::vnode::dispatch` / `rebuild_continuums` / `PeerTokens` | token ring math. |
| `dynomite::cluster::snitch::rack_distance` / `pick_target_rack` / `RackDistance` | rack proximity. |
| `dynomite::cluster::gossip::GossipState` / `GossipNode` / `GossipStep` / `GossipConfig` / `SeedRecord` / `parse_seed_node` / `parse_seed_blob` | gossip state machine + seed parser. |
| `dynomite::cluster::dispatch::ClusterDispatcher` / `DispatchPlan` / `ReplicaTarget` | cluster-aware [`Dispatcher`](crate::net::Dispatcher) implementation. |
| `dynomite::seeds::SeedsProvider` / `SeedsError` | seeds provider trait + error type. |
| `dynomite::seeds::simple::SimpleSeedsProvider` | YAML-loaded list. |
| `dynomite::seeds::dns::DnsSeedsProvider` / `Resolver` / `ResolvedSeeds` | DNS-driven provider with injectable resolver. |
| `dynomite::seeds::florida::FloridaSeedsProvider` | HTTP/1.0 over `tokio::net::TcpStream` + `httparse`. |

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

### dyn_message.{c,h}

| C symbol | Rust home | Notes |
|---|---|---|
| `MSG_TYPE_CODEC` (X-macro) | `dynomite::msg::MsgType` (179 variants, indices identical) | done (Stage 7) |
| `enum msg_type` / `MSG_*` variants | `dynomite::msg::MsgType` variants (`Unknown`, `ReqMcGet`, ..., `EndIdx`) | done (Stage 7) |
| `enum msg_parse_result` | `dynomite::msg::MsgParseResult` | done (Stage 7) |
| `enum dyn_error` | `dynomite::msg::DynErrorCode` | done (Stage 7) |
| `dn_strerror` | `dynomite::msg::DynErrorCode::message` | done (Stage 7) |
| `dyn_error_source` | `dynomite::msg::DynErrorCode::source` | done (Stage 7) |
| `enum consistency` (`DC_ONE`, `DC_QUORUM`, `DC_SAFE_QUORUM`, `DC_EACH_SAFE_QUORUM`) | `dynomite::msg::ConsistencyLevel` | done (Stage 7) |
| `get_consistency_string` / `get_consistency_enum_from_string` | `ConsistencyLevel::name` / `from_name` | done (Stage 7) |
| `enum msg_routing` | `dynomite::msg::MsgRouting` | done (Stage 7) |
| `get_msg_routing_string` | `MsgRouting::name` | done (Stage 7) |
| `struct keypos` / `struct argpos` | `dynomite::msg::keypos::{KeyPos, ArgPos}` (re-exported from `dynomite::msg`) | done (Stage 8) |
| `struct write_with_ts` | folded into `dynomite::proto::redis::repair::scripts` constants plus the eligibility predicates in `dynomite::proto::redis::repair::{make, clear, rewrite}` | done (Stage 8) for the data-shape side; the post-parse argument walker that the C engine populates lands with Stage 9 |
| `struct msg` | `dynomite::msg::Msg` | done (Stage 7) (data shape; recv/send paths defer to Stage 9) |
| `MSG_TYPE_CODEC` strings (`msg_type_strings`) | `MsgType::name` | done (Stage 7) |
| `g_read_repairs_enabled` | `dynomite::msg::is_read_repairs_enabled` / `set_read_repairs_enabled` (backed by `OnceLock<bool>`) | done (Stage 7) |
| `g_pre_coalesce` / `g_post_coalesce` / `g_fragment` / `g_verify_request` / `g_is_multikey_request` / `g_reconcile_responses` / `g_rewrite_query` / `g_rewrite_query_with_timestamp_md` / `g_make_repair_query` / `g_clear_repair_md_for_key` | `dynomite::proto::{redis, memcache}::*` (per-protocol entry points) | done (Stage 8) |
| `set_datastore_ops` | folded into the per-protocol entry points; the Stage 9 conn FSM picks the protocol based on the configured `data_store` discriminator. | done (Stage 8) for the data-shape side; per-conn dispatch deferred to Stage 9 |
| `g_read_consistency` / `g_write_consistency` / `g_timeout_factor` | deferred to Stage 9 (consumed by the conn FSM) | omitted-for-stage |
| `msg_init` / `msg_deinit` | replaced by `Msg::new` and `Drop` (Rust ownership replaces the free queue) | done (Stage 7) |
| `msg_get` / `_msg_get` / `msg_put` / `msg_free` | replaced by `Msg::new` + `Drop`; the alloc-budget cap moves to `dynomited` startup wiring (Stage 12) | done (Stage 7) for the data-shape side; cap deferred to Stage 12 |
| `msg_get_error` | `dynomite::msg::response::make_error` | done (Stage 7) |
| `msg_get_rsp_integer` | folded into `Msg::set_integer` (the parser populates `integer` directly on the response message) | done (Stage 8) |
| `msg_clone` | deferred to Stage 9 (uses connection ownership) | omitted-for-stage |
| `msg_type_string` | `MsgType::name` | done (Stage 7) |
| `msg_empty` | `Msg::mlen() == 0` (the BAD_FORMAT side is folded into `dyn_error_code`) | done (Stage 7) |
| `msg_dump` | omitted: replaced by `tracing::debug!(?msg)` once tracing is wired across the engine. |
| `msg_length` / `msg_mbuf_size` | `Msg::recompute_mlen` and `MbufQueue::len` | done (Stage 7) |
| `msg_alloc_msgs` / `msg_free_queue_size` | omitted: the C alloc cap is replaced by ownership and a Stage 12 budget knob. |
| `msg_payload_crc32` | deferred to Stage 9 (uses `crc32_sz` over the payload region defined by the redis parser; the multikey reconciler that consumes it lives in Stage 9) | omitted-for-stage |
| `msg_gen_frag_id` | folded into `dynomite::proto::redis::fragment` and `dynomite::proto::memcache::fragment` (each owns a private monotonic counter) | done (Stage 8) |
| `msg_ensure_mbuf` / `msg_append` / `msg_append_format` / `msg_prepend` / `msg_prepend_format` | folded into `dynomite::proto::redis::fragment::write_buf_into_chain` and `dynomite::proto::redis::repair::rewrite::write_into_chain` (each writes the formatted payload into a fresh mbuf chain on the message). | done (Stage 8) for the parser/repair callsites; the conn-coupled error-reply emitters land with Stage 9 |
| `msg_get_full_key` / `msg_get_tagged_key` / `msg_get_full_key_copy` / `msg_get_arg_copy` / `msg_get_key` / `msg_get_arg` | folded into `Msg::keys()` and `Msg::args()` accessors (the parsed `KeyPos`/`ArgPos` carry the byte ranges directly). | done (Stage 8) |
| `msg_recv` / `msg_recv_chain` / `msg_send` / `msg_send_chain` / `msg_parse` / `msg_parsed` / `msg_repair` | deferred to Stage 9 (conn FSM + reactor wiring) | omitted-for-stage |
| `msg_tmo_min` / `msg_tmo_insert` / `msg_tmo_delete` / `msg_from_rbe` | deferred to Stage 9 (timeout queue is conn-coupled) | omitted-for-stage |
| `msg_incr_awaiting_rsps` / `msg_decr_awaiting_rsps` | `Msg::incr_awaiting_rsps` / `decr_awaiting_rsps` | done (Stage 7) |
| `msg_handle_response` | deferred to Stage 9 (rsp_handler vtable lives on `Conn`) | omitted-for-stage |
| `craft_ok_rsp` / `simulate_ok_rsp` / `msg_apply_config` | deferred to Stage 9 (conn-coupled OK-response synthesis); the data-shape side (`HACK_SETTING_CONN_CONSISTENCY` discriminator) is recognised by the parser. | omitted-for-stage |
| `is_msg_type_dyno_config` | `MsgType::HackSettingConnConsistency` discriminator (caller compares directly) | done (Stage 7) |
| `print_req` / `print_rsp` / `init_object` | omitted: replaced by `#[derive(Debug)]` on `Msg`. |
| `parse_int_arg_for_formatting` / `parse_llu_arg_for_formatting` / `parse_string_arg_for_formatting` | omitted: replaced by `format!` and `write!` at the call sites in Stage 8. |
| (new) | `dynomite::msg::MsgFlags` | Bag mirroring the `unsigned :1` flag fields on `struct msg`. |
| (new) | `dynomite::msg::MsgQueue` | Owning queue replacing every `msg_tqh` list. |
| (new) | `dynomite::msg::MsgIndex` | Owning index keyed on `MsgId`, replacing the `outstanding_msgs_dict` use of `dict_msg_id`. |
| (new) | `dynomite::msg::ConnId` | Stage 7 placeholder for the Stage 9 connection reference. |

### dyn_dnode_msg.{c,h}

| C symbol | Rust home | Notes |
|---|---|---|
| `dyn_parse_state_t` | `dynomite::proto::dnode::DynParseState` (16 variants, names and order identical) | done (Stage 7) |
| `dmsg_type_t` | `dynomite::proto::dnode::DmsgType` (14 variants, on-the-wire integer values identical) | done (Stage 7) |
| `dmsg_version_t` / `VERSION_10` | `dynomite::proto::dnode::VERSION_10` | done (Stage 7) |
| `MAGIC_STR` (`"   $2014$ "`) | encoder writes the same literal; parser tolerates leading whitespace and matches the `$2014$` core via `dynomite::proto::dnode::MAGIC` | done (Stage 7) |
| `CRLF_STR` | `dynomite::proto::dnode::CRLF` | done (Stage 7) |
| `struct dval` | omitted: the C struct is unused in the live call graph (no in-tree caller). |
| `struct dmsg` | `dynomite::proto::dnode::Dmsg` | done (Stage 7) |
| `dmsg_init` / `dmsg_deinit` / `dmsg_get` / `dmsg_put` / `dmsg_free` | replaced by `Dmsg::new` + Rust ownership | done (Stage 7) |
| `dmsg_empty` | `Dmsg::mlen == 0` (caller-side check) | done (Stage 7) |
| `dmsg_dump` | omitted: replaced by `#[derive(Debug)]` on `Dmsg`. |
| `dyn_parse_core` | `dynomite::proto::dnode::DnodeParser::step` | done (Stage 7) |
| `dyn_parse_req` | `dynomite::proto::dnode::parse_req` | done (Stage 7) (data-shape; the encrypted-payload decryption hook lands in Stage 9) |
| `dyn_parse_rsp` | `dynomite::proto::dnode::parse_rsp` | done (Stage 7) |
| `data_store_parse_req` / `data_store_parse_rsp` | deferred to Stage 8 (redis/memcache dispatch) | omitted-for-stage |
| `dmsg_write` | `dynomite::proto::dnode::dmsg_write` | done (Stage 7) |
| `dmsg_write_mbuf` | `dynomite::proto::dnode::dmsg_write_mbuf` | done (Stage 7) |
| `dmsg_process` | `dynomite::proto::dnode::dmsg_process` (returns `DmsgDispatch::{Bypass, Forward}`); gossip-message decoders ship with Stage 10 | done (Stage 7) |
| `dmsg_parse` / `dmsg_parse_host_id` | deferred to Stage 10 (gossip ring messages live there) | omitted-for-stage |
| `dmsg_to_gossip` | deferred to Stage 10 (uses `C2G_InQ`) | omitted-for-stage |
| (new) | `dynomite::proto::dnode::DnodeParser` | Public streaming state machine. |
| (new) | `dynomite::proto::dnode::ParseStep` | Step-result enum used by `DnodeParser::step`. |
| (new) | `dynomite::proto::dnode::DnodeError` | Typed encode/parse error. |
| (new) | `dynomite::proto::dnode::DmsgDispatch` | Result of `dmsg_process` classifying control vs data plane. |
| (new) | `dynomite::proto::dnode::DMSG_FLAG_ENCRYPTED` / `DMSG_FLAG_COMPRESSED` | Bit constants for `Dmsg::flags`. |
| (new) | `dynomite::proto::dnode::HANDSHAKE_PLACEHOLDER_DATA` / `GOSSIP_PLACEHOLDER_DATA` | Single-byte constants emitted by the two encoder flavours. |
| (new) | `dynomite::proto::dnode::flatten_chain` | Test helper draining an `MbufQueue` into a `Vec<u8>`. |

### dyn_response_mgr.{c,h}

| C symbol | Rust home | Notes |
|---|---|---|
| `MAX_REPLICAS_PER_DC` | `dynomite::msg::MAX_REPLICAS_PER_DC` | done (Stage 7) |
| `struct response_mgr` | `dynomite::msg::ResponseMgr` | done (Stage 7) |
| `init_response_mgr` | `ResponseMgr::new` | done (Stage 7) |
| `init_response_mgr_all_dcs` / `init_response_mgr_each_quorum_helper` | data-shape side via `Msg::set_rspmgr` + `Msg::additional_rspmgrs_mut`; the wiring that walks the cluster's datacenter list lives in Stage 10 | done (Stage 7) for data shape; per-DC fan-out deferred to Stage 10 |
| `rspmgr_check_is_done` / `rspmgr_is_quorum_achieved` | `ResponseMgr::is_done` / `outcome` (folded into a single `QuorumOutcome` enum) | done (Stage 7) |
| `rspmgr_get_response` | `ResponseMgr::pick_response` (data-shape) plus `error_response` for the failure path; the read-repair side-effect path lands in Stage 8 | done (Stage 7) for data shape |
| `rspmgr_submit_response` | `ResponseMgr::submit_response` | done (Stage 7) |
| `rspmgr_free_response` / `rspmgr_free_other_responses` | replaced by Rust ownership: `Drop` on `ResponseMgr` releases all retained responses; `pick_response` borrows so the caller still owns the manager. | done (Stage 7) |
| `rspmgr_clone_responses` | deferred to Stage 9 (uses `msg_clone`) | omitted-for-stage |
| `msg_local_one_rsp_handler` | deferred to Stage 9 (the response handler vtable lives on `Conn`) | omitted-for-stage |
| `perform_repairs_if_necessary` | deferred to Stage 8 (calls into `g_make_repair_query`) | omitted-for-stage |
| `rspmgr_incr_non_quorum_responses_stats` | deferred to Stage 9 (uses pool-level stats counters that need the `Conn` reference) | omitted-for-stage |
| (new) | `dynomite::msg::QuorumOutcome` | Enum unifying `done` + `is_quorum_achieved` into a single decision. |

### dyn_request.c

| C symbol | Rust home | Notes |
|---|---|---|
| `req_get` / `req_put` | deferred to Stage 9 (conn-coupled allocator + timeout queue) | omitted-for-stage |
| `req_done` (data-shape side: `is this single request resolved?`) | `dynomite::msg::request::is_done` | done (Stage 7) |
| `req_done` (sibling-walk that flips `fdone` on every fragment) | deferred to Stage 9 (walks the connection's client tail-queue) | omitted-for-stage |
| `req_error` | `dynomite::msg::request::is_error` (data-shape side); sibling-walk deferred to Stage 9 | done (Stage 7) for data shape |
| `req_make_reply` | deferred to Stage 9 (allocates a paired response on the conn outq) | omitted-for-stage |
| `req_recv_next` / `req_recv_done` / `req_send_next` / `req_send_done` / `req_forward_error` / `req_forward_local_datastore` / `req_forward_all_racks_for_dc` | deferred to Stage 9 (connection FSM + reactor wiring) | omitted-for-stage |
| (new) | `dynomite::msg::request::set_error` | Idempotent error-state setter. |
| (new) | `dynomite::msg::request::move_completed` | Data-shape building block for sibling-walk tests. |

### dyn_response.c

| C symbol | Rust home | Notes |
|---|---|---|
| `rsp_get` / `rsp_put` | deferred to Stage 9 (conn-coupled allocator) | omitted-for-stage |
| `rsp_make_error` (response-construction side) | `dynomite::msg::response::make_error` | done (Stage 7) |
| `rsp_make_error` (fragment dequeue side) | deferred to Stage 9 (walks the conn outq) | omitted-for-stage |
| `rsp_recv_next` / `server_rsp_recv_done` / `rsp_send_next` / `rsp_send_done` | deferred to Stage 9 (connection FSM) | omitted-for-stage |
| (new) | `dynomite::msg::response::link` | Pairs a response with its request; used by tests and by the Stage 9 dispatcher. |

### dyn_dnode_request.c

| C symbol | Rust home | Notes |
|---|---|---|
| `dnode_req_forward_error` / `dnode_peer_req_forward` / `dnode_peer_gossip_forward` / `dnode_peer_req_forward_stats` | deferred to Stage 10 (peer / gossip plumbing) | omitted-for-stage |

### dyn_connection.{c,h}

| C symbol | Rust home | Notes |
|---|---|---|
| `MAX_CONN_QUEUE_SIZE` | `dynomite::net::conn::MAX_CONN_QUEUE_SIZE` | done (Stage 9) |
| `MAX_CONN_ALLOWABLE_NON_RECV` / `MAX_CONN_ALLOWABLE_NON_SEND` | omitted: the C constants are referenced only by commented-out heuristic code paths in `dyn_connection.c`. |
| `connection_type_t` (`CONN_PROXY` / `CONN_CLIENT` / `CONN_SERVER` / `CONN_DNODE_PEER_PROXY` / `CONN_DNODE_PEER_CLIENT` / `CONN_DNODE_PEER_SERVER`) | `dynomite::io::reactor::ConnRole` (Stage 2 placed the discriminants; Stage 9 wires them into the FSM dispatch) | done (Stage 9) |
| `struct conn_ops` (`recv` / `recv_next` / `recv_done` / `send` / `send_next` / `send_done` / `close` / `active` / `ref` / `unref` / `enqueue_inq` / `dequeue_inq` / `enqueue_outq` / `dequeue_outq` / `rsp_handler`) | replaced by per-role driver functions (`dynomite::net::client::client_loop`, `dynomite::net::server::ServerConn::run`, `dynomite::net::dnode_client::dnode_client_loop`, `dynomite::net::dnode_server::DnodeServerConn::run`) plus the `dynomite::net::dispatcher::Dispatcher` trait. | done (Stage 9) |
| `struct conn` | `dynomite::net::conn::Conn` | done (Stage 9) (data-shape; per-conn `outstanding_msgs_dict` is a `HashMap<MsgId, MsgId>` placeholder until Stage 10 wires the cluster-side response router) |
| `conn_cant_handle_response` | omitted: the C engine uses it as a vtable poison; the Rust port does not install dispatchers on listener-only roles, so the slot has no equivalent. |
| `conn_handle_response` (macro) | folded into the per-role driver loops; the dispatcher channel performs the same routing without a separate dispatch fn. |
| `conn_recv` / `conn_recv_next` / `conn_recv_done` / `conn_send` / `conn_send_next` / `conn_send_done` (macros) | folded into the per-role drivers (`client::client_loop`, `server::ServerConn::run`, `dnode_client::dnode_client_loop`, `dnode_server::DnodeServerConn::run`). | done (Stage 9) |
| `conn_close` (macro) | `dynomite::net::conn::Conn::close` | done (Stage 9) |
| `conn_active` (macro) | folded: each driver tests its own queues + transport state directly. |
| `conn_ref` / `conn_unref` (macros) | omitted: Rust ownership replaces the manual ref/unref dance. The owner pointer the C engine threaded through the vtable is the dispatcher arc held by the driver task. |
| `conn_enqueue_inq` / `conn_dequeue_inq` / `conn_enqueue_outq` / `conn_dequeue_outq` (macros) | `dynomite::net::conn::Conn::enqueue_in` / pop on `imsg_q_mut` / `enqueue_out` / pop on `omsg_q_mut` | done (Stage 9) |
| `g_read_consistency` / `g_write_consistency` (globals) | `dynomite::net::conn::Conn::read_consistency` / `write_consistency` (per-conn fields seeded from config; the global default lives in `crate::msg::ConsistencyLevel`) | done (Stage 9) |
| `conn_is_req_first_in_outqueue` | folded into per-role drivers via direct front-of-queue inspection. |
| `conn_to_ctx` | omitted: Rust drivers receive the cluster context through the supplied `Dispatcher` rather than a per-conn back-pointer. |
| `conn_set_read_consistency` / `conn_get_read_consistency` / `conn_set_write_consistency` / `conn_get_write_consistency` | `Conn::set_read_consistency` / `read_consistency` / `set_write_consistency` / `write_consistency` | done (Stage 9) |
| `conn_event_del_conn` / `conn_event_add_out` / `conn_event_add_conn` / `conn_event_del_out` | omitted: tokio's reactor handles event registration implicitly (Stage 2). |
| `conn_get` / `conn_put` | replaced by `Conn::new` and `Drop` (Rust ownership; the C engine's free list is unnecessary). |
| `conn_init` / `conn_deinit` | omitted: Rust types are RAII. |
| `conn_listen` | `dynomite::net::listener::bind_dual_stack` plus the role-specific `Proxy::bind` / `DnodeProxy::bind` constructors. | done (Stage 9) |
| `conn_connect` | folded into the `ConnFactory` closure callers register with `dynomite::net::ConnPool::with_factory`. | done (Stage 9) |
| `conn_recv_data` / `conn_sendv_data` | replaced by the tokio `AsyncRead` / `AsyncWrite` impls on `dynomite::io::reactor::TcpTransport` and `crate::net::quic::QuicTransport`. | done (Stage 9) |
| `DYN_KEEPALIVE_INTERVAL_S` (constant) | `dynomite::net::conn::DYN_KEEPALIVE_INTERVAL_S` is not yet exposed; the per-stream keepalive is delegated to tokio + the OS defaults. | omitted: tokio's TCP socket honors the OS keepalive default; explicit tuning lands with Stage 12 binary wiring. |

### dyn_connection_internal.{c,h}

| C symbol | Rust home | Notes |
|---|---|---|
| `nfree_connq` / `free_connq` | omitted: replaced by Rust ownership (no free list). |
| `_conn_get_type_string` | `dynomite::io::reactor::ConnRole` Debug impl. |
| `_print_conn` / `print_obj` (per-conn) | replaced by `#[derive(Debug)]` on `Conn` plus the manual `Debug` impl in `net::conn`. |
| `_conn_get` / `_conn_put` / `_conn_free` / `_conn_init` / `_conn_deinit` | replaced by `Conn::new` and `Drop`. |
| `_add_to_ready_q` / `_remove_from_ready_q` | omitted: tokio drives readiness through `select!`; there is no per-pool ready queue. |
| `_conn_reuse` | folded into `dynomite::net::listener::BindOptions::reuseaddr` + `socket2::Socket::set_reuse_address`. |

### dyn_connection_pool.{c,h}

| C symbol | Rust home | Notes |
|---|---|---|
| `MIN_WAIT_BEFORE_RECONNECT_IN_SECS` | folded into `dynomite::net::pool::Backoff::record_failure` (initial 1s, doubling). | done (Stage 9) |
| `struct conn_pool` | `dynomite::net::pool::ConnPool` | done (Stage 9) |
| `_print_conn_pool` | replaced by `Debug` impl. |
| `_create_missing_connections` | replaced by `ConnPool::get`'s lazy fill (the factory is invoked only when an idle slot is missing). |
| `conn_pool_create` | `ConnPool::new` and `ConnPool::with_factory` | done (Stage 9) |
| `conn_pool_preconnect` | folded into `ConnPool::with_factory` + caller-driven `get` loop; an explicit eager-preconnect helper lands with Stage 12 startup wiring. |
| `conn_pool_get` | `ConnPool::get` | done (Stage 9) (deviation: returns a typed `ConnHandle` whose `Drop` returns the connection to the pool, eliminating the C `conn_pool_notify_conn_close` callback shape) |
| `conn_pool_destroy` | `ConnPool::shutdown` | done (Stage 9) |
| `conn_pool_notify_conn_close` | folded into `ConnHandle::release` and `ConnHandle::discard`. |
| `conn_pool_notify_conn_errored` | folded into `ConnPool::get`'s failure-arm + `Backoff::record_failure`. |
| `conn_pool_connected` | folded into the success arm of `ConnPool::get` (`auto_eject.record_success` + `backoff.record_success`). |
| `conn_pool_active_count` | `ConnPool::idle_count` + `ConnPool::in_flight` (sum) | done (Stage 9) |
| `_conn_pool_reconnect_task` | replaced by the `tokio::sync::Notify` + retry loop inside `ConnPool::get`; the C engine's task scheduler is replaced by tokio. |

### dyn_proxy.{c,h}

| C symbol | Rust home | Notes |
|---|---|---|
| `proxy_ref` | folded into `Proxy::bind` (the bound listener becomes the pool's `p_conn`-equivalent owner). |
| `proxy_unref` | folded into `Proxy::run`'s shutdown arm. |
| `proxy_close` | folded into `Proxy::run`'s exit. |
| `proxy_init` | `dynomite::net::proxy::Proxy::bind` | done (Stage 9) |
| `proxy_deinit` | folded into `Proxy::run`'s cancel branch. |
| `proxy_accept` | folded into `Proxy::run`'s accept loop, including `set_nodelay(true)` on each accepted client socket. | done (Stage 9) |
| `proxy_recv` | folded into `Proxy::run`'s accept loop (tokio replaces the level-triggered recv-ready spin). |
| `proxy_ops` (vtable) | replaced by the per-role driver pattern; PROXY only needs an accept loop, which is `Proxy::run`. |
| `init_proxy_conn` | folded into `Proxy::bind`. |

### dyn_client.{c,h}

| C symbol | Rust home | Notes |
|---|---|---|
| `client_ref` / `client_unref` / `client_unref_internal_try_put` / `client_unref_and_try_put` | replaced by Rust ownership: the per-client tokio task owns the `Conn` and drops it when the loop exits. The `outstanding_msgs_dict` is a `HashMap` field on `Conn` cleared in `client_loop`. |
| `client_active` | folded into `client_loop`'s exit conditions (recv_chain + send_chain + queues empty + EOF). |
| `client_close_stats` | deferred to Stage 12 (stats wiring) | omitted-for-stage |
| `client_close` | folded into `client_loop`'s exit. |
| `client_handle_response` | folded into `client_loop::handle_response`; the C engine's per-conn dictionary lookup is replaced by the dispatcher's response channel. | done (Stage 9) |
| `req_recv_next` | folded into `client_loop::drive_parser`. |
| `req_filter` | folded into `client_loop::drive_parser`'s `MsgParseResult::Noop` branch (covers empty, quit, dyno-config). | done (Stage 9) for shape; quit-emit + dyno-config response synthesis depend on Stage 10 dispatcher hooks. |
| `req_forward_error` | deferred to Stage 10 (cluster routing) | omitted-for-stage |
| `req_redis_stats` / `req_forward_stats` | deferred to Stage 12 (stats wiring) | omitted-for-stage |
| `req_forward_local_datastore` / `req_forward_to_peer` / `req_forward_all_racks_for_dc` / `req_forward_all_dcs_all_racks_all_nodes` / `req_forward_remote_dc` / `req_forward_local_dc` / `req_forward` | deferred to Stage 10 (cluster routing) | omitted-for-stage |
| `rewrite_query_if_necessary` / `rewrite_query_with_timestamp_md` / `fragment_query_if_necessary` | folded into Stage 8's `redis_rewrite_query` / `redis_rewrite_query_with_timestamp_md` / `redis_fragment` plus the equivalents for memcache; Stage 10 invokes them through the dispatcher. |
| `req_recv_done` | folded into `client_loop::drive_parser` + dispatcher hand-off. |
| `msg_get_rsp_handler` / `msg_local_one_rsp_handler` / `swallow_extra_rsp` / `msg_quorum_rsp_handler` / `find_rspmgr_idx` / `all_rspmgrs_done` / `all_rspmgrs_get_response` / `msg_each_quorum_rsp_handler` | deferred to Stage 10 (per-DC quorum machinery) | omitted-for-stage |
| `req_client_enqueue_omsgq` / `req_client_dequeue_omsgq` | folded into `Conn::enqueue_out` and `Conn::omsg_q_mut().pop_front`. |
| `client_ops` (vtable) | replaced by `client_loop`. |
| `init_client_conn` | folded into `Proxy::run`'s spawn arm (`Conn::new(transport, ConnRole::Client)`). |
| `admin_req_forward_local_datastore` | deferred to Stage 12 (admin mode) | omitted-for-stage |
| `request_send_to_all_dcs` / `request_send_to_all_local_racks` | deferred to Stage 10 (routing policy) | omitted-for-stage |

### dyn_dnode_client.{c,h}

| C symbol | Rust home | Notes |
|---|---|---|
| `dnode_client_ref` / `dnode_client_unref_internal_try_put` / `dnode_client_unref_and_try_put` / `dnode_client_unref` | replaced by Rust ownership: the dnode-client tokio task owns the `Conn`. |
| `dnode_client_active` | folded into `dnode_client_loop`'s exit conditions. |
| `dnode_client_close_stats` | deferred to Stage 12 (stats wiring). |
| `dnode_client_close` | folded into `dnode_client_loop`'s exit. |
| `dnode_client_handle_response` | folded into `dnode_client_loop`'s response branch. | done (Stage 9) |
| `dnode_req_filter` | folded into `dnode_client_loop::drive_dnode_parser` (the `dmsg_process` classification arm). | done (Stage 9) |
| `dnode_req_forward` | the inbound peer-client driver hands the parsed `Msg` to the configured `Dispatcher` via `handler.dispatcher().dispatch(msg, responder)`; the cluster-aware dispatcher (DC/rack routing) lands with Stage 10. | done (Stage 9) for the dispatch hook; cluster-aware routing depends on Stage 10. |
| `dnode_req_recv_next` / `dnode_req_recv_done` | folded into `dnode_client_loop::drive_dnode_parser`, which now invokes the configured `Dispatcher` and routes the result through the per-connection responder channel. | done (Stage 9) |
| (encrypted payload decrypt) | `dnode_client::decrypt_dnode_payload` calls `Crypto::aes_decrypt(payload, conn.aes_key())` when the dnode header marks the payload as encrypted; failure collapses to a single opaque `NetError::Dnode` (see Deviation "Stage 9: dnode decrypt error collapse"). | done (Stage 9) |
| `dnode_req_client_enqueue_omsgq` / `dnode_req_client_dequeue_omsgq` | folded into `Conn::enqueue_out` and `omsg_q_mut().pop_front`. |
| `dnode_rsp_send_next` | folded into `dnode_server::DnodeServerConn::run`'s response-prepend path. | done (Stage 9) |
| `dnode_rsp_send_done` | folded into `dnode_server::DnodeServerConn::run`'s post-send arm. | done (Stage 9) |
| `dnode_client_ops` (vtable) | replaced by `dnode_client_loop`. |
| `init_dnode_client_conn` | folded into `DnodeProxy::run`'s spawn arm (`Conn::new(transport, ConnRole::DnodePeerClient)`). |

### dyn_dnode_proxy.{c,h}

| C symbol | Rust home | Notes |
|---|---|---|
| `dnode_ref` / `dnode_unref` / `dnode_close` | folded into `DnodeProxy::run`'s lifecycle (Rust ownership replaces ref/unref). |
| `dnode_proxy_init` | `dynomite::net::dnode_proxy::DnodeProxy::bind` | done (Stage 9) |
| `dnode_proxy_deinit` | folded into `DnodeProxy::run`'s cancel branch. |
| `dnode_accept` | folded into `DnodeProxy::run`'s accept loop. |
| `dnode_recv` | folded into `DnodeProxy::run`'s accept loop. |
| `dnode_server_ops` (vtable) | replaced by `dnode_client_loop` for accepted peer connections. |
| `init_dnode_proxy_conn` | folded into `DnodeProxy::bind`. |

### dyn_request.c (Stage 9 portion)

| C symbol | Rust home | Notes |
|---|---|---|
| `req_get` / `req_put` | replaced by `Msg::new` + Rust ownership (`Drop` releases the message). |
| `req_done` (sibling-walk that flips `fdone` on every fragment) | deferred to Stage 10 (walks the connection's client tail-queue threaded with the cluster-supplied frag id). | omitted-for-stage |
| `req_error` (sibling-walk) | deferred to Stage 10. | omitted-for-stage |
| `req_make_reply` | folded into `client_loop`'s response arm: the dispatcher delivers the `Msg` and the loop writes it. |
| `req_recv_next` / `req_recv_done` / `req_send_next` / `req_send_done` | folded into `client_loop::drive_parser` and `dnode_client_loop::drive_dnode_parser`. | done (Stage 9) |

### dyn_response.c (Stage 9 portion)

| C symbol | Rust home | Notes |
|---|---|---|
| `rsp_get` / `rsp_put` | replaced by `Msg::new` + Rust ownership. |
| `rsp_make_error` (fragment dequeue side) | deferred to Stage 10 (fragment-walk depends on the cluster's frag-id bookkeeping). | omitted-for-stage |
| `rsp_recv_next` / `server_rsp_recv_done` / `rsp_send_next` / `rsp_send_done` | folded into `server::ServerConn::run` and `dnode_server::DnodeServerConn::run`. | done (Stage 9) |

### Stage-9 new types

| New type | Rust home | Notes |
|---|---|---|
| `dynomite::net::conn::Conn` | the per-connection state struct. |
| `dynomite::net::conn::ConnHandle` | stable connection identifier (replaces the C engine's socket-fd-based identification). |
| `dynomite::net::conn::ConnStats` | rolling per-conn counters (recv/send bytes + events + msg counts). |
| `dynomite::net::dispatcher::Dispatcher` / `DispatchOutcome` / `OutboundEnvelope` / `ServerSink` / `NoopDispatcher` | cluster-side seam (Stage 10 plug-in point). |
| `dynomite::net::pool::ConnPool` / `ConnPoolConfig` / `ConnHandle` / `ConnFactory` / `ConnFuture` | bounded outbound pool with backoff. |
| `dynomite::net::auto_eject::AutoEject` / `AutoEjectState` | consecutive-failure auto-eject decision. |
| `dynomite::net::listener::bind_dual_stack` / `BindOptions` | dual-stack v4+v6 binder via `socket2::Socket::set_only_v6(false)`. |
| `dynomite::net::client::ClientHandler` / `ClientLoopOutcome` / `client_loop` | CLIENT FSM driver. |
| `dynomite::net::server::ServerConn` / `OutboundRequest` | SERVER outbound driver. |
| `dynomite::net::dnode_client::dnode_client_loop` | DNODE_PEER_CLIENT inbound driver. |
| `dynomite::net::dnode_server::DnodeServerConn` | DNODE_PEER_SERVER outbound driver. |
| `dynomite::net::dnode_proxy::DnodeProxy` | DNODE_PEER_PROXY listener. |
| `dynomite::net::proxy::Proxy` | PROXY listener. |
| `dynomite::net::quic::QuicTransport` / `QuicListener` / `QuicConfig` / `connect` | QUIC transport (feature-gated). |
| `dynomite::net::NetError` | top-level error type. |

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

### Stage 7: `_/dynomite/docs/dyn_protocol.txt` example header is out of date

The legacy spec at `_/dynomite/docs/dyn_protocol.txt` shows an
example DNODE header `"2014 1344 5 1 1 0\r\n*4 minh\r\n..."` that
omits the `$` magic delimiters, the `*<mlen>` data-length marker,
the inline data byte, and the `*<plen>` payload-length marker.
The live encoder in `_/dynomite/src/dyn_dnode_msg.c::dmsg_write`
emits `"   $2014$ <id> <type> <flags> <version> <same-dc> *<mlen>
<data> *<plen>\r\n"`, and the live parser
(`dyn_parse_core`) requires that exact shape. The Rust port
reproduces the live-on-the-wire framing; the textual spec is
recorded here as background only and not used as authoritative.

### Stage 7: `dval` struct dropped

The header `_/dynomite/src/dyn_dnode_msg.h` declares `struct dval`
but the symbol has no in-tree caller; the Rust port omits it
entirely rather than carrying a never-instantiated type.

### Stage 7: parser does not span an mbuf chain in this stage

`_/dynomite/src/dyn_dnode_msg.c::dyn_parse_core` walks the bytes
of the last mbuf in the connection's chain; on truncation it
yields `MSG_PARSE_AGAIN` and returns to the caller, which feeds in
more data. The Stage 7 Rust parser flattens the chain into a
contiguous slice in `parse_req` / `parse_rsp` before driving the
state machine. The streaming primitive
([`DnodeParser::step`]) accepts arbitrary boundaries and is the
entry point Stage 9 will use directly from the connection FSM
when it spans buffers.

## Deviations

### Stage 11: entropy crypto uses PKCS#7 padding instead of zero-padding-required-by-caller

The reference engine sets `EVP_CIPHER_CTX_set_padding(ctx, 0)` in
`entropy_encrypt` (`_/dynomite/src/entropy/dyn_entropy_util.c` lines
179-180) and feeds the cipher chunks of the configured
`buffer_size` plus a possibly-undersized final chunk
(`last_chunk_size = file_size - (nchunk - 1) * buffer_size`). With
no padding mode, `EVP_EncryptUpdate` requires the input length to
be a multiple of the AES block size, so the C path silently
produces a corrupt final chunk whenever `last_chunk_size` is not a
multiple of 16 (a likely outcome for an arbitrary AOF file). The
Rust port enables PKCS#7 padding (`Pkcs7` from the RustCrypto
stack), which always emits one full extra block and lets the
receiver recover an exact byte count. The negotiation header
advertises a `cipher_size` of `buffer_size + 16` to accommodate
the extra block.

Pinned by `dynomite::entropy::send::tests::encrypt_chunk_round_trips_with_pkcs7`
and the integration test
`crates/dynomite/tests/stage_11_entropy.rs::encrypted_roundtrip_with_bundled_fixtures`.

### Stage 11: entropy chunks are length-prefixed instead of fixed `cipher_size`

The reference engine sends every chunk as exactly `cipher_size`
bytes on the wire (`send(peer_socket, ciphertext, sizeof(ciphertext), 0)`,
`_/dynomite/src/entropy/dyn_entropy_snd.c` line 257). With
`cipher_size > buffer_size`, the wire bytes after the actual
ciphertext are uninitialised stack contents on the C side and are
ignored by the receiver. The Rust port prefixes each chunk with a
4-byte big-endian `chunk_len` and emits exactly that many bytes,
which both removes the uninitialised-stack leak and lets the
receiver recover an exact ciphertext length when PKCS#7 padding
produces sub-`cipher_size` outputs. The negotiation `cipher_size`
field is retained as the maximum payload size for validation.

### Stage 11: entropy snd/rcv wire formats unified into a single file-stream framing

The reference engine ships two wholly different wire formats: the
`snd` path streams the entire AOF file as opaque bytes
(`_/dynomite/src/entropy/dyn_entropy_snd.c`), while the `rcv` path
expects a header `numberOfKeys` followed by per-record
`keyValueLength` plus that many bytes
(`_/dynomite/src/entropy/dyn_entropy_rcv.c`). The two paths are
not inverses of each other; the C engine relies on an external
Lepton/Spark cluster to bridge between them. The Rust port
unifies the framing on the file-stream shape and exposes the
record-level interpretation through the user-supplied
`SnapshotSink` so an embedder can layer either contract. The
default `RedisReplaySink` writes the decrypted bytes at a Redis
TCP socket (matching what `entropy_rcv_start` does for each key in
the C engine, only as a single write).

### Stage 11: `BGREWRITEAOF` invoked over RESP instead of via `system("redis-cli ...")`

The reference snapshot pipeline shells out to `redis-cli`
(`_/dynomite/src/entropy/dyn_entropy_snd.c::entropy_redis_compact_aof`,
line 38). The Rust port speaks the RESP wire format directly to
the configured Redis endpoint to remove the `redis-cli` runtime
dependency. Behavioural shape is identical: one `BGREWRITEAOF`
command, retry once after a configurable pause on transport
failure.

### Stage 11: throughput throttler removed

The C `entropy_snd_start` interleaves `usleep` calls to cap
throughput at `THROUGHPUT_THROTTLE` bytes per second
(`_/dynomite/src/entropy/dyn_entropy_snd.c` lines 220-235). The
Rust port omits the throttle: tokio's cooperative scheduling
yields the task often enough that explicit sleeping serves no
purpose, and the throttle constant is hardcoded in the C source
rather than configured. Embedders that need rate limiting can
plug their own `SnapshotSource` that yields chunks at a controlled
cadence.

### Stage 11: `recon_key.pem` / `recon_iv.pem` content is honoured

The reference loader
(`_/dynomite/src/entropy/dyn_entropy_util.c::entropy_key_iv_load`)
reads each file with `fgets` and immediately discards the result:
the `theKey = (unsigned char *)buff` and matching IV assignment
are commented out, so the cipher always runs against the
hardcoded literal `0123456789012345`. The Rust loader honours the
file contents. To absorb the off-by-one in the bundled fixture
(`_/dynomite/conf/recon_key.pem` is `01234567890123456` -- 17
characters, not 16), the Rust loader trims trailing whitespace and
takes the first 16 bytes provided the file contains at least that
many; the truncation result equals the C hardcoded literal
byte-for-byte, preserving cipher compatibility.

Pinned by `dynomite::entropy::util::tests::loads_bundled_recon_fixtures`,
`truncates_oversized_key_to_16_bytes`, and
`truncates_oversized_iv_to_16_bytes`.

### Stage 7: `DynErrorCode::BadFormat.message()` returns a stable string instead of `strerror(8)`

`_/dynomite/src/dyn_message.h::dn_strerror` (lines 300-332) has
no explicit arm for `BAD_FORMAT` and falls through to the
`default: return strerror(err)` branch. `BAD_FORMAT` evaluates
to enum index 8, so the C path delegates to the platform's
`strerror(8)`, which on Linux is `"Exec format error"`. That
string is unrelated to message framing; it is an artefact of the
C switch's default arm picking up the enum's numeric value as if
it were an `errno`.

The Rust port returns the constant `"Bad message format"` from
`DynErrorCode::BadFormat.message()`. Aligning with the C output
would require either depending on libc's per-platform `strerror`
table (which makes the message platform- and locale-dependent
and defeats the rustdoc doctest in `crates/dynomite/src/msg/mod.rs`)
or freezing the literal string `"Exec format error"` into the
Rust code (which would commit the Rust port to the C bug). The
faithful behaviour is to surface the message-framing meaning the
caller actually wants, and document the deviation here.

Pinned by `crates/dynomite/src/msg/mod.rs::tests::dyn_error_code_strings_match_c`
(and `DynErrorCode` as a whole).

### Stage 6: long-lived `EVP_CIPHER_CTX` globals replaced by per-call `Crypter`

The C reference holds two static `EVP_CIPHER_CTX *` (one for
encryption, one for decryption) for the entire process lifetime
and re-keys them on every call. This is incompatible with the
tokio-driven multi-task model: two worker tasks could re-key the
shared context concurrently. The Rust port allocates a fresh
`cbc::Encryptor<Aes128>` / `cbc::Decryptor<Aes128>` per call,
which is functionally equivalent but lock-free.

### Stage 6: RSA padding choice

The Rust port uses PKCS#1 OAEP padding (with the SHA-1 hash and
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

### Stage 9: `ConnPool` ownership replaces the C ref/unref dance

The C `dyn_connection_pool.c` threads ref/unref calls through
the per-conn ops vtable (`conn_ref` / `conn_unref`) and uses the
`conn_pool_notify_conn_close` callback to remove the conn from
the active array on close. The Rust port replaces both with
ordinary ownership: `ConnPool::get` returns a `ConnHandle` whose
`Drop` impl returns the connection to the pool (or, with
`ConnHandle::discard`, retires the slot). The behaviour is
identical at the observable level (the same number of
connections live in the same configurations); the API shape
diverges to fit Rust's RAII model.

### Stage 9: `auto_eject` lifted out of `dyn_server.c`

`datastore_check_autoeject` and the symmetric peer-side guard
(`dnode_peer_check_autoeject`) implement the same flow over
different owner structs in the C reference. The Rust port
factors the policy into [`net::auto_eject::AutoEject`] so the
pool and the Stage 10 cluster code share a single
implementation. Behaviour is preserved.

### Stage 9: dnode decrypt error collapse

The Stage 6 review flagged that
`crypto::aes::decrypt_to_vec` returns either
`CryptoError::BadPadding` (from the unpadding step) or
`CryptoError::DecryptionFailed` (from a length validation or a
block-cipher feed step). Surfacing the two variants to a peer
is a textbook Vaudenay padding-oracle. The Stage 9 dnode
peer-client driver consumes the decrypt result through
`net::dnode_client::decrypt_dnode_payload`, which collapses any
failure into a single opaque `NetError::Dnode("dnode payload
decrypt failed")` before the loop can write a response
frame. The detail-level error is dropped on the floor (no
`tracing::warn!`) so an attacker cannot use log timing or
content to distinguish the variants either. The Rust port
is therefore strictly safer than the C reference on this
surface.

### Stage 9: QUIC driver pump cadence

The `net::quic` driver task wakes on a tokio `select!` whose
timeout arm is capped at 10 ms. Without the cap, `quiche::Connection::timeout()`
returns the QUIC idle-timeout cadence (multiple seconds) once the
connection is established, which is far too coarse for an
interactive proxy where the application pushes bytes between
QUIC datagrams. The cap is conservative; a real wake-on-app-data
`Notify` is tracked for Stage 14 hardening (`docs/journal/blocked.md`).

The driver also buffers outgoing application bytes locally until
`quiche::Connection::is_established()` returns true. The C
reference does not have an analogue because it does not ship a
QUIC transport; this is a deviation from the TCP-only behaviour
of the original engine but is the canonical pattern for QUIC
drivers that share a single bidirectional stream with their
application layer.

### Stage 9: QUIC + crypto coexistence (resolved)
the `vendored` feature, which statically links a copy of
OpenSSL's libcrypto. The `quic` cargo feature enables `quiche`,
which bundles its own BoringSSL. A binary that links both static
archives (test binaries built with `--all-features`) saw duplicate
symbol definitions (`EVP_rc2_40_cbc`, `EVP_rc4`, `EVP_BytesToKey`,
and others) and failed the link. The resolution was to migrate
Stage 6 crypto onto the pure-Rust RustCrypto stack
(`aes`/`cbc`/`rsa`/`sha1`/`rand`); there is no longer any
C-binding crypto in the workspace, and `cargo build --workspace
--all-features` plus `cargo nextest run --workspace --all-features`
both succeed. The `rsa` crate carries RUSTSEC-2023-0071 (Marvin
Attack timing sidechannel); `dynomite::crypto` uses OAEP, not the
affected PKCS#1 v1.5 path, and the advisory is documented and
ignored in `deny.toml` and `scripts/check.sh` per
`docs/journal/blocked.md`.

The Stage 9 review confirmed `--features quic` now runs an
end-to-end loopback test (`tests/stage_09_quic.rs`) using a
pure-Rust `rcgen`-generated self-signed cert.

### Stage 10: snitch rack-distance helpers added

The reference engine's `dyn_node_snitch.{c,h}` implements only
four helpers: `get_broadcast_address`, `get_public_hostname`,
`get_public_ip4`, `get_private_ip4` (each an env-var lookup with
a peer-name fallback) plus `hostname_to_private_ip4` which is a
thin wrapper around `gethostbyname`. The brief for Stage 10
asked for `pick_target_rack` and `rack_distance` helpers in
the snitch module "mirroring `dyn_node_snitch.c`". They are not
in that file. The reference's only DC/rack proximity decision
lives in `dyn_dnode_peer.c::preselect_remote_rack_for_replication`,
which picks one rack per remote DC for replication. We honor the
brief by adding `rack_distance` / `pick_target_rack` /
`RackDistance` to `cluster::snitch` (they are pure functions of
`(self_dc, self_rack, other_dc, other_rack)`) and use them from
`ClusterDispatcher::plan` to order replica candidates. The
functions are not present in the reference snitch unit; they
represent a Rust-side convenience that the dispatcher needs.

Pinned by `dynomite::cluster::snitch::tests::distance_orders_correctly`,
`dynomite::cluster::snitch::tests::pick_target_rack_*`.

### Stage 10: gossip data shapes consolidated into a single map

`gossip_node_pool` carries both `datacenters: array<gossip_dc>`
(walked iteratively) and `dict_dc: dict<string, gossip_dc>`
(used for O(1) DC lookup); each `gossip_dc` likewise carries
both `racks` and `dict_rack`, and each `gossip_rack` carries
both `nodes` and `dict_token_nodes` / `dict_name_nodes`. The
update path keeps every dict in sync with the corresponding
array. The Rust port compresses this into a single
`HashMap<(dc, rack, token_str), GossipNode>` plus a parallel
`(dc, rack, host) -> GossipNode` map for IP-replacement
detection. The walker that emits the gossip state digest
reconstructs the per-DC / per-rack groupings on demand. The
observable behaviour is identical; the dict-and-array
duplication is dropped.

Pinned by `dynomite::cluster::gossip::tests::*` (the full
add / replace / unchanged / state-changed matrix).

### Stage 10: `init_response_mgr_all_dcs` lives on `ServerPool`

The Stage 7 brief recorded that the per-DC response manager
walker (`init_response_mgr_all_dcs`) belonged to Stage 10
because it walks the cluster's DC list. It now lives on
`ServerPool` as `init_response_mgrs`, returns a `Vec<ResponseMgr>`
of one entry per DC sized to the rack count (capped at
`MAX_REPLICAS_PER_DC = 3`), and is the integration point the
Stage 12 binary wires into the request lifecycle.

### Stage 10: HTTP florida client hand-rolled on tokio

The reference engine uses blocking BSD sockets for the Florida
fetch. The Rust port hand-rolls an HTTP/1.0 client over
`tokio::net::TcpStream` (the locked dependency set in PLAN.md
Section 2 forbids `hyper`/`reqwest`). Status-line and headers
are parsed by `httparse`; the body is everything past the
status-line + header block. The wire shape is unchanged.

Pinned by
`dynomite::seeds::florida::tests::ok_response_parsed` and
`tests/stage_10_cluster.rs::florida_seeds_via_canned_listener`.

### Stage 10: `is_aws_env` interprets `env` as the string

`_/dynomite/src/dyn_node_snitch.c:21-23` calls
`dn_strncmp(&sp->env.data, CONF_DEFAULT_ENV, 3)`. The `&` on
`sp->env.data` produces the address of the `data` POINTER
field rather than the string it references, so the C compares
the first three bytes of a `unsigned char *` against `"aws"`,
which is almost never equal regardless of the configured `env`.
This is a likely bug in the reference engine.

The Rust `cluster::snitch::is_aws_env` interprets the configured
environment label as the string the C author meant to compare
(`label.starts_with("aws")`). This is the only place Stage 10
diverges from a literal byte-faithful port; the chosen behaviour
matches the engine's intended semantics rather than its
dereference bug. Pinned by `cluster::snitch::tests::aws_env_*`.

### Stage 10: `init_response_mgrs` clamps to MAX_REPLICAS_PER_DC

`init_response_mgr_each_quorum_helper`
(`_/dynomite/src/dyn_response_mgr.c:81-95`) passes
`(uint8_t) array_n(&dc->racks)` straight through to
`init_response_mgr` with no upper bound. The C
`struct response_mgr` has a static
`responses[MAX_REPLICAS_PER_DC]` storage array, so any DC
configuration with more than 3 racks writes past the array on
response submission - a buffer-bound bug in the reference.

The Rust `cluster::pool::ServerPool::init_response_mgrs`
clamps `max_responses` to
`crate::msg::response_mgr::MAX_REPLICAS_PER_DC` (3). Real
Dynomite deployments do not exceed three replicas per DC, so
the practical impact of the divergence is zero, and the Rust
clamp prevents the C buffer-bound bug from being reproduced.
Pinned by
`cluster::pool::tests::init_response_mgrs_clamps_to_max_replicas_per_dc`.

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
