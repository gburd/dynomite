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

## src/event/

(Stage 2 - replaced wholesale by tokio-based reactor.)

## src/hashkit/

(Stage 3.)

## src/proto/

(Stage 8.)

## src/seedsprovider/

(Stage 10.)

## src/entropy/

(Stage 11.)

## src/tools/

(Stage 3 ships `crates/dyn-hash-tool/`.)

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

(none yet)
