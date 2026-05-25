# 2026-05-24 -- dyn-riak deferred items #4 + #5

Branch: `stage/dyn-riak-deferred-items-4-5`
Base: `main` at `87c3dc4` (`chore(deps): pull latest Noxu`).

## Scope

Two small deferred items in `dyn-riak`:

* Item 4: wire
  [`mapreduce::wasm::load_modules_from_config`] into `ConfRiak`
  so operators can declare Wasm modules in YAML.
* Item 5: implement the sibling-aware merge in `aae::repair`
  (cross-replica winner-selection that respects vector clocks).

## Item 4: ConfRiak.wasm_modules

### YAML schema

The `riak:` block grew a `wasm_modules:` field (append-only;
no existing entries reformatted):

```yaml
p:
  listen: 127.0.0.1:8102
  servers: [127.0.0.1:6379:1]
  tokens: '0'
  riak:
    pbc_listen: 127.0.0.1:8087
    wasm_modules:
      - id: identity
        path: /etc/dynomited/wasm/identity.wasm
      - id: sum
        path: /etc/dynomited/wasm/sum.wasm
```

Each entry is a `ConfRiakWasmModule { id: String, path:
PathBuf }`. Validation at config-parse time catches:

* empty `id` -- rejected with
  `field: "wasm_modules.id"`.
* duplicate `id` -- rejected with the same field tag and a
  `wasm module ids must be unique` reason.
* missing file at `path` -- rejected with
  `field: "wasm_modules.path"`.

### dynomited wiring

`dynomited` grew a new Cargo feature `wasm` that implies
`riak` and forwards to `dyn-riak/wasm`. With the feature on,
`dynomited::riak::build_handles` populates a new
`RiakHandles::wasm: Option<Arc<WasmModuleStore>>` field by
calling the new `build_wasm_store_from_config` helper. The
helper trampolines through
`dyn_riak::mapreduce::wasm::load_modules_from_config` so the
loader code path is shared with the existing in-tree tests.

The Riak HTTP `/mapred` route handler (`crates/dyn-riak/src/proto/`)
is owned by another worker stream and is on the do-not-touch
list; plumbing the loaded `WasmModuleStore` into the
HTTP-driven executor lives behind that handoff. The
integration test below exercises the loader and the executor
directly to confirm the end-to-end shape.

### Tests

* `crates/dynomite/src/conf/pool.rs` (5 new unit tests):
  * `riak_wasm_modules_yaml_round_trip` -- YAML parses,
    re-serialises, re-parses; field equality round-trips.
  * `riak_wasm_modules_unique_ids_required` -- duplicate id
    rejected with the expected field tag.
  * `riak_wasm_modules_path_must_exist` -- missing path
    rejected.
  * `riak_wasm_modules_empty_id_rejected` -- empty id
    rejected.
  * Existing exhaustive `ConfRiak { ... }` doctest updated to
    set `wasm_modules: None`.
* `crates/dynomited/tests/riak.rs` (gated on the new
  `dynomited` `wasm` feature): two integration tests:
  * `riak_wasm_modules_yaml_loads_and_executor_accepts_phase`
    -- writes an identity WAT to a tempdir, builds a
    `ConfRiak`, validates, calls
    `build_wasm_store_from_config`, and submits a
    `Phase::WasmModule` job to `run_job_with_wasm`. Asserts
    the executor accepts the phase and the identity module
    round-trips its inputs.
  * `riak_wasm_modules_validate_rejects_missing_path` -- guards
    the validator's negative path.

## Item 5: sibling-aware cross-replica merge

`aae::repair` got two new public types:

```rust
pub enum RepairOutcome {
    Winner { value: Bytes, vclock: Vclock },
    Siblings(Vec<(Bytes, Vclock)>),
}

impl RepairTask {
    pub fn evaluate(
        replicas: &[(Bytes, Vclock)]
    ) -> RepairOutcome { ... }
}
```

`evaluate` walks the `replicas` slice, drops any entry whose
vector clock is strictly dominated by another entry's clock
under `Vclock::compare`, deduplicates exact-equal clocks
(keeping the lower-indexed entry), and returns either
`Winner(_)` (exactly one survivor) or `Siblings(_)` (two or
more concurrent survivors).

`RepairOutcome::resolve_with_warn(key)` is the v1 escape
hatch: when the outcome is `Siblings`, it emits a
`tracing::warn!` carrying the divergent key and the count of
concurrent siblings, then defers to the lex-largest sibling
value (vclock bytes break ties). First-class siblings storage
is left for a follow-up; the warning lets operators count
divergences without losing forward progress.

### Tests

`crates/dyn-riak/src/aae/repair.rs` (6 new unit tests):

* `evaluate_winner_when_one_dominates_others` -- A's clock
  dominates B's and C's; outcome is `Winner(A)`.
* `evaluate_siblings_when_all_concurrent` -- three replicas,
  three distinct actor clocks, outcome is `Siblings(3)`.
* `evaluate_siblings_excludes_dominated_entries` -- A
  dominates B (drops B), A and C are concurrent; outcome is
  `Siblings(2)` containing exactly A and C.
* `evaluate_dedupes_equal_clocks` -- two replicas with
  identical clocks dedupe to one survivor and return
  `Winner`.
* `resolve_with_warn_picks_lex_largest_on_siblings` -- the
  v1 fallback selects the lex-largest sibling.
* `resolve_with_warn_passes_winner_through` -- the
  `Winner(_)` outcome is returned unchanged.

The journal entry `2026-05-24-dyn-riak-aae.md` got a sibling-
aware-merge update note appended.

## Verification

* `cargo build --workspace --all-targets --locked` -- clean.
* `cargo build -p dynomited --features riak --all-targets --locked` -- clean.
* `cargo build -p dynomited --features wasm --all-targets --locked` -- clean.
* `cargo fmt -p dynomite -p dynomited -p dyn-hash-tool -p dyn-encoding -p dyn-riak -p dyn-admin -- --check` -- clean.
* `cargo clippy --workspace --all-targets --all-features -- -D warnings` -- clean.
* `cargo nextest run --workspace`: 1111 -> 1121 (+10 new
  tests; 5 in `dynomite::conf::pool::tests`, 6 in
  `dyn-riak::aae::repair::tests`, the existing
  `repair_for_divergent_key_reaches_channel` was preserved
  rather than counted as new).
* `cargo nextest run -p dyn-riak --features wasm`: 302 passed.
* `cargo nextest run -p dynomited --features wasm`: 65 passed
  including the two new integration tests.
* `cargo test --doc --workspace` -- clean.
* `bash scripts/check_no_todos.sh && bash scripts/check_no_port_comments.sh && bash scripts/check_ascii.sh` -- clean.

## Deferred follow-ups

* **HTTP /mapred wasm wiring**: the route handler in
  `crates/dyn-riak/src/proto/http/routes.rs` currently calls
  `run_job` (no WasmHook). Plumbing the loaded
  `WasmModuleStore` through to `run_job_with_wasm` is
  straightforward (extend `serve_http` / `serve_pbc` with an
  optional `Arc<dyn WasmHook>`) but lives in the proto/
  surface owned by another stream. The loader is in place;
  the consumer side is the next slice.
* **First-class siblings storage**: today
  `RepairOutcome::Siblings` falls back to lex-largest. Real
  Riak stores all siblings on the `RiakObject`; surfacing
  that back to clients (and through the repair RPC frame)
  is a separate slice.
