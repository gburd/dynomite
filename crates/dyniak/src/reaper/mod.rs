//! TTL-driven sibling and tombstone garbage collection.
//!
//! Riak's `riak_kv_reaper` periodically scans bucket types whose
//! policy says "delete tombstones older than N seconds" and
//! removes them. The same loop also evicts orphaned siblings
//! older than the configured conflict-resolution window so
//! divergent replicas do not pile up indefinitely.
//!
//! `dyniak` already commits a tombstone on the DELETE path
//! (see [`crate::server`]). Without a periodic sweep those
//! tombstones live forever, both inflating the on-disk footprint
//! and slowing down read-repair (which has to walk past every
//! tombstone in the bucket). This module ports the reaper loop
//! on top of the substrate's [`gen_fsm`] runtime so the same
//! state-functions discipline that drives handoff and AAE also
//! drives reaping.
//!
//! # State graph
//!
//! ```text
//! Idle --tick (every reap_interval)--> Scanning(partition_idx=0)
//!
//! Scanning(idx=N) --next_segment_done--> Scanning(idx=N+1) until done
//! Scanning(...done) --batch_built--> Reaping
//!
//! Reaping --batch_acked--> Idle
//!         --cycle_error--> Idle (with partial-cycle audit event)
//! ```
//!
//! The handler is intentionally I/O-free: it owns no datastore,
//! no socket, no clock other than what [`gen_fsm::FsmDriver`]
//! injects through [`gen_fsm::Action::SetStateTimeout`]. The
//! production wiring spawns an orchestrator that:
//!
//! 1. On entry to [`fsm::State::Scanning`], asks the storage
//!    engine for one segment (key+age stream) of the partition
//!    identified by [`fsm::ReaperHandler::current_partition`]
//!    and posts an [`fsm::Event::KeyScanned`] per key seen, then
//!    [`fsm::Event::NextSegmentDone`] when the segment is
//!    drained.
//! 2. On entry to [`fsm::State::Reaping`], drains the FSM's
//!    [`fsm::ReaperHandler::take_batch`] queue, issues
//!    `riak_delete` calls (rate-limited via the FSM's
//!    [`fsm::ReaperHandler::try_admit_reap`] gate), and posts a
//!    matching [`fsm::Event::KeyReaped`] per result. Missing
//!    keys count as reaped (idempotent).
//! 3. When the batch is drained, posts
//!    [`fsm::Event::BatchAcked`] which returns the FSM to
//!    [`fsm::State::Idle`] and emits an
//!    [`fsm::ReaperCycleComplete`] audit event reachable through
//!    [`fsm::ReaperHandler::take_last_complete`].
//!
//! # Per-bucket-type configuration
//!
//! [`fsm::ReaperConfig`] carries the four knobs the reaper
//! consults:
//!
//! * `reap_tombstones_after_seconds` -- minimum tombstone age
//!   before it becomes a candidate for reaping;
//! * `reap_siblings_after_seconds` -- minimum sibling age
//!   before the orphan eviction kicks in;
//! * `reap_max_per_cycle` -- per-cycle batch ceiling. Excess
//!   candidates are dropped on the floor and re-discovered next
//!   cycle (guaranteeing forward progress under load);
//! * `reap_interval_seconds` -- wall-clock period between
//!   ticks. Drives the [`gen_fsm::Action::SetStateTimeout`]
//!   armed on entry to [`fsm::State::Idle`].
//!
//! # Token-range awareness
//!
//! The reaper reaps only keys in partitions this peer is the
//! primary owner for. The orchestrator builds the partition
//! list from [`dynomite::cluster::apl::get_apl_ann`] and seeds
//! the FSM through [`fsm::ReaperHandler::with_partitions`]; the
//! FSM never reaches into the cluster substrate itself.
//!
//! # Audit events
//!
//! After each cycle the FSM stores a
//! [`fsm::ReaperCycleComplete`] record carrying the bucket
//! name, count of keys reaped, count of keys scanned, and
//! duration. The orchestrator forwards this record onto the
//! cluster-wide [`dynomite::events::EventManager`] (typically
//! re-wrapped as a future
//! `dynomite::events::ClusterEvent::ReaperCycleComplete`
//! variant). The local form is reachable for test assertions
//! through [`fsm::ReaperHandler::take_last_complete`].
//!
//! # Idempotency
//!
//! Every reap is idempotent at three layers:
//!
//! 1. The reap candidate set is rebuilt from scratch each cycle;
//!    a key that is already gone simply never enters the batch.
//! 2. A datastore that returns "not found" for a reap is treated
//!    the same as a successful delete (the orchestrator posts
//!    [`fsm::Event::KeyReaped`] either way).
//! 3. The FSM accepts more `KeyReaped` events than there are
//!    queued candidates; surplus events are silently ignored so
//!    a re-driver can replay the tail of a cycle without trip-
//!    ping the FSM into [`fsm::State::Idle`] early.
//!
//! # Submodules
//!
//! * [`fsm`] -- the reaper coordinator [`gen_fsm::FsmHandler`].

pub mod fsm;

pub use crate::reaper::fsm::{
    Event, KeyKind, ReaperConfig, ReaperCycleComplete, ReaperHandler, ReaperOutcome, ScannedKey,
    State, DEFAULT_REAPS_PER_SEC, DEFAULT_REAP_INTERVAL_SECONDS, DEFAULT_REAP_MAX_PER_CYCLE,
    DEFAULT_REAP_SIBLINGS_AFTER_SECONDS, DEFAULT_REAP_TOMBSTONES_AFTER_SECONDS,
};
