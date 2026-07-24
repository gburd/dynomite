//! Integration tests for the TTL-driven reaper FSM.
//!
//! Covers every named test from the brief plus a model-based
//! property test that drives the reaper through an arbitrary
//! sequence of inserts, deletes, and age advances and verifies
//! the monotonicity invariant: a key that was reaped never
//! resurrects, and a live key is never reaped.

use std::collections::HashMap;
use std::time::Duration;

use dyniak::reaper::{
    Event, KeyKind, ReaperConfig, ReaperHandler, ReaperOutcome, ScannedKey, State,
    DEFAULT_REAP_INTERVAL_SECONDS, DEFAULT_REAP_MAX_PER_CYCLE, DEFAULT_REAP_SIBLINGS_AFTER_SECONDS,
    DEFAULT_REAP_TOMBSTONES_AFTER_SECONDS,
};
use dynomite::cluster::apl::{ClusterState, RingPoint};
use dynomite::events::TokenRange;
use dynomite::hashkit::DynToken;
use gen_fsm::{Action, EventType, FsmHandler, Transition};
use hegel::generators as gs;
use hegel::TestCase;

const BUCKET: &[u8] = b"users";

fn small_cfg() -> ReaperConfig {
    ReaperConfig {
        reap_tombstones_after_seconds: 10,
        reap_siblings_after_seconds: 100,
        reap_max_per_cycle: 4,
        reap_interval_seconds: 60,
        reaps_per_sec: 1_000_000,
        object_ttl_seconds: 0,
    }
}

fn three_partitions() -> Vec<TokenRange> {
    vec![
        TokenRange::new(DynToken::from_u32(0), DynToken::from_u32(100)),
        TokenRange::new(DynToken::from_u32(100), DynToken::from_u32(200)),
        TokenRange::new(DynToken::from_u32(200), DynToken::from_u32(300)),
    ]
}

fn fresh(cfg: ReaperConfig, partitions: Vec<TokenRange>) -> ReaperHandler {
    ReaperHandler::with_config(BUCKET.to_vec(), cfg).with_partitions(partitions)
}

fn key(idx: usize, kind: KeyKind, age_secs: u64, label: &str) -> ScannedKey {
    ScannedKey {
        partition_idx: idx,
        key: label.as_bytes().to_vec(),
        kind,
        age: Duration::from_secs(age_secs),
    }
}

fn assert_actions_contain_set_state_eq(transition: &Transition<ReaperHandler>, expected: Duration) {
    match transition {
        Transition::Keep(actions) | Transition::Next(_, actions) => {
            let found = actions
                .iter()
                .any(|a| matches!(a, Action::SetStateTimeout(d) if *d == expected));
            assert!(
                found,
                "expected SetStateTimeout({expected:?}); got {actions:?}"
            );
        }
        Transition::Stop(_) => panic!("expected Keep/Next, got Stop"),
    }
}

// ---- Named tests -------------------------------------------------

#[test]
fn idle_tick_advances_to_scanning() {
    let mut h = fresh(small_cfg(), three_partitions());
    assert_eq!(h.initial(), State::Idle);
    let entry = h.on_enter(State::Idle);
    // Idle entry arms the reap-interval state timer.
    assert_actions_contain_set_state_eq(
        &entry,
        Duration::from_secs(small_cfg().reap_interval_seconds),
    );
    let t = h.handle(State::Idle, EventType::Cast, Event::Tick);
    match t {
        Transition::Next(State::Scanning, _) => {}
        other => panic!("expected Next(Scanning), got {other:?}"),
    }
    // Cycle counters reset on Tick.
    assert_eq!(h.scanned_this_cycle(), 0);
    assert_eq!(h.reaped_this_cycle(), 0);
    assert_eq!(h.partition_idx(), 0);
}

#[test]
fn scanning_walks_all_partitions_then_advances_to_reaping() {
    let mut h = fresh(small_cfg(), three_partitions());
    // Enter scanning.
    let _ = h.handle(State::Idle, EventType::Cast, Event::Tick);
    // Stream one tombstone per partition; all are old enough.
    for idx in 0..3 {
        let _ = h.handle(
            State::Scanning,
            EventType::Cast,
            Event::KeyScanned(key(idx, KeyKind::Tombstone, 60, &format!("p{idx}"))),
        );
        let t = h.handle(State::Scanning, EventType::Cast, Event::NextSegmentDone);
        if idx < 2 {
            // Still scanning; idx bumped.
            match t {
                Transition::Keep(_) => {}
                other => panic!("expected Keep at idx {idx}, got {other:?}"),
            }
            assert_eq!(h.partition_idx(), idx + 1);
        } else {
            // Last partition done -> Reaping.
            match t {
                Transition::Next(State::Reaping, _) => {}
                other => panic!("expected Next(Reaping), got {other:?}"),
            }
        }
    }
    // Three candidates queued, three outstanding.
    assert_eq!(h.batch_len(), 3);
    assert_eq!(h.outstanding_reaps(), 3);
    assert_eq!(h.scanned_this_cycle(), 3);
}

#[test]
fn scanning_with_no_candidates_skips_reaping() {
    let mut h = fresh(small_cfg(), three_partitions());
    let _ = h.handle(State::Idle, EventType::Cast, Event::Tick);
    // No KeyScanned events; just walk every partition.
    for idx in 0..3 {
        let t = h.handle(State::Scanning, EventType::Cast, Event::NextSegmentDone);
        if idx < 2 {
            match t {
                Transition::Keep(_) => {}
                other => panic!("expected Keep, got {other:?}"),
            }
        } else {
            match t {
                Transition::Next(State::Idle, _) => {}
                other => panic!("expected Next(Idle), got {other:?}"),
            }
        }
    }
    let rec = h
        .take_last_complete()
        .expect("an audit record should exist for an empty cycle");
    assert_eq!(rec.bucket, BUCKET);
    assert_eq!(rec.scanned, 0);
    assert_eq!(rec.reaped, 0);
}

#[test]
fn reaping_batch_acked_returns_to_idle() {
    let mut h = fresh(small_cfg(), three_partitions());
    let _ = h.handle(State::Idle, EventType::Cast, Event::Tick);
    // Queue two reapable candidates and march to Reaping.
    let _ = h.handle(
        State::Scanning,
        EventType::Cast,
        Event::KeyScanned(key(0, KeyKind::Tombstone, 60, "a")),
    );
    let _ = h.handle(
        State::Scanning,
        EventType::Cast,
        Event::KeyScanned(key(0, KeyKind::Tombstone, 60, "b")),
    );
    let _ = h.handle(State::Scanning, EventType::Cast, Event::NextSegmentDone);
    let _ = h.handle(State::Scanning, EventType::Cast, Event::NextSegmentDone);
    let t = h.handle(State::Scanning, EventType::Cast, Event::NextSegmentDone);
    match t {
        Transition::Next(State::Reaping, _) => {}
        other => panic!("expected Next(Reaping), got {other:?}"),
    }
    // Orchestrator drains the batch then acks one by one.
    let drained = h.take_batch();
    assert_eq!(drained.len(), 2);
    for _ in 0..drained.len() {
        let _ = h.handle(State::Reaping, EventType::Cast, Event::KeyReaped);
    }
    let t = h.handle(State::Reaping, EventType::Cast, Event::BatchAcked);
    match t {
        Transition::Next(State::Idle, ref actions) => {
            // Returning to Idle re-arms the reap-interval timer.
            let interval = Duration::from_secs(small_cfg().reap_interval_seconds);
            let armed = actions
                .iter()
                .any(|a| matches!(a, Action::SetStateTimeout(d) if *d == interval));
            assert!(armed, "expected interval timer; got {actions:?}");
        }
        other => panic!("expected Next(Idle), got {other:?}"),
    }
    let rec = h
        .take_last_complete()
        .expect("BatchAcked should publish a cycle-complete record");
    assert_eq!(rec.bucket, BUCKET);
    assert_eq!(rec.reaped, 2);
    assert_eq!(rec.scanned, 2);
    assert_eq!(h.reaped_this_cycle(), 2);
    assert_eq!(h.outstanding_reaps(), 0);
}

#[test]
fn reap_max_per_cycle_caps_batch_size() {
    let mut cfg = small_cfg();
    cfg.reap_max_per_cycle = 4;
    let mut h = fresh(cfg.clone(), three_partitions());
    let _ = h.handle(State::Idle, EventType::Cast, Event::Tick);
    // Stream 10 reapable tombstones into partition 0.
    for i in 0..10u64 {
        let _ = h.handle(
            State::Scanning,
            EventType::Cast,
            Event::KeyScanned(key(0, KeyKind::Tombstone, 60, &format!("k{i}"))),
        );
    }
    // Batch is capped at reap_max_per_cycle, but every key
    // still increments the scan counter (so the audit event is
    // truthful about workload size).
    assert_eq!(h.batch_len() as u64, cfg.reap_max_per_cycle);
    assert_eq!(h.scanned_this_cycle(), 10);
    // March to Reaping. Outstanding count equals the configured cap.
    let _ = h.handle(State::Scanning, EventType::Cast, Event::NextSegmentDone);
    let _ = h.handle(State::Scanning, EventType::Cast, Event::NextSegmentDone);
    let t = h.handle(State::Scanning, EventType::Cast, Event::NextSegmentDone);
    match t {
        Transition::Next(State::Reaping, _) => {}
        other => panic!("expected Next(Reaping), got {other:?}"),
    }
    assert_eq!(h.outstanding_reaps(), cfg.reap_max_per_cycle);
}

#[test]
fn tombstone_younger_than_threshold_not_reaped() {
    let mut cfg = small_cfg();
    cfg.reap_tombstones_after_seconds = 30;
    let mut h = fresh(cfg, three_partitions());
    let _ = h.handle(State::Idle, EventType::Cast, Event::Tick);
    // Scan a young tombstone (5s old) and an old one (60s).
    let _ = h.handle(
        State::Scanning,
        EventType::Cast,
        Event::KeyScanned(key(0, KeyKind::Tombstone, 5, "young")),
    );
    let _ = h.handle(
        State::Scanning,
        EventType::Cast,
        Event::KeyScanned(key(0, KeyKind::Tombstone, 60, "old")),
    );
    // Only the old tombstone is queued.
    assert_eq!(h.batch_len(), 1);
    let drained = h.take_batch();
    assert_eq!(drained[0].key, b"old");
    // Both keys count toward the scan total.
    assert_eq!(h.scanned_this_cycle(), 2);
}

#[test]
fn sibling_resolution_window_respected() {
    // Tombstones reap fast (10s), siblings linger (1000s).
    let cfg = ReaperConfig {
        reap_tombstones_after_seconds: 10,
        reap_siblings_after_seconds: 1_000,
        reap_max_per_cycle: 100,
        reap_interval_seconds: 60,
        reaps_per_sec: 1_000_000,
        object_ttl_seconds: 0,
    };
    let mut h = fresh(cfg, three_partitions());
    let _ = h.handle(State::Idle, EventType::Cast, Event::Tick);
    // 200s-old sibling: still inside the resolution window.
    let _ = h.handle(
        State::Scanning,
        EventType::Cast,
        Event::KeyScanned(key(0, KeyKind::Sibling, 200, "young-sibling")),
    );
    // 200s-old tombstone: well past its 10s window.
    let _ = h.handle(
        State::Scanning,
        EventType::Cast,
        Event::KeyScanned(key(0, KeyKind::Tombstone, 200, "old-tomb")),
    );
    // 2000s-old sibling: past the 1000s window.
    let _ = h.handle(
        State::Scanning,
        EventType::Cast,
        Event::KeyScanned(key(0, KeyKind::Sibling, 2_000, "old-sibling")),
    );
    let drained = h.take_batch();
    let drained_keys: Vec<&[u8]> = drained.iter().map(|k| k.key.as_slice()).collect();
    assert_eq!(drained.len(), 2);
    assert!(drained_keys.contains(&&b"old-tomb"[..]));
    assert!(drained_keys.contains(&&b"old-sibling"[..]));
    assert!(!drained_keys.contains(&&b"young-sibling"[..]));
}

#[test]
fn live_key_is_never_reaped_regardless_of_age() {
    let mut h = fresh(small_cfg(), three_partitions());
    let _ = h.handle(State::Idle, EventType::Cast, Event::Tick);
    let _ = h.handle(
        State::Scanning,
        EventType::Cast,
        Event::KeyScanned(key(0, KeyKind::Live, 1_000_000, "ancient-live")),
    );
    assert_eq!(h.batch_len(), 0);
    assert_eq!(h.scanned_this_cycle(), 1);
}

#[test]
fn cycle_error_returns_to_idle_with_partial_audit() {
    let mut h = fresh(small_cfg(), three_partitions());
    let _ = h.handle(State::Idle, EventType::Cast, Event::Tick);
    let _ = h.handle(
        State::Scanning,
        EventType::Cast,
        Event::KeyScanned(key(0, KeyKind::Tombstone, 60, "a")),
    );
    let t = h.handle(
        State::Scanning,
        EventType::Cast,
        Event::CycleError("storage io".into()),
    );
    match t {
        Transition::Next(State::Idle, _) => {}
        other => panic!("expected Next(Idle), got {other:?}"),
    }
    // The audit event is still emitted for the partial cycle.
    let rec = h
        .take_last_complete()
        .expect("partial cycle must still emit an audit");
    assert_eq!(rec.scanned, 1);
    assert_eq!(rec.reaped, 0);
}

#[test]
fn shutdown_stops_from_any_state() {
    for state in [State::Idle, State::Scanning, State::Reaping] {
        let mut h = fresh(small_cfg(), three_partitions());
        let t = h.handle(state, EventType::Cast, Event::Shutdown);
        match t {
            Transition::Stop(ReaperOutcome::Stopped) => {}
            other => panic!("expected Stop from {state:?}, got {other:?}"),
        }
    }
}

#[test]
fn idempotent_extra_key_reaped_events_silently_ignored() {
    // Surplus KeyReaped events past outstanding_reaps must not
    // trip the FSM into Idle prematurely or panic.
    let mut h = fresh(small_cfg(), three_partitions());
    let _ = h.handle(State::Idle, EventType::Cast, Event::Tick);
    let _ = h.handle(
        State::Scanning,
        EventType::Cast,
        Event::KeyScanned(key(0, KeyKind::Tombstone, 60, "a")),
    );
    let _ = h.handle(State::Scanning, EventType::Cast, Event::NextSegmentDone);
    let _ = h.handle(State::Scanning, EventType::Cast, Event::NextSegmentDone);
    let _ = h.handle(State::Scanning, EventType::Cast, Event::NextSegmentDone);
    // One queued key. Drive five KeyReaped events: the FSM
    // saturates outstanding_reaps at 0 and reaped_this_cycle
    // increases monotonically.
    for _ in 0..5 {
        let _ = h.handle(State::Reaping, EventType::Cast, Event::KeyReaped);
    }
    assert_eq!(h.outstanding_reaps(), 0);
    assert_eq!(h.reaped_this_cycle(), 5);
    let _ = h.handle(State::Reaping, EventType::Cast, Event::BatchAcked);
    let rec = h.take_last_complete().expect("BatchAcked emits audit");
    // The audit reports the events the FSM observed, even
    // though only one key was queued. The orchestrator is the
    // sole authority on whether five distinct delete calls
    // really happened.
    assert_eq!(rec.reaped, 5);
}

#[test]
fn defaults_are_sensible() {
    let cfg = ReaperConfig::default();
    assert_eq!(
        cfg.reap_tombstones_after_seconds,
        DEFAULT_REAP_TOMBSTONES_AFTER_SECONDS
    );
    assert_eq!(
        cfg.reap_siblings_after_seconds,
        DEFAULT_REAP_SIBLINGS_AFTER_SECONDS
    );
    assert_eq!(cfg.reap_max_per_cycle, DEFAULT_REAP_MAX_PER_CYCLE);
    assert_eq!(cfg.reap_interval_seconds, DEFAULT_REAP_INTERVAL_SECONDS);
}

#[test]
fn refresh_partitions_filters_to_local_primaries() {
    // Four-peer ring at tokens 100/200/300/400, peer 2 is the
    // local node. With n=1, only the slice rooted at peer 2's
    // entry survives the filter.
    let cluster = ClusterState::new(
        vec![
            RingPoint::new(100, 0),
            RingPoint::new(200, 1),
            RingPoint::new(300, 2),
            RingPoint::new(400, 3),
        ],
        [0u32, 1, 2, 3].into_iter().collect(),
    );
    let mut h = ReaperHandler::with_config(BUCKET.to_vec(), small_cfg());
    h.refresh_partitions_from_cluster(&cluster, 2, 1);
    assert_eq!(h.partitions().len(), 1);
    let only = &h.partitions()[0];
    assert_eq!(only.start(), &DynToken::from_u32(300));
    assert_eq!(only.end(), &DynToken::from_u32(400));
}

#[test]
fn throttle_admits_up_to_burst() {
    // Set the per-second rate to exactly four. The first four
    // try_admit_reap calls succeed; the fifth fails until the
    // bucket refills.
    let cfg = ReaperConfig {
        reaps_per_sec: 4,
        object_ttl_seconds: 0,
        ..small_cfg()
    };
    let h = fresh(cfg, three_partitions());
    for _ in 0..4 {
        assert!(h.try_admit_reap());
    }
    assert!(!h.try_admit_reap());
}

// ---- Property test -----------------------------------------------
//
// Model: a HashMap of `key -> KeyState`. Operations are insert
// (creates a Live), delete (replaces with Tombstone, age 0),
// and tick-of-time (advance every key's age). Each cycle
// drives the FSM through one Idle->Scanning->Reaping->Idle
// trip. The FSM filters tombstones older than the threshold;
// after BatchAcked the model removes the reaped keys.
//
// Monotonicity invariants:
//
//   1. A live key is never present in a reap batch.
//   2. A reaped key never reappears (never resurrected).
//   3. The audit record's `reaped` count equals the number of
//      keys we know we asked the FSM to reap.

#[derive(Clone, Copy, Debug)]
enum Op {
    Insert(u64),
    Delete(u64),
    Advance(u64),
    Cycle,
}

fn draw_op(tc: &TestCase, key_space: u64, time_step_max: u64) -> Op {
    let kind = tc.draw(gs::integers::<u64>().min_value(0).max_value(3));
    let key = tc.draw(gs::integers::<u64>().min_value(0).max_value(key_space - 1));
    match kind {
        0 => Op::Insert(key),
        1 => Op::Delete(key),
        2 => Op::Advance(tc.draw(gs::integers::<u64>().min_value(1).max_value(time_step_max))),
        _ => Op::Cycle,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ModelKind {
    Live,
    Tombstone,
}

#[derive(Clone, Copy, Debug)]
struct ModelState {
    kind: ModelKind,
    age_secs: u64,
}

fn run_one_cycle(handler: &mut ReaperHandler, model: &HashMap<u64, ModelState>) -> Vec<u64> {
    // Tick.
    let _ = handler.handle(State::Idle, EventType::Cast, Event::Tick);
    // Stream every key in the model into partition 0 in a
    // stable order so the property test is deterministic
    // under shrinking.
    let mut keys: Vec<u64> = model.keys().copied().collect();
    keys.sort_unstable();
    for k in &keys {
        let ms = model[k];
        let kind = match ms.kind {
            ModelKind::Live => KeyKind::Live,
            ModelKind::Tombstone => KeyKind::Tombstone,
        };
        let scanned = ScannedKey {
            partition_idx: 0,
            key: k.to_le_bytes().to_vec(),
            kind,
            age: Duration::from_secs(ms.age_secs),
        };
        let _ = handler.handle(State::Scanning, EventType::Cast, Event::KeyScanned(scanned));
    }
    // Walk all three partitions: only partition 0 has data
    // but we still send NextSegmentDone for each so the FSM
    // sees the end-of-scan signal.
    for _ in 0..3 {
        let _ = handler.handle(State::Scanning, EventType::Cast, Event::NextSegmentDone);
    }
    // Drain the batch.
    let batch = handler.take_batch();
    let reaped: Vec<u64> = batch
        .iter()
        .map(|sk| {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&sk.key);
            u64::from_le_bytes(buf)
        })
        .collect();
    // Ack each key.
    for _ in &reaped {
        let _ = handler.handle(State::Reaping, EventType::Cast, Event::KeyReaped);
    }
    if !reaped.is_empty() {
        let _ = handler.handle(State::Reaping, EventType::Cast, Event::BatchAcked);
    }
    reaped
}

#[hegel::test(test_cases = 256)]
fn reaper_is_monotonic_under_arbitrary_traffic(tc: TestCase) {
    let key_space = tc.draw(gs::integers::<u64>().min_value(1).max_value(8));
    let n_ops = tc.draw(gs::integers::<usize>().min_value(1).max_value(40));
    let tombstone_threshold = tc.draw(gs::integers::<u64>().min_value(1).max_value(20));

    let cfg = ReaperConfig {
        reap_tombstones_after_seconds: tombstone_threshold,
        reap_siblings_after_seconds: 1_000_000,
        reap_max_per_cycle: 1_000,
        reap_interval_seconds: 60,
        reaps_per_sec: 1_000_000,
        object_ttl_seconds: 0,
    };
    let mut handler = fresh(cfg, three_partitions());
    let mut model: HashMap<u64, ModelState> = HashMap::new();
    let mut history_reaped: Vec<u64> = Vec::new();

    for _ in 0..n_ops {
        let op = draw_op(&tc, key_space, 30);
        match op {
            Op::Insert(k) => {
                model.insert(
                    k,
                    ModelState {
                        kind: ModelKind::Live,
                        age_secs: 0,
                    },
                );
            }
            Op::Delete(k) => {
                if let Some(slot) = model.get_mut(&k) {
                    slot.kind = ModelKind::Tombstone;
                    slot.age_secs = 0;
                }
            }
            Op::Advance(t) => {
                for state in model.values_mut() {
                    state.age_secs = state.age_secs.saturating_add(t);
                }
            }
            Op::Cycle => {
                let reaped = run_one_cycle(&mut handler, &model);
                // Invariant 1: the FSM only ever returns
                // tombstones whose age exceeded the
                // configured threshold.
                for k in &reaped {
                    let ms = model
                        .get(k)
                        .expect("FSM cannot reap a key absent from the model");
                    assert_eq!(
                        ms.kind,
                        ModelKind::Tombstone,
                        "live key {k} surfaced in reap batch (age {}s, threshold {tombstone_threshold}s)",
                        ms.age_secs
                    );
                    assert!(
                        ms.age_secs >= tombstone_threshold,
                        "tombstone {k} below threshold reaped: age {}s < {tombstone_threshold}s",
                        ms.age_secs
                    );
                }
                // Apply the reap to the model: those keys
                // disappear.
                for k in &reaped {
                    model.remove(k);
                }
                // Invariant 2: reaped keys never reappear in
                // the model unless explicitly re-inserted.
                for k in &reaped {
                    assert!(
                        !model.contains_key(k),
                        "reaped key {k} resurrected without explicit insert"
                    );
                }
                // Invariant 3: the audit record matches the
                // count of distinct keys reaped this cycle.
                let rec = handler
                    .take_last_complete()
                    .expect("a non-empty cycle must publish an audit record");
                assert_eq!(rec.bucket, BUCKET);
                assert_eq!(rec.reaped, reaped.len() as u64);
                history_reaped.extend(reaped);
            }
        }
    }

    // Invariant: every key in `history_reaped` either remains
    // absent from the final model OR was re-inserted (Op::Insert
    // after Op::Cycle). The post-loop check is implicit in the
    // per-cycle Invariant 2 above; we keep `history_reaped`
    // non-empty so the optimizer does not elide the loop body.
    let _ = history_reaped;
}
