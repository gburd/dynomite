//! Golden-history recorder for the RAMP-Fast read-atomic path.
//!
//! Drives the REAL RAMP code -- the same `ramp_write` / `ramp_read`
//! entry points the `POST /ramp/transactions` and `POST /ramp/read`
//! HTTP handlers call -- with multi-key list-append transactions
//! against one shared [`NoxuDatastore`], and records the
//! per-transaction history in the Elle list-append JSON-lines shape
//! that `scripts/consistency/elle_check.py` consumes.
//!
//! The recorded history is written to
//! `crates/dynomited/tests/fixtures/consistency/ramp_list_append.jsonl`
//! so the consistency gate (`scripts/consistency/check.sh`, AGENTS.md
//! Section 6.5) checks a history captured from real code exercising the
//! RAMP path. Read atomicity forbids a reader observing one RAMP
//! transaction's write to key `a` while missing its write to `b`; the
//! Elle checker's NONMONO / DUP / G1a / CYCLE classes over this history
//! must report zero anomalies.
//!
//! This is a `#[test]` (deterministic, seeded, in-process) rather than
//! a live-binary run: the RAMP code path exercised is identical (the
//! HTTP handler is a thin JSON shim over `ramp_write` / `ramp_read`),
//! and an in-process recording keeps the gate CI-friendly and
//! reproducible. The `scripts/consistency/txn_history_workload.py
//! --ramp` mode records the same shape against a live `dynomited` for
//! the distributed EC2 runs.

#![cfg(feature = "noxu")]

use std::fmt::Write as _;

use dyniak::datastore::NoxuDatastore;
use dyniak::ramp_store::{ramp_read, ramp_write, RampWrite};

/// One recorded history line in the Elle list-append shape.
struct Rec {
    index: usize,
    process: usize,
    typ: &'static str,
    time_ns: u128,
    /// The micro-op list rendered as JSON.
    value: String,
}

fn parse_list(s: &[u8]) -> Vec<i64> {
    String::from_utf8_lossy(s)
        .split(',')
        .filter_map(|x| x.trim().parse::<i64>().ok())
        .collect()
}

fn format_list(vs: &[i64]) -> String {
    let mut out = String::new();
    for (i, v) in vs.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let _ = write!(out, "{v}");
    }
    out
}

fn list_json(vs: &[i64]) -> String {
    let mut out = String::from("[");
    for (i, v) in vs.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let _ = write!(out, "{v}");
    }
    out.push(']');
    out
}

/// Simple deterministic LCG over `usize` so the recorded history is
/// reproducible without pulling in an RNG dependency.
struct Lcg(usize);
impl Lcg {
    fn next(&mut self) -> usize {
        self.0 = self.0.wrapping_mul(6_364_136_223_846_793).wrapping_add(1);
        self.0 >> 8
    }
}

/// Run one list-append transaction over the real RAMP code and append
/// its invoke + ok lines to `recs`.
fn one_txn(
    store: &NoxuDatastore,
    keys: &[Vec<u8>],
    process: usize,
    rng: &mut Lcg,
    value_counter: &mut i64,
    recs: &mut Vec<Rec>,
) {
    let now = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    };
    let n = 1 + rng.next() % 3;
    let mut chosen: Vec<usize> = Vec::new();
    while chosen.len() < n {
        let k = rng.next() % keys.len();
        if !chosen.contains(&k) {
            chosen.push(k);
        }
    }
    chosen.sort_unstable();

    let mut appends: Vec<(usize, i64)> = Vec::new();
    let mut micro_invoke = String::from("[");
    for (i, &k) in chosen.iter().enumerate() {
        if i > 0 {
            micro_invoke.push(',');
        }
        if rng.next() % 10 < 6 {
            *value_counter += 1;
            let v = i64::try_from(process).unwrap_or(0) * 10_000_000 + *value_counter;
            appends.push((k, v));
            let _ = write!(micro_invoke, "[\"append\",{k},{v}]");
        } else {
            let _ = write!(micro_invoke, "[\"r\",{k},null]");
        }
    }
    micro_invoke.push(']');
    let idx = recs.len();
    recs.push(Rec {
        index: idx,
        process,
        typ: "invoke",
        time_ns: now(),
        value: micro_invoke,
    });

    // One RAMP read gives an atomic snapshot of every touched key.
    let touched_keys: Vec<Vec<u8>> = chosen.iter().map(|&k| keys[k].clone()).collect();
    let (snap, _rounds) = ramp_read(store, &touched_keys).expect("ramp read");
    let observed =
        |k: usize| -> Vec<i64> { snap.get(&keys[k]).map_or_else(Vec::new, |v| parse_list(v)) };

    let writes: Vec<RampWrite> = appends
        .iter()
        .map(|&(k, v)| {
            let mut list = observed(k);
            list.push(v);
            RampWrite {
                key: keys[k].clone(),
                value: format_list(&list).into_bytes(),
            }
        })
        .collect();
    if !writes.is_empty() {
        ramp_write(store, 0, &writes).expect("ramp write");
    }

    let mut micro_ok = String::from("[");
    for (i, &k) in chosen.iter().enumerate() {
        if i > 0 {
            micro_ok.push(',');
        }
        if let Some(&(_, v)) = appends.iter().find(|(ak, _)| *ak == k) {
            let _ = write!(micro_ok, "[\"append\",{k},{v}]");
        } else {
            let _ = write!(micro_ok, "[\"r\",{k},{}]", list_json(&observed(k)));
        }
    }
    micro_ok.push(']');
    let idx = recs.len();
    recs.push(Rec {
        index: idx,
        process,
        typ: "ok",
        time_ns: now(),
        value: micro_ok,
    });
}

#[test]
fn record_ramp_list_append_golden_history() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = NoxuDatastore::open(dir.path()).expect("open store");
    let keys: Vec<Vec<u8>> = (0u8..6).map(|k| vec![b'r', b'0' + k]).collect();

    let processes = 4usize;
    let rounds = 60usize;
    let mut rng = Lcg(0x9E37_79B9);
    let mut value_counter = 0i64;
    let mut recs: Vec<Rec> = Vec::new();

    for round in 0..rounds {
        one_txn(
            &store,
            &keys,
            round % processes,
            &mut rng,
            &mut value_counter,
            &mut recs,
        );
    }

    let mut out = String::new();
    for r in &recs {
        let _ = writeln!(
            out,
            "{{\"index\":{},\"process\":{},\"type\":\"{}\",\"time_ns\":{},\"value\":{}}}",
            r.index, r.process, r.typ, r.time_ns, r.value
        );
    }

    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../dynomited/tests/fixtures/consistency/ramp_list_append.jsonl");
    std::fs::create_dir_all(fixture.parent().expect("fixture has a parent"))
        .expect("mkdir fixtures");
    std::fs::write(&fixture, out.as_bytes()).expect("write fixture");

    assert!(recs.len() >= 2 * rounds, "expected a full history");
}
