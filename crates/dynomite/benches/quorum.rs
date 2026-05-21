//! ResponseMgr quorum decision-table benches.
//!
//! Run with `cargo bench --bench quorum`. The harness sweeps over
//! every legal `max_responses` value (1, 2, 3) and exercises the
//! full submission space (good responses, error responses, mixed
//! orderings) so the criterion timing reflects the worst-case path
//! through `outcome()` and `is_done()`.
//!
//! The corresponding baseline JSON file lives at
//! `crates/dynomite/benches/baseline/quorum.json`.

#![allow(missing_docs)]

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use dynomite::msg::{Msg, MsgType, ResponseMgr};

fn req() -> Msg {
    Msg::new(1, MsgType::ReqRedisGet, true)
}

fn good() -> Msg {
    Msg::new(2, MsgType::RspRedisStatus, false)
}

fn err() -> Msg {
    let mut m = Msg::new(3, MsgType::RspRedisStatus, false);
    m.flags_mut().is_error = true;
    m
}

fn full_table_decision(c: &mut Criterion) {
    type Builder = Box<dyn Fn() -> ResponseMgr>;
    let mut group = c.benchmark_group("quorum_full_table");
    for max in 1u8..=3 {
        // Three submission orderings cover the canonical paths:
        // 1. all-good (achieved at quorum)
        // 2. all-error (failed before quorum even possible)
        // 3. mixed (one error, rest good)
        let orderings: [(&str, Builder); 3] = [
            (
                "all_good",
                Box::new(move || {
                    let mut m = ResponseMgr::new(&req(), max, None);
                    for i in 0..max {
                        m.submit_response(good(), u32::from(i) + 1);
                    }
                    m
                }),
            ),
            (
                "all_error",
                Box::new(move || {
                    let mut m = ResponseMgr::new(&req(), max, None);
                    for _ in 0..max {
                        m.submit_response(err(), 0);
                    }
                    m
                }),
            ),
            (
                "mixed",
                Box::new(move || {
                    let mut m = ResponseMgr::new(&req(), max, None);
                    m.submit_response(err(), 0);
                    for i in 1..max {
                        m.submit_response(good(), u32::from(i));
                    }
                    m
                }),
            ),
        ];
        for (label, build) in orderings {
            let id = BenchmarkId::new(format!("max{max}/{label}"), max);
            group.bench_function(id, |b| {
                b.iter(|| {
                    let mgr = build();
                    black_box(mgr.outcome());
                    black_box(mgr.is_done());
                });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, full_table_decision);
criterion_main!(benches);
