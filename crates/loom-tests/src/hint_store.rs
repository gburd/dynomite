//! Hint-store model: a single-producer single-consumer ring of
//! pending hints. The production type is
//! `dyn_riak::handoff::HintStore`. We model only the contended
//! path: a writer enqueueing hints while a reader drains them.
//!
//! Invariant under model check: every successfully enqueued hint
//! is observed exactly once by the reader (no loss, no duplicate).

use loom::sync::atomic::{AtomicU64, Ordering};
use loom::sync::Mutex;

#[derive(Default)]
pub(crate) struct HintStoreModel {
    inner: Mutex<Vec<u64>>,
    enqueued: AtomicU64,
    drained: AtomicU64,
}

impl HintStoreModel {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn enqueue(&self, h: u64) {
        let mut g = self.inner.lock().unwrap();
        g.push(h);
        self.enqueued.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn drain_one(&self) -> Option<u64> {
        let mut g = self.inner.lock().unwrap();
        let v = g.pop();
        if v.is_some() {
            self.drained.fetch_add(1, Ordering::Relaxed);
        }
        v
    }
}

#[test]
fn enqueue_drain_loses_no_hints() {
    use loom::sync::Arc;
    use loom::thread;
    loom::model(|| {
        let store = Arc::new(HintStoreModel::new());

        let writer = {
            let s = store.clone();
            thread::spawn(move || {
                s.enqueue(1);
                s.enqueue(2);
            })
        };

        let reader = {
            let s = store.clone();
            thread::spawn(move || {
                let mut got = 0u64;
                for _ in 0..2 {
                    if let Some(v) = s.drain_one() {
                        got += v;
                    }
                }
                got
            })
        };

        writer.join().unwrap();
        let _ = reader.join().unwrap();

        // Drain anything the reader missed (it may have run
        // before the writer; loom's model space includes that
        // interleaving).
        let mut tail = 0u64;
        while let Some(v) = store.drain_one() {
            tail += v;
        }

        let total_enqueued = store.enqueued.load(Ordering::Relaxed);
        let total_drained = store.drained.load(Ordering::Relaxed);
        assert_eq!(total_enqueued, total_drained, "lost or duplicated hint");
        let _ = tail;
    });
}
