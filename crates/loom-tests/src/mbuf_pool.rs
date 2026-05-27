//! Mbuf pool model: a free-list of fixed-size buffers protected
//! by a mutex. Production type is `dynomite::io::mbuf::MbufPool`.
//! The model exercises two threads concurrently allocating and
//! freeing buffers from the same pool.
//!
//! Invariant under model check: the pool's free-list never hands
//! out the same buffer to two threads simultaneously (no aliasing).

use loom::sync::Mutex;

pub(crate) struct MbufPoolModel {
    free: Mutex<Vec<u32>>,
    total: u32,
}

impl MbufPoolModel {
    pub(crate) fn new(capacity: u32) -> Self {
        let mut v = Vec::with_capacity(capacity as usize);
        for i in 0..capacity {
            v.push(i);
        }
        Self {
            free: Mutex::new(v),
            total: capacity,
        }
    }

    pub(crate) fn alloc(&self) -> Option<u32> {
        let mut g = self.free.lock().unwrap();
        g.pop()
    }

    pub(crate) fn free(&self, id: u32) {
        let mut g = self.free.lock().unwrap();
        assert!(id < self.total, "freeing out-of-range buffer");
        assert!(!g.contains(&id), "double-free of buffer {id}");
        g.push(id);
    }
}

#[test]
fn alloc_free_concurrent_no_aliasing() {
    use loom::sync::Arc;
    use loom::thread;
    loom::model(|| {
        let pool = Arc::new(MbufPoolModel::new(4));

        let t1 = {
            let p = pool.clone();
            thread::spawn(move || {
                if let Some(b) = p.alloc() {
                    p.free(b);
                }
            })
        };
        let t2 = {
            let p = pool.clone();
            thread::spawn(move || {
                if let Some(b) = p.alloc() {
                    p.free(b);
                }
            })
        };

        t1.join().unwrap();
        t2.join().unwrap();

        // After both threads complete, all 4 buffers must be in
        // the free list (alloc + free is balanced).
        let g = pool.free.lock().unwrap();
        assert_eq!(g.len(), 4, "buffer leaked or duplicated");
    });
}
