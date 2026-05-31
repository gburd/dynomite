//! Thread-safe lazy cache for a 32-byte BLAKE3 segment digest.
//!
//! The default build uses [`std::sync::OnceLock`]: the first
//! observer that wins the publication race writes the digest;
//! every subsequent reader takes the fast `Acquire` load path
//! and returns the cached value. The cell is `Sync` so the
//! enclosing [`crate::Segment`] (and therefore [`crate::HashTree`])
//! is shareable across threads.
//!
//! Under `RUSTFLAGS='--cfg loom'` the same surface is provided
//! by a [`loom::sync::Mutex`]-backed alternative. Loom does not
//! ship a `OnceLock` shadow at the version pinned by this
//! workspace, so the loom alternative trades a slightly
//! over-synchronised structure for the ability to be
//! exhaustively model-checked. The observable contract that
//! both implementations honour is:
//!
//! * `get_or_init(f)` returns a single, stable value: every
//!   subsequent call (on the same instance) returns the same
//!   `Hash` regardless of which thread won the race.
//! * The closure `f` runs on at least one thread; readers that
//!   lose the race do not observe a torn or partially written
//!   digest.
//! * `LazyHash::new()` produces a fresh, empty cell; cloning a
//!   [`LazyHash`] does **not** propagate the cached digest.
//!   This keeps `#[derive(Clone)]` on [`crate::HashTree`]
//!   well-defined under both back-ends (Loom's `Mutex` is not
//!   `Clone`).
//!
//! The loom tests live in `crates/hashtree/tests/loom.rs` and
//! verify the contract above by spawning concurrent
//! `segment_hash` / `root` / `diff` callers under
//! `loom::model`.

use crate::Hash;

#[cfg(not(loom))]
use std::sync::OnceLock;

#[cfg(loom)]
use loom::sync::Mutex;

/// Lazy-init cache for a single [`Hash`] value.
///
/// See the module-level docs for the dual-build rationale and
/// the observable contract honoured by both back-ends.
#[derive(Debug)]
pub(crate) struct LazyHash {
    #[cfg(not(loom))]
    cell: OnceLock<Hash>,
    #[cfg(loom)]
    cell: Mutex<Option<Hash>>,
}

impl LazyHash {
    /// Construct an empty cache. Equivalent to [`LazyHash::default`].
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Return the cached digest, computing it from `f` on the
    /// first call. Subsequent calls return the cached value
    /// without re-invoking `f` under the default back-end; the
    /// loom back-end likewise calls `f` at most once per
    /// successful publication.
    pub(crate) fn get_or_init<F>(&self, f: F) -> Hash
    where
        F: FnOnce() -> Hash,
    {
        #[cfg(not(loom))]
        {
            *self.cell.get_or_init(f)
        }
        #[cfg(loom)]
        {
            // Loom alt: a critical section serialises the
            // double-check pattern. The first thread to acquire
            // the lock and observe `None` runs `f` and stores
            // the result; every subsequent observer sees
            // `Some(_)` and returns it. Loom verifies that no
            // interleaving of two threads racing on `get_or_init`
            // produces inconsistent return values.
            let mut g = self
                .cell
                .lock()
                .expect("invariant: LazyHash mutex is never poisoned");
            if let Some(h) = *g {
                return h;
            }
            let h = f();
            *g = Some(h);
            h
        }
    }
}

impl Default for LazyHash {
    fn default() -> Self {
        Self {
            #[cfg(not(loom))]
            cell: OnceLock::new(),
            #[cfg(loom)]
            cell: Mutex::new(None),
        }
    }
}

impl Clone for LazyHash {
    /// Clone produces a fresh, empty cache. Cached digests are
    /// cheap to recompute and propagating them across clones
    /// would require the loom back-end's `Mutex` to be
    /// cloneable, which it is not.
    fn clone(&self) -> Self {
        Self::default()
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn get_or_init_returns_first_value_and_caches_it() {
        let cell = LazyHash::new();
        let first = cell.get_or_init(|| [0xAA; 32]);
        let second = cell.get_or_init(|| [0xBB; 32]);
        assert_eq!(first, [0xAA; 32]);
        assert_eq!(second, [0xAA; 32], "cached value must be stable");
    }

    #[test]
    fn clone_does_not_propagate_cached_value() {
        let cell = LazyHash::new();
        let _ = cell.get_or_init(|| [0xCD; 32]);
        let cloned = cell.clone();
        let fresh = cloned.get_or_init(|| [0xEF; 32]);
        assert_eq!(fresh, [0xEF; 32]);
    }

    #[test]
    fn default_is_empty() {
        let cell = LazyHash::default();
        let v = cell.get_or_init(|| [0x11; 32]);
        assert_eq!(v, [0x11; 32]);
    }
}
