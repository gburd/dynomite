//! Per-server registry of suggestion dictionaries.
//!
//! [`SuggestionRegistry`] is the home of the autocomplete
//! state for the FT.SUG* command family. Each suggestion
//! key (the `<key>` argument to `FT.SUGADD` and friends)
//! maps to one [`SuggestionDict`]; the dispatcher consults
//! this registry on every FT.SUG* command.
//!
//! Concurrency: the registry is held behind a
//! [`parking_lot::RwLock`] over a [`BTreeMap`]. Read-only
//! commands (`FT.SUGGET`, `FT.SUGLEN`) take the read lock
//! plus a short-lived inner lock on the dict snapshot; the
//! write commands (`FT.SUGADD`, `FT.SUGDEL`) take the
//! registry write lock long enough to either insert a new
//! dict slot or look up the existing one, and then drop it
//! before mutating the dict. The dictionaries themselves are
//! held in [`Arc<parking_lot::Mutex<SuggestionDict>>`] so the
//! registry lock can be released while a single dict is
//! mutated.
//!
//! The registry is keyed by raw `Vec<u8>` because RediSearch
//! suggestion keys are arbitrary bytes (Redis treats keys as
//! binary-safe); the FT.SUG* parser preserves the bytes
//! verbatim.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use crate::sugest::{SuggestionDict, SuggestionHit};

/// Inner shape held by [`SuggestionRegistry`]: each
/// suggestion key maps to one [`SuggestionDict`] held
/// behind a short-lived [`Mutex`]. Aliased to keep clippy's
/// type-complexity lint happy on field declarations.
type DictMap = BTreeMap<Vec<u8>, Arc<Mutex<SuggestionDict>>>;

/// Per-server suggestion-dictionary registry.
///
/// Construct one with [`SuggestionRegistry::new`] and clone
/// the returned handle freely; clones share state.
#[derive(Clone, Default)]
pub struct SuggestionRegistry {
    inner: Arc<RwLock<DictMap>>,
}

impl std::fmt::Debug for SuggestionRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let count = self.inner.read().len();
        f.debug_struct("SuggestionRegistry")
            .field("dict_count", &count)
            .finish()
    }
}

impl SuggestionRegistry {
    /// Build an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a suggestion to the dictionary at `key`.
    /// Allocates the dictionary on first touch. Returns the
    /// post-insert dictionary size, which is what
    /// `FT.SUGADD` returns to the wire.
    pub fn add(
        &self,
        key: &[u8],
        suggestion: Vec<u8>,
        score: f64,
        incr: bool,
        payload: Option<Vec<u8>>,
    ) -> usize {
        let dict = self.get_or_create(key);
        let mut guard = dict.lock();
        guard.add(suggestion, score, incr, payload)
    }

    /// Run a SUGGET-shaped lookup against `key`. Returns an
    /// empty `Vec` when the key is unknown (this is what
    /// RediSearch does as well: a missing dictionary is
    /// indistinguishable from an empty one for SUGGET).
    #[must_use]
    pub fn get(
        &self,
        key: &[u8],
        prefix: &[u8],
        max: usize,
        fuzzy: bool,
        with_scores: bool,
        with_payloads: bool,
    ) -> Vec<SuggestionHit> {
        let Some(dict) = self.lookup(key) else {
            return Vec::new();
        };
        let guard = dict.lock();
        guard.get(prefix, max, fuzzy, with_scores, with_payloads)
    }

    /// Delete `suggestion` from the dictionary at `key`.
    /// Returns `true` when the suggestion was present, even
    /// when the dictionary was at size 1 prior to the call.
    /// A missing dictionary returns `false`.
    pub fn del(&self, key: &[u8], suggestion: &[u8]) -> bool {
        let Some(dict) = self.lookup(key) else {
            return false;
        };
        let mut guard = dict.lock();
        guard.del(suggestion)
    }

    /// Return the size of the dictionary at `key`. A missing
    /// dictionary reports `0`, mirroring RediSearch.
    #[must_use]
    pub fn len(&self, key: &[u8]) -> usize {
        let Some(dict) = self.lookup(key) else {
            return 0;
        };
        let guard = dict.lock();
        guard.len()
    }

    /// True when the registry has not seen any FT.SUGADD.
    /// Convenience for tests; not exposed on the wire.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }

    fn lookup(&self, key: &[u8]) -> Option<Arc<Mutex<SuggestionDict>>> {
        self.inner.read().get(key).cloned()
    }

    fn get_or_create(&self, key: &[u8]) -> Arc<Mutex<SuggestionDict>> {
        if let Some(existing) = self.lookup(key) {
            return existing;
        }
        let mut guard = self.inner.write();
        // Re-check under the write lock; another writer may
        // have raced ahead.
        if let Some(existing) = guard.get(key) {
            return existing.clone();
        }
        let fresh = Arc::new(Mutex::new(SuggestionDict::new()));
        guard.insert(key.to_vec(), fresh.clone());
        fresh
    }

    /// Build the serialisable snapshot of every dictionary.
    /// Used by the persistence path on [`crate::VectorRegistry`].
    pub(crate) fn to_snapshot(&self) -> crate::registry::SuggestionsSnapshot {
        let guard = self.inner.read();
        let mut dicts = Vec::new();
        for (key, dict) in guard.iter() {
            let entries = dict.lock().snapshot_entries();
            dicts.push(crate::registry::SuggestionDictSnapshot {
                key: key.clone(),
                entries,
            });
        }
        crate::registry::SuggestionsSnapshot { dicts }
    }

    /// Repopulate dictionaries from a snapshot. Existing state
    /// is preserved; snapshotted entries are added on top
    /// (the registry is freshly built before load, so this is
    /// effectively a full restore).
    pub(crate) fn load_snapshot(&self, snapshot: &crate::registry::SuggestionsSnapshot) {
        for dict in &snapshot.dicts {
            for (value, score, payload) in &dict.entries {
                self.add(&dict.key, value.clone(), *score, false, payload.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_then_len_round_trip() {
        let reg = SuggestionRegistry::new();
        assert_eq!(reg.add(b"k", b"alpha".to_vec(), 1.0, false, None), 1);
        assert_eq!(reg.add(b"k", b"beta".to_vec(), 2.0, false, None), 2);
        assert_eq!(reg.len(b"k"), 2);
        assert_eq!(reg.len(b"missing"), 0);
    }

    #[test]
    fn del_reports_presence() {
        let reg = SuggestionRegistry::new();
        reg.add(b"k", b"alpha".to_vec(), 1.0, false, None);
        assert!(reg.del(b"k", b"alpha"));
        assert!(!reg.del(b"k", b"alpha"));
        assert!(!reg.del(b"missing", b"x"));
    }

    #[test]
    fn distinct_keys_have_independent_dicts() {
        let reg = SuggestionRegistry::new();
        reg.add(b"k1", b"alpha".to_vec(), 1.0, false, None);
        reg.add(b"k2", b"beta".to_vec(), 1.0, false, None);
        assert_eq!(reg.len(b"k1"), 1);
        assert_eq!(reg.len(b"k2"), 1);
        let h1 = reg.get(b"k1", b"a", 5, false, false, false);
        assert_eq!(h1.len(), 1);
        assert_eq!(h1[0].value, b"alpha");
    }
}
