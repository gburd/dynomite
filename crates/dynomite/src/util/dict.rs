//! Typed hash-map wrappers over [`ahash::AHashMap`].
//!
//! The engine needs a small generic hash map plus a specialized
//! variant keyed on [`MsgId`] for the in-flight message index. Both
//! are thin wrappers around [`ahash::AHashMap`] that expose only the
//! operations the rest of the engine actually uses.

use std::collections::hash_map::IntoIter;
use std::hash::Hash;

use ahash::AHashMap;

use crate::core::types::MsgId;

/// Generic typed map keyed by `K` with an [`ahash`] hasher.
///
/// The wrapper exposes the small set of operations used by the rest
/// of the engine and intentionally hides the underlying hasher and
/// table layout. Use [`DictMap::iter`] for read-only traversal and
/// [`DictMap::drain`] when you need to consume the entire map.
///
/// # Examples
///
/// ```
/// use dynomite::util::dict::DictMap;
///
/// let mut m: DictMap<&'static str, u32> = DictMap::new();
/// m.insert("k", 7);
/// assert_eq!(m.get(&"k"), Some(&7));
/// assert_eq!(m.len(), 1);
/// assert!(m.remove(&"k").is_some());
/// assert!(m.is_empty());
/// ```
#[derive(Debug, Default, Clone)]
pub struct DictMap<K: Eq + Hash, V> {
    inner: AHashMap<K, V>,
}

impl<K: Eq + Hash, V> DictMap<K, V> {
    /// Construct an empty map.
    pub fn new() -> Self {
        Self {
            inner: AHashMap::new(),
        }
    }

    /// Construct an empty map with at least `capacity` slots.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: AHashMap::with_capacity(capacity),
        }
    }

    /// Insert `value` at `key`, returning the previous value (if any).
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.inner.insert(key, value)
    }

    /// Remove and return the value at `key`.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.inner.remove(key)
    }

    /// Look up an immutable reference to the value at `key`.
    pub fn get(&self, key: &K) -> Option<&V> {
        self.inner.get(key)
    }

    /// Look up a mutable reference to the value at `key`.
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.inner.get_mut(key)
    }

    /// Whether `key` is in the map.
    pub fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(key)
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether the map is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Drop every entry.
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// Borrowed iterator over `(key, value)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.inner.iter()
    }

    /// Mutable iterator over `(key, value)` pairs.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&K, &mut V)> {
        self.inner.iter_mut()
    }

    /// Consume the map and yield `(key, value)` pairs.
    pub fn drain(&mut self) -> impl Iterator<Item = (K, V)> + '_ {
        self.inner.drain()
    }
}

impl<K: Eq + Hash, V> IntoIterator for DictMap<K, V> {
    type Item = (K, V);
    type IntoIter = IntoIter<K, V>;
    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

/// Specialization keyed on [`MsgId`], the Rust home of the C
/// `msg_table_dict_type` from `dyn_dict_msg_id.c`.
///
/// # Examples
///
/// ```
/// use dynomite::util::dict::MsgIndex;
///
/// let mut idx: MsgIndex<&'static str> = MsgIndex::new();
/// idx.insert(42, "hello");
/// assert_eq!(idx.get(&42), Some(&"hello"));
/// ```
pub type MsgIndex<V> = DictMap<MsgId, V>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_get_remove_round_trip() {
        let mut m: DictMap<u64, &'static str> = DictMap::new();
        assert!(m.is_empty());
        m.insert(1, "one");
        m.insert(2, "two");
        assert_eq!(m.len(), 2);
        assert_eq!(m.get(&1), Some(&"one"));
        assert!(m.contains_key(&2));
        assert_eq!(m.remove(&1), Some("one"));
        assert_eq!(m.remove(&1), None);
        assert_eq!(m.len(), 1);
        m.clear();
        assert!(m.is_empty());
    }

    #[test]
    fn msg_index_alias_resolves() {
        let mut idx: MsgIndex<u32> = MsgIndex::new();
        idx.insert(7, 700);
        assert_eq!(idx.get(&7), Some(&700));
    }

    #[test]
    fn iter_visits_every_entry() {
        let mut m: DictMap<u32, u32> = DictMap::with_capacity(4);
        for i in 0..4 {
            m.insert(i, i * i);
        }
        let mut seen: Vec<(u32, u32)> = m.iter().map(|(k, v)| (*k, *v)).collect();
        seen.sort_unstable();
        assert_eq!(seen, vec![(0, 0), (1, 1), (2, 4), (3, 9)]);
    }
}
