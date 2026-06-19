//! Ordered key/value map with a `lower_bound` operation.
//!
//! Used for token-ring lookups, where callers want the smallest key
//! greater than or equal to a probe value. This wraps
//! [`std::collections::BTreeMap`], which supports both ordered
//! iteration and `range` queries efficiently.

use std::collections::BTreeMap;

/// Ordered key/value map backed by [`BTreeMap`].
///
/// # Examples
///
/// ```
/// use dynomite::util::rbtree::OrderedMap;
///
/// let mut t: OrderedMap<u64, &'static str> = OrderedMap::new();
/// t.insert(10, "ten");
/// t.insert(30, "thirty");
/// t.insert(20, "twenty");
/// assert_eq!(t.lower_bound(&15), Some((&20, &"twenty")));
/// assert_eq!(t.min(), Some((&10, &"ten")));
/// ```
#[derive(Debug, Default, Clone)]
pub struct OrderedMap<K: Ord, V> {
    inner: BTreeMap<K, V>,
}

impl<K: Ord, V> OrderedMap<K, V> {
    /// Construct an empty map.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::util::rbtree::OrderedMap;
    /// let m: OrderedMap<u32, u32> = OrderedMap::new();
    /// assert!(m.is_empty());
    /// ```
    pub fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }

    /// Insert `value` at `key`, returning the previous value (if any).
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::util::rbtree::OrderedMap;
    /// let mut m: OrderedMap<u32, u32> = OrderedMap::new();
    /// assert_eq!(m.insert(1, 10), None);
    /// assert_eq!(m.insert(1, 20), Some(10));
    /// ```
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.inner.insert(key, value)
    }

    /// Remove the entry at `key`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::util::rbtree::OrderedMap;
    /// let mut m: OrderedMap<u32, u32> = OrderedMap::new();
    /// m.insert(1, 10);
    /// assert_eq!(m.remove(&1), Some(10));
    /// ```
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.inner.remove(key)
    }

    /// Look up the value at `key`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::util::rbtree::OrderedMap;
    /// let mut m: OrderedMap<u32, u32> = OrderedMap::new();
    /// m.insert(1, 10);
    /// assert_eq!(m.get(&1), Some(&10));
    /// ```
    pub fn get(&self, key: &K) -> Option<&V> {
        self.inner.get(key)
    }

    /// Number of entries.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::util::rbtree::OrderedMap;
    /// let mut m: OrderedMap<u32, u32> = OrderedMap::new();
    /// m.insert(1, 1);
    /// assert_eq!(m.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether the map is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::util::rbtree::OrderedMap;
    /// let m: OrderedMap<u32, u32> = OrderedMap::new();
    /// assert!(m.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Drop every entry.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::util::rbtree::OrderedMap;
    /// let mut m: OrderedMap<u32, u32> = OrderedMap::new();
    /// m.insert(1, 1);
    /// m.clear();
    /// assert!(m.is_empty());
    /// ```
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// Smallest entry, or [`None`] if the map is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::util::rbtree::OrderedMap;
    /// let mut m: OrderedMap<u32, u32> = OrderedMap::new();
    /// m.insert(3, 30);
    /// m.insert(1, 10);
    /// assert_eq!(m.min(), Some((&1, &10)));
    /// ```
    pub fn min(&self) -> Option<(&K, &V)> {
        self.inner.iter().next()
    }

    /// Largest entry, or [`None`] if the map is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::util::rbtree::OrderedMap;
    /// let mut m: OrderedMap<u32, u32> = OrderedMap::new();
    /// m.insert(3, 30);
    /// m.insert(1, 10);
    /// assert_eq!(m.max(), Some((&3, &30)));
    /// ```
    pub fn max(&self) -> Option<(&K, &V)> {
        self.inner.iter().next_back()
    }

    /// Smallest entry whose key is `>= probe`. Used by the token ring
    /// for "find the owner of this key" queries.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::util::rbtree::OrderedMap;
    /// let mut t: OrderedMap<u64, &str> = OrderedMap::new();
    /// t.insert(10, "a");
    /// t.insert(20, "b");
    /// assert_eq!(t.lower_bound(&15), Some((&20, &"b")));
    /// assert_eq!(t.lower_bound(&25), None);
    /// assert_eq!(t.lower_bound(&20), Some((&20, &"b")));
    /// ```
    pub fn lower_bound(&self, probe: &K) -> Option<(&K, &V)> {
        self.inner.range(probe..).next()
    }

    /// In-order iterator over the entries.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::util::rbtree::OrderedMap;
    /// let mut m: OrderedMap<u32, u32> = OrderedMap::new();
    /// m.insert(2, 0);
    /// m.insert(1, 0);
    /// let keys: Vec<u32> = m.iter().map(|(k, _)| *k).collect();
    /// assert_eq!(keys, vec![1, 2]);
    /// ```
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.inner.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordered_iteration() {
        let mut t: OrderedMap<u32, u32> = OrderedMap::new();
        for k in [3, 1, 5, 2, 4] {
            t.insert(k, k * 10);
        }
        let keys: Vec<u32> = t.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn min_and_max() {
        let mut t: OrderedMap<u32, u32> = OrderedMap::new();
        assert!(t.min().is_none());
        t.insert(7, 0);
        t.insert(3, 0);
        t.insert(11, 0);
        assert_eq!(t.min().map(|(k, _)| *k), Some(3));
        assert_eq!(t.max().map(|(k, _)| *k), Some(11));
    }

    #[test]
    fn lower_bound_wraps_at_end() {
        let mut t: OrderedMap<u32, u32> = OrderedMap::new();
        t.insert(10, 1);
        t.insert(20, 2);
        t.insert(30, 3);
        assert_eq!(t.lower_bound(&0).map(|(k, _)| *k), Some(10));
        assert_eq!(t.lower_bound(&15).map(|(k, _)| *k), Some(20));
        assert_eq!(t.lower_bound(&30).map(|(k, _)| *k), Some(30));
        assert!(t.lower_bound(&31).is_none());
    }
}
