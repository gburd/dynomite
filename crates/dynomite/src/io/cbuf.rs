//! Fixed-size SPSC ring buffer.
//!
//! [`CBuf<T>`] is a bounded single-producer single-consumer ring used
//! for the rings that connect the worker, gossip, and entropy
//! threads. It wraps [`crossbeam_queue::ArrayQueue`], a lock-free
//! MPMC ring; the SPSC use case is the strictest restriction of
//! MPMC, so the same container serves both. It is allocation-free on
//! the hot path and implemented entirely with safe Rust at the
//! consumer boundary, so the workspace's `forbid(unsafe_code)` lint
//! stays in effect.
//!
//! Capacity does not need to be a power of two; `ArrayQueue` accepts
//! any non-zero `usize`.
//!
//! # Examples
//!
//! ```
//! use dynomite::io::cbuf::CBuf;
//!
//! let q: CBuf<u32> = CBuf::new(4);
//! q.push(1).unwrap();
//! q.push(2).unwrap();
//! assert_eq!(q.len(), 2);
//! assert_eq!(q.pop(), Some(1));
//! assert_eq!(q.pop(), Some(2));
//! assert!(q.is_empty());
//! ```

use crossbeam_queue::ArrayQueue;

/// Bounded SPSC ring queue.
///
/// `CBuf` is a bounded ring that remains safe to use across multiple
/// producers or consumers (the underlying `ArrayQueue` is MPMC). A
/// new ring of capacity `N` admits up to `N` elements before
/// [`push`](Self::push) starts returning `Err`.
pub struct CBuf<T> {
    inner: ArrayQueue<T>,
}

impl<T> CBuf<T> {
    /// Create a ring with room for exactly `capacity` elements.
    ///
    /// # Panics
    ///
    /// Panics if `capacity == 0` (matching the precondition on
    /// [`ArrayQueue::new`]).
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::io::cbuf::CBuf;
    /// let q: CBuf<u8> = CBuf::new(8);
    /// assert_eq!(q.capacity(), 8);
    /// ```
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: ArrayQueue::new(capacity),
        }
    }

    /// Capacity in elements. Mirrors `cbuf##_SIZE`.
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Append an element. Returns `Err(item)` when the ring is full,
    /// echoing `try_send` on bounded channels. On overflow the
    /// failure is reported rather than silently dropping the item,
    /// so callers cannot lose data inadvertently.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::io::cbuf::CBuf;
    /// let q: CBuf<u8> = CBuf::new(2);
    /// q.push(1).unwrap();
    /// q.push(2).unwrap();
    /// assert_eq!(q.push(3), Err(3));
    /// ```
    pub fn push(&self, item: T) -> Result<(), T> {
        self.inner.push(item)
    }

    /// Remove and return the head element. Mirrors `CBUF_Pop`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::io::cbuf::CBuf;
    /// let q: CBuf<u8> = CBuf::new(2);
    /// q.push(7).unwrap();
    /// assert_eq!(q.pop(), Some(7));
    /// assert_eq!(q.pop(), None);
    /// ```
    pub fn pop(&self) -> Option<T> {
        self.inner.pop()
    }

    /// Number of elements currently buffered. Mirrors `CBUF_Len`.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// True when the ring contains no elements. Mirrors `CBUF_IsEmpty`.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// True when the ring is at capacity. Mirrors `CBUF_IsFull`.
    pub fn is_full(&self) -> bool {
        self.inner.is_full()
    }
}

impl<T> std::fmt::Debug for CBuf<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CBuf")
            .field("capacity", &self.inner.capacity())
            .field("len", &self.inner.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_pop_fifo_ordering() {
        let q: CBuf<u32> = CBuf::new(8);
        for i in 0..8u32 {
            q.push(i).unwrap();
        }
        assert!(q.is_full());
        for i in 0..8u32 {
            assert_eq!(q.pop(), Some(i));
        }
        assert!(q.is_empty());
    }

    #[test]
    fn push_returns_item_on_full() {
        let q: CBuf<u32> = CBuf::new(2);
        q.push(1).unwrap();
        q.push(2).unwrap();
        assert_eq!(q.push(3), Err(3));
    }

    #[test]
    fn pop_returns_none_on_empty() {
        let q: CBuf<u32> = CBuf::new(2);
        assert_eq!(q.pop(), None);
    }

    #[test]
    fn capacity_reports_initial_size() {
        let q: CBuf<u32> = CBuf::new(13);
        assert_eq!(q.capacity(), 13);
    }

    #[test]
    fn debug_reports_capacity_and_len() {
        // The Debug impl exposes capacity and the live element count.
        let q: CBuf<u32> = CBuf::new(4);
        q.push(1).unwrap();
        q.push(2).unwrap();
        let s = format!("{q:?}");
        assert!(s.contains("capacity: 4"), "got {s}");
        assert!(s.contains("len: 2"), "got {s}");
    }
}
