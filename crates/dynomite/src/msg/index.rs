//! Message-id to message lookup.
//!
//! The reference engine keeps a per-connection dictionary
//! (`outstanding_msgs_dict`) keyed on `msgid_t` so that a response
//! arriving on the wire can be paired with its outstanding request in
//! constant time. Earlier stages introduced
//! [`crate::util::dict::MsgIndex`] (an `ahash`-backed
//! [`std::collections::HashMap`]) keyed on [`MsgId`]; this module
//! provides the message-flavored alias plus a thin newtype that adds
//! the verbs the message layer actually uses.
//!
//! [`MsgId`]: crate::core::types::MsgId

use crate::core::types::MsgId;
use crate::util::dict::DictMap;

use super::message::Msg;

/// Owning index of [`Msg`] values keyed on [`MsgId`].
///
/// The C engine stores `struct msg *` pointers; the Rust port owns
/// the value directly so dropping the index releases every contained
/// message. Lookups return references; transferring ownership out of
/// the index requires [`MsgIndex::remove`].
///
/// # Thread safety
///
/// `MsgIndex` mirrors the reference engine's `outstanding_msgs_dict`,
/// which is per-connection and accessed only from the connection's
/// owning event-loop thread. The Rust port preserves that
/// single-threaded contract: `MsgIndex` is `Send` (it can be moved
/// to another task or thread, e.g. when a connection migrates) but
/// is intentionally not exposed through any synchronisation
/// primitive. The wrapped [`std::collections::HashMap`] inside
/// [`DictMap`](crate::util::dict::DictMap) is not `Sync`, so two
/// tasks cannot share a `&MsgIndex` and call its mutating methods
/// concurrently. Stages 9 and beyond keep the index private to the
/// per-connection FSM; if a future caller ever needs cross-thread
/// shared access, that caller is responsible for wrapping it in a
/// `Mutex` (or equivalent), not the type itself.
#[derive(Debug)]
pub struct MsgIndex {
    inner: DictMap<MsgId, Msg>,
}

impl Default for MsgIndex {
    fn default() -> Self {
        Self {
            inner: DictMap::new(),
        }
    }
}

impl MsgIndex {
    /// Build an empty index.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::MsgIndex;
    /// let idx = MsgIndex::new();
    /// assert!(idx.is_empty());
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert `msg` under its own id. The previous value, if any, is
    /// returned to the caller.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::{Msg, MsgIndex, MsgType};
    ///
    /// let mut idx = MsgIndex::new();
    /// let m = Msg::new(7, MsgType::ReqRedisGet, true);
    /// assert!(idx.insert(m).is_none());
    /// assert!(idx.contains_key(7));
    /// ```
    pub fn insert(&mut self, msg: Msg) -> Option<Msg> {
        self.inner.insert(msg.id(), msg)
    }

    /// Remove the message stored under `id` and transfer ownership
    /// to the caller.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::{Msg, MsgIndex, MsgType};
    ///
    /// let mut idx = MsgIndex::new();
    /// idx.insert(Msg::new(7, MsgType::ReqRedisGet, true));
    /// assert!(idx.remove(7).is_some());
    /// assert!(idx.remove(7).is_none());
    /// ```
    pub fn remove(&mut self, id: MsgId) -> Option<Msg> {
        self.inner.remove(&id)
    }

    /// Borrow the message stored under `id`, if any.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::{Msg, MsgIndex, MsgType};
    ///
    /// let mut idx = MsgIndex::new();
    /// idx.insert(Msg::new(7, MsgType::ReqRedisGet, true));
    /// assert_eq!(idx.get(7).unwrap().id(), 7);
    /// ```
    #[must_use]
    pub fn get(&self, id: MsgId) -> Option<&Msg> {
        self.inner.get(&id)
    }

    /// Mutably borrow the message stored under `id`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::{Msg, MsgIndex, MsgType};
    ///
    /// let mut idx = MsgIndex::new();
    /// idx.insert(Msg::new(7, MsgType::ReqRedisGet, true));
    /// idx.get_mut(7).unwrap().set_done(true);
    /// ```
    pub fn get_mut(&mut self, id: MsgId) -> Option<&mut Msg> {
        self.inner.get_mut(&id)
    }

    /// True when an entry exists for `id`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::MsgIndex;
    /// assert!(!MsgIndex::new().contains_key(0));
    /// ```
    #[must_use]
    pub fn contains_key(&self, id: MsgId) -> bool {
        self.inner.contains_key(&id)
    }

    /// Number of indexed messages.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::MsgIndex;
    /// assert_eq!(MsgIndex::new().len(), 0);
    /// ```
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// True when the index has no entries.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::MsgIndex;
    /// assert!(MsgIndex::new().is_empty());
    /// ```
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msg::{Msg, MsgType};

    #[test]
    fn round_trip() {
        let mut idx = MsgIndex::new();
        let m = Msg::new(42, MsgType::ReqRedisGet, true);
        assert!(idx.insert(m).is_none());
        assert!(idx.contains_key(42));
        let popped = idx.remove(42).expect("present");
        assert_eq!(popped.id(), 42);
        assert!(!idx.contains_key(42));
    }
}
