//! Owning queue of [`Msg`] values.
//!
//! Messages thread through [`MsgQueue`] lists backed by a
//! [`VecDeque`]. The owning relationship is
//! straightforward: pushing a [`Msg`] into a queue transfers
//! ownership; popping one transfers it back to the caller.
//!
//! [`Msg`]: super::message::Msg

use std::collections::VecDeque;

use crate::core::types::MsgId;

use super::message::Msg;

/// Ordered queue of owned messages.
#[derive(Debug, Default)]
pub struct MsgQueue {
    inner: VecDeque<Msg>,
}

impl<'a> IntoIterator for &'a MsgQueue {
    type Item = &'a Msg;
    type IntoIter = std::collections::vec_deque::Iter<'a, Msg>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.iter()
    }
}

impl MsgQueue {
    /// Build an empty queue.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::MsgQueue;
    /// let q = MsgQueue::new();
    /// assert!(q.is_empty());
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: VecDeque::new(),
        }
    }

    /// Build an empty queue with capacity for at least `n` messages.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::MsgQueue;
    /// let q = MsgQueue::with_capacity(8);
    /// assert!(q.is_empty());
    /// ```
    #[must_use]
    pub fn with_capacity(n: usize) -> Self {
        Self {
            inner: VecDeque::with_capacity(n),
        }
    }

    /// Append `msg` to the tail of the queue.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::{Msg, MsgQueue, MsgType};
    ///
    /// let mut q = MsgQueue::new();
    /// q.push_back(Msg::new(7, MsgType::ReqRedisGet, true));
    /// assert_eq!(q.len(), 1);
    /// ```
    pub fn push_back(&mut self, msg: Msg) {
        self.inner.push_back(msg);
    }

    /// Push `msg` to the head of the queue.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::{Msg, MsgQueue, MsgType};
    ///
    /// let mut q = MsgQueue::new();
    /// q.push_front(Msg::new(1, MsgType::ReqMcGet, true));
    /// assert_eq!(q.len(), 1);
    /// ```
    pub fn push_front(&mut self, msg: Msg) {
        self.inner.push_front(msg);
    }

    /// Remove and return the head of the queue.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::{Msg, MsgQueue, MsgType};
    ///
    /// let mut q = MsgQueue::new();
    /// q.push_back(Msg::new(1, MsgType::ReqMcGet, true));
    /// assert!(q.pop_front().is_some());
    /// assert!(q.pop_front().is_none());
    /// ```
    pub fn pop_front(&mut self) -> Option<Msg> {
        self.inner.pop_front()
    }

    /// Remove and return the tail of the queue.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::{Msg, MsgQueue, MsgType};
    ///
    /// let mut q = MsgQueue::new();
    /// q.push_back(Msg::new(1, MsgType::ReqMcGet, true));
    /// q.push_back(Msg::new(2, MsgType::ReqMcGet, true));
    /// assert_eq!(q.pop_back().unwrap().id(), 2);
    /// ```
    pub fn pop_back(&mut self) -> Option<Msg> {
        self.inner.pop_back()
    }

    /// Number of messages currently in the queue.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::MsgQueue;
    /// assert_eq!(MsgQueue::new().len(), 0);
    /// ```
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// True when the queue is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::MsgQueue;
    /// assert!(MsgQueue::new().is_empty());
    /// ```
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Borrow the front message without removing it.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::{Msg, MsgQueue, MsgType};
    ///
    /// let mut q = MsgQueue::new();
    /// q.push_back(Msg::new(42, MsgType::ReqRedisGet, true));
    /// assert_eq!(q.front().unwrap().id(), 42);
    /// ```
    #[must_use]
    pub fn front(&self) -> Option<&Msg> {
        self.inner.front()
    }

    /// Mutably borrow the front message.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::{Msg, MsgQueue, MsgType};
    ///
    /// let mut q = MsgQueue::new();
    /// q.push_back(Msg::new(1, MsgType::ReqRedisGet, true));
    /// q.front_mut().unwrap().set_swallow(true);
    /// ```
    pub fn front_mut(&mut self) -> Option<&mut Msg> {
        self.inner.front_mut()
    }

    /// Iterate over the queue in front-to-back order.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::{Msg, MsgQueue, MsgType};
    ///
    /// let mut q = MsgQueue::new();
    /// q.push_back(Msg::new(1, MsgType::ReqRedisGet, true));
    /// q.push_back(Msg::new(2, MsgType::ReqRedisSet, true));
    /// let ids: Vec<u64> = q.iter().map(|m| m.id()).collect();
    /// assert_eq!(ids, vec![1, 2]);
    /// ```
    pub fn iter(&self) -> std::collections::vec_deque::Iter<'_, Msg> {
        self.inner.iter()
    }

    /// Find a message by id and return a shared reference.
    ///
    /// This is the queue-side counterpart to the C
    /// `dict_msg_id`-shaped lookup that traverses the outstanding-msg
    /// list. Use [`crate::msg::index::MsgIndex`] when an `O(1)`
    /// lookup is required across many queues.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::{Msg, MsgQueue, MsgType};
    ///
    /// let mut q = MsgQueue::new();
    /// q.push_back(Msg::new(99, MsgType::ReqRedisGet, true));
    /// assert!(q.msg_get_id_lookup(99).is_some());
    /// assert!(q.msg_get_id_lookup(1).is_none());
    /// ```
    #[must_use]
    pub fn msg_get_id_lookup(&self, id: MsgId) -> Option<&Msg> {
        self.inner.iter().find(|m| m.id() == id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msg::{Msg, MsgType};

    #[test]
    fn fifo_order() {
        let mut q = MsgQueue::new();
        for i in 1..=3 {
            q.push_back(Msg::new(i, MsgType::ReqRedisGet, true));
        }
        assert_eq!(q.pop_front().unwrap().id(), 1);
        assert_eq!(q.pop_front().unwrap().id(), 2);
        assert_eq!(q.pop_front().unwrap().id(), 3);
        assert!(q.is_empty());
    }

    #[test]
    fn lookup_finds_by_id() {
        let mut q = MsgQueue::new();
        q.push_back(Msg::new(11, MsgType::ReqRedisGet, true));
        q.push_back(Msg::new(22, MsgType::ReqRedisSet, true));
        assert_eq!(q.msg_get_id_lookup(22).unwrap().id(), 22);
        assert!(q.msg_get_id_lookup(33).is_none());
    }
}
