//! Cross-thread ring queues used between the worker, gossip, and stats
//! threads.
//!
//! The C engine uses two SPSC ring buffers, `C2G_InQ` and `C2G_OutQ`,
//! sized at 256 slots each. The Rust port replaces them with bounded
//! [`crossbeam_channel`] pairs of the same capacity. The `RingChannels`
//! struct owns both directions (core -> gossip and gossip -> core), so
//! callers receive a single value to wire into their thread spawning
//! code.
//!
//! # Examples
//!
//! ```
//! use dynomite::core::ring_queue::RingChannels;
//!
//! let chans: RingChannels<u32, u32> = RingChannels::new();
//! chans.in_tx.send(7).unwrap();
//! assert_eq!(chans.in_rx.recv().unwrap(), 7);
//! chans.out_tx.send(11).unwrap();
//! assert_eq!(chans.out_rx.recv().unwrap(), 11);
//! ```

use crossbeam_channel::{bounded, Receiver, Sender};

/// Maximum in-flight ring messages for the core -> gossip direction.
pub const C2G_IN_CAPACITY: usize = 256;

/// Maximum in-flight ring messages for the gossip -> core direction.
pub const C2G_OUT_CAPACITY: usize = 256;

/// A pair of bounded channels that carry the core <-> gossip ring
/// traffic.
///
/// `I` is the message type the core thread sends *to* the secondary
/// thread (typically gossip); `O` is the message type the secondary
/// thread sends *back*.
#[derive(Debug, Clone)]
pub struct RingChannels<I, O> {
    /// Sender owned by the producer of inbound messages.
    pub in_tx: Sender<I>,
    /// Receiver owned by the consumer of inbound messages.
    pub in_rx: Receiver<I>,
    /// Sender owned by the producer of outbound replies.
    pub out_tx: Sender<O>,
    /// Receiver owned by the consumer of outbound replies.
    pub out_rx: Receiver<O>,
}

impl<I, O> RingChannels<I, O> {
    /// Create a new pair sized at the C2G defaults.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::core::ring_queue::RingChannels;
    /// let _: RingChannels<(), ()> = RingChannels::new();
    /// ```
    pub fn new() -> Self {
        Self::with_capacities(C2G_IN_CAPACITY, C2G_OUT_CAPACITY)
    }

    /// Create a new pair with explicit capacities. Useful for tests
    /// and downstream stages that wire smaller queues.
    pub fn with_capacities(in_cap: usize, out_cap: usize) -> Self {
        let (in_tx, in_rx) = bounded(in_cap);
        let (out_tx, out_rx) = bounded(out_cap);
        Self {
            in_tx,
            in_rx,
            out_tx,
            out_rx,
        }
    }
}

impl<I, O> Default for RingChannels<I, O> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacities_match_c_constants() {
        assert_eq!(C2G_IN_CAPACITY, 256);
        assert_eq!(C2G_OUT_CAPACITY, 256);
    }

    #[test]
    fn fifo_ordering_is_preserved() {
        let chans: RingChannels<u32, ()> = RingChannels::new();
        for i in 0..16u32 {
            chans.in_tx.send(i).unwrap();
        }
        for i in 0..16u32 {
            assert_eq!(chans.in_rx.recv().unwrap(), i);
        }
    }

    #[test]
    fn bounded_capacity_blocks_when_full() {
        let chans: RingChannels<u32, ()> = RingChannels::with_capacities(2, 1);
        chans.in_tx.send(1).unwrap();
        chans.in_tx.send(2).unwrap();
        // The third send must hit the bound and fail under a non-blocking try.
        assert!(chans.in_tx.try_send(3).is_err());
    }
}
