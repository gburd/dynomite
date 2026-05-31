//! Explicit handoff coordinator FSM.
//!
//! The handoff coordinator drives one (`src_peer`, `dst_peer`,
//! `token_range`) handoff session through five protocol states:
//! [`State::Init`] -> [`State::Negotiating`] -> [`State::Sending`]
//! -> [`State::Flushing`] -> [`State::Finalizing`], with a
//! terminal [`State::Failed`] reachable from every state on a
//! [`Event::PeerError`] or per-state timeout.
//!
//! The FSM is intentionally I/O-free: it owns no socket, no
//! datastore, and no clock other than what
//! [`gen_fsm::FsmDriver`] injects through the per-state
//! [`gen_fsm::Action::SetStateTimeout`] /
//! [`gen_fsm::Action::SetEventTimeout`] actions. Production
//! wirings spawn an I/O orchestrator that translates each
//! state-entry into a wire request (encoded as a
//! [`dynomite::proto::dnode::DmsgType::HandoffChunk`] frame),
//! collects the peer's response, and casts the matching
//! [`Event`] back into the FSM. Tests drive the same handler
//! synchronously without a runtime.
//!
//! # Backpressure and throttling
//!
//! The handler holds two admission gates that the orchestrator
//! consults before serialising the next chunk:
//!
//! * [`HandoffHandler::has_in_flight_capacity`] - returns true
//!   while `sent_chunks - acked_chunks < max_in_flight`.
//! * [`HandoffHandler::try_admit_chunk`] - tries to take one
//!   token from the [`throttle_core::Throttle`] bucket. The
//!   default rate is [`DEFAULT_CHUNKS_PER_SEC`] (100 chunks/s)
//!   with the same number as a one-second burst capacity.
//!
//! Both gates are pure (no async); the orchestrator polls them
//! and posts an internal [`Event::NextChunkBuilt`] only when
//! both succeed.
//!
//! # Idempotency
//!
//! Every chunk leaves the sender with a strictly-monotonic
//! `chunk_id`. Acks are tolerated out of order: each
//! [`Event::ChunkAcked`] increments `acked_chunks` by one
//! regardless of which `chunk_id` was acked. The handler
//! deduplicates obvious double-acks by tracking the highest
//! seen `chunk_id` and rejecting any ack with `chunk_id`
//! greater than `sent_chunks - 1`, which would otherwise allow
//! `acked_chunks > sent_chunks`.
//!
//! # Partial-failure recovery
//!
//! On entry to [`State::Failed`] the handler reports
//! [`HandoffOutcome::Failed::partial_count`] equal to the
//! number of *acknowledged* keys (acked chunks times the
//! configured chunk size). The operator can then resume the
//! handoff by constructing a fresh [`HandoffHandler`] with the
//! [`TokenCursor`] advanced past the acked range; the new
//! sender starts at the failure point rather than re-streaming
//! the entire range.

use std::time::{Duration, Instant};

use dynomite::events::{PeerId, TokenRange};
use dynomite::hashkit::DynToken;
use gen_fsm::{Action, EventType, FsmHandler, TimeoutKind, Transition};
use throttle_core::{SystemClock, Throttle};

/// Default keys per chunk. The legacy AAE exchange uses 1024 as
/// well; keeping the default in step minimises operator
/// surprise and lets early benches share the same baseline.
pub const DEFAULT_CHUNK_SIZE: u64 = 1024;

/// Default number of chunks the sender allows in flight before
/// it pauses for an ack. Bounded so a slow receiver cannot
/// inflate the sender's outbound queue without bound.
pub const DEFAULT_MAX_IN_FLIGHT: u64 = 4;

/// Default chunks-per-second admitted by the rate-limit
/// throttle. 100 chunks/s at the default chunk size of 1024
/// keys gives ~100k keys/s of handoff throughput, well below
/// even a modest cluster's client-traffic budget.
pub const DEFAULT_CHUNKS_PER_SEC: u64 = 100;

/// State timeout armed on entry to [`State::Negotiating`].
pub const NEGOTIATING_STATE_TIMEOUT: Duration = Duration::from_secs(10);

/// Event timeout armed on every chunk dispatch in
/// [`State::Sending`]. Cancelled by *any* arriving event,
/// matching the gen_statem `event_timeout` semantics.
pub const SENDING_EVENT_TIMEOUT: Duration = Duration::from_secs(30);

/// State timeout armed on entry to [`State::Flushing`].
pub const FLUSHING_STATE_TIMEOUT: Duration = Duration::from_secs(60);

/// State timeout armed on entry to [`State::Finalizing`].
pub const FINALIZING_STATE_TIMEOUT: Duration = Duration::from_secs(30);

/// Protocol states for the handoff coordinator.
///
/// Every non-terminal state arms a per-state or per-event
/// timeout on entry. See [`NEGOTIATING_STATE_TIMEOUT`],
/// [`SENDING_EVENT_TIMEOUT`], [`FLUSHING_STATE_TIMEOUT`], and
/// [`FINALIZING_STATE_TIMEOUT`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum State {
    /// Initial state. The sender has been constructed but has
    /// not yet announced itself to the receiver. A
    /// [`Event::SendRequestReceived`] advances to
    /// [`State::Negotiating`].
    Init,
    /// The sender has emitted its `SendRequest` and is waiting
    /// for the receiver's accept / reject decision.
    Negotiating,
    /// The receiver accepted the handoff. Chunks stream out
    /// while the throttle and in-flight gates allow.
    Sending,
    /// All chunks for the current batch have been dispatched;
    /// the sender is waiting for the receiver to drain its
    /// inbound queue and ack the batch as a whole.
    Flushing,
    /// The receiver has acked every chunk; the sender is
    /// waiting for the receiver's `Finalize` ack so it can
    /// release ownership of the token range.
    Finalizing,
    /// Terminal failure state. Entry stops the FSM with a
    /// [`HandoffOutcome::Failed`] outcome.
    Failed,
}

/// Cursor tracking the sender's progress through the source
/// token range.
///
/// The cursor is modelled as a pair of [`DynToken`] values plus
/// a count of keys already drained. The orchestrator advances
/// `keys_drained` after each chunk is built; the FSM consults
/// the cursor only to decide whether more chunks remain and to
/// compute the partial-failure count on a peer error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenCursor {
    /// The first token of the source range.
    pub start: DynToken,
    /// The exclusive upper bound of the source range.
    pub end: DynToken,
    /// Number of keys read out of the source range so far.
    pub keys_drained: u64,
    /// Total number of keys the sender expects to transfer
    /// (filled in by the orchestrator after the source range
    /// has been counted; `0` until then).
    pub total_keys: u64,
}

impl TokenCursor {
    /// Build a fresh cursor for a token range.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::events::TokenRange;
    /// use dynomite::hashkit::DynToken;
    /// use dyniak::handoff::TokenCursor;
    ///
    /// let range = TokenRange::new(DynToken::from_u32(0), DynToken::from_u32(1024));
    /// let cur = TokenCursor::new(&range, 1024);
    /// assert_eq!(cur.total_keys, 1024);
    /// assert_eq!(cur.keys_drained, 0);
    /// ```
    #[must_use]
    pub fn new(range: &TokenRange, total_keys: u64) -> Self {
        Self {
            start: range.start().clone(),
            end: range.end().clone(),
            keys_drained: 0,
            total_keys,
        }
    }

    /// True when no more keys remain in the source range.
    #[must_use]
    pub const fn is_drained(&self) -> bool {
        self.keys_drained >= self.total_keys
    }
}

/// Receiver-bound announcement that a sender has handoff data
/// ready for `token_range`.
///
/// The wire-side decoder strips this off the
/// [`dynomite::proto::dnode::DmsgType::HandoffChunk`] frame and
/// posts it as a [`Event::SendRequestReceived`]. The sender
/// waits for the receiver's [`Event::NegotiationAck`] before
/// streaming any chunks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SendRequest {
    /// Source peer that owns the chunks. Mirrors
    /// [`HandoffHandler::src_peer`] for receiver-side
    /// validation.
    pub src_peer: PeerId,
    /// Destination peer. Mirrors [`HandoffHandler::dst_peer`].
    pub dst_peer: PeerId,
    /// Token range the handoff is covering.
    pub token_range: TokenRange,
    /// Total number of keys the sender intends to transfer.
    /// Used by the receiver for capacity planning and by the
    /// FSM to compute completion progress.
    pub total_keys: u64,
}

/// One handoff chunk built by the source orchestrator.
///
/// The FSM stores only the metadata needed to track ack
/// progress; the raw key/value bytes live in the orchestrator's
/// outbound queue. Tests construct [`Chunk`] values directly to
/// drive [`Event::NextChunkBuilt`] transitions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Chunk {
    /// Strictly-monotonic chunk id assigned by the sender.
    pub chunk_id: u64,
    /// Number of keys packed into this chunk. Always less than
    /// or equal to [`HandoffHandler::chunk_size`].
    pub keys: u64,
}

/// Events accepted by [`HandoffHandler::handle`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// Receiver-side: the inbound `SendRequest` arrived and is
    /// being routed to the local handoff coordinator.
    SendRequestReceived(SendRequest),
    /// Sender-side: receiver replied to the `SendRequest` with
    /// either an accept or a reject decision.
    NegotiationAck {
        /// True when the receiver accepted. Any false value
        /// transitions the FSM to [`State::Failed`].
        accepted: bool,
        /// Maximum chunk size the receiver is willing to
        /// accept. The sender must not emit chunks larger
        /// than this value; the FSM clamps
        /// [`HandoffHandler::chunk_size`] on accept.
        max_chunk_size: u64,
    },
    /// Sender-side: the orchestrator has serialised the next
    /// chunk and is asking the FSM to record it as in-flight.
    NextChunkBuilt(Chunk),
    /// Sender-side: receiver acked one chunk. Acks may arrive
    /// out of order; the FSM only checks that `chunk_id` is in
    /// the range `[0, sent_chunks)` and increments the counter
    /// once. Repeated acks for the same `chunk_id` are
    /// silently ignored.
    ChunkAcked {
        /// Identifier of the chunk being acknowledged.
        chunk_id: u64,
    },
    /// Sender-side: the orchestrator has no more chunks to
    /// produce (either the cursor is drained or the operator
    /// requested a flush). Advances to [`State::Flushing`].
    BatchDone,
    /// Receiver-side: the receiver has confirmed it has
    /// durably accepted every chunk and is ready to assume
    /// ownership. Advances [`State::Flushing`] to
    /// [`State::Finalizing`].
    BatchAcked,
    /// Sender-side: the receiver acknowledged the finalize.
    /// Stops the FSM with a [`HandoffOutcome::Completed`].
    FinalizeAcked,
    /// Any state: the peer reported a transport-level or
    /// application-level error. Transitions to
    /// [`State::Failed`] regardless of the current state.
    PeerError(String),
}

/// Final outcome reported when the FSM stops.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HandoffOutcome {
    /// The handoff streamed every chunk, the receiver
    /// finalised, and the sender released ownership cleanly.
    Completed {
        /// Number of keys delivered. Equal to
        /// `acked_chunks * chunk_size` clamped to the source
        /// range's `total_keys`.
        keys_transferred: u64,
        /// Wall-clock duration from [`HandoffHandler::new`] to
        /// the final ack.
        duration: Duration,
    },
    /// The handoff aborted in some non-terminal state.
    Failed {
        /// Free-form cause string. Either the peer-side error
        /// payload, `"negotiation rejected"`, or
        /// `"<state> timeout"` for a state-timer fire.
        reason: String,
        /// Number of keys that the receiver durably acked
        /// before the failure. The operator can resume from
        /// this offset on retry.
        partial_count: u64,
        /// State the FSM was in when the failure was raised.
        last_state: State,
    },
}

/// Coordinator FSM owning one handoff session.
///
/// The handler is constructed with the (sender, receiver,
/// range) triple and an optional [`Throttle`]. It tracks the
/// in-flight cursor and the ack progress; the production wire
/// path drives it through [`gen_fsm::FsmDriver`].
pub struct HandoffHandler {
    src_peer: PeerId,
    dst_peer: PeerId,
    token_range: TokenRange,
    chunk_size: u64,
    max_in_flight: u64,
    throttle: Throttle<SystemClock>,
    cursor: TokenCursor,
    sent_chunks: u64,
    acked_chunks: u64,
    /// Tracks chunk ids we have already acked so a repeated
    /// [`Event::ChunkAcked`] does not double-count. Kept as a
    /// sorted `Vec` because the typical in-flight window is
    /// small (default 4).
    seen_acks: Vec<u64>,
    started_at: Instant,
    last_error: Option<String>,
    last_state: State,
}

impl HandoffHandler {
    /// Build a handler with default chunking, in-flight, and
    /// throttle settings.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::events::TokenRange;
    /// use dynomite::hashkit::DynToken;
    /// use dyniak::handoff::HandoffHandler;
    ///
    /// let range = TokenRange::new(
    ///     DynToken::from_u32(0),
    ///     DynToken::from_u32(1024),
    /// );
    /// let h = HandoffHandler::new(1, 2, range, 4096);
    /// assert_eq!(h.src_peer(), 1);
    /// assert_eq!(h.dst_peer(), 2);
    /// ```
    #[must_use]
    pub fn new(
        src_peer: PeerId,
        dst_peer: PeerId,
        token_range: TokenRange,
        total_keys: u64,
    ) -> Self {
        Self::with_settings(
            src_peer,
            dst_peer,
            token_range,
            total_keys,
            DEFAULT_CHUNK_SIZE,
            DEFAULT_MAX_IN_FLIGHT,
            DEFAULT_CHUNKS_PER_SEC,
        )
    }

    /// Build a handler with explicit chunking, in-flight, and
    /// throttle settings. `chunk_size` must be at least one;
    /// values of zero are clamped up to one to keep the chunk
    /// arithmetic monotonic.
    #[must_use]
    pub fn with_settings(
        src_peer: PeerId,
        dst_peer: PeerId,
        token_range: TokenRange,
        total_keys: u64,
        chunk_size: u64,
        max_in_flight: u64,
        chunks_per_sec: u64,
    ) -> Self {
        let chunk_size = chunk_size.max(1);
        let max_in_flight = max_in_flight.max(1);
        let burst = chunks_per_sec.max(1);
        let cursor = TokenCursor::new(&token_range, total_keys);
        Self {
            src_peer,
            dst_peer,
            token_range,
            chunk_size,
            max_in_flight,
            throttle: Throttle::new(burst, chunks_per_sec),
            cursor,
            sent_chunks: 0,
            acked_chunks: 0,
            seen_acks: Vec::new(),
            started_at: Instant::now(),
            last_error: None,
            last_state: State::Init,
        }
    }

    /// Source peer identifier. Read-only; the FSM never
    /// mutates the (src, dst) pair.
    #[must_use]
    pub const fn src_peer(&self) -> PeerId {
        self.src_peer
    }

    /// Destination peer identifier.
    #[must_use]
    pub const fn dst_peer(&self) -> PeerId {
        self.dst_peer
    }

    /// Token range the handoff is covering.
    #[must_use]
    pub const fn token_range(&self) -> &TokenRange {
        &self.token_range
    }

    /// Configured chunk size in keys. Reflects the latest
    /// negotiated value after [`Event::NegotiationAck`].
    #[must_use]
    pub const fn chunk_size(&self) -> u64 {
        self.chunk_size
    }

    /// Configured maximum number of in-flight chunks.
    #[must_use]
    pub const fn max_in_flight(&self) -> u64 {
        self.max_in_flight
    }

    /// Number of chunks the sender has put on the wire so far.
    #[must_use]
    pub const fn sent_chunks(&self) -> u64 {
        self.sent_chunks
    }

    /// Number of chunks the receiver has acked.
    #[must_use]
    pub const fn acked_chunks(&self) -> u64 {
        self.acked_chunks
    }

    /// Snapshot of the per-range cursor.
    #[must_use]
    pub const fn cursor(&self) -> &TokenCursor {
        &self.cursor
    }

    /// True when the in-flight window has slack for one more
    /// chunk. Orchestrators check this before serialising the
    /// next chunk to enforce backpressure.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::events::TokenRange;
    /// use dynomite::hashkit::DynToken;
    /// use dyniak::handoff::HandoffHandler;
    ///
    /// let range = TokenRange::new(
    ///     DynToken::from_u32(0),
    ///     DynToken::from_u32(8),
    /// );
    /// let h = HandoffHandler::new(0, 1, range, 8);
    /// assert!(h.has_in_flight_capacity());
    /// ```
    #[must_use]
    pub fn has_in_flight_capacity(&self) -> bool {
        self.sent_chunks.saturating_sub(self.acked_chunks) < self.max_in_flight
    }

    /// Try to take one token from the rate-limit throttle.
    /// Returns true on success.
    pub fn try_admit_chunk(&self) -> bool {
        self.throttle.try_acquire(1)
    }

    /// Number of keys the receiver has durably acked. Equal to
    /// `acked_chunks * chunk_size` clamped at the source
    /// range's `total_keys`.
    #[must_use]
    pub fn acked_keys(&self) -> u64 {
        let raw = self.acked_chunks.saturating_mul(self.chunk_size);
        let total = self.cursor.total_keys;
        if total == 0 {
            raw
        } else {
            raw.min(total)
        }
    }

    /// Wall-clock duration since [`Self::new`].
    #[must_use]
    pub fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }

    /// Last `State` the FSM was in. Updated on every state
    /// transition; surfaced through [`HandoffOutcome::Failed`]
    /// when the FSM enters [`State::Failed`].
    #[must_use]
    pub const fn last_state(&self) -> State {
        self.last_state
    }

    fn record_state(&mut self, state: State) {
        self.last_state = state;
    }
}

impl FsmHandler for HandoffHandler {
    type State = State;
    type Event = Event;
    type Reply = ();
    type Stop = HandoffOutcome;

    fn initial(&self) -> State {
        State::Init
    }

    fn on_enter(&mut self, state: State) -> Transition<Self> {
        // Record the new state for diagnostic surfaces. We do
        // not overwrite when entering Failed: callers want to
        // see the state that triggered the failure, not the
        // terminal Failed marker.
        if state != State::Failed {
            self.record_state(state);
        }
        match state {
            State::Init => Transition::Keep(vec![]),
            State::Negotiating => {
                Transition::Keep(vec![Action::set_state_timeout(NEGOTIATING_STATE_TIMEOUT)])
            }
            State::Sending => {
                // Arm the per-event timeout so a stalled peer
                // (no acks within 30s) trips the failed
                // transition. The timeout is cancelled on every
                // arriving event by the gen_fsm driver.
                Transition::Keep(vec![Action::set_event_timeout(SENDING_EVENT_TIMEOUT)])
            }
            State::Flushing => {
                Transition::Keep(vec![Action::set_state_timeout(FLUSHING_STATE_TIMEOUT)])
            }
            State::Finalizing => {
                Transition::Keep(vec![Action::set_state_timeout(FINALIZING_STATE_TIMEOUT)])
            }
            State::Failed => Transition::Stop(HandoffOutcome::Failed {
                reason: self
                    .last_error
                    .clone()
                    .unwrap_or_else(|| "handoff failed".to_string()),
                partial_count: self.acked_keys(),
                last_state: self.last_state,
            }),
        }
    }

    fn handle(&mut self, state: State, _et: EventType, ev: Event) -> Transition<Self> {
        // Record the state in which the event was delivered
        // so a subsequent failure surfaces a useful
        // last_state, even if the FSM transitions out before
        // the next on_enter fires.
        self.record_state(state);
        // PeerError at any non-terminal state -> Failed.
        if let Event::PeerError(msg) = ev.clone() {
            self.last_error = Some(msg);
            return Transition::Next(State::Failed, vec![]);
        }
        match (state, ev) {
            // Init -> Negotiating on the receiver-bound
            // SendRequest. The handler validates the
            // (src, dst, range) triple matches its own and
            // jumps to Failed if not.
            (State::Init, Event::SendRequestReceived(req)) => {
                if req.src_peer != self.src_peer
                    || req.dst_peer != self.dst_peer
                    || req.token_range != self.token_range
                {
                    self.last_error = Some(format!(
                        "send request mismatch: got src={} dst={} expected src={} dst={}",
                        req.src_peer, req.dst_peer, self.src_peer, self.dst_peer,
                    ));
                    return Transition::Next(State::Failed, vec![]);
                }
                if req.total_keys != self.cursor.total_keys {
                    self.cursor.total_keys = req.total_keys;
                }
                Transition::Next(State::Negotiating, vec![])
            }
            // Negotiating -> Sending on accept, -> Failed on
            // reject. The receiver's negotiated max chunk
            // size clamps the local chunk_size for the rest
            // of the run.
            (
                State::Negotiating,
                Event::NegotiationAck {
                    accepted,
                    max_chunk_size,
                },
            ) => {
                if !accepted {
                    self.last_error = Some("negotiation rejected".to_string());
                    return Transition::Next(State::Failed, vec![]);
                }
                if max_chunk_size > 0 && max_chunk_size < self.chunk_size {
                    self.chunk_size = max_chunk_size;
                }
                Transition::Next(State::Sending, vec![])
            }
            // Sending: chunk built -> bump sent_chunks. The
            // orchestrator is expected to have already taken
            // a throttle token via try_admit_chunk and
            // confirmed in-flight slack via
            // has_in_flight_capacity.
            (State::Sending, Event::NextChunkBuilt(chunk)) => {
                self.sent_chunks = self.sent_chunks.saturating_add(1);
                self.cursor.keys_drained = self.cursor.keys_drained.saturating_add(chunk.keys);
                // Re-arm the event timeout so the next 30s
                // window starts now.
                Transition::Keep(vec![Action::set_event_timeout(SENDING_EVENT_TIMEOUT)])
            }
            (State::Sending, Event::ChunkAcked { chunk_id }) => {
                if chunk_id >= self.sent_chunks {
                    // Ack for a chunk we never sent. Treat as
                    // a peer-side error to fail loudly.
                    self.last_error = Some(format!(
                        "ack for unknown chunk_id={chunk_id} (sent={})",
                        self.sent_chunks
                    ));
                    return Transition::Next(State::Failed, vec![]);
                }
                if self.seen_acks.binary_search(&chunk_id).is_ok() {
                    // Idempotent: ignore double-acks.
                    return Transition::Keep(vec![Action::set_event_timeout(
                        SENDING_EVENT_TIMEOUT,
                    )]);
                }
                let pos = self
                    .seen_acks
                    .binary_search(&chunk_id)
                    .unwrap_or_else(|p| p);
                self.seen_acks.insert(pos, chunk_id);
                self.acked_chunks = self.acked_chunks.saturating_add(1);
                Transition::Keep(vec![Action::set_event_timeout(SENDING_EVENT_TIMEOUT)])
            }
            (State::Sending, Event::BatchDone) => Transition::Next(State::Flushing, vec![]),
            // Flushing: receiver acks the whole batch -> jump
            // to Finalizing.
            (State::Flushing, Event::BatchAcked) => Transition::Next(State::Finalizing, vec![]),
            // Late chunk acks during flushing are tolerated:
            // the same idempotency rules apply.
            (State::Flushing, Event::ChunkAcked { chunk_id }) => {
                if chunk_id < self.sent_chunks && self.seen_acks.binary_search(&chunk_id).is_err() {
                    let pos = self
                        .seen_acks
                        .binary_search(&chunk_id)
                        .unwrap_or_else(|p| p);
                    self.seen_acks.insert(pos, chunk_id);
                    self.acked_chunks = self.acked_chunks.saturating_add(1);
                }
                Transition::Keep(vec![])
            }
            // Finalizing: finalize ack -> stop ok.
            (State::Finalizing, Event::FinalizeAcked) => {
                let outcome = HandoffOutcome::Completed {
                    keys_transferred: self.acked_keys(),
                    duration: self.elapsed(),
                };
                Transition::Stop(outcome)
            }
            // Stale or out-of-state events are silently
            // dropped: per-state timeouts catch a genuinely
            // stuck FSM.
            _ => Transition::Keep(vec![]),
        }
    }

    fn on_timeout(&mut self, state: State, kind: TimeoutKind) -> Transition<Self> {
        let label = match kind {
            TimeoutKind::State => "state timeout",
            TimeoutKind::Event => "event timeout",
            TimeoutKind::Generic(name) => name,
        };
        self.last_error = Some(format!("{label} in {state:?}"));
        self.record_state(state);
        Transition::Next(State::Failed, vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range() -> TokenRange {
        TokenRange::new(DynToken::from_u32(0), DynToken::from_u32(8192))
    }

    fn handler(total: u64) -> HandoffHandler {
        HandoffHandler::with_settings(7, 11, range(), total, 64, 4, 1_000_000)
    }

    fn assert_set_state(transition: &Transition<HandoffHandler>, expected: Duration) {
        match transition {
            Transition::Keep(actions) | Transition::Next(_, actions) => {
                let found = actions
                    .iter()
                    .any(|a| matches!(a, Action::SetStateTimeout(d) if *d == expected));
                assert!(
                    found,
                    "expected SetStateTimeout({expected:?}); actions = {actions:?}"
                );
            }
            Transition::Stop(_) => panic!("expected Keep/Next, got Stop"),
        }
    }

    fn assert_set_event(transition: &Transition<HandoffHandler>, expected: Duration) {
        match transition {
            Transition::Keep(actions) | Transition::Next(_, actions) => {
                let found = actions
                    .iter()
                    .any(|a| matches!(a, Action::SetEventTimeout(d) if *d == expected));
                assert!(
                    found,
                    "expected SetEventTimeout({expected:?}); actions = {actions:?}"
                );
            }
            Transition::Stop(_) => panic!("expected Keep/Next, got Stop"),
        }
    }

    #[test]
    fn negotiating_entry_arms_state_timeout() {
        let mut h = handler(8);
        let t = h.on_enter(State::Negotiating);
        assert_set_state(&t, NEGOTIATING_STATE_TIMEOUT);
    }

    #[test]
    fn sending_entry_arms_event_timeout() {
        let mut h = handler(8);
        let t = h.on_enter(State::Sending);
        assert_set_event(&t, SENDING_EVENT_TIMEOUT);
    }

    #[test]
    fn flushing_entry_arms_state_timeout() {
        let mut h = handler(8);
        let t = h.on_enter(State::Flushing);
        assert_set_state(&t, FLUSHING_STATE_TIMEOUT);
    }

    #[test]
    fn finalizing_entry_arms_state_timeout() {
        let mut h = handler(8);
        let t = h.on_enter(State::Finalizing);
        assert_set_state(&t, FINALIZING_STATE_TIMEOUT);
    }

    #[test]
    fn negotiation_accepted_clamps_chunk_size() {
        let mut h = handler(1024);
        let t = h.handle(
            State::Negotiating,
            EventType::Cast,
            Event::NegotiationAck {
                accepted: true,
                max_chunk_size: 16,
            },
        );
        match t {
            Transition::Next(State::Sending, _) => {}
            other => panic!("expected Next(Sending), got {other:?}"),
        }
        assert_eq!(h.chunk_size(), 16);
    }

    #[test]
    fn double_ack_does_not_double_count() {
        let mut h = handler(64);
        let _ = h.handle(
            State::Sending,
            EventType::Cast,
            Event::NextChunkBuilt(Chunk {
                chunk_id: 0,
                keys: 64,
            }),
        );
        let _ = h.handle(
            State::Sending,
            EventType::Cast,
            Event::ChunkAcked { chunk_id: 0 },
        );
        let _ = h.handle(
            State::Sending,
            EventType::Cast,
            Event::ChunkAcked { chunk_id: 0 },
        );
        assert_eq!(h.acked_chunks(), 1);
    }

    #[test]
    fn ack_for_unknown_chunk_advances_to_failed() {
        let mut h = handler(64);
        let t = h.handle(
            State::Sending,
            EventType::Cast,
            Event::ChunkAcked { chunk_id: 99 },
        );
        match t {
            Transition::Next(State::Failed, _) => {}
            other => panic!("expected Next(Failed), got {other:?}"),
        }
    }
}
