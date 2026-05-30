//! Explicit token-range handoff between peers.
//!
//! When ownership of a slice of the consistent-hashing ring
//! transfers from one peer to another (a peer joins, leaves, or
//! reconfigures its token list), the `dyn_riak::aae` exchange
//! eventually reconciles the two replicas. That is fine for a
//! steady-state cluster, but it gives operators no deterministic
//! handle on "when is the rebalance done?": the AAE scheduler
//! sweeps in the background and may take many cycles to converge.
//!
//! This module ports the explicit handoff flow modeled on Riak
//! Core's `riak_core_handoff`: a chunked, throttled, checkpointed
//! stream from the *previous* owner of a token range to the
//! *new* owner, with explicit start, accept, ack, and finalize
//! events. The receiver can resume from the last acknowledged
//! chunk after a transient peer error, and the sender bounds
//! both the number of in-flight chunks (backpressure) and the
//! per-second chunk rate (throttle) so the handoff does not
//! starve client traffic.
//!
//! # State graph
//!
//! ```text
//! Init --send_request_received--> Negotiating
//!                                 |--accepted--> Sending
//!                                 |--rejected--> Failed
//!
//! Sending --chunk_acked--> Sending
//!         --batch_done--> Flushing
//!         --peer_error / event_timeout--> Failed
//!
//! Flushing --ack_received--> Finalizing
//!          --state_timeout / peer_error--> Failed
//!
//! Finalizing --finalize_acked--> stop ok
//!            --state_timeout / peer_error--> Failed
//! ```
//!
//! # Submodules
//!
//! * [`fsm`] -- the handoff coordinator [`gen_fsm::FsmHandler`].
//!
//! Production wirings hand the [`fsm::HandoffHandler`] to a
//! [`gen_fsm::FsmDriver`] and feed it events from the wire path
//! that decodes [`dynomite::proto::dnode::DmsgType::HandoffChunk`]
//! frames.

pub mod fsm;

pub use crate::handoff::fsm::{
    Chunk, Event, HandoffHandler, HandoffOutcome, SendRequest, State, TokenCursor,
    DEFAULT_CHUNKS_PER_SEC, DEFAULT_CHUNK_SIZE, DEFAULT_MAX_IN_FLIGHT, FINALIZING_STATE_TIMEOUT,
    FLUSHING_STATE_TIMEOUT, NEGOTIATING_STATE_TIMEOUT, SENDING_EVENT_TIMEOUT,
};
