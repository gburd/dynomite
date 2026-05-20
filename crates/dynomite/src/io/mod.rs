//! I/O substrate: chunked buffer pool, fixed-size SPSC ring, and a
//! transport abstraction layered on tokio.
//!
//! The submodules replace the C engine's `dyn_mbuf`, `dyn_cbuf`, and
//! the per-platform `src/event/` reactor. The reactor module defines a
//! [`Transport`](reactor::Transport) trait so that downstream stages can
//! plug in alternative wire transports (TCP today, QUIC in Stage 9)
//! without changing the connection state machine.

pub mod cbuf;
pub mod mbuf;
pub mod reactor;
