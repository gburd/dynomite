//! I/O substrate: chunked buffer pool, fixed-size SPSC ring, and a
//! transport abstraction layered on tokio.
//!
//! The submodules provide the chunked buffer pool, the SPSC ring,
//! and the reactor. The reactor module defines a
//! [`Transport`](reactor::Transport) trait so that callers can plug
//! in alternative wire transports (TCP and QUIC both ship) without
//! changing the connection state machine.

pub mod cbuf;
pub mod mbuf;
pub mod reactor;
