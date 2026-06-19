//! Cluster-side dispatch hook.
//!
//! Routing decisions (whether to send a request to the local
//! datastore, fan it out across racks, or relay it to a remote DC)
//! live in the [`crate::cluster`] module. The per-connection FSMs
//! own the wire path and expose a seam, [`Dispatcher`], that the
//! cluster layer plugs into.
//!
//! [`Dispatcher`] is the seam between the two layers. The
//! client / dnode-client FSMs hand each fully parsed [`Msg`] to a
//! `Dispatcher` and inspect [`DispatchOutcome`] to decide whether
//! the response can be returned synchronously or whether they
//! should wait for a downstream response.
//! [`crate::cluster::dispatch::ClusterDispatcher`] is the
//! cluster-aware implementation; [`NoopDispatcher`] is the test
//! double.
//!
//! [`Msg`]: crate::msg::Msg

use std::sync::Arc;

use tokio::sync::mpsc;

use crate::msg::Msg;

/// Outcome of dispatching a parsed message.
#[derive(Debug)]
pub enum DispatchOutcome {
    /// The dispatcher took ownership of the request and will deliver
    /// the response asynchronously (over the connection's response
    /// channel installed by the FSM).
    Pending,
    /// The dispatcher wants the FSM to reply with the supplied
    /// message immediately. Used for control plane / synthetic
    /// responses (e.g. swallowed `QUIT` commands).
    Inline(Msg),
    /// The dispatcher rejected the request with an error response
    /// the FSM should return to the client immediately.
    Error(Msg),
    /// The request must be dropped; no response will be sent. Used
    /// for swallowed / quit messages.
    Drop,
}

/// Cluster-side dispatch hook implemented by the cluster layer and
/// by tests.
///
/// The dispatcher is invoked from a tokio task; implementations may
/// do async work but should avoid blocking. The trait uses
/// `&self` so the dispatcher can be shared across many connections.
pub trait Dispatcher: Send + Sync {
    /// Hand a parsed request to the dispatcher.
    ///
    /// `responder` is a per-connection channel the dispatcher uses
    /// to deliver responses (or errors) back to the FSM that owns
    /// the originating client connection.
    fn dispatch(&self, req: Msg, responder: ServerSink) -> DispatchOutcome;
}

/// Channel the dispatcher uses to send responses back to a client
/// FSM. The FSM owns the receiving half.
pub type ServerSink = mpsc::Sender<OutboundEnvelope>;

/// Envelope wrapping a dispatcher response and the request id it
/// corresponds to.
///
/// `span` carries the originating request span back to the
/// client-side FSM so the response writeback nests under the
/// originating client span. The default is
/// [`tracing::Span::none`].
///
/// `source_peer_idx` identifies the peer this response came from
/// when the dispatcher fanned the request to multiple replicas.
/// `None` is used for synthetic / inline / single-target paths
/// where the source is unambiguous.
#[derive(Debug)]
pub struct OutboundEnvelope {
    /// Request id the response is for.
    pub req_id: crate::core::types::MsgId,
    /// The response message.
    pub rsp: Msg,
    /// Originating request span for cross-task propagation.
    pub span: tracing::Span,
    /// Index of the peer this reply was produced by, when known.
    /// Set by the per-target server / dnode-server drivers and
    /// consumed by the per-request reply coalescer.
    pub source_peer_idx: Option<u32>,
}

/// Dispatcher that drops every request and emits no response.
///
/// Useful as a placeholder in tests that only exercise framing.
#[derive(Debug, Default, Clone)]
pub struct NoopDispatcher;

impl Dispatcher for NoopDispatcher {
    fn dispatch(&self, _req: Msg, _responder: ServerSink) -> DispatchOutcome {
        DispatchOutcome::Drop
    }
}

impl<T: Dispatcher + ?Sized> Dispatcher for Arc<T> {
    fn dispatch(&self, req: Msg, responder: ServerSink) -> DispatchOutcome {
        (**self).dispatch(req, responder)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msg::MsgType;

    #[test]
    fn noop_returns_drop() {
        let (tx, _rx) = mpsc::channel(1);
        let outcome = NoopDispatcher.dispatch(Msg::new(1, MsgType::ReqRedisGet, true), tx);
        matches!(outcome, DispatchOutcome::Drop);
    }
}
