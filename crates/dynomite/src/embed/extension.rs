//! Command-dispatch extension hook.
//!
//! The cluster substrate ships the parser, the dispatcher, and
//! the standard data-plane commands (GET / SET / HSET / ...).
//! Layered surfaces - notably the RediSearch FT.* commands -
//! plug in via the [`CommandExtension`] trait so the substrate
//! does not need to know about them at compile time.
//!
//! # Lifecycle
//!
//! 1. The embedder constructs a
//!    [`crate::embed::ServerBuilder`].
//! 2. The embedder (or a helper crate such as
//!    `dynomite-search`) attaches a [`CommandExtension`] via
//!    [`crate::embed::ServerBuilder::with_command_extension`]
//!    or [`crate::embed::ServerBuilder::set_command_extension`].
//! 3. The dispatcher consults the extension in the hot path:
//!    * For commands the parser tags as
//!      [`crate::msg::MsgType::ReqRedisFtCreate`] /
//!      [`crate::msg::MsgType::ReqRedisFtSearch`] /
//!      [`crate::msg::MsgType::ReqRedisFtInfo`] /
//!      [`crate::msg::MsgType::ReqRedisFtList`] /
//!      [`crate::msg::MsgType::ReqRedisFtDropindex`] /
//!      [`crate::msg::MsgType::ReqRedisFtRegex`] /
//!      [`crate::msg::MsgType::ReqRedisFtUnknown`] the
//!      dispatcher checks
//!      [`CommandExtension::handles_msg_type`] and, if true,
//!      delegates execution to
//!      [`CommandExtension::try_dispatch`].
//!    * Every HSET request is offered to
//!      [`CommandExtension::try_intercept_hset`] before the
//!      standard fan-out path runs.
//! 4. When no extension is wired the dispatcher behaves
//!    exactly as it did before this hook existed: FT.* keywords
//!    are forwarded to the local datastore (which typically
//!    rejects them with `-ERR unknown command`).
//!
//! Implementations are object-safe; the dispatcher holds an
//! [`std::sync::Arc<dyn CommandExtension>`] and clones the
//! handle freely across tasks.

use std::fmt::Debug;

use crate::msg::MsgType;

/// Outcome of [`CommandExtension::try_intercept_hset`].
///
/// The HSET interception path runs before the dispatcher's
/// routing planner. The extension can either absorb the write
/// (the standard storage write still fires; the engine just
/// got a free side-effect), reject it with a structured error
/// reply, or pass through.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum HsetOutcome {
    /// The HSET key matched a registered prefix and the
    /// extension absorbed the write side-effect. The
    /// dispatcher proceeds with the standard storage write so
    /// the underlying hash document still lands on the backend.
    Absorbed,
    /// The HSET key did not match any registered prefix.
    /// Equivalent to no extension being installed for this
    /// command.
    NotIndexed,
    /// The HSET key matched a registered prefix but the
    /// payload was malformed. The dispatcher synthesises a
    /// `-ERR <message>\r\n` reply and returns it directly to
    /// the client without writing to the backend.
    Error(String),
}

/// Pluggable command-dispatch hook.
///
/// Implementors short-circuit dispatcher routing for the
/// command families they own; everything else falls through
/// to the standard substrate. See the module-level docs for
/// the lifecycle and the standard-library hook used by
/// `dynomite-search`.
pub trait CommandExtension: Send + Sync + Debug {
    /// True when the parsed `MsgType` is one this extension
    /// wants to dispatch. The dispatcher only invokes
    /// [`Self::try_dispatch`] when this returns `true`.
    fn handles_msg_type(&self, ty: MsgType) -> bool;

    /// Try to dispatch a command. `args` is the parsed RESP
    /// argument vector starting with the command keyword
    /// (e.g. `[b"FT.SEARCH", b"idx", ...]`).
    ///
    /// Returns `Some(resp_bytes)` when the extension produced
    /// a complete RESP reply for the client; `None` to fall
    /// through to the standard dispatch path. The dispatcher
    /// only consults this method after
    /// [`Self::handles_msg_type`] returns `true`, so a
    /// well-behaved implementation may safely assume the
    /// command keyword is one of the families it advertised.
    fn try_dispatch(&self, args: &[&[u8]]) -> Option<Vec<u8>>;

    /// Inspect an HSET argument list and, if it matches a
    /// registered prefix / shape, perform any side-effects the
    /// extension wants. `args` is `[key, f1, v1, f2, v2, ...]`
    /// (without the leading `HSET` keyword).
    ///
    /// See [`HsetOutcome`] for the response shape. The default
    /// impl returns [`HsetOutcome::NotIndexed`] so trait
    /// implementors that do not care about HSET interception
    /// only need to implement [`Self::try_dispatch`].
    fn try_intercept_hset(&self, _args: &[&[u8]]) -> HsetOutcome {
        HsetOutcome::NotIndexed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct NoOp;

    impl CommandExtension for NoOp {
        fn handles_msg_type(&self, _ty: MsgType) -> bool {
            false
        }
        fn try_dispatch(&self, _args: &[&[u8]]) -> Option<Vec<u8>> {
            None
        }
    }

    #[test]
    fn default_hset_is_not_indexed() {
        let ext = NoOp;
        let outcome = ext.try_intercept_hset(&[b"key" as &[u8], b"f", b"v"]);
        assert_eq!(outcome, HsetOutcome::NotIndexed);
    }
}
