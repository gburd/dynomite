//! Per-DC response aggregation and quorum decisions.
//!
//! When the engine forwards a request to multiple peer replicas, the
//! arriving responses are routed through a [`ResponseMgr`] that
//! tracks how many replies are good, how many errored, and whether
//! the body checksums agree. The reference engine's
//! `dyn_response_mgr.c` keeps a fixed-size array sized for
//! [`MAX_REPLICAS_PER_DC`] replicas; the Rust port mirrors the same
//! layout because the consistency model is fundamentally a small,
//! finite-state decision table.
//!
//! The state machine is the union of two observations:
//!
//! 1. quorum size is `max_responses / 2 + 1`, matching the C
//!    formula in `init_response_mgr`;
//! 2. once at least `quorum_responses` good replies have arrived,
//!    the manager checks whether their body checksums agree and
//!    declares the request done.

use crate::core::types::MsgId;

use super::message::Msg;
use super::msg_type::MsgType;

/// Maximum replicas per datacenter the engine tracks. Matches
/// `MAX_REPLICAS_PER_DC` from the C reference.
pub const MAX_REPLICAS_PER_DC: usize = 3;

/// One accepted response together with its payload checksum.
#[derive(Debug)]
struct GoodResponse {
    msg: Box<Msg>,
    checksum: u32,
}

/// Decision the manager has reached about an outstanding request.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum QuorumOutcome {
    /// Still waiting for more responses.
    Pending,
    /// Quorum was achieved (a majority agree on the body checksum).
    Achieved,
    /// Enough responses arrived but they disagree on the body and no
    /// further responses are pending; the dispatcher must reconcile.
    Mismatched,
    /// Quorum is impossible: too many error responses.
    Failed,
}

/// Per-DC response aggregator.
///
/// The manager is bound to a single request and caps the reply count
/// at the rack count of its datacenter.
#[derive(Debug)]
pub struct ResponseMgr {
    is_read: bool,
    max_responses: u8,
    quorum_responses: u8,
    error_responses: u8,
    good: Vec<GoodResponse>,
    err_rsp: Option<Box<Msg>>,
    msg_id: MsgId,
    msg_type: MsgType,
    dc_name: Option<String>,
}

impl ResponseMgr {
    /// Build a manager bound to `req` that expects at most
    /// `max_responses` replies.
    ///
    /// `max_responses` must be in the range `1..=MAX_REPLICAS_PER_DC`;
    /// the reference engine derives it from the rack count of the
    /// target datacenter and panics on out-of-range values, which the
    /// Rust port surfaces as a debug assertion.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::{Msg, MsgType, ResponseMgr};
    ///
    /// let req = Msg::new(1, MsgType::ReqRedisGet, true);
    /// let mgr = ResponseMgr::new(&req, 3, None);
    /// assert_eq!(mgr.max_responses(), 3);
    /// assert_eq!(mgr.quorum_responses(), 2);
    /// ```
    #[must_use]
    pub fn new(req: &Msg, max_responses: u8, dc_name: Option<String>) -> Self {
        debug_assert!(max_responses >= 1);
        debug_assert!(usize::from(max_responses) <= MAX_REPLICAS_PER_DC);
        let max_responses = max_responses.max(1);
        Self {
            is_read: req.flags().is_read,
            max_responses,
            quorum_responses: max_responses / 2 + 1,
            error_responses: 0,
            good: Vec::with_capacity(MAX_REPLICAS_PER_DC),
            err_rsp: None,
            msg_id: req.id(),
            msg_type: req.ty(),
            dc_name,
        }
    }

    /// True when the manager is bound to a read request.
    ///
    /// # Examples
    /// ```
    /// use dynomite::msg::{Msg, MsgType, ResponseMgr};
    /// let req = Msg::new(1, MsgType::ReqRedisGet, true);
    /// assert!(ResponseMgr::new(&req, 1, None).is_read());
    /// ```
    #[must_use]
    pub fn is_read(&self) -> bool {
        self.is_read
    }

    /// Highest response count this manager will ever accept.
    #[must_use]
    pub fn max_responses(&self) -> u8 {
        self.max_responses
    }

    /// Number of good responses required before quorum can be
    /// declared.
    #[must_use]
    pub fn quorum_responses(&self) -> u8 {
        self.quorum_responses
    }

    /// Number of accepted (non-error) responses received so far.
    #[must_use]
    pub fn good_responses(&self) -> u8 {
        u8::try_from(self.good.len()).unwrap_or(u8::MAX)
    }

    /// Number of error responses received so far.
    #[must_use]
    pub fn error_responses(&self) -> u8 {
        self.error_responses
    }

    /// Number of replies still expected before any decision.
    #[must_use]
    pub fn pending_responses(&self) -> u8 {
        self.max_responses
            .saturating_sub(self.good_responses())
            .saturating_sub(self.error_responses)
    }

    /// Datacenter label this manager was created for.
    #[must_use]
    pub fn dc_name(&self) -> Option<&str> {
        self.dc_name.as_deref()
    }

    /// Id of the request this manager is bound to.
    #[must_use]
    pub fn msg_id(&self) -> MsgId {
        self.msg_id
    }

    /// Type tag of the request this manager is bound to.
    #[must_use]
    pub fn msg_type(&self) -> MsgType {
        self.msg_type
    }

    /// Submit `rsp` paired with its body checksum `checksum`.
    ///
    /// Errors are tallied separately; the first error response is
    /// retained as the canonical error to propagate when no quorum
    /// is possible. Good responses past `MAX_REPLICAS_PER_DC` are
    /// dropped to mirror the fixed-size array in the reference
    /// engine.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::{Msg, MsgType, ResponseMgr};
    ///
    /// let req = Msg::new(1, MsgType::ReqRedisGet, true);
    /// let mut mgr = ResponseMgr::new(&req, 1, None);
    /// let rsp = Msg::new(2, MsgType::RspRedisStatus, false);
    /// mgr.submit_response(rsp, 0xdead_beef);
    /// assert_eq!(mgr.good_responses(), 1);
    /// ```
    pub fn submit_response(&mut self, rsp: Msg, checksum: u32) {
        if rsp.flags().is_error {
            self.error_responses = self.error_responses.saturating_add(1);
            if self.err_rsp.is_none() {
                self.err_rsp = Some(Box::new(rsp));
            }
            return;
        }
        if self.good.len() < MAX_REPLICAS_PER_DC {
            self.good.push(GoodResponse {
                msg: Box::new(rsp),
                checksum,
            });
        }
    }

    /// Determine whether the manager has reached a final decision
    /// and, if so, what kind. Mirrors `rspmgr_check_is_done` plus
    /// `rspmgr_is_quorum_achieved`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::{
    ///     Msg, MsgType, QuorumOutcome, ResponseMgr,
    /// };
    ///
    /// let req = Msg::new(1, MsgType::ReqRedisGet, true);
    /// let mut mgr = ResponseMgr::new(&req, 1, None);
    /// assert_eq!(mgr.outcome(), QuorumOutcome::Pending);
    /// mgr.submit_response(Msg::new(2, MsgType::RspRedisStatus, false), 1);
    /// assert_eq!(mgr.outcome(), QuorumOutcome::Achieved);
    /// ```
    #[must_use]
    pub fn outcome(&self) -> QuorumOutcome {
        let good = self.good_responses();
        let pending = self.pending_responses();

        if good < self.quorum_responses {
            if pending + good < self.quorum_responses {
                return QuorumOutcome::Failed;
            }
            return QuorumOutcome::Pending;
        }

        if self.is_quorum_achieved() {
            return QuorumOutcome::Achieved;
        }

        if pending == 0 {
            QuorumOutcome::Mismatched
        } else {
            QuorumOutcome::Pending
        }
    }

    /// Convenience: true when `outcome` reports anything but
    /// [`QuorumOutcome::Pending`].
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::{Msg, MsgType, ResponseMgr};
    ///
    /// let req = Msg::new(1, MsgType::ReqRedisGet, true);
    /// let mgr = ResponseMgr::new(&req, 3, None);
    /// assert!(!mgr.is_done());
    /// ```
    #[must_use]
    pub fn is_done(&self) -> bool {
        !matches!(self.outcome(), QuorumOutcome::Pending)
    }

    fn is_quorum_achieved(&self) -> bool {
        let good = self.good_responses();
        if self.quorum_responses == 1 && good == self.quorum_responses {
            return true;
        }
        if good < self.quorum_responses {
            return false;
        }
        let chk0 = self.good[0].checksum;
        let chk1 = self.good[1].checksum;
        if chk0 == chk1 {
            return true;
        }
        if good < 3 {
            return false;
        }
        let chk2 = self.good[2].checksum;
        chk1 == chk2 || chk0 == chk2
    }

    /// Borrow the first error response, if any.
    ///
    /// # Examples
    /// ```
    /// use dynomite::msg::{Msg, MsgType, ResponseMgr};
    /// let req = Msg::new(1, MsgType::ReqRedisGet, true);
    /// let mgr = ResponseMgr::new(&req, 1, None);
    /// assert!(mgr.error_response().is_none());
    /// ```
    #[must_use]
    pub fn error_response(&self) -> Option<&Msg> {
        self.err_rsp.as_deref()
    }

    /// Iterate over the accepted responses paired with their
    /// checksums.
    pub fn good_iter(&self) -> impl Iterator<Item = (&Msg, u32)> {
        self.good.iter().map(|g| (g.msg.as_ref(), g.checksum))
    }

    /// Pick a response to forward to the client according to the
    /// majority-checksum rule.
    ///
    /// Returns `None` when quorum has not been achieved (the caller
    /// should consult [`Self::outcome`] and propagate the error
    /// response from [`Self::error_response`]).
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::{
    ///     Msg, MsgType, QuorumOutcome, ResponseMgr,
    /// };
    ///
    /// let req = Msg::new(1, MsgType::ReqRedisGet, true);
    /// let mut mgr = ResponseMgr::new(&req, 3, None);
    /// for id in 2..=4 {
    ///     mgr.submit_response(Msg::new(id, MsgType::RspRedisStatus, false), 7);
    /// }
    /// assert_eq!(mgr.outcome(), QuorumOutcome::Achieved);
    /// assert!(mgr.pick_response().is_some());
    /// ```
    #[must_use]
    pub fn pick_response(&self) -> Option<&Msg> {
        if !matches!(self.outcome(), QuorumOutcome::Achieved) {
            return None;
        }
        match self.good.len() {
            1 | 2 => Some(self.good[0].msg.as_ref()),
            3 => {
                let c0 = self.good[0].checksum;
                let c1 = self.good[1].checksum;
                let c2 = self.good[2].checksum;
                if c0 == c1 {
                    Some(self.good[0].msg.as_ref())
                } else if c1 == c2 {
                    Some(self.good[1].msg.as_ref())
                } else if c0 == c2 {
                    Some(self.good[0].msg.as_ref())
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msg::{Msg, MsgType};

    fn req() -> Msg {
        let mut m = Msg::new(1, MsgType::ReqRedisGet, true);
        m.flags_mut().is_read = true;
        m
    }

    fn rsp(id: u64, is_error: bool) -> Msg {
        let mut m = Msg::new(id, MsgType::RspRedisStatus, false);
        m.flags_mut().is_error = is_error;
        m
    }

    #[test]
    fn dc_one_one_good() {
        let mut mgr = ResponseMgr::new(&req(), 1, Some("dc1".into()));
        assert_eq!(mgr.outcome(), QuorumOutcome::Pending);
        mgr.submit_response(rsp(2, false), 1);
        assert_eq!(mgr.outcome(), QuorumOutcome::Achieved);
        assert!(mgr.pick_response().is_some());
    }

    #[test]
    fn dc_one_single_error() {
        let mut mgr = ResponseMgr::new(&req(), 1, None);
        mgr.submit_response(rsp(2, true), 0);
        assert_eq!(mgr.outcome(), QuorumOutcome::Failed);
        assert!(mgr.error_response().is_some());
        assert!(mgr.pick_response().is_none());
    }

    #[test]
    fn dc_quorum_two_matching() {
        let mut mgr = ResponseMgr::new(&req(), 2, None);
        assert_eq!(mgr.quorum_responses(), 2);
        mgr.submit_response(rsp(2, false), 7);
        assert_eq!(mgr.outcome(), QuorumOutcome::Pending);
        mgr.submit_response(rsp(3, false), 7);
        assert_eq!(mgr.outcome(), QuorumOutcome::Achieved);
    }

    #[test]
    fn dc_quorum_two_mismatched_no_third_response() {
        let mut mgr = ResponseMgr::new(&req(), 2, None);
        mgr.submit_response(rsp(2, false), 7);
        mgr.submit_response(rsp(3, false), 9);
        assert_eq!(mgr.outcome(), QuorumOutcome::Mismatched);
    }

    #[test]
    fn dc_quorum_one_good_one_error_fails() {
        let mut mgr = ResponseMgr::new(&req(), 2, None);
        mgr.submit_response(rsp(2, false), 7);
        mgr.submit_response(rsp(3, true), 0);
        assert_eq!(mgr.outcome(), QuorumOutcome::Failed);
    }

    #[test]
    fn dc_safe_quorum_three_all_match() {
        let mut mgr = ResponseMgr::new(&req(), 3, None);
        assert_eq!(mgr.quorum_responses(), 2);
        for id in 2..=4 {
            mgr.submit_response(rsp(id, false), 11);
        }
        assert_eq!(mgr.outcome(), QuorumOutcome::Achieved);
        assert_eq!(mgr.pick_response().unwrap().id(), 2);
    }

    #[test]
    fn dc_safe_quorum_two_match_one_dissent() {
        let mut mgr = ResponseMgr::new(&req(), 3, None);
        mgr.submit_response(rsp(2, false), 5);
        mgr.submit_response(rsp(3, false), 5);
        // Quorum already achieved before the third reply lands.
        assert_eq!(mgr.outcome(), QuorumOutcome::Achieved);
        mgr.submit_response(rsp(4, false), 9);
        assert_eq!(mgr.outcome(), QuorumOutcome::Achieved);
    }

    #[test]
    fn dc_safe_quorum_three_disagreeing_mismatched() {
        let mut mgr = ResponseMgr::new(&req(), 3, None);
        mgr.submit_response(rsp(2, false), 1);
        mgr.submit_response(rsp(3, false), 2);
        // First two disagree, but a third reply is still pending.
        assert_eq!(mgr.outcome(), QuorumOutcome::Pending);
        mgr.submit_response(rsp(4, false), 3);
        assert_eq!(mgr.outcome(), QuorumOutcome::Mismatched);
        assert!(mgr.pick_response().is_none());
    }

    #[test]
    fn dc_safe_quorum_two_errors_fail_immediately() {
        let mut mgr = ResponseMgr::new(&req(), 3, None);
        mgr.submit_response(rsp(2, true), 0);
        mgr.submit_response(rsp(3, true), 0);
        assert_eq!(mgr.outcome(), QuorumOutcome::Failed);
    }

    #[test]
    fn dc_safe_quorum_three_errors_fail() {
        let mut mgr = ResponseMgr::new(&req(), 3, None);
        for id in 2..=4 {
            mgr.submit_response(rsp(id, true), 0);
        }
        assert_eq!(mgr.outcome(), QuorumOutcome::Failed);
    }

    #[test]
    fn dc_safe_quorum_one_dissent_picks_majority() {
        let mut mgr = ResponseMgr::new(&req(), 3, None);
        mgr.submit_response(rsp(2, false), 1);
        mgr.submit_response(rsp(3, false), 2);
        mgr.submit_response(rsp(4, false), 2);
        assert_eq!(mgr.outcome(), QuorumOutcome::Achieved);
        // chk1 == chk2 -> response index 1 wins.
        assert_eq!(mgr.pick_response().unwrap().id(), 3);
    }

    #[test]
    fn excess_good_responses_are_dropped() {
        let mut mgr = ResponseMgr::new(&req(), 3, None);
        for id in 2..=10 {
            mgr.submit_response(rsp(id, false), 1);
        }
        assert_eq!(mgr.good_responses() as usize, MAX_REPLICAS_PER_DC);
    }
}
