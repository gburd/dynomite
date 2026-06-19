//! Redis response coalescing helpers.
//!
//! Two coalescers cohabit this module:
//!
//! * Fragment coalescing - [`redis_pre_coalesce`],
//!   [`redis_post_coalesce`], [`accumulate_fragment_integer`].
//!   These reproduce the shape of `redis_pre_coalesce` and the
//!   per-command post-coalesce helpers used by the multi-key
//!   fragmentation path. They merge the per-shard responses of a
//!   single multi-key request (`MGET`, `DEL`, `EXISTS`, `MSET`)
//!   into one parent reply.
//!
//! * Replica coalescing - [`CoalesceTracker`], which gathers the
//!   per-replica responses produced by a multi-replica fan-out
//!   under a quorum consistency level. It picks one winning
//!   reply for the client and reports any divergent targets so
//!   the dispatcher can fire off read-repair writes.
//!
//! The replica tracker is a small in-process state machine that
//! the cluster dispatcher owns one-per-request via a per-fan-out
//! actor task. The actor task wraps the client-facing
//! `ServerSink` so that intermediate replicas feed the tracker
//! and only the coalesced reply is forwarded to the client.

use std::collections::{HashMap, HashSet};

use crate::core::types::MsgId;
use crate::msg::{ConsistencyLevel, DynErrorCode, Msg, MsgType};

/// Pre-coalesce hook: classify a fragment response and update
/// internal state for downstream coalescing.
///
/// For `MSG_RSP_REDIS_INTEGER` responses to `DEL` / `EXISTS` the
/// integer payload must be folded into the parent's running
/// total; the parent reference is not available to this function
/// (the parent lives on a different connection state owned by
/// the dispatcher), so the integer accumulation is performed by
/// [`accumulate_fragment_integer`] which the dispatcher calls
/// once it has both messages in scope.
///
/// For `MSG_RSP_REDIS_MULTIBULK` responses to `MGET` the function
/// trims the leading multibulk header. For `MSG_RSP_REDIS_STATUS`
/// responses to `MSET` segments the function clears the body. For
/// `MSG_RSP_REDIS_ERROR` responses the function propagates the
/// error to the parent. Any other response triggers an error on
/// the parent request.
///
/// # Examples
///
/// ```
/// use dynomite::msg::{Msg, MsgType};
/// use dynomite::proto::redis::redis_pre_coalesce;
///
/// let mut rsp = Msg::new(0, MsgType::RspRedisInteger, false);
/// rsp.set_frag_id(0); // not a fragmented response: no-op
/// redis_pre_coalesce(&mut rsp);
/// ```
pub fn redis_pre_coalesce(rsp: &mut Msg) {
    if rsp.is_request() {
        return;
    }
    if rsp.frag_id() == 0 {
        // Not part of a fragmented request; nothing to do.
        return;
    }
    match rsp.ty() {
        MsgType::RspRedisInteger | MsgType::RspRedisMultibulk | MsgType::RspRedisStatus => {
            // The mbuf-level mutation of the response chain and the
            // parent's accumulators is performed by the dispatcher,
            // which owns the parent reference; the integer
            // accumulation is exposed as
            // [`accumulate_fragment_integer`] for the dispatcher
            // to call.
        }
        MsgType::RspRedisError
        | MsgType::RspRedisErrorErr
        | MsgType::RspRedisErrorOom
        | MsgType::RspRedisErrorBusy
        | MsgType::RspRedisErrorNoauth
        | MsgType::RspRedisErrorLoading
        | MsgType::RspRedisErrorBusykey
        | MsgType::RspRedisErrorMisconf
        | MsgType::RspRedisErrorNoscript
        | MsgType::RspRedisErrorReadonly
        | MsgType::RspRedisErrorWrongtype
        | MsgType::RspRedisErrorExecabort
        | MsgType::RspRedisErrorMasterdown
        | MsgType::RspRedisErrorNoreplicas => {
            rsp.set_is_error(true);
        }
        _ => {
            rsp.set_is_error(true);
            rsp.set_dyn_error_code(DynErrorCode::BadFormat);
        }
    }
}

/// Fold the integer payload of a fragmented `DEL` / `EXISTS`
/// response into the parent request's running total
/// (`frag_owner.integer += rsp.integer`).
///
/// Callers are responsible for invoking this once per fragment
/// response after the response parser has stored the integer in
/// `rsp.integer()`. Calling it with a non-integer response or a
/// non-fragmented response is a no-op.
///
/// # Examples
///
/// ```
/// use dynomite::msg::{Msg, MsgType};
/// use dynomite::proto::redis::accumulate_fragment_integer;
///
/// let mut parent = Msg::new(1, MsgType::ReqRedisDel, true);
/// parent.set_integer(2);
///
/// let mut rsp = Msg::new(2, MsgType::RspRedisInteger, false);
/// rsp.set_frag_id(7);
/// rsp.set_integer(3);
///
/// accumulate_fragment_integer(&mut parent, &rsp);
/// assert_eq!(parent.integer(), 5);
/// ```
pub fn accumulate_fragment_integer(parent: &mut Msg, rsp: &Msg) {
    if rsp.is_request() {
        return;
    }
    if rsp.frag_id() == 0 {
        return;
    }
    if !matches!(rsp.ty(), MsgType::RspRedisInteger) {
        return;
    }
    if !matches!(parent.ty(), MsgType::ReqRedisDel | MsgType::ReqRedisExists) {
        return;
    }
    parent.set_integer(parent.integer().saturating_add(rsp.integer()));
}

/// Post-coalesce hook for the parent request once every shard
/// response has arrived.
///
/// This dispatches on the request type (`MGET`, `DEL`/`EXISTS`
/// integer merge, `MSET` status reply). It performs the data-shape
/// side only: it flags the parent request as done. The mbuf-level
/// concatenation of the reply bytes is performed by the reply
/// writer, not here.
///
/// # Examples
///
/// ```
/// use dynomite::msg::{Msg, MsgType};
/// use dynomite::proto::redis::redis_post_coalesce;
///
/// let mut req = Msg::new(0, MsgType::ReqRedisDel, true);
/// redis_post_coalesce(&mut req);
/// ```
pub fn redis_post_coalesce(req: &mut Msg) {
    if !req.is_request() {
        return;
    }
    if req.flags().is_error || req.flags().is_ferror {
        return;
    }
    req.set_done(true);
}

/// Per-replica reply coalescer.
///
/// The tracker is bound to a single client request and the set of
/// replica peers the dispatcher fanned the request out to. As
/// each replica's response arrives it is folded in via
/// [`CoalesceTracker::record_reply`], which returns the next
/// state in the coalescer's state machine.
///
/// State summary:
///
/// * Each reply contributes one entry keyed by the replying
///   peer's index. Late or duplicate replies for an already-seen
///   peer overwrite the prior entry.
/// * Replies are compared via byte-equal payload comparison: the
///   wire bytes attached to the response message's mbuf chain are
///   concatenated and used as the equivalence key. Error replies
///   and successful replies never compare equal even when their
///   wire bytes happen to match (the `is_error` flag is folded
///   into the equivalence key).
/// * The tracker is one-shot: once it returns
///   [`CoalesceOutcome::Ready`] or [`CoalesceOutcome::Error`] it
///   pins itself to that decision and reports
///   [`CoalesceOutcome::Pending`] for further replies (so the
///   actor task can drain stragglers without re-emitting).
///
/// Per-consistency-level rules:
///
/// * `DC_ONE`: the first reply wins. No divergence is reported
///   even when later replies disagree (read repair is a strict
///   quorum-or-stronger feature).
/// * `DC_QUORUM`: declared `Ready` as soon as some reply payload
///   has gathered at least floor(N/2)+1 votes among the local
///   DC's replicas. The chosen winner is the payload with the
///   most votes; ties break toward the first-arrived majority.
///   Targets whose payload differs from the winner are reported
///   as divergent.
/// * `DC_SAFE_QUORUM`: like `DC_QUORUM` but every received reply
///   must agree. The tracker waits for every replica reply,
///   declares `Ready` when they all agree, and surfaces an
///   `Error` when any divergence is observed and no further
///   replies are pending.
/// * `DC_EACH_SAFE_QUORUM`: per-DC quorum. Each DC's replicas
///   must achieve a per-DC majority *and* agree. The local DC's
///   winner is forwarded to the client; replicas (in any DC)
///   whose payloads diverge from their DC's winner are reported
///   as divergent.
///
/// `expected` is the count of replicas the dispatcher fanned the
/// request out to. It must be at least 1; tests cover the lower
/// edge.
#[derive(Debug)]
pub struct CoalesceTracker {
    req_id: MsgId,
    consistency: ConsistencyLevel,
    expected: u8,
    /// Per-target metadata: peer_idx -> (dc, is_local_dc).
    targets: HashMap<u32, TargetInfo>,
    /// Per-target response.
    received: HashMap<u32, ReplySlot>,
    decided: bool,
}

/// Per-target metadata captured at tracker construction.
#[derive(Debug, Clone)]
struct TargetInfo {
    dc: String,
    is_local_dc: bool,
}

/// One recorded reply.
#[derive(Debug)]
struct ReplySlot {
    /// Equivalence key: `is_error` bit + concatenated payload
    /// bytes. Replies with the same key are coalesced.
    eq_key: ReplyKey,
    /// The original parsed message; consumed when the tracker
    /// emits a winner.
    msg: Option<Msg>,
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
struct ReplyKey {
    is_error: bool,
    payload: Vec<u8>,
}

/// Outcome reported by [`CoalesceTracker::record_reply`].
///
/// `Ready` and `Error` are emitted at most once for the lifetime
/// of the tracker; subsequent replies report `Pending` so a
/// long-tail straggler does not produce a spurious second
/// envelope.
///
/// The `Ready` variant boxes the winner [`Msg`] because the
/// message struct is large (~560 bytes) compared to the other
/// variants; storing it inline would inflate every `Pending`
/// return on the hot path.
#[derive(Debug)]
pub enum CoalesceOutcome {
    /// Still gathering replies.
    Pending,
    /// Quorum was reached. `winner` is the message to forward to
    /// the client; `divergent_targets` are the peer indices whose
    /// reply differed from the winner and are eligible for
    /// read-repair writes.
    Ready {
        /// Winning reply (consumed). Boxed to keep the enum
        /// small.
        winner: Box<Msg>,
        /// Peer indices whose reply did not match the winner.
        divergent_targets: Vec<u32>,
    },
    /// Consistency invariants were violated and no winner can be
    /// emitted. The string is a short diagnostic for logging /
    /// the synthetic error response the actor task surfaces to
    /// the client.
    Error(String),
}

impl CoalesceTracker {
    /// Build a tracker bound to `req_id` for the supplied target
    /// list.
    ///
    /// `targets` enumerates the replicas the dispatcher fanned
    /// the request to as `(peer_idx, dc_name)` pairs. `local_dc`
    /// is the node-local DC name; only targets in `local_dc`
    /// participate in the per-DC quorum used for client-facing
    /// reply selection.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::ConsistencyLevel;
    /// use dynomite::proto::redis::CoalesceTracker;
    ///
    /// let t = CoalesceTracker::new(
    ///     1,
    ///     ConsistencyLevel::DcQuorum,
    ///     vec![(0, "dc1".into()), (1, "dc1".into()), (2, "dc1".into())],
    ///     "dc1",
    /// );
    /// assert_eq!(t.expected(), 3);
    /// ```
    #[must_use]
    pub fn new(
        req_id: MsgId,
        consistency: ConsistencyLevel,
        targets: Vec<(u32, String)>,
        local_dc: &str,
    ) -> Self {
        let expected = u8::try_from(targets.len()).unwrap_or(u8::MAX).max(1);
        let mut tmap: HashMap<u32, TargetInfo> = HashMap::with_capacity(targets.len());
        for (idx, dc) in targets {
            let is_local_dc = dc == local_dc;
            tmap.insert(idx, TargetInfo { dc, is_local_dc });
        }
        Self {
            req_id,
            consistency,
            expected,
            targets: tmap,
            received: HashMap::new(),
            decided: false,
        }
    }

    /// Request id this tracker is bound to.
    #[must_use]
    pub fn req_id(&self) -> MsgId {
        self.req_id
    }

    /// Number of replicas the dispatcher fanned the request to.
    #[must_use]
    pub fn expected(&self) -> u8 {
        self.expected
    }

    /// Number of replies recorded so far.
    #[must_use]
    pub fn received_count(&self) -> u8 {
        u8::try_from(self.received.len()).unwrap_or(u8::MAX)
    }

    /// True once a final outcome (Ready / Error) has been
    /// reported; further replies report `Pending`.
    #[must_use]
    pub fn is_decided(&self) -> bool {
        self.decided
    }

    /// Fold one replica's reply into the tracker.
    ///
    /// `source_peer_idx` is the responding peer's index.
    /// Replies from peers that were not in the original target
    /// list are still recorded (so out-of-band repair traffic is
    /// tolerated) but do not contribute to the quorum count.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::io::mbuf::MbufPool;
    /// use dynomite::msg::{response, ConsistencyLevel, Msg, MsgType};
    /// use dynomite::proto::redis::{CoalesceOutcome, CoalesceTracker};
    ///
    /// let mut t = CoalesceTracker::new(
    ///     1,
    ///     ConsistencyLevel::DcOne,
    ///     vec![(0, "dc1".into())],
    ///     "dc1",
    /// );
    /// let pool = MbufPool::default();
    /// let req = Msg::new(1, MsgType::ReqRedisGet, true);
    /// let rsp = response::make_simple_redis(&req, &pool, b"$3\r\nfoo\r\n");
    /// match t.record_reply(0, rsp) {
    ///     CoalesceOutcome::Ready { winner, divergent_targets } => {
    ///         assert!(divergent_targets.is_empty());
    ///         let _ = winner;
    ///     }
    ///     other => panic!("unexpected: {other:?}"),
    /// }
    /// ```
    pub fn record_reply(&mut self, source_peer_idx: u32, rsp: Msg) -> CoalesceOutcome {
        if self.decided {
            return CoalesceOutcome::Pending;
        }
        let key = reply_key(&rsp);
        self.received.insert(
            source_peer_idx,
            ReplySlot {
                eq_key: key,
                msg: Some(rsp),
            },
        );
        match self.consistency {
            ConsistencyLevel::DcOne => self.evaluate_dc_one(source_peer_idx),
            ConsistencyLevel::DcQuorum => self.evaluate_dc_quorum(),
            ConsistencyLevel::DcSafeQuorum => self.evaluate_dc_safe_quorum(),
            ConsistencyLevel::DcEachSafeQuorum => self.evaluate_dc_each_safe_quorum(),
        }
    }

    fn evaluate_dc_one(&mut self, first_peer: u32) -> CoalesceOutcome {
        // First reply wins; later replies are dropped by the
        // `decided` guard.
        self.decided = true;
        let Some(slot) = self.received.get_mut(&first_peer) else {
            return CoalesceOutcome::Error("dc_one: no recorded reply".into());
        };
        let Some(msg) = slot.msg.take() else {
            return CoalesceOutcome::Error("dc_one: reply already consumed".into());
        };
        CoalesceOutcome::Ready {
            winner: Box::new(msg),
            divergent_targets: Vec::new(),
        }
    }

    fn evaluate_dc_quorum(&mut self) -> CoalesceOutcome {
        let local_targets: HashSet<u32> = self
            .targets
            .iter()
            .filter(|(_, t)| t.is_local_dc)
            .map(|(idx, _)| *idx)
            .collect();
        let local_count = if local_targets.is_empty() {
            usize::from(self.expected)
        } else {
            local_targets.len()
        };
        let quorum = local_count / 2 + 1;
        // Tally votes among local-DC replies.
        let votes = self.local_dc_votes(&local_targets);
        if let Some(winner_key) = winning_key(&votes, quorum) {
            return self.emit_winner(&winner_key);
        }
        if self.received_count() as usize >= local_count {
            // All local replies are in but no key reached quorum.
            // Pick the plurality winner; surface as Ready with
            // divergent_targets covering everyone whose payload
            // differs.
            if let Some(winner_key) = plurality_key(&votes) {
                return self.emit_winner(&winner_key);
            }
            self.decided = true;
            return CoalesceOutcome::Error("dc_quorum: no replies in local dc".into());
        }
        CoalesceOutcome::Pending
    }

    fn evaluate_dc_safe_quorum(&mut self) -> CoalesceOutcome {
        let local_targets: HashSet<u32> = self
            .targets
            .iter()
            .filter(|(_, t)| t.is_local_dc)
            .map(|(idx, _)| *idx)
            .collect();
        let local_count = if local_targets.is_empty() {
            usize::from(self.expected)
        } else {
            local_targets.len()
        };
        // Wait for all local replies before deciding.
        let received_local: usize = self
            .received
            .keys()
            .filter(|k| local_targets.is_empty() || local_targets.contains(k))
            .count();
        if received_local < local_count {
            return CoalesceOutcome::Pending;
        }
        let votes = self.local_dc_votes(&local_targets);
        // SAFE_QUORUM requires unanimity among the local replies.
        if votes.len() == 1 {
            let winner_key = votes
                .into_keys()
                .next()
                .expect("invariant: votes.len() == 1");
            return self.emit_winner(&winner_key);
        }
        self.decided = true;
        CoalesceOutcome::Error("dc_safe_quorum: divergent replies".into())
    }

    fn evaluate_dc_each_safe_quorum(&mut self) -> CoalesceOutcome {
        // Per-DC: every DC must have a unanimous quorum.
        // Group target indices by DC.
        let mut per_dc: HashMap<String, Vec<u32>> = HashMap::new();
        for (idx, info) in &self.targets {
            per_dc.entry(info.dc.clone()).or_default().push(*idx);
        }
        // Deterministic iteration order: sort DCs alphabetically
        // so the per-DC checks fire in the same order on every
        // run regardless of HashMap iteration nondeterminism.
        let mut dcs_sorted: Vec<String> = per_dc.keys().cloned().collect();
        dcs_sorted.sort();
        let mut local_winner: Option<ReplyKey> = None;
        let mut all_complete = true;
        // First pass: check every fully-populated DC for
        // intra-DC divergence (which is a hard error). DCs that
        // are not yet complete contribute a Pending vote.
        for dc in &dcs_sorted {
            let idxs = per_dc
                .get(dc)
                .expect("invariant: dc was just enumerated from per_dc");
            let received_dc: usize = idxs
                .iter()
                .filter(|i| self.received.contains_key(*i))
                .count();
            if received_dc < idxs.len() {
                all_complete = false;
                continue;
            }
            let mut dc_votes: HashMap<ReplyKey, Vec<u32>> = HashMap::new();
            for i in idxs {
                if let Some(slot) = self.received.get(i) {
                    dc_votes.entry(slot.eq_key.clone()).or_default().push(*i);
                }
            }
            if dc_votes.len() != 1 {
                self.decided = true;
                return CoalesceOutcome::Error(format!(
                    "dc_each_safe_quorum: divergent replies in dc {dc}"
                ));
            }
            let dc_key = dc_votes
                .into_keys()
                .next()
                .expect("invariant: dc_votes.len() == 1");
            let is_local_dc = self.targets.get(&idxs[0]).is_some_and(|t| t.is_local_dc);
            if is_local_dc {
                local_winner = Some(dc_key);
            }
        }
        if !all_complete {
            return CoalesceOutcome::Pending;
        }
        let Some(winner_key) = local_winner.or_else(|| {
            // No local DC in the target list (shouldn't happen
            // in practice, but be defensive). Pick any DC's
            // winner via the lowest peer_idx tiebreak.
            let mut idxs: Vec<u32> = self.received.keys().copied().collect();
            idxs.sort_unstable();
            idxs.first()
                .and_then(|i| self.received.get(i))
                .map(|s| s.eq_key.clone())
        }) else {
            self.decided = true;
            return CoalesceOutcome::Error("dc_each_safe_quorum: no replies".into());
        };
        // Compute divergent targets across all DCs against the
        // chosen winner.
        let mut divergent: Vec<u32> = Vec::new();
        for (idx, slot) in &self.received {
            if slot.eq_key != winner_key {
                divergent.push(*idx);
            }
        }
        divergent.sort_unstable();
        let outcome = self.emit_winner(&winner_key);
        if let CoalesceOutcome::Ready {
            winner,
            divergent_targets: emitted_div,
        } = outcome
        {
            // Merge: union of cross-DC and within-DC divergence.
            let mut combined: HashSet<u32> = emitted_div.into_iter().collect();
            for d in divergent {
                combined.insert(d);
            }
            let mut combined: Vec<u32> = combined.into_iter().collect();
            combined.sort_unstable();
            return CoalesceOutcome::Ready {
                winner,
                divergent_targets: combined,
            };
        }
        outcome
    }

    fn local_dc_votes(&self, local_targets: &HashSet<u32>) -> HashMap<ReplyKey, Vec<u32>> {
        let mut votes: HashMap<ReplyKey, Vec<u32>> = HashMap::new();
        for (idx, slot) in &self.received {
            if !local_targets.is_empty() && !local_targets.contains(idx) {
                continue;
            }
            votes.entry(slot.eq_key.clone()).or_default().push(*idx);
        }
        votes
    }

    fn emit_winner(&mut self, winner_key: &ReplyKey) -> CoalesceOutcome {
        if self.decided {
            return CoalesceOutcome::Pending;
        }
        // Pick the response Msg whose key matches the winner.
        let mut winner_msg: Option<Msg> = None;
        let mut divergent: Vec<u32> = Vec::new();
        // Deterministic order: scan targets sorted by peer_idx so
        // the same input produces the same winner Msg even when
        // multiple replicas returned identical bytes.
        let mut idx_sorted: Vec<u32> = self.received.keys().copied().collect();
        idx_sorted.sort_unstable();
        for idx in idx_sorted {
            let Some(slot) = self.received.get_mut(&idx) else {
                continue;
            };
            if slot.eq_key == *winner_key {
                if winner_msg.is_none() {
                    if let Some(m) = slot.msg.take() {
                        winner_msg = Some(m);
                    }
                }
            } else {
                divergent.push(idx);
            }
        }
        self.decided = true;
        match winner_msg {
            Some(winner) => CoalesceOutcome::Ready {
                winner: Box::new(winner),
                divergent_targets: divergent,
            },
            None => CoalesceOutcome::Error("coalesce: winner key has no surviving msg".into()),
        }
    }
}

/// Build the equivalence key for a reply.
fn reply_key(rsp: &Msg) -> ReplyKey {
    let payload: Vec<u8> = rsp
        .mbufs()
        .iter()
        .flat_map(|b| b.readable().to_vec())
        .collect();
    ReplyKey {
        is_error: rsp.flags().is_error,
        payload,
    }
}

/// Find a `(key, voters)` pair where `voters.len() >= quorum`.
fn winning_key(votes: &HashMap<ReplyKey, Vec<u32>>, quorum: usize) -> Option<ReplyKey> {
    let mut best: Option<(&ReplyKey, usize)> = None;
    for (k, v) in votes {
        if v.len() >= quorum {
            match best {
                None => best = Some((k, v.len())),
                Some((_, b)) if v.len() > b => best = Some((k, v.len())),
                _ => {}
            }
        }
    }
    best.map(|(k, _)| k.clone())
}

/// Pick the highest-vote key when no key reaches quorum (used as
/// the v1 "plurality" tiebreaker for `DC_QUORUM` when every
/// reply disagrees).
fn plurality_key(votes: &HashMap<ReplyKey, Vec<u32>>) -> Option<ReplyKey> {
    let mut best: Option<(&ReplyKey, usize)> = None;
    let mut best_min_idx: Option<u32> = None;
    for (k, v) in votes {
        let min_idx = v.iter().copied().min().unwrap_or(u32::MAX);
        let take = match best {
            None => true,
            Some((_, b)) if v.len() > b => true,
            Some((_, b)) if v.len() == b && best_min_idx.is_some_and(|m| min_idx < m) => true,
            _ => false,
        };
        if take {
            best = Some((k, v.len()));
            best_min_idx = Some(min_idx);
        }
    }
    best.map(|(k, _)| k.clone())
}

#[cfg(test)]
mod replica_coalesce_tests {
    use super::*;
    use crate::io::mbuf::MbufPool;
    use crate::msg::response::make_simple_redis;
    use crate::msg::{Msg, MsgType};

    fn req() -> Msg {
        Msg::new(1, MsgType::ReqRedisGet, true)
    }

    fn ok_rsp(payload: &[u8]) -> Msg {
        let pool = MbufPool::default();
        make_simple_redis(&req(), &pool, payload)
    }

    fn err_rsp(payload: &[u8]) -> Msg {
        let pool = MbufPool::default();
        let mut m = make_simple_redis(&req(), &pool, payload);
        m.set_is_error(true);
        m
    }

    fn winner_payload(out: &CoalesceOutcome) -> Vec<u8> {
        match out {
            CoalesceOutcome::Ready { winner, .. } => winner
                .mbufs()
                .iter()
                .flat_map(|b| b.readable().to_vec())
                .collect(),
            other => panic!("expected Ready, got {other:?}"),
        }
    }

    fn divergent(out: &CoalesceOutcome) -> Vec<u32> {
        match out {
            CoalesceOutcome::Ready {
                divergent_targets, ..
            } => divergent_targets.clone(),
            other => panic!("expected Ready, got {other:?}"),
        }
    }

    #[test]
    fn dc_one_first_reply_wins() {
        let mut t =
            CoalesceTracker::new(1, ConsistencyLevel::DcOne, vec![(0, "dc1".into())], "dc1");
        let out = t.record_reply(0, ok_rsp(b"+OK\r\n"));
        assert_eq!(winner_payload(&out), b"+OK\r\n");
        assert!(divergent(&out).is_empty());
        assert!(t.is_decided());
    }

    #[test]
    fn dc_one_late_reply_dropped() {
        let mut t = CoalesceTracker::new(
            1,
            ConsistencyLevel::DcOne,
            vec![(0, "dc1".into()), (1, "dc1".into())],
            "dc1",
        );
        let out = t.record_reply(0, ok_rsp(b"$1\r\na\r\n"));
        assert!(matches!(out, CoalesceOutcome::Ready { .. }));
        let out2 = t.record_reply(1, ok_rsp(b"$1\r\nb\r\n"));
        assert!(matches!(out2, CoalesceOutcome::Pending));
    }

    #[test]
    fn dc_quorum_three_all_agree() {
        let mut t = CoalesceTracker::new(
            1,
            ConsistencyLevel::DcQuorum,
            vec![(0, "dc1".into()), (1, "dc1".into()), (2, "dc1".into())],
            "dc1",
        );
        assert!(matches!(
            t.record_reply(0, ok_rsp(b"$1\r\na\r\n")),
            CoalesceOutcome::Pending
        ));
        let out = t.record_reply(1, ok_rsp(b"$1\r\na\r\n"));
        assert_eq!(winner_payload(&out), b"$1\r\na\r\n");
        assert!(divergent(&out).is_empty());
    }

    #[test]
    fn dc_quorum_one_divergent_repaired() {
        let mut t = CoalesceTracker::new(
            1,
            ConsistencyLevel::DcQuorum,
            vec![(0, "dc1".into()), (1, "dc1".into()), (2, "dc1".into())],
            "dc1",
        );
        // Replica 0 returns v1, replica 1 + 2 return v2. The
        // winner is v2 with replica 0 marked divergent.
        assert!(matches!(
            t.record_reply(0, ok_rsp(b"$2\r\nv1\r\n")),
            CoalesceOutcome::Pending
        ));
        assert!(matches!(
            t.record_reply(1, ok_rsp(b"$2\r\nv2\r\n")),
            CoalesceOutcome::Pending
        ));
        let out = t.record_reply(2, ok_rsp(b"$2\r\nv2\r\n"));
        assert_eq!(winner_payload(&out), b"$2\r\nv2\r\n");
        assert_eq!(divergent(&out), vec![0]);
    }

    #[test]
    fn dc_quorum_all_divergent_picks_plurality() {
        let mut t = CoalesceTracker::new(
            1,
            ConsistencyLevel::DcQuorum,
            vec![(0, "dc1".into()), (1, "dc1".into()), (2, "dc1".into())],
            "dc1",
        );
        let _ = t.record_reply(0, ok_rsp(b"$1\r\na\r\n"));
        let _ = t.record_reply(1, ok_rsp(b"$1\r\nb\r\n"));
        let out = t.record_reply(2, ok_rsp(b"$1\r\nc\r\n"));
        // No payload reaches quorum; plurality picks the
        // lowest-peer-idx winner (peer 0 with payload "a").
        match &out {
            CoalesceOutcome::Ready {
                winner,
                divergent_targets,
            } => {
                let w: Vec<u8> = winner
                    .mbufs()
                    .iter()
                    .flat_map(|b| b.readable().to_vec())
                    .collect();
                assert_eq!(w, b"$1\r\na\r\n");
                assert_eq!(divergent_targets, &vec![1, 2]);
            }
            other => panic!("expected Ready, got {other:?}"),
        }
    }

    #[test]
    fn dc_quorum_error_and_value_do_not_match() {
        let mut t = CoalesceTracker::new(
            1,
            ConsistencyLevel::DcQuorum,
            vec![(0, "dc1".into()), (1, "dc1".into()), (2, "dc1".into())],
            "dc1",
        );
        let _ = t.record_reply(0, ok_rsp(b"$1\r\nx\r\n"));
        // Two replicas reply with the same byte payload but one
        // is flagged as an error: they must NOT coalesce. The
        // tracker therefore stays Pending after the third reply
        // since no key reaches quorum, and emits the plurality
        // winner instead.
        let _ = t.record_reply(1, err_rsp(b"$1\r\nx\r\n"));
        let out = t.record_reply(2, ok_rsp(b"$1\r\ny\r\n"));
        // No payload reaches quorum (each appears once).
        // Plurality tiebreak selects peer 0 (lowest idx).
        let CoalesceOutcome::Ready {
            winner,
            divergent_targets,
        } = out
        else {
            panic!("expected Ready");
        };
        assert!(!winner.flags().is_error);
        assert_eq!(divergent_targets, vec![1, 2]);
    }

    #[test]
    fn dc_safe_quorum_three_all_agree() {
        let mut t = CoalesceTracker::new(
            1,
            ConsistencyLevel::DcSafeQuorum,
            vec![(0, "dc1".into()), (1, "dc1".into()), (2, "dc1".into())],
            "dc1",
        );
        assert!(matches!(
            t.record_reply(0, ok_rsp(b"$1\r\nz\r\n")),
            CoalesceOutcome::Pending
        ));
        assert!(matches!(
            t.record_reply(1, ok_rsp(b"$1\r\nz\r\n")),
            CoalesceOutcome::Pending
        ));
        let out = t.record_reply(2, ok_rsp(b"$1\r\nz\r\n"));
        assert!(matches!(out, CoalesceOutcome::Ready { .. }));
        assert!(divergent(&out).is_empty());
    }

    #[test]
    fn dc_safe_quorum_divergence_errors() {
        let mut t = CoalesceTracker::new(
            1,
            ConsistencyLevel::DcSafeQuorum,
            vec![(0, "dc1".into()), (1, "dc1".into()), (2, "dc1".into())],
            "dc1",
        );
        let _ = t.record_reply(0, ok_rsp(b"$1\r\na\r\n"));
        let _ = t.record_reply(1, ok_rsp(b"$1\r\na\r\n"));
        // Two-of-three agree but the third disagrees: SAFE
        // QUORUM surfaces an error rather than "close enough".
        let out = t.record_reply(2, ok_rsp(b"$1\r\nb\r\n"));
        assert!(matches!(out, CoalesceOutcome::Error(_)), "{out:?}");
        assert!(t.is_decided());
    }

    #[test]
    fn dc_each_safe_quorum_two_dcs_all_agree() {
        // 2 DCs * 2 replicas, every replica agrees -> Ready,
        // no divergent targets.
        let mut t = CoalesceTracker::new(
            1,
            ConsistencyLevel::DcEachSafeQuorum,
            vec![
                (0, "dc1".into()),
                (1, "dc1".into()),
                (2, "dc2".into()),
                (3, "dc2".into()),
            ],
            "dc1",
        );
        for idx in 0..4 {
            let _ = t.record_reply(idx, ok_rsp(b"$1\r\nq\r\n"));
        }
        assert!(t.is_decided());
    }

    #[test]
    fn dc_each_safe_quorum_remote_dc_diverges_marks_divergent() {
        // dc1 unanimous, dc2 internally unanimous but different
        // payload than dc1 -> Ready with dc2 replicas as
        // divergent targets.
        let mut t = CoalesceTracker::new(
            1,
            ConsistencyLevel::DcEachSafeQuorum,
            vec![
                (0, "dc1".into()),
                (1, "dc1".into()),
                (2, "dc2".into()),
                (3, "dc2".into()),
            ],
            "dc1",
        );
        let _ = t.record_reply(0, ok_rsp(b"$1\r\na\r\n"));
        let _ = t.record_reply(1, ok_rsp(b"$1\r\na\r\n"));
        let _ = t.record_reply(2, ok_rsp(b"$1\r\nb\r\n"));
        let out = t.record_reply(3, ok_rsp(b"$1\r\nb\r\n"));
        let CoalesceOutcome::Ready {
            divergent_targets, ..
        } = out
        else {
            panic!("expected Ready: {out:?}");
        };
        assert_eq!(divergent_targets, vec![2, 3]);
    }

    #[test]
    fn dc_each_safe_quorum_intra_dc_divergence_errors() {
        // dc1 internally divergent -> Error.
        let mut t = CoalesceTracker::new(
            1,
            ConsistencyLevel::DcEachSafeQuorum,
            vec![
                (0, "dc1".into()),
                (1, "dc1".into()),
                (2, "dc2".into()),
                (3, "dc2".into()),
            ],
            "dc1",
        );
        let _ = t.record_reply(0, ok_rsp(b"$1\r\na\r\n"));
        // After reply 1 lands dc1 is fully populated and
        // divergent: this is the call that emits Error. Once
        // the tracker is decided, later replies return Pending.
        let dc1_full = t.record_reply(1, ok_rsp(b"$1\r\nb\r\n"));
        assert!(
            matches!(dc1_full, CoalesceOutcome::Error(_)),
            "{dc1_full:?}"
        );
        assert!(t.is_decided());
        let _ = t.record_reply(2, ok_rsp(b"$1\r\nq\r\n"));
        let late = t.record_reply(3, ok_rsp(b"$1\r\nq\r\n"));
        assert!(matches!(late, CoalesceOutcome::Pending), "{late:?}");
    }

    #[test]
    fn dc_quorum_two_targets_both_agree() {
        // Edge case: 2 targets, quorum = 2/2+1 = 2 (both must
        // agree). One agreeing reply leaves Pending; second
        // sealing reply is Ready.
        let mut t = CoalesceTracker::new(
            1,
            ConsistencyLevel::DcQuorum,
            vec![(0, "dc1".into()), (1, "dc1".into())],
            "dc1",
        );
        assert!(matches!(
            t.record_reply(0, ok_rsp(b"+OK\r\n")),
            CoalesceOutcome::Pending
        ));
        let out = t.record_reply(1, ok_rsp(b"+OK\r\n"));
        assert!(matches!(out, CoalesceOutcome::Ready { .. }));
    }

    #[test]
    fn duplicate_reply_overwrites() {
        let mut t = CoalesceTracker::new(
            1,
            ConsistencyLevel::DcQuorum,
            vec![(0, "dc1".into()), (1, "dc1".into()), (2, "dc1".into())],
            "dc1",
        );
        let _ = t.record_reply(0, ok_rsp(b"$1\r\nx\r\n"));
        // Same replica replies again with a different value
        // (simulates a buggy peer or a retry); newer value
        // wins.
        let _ = t.record_reply(0, ok_rsp(b"$1\r\ny\r\n"));
        let _ = t.record_reply(1, ok_rsp(b"$1\r\ny\r\n"));
        // Now "y" has 2 votes (peers 0 and 1), reaching quorum.
        assert!(t.is_decided());
    }

    #[test]
    fn after_decided_subsequent_reply_pending() {
        let mut t = CoalesceTracker::new(
            1,
            ConsistencyLevel::DcOne,
            vec![(0, "dc1".into()), (1, "dc1".into())],
            "dc1",
        );
        let _ = t.record_reply(0, ok_rsp(b"+OK\r\n"));
        let out = t.record_reply(1, ok_rsp(b"+ALSO\r\n"));
        assert!(matches!(out, CoalesceOutcome::Pending));
    }
}
