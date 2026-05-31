//! Distributed k-NN coordinator.
//!
//! When an FT.SEARCH command lands on any node, the query is
//! broadcast to every primary peer covering the index's key
//! range, each peer runs the search against its local HNSW
//! index, and the coordinator merges the per-peer top-K
//! results.
//!
//! The coordinator is shaped as a [`gen_fsm::FsmHandler`] state
//! machine with four states:
//!
//! ```text
//!     Init  ->  Fanout  ->  Gather  ->  Merge  ->  (stopped)
//! ```
//!
//! State responsibilities:
//!
//! * [`State::Init`]: receives the [`SearchRequest`], chooses the
//!   peer set, and posts a [`Event::Fanout`] internal event to
//!   move on.
//! * [`State::Fanout`]: forwards the request to each peer via the
//!   supplied [`PeerProbe`] and posts a [`Event::Gather`] event.
//! * [`State::Gather`]: receives [`Event::PeerHits`] events. Once
//!   either every peer has replied or the deadline elapses, it
//!   moves to [`State::Merge`].
//! * [`State::Merge`]: collapses the per-peer hits down to a
//!   global top-K and stashes the result on the response cell
//!   the caller holds.
//!
//! The coordinator does not perform any I/O on its own; the
//! [`PeerProbe`] callback is supplied by the caller and is
//! responsible for actually contacting peers. This keeps the
//! FSM testable in-process without standing up a real cluster.
//!
//! Phase B (this commit) places the FSM under the
//! `dynomite-search` crate so the future Phase C wiring can
//! connect it to the existing [`dynomite::cluster::apl`]
//! preference-list walker and `dynomite::cluster::vnode`
//! dispatch without a cross-crate dependency. The
//! [`PeerProbe`] callback remains the integration seam.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use gen_fsm::{Action, EventType, FsmDriver, FsmHandler, Transition};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use dynvec::SearchResult;

use dynomite::cluster::apl::{walk_n_successors, ClusterState};
use dynomite::embed::events::PeerId;

/// Default per-peer deadline applied by [`broadcast`].
///
/// 5 seconds matches the operational target captured in the
/// PLAN.md FT.SEARCH wire ticket. Operators that prefer a
/// shorter or longer ceiling pass an explicit
/// [`Duration`] to [`broadcast`]; tests typically use a much
/// smaller value to avoid slowing the suite.
pub const DEFAULT_PER_PEER_DEADLINE_MS: u64 = 5_000;

/// One per-peer reply.
#[derive(Clone, Debug, PartialEq)]
pub struct PeerHits {
    /// Identifier of the peer that produced the hits.
    pub peer: String,
    /// Hits returned by that peer's local search, already sorted
    /// closest-first.
    pub hits: Vec<SearchResult>,
}

/// k-NN query request. The coordinator does not interpret
/// `vector` directly; that is the caller's job (the
/// [`PeerProbe`] receives the entire request unchanged).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SearchRequest {
    /// Index name (the FT.CREATE first argument).
    pub table: String,
    /// Query vector in `f32`.
    pub vector: Vec<f32>,
    /// Number of results to return.
    pub k: usize,
    /// Optional override of the index's default `ef_search`.
    pub ef: Option<usize>,
}

/// Final response sent back to the client.
#[derive(Clone, Debug, PartialEq)]
pub struct SearchResponse {
    /// Top-K hits across the whole cluster.
    pub hits: Vec<SearchResult>,
    /// Number of peers whose replies were folded in.
    pub peers_consulted: usize,
}

/// Type-erased peer probe. Returns the per-peer hit list for
/// `request`, or an error message if the peer is unreachable.
pub type PeerProbe =
    Arc<dyn Fn(&str, SearchRequest) -> Result<Vec<SearchResult>, String> + Send + Sync + 'static>;

/// FSM event types.
#[derive(Debug)]
pub enum Event {
    /// Internal: move from Init -> Fanout.
    Fanout,
    /// Internal: move from Fanout -> Gather.
    Gather,
    /// External: a peer's search completed.
    PeerHits(PeerHits),
    /// Internal: every peer has replied; move to Merge.
    GatherComplete,
}

/// FSM states.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    /// Pre-fanout: validating request shape.
    Init,
    /// Issuing requests to peers.
    Fanout,
    /// Waiting for replies.
    Gather,
    /// Producing the merged result.
    Merge,
}

/// Coordinator handler. One instance is bound to one in-flight
/// query; finalising the FSM produces a [`SearchResponse`].
pub struct Coordinator {
    request: SearchRequest,
    peers: Vec<String>,
    probe: PeerProbe,
    hits: HashMap<String, Vec<SearchResult>>,
    response: Arc<Mutex<Option<SearchResponse>>>,
    /// Optional deadline; if any peer fails to reply by this
    /// duration after Fanout, the coordinator merges what it
    /// has.
    deadline: Duration,
}

impl Coordinator {
    /// Build a new coordinator. `peers` is the list of peer
    /// identifiers the request will fan out to; `probe` is
    /// invoked synchronously per peer to fetch hits.
    ///
    /// The coordinator's `peers_consulted` field on the eventual
    /// response counts the number of peers that returned hits
    /// (errors are logged through `tracing::warn!` and
    /// otherwise dropped).
    #[must_use]
    pub fn new(
        request: SearchRequest,
        peers: Vec<String>,
        probe: PeerProbe,
        deadline: Duration,
    ) -> (Self, Arc<Mutex<Option<SearchResponse>>>) {
        let response = Arc::new(Mutex::new(None));
        let coord = Self {
            request,
            peers,
            probe,
            hits: HashMap::new(),
            response: Arc::clone(&response),
            deadline,
        };
        (coord, response)
    }
}

impl FsmHandler for Coordinator {
    type State = State;
    type Event = Event;
    type Reply = ();
    type Stop = String;

    fn initial(&self) -> Self::State {
        State::Init
    }

    fn handle(
        &mut self,
        state: Self::State,
        _event_type: EventType,
        event: Self::Event,
    ) -> Transition<Self> {
        match (state, event) {
            (State::Init, Event::Fanout) => {
                Transition::Next(State::Fanout, vec![Action::post_internal(Event::Gather)])
            }
            (State::Fanout, Event::Gather) => {
                // Issue probes synchronously, post per-peer hits
                // back on the FSM mailbox.
                let mut completion: Vec<Action<Self>> = Vec::new();
                for peer in self.peers.clone() {
                    let res = (self.probe)(&peer, self.request.clone());
                    match res {
                        Ok(hits) => {
                            completion.push(Action::post_internal(Event::PeerHits(PeerHits {
                                peer,
                                hits,
                            })));
                        }
                        Err(err) => {
                            tracing::warn!(peer=%peer, error=%err, "peer probe failed");
                            // Record an empty reply so the
                            // gather predicate still terminates.
                            completion.push(Action::post_internal(Event::PeerHits(PeerHits {
                                peer,
                                hits: Vec::new(),
                            })));
                        }
                    }
                }
                completion.push(Action::set_state_timeout(self.deadline));
                if completion.is_empty() {
                    Transition::Next(
                        State::Merge,
                        vec![Action::post_internal(Event::GatherComplete)],
                    )
                } else {
                    Transition::Next(State::Gather, completion)
                }
            }
            (State::Gather, Event::PeerHits(reply)) => {
                self.hits.insert(reply.peer, reply.hits);
                if self.hits.len() >= self.peers.len() {
                    Transition::Next(
                        State::Merge,
                        vec![Action::post_internal(Event::GatherComplete)],
                    )
                } else {
                    Transition::Keep(vec![])
                }
            }
            (State::Merge, Event::GatherComplete) => {
                let merged = merge_hits(&self.hits, self.request.k);
                let response = SearchResponse {
                    hits: merged,
                    peers_consulted: self.hits.values().filter(|h| !h.is_empty()).count(),
                };
                *self.response.lock() = Some(response);
                Transition::Stop("complete".to_string())
            }
            // Defensive: ignore stray events rather than panicking.
            (_, _) => Transition::Keep(vec![]),
        }
    }

    fn on_timeout(&mut self, state: Self::State, _kind: gen_fsm::TimeoutKind) -> Transition<Self> {
        match state {
            State::Gather => Transition::Next(
                State::Merge,
                vec![Action::post_internal(Event::GatherComplete)],
            ),
            _ => Transition::Keep(vec![]),
        }
    }
}

/// Merge per-peer hit lists into a global top-K.
///
/// Each per-peer list is assumed to be sorted closest-first.
/// The merge is a heap-of-iterators: O((P*K) log P).
#[must_use]
pub fn merge_hits<S: std::hash::BuildHasher>(
    per_peer: &HashMap<String, Vec<SearchResult>, S>,
    k: usize,
) -> Vec<SearchResult> {
    let mut all: Vec<SearchResult> = per_peer.values().flatten().cloned().collect();
    all.sort_by(|a, b| {
        a.score
            .partial_cmp(&b.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    // Deduplicate by id; if the same id appears in multiple
    // peers' replies (duplicate replication), keep the smallest
    // score.
    let mut seen: HashMap<u64, f32> = HashMap::new();
    let mut deduped: Vec<SearchResult> = Vec::with_capacity(all.len());
    for r in all {
        let entry = seen.entry(r.id).or_insert(r.score);
        if r.score <= *entry {
            *entry = r.score;
            deduped.push(r);
        }
    }
    // After dedup, re-sort and take top-k. The deduped vec may
    // have multiple entries for the same id (one per peer that
    // returned it); the dedup step below keeps only the
    // first occurrence of each id, which is the lowest-scored
    // because the input was sorted.
    deduped.sort_by(|a, b| {
        a.score
            .partial_cmp(&b.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let mut final_seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
    let mut out: Vec<SearchResult> = Vec::with_capacity(k);
    for r in deduped {
        if final_seen.insert(r.id) {
            out.push(r);
            if out.len() >= k {
                break;
            }
        }
    }
    out
}

/// Drive the coordinator to completion.
///
/// This is the public entry point: a caller with a
/// [`SearchRequest`], a peer list, and a [`PeerProbe`] can build
/// the FSM, post the initial event, wait for completion, and
/// extract the [`SearchResponse`].
///
/// # Errors
///
/// Surfaces any [`gen_fsm::DriverError`] from the underlying
/// FSM driver.
pub async fn run(
    request: SearchRequest,
    peers: Vec<String>,
    probe: PeerProbe,
    deadline: Duration,
) -> Result<SearchResponse, gen_fsm::DriverError> {
    let (coord, response) = Coordinator::new(request, peers, probe, deadline);
    let driver = gen_fsm::FsmDriver::start(coord);
    driver.cast_checked(Event::Fanout).await?;
    let _stop = driver.join().await?;
    let final_resp = response.lock().clone().unwrap_or(SearchResponse {
        hits: Vec::new(),
        peers_consulted: 0,
    });
    Ok(final_resp)
}

// =====================================================================
// Cluster-coordinated FT.SEARCH coordinator.
// =====================================================================
//
// The block below extends the original local-only [`Coordinator`]
// with a properly distributed broadcast path that:
//
//   * fans out the request to every primary peer covering the
//     index's key range;
//   * applies a per-peer deadline (each peer is timed out
//     independently of the others);
//   * merges per-peer top-K lists with explicit ranking
//     (score-ascending for k-NN, doc-id-ascending for the
//     trigram and regex text paths);
//   * surfaces partial results when one or more peers time out,
//     rather than failing the whole query.
//
// The state machine still uses [`gen_fsm`]: the orchestrator
// spawns one task per peer (each task wraps the probe in a
// [`tokio::time::timeout`]), and each task posts a
// [`BroadcastEvent::PeerReplied`] event back to the FSM. The FSM
// transitions Init -> Gathering(N/n) -> Merging -> Done as the
// per-peer replies come in; an overall safety-net deadline
// transitions Gathering -> Merging-with-partial.

/// Wire-shape-friendly representation of the FT.SEARCH query
/// the coordinator broadcasts to peers.
///
/// The coordinator does not interpret the contents itself; per-peer
/// query execution decodes the variant and runs the matching local
/// path (k-NN against the HNSW engine, trigram substring match
/// against the inverted index, or TRE-backed regex match).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SerializedQuery {
    /// `FT.SEARCH idx "*=>[KNN k @field $param]"` form.
    Knn {
        /// Schema vector field name (the `@field` token).
        vector_field: String,
        /// Raw little-endian f32 query bytes.
        vector_bytes: Vec<u8>,
        /// Optional override of the index's default `ef_search`.
        ef: Option<u32>,
    },
    /// `FT.SEARCH idx "@field:substring"` form.
    Text {
        /// Schema TEXT field name.
        field: String,
        /// Raw substring bytes.
        query: Vec<u8>,
    },
    /// `FT.REGEX idx field pattern [K=n]` (Dynomite extension).
    Regex {
        /// Schema TEXT field name.
        field: String,
        /// POSIX-extended regex pattern.
        pattern: String,
        /// `K=` parameter; zero selects the exact-regex path.
        max_errors: u16,
    },
}

/// One cluster-wide FT.SEARCH hit.
///
/// Distinct from [`SearchResult`] (which uses an HNSW-internal
/// `u64` id): the cluster coordinator works in user-visible
/// document keys because the same logical document may sit on
/// different peers under different internal ids.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HitWithScore {
    /// User-visible document key (the HSET `key` argument).
    pub doc_id: Vec<u8>,
    /// Distance score (smaller is closer for k-NN; ignored
    /// when [`MergeOrder::DocIdAscending`] is in effect).
    pub score: f32,
}

/// One peer's reply to a broadcast.
///
/// `timed_out == true` is the protocol's explicit signal that
/// the per-peer deadline elapsed before the peer produced a
/// reply; the coordinator counts these toward
/// [`BroadcastResponse::peers_timed_out`] and tags the result
/// as partial.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PeerReply {
    /// Per-peer top-K, already sorted by the peer.
    pub hits: Vec<HitWithScore>,
    /// True when the per-peer deadline elapsed.
    pub timed_out: bool,
}

/// Cluster-wide FT.SEARCH request.
///
/// Crosses the wire as the payload of a
/// [`dynomite::proto::dnode::DmsgType::FtSearchReq`] frame; see
/// [`super::wire`] for the codec.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BroadcastRequest {
    /// Index name (the FT.CREATE first argument).
    pub table: String,
    /// Encoded query body.
    pub query: SerializedQuery,
    /// Number of results to return.
    pub top_k: u32,
}

/// Cluster-wide FT.SEARCH response.
///
/// Returned by [`broadcast`]. The `partial` flag is true when
/// at least one peer timed out; the client surfaces this as a
/// `+WARNING` (today the test rig asserts on the flag rather
/// than the wire-level marker).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BroadcastResponse {
    /// Merged global top-K.
    pub hits: Vec<HitWithScore>,
    /// Number of peers whose replies were folded in (any peer
    /// that returned even an empty reply within the deadline).
    pub peers_consulted: usize,
    /// Number of peers whose per-peer deadline elapsed.
    pub peers_timed_out: usize,
    /// True when at least one peer timed out and the merged
    /// result therefore covers a strict subset of the cluster.
    pub partial: bool,
}

/// Tie-breaking and primary-sort policy applied by
/// [`merge_hits_ranked`] / [`broadcast`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergeOrder {
    /// Smallest-score first; ties broken by `doc_id` ASC.
    /// Used by the k-NN path (smaller distance is closer).
    ScoreAscending,
    /// `doc_id` ASC, ignoring score. Used by the text and
    /// regex paths, where peers return matches without a
    /// score and a deterministic ordering is enough for the
    /// client.
    DocIdAscending,
}

/// Async per-peer probe callback.
///
/// Production wiring builds this on top of the dnode peer
/// channel (encode the [`BroadcastRequest`], send the resulting
/// [`dynomite::proto::dnode::DmsgType::FtSearchReq`] frame, await
/// the matching [`dynomite::proto::dnode::DmsgType::FtSearchRep`],
/// decode and return the hits). Tests pass an in-memory
/// callback that simulates per-peer behaviour without standing
/// up real connections.
pub type AsyncPeerProbe = Arc<
    dyn Fn(
            PeerId,
            BroadcastRequest,
        )
            -> Pin<Box<dyn Future<Output = Result<Vec<HitWithScore>, String>> + Send + 'static>>
        + Send
        + Sync
        + 'static,
>;

/// Pick one peer per primary token range covered by the local
/// FT.SEARCH coordinator.
///
/// The walker traverses the ring once starting at token 0 and
/// dedups by peer id, so each canonical-owner peer is visited
/// exactly once regardless of how many vnodes it owns. Down
/// peers (those not in `cluster.alive`) are filtered out so the
/// caller never blocks waiting for a peer the failure detector
/// already gave up on.
///
/// The returned vector is in walk order, which is deterministic
/// for a given ring + liveness snapshot. Callers that want a
/// specific ordering for stability tests can sort the returned
/// vector.
///
/// # Examples
///
/// ```
/// use std::collections::HashSet;
/// use dynomite::cluster::apl::{ClusterState, RingPoint};
/// use dynomite_search::query_fsm::select_primary_peers;
/// let cs = ClusterState::new(
///     vec![
///         RingPoint::new(100, 0),
///         RingPoint::new(200, 1),
///         RingPoint::new(300, 2),
///     ],
///     [0u32, 1, 2].into_iter().collect::<HashSet<_>>(),
/// );
/// assert_eq!(select_primary_peers(&cs).len(), 3);
/// ```
#[must_use]
pub fn select_primary_peers(cluster: &ClusterState) -> Vec<PeerId> {
    let len = cluster.ring().len();
    if len == 0 {
        return Vec::new();
    }
    walk_n_successors(cluster, 0, len)
        .into_iter()
        .filter(|(_, pid)| cluster.is_alive(*pid))
        .map(|(_, pid)| pid)
        .collect()
}

/// Default per-peer fanout deadline.
#[must_use]
pub const fn default_per_peer_deadline() -> Duration {
    Duration::from_millis(DEFAULT_PER_PEER_DEADLINE_MS)
}

/// Merge per-peer hit lists into a global top-K ordered by
/// `order`.
///
/// Each per-peer list is assumed to be sorted by the peer in
/// the same order; the merge re-sorts the union and keeps the
/// first `top_k` entries after deduplicating by `doc_id`. For
/// [`MergeOrder::ScoreAscending`] duplicate doc ids keep the
/// smallest score; for [`MergeOrder::DocIdAscending`] duplicate
/// doc ids are simply elided.
///
/// `top_k` of zero returns an empty vector. Empty per-peer
/// lists contribute nothing.
///
/// # Examples
///
/// ```
/// use dynomite_search::query_fsm::{
///     merge_hits_ranked, HitWithScore, MergeOrder, PeerReply,
/// };
/// let p1 = PeerReply {
///     hits: vec![HitWithScore { doc_id: b"a".to_vec(), score: 0.1 }],
///     timed_out: false,
/// };
/// let p2 = PeerReply {
///     hits: vec![HitWithScore { doc_id: b"b".to_vec(), score: 0.05 }],
///     timed_out: false,
/// };
/// let merged = merge_hits_ranked(&[p1, p2], 2, MergeOrder::ScoreAscending);
/// assert_eq!(merged[0].doc_id, b"b");
/// assert_eq!(merged[1].doc_id, b"a");
/// ```
#[must_use]
pub fn merge_hits_ranked(
    per_peer: &[PeerReply],
    top_k: u32,
    order: MergeOrder,
) -> Vec<HitWithScore> {
    let cap = usize::try_from(top_k).unwrap_or(usize::MAX);
    if cap == 0 {
        return Vec::new();
    }
    let mut all: Vec<HitWithScore> = per_peer
        .iter()
        .flat_map(|reply| reply.hits.iter().cloned())
        .collect();
    sort_hits(&mut all, order);
    let mut seen: HashSet<Vec<u8>> = HashSet::with_capacity(all.len().min(cap));
    let mut out: Vec<HitWithScore> = Vec::with_capacity(cap);
    for hit in all {
        if seen.insert(hit.doc_id.clone()) {
            out.push(hit);
            if out.len() >= cap {
                break;
            }
        }
    }
    out
}

fn sort_hits(hits: &mut [HitWithScore], order: MergeOrder) {
    match order {
        MergeOrder::ScoreAscending => {
            hits.sort_by(|a, b| {
                a.score
                    .partial_cmp(&b.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.doc_id.cmp(&b.doc_id))
            });
        }
        MergeOrder::DocIdAscending => {
            hits.sort_by(|a, b| a.doc_id.cmp(&b.doc_id));
        }
    }
}

// ---- Distributed broadcast FSM ----------------------------------------

/// Events consumed by the distributed broadcast FSM.
#[derive(Debug)]
pub enum BroadcastEvent {
    /// One peer's reply (success, application error, or timeout).
    PeerReplied(PeerReply),
    /// Internal: every peer has reported back; transition
    /// from [`BroadcastState::Gathering`] to
    /// [`BroadcastState::Merging`].
    AllReceived,
    /// Internal: merge has produced the final response;
    /// transition from [`BroadcastState::Merging`] to
    /// terminal stop.
    MergeDone,
}

/// States of the distributed broadcast FSM.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BroadcastState {
    /// Pre-fanout: waiting for the orchestrator to start
    /// posting replies.
    Init,
    /// Receiving per-peer replies. Counters live on the FSM
    /// data; the variant itself is parameter-free so it stays
    /// `Copy`.
    Gathering,
    /// Merging the per-peer hit lists into the global top-K.
    Merging,
}

/// Distributed broadcast coordinator.
///
/// Holds the FSM data: the request, the running list of
/// per-peer replies, the merge order, the response cell, and
/// the peer count needed to detect completion.
pub struct BroadcastCoordinator {
    request: BroadcastRequest,
    expected_peers: usize,
    replies: Vec<PeerReply>,
    order: MergeOrder,
    response: Arc<Mutex<Option<BroadcastResponse>>>,
    overall_deadline: Duration,
}

impl BroadcastCoordinator {
    /// Construct a fresh coordinator.
    #[must_use]
    pub fn new(
        request: BroadcastRequest,
        expected_peers: usize,
        order: MergeOrder,
        overall_deadline: Duration,
    ) -> (Self, Arc<Mutex<Option<BroadcastResponse>>>) {
        let response = Arc::new(Mutex::new(None));
        let coord = Self {
            request,
            expected_peers,
            replies: Vec::with_capacity(expected_peers),
            order,
            response: Arc::clone(&response),
            overall_deadline,
        };
        (coord, response)
    }

    fn finalise(&self) -> BroadcastResponse {
        let timed_out = self.replies.iter().filter(|r| r.timed_out).count();
        let consulted = self.replies.len();
        let merged = merge_hits_ranked(&self.replies, self.request.top_k, self.order);
        BroadcastResponse {
            hits: merged,
            peers_consulted: consulted,
            peers_timed_out: timed_out,
            partial: timed_out > 0 || consulted < self.expected_peers,
        }
    }
}

impl FsmHandler for BroadcastCoordinator {
    type State = BroadcastState;
    type Event = BroadcastEvent;
    type Reply = ();
    type Stop = String;

    fn initial(&self) -> Self::State {
        BroadcastState::Init
    }

    fn handle(
        &mut self,
        state: Self::State,
        _event_type: EventType,
        event: Self::Event,
    ) -> Transition<Self> {
        match (state, event) {
            (BroadcastState::Init | BroadcastState::Gathering, BroadcastEvent::PeerReplied(r)) => {
                self.replies.push(r);
                if self.replies.len() >= self.expected_peers {
                    Transition::Next(
                        BroadcastState::Merging,
                        vec![
                            Action::cancel_state_timeout(),
                            Action::post_internal(BroadcastEvent::AllReceived),
                        ],
                    )
                } else if state == BroadcastState::Init {
                    Transition::Next(
                        BroadcastState::Gathering,
                        vec![Action::set_state_timeout(self.overall_deadline)],
                    )
                } else {
                    Transition::Keep(vec![])
                }
            }
            (BroadcastState::Merging, BroadcastEvent::AllReceived | BroadcastEvent::MergeDone) => {
                let resp = self.finalise();
                *self.response.lock() = Some(resp);
                Transition::Stop("broadcast complete".to_string())
            }
            // Any other (state, event) pair is benign: stray
            // PeerReplied frames after the merge already kicked
            // off, or AllReceived posted a second time by a
            // racing internal event. We swallow them rather
            // than panicking.
            _ => Transition::Keep(vec![]),
        }
    }

    fn on_timeout(&mut self, state: Self::State, _kind: gen_fsm::TimeoutKind) -> Transition<Self> {
        if matches!(state, BroadcastState::Gathering | BroadcastState::Init) {
            // Synthesise a timed-out reply for every still-missing
            // peer so the merge knows the broadcast is partial.
            while self.replies.len() < self.expected_peers {
                self.replies.push(PeerReply {
                    hits: Vec::new(),
                    timed_out: true,
                });
            }
            Transition::Next(
                BroadcastState::Merging,
                vec![Action::post_internal(BroadcastEvent::AllReceived)],
            )
        } else {
            Transition::Keep(vec![])
        }
    }
}

/// Drive the distributed FT.SEARCH coordinator to completion.
///
/// `peers` is the list of peer ids the request will be
/// broadcast to; build it via [`select_primary_peers`] from a
/// [`dynomite::cluster::apl::ClusterState`] in production. `probe`
/// is invoked once per peer and is responsible for actually
/// running the per-peer search (in production, by serialising
/// the request via [`super::wire::encode_request`] and writing
/// it down the dnode peer channel).
///
/// Each per-peer probe is wrapped in a
/// [`tokio::time::timeout`] of `per_peer_deadline`. A timed-out
/// peer contributes an empty [`PeerReply`] flagged
/// `timed_out = true`; it does not abort the broadcast.
///
/// `order` selects the merge ranking: pass
/// [`MergeOrder::ScoreAscending`] for the k-NN path (smaller
/// distance is closer) or [`MergeOrder::DocIdAscending`] for
/// the trigram and regex text paths.
///
/// Returns a [`BroadcastResponse`] whose `partial` flag is
/// `true` when at least one peer timed out (or when no peers
/// were supplied at all).
///
/// # Errors
///
/// Surfaces any [`gen_fsm::DriverError`] from the underlying
/// FSM driver.
pub async fn broadcast(
    request: BroadcastRequest,
    peers: Vec<PeerId>,
    probe: AsyncPeerProbe,
    per_peer_deadline: Duration,
    order: MergeOrder,
) -> Result<BroadcastResponse, gen_fsm::DriverError> {
    if peers.is_empty() {
        return Ok(BroadcastResponse {
            hits: Vec::new(),
            peers_consulted: 0,
            peers_timed_out: 0,
            partial: true,
        });
    }
    // Overall deadline: a generous safety net above the
    // per-peer deadline. The coordinator drives termination
    // off the per-peer fan-in; this only fires if a probe task
    // panics or the runtime stalls before the per-peer timeout
    // can elapse.
    let overall = per_peer_deadline
        .saturating_mul(2)
        .saturating_add(Duration::from_secs(1));
    let n = peers.len();
    let (handler, response) = BroadcastCoordinator::new(request.clone(), n, order, overall);
    let driver: FsmDriver<BroadcastCoordinator> = FsmDriver::start(handler);
    let (reply_tx, mut reply_rx) = mpsc::channel::<PeerReply>(n);
    for peer in peers {
        let probe = Arc::clone(&probe);
        let req = request.clone();
        let tx = reply_tx.clone();
        tokio::spawn(async move {
            let fut = probe(peer, req);
            let reply = match tokio::time::timeout(per_peer_deadline, fut).await {
                Ok(Ok(hits)) => PeerReply {
                    hits,
                    timed_out: false,
                },
                Ok(Err(err)) => {
                    tracing::warn!(peer=peer, error=%err, "FT.SEARCH peer probe failed");
                    PeerReply {
                        hits: Vec::new(),
                        timed_out: false,
                    }
                }
                Err(_) => {
                    tracing::warn!(
                        peer = peer,
                        "FT.SEARCH peer probe timed out (per-peer deadline elapsed)"
                    );
                    PeerReply {
                        hits: Vec::new(),
                        timed_out: true,
                    }
                }
            };
            let _ = tx.send(reply).await;
        });
    }
    drop(reply_tx);
    let driver_for_pump = driver.clone();
    let pump = tokio::spawn(async move {
        while let Some(reply) = reply_rx.recv().await {
            if driver_for_pump
                .cast_checked(BroadcastEvent::PeerReplied(reply))
                .await
                .is_err()
            {
                break;
            }
        }
    });
    let _ = driver.join().await?;
    let _ = pump.await;
    let final_resp = response
        .lock()
        .clone()
        .unwrap_or_else(|| BroadcastResponse {
            hits: Vec::new(),
            peers_consulted: 0,
            peers_timed_out: n,
            partial: true,
        });
    Ok(final_resp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use dynvec::SearchResult;

    fn req() -> SearchRequest {
        SearchRequest {
            table: "t".to_string(),
            vector: vec![0.0; 4],
            k: 3,
            ef: None,
        }
    }

    #[tokio::test]
    async fn merges_hits_from_multiple_peers() {
        let hits_p1 = vec![
            SearchResult { id: 1, score: 0.1 },
            SearchResult { id: 2, score: 0.5 },
        ];
        let hits_p2 = vec![
            SearchResult { id: 3, score: 0.2 },
            SearchResult { id: 4, score: 0.6 },
        ];
        let probe: PeerProbe = Arc::new(move |peer, _r| match peer {
            "p1" => Ok(hits_p1.clone()),
            "p2" => Ok(hits_p2.clone()),
            _ => Err("unknown peer".to_string()),
        });
        let resp = run(
            req(),
            vec!["p1".to_string(), "p2".to_string()],
            probe,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(resp.peers_consulted, 2);
        assert_eq!(resp.hits.len(), 3);
        assert_eq!(resp.hits[0].id, 1);
        assert_eq!(resp.hits[1].id, 3);
        assert_eq!(resp.hits[2].id, 2);
    }

    #[tokio::test]
    async fn missing_peers_are_tolerated() {
        let probe: PeerProbe = Arc::new(|peer, _r| match peer {
            "good" => Ok(vec![SearchResult { id: 1, score: 0.1 }]),
            _ => Err("dead".to_string()),
        });
        let resp = run(
            req(),
            vec!["good".to_string(), "bad".to_string()],
            probe,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(resp.peers_consulted, 1);
        assert_eq!(resp.hits.len(), 1);
        assert_eq!(resp.hits[0].id, 1);
    }

    #[tokio::test]
    async fn duplicate_ids_collapsed() {
        let probe: PeerProbe = Arc::new(|peer, _r| match peer {
            "p1" => Ok(vec![SearchResult { id: 1, score: 0.10 }]),
            "p2" => Ok(vec![SearchResult { id: 1, score: 0.05 }]),
            _ => Err("unknown".to_string()),
        });
        let resp = run(
            SearchRequest {
                table: "t".to_string(),
                vector: vec![],
                k: 2,
                ef: None,
            },
            vec!["p1".to_string(), "p2".to_string()],
            probe,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(resp.hits.len(), 1);
        assert!((resp.hits[0].score - 0.05).abs() < 1e-6);
    }

    // ---- Distributed broadcast FSM tests --------------------

    use std::collections::HashSet;

    use dynomite::cluster::apl::{ClusterState, RingPoint};

    fn knn_request(top_k: u32) -> BroadcastRequest {
        BroadcastRequest {
            table: "idx".into(),
            query: SerializedQuery::Knn {
                vector_field: "v".into(),
                vector_bytes: vec![0u8; 16],
                ef: None,
            },
            top_k,
        }
    }

    fn fixed_probe(per_peer: HashMap<PeerId, Vec<HitWithScore>>) -> AsyncPeerProbe {
        Arc::new(move |peer, _req| {
            let hits = per_peer.get(&peer).cloned().unwrap_or_default();
            Box::pin(async move { Ok(hits) })
        })
    }

    #[tokio::test]
    async fn merge_score_ascending_picks_smallest_scores() {
        let p0 = PeerReply {
            hits: vec![
                HitWithScore {
                    doc_id: b"a".to_vec(),
                    score: 0.1,
                },
                HitWithScore {
                    doc_id: b"b".to_vec(),
                    score: 0.5,
                },
            ],
            timed_out: false,
        };
        let p1 = PeerReply {
            hits: vec![
                HitWithScore {
                    doc_id: b"c".to_vec(),
                    score: 0.05,
                },
                HitWithScore {
                    doc_id: b"d".to_vec(),
                    score: 0.6,
                },
            ],
            timed_out: false,
        };
        let merged = merge_hits_ranked(&[p0, p1], 3, MergeOrder::ScoreAscending);
        assert_eq!(merged.len(), 3);
        assert_eq!(merged[0].doc_id, b"c");
        assert_eq!(merged[1].doc_id, b"a");
        assert_eq!(merged[2].doc_id, b"b");
    }

    #[tokio::test]
    async fn merge_doc_id_ascending_orders_by_key() {
        let p0 = PeerReply {
            hits: vec![
                HitWithScore {
                    doc_id: b"key:9".to_vec(),
                    score: 0.0,
                },
                HitWithScore {
                    doc_id: b"key:1".to_vec(),
                    score: 0.0,
                },
            ],
            timed_out: false,
        };
        let p1 = PeerReply {
            hits: vec![HitWithScore {
                doc_id: b"key:5".to_vec(),
                score: 0.0,
            }],
            timed_out: false,
        };
        let merged = merge_hits_ranked(&[p0, p1], 5, MergeOrder::DocIdAscending);
        assert_eq!(
            merged.iter().map(|h| h.doc_id.clone()).collect::<Vec<_>>(),
            vec![b"key:1".to_vec(), b"key:5".to_vec(), b"key:9".to_vec()],
        );
    }

    #[tokio::test]
    async fn merge_dedups_doc_ids_in_score_order() {
        let p0 = PeerReply {
            hits: vec![HitWithScore {
                doc_id: b"a".to_vec(),
                score: 0.10,
            }],
            timed_out: false,
        };
        let p1 = PeerReply {
            hits: vec![HitWithScore {
                doc_id: b"a".to_vec(),
                score: 0.05,
            }],
            timed_out: false,
        };
        let merged = merge_hits_ranked(&[p0, p1], 5, MergeOrder::ScoreAscending);
        assert_eq!(merged.len(), 1);
        assert!((merged[0].score - 0.05).abs() < 1e-6);
    }

    #[tokio::test]
    async fn merge_top_k_zero_returns_empty() {
        let p = PeerReply {
            hits: vec![HitWithScore {
                doc_id: b"a".to_vec(),
                score: 0.1,
            }],
            timed_out: false,
        };
        assert!(merge_hits_ranked(&[p], 0, MergeOrder::ScoreAscending).is_empty());
    }

    #[tokio::test]
    async fn broadcast_with_no_peers_returns_partial_empty() {
        let probe: AsyncPeerProbe = Arc::new(|_peer, _req| Box::pin(async { Ok(Vec::new()) }));
        let resp = broadcast(
            knn_request(5),
            Vec::new(),
            probe,
            Duration::from_millis(50),
            MergeOrder::ScoreAscending,
        )
        .await
        .unwrap();
        assert!(resp.hits.is_empty());
        assert_eq!(resp.peers_consulted, 0);
        assert!(resp.partial);
    }

    #[tokio::test]
    async fn broadcast_one_peer_returns_local_top_k() {
        let mut per_peer: HashMap<PeerId, Vec<HitWithScore>> = HashMap::new();
        per_peer.insert(
            7,
            vec![
                HitWithScore {
                    doc_id: b"a".to_vec(),
                    score: 0.10,
                },
                HitWithScore {
                    doc_id: b"b".to_vec(),
                    score: 0.30,
                },
            ],
        );
        let resp = broadcast(
            knn_request(2),
            vec![7],
            fixed_probe(per_peer),
            Duration::from_millis(200),
            MergeOrder::ScoreAscending,
        )
        .await
        .unwrap();
        assert_eq!(resp.peers_consulted, 1);
        assert_eq!(resp.peers_timed_out, 0);
        assert!(!resp.partial);
        assert_eq!(resp.hits.len(), 2);
        assert_eq!(resp.hits[0].doc_id, b"a");
    }

    #[tokio::test]
    async fn broadcast_two_peers_merges() {
        let mut per_peer: HashMap<PeerId, Vec<HitWithScore>> = HashMap::new();
        per_peer.insert(
            1,
            vec![
                HitWithScore {
                    doc_id: b"a".to_vec(),
                    score: 0.10,
                },
                HitWithScore {
                    doc_id: b"b".to_vec(),
                    score: 0.40,
                },
            ],
        );
        per_peer.insert(
            2,
            vec![
                HitWithScore {
                    doc_id: b"c".to_vec(),
                    score: 0.05,
                },
                HitWithScore {
                    doc_id: b"d".to_vec(),
                    score: 0.50,
                },
            ],
        );
        let resp = broadcast(
            knn_request(3),
            vec![1, 2],
            fixed_probe(per_peer),
            Duration::from_millis(200),
            MergeOrder::ScoreAscending,
        )
        .await
        .unwrap();
        assert_eq!(resp.peers_consulted, 2);
        assert_eq!(resp.hits.len(), 3);
        assert_eq!(resp.hits[0].doc_id, b"c");
        assert_eq!(resp.hits[1].doc_id, b"a");
        assert_eq!(resp.hits[2].doc_id, b"b");
    }

    #[tokio::test]
    async fn broadcast_one_peer_timeout_marks_partial() {
        let probe: AsyncPeerProbe = Arc::new(move |peer, _req| {
            Box::pin(async move {
                if peer == 9 {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    Ok(Vec::new())
                } else {
                    Ok(vec![HitWithScore {
                        doc_id: b"x".to_vec(),
                        score: 0.10,
                    }])
                }
            })
        });
        let resp = broadcast(
            knn_request(3),
            vec![1, 9],
            probe,
            Duration::from_millis(50),
            MergeOrder::ScoreAscending,
        )
        .await
        .unwrap();
        assert_eq!(resp.peers_consulted, 2);
        assert_eq!(resp.peers_timed_out, 1);
        assert!(resp.partial);
        assert_eq!(resp.hits.len(), 1);
        assert_eq!(resp.hits[0].doc_id, b"x");
    }

    #[tokio::test]
    async fn broadcast_all_peers_timeout_returns_empty_partial() {
        let probe: AsyncPeerProbe = Arc::new(|_peer, _req| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(500)).await;
                Ok(Vec::new())
            })
        });
        let resp = broadcast(
            knn_request(3),
            vec![1, 2, 3],
            probe,
            Duration::from_millis(40),
            MergeOrder::ScoreAscending,
        )
        .await
        .unwrap();
        assert_eq!(resp.peers_consulted, 3);
        assert_eq!(resp.peers_timed_out, 3);
        assert!(resp.partial);
        assert!(resp.hits.is_empty());
    }

    #[tokio::test]
    async fn select_primary_peers_returns_one_per_distinct_alive_peer() {
        // Three peers, each with one ring entry; all alive.
        let cs = ClusterState::new(
            vec![
                RingPoint::new(100, 0),
                RingPoint::new(200, 1),
                RingPoint::new(300, 2),
            ],
            [0u32, 1, 2].into_iter().collect::<HashSet<_>>(),
        );
        let mut peers = select_primary_peers(&cs);
        peers.sort_unstable();
        assert_eq!(peers, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn select_primary_peers_filters_dead_peers() {
        let cs = ClusterState::new(
            vec![
                RingPoint::new(100, 0),
                RingPoint::new(200, 1),
                RingPoint::new(300, 2),
            ],
            // Peer 1 is dead.
            [0u32, 2].into_iter().collect::<HashSet<_>>(),
        );
        let mut peers = select_primary_peers(&cs);
        peers.sort_unstable();
        assert_eq!(peers, vec![0, 2]);
    }

    #[tokio::test]
    async fn select_primary_peers_dedups_multi_vnode_peers() {
        // Peer 0 has two ring entries (multi-vnode); the
        // selector returns it once.
        let cs = ClusterState::new(
            vec![
                RingPoint::new(100, 0),
                RingPoint::new(200, 0),
                RingPoint::new(300, 1),
            ],
            [0u32, 1].into_iter().collect::<HashSet<_>>(),
        );
        let mut peers = select_primary_peers(&cs);
        peers.sort_unstable();
        assert_eq!(peers, vec![0, 1]);
    }
}
