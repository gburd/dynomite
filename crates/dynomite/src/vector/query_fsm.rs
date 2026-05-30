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
//! Phase B (this commit) places the FSM under
//! `dynomite::vector` so the future Phase C wiring can connect
//! it to the existing [`crate::cluster::apl`] preference-list
//! walker and [`crate::cluster::vnode::dispatch`] without a
//! cross-crate dependency. The [`PeerProbe`] callback remains
//! the integration seam.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use gen_fsm::{Action, EventType, FsmHandler, Transition};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use dynvec::SearchResult;

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
}
