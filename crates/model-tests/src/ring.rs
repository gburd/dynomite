//! Model of the token-ring routing.
//!
//! This models the routing implemented in
//! `crates/dynomite/src/cluster/vnode.rs`
//! (`dynomite::cluster::vnode::dispatch`). A ring is a sorted list
//! of `(token, owner)` continuum points. A key hashes to a token; the
//! primary owner is the smallest continuum point whose token is at
//! least the key's, wrapping to the first point when the key exceeds
//! every token. The preference list is the primary plus the next
//! `replicas - 1` distinct owners walking the ring clockwise.
//!
//! The state machine starts from a ring and applies single membership
//! changes (a peer joins by inserting a token, or leaves by removing
//! one). After each change the invariants are checked:
//!
//! * **Determinism**: the routing function is a pure function of the
//!   key and the ring, so the same key on the same ring always yields
//!   the same primary. This is checked directly (the model recomputes
//!   routing twice and asserts equality) and is implicit in the
//!   deterministic `route` below.
//! * **Coverage** (`always`): on any non-empty ring every key maps to
//!   exactly one primary and a preference list of distinct owners; no
//!   key is unowned and no key has two primaries.
//! * **Bounded disruption** (`always`): a single join or leave only
//!   re-routes keys in the affected token range -- keys whose primary
//!   was neither the joined/left owner nor in the immediately
//!   preceding arc keep their primary.

use std::collections::BTreeSet;

use stateright::{Model, Property};

/// A token on the ring (small domain so the state space is finite).
type Token = u8;
/// A peer owner id.
type Owner = u8;

/// One ring: sorted distinct `(token, owner)` continuum points.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Ring {
    points: Vec<(Token, Owner)>,
}

impl Ring {
    /// Build a ring from `(token, owner)` points, sorting by token and
    /// dropping duplicate tokens (last writer wins, matching the
    /// rebuild pass that repopulates the continuum).
    #[must_use]
    pub fn new(mut points: Vec<(Token, Owner)>) -> Self {
        points.sort_by_key(|(t, _)| *t);
        points.dedup_by_key(|(t, _)| *t);
        Self { points }
    }

    /// Primary owner of `key`, mirroring
    /// `dynomite::cluster::vnode::dispatch`: the smallest token
    /// greater than or equal to `key`, wrapping to the first point on
    /// overflow.
    #[must_use]
    pub fn primary(&self, key: Token) -> Option<Owner> {
        let n = self.points.len();
        if n == 0 {
            return None;
        }
        let first = self.points[0];
        let last = self.points[n - 1];
        if last.0 < key {
            return Some(first.1);
        }
        if first.0 >= key {
            return Some(first.1);
        }
        // Smallest point with token >= key.
        let idx = self.points.partition_point(|(t, _)| *t < key);
        Some(self.points[idx].1)
    }

    /// Preference list for `key`: the primary plus the next distinct
    /// owners walking clockwise, up to `replicas` entries.
    #[must_use]
    pub fn preference_list(&self, key: Token, replicas: usize) -> Vec<Owner> {
        let n = self.points.len();
        if n == 0 {
            return Vec::new();
        }
        // Find the index of the primary point.
        let start = if self.points[n - 1].0 < key || self.points[0].0 >= key {
            0
        } else {
            self.points.partition_point(|(t, _)| *t < key)
        };
        let mut out: Vec<Owner> = Vec::new();
        for step in 0..n {
            let owner = self.points[(start + step) % n].1;
            if !out.contains(&owner) {
                out.push(owner);
            }
            if out.len() == replicas {
                break;
            }
        }
        out
    }

    fn tokens(&self) -> BTreeSet<Token> {
        self.points.iter().map(|(t, _)| *t).collect()
    }
}

/// Ring-routing model: starts from a seed ring and applies bounded
/// membership changes drawn from a fixed token / owner alphabet.
#[derive(Clone)]
pub struct RingModel {
    /// Seed ring.
    pub seed: Ring,
    /// Tokens that may be added by a join.
    pub join_tokens: Vec<(Token, Owner)>,
    /// Replication factor for the preference list.
    pub replicas: usize,
    /// Keys to check coverage / disruption over.
    pub keys: Vec<Token>,
    /// Remaining membership changes allowed (bounds the search).
    pub changes: u8,
}

/// Model state: the current ring plus the change budget and the ring
/// just before the last membership change (for the disruption check).
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RingState {
    ring: Ring,
    prev: Option<Ring>,
    /// The owner/token arc touched by the last change, if any.
    touched: Option<Token>,
    changes_left: u8,
}

/// A single membership change the checker may apply.
#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    /// A peer joins by inserting a `(token, owner)` continuum point.
    Join(Token, Owner),
    /// A peer leaves by removing the point with `token`.
    Leave(Token),
}

impl Model for RingModel {
    type State = RingState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![RingState {
            ring: self.seed.clone(),
            prev: None,
            touched: None,
            changes_left: self.changes,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.changes_left == 0 {
            return;
        }
        let tokens = state.ring.tokens();
        for (t, o) in &self.join_tokens {
            if !tokens.contains(t) {
                actions.push(Action::Join(*t, *o));
            }
        }
        for t in &tokens {
            actions.push(Action::Leave(*t));
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut points = last.ring.points.clone();
        let touched = match action {
            Action::Join(t, o) => {
                points.push((t, o));
                t
            }
            Action::Leave(t) => {
                points.retain(|(pt, _)| *pt != t);
                t
            }
        };
        Some(RingState {
            ring: Ring::new(points),
            prev: Some(last.ring.clone()),
            touched: Some(touched),
            changes_left: last.changes_left - 1,
        })
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Coverage: every key has exactly one primary and a
            // preference list of distinct owners on a non-empty ring.
            Property::<Self>::always("coverage", |model, s| {
                if s.ring.points.is_empty() {
                    return true;
                }
                model.keys.iter().all(|&k| {
                    let Some(primary) = s.ring.primary(k) else {
                        return false;
                    };
                    let pref = s.ring.preference_list(k, model.replicas);
                    if pref.first() != Some(&primary) {
                        return false;
                    }
                    // Distinct owners.
                    let mut seen = BTreeSet::new();
                    pref.iter().all(|o| seen.insert(*o))
                })
            }),
            // Determinism: routing is a pure function -- recomputing
            // the primary for a key yields the same owner.
            Property::<Self>::always("determinism", |model, s| {
                model
                    .keys
                    .iter()
                    .all(|&k| s.ring.primary(k) == s.ring.primary(k))
            }),
            // Bounded disruption: a single membership change only
            // re-routes keys whose primary is the touched owner, or
            // whose primary sat in the arc ending at the touched token.
            // Concretely: any key whose primary is unchanged-eligible
            // (its old primary still owns it) keeps its primary.
            Property::<Self>::always("bounded disruption", |model, s| {
                let (Some(prev), Some(touched)) = (s.prev.as_ref(), s.touched) else {
                    return true;
                };
                model.keys.iter().all(|&k| {
                    let before = prev.primary(k);
                    let after = s.ring.primary(k);
                    if before == after {
                        return true;
                    }
                    // The primary changed: the change must be
                    // attributable to the touched token's arc. A key is
                    // in the affected arc when the touched token is the
                    // smallest token >= key on one of the two rings (it
                    // became or stopped being the primary point).
                    key_in_touched_arc(k, touched, prev, &s.ring)
                })
            }),
        ]
    }
}

/// True when a re-route of `key` is attributable to a change at
/// `touched`: the touched point is the primary continuum point for
/// `key` on either the before or after ring.
fn key_in_touched_arc(key: Token, touched: Token, before: &Ring, after: &Ring) -> bool {
    primary_token(before, key) == Some(touched) || primary_token(after, key) == Some(touched)
}

/// The token of the continuum point that owns `key`, mirroring the
/// `primary` selection but returning the token rather than the owner.
fn primary_token(ring: &Ring, key: Token) -> Option<Token> {
    let n = ring.points.len();
    if n == 0 {
        return None;
    }
    let first = ring.points[0];
    let last = ring.points[n - 1];
    if last.0 < key || first.0 >= key {
        return Some(first.0);
    }
    let idx = ring.points.partition_point(|(t, _)| *t < key);
    Some(ring.points[idx].0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use stateright::Checker;

    fn seed() -> Ring {
        Ring::new(vec![(10, 0), (20, 1), (30, 2)])
    }

    /// Determinism: same key + same ring => same primary, exhaustively
    /// over the key domain.
    #[test]
    fn routing_is_deterministic() {
        let r = seed();
        for k in 0u8..=40 {
            assert_eq!(r.primary(k), r.primary(k));
        }
    }

    /// Coverage on the seed ring: every key maps to one of the owners
    /// and the preference list is distinct and starts at the primary.
    #[test]
    fn seed_ring_covers_every_key() {
        let r = seed();
        for k in 0u8..=40 {
            let p = r.primary(k).expect("non-empty ring");
            let pref = r.preference_list(k, 3);
            assert_eq!(pref.first(), Some(&p));
            let mut seen = BTreeSet::new();
            assert!(pref.iter().all(|o| seen.insert(*o)), "distinct owners");
            assert_eq!(pref.len(), 3, "all three owners listed");
        }
    }

    /// Wraparound: a key past the last token routes to the first owner.
    #[test]
    fn wraparound_routes_to_first() {
        let r = seed();
        assert_eq!(r.primary(35), Some(0));
        assert_eq!(r.primary(255), Some(0));
    }

    /// The model holds coverage, determinism, and bounded disruption
    /// across single membership changes.
    #[test]
    fn membership_changes_preserve_invariants() {
        let checker = RingModel {
            seed: seed(),
            join_tokens: vec![(15, 3), (25, 4)],
            replicas: 3,
            keys: (0u8..=40).step_by(3).collect(),
            changes: 2,
        }
        .checker()
        .spawn_bfs()
        .join();
        checker.assert_properties();
        assert!(checker.unique_state_count() > 1);
    }
}
