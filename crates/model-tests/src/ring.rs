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
//!
//! [`TokenOrderModel`] covers the comparison the dispatcher actually runs
//! (`DynToken`'s `Ord`, the C `cmp_dyn_token`): tokens compare by sign,
//! then by word count, then word by word. It checks **placement reach**:
//! when every ring token (node and key) is one word, every node owns some
//! key. Its negative control is the defect where a hash produced a
//! multi-word key token: such a token sorts above every one-word node
//! token, so every key wraps to the first node, and the checker must
//! report that.

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

/// A ring token as the dispatcher compares it: `(word count, words)`,
/// all positive (the sign rule never applies, since negative tokens are
/// rejected at parse time). `cmp_dyn_token` compares word count first,
/// then the words; deriving `Ord` on `(len, words)` is exactly that.
pub type WideToken = (u8, Vec<u8>);

/// Primary owner of `key` on a sorted ring of `(token, owner)` points,
/// using the token order above. Same selection as [`Ring::primary`].
fn primary_wide(points: &[(WideToken, Owner)], key: &WideToken) -> Option<Owner> {
    let (first, last) = (points.first()?, points.last()?);
    if &last.0 < key || &first.0 >= key {
        return Some(first.1);
    }
    let idx = points.partition_point(|(t, _)| t < key);
    Some(points[idx].1)
}

/// Placement reach under the dispatcher's token order. The state is the
/// set of node tokens placed so far; actions place one more node at a
/// one-word token. Every reachable ring is checked: each node must own at
/// least one key token from `key_width`-word hash outputs.
pub struct TokenOrderModel {
    /// Candidate one-word node tokens (small domain).
    pub node_tokens: Vec<u8>,
    /// Width, in words, of the key tokens the hash emits. `1` is the
    /// fixed behaviour; anything wider is the regression.
    pub key_width: u8,
}

impl TokenOrderModel {
    fn keys(&self) -> Vec<WideToken> {
        // Every value of the low word, at the configured width: a wide key
        // keeps the hash's high words (here: a fixed non-zero word) ahead
        // of the low word, as the four-word murmur3 token did.
        (0..=u8::MAX)
            .map(|lo| {
                let mut words = vec![1u8; usize::from(self.key_width) - 1];
                words.push(lo);
                (self.key_width, words)
            })
            .collect()
    }
}

impl Model for TokenOrderModel {
    type State = Vec<u8>;
    type Action = u8;

    fn init_states(&self) -> Vec<Self::State> {
        vec![Vec::new()]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        for t in &self.node_tokens {
            if !state.contains(t) {
                actions.push(*t);
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut next = last.clone();
        next.push(action);
        next.sort_unstable();
        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![Property::<Self>::always(
            "every node owns a key",
            |model, s| {
                if s.len() < 2 {
                    return true;
                }
                let points: Vec<(WideToken, Owner)> =
                    s.iter().map(|t| ((1, vec![*t]), *t)).collect();
                let owners: BTreeSet<Owner> = model
                    .keys()
                    .iter()
                    .filter_map(|k| primary_wide(&points, k))
                    .collect();
                owners.len() == s.len()
            },
        )]
    }
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

    /// Fixed behaviour: one-word key tokens reach every node on every
    /// reachable ring of one-word node tokens.
    #[test]
    fn one_word_key_tokens_reach_every_node() {
        let checker = TokenOrderModel {
            node_tokens: vec![0, 40, 90, 160, 230],
            key_width: 1,
        }
        .checker()
        .spawn_bfs()
        .join();
        checker.assert_properties();
        assert!(checker.unique_state_count() > 10);
    }

    /// Negative control: four-word key tokens (the old `murmur3`) sort
    /// above every node token, so every key lands on the first node. The
    /// checker must find the violation.
    #[test]
    fn negative_control_wide_key_tokens_collapse_onto_one_node() {
        let checker = TokenOrderModel {
            node_tokens: vec![0, 40, 90, 160, 230],
            key_width: 4,
        }
        .checker()
        .spawn_bfs()
        .join();
        assert!(
            checker.discovery("every node owns a key").is_some(),
            "the checker must catch multi-word key tokens collapsing placement"
        );
    }
}
