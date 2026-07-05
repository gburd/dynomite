//! Property tests for the SWIM + Lifeguard membership state machine
//! ([`dynomite::cluster::swim`]).
//!
//! Three invariants from the brief:
//!
//! * **Incarnation monotonicity refutes stale suspicions.** A
//!   refutation carries a strictly higher incarnation; once a node
//!   has seen it, no later-arriving suspicion at an
//!   equal-or-lower incarnation can put the member back down.
//! * **Suspect -> confirm timeout math.** The confirm deadline is
//!   monotone in the base timeout and the nack score (Lifeguard
//!   dilation always waits at least as long), and monotone
//!   non-increasing in the dogpile suspector count (more suspectors
//!   never wait longer), and always at least one period past the
//!   suspicion start.
//! * **Dissemination reaches a connected graph.** Pushing one
//!   node's view around a fully-connected set of SWIM state machines
//!   converges: every node ends holding the same projected peer
//!   state.

use dynomite::cluster::peer::PeerState;
use dynomite::cluster::swim::{ProbeResult, Status, SwimConfig, SwimState, Update};
use hegel::generators as gs;
use hegel::TestCase;

/// Incarnation monotonicity: a refutation at incarnation `r` makes a
/// later stale suspicion at incarnation `s <= r` a no-op; the member
/// stays routable.
#[hegel::test(test_cases = 256)]
fn incarnation_monotonicity_refutes_stale_suspicions(tc: TestCase) {
    let n = tc.draw(gs::integers::<usize>().min_value(2).max_value(8));
    let member = tc.draw(gs::integers::<usize>().min_value(1).max_value(n - 1));
    let refute_inc = tc.draw(gs::integers::<u64>().min_value(1).max_value(50));
    // Stale suspicion arrives at an incarnation strictly BELOW the
    // refutation. (At an EQUAL incarnation a suspicion legitimately
    // re-applies -- that is how suspicion spreads before the target
    // refutes; the strictly-higher refutation is what overrides it.
    // See `higher_incarnation_suspicion_takes_effect` for the
    // equal/higher direction.)
    let stale_inc = tc.draw(gs::integers::<u64>().min_value(0).max_value(refute_inc - 1));

    let mut s = SwimState::new(0, n, SwimConfig::default());
    // First, suspect the member (so it is Down).
    s.on_probe(1, member, ProbeResult::Failed);
    assert_eq!(s.member_state(member), PeerState::Down);
    // The member refutes with a strictly higher incarnation.
    s.on_update(
        2,
        Update {
            member,
            incarnation: refute_inc,
            status: Status::Alive,
        },
    );
    assert_eq!(
        s.member_state(member),
        PeerState::Normal,
        "refutation at incarnation {refute_inc} should restore Normal"
    );
    // A stale suspicion at <= the refutation incarnation must be
    // ignored: the member stays Normal.
    s.on_update(
        3,
        Update {
            member,
            incarnation: stale_inc,
            status: Status::Suspect {
                since: 3,
                suspectors: 1,
            },
        },
    );
    assert_eq!(
        s.member_state(member),
        PeerState::Normal,
        "stale suspicion at incarnation {stale_inc} < refutation {refute_inc} \
         must not override; member should stay Normal"
    );
}

/// A suspicion at a strictly HIGHER incarnation than the refutation
/// DOES take effect (guards against the refutation being permanent).
#[hegel::test(test_cases = 256)]
fn higher_incarnation_suspicion_takes_effect(tc: TestCase) {
    let refute_inc = tc.draw(gs::integers::<u64>().min_value(0).max_value(50));
    let new_inc = refute_inc + tc.draw(gs::integers::<u64>().min_value(1).max_value(10));

    let mut s = SwimState::new(0, 3, SwimConfig::default());
    s.on_update(
        1,
        Update {
            member: 1,
            incarnation: refute_inc,
            status: Status::Alive,
        },
    );
    assert_eq!(s.member_state(1), PeerState::Normal);
    s.on_update(
        2,
        Update {
            member: 1,
            incarnation: new_inc,
            status: Status::Suspect {
                since: 2,
                suspectors: 1,
            },
        },
    );
    assert_eq!(
        s.member_state(1),
        PeerState::Down,
        "a suspicion at incarnation {new_inc} > refutation {refute_inc} must apply"
    );
}

/// Suspect -> confirm timeout math: the deadline never confirms
/// before the suspicion started + 1 period; dilating by nack score
/// never shortens the wait; piling on suspectors never lengthens it.
#[hegel::test(test_cases = 256)]
fn confirm_deadline_math_is_monotone(tc: TestCase) {
    let base = tc.draw(gs::integers::<u64>().min_value(1).max_value(16));
    let dilation = tc.draw(gs::integers::<u64>().min_value(0).max_value(4));
    let nack_bumps = tc.draw(gs::integers::<u32>().min_value(0).max_value(8));
    let extra_suspectors = tc.draw(gs::integers::<u32>().min_value(0).max_value(6));

    let cfg = SwimConfig {
        indirect_probes: 3,
        suspicion_periods_base: base,
        ns_dilation: dilation,
        ns_max: 8,
        refutation_enabled: true,
    };

    // Baseline: healthy observer (no nack score), lone suspicion.
    let mut lone = SwimState::new(0, 3, cfg);
    lone.on_probe(1, 2, ProbeResult::Failed);
    let d_lone = lone.confirm_deadline(2).unwrap();
    assert!(
        d_lone > 1,
        "deadline must be at least one period past suspicion start (since=1), got {d_lone}"
    );

    // Dilated: same lone suspicion but the observer is slow.
    let mut slow = SwimState::new(0, 3, cfg);
    for _ in 0..nack_bumps {
        slow.on_probe(0, 2, ProbeResult::IndirectAcked);
    }
    slow.on_probe(1, 2, ProbeResult::Failed);
    let d_slow = slow.confirm_deadline(2).unwrap();
    assert!(
        d_slow >= d_lone,
        "nack-score dilation must never shorten the wait: slow={d_slow} < lone={d_lone}"
    );

    // Dogpile: add independent suspectors; the deadline must not
    // grow (it shrinks or stays equal).
    let mut piled = SwimState::new(0, 3, cfg);
    piled.on_probe(1, 2, ProbeResult::Failed);
    let d_one = piled.confirm_deadline(2).unwrap();
    for _ in 0..extra_suspectors {
        piled.on_update(
            1,
            Update {
                member: 2,
                incarnation: 0,
                status: Status::Suspect {
                    since: 1,
                    suspectors: 1,
                },
            },
        );
    }
    let d_many = piled.confirm_deadline(2).unwrap();
    assert!(
        d_many <= d_one,
        "dogpile must never lengthen the wait: many={d_many} > one={d_one}"
    );
}

/// Dissemination reaches a connected graph: seed one node's fact and
/// gossip it round-robin over a fully-connected set of state
/// machines; every node converges to the SAME view of the target.
///
/// A live target refutes any Suspect/Dead fact about itself (that is
/// the whole point of SWIM), so the *agreed* value is the
/// highest-incarnation fact present, not necessarily the seeded one.
/// The invariant here is agreement, matching the gossip convergence
/// model; the Alive case additionally pins the agreed value to
/// Normal.
#[hegel::test(test_cases = 256)]
fn dissemination_converges_on_connected_graph(tc: TestCase) {
    let n = tc.draw(gs::integers::<usize>().min_value(2).max_value(6));
    let target = tc.draw(gs::integers::<usize>().min_value(0).max_value(n - 1));
    // Which fact to disseminate: alive, suspect, or dead, at a fresh
    // high incarnation so it wins every merge.
    let which = tc.draw(gs::integers::<u8>().min_value(0).max_value(2));
    let inc = tc.draw(gs::integers::<u64>().min_value(1).max_value(100));

    let status = match which {
        0 => Status::Alive,
        1 => Status::Suspect {
            since: 1,
            suspectors: 1,
        },
        _ => Status::Dead,
    };

    // Every node runs its own SWIM state machine.
    let mut nodes: Vec<SwimState> = (0..n)
        .map(|me| SwimState::new(me, n, SwimConfig::default()))
        .collect();

    // Node 0 learns the fact first (unless it is about node 0 itself,
    // which cannot be suspected of itself; in that case seed node 1).
    let seed = usize::from(target == 0);
    let seed = seed.min(n - 1);
    nodes[seed].on_update(
        1,
        Update {
            member: target,
            incarnation: inc,
            status,
        },
    );

    // Fully-connected round-robin gossip. Each round the target may
    // refute a suspicion about itself, bumping its incarnation, which
    // disseminates in the next round -- so 2*n rounds give
    // refutations time to settle everywhere.
    for _ in 0..(2 * n) {
        // Snapshot each node's view of the target, then push it
        // everywhere.
        let updates: Vec<(usize, u64, Status)> = nodes
            .iter()
            .map(|node| (target, node.incarnation(target), node.status(target)))
            .collect();
        for node in &mut nodes {
            for &(m, incarnation, st) in &updates {
                node.on_update(
                    2,
                    Update {
                        member: m,
                        incarnation,
                        status: st,
                    },
                );
            }
        }
    }

    // Convergence: every non-target node agrees on the target's
    // projected state (the target itself always views itself Normal).
    let mut agreed: Option<PeerState> = None;
    for (i, node) in nodes.iter().enumerate() {
        if i == target {
            assert_eq!(
                node.member_state(target),
                PeerState::Normal,
                "node {i} views itself as Normal always"
            );
            continue;
        }
        let st = node.member_state(target);
        match agreed {
            None => agreed = Some(st),
            Some(a) => assert_eq!(
                st, a,
                "node {i} disagrees on target {target}: {st:?} vs {a:?} \
                 (dissemination did not converge)"
            ),
        }
    }

    // An Alive fact is never refuted, so it must converge to Normal
    // on every node.
    if matches!(status, Status::Alive) {
        assert_eq!(
            agreed,
            Some(PeerState::Normal),
            "an Alive fact must converge to Normal on every node"
        );
    }
}
