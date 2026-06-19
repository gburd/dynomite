//! Property tests for residual cluster invariants.
//!
//! Two properties drawn from the coverage brief:
//!
//! * Phi-accrual suspicion is monotone non-decreasing in the
//!   silence interval once a stable heartbeat history exists.
//! * Capability negotiation is commutative in the *outcome
//!   value*: with two registries built from the same supported
//!   sets, negotiating A-against-B and B-against-A picks the same
//!   value for every shared capability.

use std::time::{Duration, Instant};

use dynomite::cluster::capability::{
    Capability, CapabilityAd, CapabilityAdEntry, CapabilityRegistry,
};
use dynomite::cluster::failure_detector::PhiAccrual;
use hegel::generators as gs;
use hegel::TestCase;

#[hegel::test(test_cases = 256)]
fn phi_is_monotone_in_silence(tc: TestCase) {
    // Feed a steady heartbeat history, then probe phi at two
    // silence durations s1 <= s2 past the last heartbeat. Phi
    // must not decrease as the silence grows.
    let interval_ms = tc.draw(gs::integers::<u64>().min_value(10).max_value(2_000));
    let beats = tc.draw(gs::integers::<u64>().min_value(2).max_value(40));
    let s1 = tc.draw(gs::integers::<u64>().min_value(0).max_value(60_000));
    let extra = tc.draw(gs::integers::<u64>().min_value(0).max_value(60_000));
    let s2 = s1 + extra;

    let mut fd = PhiAccrual::new(64, 8.0).with_min_mean(Duration::from_millis(1));
    let anchor = Instant::now();
    for i in 0..beats {
        fd.record_heartbeat(anchor + Duration::from_millis(i * interval_ms));
    }
    let last = anchor + Duration::from_millis((beats - 1) * interval_ms);
    let phi1 = fd.phi(last + Duration::from_millis(s1));
    let phi2 = fd.phi(last + Duration::from_millis(s2));
    assert!(
        phi2 + 1e-9 >= phi1,
        "phi decreased as silence grew: phi({s1})={phi1} > phi({s2})={phi2}"
    );
}

/// Capability over a small u32 domain. "Highest" common value
/// wins, mirroring the stock framing-version cap.
struct U32Cap {
    supported: Vec<u32>,
}
impl Capability for U32Cap {
    type Value = u32;
    fn name(&self) -> &'static str {
        "framing"
    }
    fn supported_values(&self) -> Vec<u32> {
        self.supported.clone()
    }
    fn merge(&self, peer: &[u32]) -> Option<u32> {
        self.supported
            .iter()
            .filter(|v| peer.contains(v))
            .max()
            .copied()
    }
    fn encode_value(&self, v: &u32) -> Vec<u8> {
        v.to_le_bytes().to_vec()
    }
    fn decode_value(&self, b: &[u8]) -> Option<u32> {
        <[u8; 4]>::try_from(b).ok().map(u32::from_le_bytes)
    }
}

fn registry_for(values: &[u32]) -> CapabilityRegistry {
    let mut reg = CapabilityRegistry::new();
    reg.register(U32Cap {
        supported: values.to_vec(),
    });
    reg
}

fn ad_for(values: &[u32]) -> CapabilityAd {
    CapabilityAd::from_entries(vec![CapabilityAdEntry::new(
        "framing".into(),
        values.iter().map(|v| v.to_le_bytes().to_vec()).collect(),
    )])
}

#[hegel::test(test_cases = 256)]
fn negotiation_outcome_is_commutative(tc: TestCase) {
    // Two non-empty supported sets drawn from {1..=8}.
    let a: Vec<u32> = {
        let mut v: Vec<u32> = (1..=8).filter(|_| tc.draw(gs::booleans())).collect();
        if v.is_empty() {
            v.push(1);
        }
        v
    };
    let b: Vec<u32> = {
        let mut v: Vec<u32> = (1..=8).filter(|_| tc.draw(gs::booleans())).collect();
        if v.is_empty() {
            v.push(1);
        }
        v
    };

    // A negotiating against B's ad.
    let reg_a = registry_for(&a);
    let res_a = reg_a.negotiate(&ad_for(&b));
    let chosen_a = res_a.get("framing").map(<[u8]>::to_vec);

    // B negotiating against A's ad.
    let reg_b = registry_for(&b);
    let res_b = reg_b.negotiate(&ad_for(&a));
    let chosen_b = res_b.get("framing").map(<[u8]>::to_vec);

    // Both sides pick the highest common value (or, with no
    // overlap, their own floor). The highest *common* value is
    // symmetric; only the no-overlap floor can differ, so we
    // assert: when there is overlap, both agree on the max.
    let max_common = a.iter().filter(|v| b.contains(v)).max().copied();
    if let Some(expected) = max_common {
        let want = expected.to_le_bytes().to_vec();
        assert_eq!(chosen_a, Some(want.clone()), "A side mismatch");
        assert_eq!(chosen_b, Some(want), "B side mismatch");
    } else {
        // No overlap: each falls back to its own floor (the first
        // supported value), which need not match across sides.
        assert_eq!(chosen_a, Some(a[0].to_le_bytes().to_vec()));
        assert_eq!(chosen_b, Some(b[0].to_le_bytes().to_vec()));
    }
}
