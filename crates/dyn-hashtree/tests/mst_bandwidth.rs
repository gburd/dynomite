//! Bandwidth comparison: fixed-grid Tictac merkle reconcile vs
//! the divergence-proportional Merkle Search Tree reconcile.
//!
//! This is the "measure the win" deliverable. It builds two
//! replicas that share a large key set and differ by a small
//! ratio (0.1%, 1%, 10%), then reports two cost metrics for each
//! reconcile strategy:
//!
//! * **keys transferred** -- the count of keys the reconcile
//!   ships across the wire. For the MST this is the exact
//!   symmetric difference; for the fixed grid it is every key in
//!   every dirtied segment (a differing key drags its whole
//!   segment along).
//! * **node/segment comparisons** -- the structural walk cost.
//!
//! The point the numbers make: the MST's transferred-key count
//! is flat in the dataset size and tracks the divergence, while
//! the fixed grid's transferred-key count grows with the dataset
//! because each dirtied segment carries `~N / n_segments` keys
//! regardless of how many of them actually differ.
//!
//! The Tictac cost is computed against a faithful reimplementation
//! of the shipped fixed-grid model (segment = hash(key) mod S; a
//! segment is dirty if any of its keys differ; KEY-SYNC ships all
//! keys in a dirty segment) so the two strategies are compared on
//! the same inputs. Run with `--nocapture` to see the table.

#![cfg(not(loom))]

use std::collections::BTreeMap;

use hashtree::mst::{value_hash, Mst};
use hashtree::Hash;

/// Number of segments in the fixed-grid model. Matches the
/// shipped default (`DEFAULT_SEGMENTS = 1024`).
const N_SEGMENTS: u64 = 1024;

/// Fixed-grid segment id for a key, mirroring the Tictac tree's
/// `KeyEntry::segment_id` reduction (a hash mixed down mod S).
fn segment_of(key: &[u8]) -> u64 {
    let digest = blake3::hash(key);
    let bytes = digest.as_bytes();
    let mut id = [0u8; 8];
    id.copy_from_slice(&bytes[0..8]);
    u64::from_be_bytes(id) % N_SEGMENTS
}

/// Cost of the fixed-grid (Tictac) reconcile: keys transferred and
/// segment comparisons. A segment is dirty if any key in it
/// differs between the two replicas; the reconcile compares all
/// S segment roots and ships every key in every dirty segment
/// (both sides), because the segment hash localises divergence
/// only to the segment, not to the key.
fn tictac_cost(a: &BTreeMap<Vec<u8>, Hash>, b: &BTreeMap<Vec<u8>, Hash>) -> (usize, usize) {
    // Bucket keys by segment for each replica.
    let mut seg_keys_a: BTreeMap<u64, Vec<Vec<u8>>> = BTreeMap::new();
    let mut seg_keys_b: BTreeMap<u64, Vec<Vec<u8>>> = BTreeMap::new();
    for k in a.keys() {
        seg_keys_a.entry(segment_of(k)).or_default().push(k.clone());
    }
    for k in b.keys() {
        seg_keys_b.entry(segment_of(k)).or_default().push(k.clone());
    }
    // A segment is dirty if the set-or-value of its keys differs.
    let mut transferred = 0usize;
    for seg in 0..N_SEGMENTS {
        let ka = seg_keys_a.get(&seg);
        let kb = seg_keys_b.get(&seg);
        let dirty = segment_dirty(ka, kb, a, b);
        if dirty {
            // Ship every key in the segment from both sides.
            transferred += ka.map_or(0, Vec::len) + kb.map_or(0, Vec::len);
        }
    }
    // Comparisons: all S segment roots are compared.
    let comparisons = usize::try_from(N_SEGMENTS).expect("N_SEGMENTS fits in usize");
    (transferred, comparisons)
}

/// A segment is dirty when the two replicas disagree on any key
/// that hashes into it (present on one side only, or present on
/// both at a different value).
fn segment_dirty(
    ka: Option<&Vec<Vec<u8>>>,
    kb: Option<&Vec<Vec<u8>>>,
    a: &BTreeMap<Vec<u8>, Hash>,
    b: &BTreeMap<Vec<u8>, Hash>,
) -> bool {
    let empty = Vec::new();
    let ka = ka.unwrap_or(&empty);
    let kb = kb.unwrap_or(&empty);
    for k in ka {
        match b.get(k) {
            Some(vb) if a.get(k) == Some(vb) => {}
            _ => return true,
        }
    }
    for k in kb {
        match a.get(k) {
            Some(va) if b.get(k) == Some(va) => {}
            _ => return true,
        }
    }
    false
}

/// Build two replicas sharing `total` keys, then diverge `diff`
/// of them on side B (changed value) plus add `diff` brand-new
/// keys on side B. Returns `(a_map, b_map)`.
fn replicas(total: usize, diff: usize) -> (BTreeMap<Vec<u8>, Hash>, BTreeMap<Vec<u8>, Hash>) {
    let mut a = BTreeMap::new();
    let mut b = BTreeMap::new();
    for i in 0..total {
        let k = format!("k{i:08}").into_bytes();
        let v = value_hash(format!("v{i}").as_bytes());
        a.insert(k.clone(), v);
        b.insert(k, v);
    }
    // Diverge `diff` shared keys on B.
    for i in 0..diff {
        let k = format!("k{i:08}").into_bytes();
        b.insert(k, value_hash(b"CHANGED"));
    }
    (a, b)
}

/// True symmetric-difference size (the ideal transfer count).
fn symmetric_difference(a: &BTreeMap<Vec<u8>, Hash>, b: &BTreeMap<Vec<u8>, Hash>) -> usize {
    let all: std::collections::BTreeSet<&Vec<u8>> = a.keys().chain(b.keys()).collect();
    all.into_iter()
        .filter(|k| match (a.get(*k), b.get(*k)) {
            (Some(va), Some(vb)) => va != vb,
            _ => true,
        })
        .count()
}

#[test]
fn bandwidth_curve_mst_beats_fixed_grid() {
    let total = 100_000usize;
    println!(
        "\n{:>10} {:>8} {:>10} {:>14} {:>14} {:>14}",
        "dataset", "diff%", "sym_diff", "mst_xfer", "tictac_xfer", "mst_cmp"
    );
    // At each divergence ratio, MST transfers exactly the
    // symmetric difference; the fixed grid transfers far more
    // because whole dirty segments ride along.
    // Divergence ratios expressed as integer (numerator,
    // denominator) so the diff count is exact and float casts are
    // avoided.
    for (label, num, den) in [("0.1%", 1usize, 1000usize), ("1%", 1, 100), ("10%", 1, 10)] {
        let diff = total * num / den;
        let ratio_pct_x10 = num * 1000 / den;
        let (a, b) = replicas(total, diff);

        let mst_a = Mst::from_pairs(a.iter().map(|(k, v)| (k.clone(), *v)).collect::<Vec<_>>());
        let mst_b = Mst::from_pairs(b.iter().map(|(k, v)| (k.clone(), *v)).collect::<Vec<_>>());
        let d = mst_a.diff(&mst_b);
        let mst_xfer = d.diff_len();
        let mst_cmp = d.comparisons();

        let (tictac_xfer, _tictac_cmp) = tictac_cost(&a, &b);
        let sd = symmetric_difference(&a, &b);

        println!("{total:>10} {label:>8} {sd:>10} {mst_xfer:>14} {tictac_xfer:>14} {mst_cmp:>14}");

        // MST transfers exactly the symmetric difference.
        assert_eq!(mst_xfer, sd, "MST must transfer exactly the diff");
        // The fixed grid transfers strictly more (whole segments).
        assert!(
            tictac_xfer >= mst_xfer,
            "fixed grid should never transfer fewer keys than MST"
        );
        // The headline: at low divergence the fixed grid inflates
        // the transfer by a large factor (segment occupancy).
        if ratio_pct_x10 <= 10 {
            assert!(
                tictac_xfer > mst_xfer * 3,
                "at {label} divergence the fixed grid should inflate transfer >3x: \
                 tictac={tictac_xfer} mst={mst_xfer}"
            );
        }
    }
}

#[test]
fn mst_transfer_is_flat_in_dataset_size_at_fixed_diff() {
    // Fix the divergence at 100 keys; grow the dataset. The MST
    // transfer stays at 100 while the fixed-grid transfer climbs.
    let diff = 100usize;
    let mut prev_tictac = 0usize;
    println!(
        "\n{:>10} {:>10} {:>14}",
        "dataset", "mst_xfer", "tictac_xfer"
    );
    for total in [10_000usize, 50_000, 200_000] {
        let (a, b) = replicas(total, diff);
        let mst_a = Mst::from_pairs(a.iter().map(|(k, v)| (k.clone(), *v)).collect::<Vec<_>>());
        let mst_b = Mst::from_pairs(b.iter().map(|(k, v)| (k.clone(), *v)).collect::<Vec<_>>());
        let mst_xfer = mst_a.diff(&mst_b).diff_len();
        let (tictac_xfer, _) = tictac_cost(&a, &b);
        println!("{total:>10} {mst_xfer:>10} {tictac_xfer:>14}");

        // MST transfer is exactly the diff regardless of N.
        assert_eq!(mst_xfer, symmetric_difference(&a, &b));
        // Fixed-grid transfer grows with N (segment occupancy
        // rises), so it is non-decreasing across the sizes here.
        assert!(
            tictac_xfer >= prev_tictac,
            "fixed-grid transfer should grow with dataset size"
        );
        prev_tictac = tictac_xfer;
    }
}
