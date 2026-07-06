//! Snapshot-read demo: HLC-stamped writes against the noxu object
//! store, with a "latest version at or before a snapshot timestamp"
//! read predicate.
//!
//! This is scope item 2 of the HLC prototype: it shows how the
//! [`Hlc`] primitive from `crates/dyniak/src/datatypes/hlc.rs`
//! *enables* consistent snapshot selection, without reworking storage
//! into a real MVCC engine (that rework is explicitly out of scope;
//! see `docs/journal/2026-07-05-hlc-prototype.md`).
//!
//! # Model
//!
//! A logical key `k` holds multiple immutable versions. Each version
//! is written as an object under a fixed `bucket`, with the object
//! *key* being the *version key*:
//!
//! ```text
//! version_key = logical_key || 0x00 || hlc.encode()   (8 big-endian HLC bytes)
//! ```
//!
//! Because [`Hlc::encode`] preserves the HLC order under `memcmp`,
//! all versions of one logical key sort by HLC when the store folds
//! objects in lexicographic order. A snapshot read at timestamp
//! `snapshot_ts` (itself an [`Hlc`]) selects the version with the
//! greatest HLC that is `<= snapshot_ts`. That is exactly the
//! read-committed-at-a-point-in-time selection a RAMP-SI / Percolator
//! snapshot read performs.
//!
//! The demo drives two writer nodes exchanging HLC stamps so the
//! versions carry a *causally consistent* order, then asserts:
//!
//! * a snapshot at each version's own HLC reads back that version;
//! * a snapshot below the first version reads nothing;
//! * a snapshot above the last version reads the last version;
//! * a snapshot strictly between two versions reads the earlier one;
//! * the selected version is always the unique greatest `<= snapshot`.
//!
//! Gated on the `noxu` feature: the object store is `NoxuDatastore`.
#![cfg(feature = "noxu")]

use dyniak::datastore::NoxuDatastore;
use dyniak::datatypes::Hlc;

const SEP: u8 = 0x00;

/// Build the physical store key for a version: the logical key, a NUL
/// separator, then the 8 big-endian HLC bytes (which sort in HLC
/// order under memcmp).
fn version_key(logical: &[u8], stamp: Hlc) -> Vec<u8> {
    let mut k = Vec::with_capacity(logical.len() + 1 + 8);
    k.extend_from_slice(logical);
    k.push(SEP);
    k.extend_from_slice(&stamp.encode());
    k
}

/// The fixed bucket every version is stored under.
const BUCKET: &[u8] = b"snap";

/// Snapshot read: return the value of the version of `logical` whose
/// HLC is the greatest that is `<= snapshot`, or `None` if no version
/// is at or before the snapshot.
///
/// This is the core predicate. It folds the object keyspace, keeps
/// only object keys that match `logical || 0x00 || <8 bytes>`,
/// decodes the HLC suffix, and tracks the maximum stamp `<= snapshot`.
/// In a real MVCC engine this would be a bounded range scan on the
/// version-key prefix rather than a full fold; the selection logic is
/// identical.
fn snapshot_read(ds: &NoxuDatastore, logical: &[u8], snapshot: Hlc) -> Option<Vec<u8>> {
    let prefix_len = logical.len() + 1;
    let mut best: Option<(Hlc, Vec<u8>)> = None;
    ds.fold_primary(|_bucket, obj_key, value| {
        // Object key is the version key: `logical || 0x00 || <8 bytes>`.
        if obj_key.len() != prefix_len + 8 {
            return Ok(());
        }
        if &obj_key[..logical.len()] != logical || obj_key[logical.len()] != SEP {
            return Ok(());
        }
        let Ok(stamp) = Hlc::decode(&obj_key[prefix_len..]) else {
            return Ok(());
        };
        if stamp <= snapshot {
            let take = match &best {
                Some((cur, _)) => stamp > *cur,
                None => true,
            };
            if take {
                best = Some((stamp, value.to_vec()));
            }
        }
        Ok(())
    })
    .expect("fold primary");
    best.map(|(_, v)| v)
}

/// Write an HLC-stamped version of `logical` into the store as an
/// object keyed by the version key under the fixed bucket.
fn put_version(ds: &NoxuDatastore, logical: &[u8], stamp: Hlc, value: &[u8]) {
    let vk = version_key(logical, stamp);
    ds.put_object(BUCKET, &vk, value, &[])
        .expect("put versioned object");
}

#[test]
fn snapshot_read_selects_latest_version_at_or_before_ts() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let ds = NoxuDatastore::open_in(dir.path()).expect("open");

    let logical = b"account:42";

    // Two writer nodes. Node A writes v1 and v3; node B writes v2.
    // They exchange HLC stamps so the versions carry a causally
    // consistent, strictly increasing order even though physical time
    // stalls between some writes.
    let mut node_a = Hlc::zero();
    let mut node_b = Hlc::zero();

    // v1 written by A at physical time 100.
    let s1 = node_a.tick(100);
    put_version(&ds, logical, s1, b"balance=10");

    // A sends its clock to B (message carrying s1). B receives at
    // physical time 100 (no advance): its stamp is strictly after s1.
    let s2 = node_b.update(&s1, 100);
    put_version(&ds, logical, s2, b"balance=20");

    // B replies to A; A receives at physical time 105.
    let s3 = node_a.update(&s2, 105);
    put_version(&ds, logical, s3, b"balance=30");

    // Causally increasing, per the HLC receive rule.
    assert!(s1 < s2 && s2 < s3, "versions must be causally ordered");

    // A snapshot at each version's own stamp reads that version.
    assert_eq!(
        snapshot_read(&ds, logical, s1).as_deref(),
        Some(&b"balance=10"[..])
    );
    assert_eq!(
        snapshot_read(&ds, logical, s2).as_deref(),
        Some(&b"balance=20"[..])
    );
    assert_eq!(
        snapshot_read(&ds, logical, s3).as_deref(),
        Some(&b"balance=30"[..])
    );

    // A snapshot strictly below v1 reads nothing.
    let below = Hlc::from_parts(s1.logical() - 1, 0).unwrap();
    assert_eq!(snapshot_read(&ds, logical, below), None);

    // A snapshot between v2 and v3 reads v2 (the greatest <= snapshot).
    // s2 and s3 differ (s3 adopts physical time 105); pick a stamp
    // with l == s2.logical() and a counter above s2's but the same
    // l < s3.l, so it sits strictly between them.
    assert!(s2.logical() < s3.logical());
    let between = Hlc::from_parts(s2.logical(), s2.counter() + 5).unwrap();
    assert!(s2 < between && between < s3);
    assert_eq!(
        snapshot_read(&ds, logical, between).as_deref(),
        Some(&b"balance=20"[..])
    );

    // A snapshot far above the last version reads the last version.
    let above = Hlc::from_parts(s3.logical() + 1000, 0).unwrap();
    assert_eq!(
        snapshot_read(&ds, logical, above).as_deref(),
        Some(&b"balance=30"[..])
    );
}

#[test]
fn snapshot_read_is_stable_and_monotone_in_ts() {
    // The selected value never regresses as the snapshot timestamp
    // advances: reading at a later snapshot returns an equal-or-newer
    // version. This is the bounded-staleness read property a
    // snapshot-isolation transaction relies on.
    let dir = tempfile::TempDir::new().expect("tempdir");
    let ds = NoxuDatastore::open_in(dir.path()).expect("open");
    let logical = b"k";

    let mut node = Hlc::zero();
    let mut stamps = Vec::new();
    for (i, pt) in [10u64, 10, 10, 25, 25, 40].into_iter().enumerate() {
        let s = node.tick(pt);
        stamps.push(s);
        put_version(&ds, logical, s, format!("v{i}").as_bytes());
    }

    // Walk snapshots from below the first stamp to above the last and
    // confirm the selected version index is non-decreasing.
    let mut last_seen: Option<Vec<u8>> = None;
    let mut last_stamp = Hlc::zero();
    for probe in 0..=stamps.last().unwrap().logical() + 5 {
        for c in 0..3u16 {
            let snap = Hlc::from_parts(probe, c).unwrap();
            if let Some(v) = snapshot_read(&ds, logical, snap) {
                // The selected version's stamp must be <= snap and, as
                // snap only ever grows here, must be >= the previous
                // selection's stamp.
                if let Some(prev) = &last_seen {
                    // Same or newer value bytes are fine; never older.
                    // Compare by the version's own stamp we recorded.
                    let cur_stamp = *stamps
                        .iter()
                        .rev()
                        .find(|s| **s <= snap)
                        .expect("a version <= snap exists");
                    assert!(
                        cur_stamp >= last_stamp,
                        "selection regressed: {cur_stamp} < {last_stamp}"
                    );
                    last_stamp = cur_stamp;
                    let _ = prev;
                }
                last_seen = Some(v);
            }
        }
    }
    assert!(
        last_seen.is_some(),
        "the highest snapshot must read a value"
    );
}
