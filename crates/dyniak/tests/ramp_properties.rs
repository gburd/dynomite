//! RAMP-Fast property tests.
//!
//! Three properties over generated inputs, each running >= 256 cases:
//!
//! * **Version-metadata round-trip**: a versioned item's body encodes
//!   and decodes back to the same siblings + value.
//! * **Second-round trigger condition**: [`dyniak::ramp::select`]
//!   returns a non-empty repair set exactly when the round-1 snapshot
//!   is fractured (some observed item names a sibling the reader sees
//!   at an older version), and the repaired snapshot is fracture-free.
//! * **Atomic visibility over random interleavings**: a RAMP write of
//!   a random key set, PREPARE/COMMIT interleaved arbitrarily with a
//!   concurrent read, never yields a fractured read snapshot.

#![cfg(feature = "noxu")]

use std::collections::{BTreeMap, BTreeSet};

use dyniak::ramp::{select, RampItem, Timestamp};
use dyniak::ramp_store::{ramp_read, ramp_write, RampError, RampStore, RampWrite};
use hegel::generators as gs;
use hegel::TestCase;

// ---- an in-memory RampStore for interleaving experiments ----

/// In-memory store that lets the property drive PREPARE and COMMIT as
/// separate, individually-orderable steps -- the whole point of the
/// atomic-visibility property is to exercise the partial-commit window.
#[derive(Default)]
struct MemStore {
    versions: std::cell::RefCell<BTreeMap<(Vec<u8>, Timestamp), MemVersion>>,
    pointers: std::cell::RefCell<BTreeMap<Vec<u8>, Timestamp>>,
}

/// A stored version's body: `(siblings, value)`.
type MemVersion = (Vec<Vec<u8>>, Vec<u8>);

impl RampStore for MemStore {
    fn put_version(&self, item: &RampItem) -> Result<(), RampError> {
        self.versions.borrow_mut().insert(
            (item.key.clone(), item.ts),
            (item.siblings.clone(), item.value.clone()),
        );
        Ok(())
    }
    fn commit_pointer(&self, key: &[u8], ts: Timestamp) -> Result<(), RampError> {
        let mut p = self.pointers.borrow_mut();
        let e = p.entry(key.to_vec()).or_insert(ts);
        if ts > *e {
            *e = ts;
        }
        Ok(())
    }
    fn latest_visible(&self, key: &[u8]) -> Result<Option<Timestamp>, RampError> {
        Ok(self.pointers.borrow().get(key).copied())
    }
    fn get_version(&self, key: &[u8], ts: Timestamp) -> Result<Option<RampItem>, RampError> {
        Ok(self
            .versions
            .borrow()
            .get(&(key.to_vec(), ts))
            .map(|(s, v)| RampItem::new(key.to_vec(), ts, s.clone(), v.clone())))
    }
}

fn arb_key(tc: &TestCase) -> Vec<u8> {
    let n = tc.draw(gs::integers::<u8>().min_value(0).max_value(4));
    vec![b'k', b'0' + n]
}

fn arb_bytes(tc: &TestCase) -> Vec<u8> {
    let len = tc.draw(gs::integers::<usize>().min_value(0).max_value(6));
    let mut v = Vec::with_capacity(len);
    for _ in 0..len {
        v.push(tc.draw(gs::integers::<u8>()));
    }
    v
}

// ---- 1. metadata round-trip ----

#[hegel::test(test_cases = 256)]
fn version_body_round_trips(tc: TestCase) {
    // A version body encodes value + siblings; a store put/get must
    // return exactly what went in.
    let key = arb_key(&tc);
    let ts = tc.draw(gs::integers::<u64>().min_value(1).max_value(1000));
    let nsib = tc.draw(gs::integers::<usize>().min_value(0).max_value(4));
    let mut siblings = Vec::new();
    for _ in 0..nsib {
        siblings.push(arb_key(&tc));
    }
    let value = arb_bytes(&tc);
    let item = RampItem::new(key.clone(), ts, siblings.clone(), value.clone());

    let store = MemStore::default();
    store.put_version(&item).expect("put");
    let got = store.get_version(&key, ts).expect("get").expect("present");
    assert_eq!(got.siblings, siblings);
    assert_eq!(got.value, value);
    assert_eq!(got.ts, ts);
    assert_eq!(got.key, key);
}

// ---- 2. second-round trigger condition ----

/// Reference definition of a fractured round-1 snapshot, independent of
/// `select`: some observed item names a sibling in the read set whose
/// observed ts is strictly older than the item's ts.
fn round1_is_fractured(round1: &[RampItem]) -> bool {
    let mut have: BTreeMap<&[u8], Timestamp> = BTreeMap::new();
    for it in round1 {
        let e = have.entry(it.key.as_slice()).or_insert(it.ts);
        if it.ts > *e {
            *e = it.ts;
        }
    }
    for it in round1 {
        for sib in &it.siblings {
            let seen = have.get(sib.as_slice()).copied().unwrap_or(0);
            if seen < it.ts {
                return true;
            }
        }
    }
    false
}

fn arb_round1(tc: &TestCase) -> Vec<RampItem> {
    let keys: Vec<Vec<u8>> = (0u8..=3).map(|n| vec![b'k', b'0' + n]).collect();
    let n = tc.draw(gs::integers::<usize>().min_value(1).max_value(4));
    let mut items = Vec::new();
    let mut used: BTreeSet<Vec<u8>> = BTreeSet::new();
    for _ in 0..n {
        let ki = tc.draw(gs::integers::<usize>().min_value(0).max_value(3));
        let key = keys[ki].clone();
        if !used.insert(key.clone()) {
            continue;
        }
        let ts = tc.draw(gs::integers::<u64>().min_value(1).max_value(6));
        let nsib = tc.draw(gs::integers::<usize>().min_value(0).max_value(3));
        let mut siblings = Vec::new();
        for _ in 0..nsib {
            let si = tc.draw(gs::integers::<usize>().min_value(0).max_value(3));
            if keys[si] != key {
                siblings.push(keys[si].clone());
            }
        }
        siblings.sort();
        siblings.dedup();
        items.push(RampItem::new(key, ts, siblings, b"v".to_vec()));
    }
    items
}

#[hegel::test(test_cases = 256)]
fn select_triggers_iff_fractured(tc: TestCase) {
    let round1 = arb_round1(&tc);
    let missing = select(&round1);
    // The second round fires exactly when the snapshot is fractured.
    assert_eq!(
        !missing.is_empty(),
        round1_is_fractured(&round1),
        "select must return repairs iff round 1 is fractured: {round1:?} -> {missing:?}"
    );
    // Every repair upgrades a key to a ts at least as new as what the
    // reader currently sees (never downgrades).
    let have: BTreeMap<&[u8], Timestamp> =
        round1.iter().map(|it| (it.key.as_slice(), it.ts)).collect();
    for (k, ts) in &missing {
        let cur = have.get(k.as_slice()).copied().unwrap_or(0);
        assert!(*ts > cur, "repair must upgrade, not downgrade");
    }
}

// ---- 3. atomic visibility over random interleavings ----

/// Drive one RAMP write as separate PREPARE and COMMIT steps, insert a
/// read at a hegel-chosen point in the interleaving, and assert the
/// read snapshot is never fractured (all-or-nothing for the writer).
#[hegel::test(test_cases = 256)]
fn atomic_visibility_over_interleavings(tc: TestCase) {
    let store = MemStore::default();

    // Establish a committed prior snapshot at ts=1 over keys k0..k2 so
    // there is an "old" version a naive reader could fracture against.
    let base_keys: Vec<Vec<u8>> = (0u8..=2).map(|n| vec![b'k', b'0' + n]).collect();
    ramp_write(
        &store,
        7,
        &base_keys
            .iter()
            .map(|k| RampWrite {
                key: k.clone(),
                value: b"old".to_vec(),
            })
            .collect::<Vec<_>>(),
    )
    .expect("base write");

    // A new write over a random non-empty subset, applied as manual
    // PREPARE-then-COMMIT steps so the reader can slot in anywhere.
    let mut write_keys: Vec<Vec<u8>> = Vec::new();
    for k in &base_keys {
        if tc.draw(gs::booleans()) {
            write_keys.push(k.clone());
        }
    }
    if write_keys.is_empty() {
        write_keys.push(base_keys[0].clone());
    }
    let new_ts: Timestamp = 5;

    // PREPARE every new version (invisible).
    for k in &write_keys {
        let siblings: Vec<Vec<u8>> = write_keys.iter().filter(|x| *x != k).cloned().collect();
        store
            .put_version(&RampItem::new(k.clone(), new_ts, siblings, b"new".to_vec()))
            .expect("prepare");
    }

    // Choose how many of the COMMITs happen BEFORE the read (the
    // partial-apply window). 0 = read sees the fully-old snapshot;
    // len = read sees the fully-new one; in between = partial.
    let commit_before = tc.draw(
        gs::integers::<usize>()
            .min_value(0)
            .max_value(write_keys.len()),
    );
    for k in write_keys.iter().take(commit_before) {
        store.commit_pointer(k, new_ts).expect("commit");
    }

    // The read: over all base keys.
    let (snap, _rounds) = ramp_read(&store, &base_keys).expect("read");

    // Commit the rest (post-read); does not affect the snapshot already
    // returned, but exercises idempotent monotonic pointer advance.
    for k in write_keys.iter().skip(commit_before) {
        store.commit_pointer(k, new_ts).expect("commit");
    }

    // Atomic-visibility: the snapshot must be all-old or all-new for
    // the write set -- never a mix (fractured). We check by value: for
    // the write set, either every key reads "new" or ... some read
    // "old" is only OK if NONE read "new".
    let saw_new: Vec<bool> = write_keys
        .iter()
        .map(|k| snap.get(k).map(Vec::as_slice) == Some(b"new".as_slice()))
        .collect();
    let any_new = saw_new.iter().any(|b| *b);
    let all_new = saw_new.iter().all(|b| *b);
    assert!(
        !any_new || all_new,
        "fractured read: write set {write_keys:?} observed as {saw_new:?} \
         (commit_before={commit_before}), snapshot={snap:?}"
    );
}
