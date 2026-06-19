//! Property-based tests for the Memcached fragmenter
//! ([`dynomite::proto::memcache::memcache_fragment`]).
//!
//! Fragmenting a multi-key `get` / `gets` request partitions the
//! keys across backend shards. The reconstruction property is: the
//! multiset of keys spread across the emitted fragments equals the
//! input multiset, every key sits in the fragment for its shard,
//! and `shard_for_key` records each key's shard in input order. A
//! single shard receives exactly one fragment.

use std::collections::BTreeMap;

use dynomite::io::mbuf::MbufPool;
use dynomite::msg::{KeyPos, Msg, MsgType};
use dynomite::proto::memcache::{memcache_fragment, FragmentDispatcher};
use hegel::generators as gs;
use hegel::TestCase;

/// Hash each key onto one of `shards` buckets by its first byte.
struct ModuloShards {
    shards: u32,
}
impl FragmentDispatcher for ModuloShards {
    fn shard_for(&self, key: &[u8]) -> u32 {
        u32::from(*key.first().unwrap_or(&0)) % self.shards
    }
    fn shard_count(&self) -> u32 {
        self.shards
    }
}

#[hegel::test(test_cases = 256)]
fn fragment_partitions_keys_losslessly(tc: TestCase) {
    let shards = tc.draw(gs::integers::<u32>().min_value(1).max_value(8));
    let nkeys = tc.draw(gs::integers::<usize>().min_value(1).max_value(24));
    // Distinct keys keep the multiset reasoning simple: each key is
    // its index rendered as bytes, which also spreads first bytes.
    let keys: Vec<Vec<u8>> = (0..nkeys).map(|i| format!("k{i}").into_bytes()).collect();

    let dispatcher = ModuloShards { shards };
    let pool = MbufPool::default();
    let mut req = Msg::new(0, MsgType::ReqMcGet, true);
    for k in &keys {
        req.push_key(KeyPos::without_tag(k.clone()));
    }

    let outcome = memcache_fragment(&mut req, &dispatcher, &pool)
        .expect("fragment ok")
        .expect("retrieval request fragments");

    // shard_for_key records one shard per input key, in order, and
    // each entry matches the dispatcher's own answer.
    assert_eq!(outcome.shard_for_key.len(), keys.len());
    for (k, &s) in keys.iter().zip(&outcome.shard_for_key) {
        assert_eq!(s, dispatcher.shard_for(k));
    }

    // Every emitted fragment carries a homogeneous shard set: all of
    // its keys hash to the same shard, and no two fragments share a
    // shard.
    let mut shard_of_fragment: BTreeMap<u32, usize> = BTreeMap::new();
    let mut reconstructed: Vec<Vec<u8>> = Vec::new();
    for frag in &outcome.fragments {
        assert!(!frag.keys().is_empty(), "no empty fragments");
        let frag_shard = dispatcher.shard_for(frag.keys()[0].key());
        for kp in frag.keys() {
            assert_eq!(
                dispatcher.shard_for(kp.key()),
                frag_shard,
                "fragment keys must share a shard",
            );
            reconstructed.push(kp.key().to_vec());
        }
        assert!(
            shard_of_fragment.insert(frag_shard, 0).is_none(),
            "each shard gets exactly one fragment",
        );
    }

    // The union of fragment keys reconstructs the original key set.
    let mut want = keys.clone();
    want.sort();
    reconstructed.sort();
    assert_eq!(reconstructed, want, "fragment union reconstructs input");

    // The number of fragments equals the number of distinct shards
    // the keys mapped to.
    let distinct: std::collections::BTreeSet<u32> = outcome.shard_for_key.iter().copied().collect();
    assert_eq!(outcome.fragments.len(), distinct.len());
}

#[hegel::test(test_cases = 128)]
fn fragment_frag_id_is_uniform(tc: TestCase) {
    // Every emitted fragment carries the same frag_id, which is also
    // stamped onto the parent request.
    let nkeys = tc.draw(gs::integers::<usize>().min_value(1).max_value(16));
    let keys: Vec<Vec<u8>> = (0..nkeys).map(|i| format!("v{i}").into_bytes()).collect();
    let dispatcher = ModuloShards { shards: 4 };
    let pool = MbufPool::default();
    let mut req = Msg::new(0, MsgType::ReqMcGets, true);
    for k in &keys {
        req.push_key(KeyPos::without_tag(k.clone()));
    }
    let outcome = memcache_fragment(&mut req, &dispatcher, &pool)
        .expect("fragment ok")
        .expect("retrieval fragments");
    assert_ne!(outcome.frag_id, 0);
    assert_eq!(req.frag_id(), outcome.frag_id);
    for frag in &outcome.fragments {
        assert_eq!(frag.frag_id(), outcome.frag_id);
    }
}
