//! Coverage for the small utility containers and address helper
//! (`util::rbtree`, `util::dict`, `util::sockinfo`).
//!
//! These wrappers are exercised by doctests, but doctests do not
//! contribute to the integration-test coverage profile, so the
//! thin accessor and iterator methods are pinned here as
//! integration tests.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use dynomite::util::dict::{DictMap, MsgIndex};
use dynomite::util::rbtree::OrderedMap;
use dynomite::util::sockinfo::SockInfo;

// -------------------------------------------------------------
// OrderedMap: the wrapper accessors not covered by the unit tests
// (get / remove / len / is_empty / clear).
// -------------------------------------------------------------

#[test]
fn ordered_map_get_remove_len_clear() {
    let mut t: OrderedMap<u64, &'static str> = OrderedMap::new();
    assert!(t.is_empty());
    assert_eq!(t.len(), 0);
    t.insert(10, "ten");
    t.insert(20, "twenty");
    assert_eq!(t.len(), 2);
    assert!(!t.is_empty());
    assert_eq!(t.get(&10), Some(&"ten"));
    assert_eq!(t.get(&99), None);
    assert_eq!(t.remove(&10), Some("ten"));
    assert_eq!(t.remove(&10), None);
    assert_eq!(t.len(), 1);
    t.clear();
    assert!(t.is_empty());
}

#[test]
fn ordered_map_lower_bound_min_max() {
    let mut t: OrderedMap<u64, u64> = OrderedMap::new();
    assert!(t.lower_bound(&0).is_none());
    assert!(t.min().is_none());
    assert!(t.max().is_none());
    for k in [30u64, 10, 20] {
        t.insert(k, k);
    }
    assert_eq!(t.lower_bound(&15), Some((&20, &20)));
    assert_eq!(t.lower_bound(&20), Some((&20, &20)));
    assert!(t.lower_bound(&31).is_none());
    assert_eq!(t.min(), Some((&10, &10)));
    assert_eq!(t.max(), Some((&30, &30)));
}

// -------------------------------------------------------------
// DictMap: get_mut / iter_mut / drain / into_iter.
// -------------------------------------------------------------

#[test]
fn dict_get_mut_mutates_in_place() {
    let mut m: DictMap<u32, u32> = DictMap::with_capacity(4);
    m.insert(1, 10);
    *m.get_mut(&1).unwrap() = 99;
    assert_eq!(m.get(&1), Some(&99));
    assert!(m.get_mut(&2).is_none());
}

#[test]
fn dict_iter_mut_updates_every_value() {
    let mut m: DictMap<u32, u32> = DictMap::new();
    for i in 0..4 {
        m.insert(i, i);
    }
    for (_k, v) in m.iter_mut() {
        *v += 100;
    }
    let mut vals: Vec<u32> = m.iter().map(|(_, v)| *v).collect();
    vals.sort_unstable();
    assert_eq!(vals, vec![100, 101, 102, 103]);
}

#[test]
fn dict_drain_empties_the_map() {
    let mut m: DictMap<u32, u32> = DictMap::new();
    m.insert(1, 10);
    m.insert(2, 20);
    let mut drained: Vec<(u32, u32)> = m.drain().collect();
    drained.sort_unstable();
    assert_eq!(drained, vec![(1, 10), (2, 20)]);
    assert!(m.is_empty());
}

#[test]
fn dict_into_iter_consumes_the_map() {
    let mut m: DictMap<u32, u32> = DictMap::new();
    m.insert(5, 50);
    let mut collected: Vec<(u32, u32)> = m.into_iter().collect();
    collected.sort_unstable();
    assert_eq!(collected, vec![(5, 50)]);
}

#[test]
fn msg_index_alias() {
    let mut idx: MsgIndex<&'static str> = MsgIndex::new();
    idx.insert(42, "hello");
    assert_eq!(idx.get(&42), Some(&"hello"));
    assert!(idx.contains_key(&42));
}

// -------------------------------------------------------------
// SockInfo: the Unix as_socket_addr arm and the explicit
// constructors from_v4 / from_v6.
// -------------------------------------------------------------

#[test]
fn sockinfo_unix_has_no_socket_addr() {
    let u = SockInfo::resolve("/tmp/dynomite.sock", 0).unwrap();
    assert!(!u.is_inet());
    assert!(u.as_socket_addr().is_none());
}

#[test]
fn sockinfo_from_v4_constructor() {
    let s = SockInfo::from_v4(Ipv4Addr::new(10, 0, 0, 1), 6379);
    assert!(s.is_inet());
    let addr = s.as_socket_addr().unwrap();
    assert_eq!(addr.ip(), IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
    assert_eq!(addr.port(), 6379);
    assert!(matches!(s, SockInfo::Inet(_)));
}

#[test]
fn sockinfo_from_v6_constructor() {
    let s = SockInfo::from_v6(Ipv6Addr::LOCALHOST, 6380);
    assert!(s.is_inet());
    let addr = s.as_socket_addr().unwrap();
    assert_eq!(addr.ip(), IpAddr::V6(Ipv6Addr::LOCALHOST));
    assert_eq!(addr.port(), 6380);
    assert!(matches!(s, SockInfo::Inet6(_)));
}

#[test]
fn sockinfo_resolve_v6_literal() {
    let s = SockInfo::resolve("::1", 6379).unwrap();
    assert!(matches!(s, SockInfo::Inet6(_)));
}

#[test]
fn sockinfo_resolve_rejects_zero_port() {
    let err = SockInfo::resolve("127.0.0.1", 0).unwrap_err();
    assert!(err.to_string().contains("invalid port"));
}
