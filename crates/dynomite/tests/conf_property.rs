//! Property-based round-trip tests for [`dynomite::conf`].
//!
//! Generates randomly-shaped (but always valid) configurations,
//! serializes them through `serde_yaml`, parses the result back with
//! [`Config::parse_str`], and asserts that `finalize` + `validate`
//! both succeed and that key fields survive the round-trip
//! unchanged.

#![allow(
    clippy::format_push_string,
    clippy::needless_continue,
    clippy::unnecessary_debug_formatting,
    clippy::match_same_arms,
    clippy::similar_names,
    clippy::cast_possible_truncation
)]
use dynomite::conf::{
    ConfError, ConfListen, ConfPool, Config, ConsistencyLevel, DataStore, HashType,
    SecureServerOption, Servers, TokenList,
};

use hegel::generators as gs;
use hegel::{Generator, TestCase};

#[derive(Debug, Clone)]
struct PoolFacts {
    name: String,
    listen: String,
    dyn_listen: String,
    tokens: String,
    server: String,
    secure: SecureServerOption,
    read_consistency: ConsistencyLevel,
    write_consistency: ConsistencyLevel,
    hash: HashType,
    data_store: DataStore,
    timeout: i64,
    mbuf_size: Option<i64>,
    max_msgs: Option<i64>,
}

#[hegel::composite]
fn arb_pool_facts(tc: TestCase) -> PoolFacts {
    let name = tc.draw(
        gs::from_regex("[a-z][a-z0-9_]{0,15}")
            .fullmatch(true)
            .filter(|s: &String| !s.is_empty()),
    );
    let listen = {
        let o = tc.draw(gs::integers::<u8>().min_value(1).max_value(254));
        let p = tc.draw(gs::integers::<u32>().min_value(1).max_value(65_535));
        format!("127.0.0.{o}:{p}")
    };
    let dyn_listen = {
        let o = tc.draw(gs::integers::<u8>().min_value(1).max_value(254));
        let p = tc.draw(gs::integers::<u32>().min_value(1).max_value(65_535));
        format!("127.0.0.{o}:{p}")
    };
    let tokens = tc.draw(gs::sampled_from(&[
        "0".to_string(),
        "1".to_string(),
        "12345678".to_string(),
        "101134286".to_string(),
        "437425602".to_string(),
        "1383429731".to_string(),
    ]));
    let host = tc.draw(gs::integers::<u8>().min_value(1).max_value(254));
    let port = tc.draw(gs::integers::<u32>().min_value(1).max_value(65_535));
    let weight = tc.draw(gs::integers::<u32>().min_value(1).max_value(10));
    let server = format!("127.0.0.{host}:{port}:{weight}");
    let secure = tc.draw(gs::sampled_from(&[
        SecureServerOption::None,
        SecureServerOption::Rack,
        SecureServerOption::Datacenter,
        SecureServerOption::All,
    ]));
    let read_consistency = tc.draw(gs::sampled_from(&[
        ConsistencyLevel::DcOne,
        ConsistencyLevel::DcQuorum,
        ConsistencyLevel::DcSafeQuorum,
        ConsistencyLevel::DcEachSafeQuorum,
    ]));
    let write_consistency = tc.draw(gs::sampled_from(&[
        ConsistencyLevel::DcOne,
        ConsistencyLevel::DcQuorum,
        ConsistencyLevel::DcSafeQuorum,
        ConsistencyLevel::DcEachSafeQuorum,
    ]));
    let hash = tc.draw(gs::sampled_from(&[
        HashType::OneAtATime,
        HashType::Md5,
        HashType::Crc16,
        HashType::Crc32,
        HashType::Crc32a,
        HashType::Fnv1_64,
        HashType::Fnv1a64,
        HashType::Fnv1_32,
        HashType::Fnv1a32,
        HashType::Hsieh,
        HashType::Murmur,
        HashType::Jenkins,
        HashType::Murmur3,
    ]));
    let data_store = tc.draw(gs::sampled_from(&[DataStore::Valkey, DataStore::Memcache]));
    let timeout = tc.draw(gs::integers::<i64>().min_value(1).max_value(60_000));
    let mbuf_size = tc.draw(gs::optional(gs::sampled_from(&[
        512i64, 1024, 4096, 16384, 65536,
    ])));
    let max_msgs = tc.draw(gs::optional(
        gs::integers::<i64>()
            .min_value(100_000)
            .max_value(1_000_000),
    ));
    PoolFacts {
        name,
        listen,
        dyn_listen,
        tokens,
        server,
        secure,
        read_consistency,
        write_consistency,
        hash,
        data_store,
        timeout,
        mbuf_size,
        max_msgs,
    }
}

fn render(facts: &PoolFacts) -> String {
    let mut s = String::new();
    s.push_str(&facts.name);
    s.push_str(":\n");
    s.push_str(&format!("  listen: {}\n", facts.listen));
    s.push_str(&format!("  dyn_listen: {}\n", facts.dyn_listen));
    s.push_str(&format!("  tokens: '{}'\n", facts.tokens));
    s.push_str("  servers:\n");
    s.push_str(&format!("  - {}\n", facts.server));
    s.push_str(&format!("  data_store: {}\n", facts.data_store.as_int()));
    s.push_str(&format!(
        "  secure_server_option: {}\n",
        facts.secure.as_str()
    ));
    s.push_str(&format!("  hash: {}\n", facts.hash.as_str()));
    s.push_str(&format!(
        "  read_consistency: {}\n",
        facts.read_consistency.as_str()
    ));
    s.push_str(&format!(
        "  write_consistency: {}\n",
        facts.write_consistency.as_str()
    ));
    s.push_str(&format!("  timeout: {}\n", facts.timeout));
    if facts.secure != SecureServerOption::None {
        s.push_str("  pem_key_file: conf/dynomite.pem\n");
    }
    if let Some(m) = facts.mbuf_size {
        s.push_str(&format!("  mbuf_size: {m}\n"));
    }
    if let Some(m) = facts.max_msgs {
        s.push_str(&format!("  max_msgs: {m}\n"));
    }
    s
}

#[hegel::test(test_cases = 256)]
fn parse_finalize_validate_round_trip(tc: TestCase) {
    let facts = tc.draw(arb_pool_facts());
    let yaml = render(&facts);
    let mut cfg = Config::parse_str(&yaml).expect("parse");
    assert_eq!(cfg.pool_name(), &facts.name);
    assert_eq!(cfg.pool().listen.as_ref().unwrap().pname(), &facts.listen);
    assert_eq!(
        cfg.pool().dyn_listen.as_ref().unwrap().pname(),
        &facts.dyn_listen
    );
    assert_eq!(
        cfg.pool().tokens.as_ref().unwrap().to_string(),
        facts.tokens.clone()
    );
    assert_eq!(cfg.pool().data_store, Some(facts.data_store.as_int()));
    assert_eq!(cfg.pool().timeout, Some(facts.timeout));
    cfg.finalize();
    cfg.validate().expect("validate");
}

#[hegel::test(test_cases = 256)]
fn out_of_range_mbuf_rejected_by_validation(tc: TestCase) {
    let facts = tc.draw(arb_pool_facts());
    let bogus = tc.draw(gs::sampled_from(&[
        -1i64, 0, 100, 200, 511, 700, 99999, 600_000,
    ]));
    let mut bad = facts.clone();
    bad.mbuf_size = Some(bogus);
    let yaml = render(&bad);
    let mut cfg = Config::parse_str(&yaml).expect("parse");
    cfg.finalize();
    let result = cfg.validate();
    let is_oor = matches!(result, Err(ConfError::OutOfRange { .. }));
    assert!(is_oor);
}

#[test]
fn building_pool_programmatically_is_validatable() {
    let mut p = ConfPool {
        listen: Some(ConfListen::parse("listen", "127.0.0.1:8102").unwrap()),
        servers: Some(Servers::from_vec(vec![dynomite::conf::ConfServer::parse(
            "127.0.0.1:6379:1",
        )
        .unwrap()])),
        tokens: Some(TokenList::parse("0").unwrap()),
        ..ConfPool::default()
    };
    p.apply_defaults();
    p.validate("p").unwrap();
}
