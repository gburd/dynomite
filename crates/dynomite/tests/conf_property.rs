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

use proptest::prelude::*;

fn arb_pool_name() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_]{0,15}".prop_filter("non-empty", |s| !s.is_empty())
}

fn arb_listen_v4() -> impl Strategy<Value = String> {
    (1u8..=254u8, 1u32..=65_535u32).prop_map(|(o, p)| format!("127.0.0.{o}:{p}"))
}

fn arb_token_value() -> impl Strategy<Value = String> {
    proptest::sample::select(vec![
        "0".to_string(),
        "1".to_string(),
        "12345678".to_string(),
        "101134286".to_string(),
        "437425602".to_string(),
        "1383429731".to_string(),
    ])
}

fn arb_consistency() -> impl Strategy<Value = ConsistencyLevel> {
    proptest::sample::select(vec![
        ConsistencyLevel::DcOne,
        ConsistencyLevel::DcQuorum,
        ConsistencyLevel::DcSafeQuorum,
        ConsistencyLevel::DcEachSafeQuorum,
    ])
}

fn arb_secure() -> impl Strategy<Value = SecureServerOption> {
    proptest::sample::select(vec![
        SecureServerOption::None,
        SecureServerOption::Rack,
        SecureServerOption::Datacenter,
        SecureServerOption::All,
    ])
}

fn arb_hash() -> impl Strategy<Value = HashType> {
    proptest::sample::select(vec![
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
    ])
}

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

fn arb_pool_facts() -> impl Strategy<Value = PoolFacts> {
    let common = (
        arb_pool_name(),
        arb_listen_v4(),
        arb_listen_v4(),
        arb_token_value(),
        (1u8..=254u8, 1u32..=65_535u32, 1u32..=10u32),
        arb_secure(),
        arb_consistency(),
        arb_consistency(),
    );
    let extra = (
        arb_hash(),
        proptest::sample::select(vec![DataStore::Redis, DataStore::Memcache]),
        1i64..=60_000i64,
        prop::option::of(prop::sample::select(vec![512i64, 1024, 4096, 16384, 65536])),
        prop::option::of(100_000i64..=1_000_000i64),
    );
    (common, extra).prop_map(
        |(
            (
                name,
                listen,
                dyn_listen,
                tokens,
                (host, port, weight),
                secure,
                read_consistency,
                write_consistency,
            ),
            (hash, data_store, timeout, mbuf_size, max_msgs),
        )| {
            let server = format!("127.0.0.{host}:{port}:{weight}");
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
        },
    )
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

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        ..ProptestConfig::default()
    })]

    #[test]
    fn parse_finalize_validate_round_trip(facts in arb_pool_facts()) {
        let yaml = render(&facts);
        let mut cfg = Config::parse_str(&yaml).expect("parse");
        prop_assert_eq!(cfg.pool_name(), &facts.name);
        prop_assert_eq!(
            cfg.pool().listen.as_ref().unwrap().pname(),
            &facts.listen
        );
        prop_assert_eq!(
            cfg.pool().dyn_listen.as_ref().unwrap().pname(),
            &facts.dyn_listen
        );
        prop_assert_eq!(
            cfg.pool().tokens.as_ref().unwrap().to_string(),
            facts.tokens.clone()
        );
        prop_assert_eq!(cfg.pool().data_store, Some(facts.data_store.as_int()));
        prop_assert_eq!(cfg.pool().timeout, Some(facts.timeout));
        cfg.finalize();
        cfg.validate().expect("validate");
    }

    #[test]
    fn out_of_range_mbuf_rejected_by_validation(
        facts in arb_pool_facts(),
        bogus in proptest::sample::select(vec![-1i64, 0, 100, 200, 511, 700, 99999, 600_000])
    ) {
        let mut bad = facts.clone();
        bad.mbuf_size = Some(bogus);
        let yaml = render(&bad);
        let mut cfg = Config::parse_str(&yaml).expect("parse");
        cfg.finalize();
        let result = cfg.validate();
        let is_oor = matches!(result, Err(ConfError::OutOfRange { .. }));
        prop_assert!(is_oor);
    }
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
