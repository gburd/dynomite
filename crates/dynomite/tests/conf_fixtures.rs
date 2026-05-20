//! Per-fixture regression tests for every YAML configuration shipped
//! with the C reference. Each fixture must round-trip through
//! `Config::parse_str` -> `finalize` -> `validate` and the parsed
//! [`ConfPool`] must contain the field values the YAML asks for.

use std::path::Path;

use dynomite::conf::{ConfError, Config, ConsistencyLevel, DataStore, SecureServerOption};

const FIXTURES: &str = "tests/fixtures/conf";

fn load(name: &str) -> Config {
    let path = Path::new(FIXTURES).join(name);
    Config::parse_file(&path).unwrap_or_else(|e| panic!("parse {name} failed: {e}"))
}

fn finalize_and_validate(name: &str) -> Config {
    let mut cfg = load(name);
    cfg.finalize();
    cfg.validate()
        .unwrap_or_else(|e| panic!("validate {name} failed: {e}"));
    cfg
}

#[test]
fn dynomite_yml() {
    let cfg = finalize_and_validate("dynomite.yml");
    let p = cfg.pool();
    assert_eq!(cfg.pool_name(), "dyn_o_mite");
    assert_eq!(p.listen.as_ref().unwrap().pname(), "127.0.0.1:8102");
    assert_eq!(p.dyn_listen.as_ref().unwrap().pname(), "127.0.0.1:8101");
    assert_eq!(p.tokens.as_ref().unwrap().to_string(), "101134286");
    assert_eq!(p.servers.as_ref().unwrap().len(), 1);
    assert_eq!(p.data_store, Some(DataStore::Redis.as_int()));
    assert_eq!(p.mbuf_size, Some(16384));
    assert_eq!(p.max_msgs, Some(300_000));
}

#[test]
fn dynomite_dns_single_yml() {
    let cfg = finalize_and_validate("dynomite_dns_single.yml");
    let p = cfg.pool();
    assert_eq!(p.datacenter.as_deref(), Some("dc"));
    assert_eq!(p.rack.as_deref(), Some("rack1"));
    assert_eq!(p.dyn_seed_provider.as_deref(), Some("dns_provider"));
    assert_eq!(p.tokens.as_ref().unwrap().to_string(), "0");
    assert_eq!(p.data_store, Some(0));
    assert_eq!(p.stats_listen.as_ref().unwrap().pname(), "0.0.0.0:22222");
}

#[test]
fn dynomite_florida_single_yml() {
    let cfg = finalize_and_validate("dynomite_florida_single.yml");
    let p = cfg.pool();
    assert_eq!(p.dyn_seed_provider.as_deref(), Some("florida_provider"));
    assert_eq!(
        SecureServerOption::parse(p.secure_server_option.as_deref().unwrap()).unwrap(),
        SecureServerOption::Datacenter
    );
    assert_eq!(p.pem_key_file.as_deref(), Some("conf/dynomite.pem"));
}

#[test]
fn mc_single_yml() {
    let cfg = finalize_and_validate("mc_single.yml");
    let p = cfg.pool();
    assert_eq!(p.data_store, Some(DataStore::Memcache.as_int()));
    assert_eq!(p.tokens.as_ref().unwrap().to_string(), "437425602");
}

#[test]
fn node1_yml() {
    let cfg = finalize_and_validate("node1.yml");
    let p = cfg.pool();
    assert_eq!(p.datacenter.as_deref(), Some("dc1"));
    assert_eq!(p.rack.as_deref(), Some("rack1"));
    let seeds = p.dyn_seeds.as_ref().unwrap();
    assert_eq!(seeds.len(), 1);
    let s0 = &seeds[0];
    assert_eq!(s0.host(), "127.0.0.1");
    assert_eq!(s0.port(), 8113);
    assert_eq!(s0.rack(), "rack1");
    assert_eq!(s0.dc(), "dc2");
    assert_eq!(s0.tokens().to_string(), "101134286");
    assert_eq!(p.preconnect, Some(true));
    assert_eq!(p.dyn_seed_provider.as_deref(), Some("simple_provider"));
}

#[test]
fn node2_yml() {
    let cfg = finalize_and_validate("node2.yml");
    let p = cfg.pool();
    assert_eq!(p.datacenter.as_deref(), Some("dc2"));
    let seeds = p.dyn_seeds.as_ref().unwrap();
    assert_eq!(seeds.len(), 1);
    assert_eq!(seeds[0].dc(), "dc1");
}

#[test]
fn redis_dc1_yml() {
    let cfg = finalize_and_validate("redis_dc1.yml");
    let p = cfg.pool();
    assert_eq!(p.datacenter.as_deref(), Some("dc1"));
    assert_eq!(p.rack.as_deref(), Some("rack1"));
    assert_eq!(p.tokens.as_ref().unwrap().to_string(), "12345678");
    assert_eq!(p.datastore_connections, Some(3));
    assert_eq!(p.local_peer_connections, Some(3));
    assert_eq!(p.remote_peer_connections, Some(3));
    let seeds = p.dyn_seeds.as_ref().unwrap();
    assert_eq!(seeds.len(), 1);
    assert_eq!(seeds[0].host(), "127.0.0.2");
    assert_eq!(seeds[0].dc(), "dc2");
}

#[test]
fn redis_dc2_yml() {
    let cfg = finalize_and_validate("redis_dc2.yml");
    let p = cfg.pool();
    assert_eq!(p.datacenter.as_deref(), Some("dc2"));
    assert_eq!(p.rack.as_deref(), Some("rack2"));
    assert_eq!(p.tokens.as_ref().unwrap().to_string(), "1383429731");
}

#[test]
fn redis_node1_yml() {
    let cfg = finalize_and_validate("redis_node1.yml");
    let p = cfg.pool();
    assert_eq!(p.datacenter.as_deref(), Some("dc"));
    assert_eq!(p.tokens.as_ref().unwrap().to_string(), "12345678");
}

#[test]
fn redis_node2_yml() {
    let cfg = finalize_and_validate("redis_node2.yml");
    let p = cfg.pool();
    assert_eq!(p.tokens.as_ref().unwrap().to_string(), "1383429731");
}

#[test]
fn redis_rack1_node_yml() {
    let cfg = finalize_and_validate("redis_rack1_node.yml");
    let p = cfg.pool();
    assert_eq!(p.rack.as_deref(), Some("rack1"));
    let seeds = p.dyn_seeds.as_ref().unwrap();
    assert_eq!(seeds.len(), 2);
    assert_eq!(
        ConsistencyLevel::parse("read_consistency", p.read_consistency.as_deref().unwrap())
            .unwrap(),
        ConsistencyLevel::DcSafeQuorum
    );
    assert_eq!(
        ConsistencyLevel::parse("write_consistency", p.write_consistency.as_deref().unwrap())
            .unwrap(),
        ConsistencyLevel::DcSafeQuorum
    );
}

#[test]
fn redis_rack2_node_yml() {
    let cfg = finalize_and_validate("redis_rack2_node.yml");
    let p = cfg.pool();
    assert_eq!(p.rack.as_deref(), Some("rack2"));
    assert_eq!(p.dyn_seeds.as_ref().unwrap().len(), 2);
}

#[test]
fn redis_rack3_node_yml() {
    let cfg = finalize_and_validate("redis_rack3_node.yml");
    let p = cfg.pool();
    assert_eq!(p.rack.as_deref(), Some("rack3"));
    assert_eq!(p.dyn_seeds.as_ref().unwrap().len(), 2);
}

#[test]
fn redis_single_yml() {
    let cfg = finalize_and_validate("redis_single.yml");
    let p = cfg.pool();
    assert_eq!(p.dyn_seed_provider.as_deref(), Some("simple_provider"));
    assert_eq!(p.tokens.as_ref().unwrap().to_string(), "437425602");
}

#[test]
fn test_conf_returns_pool_name_for_every_fixture() {
    for name in [
        "dynomite.yml",
        "dynomite_dns_single.yml",
        "dynomite_florida_single.yml",
        "mc_single.yml",
        "node1.yml",
        "node2.yml",
        "redis_dc1.yml",
        "redis_dc2.yml",
        "redis_node1.yml",
        "redis_node2.yml",
        "redis_rack1_node.yml",
        "redis_rack2_node.yml",
        "redis_rack3_node.yml",
        "redis_single.yml",
    ] {
        let cfg = load(name);
        let report = cfg.test_conf().unwrap_or_else(|e| {
            panic!("test_conf failed for {name}: {e}");
        });
        assert!(
            report.contains(cfg.pool_name()),
            "report for {name} missing pool name"
        );
    }
}

#[test]
fn parse_file_io_error_surfaces() {
    let err = Config::parse_file(Path::new("does/not/exist.yml")).unwrap_err();
    assert!(matches!(err, ConfError::Io { .. }));
}
