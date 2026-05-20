//! End-to-end routing determinism for the hashkit distributions.

use dynomite::hashkit::ketama::{Continuum as Ketama, ServerSpec as KetamaServer};
use dynomite::hashkit::modula::{Continuum as Modula, ServerSpec as ModulaServer};
use dynomite::hashkit::{hash, DynToken, HashType};

const KEYS: &[&[u8]] = &[
    b"alpha",
    b"beta",
    b"gamma",
    b"delta",
    b"epsilon",
    b"zeta",
    b"eta",
    b"theta",
    b"netflix:dynomite:rs",
    b"the quick brown fox",
    b"123456789",
    b"\x00\x01\x02\x03",
];

fn ketama_servers() -> Vec<KetamaServer> {
    (0..6)
        .map(|i| KetamaServer {
            name: format!("server-{i}"),
            weight: 1,
        })
        .collect()
}

fn modula_servers() -> Vec<ModulaServer> {
    (0..4)
        .map(|i| ModulaServer {
            name: format!("server-{i}"),
            weight: 2,
        })
        .collect()
}

#[test]
fn ketama_routes_each_key_to_a_stable_server() {
    let servers = ketama_servers();
    let cont = Ketama::build(&servers).expect("build");
    let mut first = Vec::new();
    for key in KEYS {
        let token = hash(HashType::Md5, key);
        first.push(cont.dispatch(&token).expect("dispatch"));
    }
    // Repeat 16 times; every dispatch must agree.
    for _ in 0..16 {
        for (i, key) in KEYS.iter().enumerate() {
            let token = hash(HashType::Md5, key);
            assert_eq!(cont.dispatch(&token).expect("dispatch"), first[i]);
        }
    }
}

#[test]
fn ketama_balanced_distribution_is_not_degenerate() {
    let servers = ketama_servers();
    let cont = Ketama::build(&servers).expect("build");
    let mut counts = vec![0usize; servers.len()];
    for h in 0u32..2048 {
        // Spread the probes across the full u32 space; sequential
        // small values all sort before the first continuum point and
        // would just measure the wrap-around bucket.
        let probe = DynToken::from_u32(h.wrapping_mul(0x9e37_79b9));
        let s = cont.dispatch(&probe).expect("dispatch");
        counts[s] += 1;
    }
    // At least 4 of the 6 servers must appear in 2k samples.
    let touched = counts.iter().filter(|c| **c > 0).count();
    assert!(
        touched >= 4,
        "ketama left {} servers idle: {:?}",
        servers.len() - touched,
        counts
    );
}

#[test]
fn modula_routing_matches_modulus() {
    let servers = modula_servers();
    let cont = Modula::build(&servers).expect("build");
    assert_eq!(cont.len(), 8);
    for h in 0u32..32 {
        let i = (h as usize) % cont.len();
        assert_eq!(cont.dispatch(h).expect("dispatch"), cont.slots()[i].server);
    }
}

#[test]
fn modula_routes_each_key_to_a_stable_server() {
    let servers = modula_servers();
    let cont = Modula::build(&servers).expect("build");
    let mut first = Vec::new();
    for key in KEYS {
        let token = hash(HashType::Crc32a, key);
        first.push(cont.dispatch(token.get_int()).expect("dispatch"));
    }
    for _ in 0..16 {
        for (i, key) in KEYS.iter().enumerate() {
            let token = hash(HashType::Crc32a, key);
            assert_eq!(cont.dispatch(token.get_int()).expect("dispatch"), first[i]);
        }
    }
}

#[test]
fn ketama_topology_change_is_local() {
    // Add a new server. At least one key continues to land on the same
    // index (the property that motivates ketama: small topology changes
    // perturb only a small fraction of the keyspace).
    let mut servers = ketama_servers();
    let before = Ketama::build(&servers).expect("before");
    servers.push(KetamaServer {
        name: "new-server".into(),
        weight: 1,
    });
    let after = Ketama::build(&servers).expect("after");

    let mut stable = 0;
    for h in 0u32..256 {
        let token = DynToken::from_u32(h.wrapping_mul(0x9e37_79b9));
        if before.dispatch(&token).unwrap() == after.dispatch(&token).unwrap() {
            stable += 1;
        }
    }
    assert!(
        stable > 50,
        "ketama lost too many keys after adding one server: {stable}/256"
    );
}
