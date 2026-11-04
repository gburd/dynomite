//! Timestamp read repair under concurrent writers (DEV-R1 evidence).
//!
//! One DC of three single-node racks at DC_SAFE_QUORUM with read repairs
//! on. Several clients, each entering through a different node, race
//! conflicting SET / DEL / HSET / HDEL on a few keys. Afterwards a
//! repairing read of every key must leave every replica's datastore in
//! the same state: the same value (or absence) and the same add / remove
//! metadata for that key. Run against the C reference first (real Lua),
//! then the Rust engine; timestamps differ between runs, so the check is
//! structural: replicas agree with each other.

#[macro_use]
mod common;

use std::time::Duration;

use common::start_or_skip;
use optest::cluster::Engine;
use optest::resp::Value;
use optest::ClusterSpec;

const T: Duration = Duration::from_secs(5);

fn engines() -> Vec<Engine> {
    let only = std::env::var("OPTEST_ENGINES").unwrap_or_default();
    let mut e = Vec::new();
    if optest::Tools::discover().c_dynomite.is_some() && only != "rust" {
        e.push(Engine::C);
    }
    if only != "c" {
        e.push(Engine::Rust);
    }
    e
}

/// What one replica holds for `key`: value, hash, and the key's entries in
/// the top-level add / remove sets (scores dropped: they are timestamps).
fn replica_view(store: &mut optest::resp::Client, key: &str) -> String {
    let ty = store.cmd(&["TYPE", key]).expect("TYPE");
    let body = match &ty {
        Value::Simple(t) if t == "string" => {
            format!("{:?}", store.cmd(&["GET", key]).expect("GET"))
        }
        Value::Simple(t) if t == "hash" => {
            let all = match store.cmd(&["HGETALL", key]).expect("HGETALL") {
                Value::Array(Some(all)) => all,
                other => panic!("HGETALL {other:?}"),
            };
            let mut pairs: Vec<String> = all.chunks(2).map(|pair| format!("{pair:?}")).collect();
            pairs.sort();
            pairs.join(",")
        }
        other => format!("{other:?}"),
    };
    let add = store
        .cmd(&["ZSCORE", "._add-set", key])
        .expect("ZSCORE add");
    let rem = store
        .cmd(&["ZSCORE", "._rem-set", key])
        .expect("ZSCORE rem");
    let fields_rem = store
        .cmd(&["ZRANGE", &format!("._rem-set_{key}"), "0", "-1"])
        .expect("ZRANGE");
    format!(
        "type={ty:?} body={body} add={} rem={} rem_fields={fields_rem:?}",
        !matches!(add, Value::Bulk(None)),
        !matches!(rem, Value::Bulk(None))
    )
}

#[test]
fn concurrent_writers_converge_after_repairing_reads() {
    sandboxed!("concurrent_writers_converge_after_repairing_reads");
    for engine in engines() {
        let spec = ClusterSpec::layout(&[(
            "dc1".into(),
            vec![
                ("rack1".into(), 1),
                ("rack2".into(), 1),
                ("rack3".into(), 1),
            ],
        )])
        .with("read_consistency", "DC_SAFE_QUORUM")
        .with("write_consistency", "DC_SAFE_QUORUM")
        .engine(engine);
        let Some(c) = start_or_skip(spec) else {
            return;
        };
        for n in &c.nodes {
            assert!(
                n.stats("/read_repairs/enable")
                    .expect("enable")
                    .contains("ENABLED"),
                "{engine:?}: read repairs not enabled"
            );
        }
        let strings = ["s1", "s2"];
        let hashes = ["h1", "h2"];
        std::thread::scope(|scope| {
            for (writer, node) in c.nodes.iter().enumerate() {
                scope.spawn(move || {
                    let mut cl = node.client(T).expect("connect");
                    for step in 0..60 {
                        let value = format!("w{writer}i{step}");
                        let string = strings[(writer + step) % 2];
                        let hash = hashes[(writer + step) % 2];
                        let field = ["f1", "f2"][step % 2];
                        let cmd: Vec<&str> = match (writer + step) % 4 {
                            0 => vec!["SET", string, &value],
                            1 => vec!["DEL", string],
                            2 => vec!["HSET", hash, field, &value],
                            _ => vec!["HDEL", hash, field],
                        };
                        let _ = cl.cmd(&cmd);
                    }
                });
            }
        });
        // Repairing reads: every key and field through every node, twice
        // (the first may only repair; the second must see agreement).
        for _ in 0..2 {
            for n in &c.nodes {
                let mut cl = n.client(T).expect("connect");
                for s in strings {
                    let _ = cl.cmd(&["GET", s]);
                }
                for h in hashes {
                    for f in ["f1", "f2"] {
                        let _ = cl.cmd(&["HGET", h, f]);
                    }
                }
            }
            std::thread::sleep(Duration::from_millis(300));
        }
        for key in strings.iter().chain(hashes.iter()) {
            let views: Vec<String> = c
                .nodes
                .iter()
                .map(|n| replica_view(&mut n.store_client().expect("store"), key))
                .collect();
            eprintln!("[{engine:?}] {key}: {}", views[0]);
            assert!(
                views.iter().all(|v| *v == views[0]),
                "{engine:?}: replicas disagree on {key} after repairing reads:\n{}",
                views.join("\n")
            );
        }
    }
}
