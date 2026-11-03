//! The C functional test suite (`test/func_test.py`, `dual_run.py`,
//! `cluster_generator.py`, the `*_request.yaml` topologies), scenario for
//! scenario.
//!
//! Every command runs against a standalone datastore and against the
//! cluster, and the replies must be identical (`dual_run.run_verify`).
//! Topologies are the two C request files:
//!
//! * `no_quorum_request.yaml`: dc1 = {rack1: 1}, dc2 = {rack1: 1,
//!   rack2: 2, rack3: 4}; `DC_ONE`.
//! * `safe_quorum_request.yaml`: two DCs of three single-node racks;
//!   `DC_SAFE_QUORUM`.
//!
//! The suites (`run_key_value_tests`, `run_multikey_test`,
//! `run_hash_tests`, `run_script_tests`, `run_read_repair_test`) keep the
//! C key names, sizes, and command sequences; the payloads come from a
//! seeded RNG, so a failure replays with `OPTEST_SEED`.
//!
//! Each scenario runs the C reference first, then `dynomited`.
//!
//! Known Rust gap (tracked, not masked; the test fails until it lands):
//! C timestamp read repair (`._add-set` metadata, DEV-R1, plan T8), so
//! `run_read_repair_test` fails `func_test_safe_quorum_request` on the
//! Rust engine.
//!
//! Not exercised here: multi-key fragmentation inside a multi-node rack.
//! Both C topologies enter through a one-node rack, so MGET/MSET never
//! split; the Rust engine does not wire `redis_fragment` into the request
//! path yet (plan T7/T9).

#[macro_use]
mod common;

use std::time::Duration;

use common::{seed, start_or_skip};
use optest::cluster::{Cluster, Engine};
use optest::resp::{Client, Value};
use optest::ClusterSpec;

const T: Duration = Duration::from_secs(10);

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

/// A small deterministic PRNG (splitmix64), so the workload is identical
/// for both engines and replayable from the printed seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    /// Uniform in `lo..=hi`.
    fn range(&mut self, lo: usize, hi: usize) -> usize {
        lo + usize::try_from(self.next() % u64::try_from(hi - lo + 1).unwrap()).unwrap()
    }
    /// `string_generator`: ASCII letters and digits.
    fn string(&mut self, size: usize) -> String {
        const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        (0..size)
            .map(|_| CHARS[self.range(0, CHARS.len() - 1)] as char)
            .collect()
    }
    /// `string_generator(size=random.randint(lo, hi))`.
    fn string_in(&mut self, lo: usize, hi: usize) -> String {
        let n = self.range(lo, hi);
        self.string(n)
    }
    /// `number_generator`: four random digits, as an integer (leading
    /// zeros dropped).
    fn number(&mut self) -> String {
        let digits: String = (0..4)
            .map(|_| char::from(b'0' + u8::try_from(self.range(0, 9)).unwrap()))
            .collect();
        digits.parse::<u32>().unwrap().to_string()
    }
}

fn create_key(test: &str, id: impl std::fmt::Display) -> String {
    format!("{test}_{id}")
}

/// `dual_run`: one standalone datastore beside the cluster, plus a client
/// to a chosen cluster node.
struct Dual<'a> {
    cluster: &'a Cluster,
    redis: Client,
    dyno: Client,
    sort: bool,
    engine: Engine,
    checked: usize,
}

impl Dual<'_> {
    /// `run_verify`: same command to both, replies must match. The C
    /// harness retries "Peer Node is not connected" up to three times.
    fn run_verify(&mut self, args: &[&str]) -> Value {
        let r = self.redis.cmd(args).expect("standalone datastore");
        let shown: Vec<String> = args.iter().map(|a| a.chars().take(40).collect()).collect();
        let mut d = self.dyno.cmd(args).unwrap_or_else(|e| {
            panic!(
                "[{:?}] cluster node failed on {shown:?} after {} ok: {e}",
                self.engine, self.checked
            )
        });
        for _ in 0..3 {
            match &d {
                Value::Error(e) if e.contains("Peer Node is not connected") => {
                    std::thread::sleep(Duration::from_millis(200));
                    d = self.dyno.cmd(args).expect("cluster node");
                }
                _ => break,
            }
        }
        let (r, d) = if self.sort {
            (sorted(r), sorted(d))
        } else {
            (r, d)
        };
        if r != d {
            // Which replicas actually hold the data: the same command
            // straight at every node's datastore.
            for n in &self.cluster.nodes {
                let direct = n.store_client().and_then(|mut s| s.cmd(args));
                let dbsize = n.store_client().and_then(|mut s| s.cmd(&["DBSIZE"]));
                eprintln!(
                    "[{:?}] {} {}/{}: dbsize {dbsize:?}, direct reply matches standalone: {}",
                    self.engine,
                    n.ip(),
                    n.spec.dc,
                    n.spec.rack,
                    direct.as_ref().is_ok_and(|v| *v == r)
                );
            }
        }
        assert_eq!(
            r, d,
            "[{:?}] result mismatch for {:?}: standalone {:?} vs dynomite {:?}",
            self.engine, args, r, d
        );
        self.checked += 1;
        d
    }

    fn run_dynomite_only(&mut self, args: &[&str]) -> Value {
        self.dyno.cmd(args).expect("cluster node")
    }
}

fn sorted(v: Value) -> Value {
    match v {
        Value::Array(Some(mut items)) => {
            items.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
            Value::Array(Some(items))
        }
        other => other,
    }
}

fn run_key_value_tests(c: &mut Dual<'_>, rng: &mut Rng, max_keys: usize, max_payload: usize) {
    let test = "KEY_VALUE";
    for x in 0..max_keys {
        let v = rng.string_in(1, max_payload);
        c.run_verify(&["SET", &create_key(test, x), &v]);
    }
    for x in 0..max_keys {
        c.run_verify(&["GET", &create_key(test, x)]);
    }
    let key = create_key(test, rng.range(0, max_keys - 1));
    let v = rng.string(6);
    c.run_verify(&["APPEND", &key, &v]);
    c.run_verify(&["GET", &key]);
    let key = create_key(test, rng.range(0, max_keys - 1));
    c.run_verify(&["EXPIRE", &key, "5"]);
    std::thread::sleep(Duration::from_secs(7));
    c.run_verify(&["EXISTS", &key]);
}

fn run_multikey_test(c: &mut Dual<'_>, rng: &mut Rng, max_keys: usize, max_payload: usize) {
    let test = "MULTIKEY";
    for _ in 0..100 {
        let n = rng.range(1, 50);
        let mut args: Vec<String> = vec!["MSET".into()];
        let mut seen = std::collections::HashMap::new();
        for _ in 0..n {
            let key = create_key(test, rng.range(0, max_keys - 1));
            let v = rng.string_in(1, max_payload);
            seen.insert(key, v);
        }
        for (k, v) in seen {
            args.push(k);
            args.push(v);
        }
        let a: Vec<&str> = args.iter().map(String::as_str).collect();
        c.run_verify(&a);
        let n = rng.range(1, 50);
        let mut keys: Vec<String> = vec!["MGET".into()];
        for _ in 0..n {
            keys.push(create_key(test, rng.range(0, max_keys - 1)));
        }
        let k: Vec<&str> = keys.iter().map(String::as_str).collect();
        c.run_verify(&k);
    }
}

fn run_hash_tests(c: &mut Dual<'_>, rng: &mut Rng, max_keys: usize, max_fields: usize) {
    let test = "HASH_MAP";
    let kf = |rng: &mut Rng, k: Option<usize>, f: Option<usize>| {
        let k = k.unwrap_or_else(|| rng.range(0, max_keys - 1));
        let f = f.unwrap_or_else(|| rng.range(0, max_fields - 1));
        let key = create_key(test, k);
        let field = format!("{key}{}", create_key("_field", f));
        (key, field)
    };
    for ki in 0..max_keys {
        for fi in 0..max_fields {
            let (key, field) = kf(rng, Some(ki), Some(fi));
            let v = rng.number();
            c.run_verify(&["HSET", &key, &field, &v]);
        }
    }
    let kid = rng.range(0, max_keys - 1);
    let (key, _) = kf(rng, Some(kid), None);
    let mut args: Vec<String> = vec!["HMSET".into(), key.clone()];
    for _ in 0..50 {
        let (_, field) = kf(rng, Some(kid), None);
        args.push(field);
        args.push(rng.number());
    }
    let a: Vec<&str> = args.iter().map(String::as_str).collect();
    c.run_verify(&a);
    let kid = rng.range(0, max_keys - 1);
    let (key, _) = kf(rng, Some(kid), None);
    let mut args: Vec<String> = vec!["HMGET".into(), key];
    for _ in 0..5 {
        args.push(kf(rng, Some(kid), None).1);
    }
    let a: Vec<&str> = args.iter().map(String::as_str).collect();
    c.run_verify(&a);
    let (key, field) = kf(rng, None, None);
    c.run_verify(&["HINCRBY", &key, &field, "50"]);
    c.run_verify(&["HDEL", &key, &field]);
    c.run_verify(&["HEXISTS", &key, &field]);
    let (key, _) = kf(rng, None, None);
    c.run_verify(&["HLEN", &key]);
    c.sort = true;
    for cmd in ["HGETALL", "HKEYS", "HVALS"] {
        let (key, _) = kf(rng, None, None);
        c.run_verify(&[cmd, &key]);
    }
    c.sort = false;
}

fn run_script_tests(c: &mut Dual<'_>) {
    let test = "SCRIPTS";
    let body = "return redis.call('get', KEYS[1])";
    let expected = "value1";
    let hash = c
        .run_verify(&["SCRIPT", "LOAD", body])
        .text()
        .expect("sha1");
    let exists = c.run_verify(&["SCRIPT", "EXISTS", &hash]);
    assert_eq!(exists, Value::Array(Some(vec![Value::Int(1)])));
    let key = create_key(test, "key1");
    c.run_verify(&["SET", &key, expected]);
    let r = c.run_verify(&["EVALSHA", &hash, "1", &key]);
    assert_eq!(r.text().as_deref(), Some(expected));
    c.run_dynomite_only(&["SCRIPT", "FLUSH"]);
    let after = c.run_dynomite_only(&["EVALSHA", &hash, "1", &key]);
    assert!(
        matches!(after, Value::Error(ref e) if e.contains("NOSCRIPT")),
        "[{:?}] EVALSHA after SCRIPT FLUSH: {after:?}",
        c.engine
    );
}

/// `run_read_repair_test`: enable read repairs, make the cluster
/// `DC_SAFE_QUORUM`, write a key, then on one replica delete it, bump its
/// add-set timestamp, and store a different value there. Quorum reads must
/// eventually return the repaired value.
fn run_read_repair_test(c: &mut Dual<'_>, rng: &mut Rng, multi_rack_dc: &str) {
    let test = "READ_REPAIR_TEST";
    for n in &c.cluster.nodes {
        let _ = n.stats("/read_repairs/enable");
        let _ = n.stats("/set_consistency/read/DC_SAFE_QUORUM");
        let _ = n.stats("/set_consistency/write/DC_SAFE_QUORUM");
    }
    let key = create_key(test, "_1");
    let value = rng.string_in(1, 1024);
    c.run_verify(&["SET", &key, &value]);
    assert_eq!(
        c.run_verify(&["GET", &key]).text().as_deref(),
        Some(value.as_str())
    );
    let holder = c
        .cluster
        .nodes
        .iter()
        .find(|n| {
            n.spec.dc == multi_rack_dc
                && n.store_client()
                    .ok()
                    .and_then(|mut s| s.cmd(&["EXISTS", &key]).ok())
                    == Some(Value::Int(1))
        })
        .expect("a replica in the multi-rack DC holds the key");
    let mut s = holder.store_client().expect("replica datastore");
    assert_eq!(s.cmd(&["DEL", &key]).unwrap(), Value::Int(1));
    let md = "._add-set";
    let score = s.cmd(&["ZSCORE", md, &key]).unwrap();
    let bumped = match score {
        Value::Bulk(Some(b)) => String::from_utf8_lossy(&b).parse::<f64>().unwrap() + 1.0,
        other => panic!("[{:?}] no add-set metadata for {key}: {other:?}", c.engine),
    };
    s.cmd(&["ZADD", md, &bumped.to_string(), &key]).unwrap();
    s.cmd(&["SET", &key, "REPAIRED_VALUE"]).unwrap();
    let ok = (0..20)
        .any(|_| c.run_dynomite_only(&["GET", &key]).text().as_deref() == Some("REPAIRED_VALUE"));
    assert!(
        ok,
        "[{:?}] read repair never returned the repaired value",
        c.engine
    );
    for n in &c.cluster.nodes {
        let _ = n.stats("/read_repairs/disable");
    }
}

/// `comparison_test` minus the parts each topology does not support.
fn comparison_test(cluster: &Cluster, engine: Engine, large: bool, read_repair_dc: Option<&str>) {
    let mut rng = Rng(seed());
    let redis = standalone_client(cluster);
    let entry = read_repair_dc
        .and_then(|dc| cluster.nodes.iter().position(|n| n.spec.dc == dc))
        .unwrap_or(0);
    let dyno = cluster.nodes[entry].client(T).expect("cluster node");
    let mut c = Dual {
        cluster,
        redis,
        dyno,
        sort: false,
        engine,
        checked: 0,
    };
    run_key_value_tests(&mut c, &mut rng, 1000, 1024);
    if large {
        run_key_value_tests(&mut c, &mut rng, 10, 5 * 1024 * 1024);
    }
    run_multikey_test(&mut c, &mut rng, 1000, 10);
    run_hash_tests(&mut c, &mut rng, 10, 100);
    run_script_tests(&mut c);
    if let Some(dc) = read_repair_dc {
        run_read_repair_test(&mut c, &mut rng, dc);
    }
    eprintln!(
        "[{engine:?}] func_test: {} commands verified against standalone",
        c.checked
    );
}

/// The standalone datastore the C harness runs beside the cluster
/// (`RedisNode` on its own IP). Here it is node 0's namespace, on a
/// separate port.
fn standalone_client(cluster: &Cluster) -> Client {
    let port = 1212;
    let tools = optest::Tools::discover();
    let bin = tools.redis_server.clone().expect("valkey-server");
    let ip = cluster.nodes[0].ip();
    let child = optest::cluster::in_netns(0, &bin)
        .args([
            "--bind",
            &ip.to_string(),
            "--port",
            &port.to_string(),
            "--save",
            "",
            "--appendonly",
            "no",
            "--protected-mode",
            "no",
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("standalone valkey");
    std::mem::forget(child); // reaped with the sandbox
    let addr = std::net::SocketAddr::from((ip, port));
    let mut c = common::eventually("standalone datastore", Duration::from_secs(10), || {
        Client::connect(addr, T).ok()
    });
    // The standalone outlives one engine's cluster (a second spawn on the
    // same port fails and this connects to the first), so the previous
    // engine's keys must go, or they read as data the next engine lost.
    assert_eq!(
        c.cmd(&["FLUSHALL"]).expect("FLUSHALL"),
        Value::Simple("OK".into())
    );
    c
}

fn no_quorum_topology() -> ClusterSpec {
    ClusterSpec::layout(&[
        ("dc1".into(), vec![("rack1".into(), 1)]),
        (
            "dc2".into(),
            vec![
                ("rack1".into(), 1),
                ("rack2".into(), 2),
                ("rack3".into(), 4),
            ],
        ),
    ])
    .with("read_consistency", "DC_ONE")
    .with("write_consistency", "DC_ONE")
}

fn safe_quorum_topology() -> ClusterSpec {
    ClusterSpec::layout(&[
        (
            "dc1".into(),
            vec![
                ("rack1".into(), 1),
                ("rack2".into(), 1),
                ("rack3".into(), 1),
            ],
        ),
        (
            "dc2".into(),
            vec![
                ("rack1".into(), 1),
                ("rack2".into(), 1),
                ("rack3".into(), 1),
            ],
        ),
    ])
    .with("read_consistency", "DC_SAFE_QUORUM")
    .with("write_consistency", "DC_SAFE_QUORUM")
}

#[test]
fn func_test_no_quorum_request() {
    sandboxed!("func_test_no_quorum_request");
    for engine in engines() {
        let Some(c) = start_or_skip(no_quorum_topology().engine(engine)) else {
            return;
        };
        comparison_test(&c, engine, true, None);
        let info = c.nodes[0].stats("/info").expect("stats /info");
        serde_json::from_str::<serde_json::Value>(&info)
            .unwrap_or_else(|e| panic!("[{engine:?}] /info is not JSON ({e}): {info}"));
    }
}

#[test]
fn func_test_safe_quorum_request() {
    sandboxed!("func_test_safe_quorum_request");
    for engine in engines() {
        let Some(c) = start_or_skip(safe_quorum_topology().engine(engine)) else {
            return;
        };
        comparison_test(&c, engine, false, Some("dc1"));
        let info = c.nodes[0].stats("/info").expect("stats /info");
        serde_json::from_str::<serde_json::Value>(&info)
            .unwrap_or_else(|e| panic!("[{engine:?}] /info is not JSON ({e}): {info}"));
    }
}
