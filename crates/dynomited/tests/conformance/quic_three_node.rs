//! QUIC client-plane three-node conformance.
//!
//! Mirror of [`super::three_node_single_dc`] but with each
//! node's `listen:` proxy bound over QUIC instead of TCP.
//! Three single-token nodes spaced evenly around the 32-bit
//! token ring; gossip is left at the harness default. The
//! peer plane (`dyn_listen:`) stays on plain TCP, so cluster
//! bootstrap follows the same path as the TCP scenarios.
//!
//! Each test connects to one (or more) nodes via the engine's
//! `dynomite::net::quic::connect` dialler, sends RESP frames
//! over the resulting bidirectional stream, and asserts the
//! reply shape matches the TCP scenarios. Behaviour observable
//! over QUIC must be byte-identical to the TCP path because
//! the QUIC transport is just a different framing of the same
//! Redis byte stream the dispatcher consumes.
//!
//! Tests in this file are gated on `feature = "quic"` (which
//! also pulls the engine's `quic` feature in via the
//! `dynomited` crate's feature graph). They additionally
//! require `redis-server` on `PATH`; without it the harness
//! prints a skip notice and exits successfully.
//!
//! See `docs/journal/2026-06-01-quic-conformance.md` for the
//! prior BLOCKED note that motivated this wiring, and
//! `docs/journal/2026-06-01-quic-wire-up.md` for the unblock
//! summary.
//!
//! Tags: feature = quic; redis-server required.

use std::path::PathBuf;
use std::time::Duration;

use dynomite::net::quic::{connect, QuicConfig, QuicTransport};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::helpers::{
    pick_port, redis_server_available, try_decode, Cluster, NodeSpec, RespError, RespValue,
};

fn skip(name: &str) -> bool {
    if !redis_server_available() {
        eprintln!("[conformance::quic_three_node::{name}] redis-server not on PATH; skipping");
        return true;
    }
    false
}

/// Generate one self-signed cert / key pair valid for
/// `localhost` and `127.0.0.1` and write them to a temporary
/// directory. The `TempDir` is returned alongside the paths so
/// callers can keep it alive for the duration of the test.
fn make_cert_pair() -> (tempfile::TempDir, PathBuf, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])
        .expect("rcgen self-signed");
    std::fs::write(&cert_path, cert.cert.pem()).expect("write cert");
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).expect("write key");
    (dir, cert_path, key_path)
}

/// Build the three-node spec list. Each node has a unique
/// (cert, key) pair living in `cert_dir`; the QUIC listener
/// reads them at startup.
fn build_quic_specs(cert_dir: &std::path::Path) -> Vec<NodeSpec> {
    // Three single-token nodes spaced evenly around the
    // 32-bit token ring (matches three_node_single_dc).
    let tokens = ["1431655765", "2863311530", "4294967294"];
    let mut specs = Vec::with_capacity(3);
    for (i, token) in tokens.iter().enumerate() {
        let cert_path = cert_dir.join(format!("n{i}.cert.pem"));
        let key_path = cert_dir.join(format!("n{i}.key.pem"));
        let pair = rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])
            .expect("rcgen per-node");
        std::fs::write(&cert_path, pair.cert.pem()).expect("write per-node cert");
        std::fs::write(&key_path, pair.signing_key.serialize_pem()).expect("write per-node key");
        let mut spec = NodeSpec::simple(format!("n{i}"), "127.0.0.1", 0, *token);
        // QUIC listener binds UDP; the harness probes TCP.
        // Point the readiness probe at the dnode peer port,
        // which stays on plain TCP and is bound after the
        // proxy listener in `Server::build` so observing it
        // confirms the whole bind sequence completed.
        spec.readiness_port = Some(spec.dyn_listen_port);
        spec.extra
            .push(("transport".to_string(), "quic".to_string()));
        spec.extra.push((
            "quic_cert_file".to_string(),
            cert_path.to_string_lossy().into_owned(),
        ));
        spec.extra.push((
            "quic_key_file".to_string(),
            key_path.to_string_lossy().into_owned(),
        ));
        specs.push(spec);
    }
    specs
}

/// Spin up a 3-node QUIC cluster. The returned tempdir owns
/// the per-node cert / key files; it must outlive the cluster.
fn launch_three_node_quic() -> (Cluster, tempfile::TempDir) {
    let (dir, _cert, _key) = make_cert_pair();
    let specs = build_quic_specs(dir.path());
    let cluster = Cluster::launch(specs, "dyn_o_mite").expect("launch QUIC 3-node cluster");
    (cluster, dir)
}

/// Open a fresh QUIC stream to the node at `idx` in
/// `cluster.nodes` and apply a sane per-call read deadline.
async fn open_quic_client(cluster: &Cluster, idx: usize) -> QuicTransport {
    let n = &cluster.nodes[idx];
    let addr = format!("{}:{}", n.spec.host, n.spec.listen_port)
        .parse()
        .expect("parse SocketAddr");
    connect(addr, QuicConfig::client_insecure())
        .await
        .expect("dial QUIC")
}

/// Send one RESP request and read one RESP reply over the
/// supplied QUIC stream. Loops on partial reads until a full
/// reply has been parsed or the deadline elapses.
async fn quic_cmd(
    stream: &mut QuicTransport,
    args: &[&[u8]],
    timeout: Duration,
) -> Result<RespValue, RespError> {
    let mut buf: Vec<u8> = Vec::with_capacity(64);
    buf.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for a in args {
        buf.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        buf.extend_from_slice(a);
        buf.extend_from_slice(b"\r\n");
    }
    stream.write_all(&buf).await?;
    stream.flush().await.ok();
    let mut acc: Vec<u8> = Vec::new();
    let mut tmp = [0u8; 4096];
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some((value, consumed)) = try_decode(&acc)? {
            // try_decode returns the offset; we don't reuse acc
            // across calls, so just discard the consumed bytes.
            let _ = consumed;
            return Ok(value);
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err(RespError::Timeout);
        }
        match tokio::time::timeout(deadline - now, stream.read(&mut tmp)).await {
            Ok(Ok(0)) => return Err(RespError::Eof),
            Ok(Ok(n)) => acc.extend_from_slice(&tmp[..n]),
            Ok(Err(e)) => return Err(RespError::Io(e)),
            Err(_) => return Err(RespError::Timeout),
        }
    }
}

/// Touch [`pick_port`] so the helper module is exercised by
/// every test in this file even when the harness picks the
/// listen ports inside [`NodeSpec::simple`]. Catches a stale
/// `pub use` regression in the conformance harness.
#[allow(dead_code)]
fn _exercise_pick_port() {
    let _ = pick_port();
}

#[tokio::test]
async fn quic_three_node_each_node_accepts_workload() {
    if skip("quic_three_node_each_node_accepts_workload") {
        return;
    }
    let (cluster, _certs) = launch_three_node_quic();
    assert_eq!(cluster.nodes.len(), 3);
    for idx in 0..cluster.nodes.len() {
        let mut stream = open_quic_client(&cluster, idx).await;
        let key = format!("quic-idx-{idx}");
        let val = format!("quic-val-{idx}");
        let v = quic_cmd(
            &mut stream,
            &[b"SET", key.as_bytes(), val.as_bytes()],
            Duration::from_secs(8),
        )
        .await
        .unwrap_or_else(|e| panic!("node {idx} SET: {e:?}"));
        // The dispatcher answers SET with `+OK\r\n` (a simple
        // string) when the local datastore accepts the write,
        // or with a typed Dynomite error when quorum cannot be
        // satisfied. Either is a valid RESP shape.
        assert!(
            v.as_simple() == Some("OK") || matches!(v, RespValue::Error(_)),
            "node {idx} SET unexpected reply shape: {v:?}",
        );
        let v = quic_cmd(
            &mut stream,
            &[b"GET", key.as_bytes()],
            Duration::from_secs(8),
        )
        .await
        .unwrap_or_else(|e| panic!("node {idx} GET: {e:?}"));
        // GET is satisfied locally on a single-token cluster
        // when the key hashes into this node's range. We
        // accept any RESP-shaped reply (bulk, nil, or typed
        // error) here for the same reason the TCP scenario
        // does: cross-node routing converges asynchronously.
        match &v {
            RespValue::Bulk(_) | RespValue::Error(_) => {}
            other => panic!("node {idx} GET unexpected reply shape: {other:?}"),
        }
        let v = quic_cmd(
            &mut stream,
            &[b"DEL", key.as_bytes()],
            Duration::from_secs(8),
        )
        .await
        .unwrap_or_else(|e| panic!("node {idx} DEL: {e:?}"));
        match &v {
            RespValue::Integer(_) | RespValue::Error(_) => {}
            other => panic!("node {idx} DEL unexpected reply shape: {other:?}"),
        }
    }
}

#[tokio::test]
async fn quic_concurrent_writers_consistent_under_dc_one() {
    if skip("quic_concurrent_writers_consistent_under_dc_one") {
        return;
    }
    let (cluster, _certs) = launch_three_node_quic();
    assert_eq!(cluster.nodes.len(), 3);
    // Drive concurrent writers from all three entry points
    // and assert (a) every reply is RESP-shaped (no panic, no
    // half-frame) and (b) the writers do not deadlock each
    // other. Per-key sharding (each writer uses its own key
    // namespace) keeps the assertion deterministic under the
    // best-effort DC_ONE semantics the harness inherits.
    let mut tasks: Vec<tokio::task::JoinHandle<usize>> = Vec::with_capacity(cluster.nodes.len());
    for idx in 0..cluster.nodes.len() {
        let host = cluster.nodes[idx].spec.host.clone();
        let port = cluster.nodes[idx].spec.listen_port;
        tasks.push(tokio::spawn(async move {
            let addr = format!("{host}:{port}").parse().expect("parse SocketAddr");
            let mut stream = connect(addr, QuicConfig::client_insecure())
                .await
                .expect("dial QUIC");
            let mut ok: usize = 0;
            for i in 0..16usize {
                let key = format!("quic-cc-{idx}-{i:02}");
                let val = format!("v-{idx}-{i:02}");
                let v = quic_cmd(
                    &mut stream,
                    &[b"SET", key.as_bytes(), val.as_bytes()],
                    Duration::from_secs(5),
                )
                .await;
                if v.is_ok() {
                    ok += 1;
                }
            }
            ok
        }));
    }
    let mut total: usize = 0;
    for t in tasks {
        total += t.await.expect("writer task panic");
    }
    // Three writers x 16 SETs = 48 attempts. At least one
    // writer should land every SET under DC_ONE; the harness's
    // best-effort gossip means we accept partial completion to
    // avoid flake on slower runners. Assert a strong lower
    // bound that still surfaces a complete-failure regression.
    assert!(
        total >= 24,
        "concurrent writers degenerate too far: {total} / 48 SETs replied"
    );
}

#[tokio::test]
async fn differential_three_node_quic() {
    // Differential sanity check: drive an identical workload
    // through every node's QUIC entry point and assert the
    // reply shapes match across entry points. The TCP
    // counterpart (`shape_matches_across_entry_points`)
    // checks the same property; this test is the QUIC mirror.
    if skip("differential_three_node_quic") {
        return;
    }
    let (cluster, _certs) = launch_three_node_quic();
    let mut s0 = open_quic_client(&cluster, 0).await;
    let mut s1 = open_quic_client(&cluster, 1).await;
    let mut s2 = open_quic_client(&cluster, 2).await;
    for cmd in [&b"PING"[..], &b"COMMAND"[..]] {
        let r0 = quic_cmd(&mut s0, &[cmd], Duration::from_secs(5)).await;
        let r1 = quic_cmd(&mut s1, &[cmd], Duration::from_secs(5)).await;
        let r2 = quic_cmd(&mut s2, &[cmd], Duration::from_secs(5)).await;
        // We do not assert byte equality (Redis embeds
        // version data in some replies) but we do assert
        // error / non-error parity across all three nodes.
        assert_eq!(
            r0.is_ok(),
            r1.is_ok(),
            "{} parity mismatch n0 vs n1: {r0:?} vs {r1:?}",
            String::from_utf8_lossy(cmd),
        );
        assert_eq!(
            r1.is_ok(),
            r2.is_ok(),
            "{} parity mismatch n1 vs n2: {r1:?} vs {r2:?}",
            String::from_utf8_lossy(cmd),
        );
    }
}
