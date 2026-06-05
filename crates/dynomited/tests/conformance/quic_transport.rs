//! QUIC transport conformance: smoke-test the
//! `dynomite::net::quic` listener + dialer against the same
//! request/reply shape the TCP integration test uses.
//!
//! Because the production `dynomited` binary does not yet
//! expose a QUIC option in its YAML schema (TCP is the only
//! supported transport at the binary level today), this
//! scenario exercises the library's QUIC primitives directly:
//! it spins up a `QuicListener` on `127.0.0.1:0`, dials it from
//! the matching `connect()` helper, and round-trips a payload
//! that is byte-equivalent to a Redis SET reply (`+OK\r\n`).
//!
//! The test is gated on `feature = "quic"` so it only compiles
//! when the QUIC stack is enabled. It does not require
//! `valkey-server`; QUIC transport conformance is independent of
//! the datastore.

use std::time::Duration;

use dynomite::net::quic::{connect, QuicConfig, QuicListener};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn make_cert_pair() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])
        .expect("rcgen");
    std::fs::write(&cert_path, cert.cert.pem()).expect("write cert");
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).expect("write key");
    (dir, cert_path, key_path)
}

#[tokio::test]
async fn quic_resp_round_trip() {
    let (_dir, cert, key) = make_cert_pair();
    let cfg = QuicConfig::server_with_cert_paths(
        cert.to_str().expect("utf8").to_owned(),
        key.to_str().expect("utf8").to_owned(),
    );

    let listener = QuicListener::bind("127.0.0.1:0".parse().unwrap(), cfg)
        .await
        .expect("bind quic listener");
    let local = listener.local_addr();

    let server = tokio::spawn(async move {
        let mut stream = listener.accept().await.expect("accept");
        let mut buf = [0u8; 64];
        let n = stream.read(&mut buf).await.expect("read");
        assert!(n > 0);
        stream.write_all(b"+OK\r\n").await.expect("write reply");
        stream.flush().await.ok();
        tokio::time::sleep(Duration::from_millis(100)).await;
    });

    let client_cfg = QuicConfig::client_insecure();
    let mut stream = connect(local, client_cfg).await.expect("dial quic");

    stream
        .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")
        .await
        .expect("write SET");
    stream.flush().await.ok();

    let mut acc = Vec::new();
    let mut tmp = [0u8; 64];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(200), stream.read(&mut tmp)).await {
            Ok(Ok(0) | Err(_)) => break,
            Ok(Ok(n)) => {
                acc.extend_from_slice(&tmp[..n]);
                if acc.windows(5).any(|w| w == b"+OK\r\n") {
                    break;
                }
            }
            Err(_) => {}
        }
    }

    assert!(
        acc.windows(5).any(|w| w == b"+OK\r\n"),
        "QUIC server reply did not contain +OK\\r\\n: got {acc:?}",
    );
    let _ = server.await;
}
