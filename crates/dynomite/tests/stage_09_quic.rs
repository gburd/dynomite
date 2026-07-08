//! Stage 9 QUIC end-to-end test.
//!
//! Runs only when the dynomite crate is built with the `quic`
//! feature. The test:
//!
//! 1. Generates a self-signed cert/key pair via the pure-Rust
//!    `rcgen` crate (no `openssl` CLI required, no checked-in
//!    fixture).
//! 2. Spawns a `QuicListener` on `127.0.0.1:0`.
//! 3. Dials it from a `connect()` client.
//! 4. Exchanges a small payload across the QUIC stream.
//! 5. Asserts the bytes round-trip.
//!
//! The `--all-features` workspace build ships this test alongside
//! the AES / RSA Stage 6 fixtures; the openssl / quiche-boringssl
//! link clash that originally blocked this combination was
//! resolved by the RustCrypto migration in commit `da36535`.

#![allow(
    clippy::format_push_string,
    clippy::needless_continue,
    clippy::unnecessary_debug_formatting,
    clippy::match_same_arms,
    clippy::similar_names,
    clippy::cast_possible_truncation
)]
#![cfg(feature = "quic")]

use std::net::SocketAddr;
use std::time::Duration;

use dynomite::net::quic::{connect, QuicConfig, QuicListener};
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Generate a fresh self-signed cert+key pair on disk and return
/// the two paths. The directory is owned by the returned
/// [`tempfile::TempDir`]; the caller must keep it alive until the
/// QUIC config has loaded the files.
fn make_cert_pair() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let dir = tempdir().expect("create tempdir for quic cert");
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");

    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])
        .expect("rcgen self-signed cert");
    std::fs::write(&cert_path, cert.cert.pem()).expect("write cert.pem");
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).expect("write key.pem");

    (dir, cert_path, key_path)
}

/// End-to-end QUIC round-trip: a small payload is written from
/// the client side and read back on the server side via the
/// `QuicTransport` `AsyncRead` / `AsyncWrite` adapters.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_loopback_round_trip() {
    let (_keep_alive, cert_path, key_path) = make_cert_pair();

    let server_cfg = QuicConfig::server_with_cert_paths(
        cert_path.to_str().expect("cert path utf-8"),
        key_path.to_str().expect("key path utf-8"),
    );
    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = QuicListener::bind(bind_addr, server_cfg)
        .await
        .expect("bind QUIC listener");
    let server_addr = listener.local_addr();

    // Server task: accept a single connection and echo the bytes
    // it reads back to the client.
    let server_task = tokio::spawn(async move {
        let mut transport = listener.accept().await.expect("accept QUIC connection");
        let mut buf = vec![0u8; 64];
        let mut got = Vec::<u8>::new();
        // Read until we have at least 32 bytes or the deadline
        // expires.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while got.len() < 32 && tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(500), transport.read(&mut buf)).await {
                Ok(Ok(0) | Err(_)) | Err(_) => continue,
                Ok(Ok(n)) => got.extend_from_slice(&buf[..n]),
            }
        }
        // Echo the accumulated bytes back. Give the QUIC driver a
        // moment to flush them onto the wire before the test
        // teardown drops the transport.
        let _ = transport.write_all(&got).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        got
    });

    // Client task: dial the listener, write 32 bytes, read the
    // echo back, and assert equality.
    let client_cfg = QuicConfig::client_insecure();
    let mut client = connect(server_addr, client_cfg)
        .await
        .expect("client connect");
    let payload: Vec<u8> = (0u8..32).collect();
    client.write_all(&payload).await.expect("client write_all");

    let mut echo = vec![0u8; payload.len()];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut filled = 0usize;
    while filled < echo.len() && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), client.read(&mut echo[filled..]))
            .await
        {
            Ok(Ok(0) | Err(_)) | Err(_) => continue,
            Ok(Ok(n)) => filled += n,
        }
    }
    assert_eq!(filled, payload.len(), "client did not read full echo");
    assert_eq!(&echo[..filled], &payload[..]);

    let server_got = tokio::time::timeout(Duration::from_secs(5), server_task)
        .await
        .expect("server task did not finish")
        .expect("server task panicked");
    assert_eq!(
        server_got, payload,
        "server did not receive the full payload"
    );
}

/// Regression: multiple sequential request/response exchanges on a
/// single QUIC connection all complete.
///
/// The listener shares one UDP socket across the accept path and every
/// connection driver. The original design had the accept loop and the
/// connection drivers each call `recv_from` on that shared socket, so
/// after the first exchange a subsequent inbound datagram was as
/// likely to be consumed (and discarded) by the accept loop as
/// delivered to the connection -- the connection stalled on its second
/// request. The demux task now owns the socket read side and routes
/// datagrams by peer, so every exchange on a live connection is
/// delivered. This test drives ten sequential round-trips; with the
/// bug it hangs on the second, so a pass is a deterministic signal the
/// routing is correct (the count is fixed, not timing-dependent).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_many_sequential_round_trips_on_one_connection() {
    const ROUNDS: u8 = 10;
    const MSG: usize = 8;

    let (_keep_alive, cert_path, key_path) = make_cert_pair();
    let server_cfg = QuicConfig::server_with_cert_paths(
        cert_path.to_str().expect("cert path utf-8"),
        key_path.to_str().expect("key path utf-8"),
    );
    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = QuicListener::bind(bind_addr, server_cfg)
        .await
        .expect("bind QUIC listener");
    let server_addr = listener.local_addr();

    // Server: loop-accept on the listener (as the real dyniak/dnode
    // QUIC servers do) while a per-connection task echoes each 8-byte
    // message back, ROUNDS times. The loop-accept is the trigger for
    // the original bug: with the shared-socket design the re-entered
    // accept path competed with the connection driver for inbound
    // datagrams and stole the connection's later packets.
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<usize>();
    let server_task = tokio::spawn(async move {
        let mut done_tx = Some(done_tx);
        loop {
            let Ok(mut transport) = listener.accept().await else {
                break;
            };
            let dt = done_tx.take();
            tokio::spawn(async move {
                let mut echoed = 0u8;
                let mut buf = vec![0u8; 64];
                let mut acc = Vec::<u8>::new();
                let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
                while echoed < ROUNDS && tokio::time::Instant::now() < deadline {
                    match tokio::time::timeout(Duration::from_millis(500), transport.read(&mut buf))
                        .await
                    {
                        Ok(Ok(n)) if n > 0 => acc.extend_from_slice(&buf[..n]),
                        _ => continue,
                    }
                    while acc.len() >= MSG {
                        let chunk: Vec<u8> = acc.drain(..MSG).collect();
                        if transport.write_all(&chunk).await.is_err() {
                            break;
                        }
                        echoed += 1;
                    }
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
                if let Some(dt) = dt {
                    let _ = dt.send(echoed as usize);
                }
            });
        }
    });

    let client_cfg = QuicConfig::client_insecure();
    let mut client = connect(server_addr, client_cfg)
        .await
        .expect("client connect");

    for round in 0u8..ROUNDS {
        let payload: Vec<u8> = (0u8..MSG as u8).map(|b| b.wrapping_add(round)).collect();
        client
            .write_all(&payload)
            .await
            .unwrap_or_else(|e| panic!("round {round}: client write: {e}"));
        let mut echo = [0u8; MSG];
        let mut filled = 0usize;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while filled < MSG && tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(500), client.read(&mut echo[filled..]))
                .await
            {
                Ok(Ok(n)) if n > 0 => filled += n,
                _ => continue,
            }
        }
        assert_eq!(
            filled, MSG,
            "round {round}: client did not read the full echo (connection stalled)"
        );
        assert_eq!(&echo[..], &payload[..], "round {round}: echo mismatch");
    }

    let rounds_echoed = tokio::time::timeout(Duration::from_secs(10), done_rx)
        .await
        .expect("server did not finish echoing")
        .expect("server task dropped the done channel");
    server_task.abort();
    assert_eq!(
        rounds_echoed, ROUNDS as usize,
        "server did not echo every round"
    );
}
