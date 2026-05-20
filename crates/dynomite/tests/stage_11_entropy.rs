//! Stage 11 integration tests: end-to-end roundtrip across the
//! [`EntropySender`] -> [`EntropyReceiver`] pipeline.
//!
//! Three scenarios are exercised:
//!
//! 1. Plaintext (`encrypt = false`) roundtrip with an in-process
//!    sender/receiver pair and an in-memory snapshot source/sink.
//! 2. Encrypted roundtrip using the bundled `recon_key.pem` and
//!    `recon_iv.pem` fixtures. The decrypted bytes must match the
//!    original snapshot.
//! 3. Failure modes: tampered ciphertext rejected, truncated stream
//!    rejected.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use dynomite::entropy::receive::MemorySink;
use dynomite::entropy::send::StaticSnapshot;
use dynomite::entropy::{boxed_sink, boxed_source, EntropyConfig, EntropyReceiver, EntropySender};

fn bundled_key_path() -> PathBuf {
    workspace_root().join("_/dynomite/conf/recon_key.pem")
}

fn bundled_iv_path() -> PathBuf {
    workspace_root().join("_/dynomite/conf/recon_iv.pem")
}

fn workspace_root() -> PathBuf {
    // crates/dynomite/tests/stage_11_entropy.rs -> repo root
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

fn cfg(listen: std::net::SocketAddr, peer: std::net::SocketAddr, encrypt: bool) -> EntropyConfig {
    EntropyConfig {
        key_file: bundled_key_path(),
        iv_file: bundled_iv_path(),
        listen_addr: listen,
        send_addr: None,
        peer_endpoint: peer,
        buffer_size: 256,
        header_size: 64,
        encrypt,
    }
}

#[tokio::test]
async fn plaintext_roundtrip_in_process() {
    let snapshot = b"the quick brown fox jumps over the lazy dog".repeat(20);
    let listen: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut recv_cfg = cfg(listen, "127.0.0.1:0".parse().unwrap(), false);
    recv_cfg.buffer_size = 32;

    let sink = Arc::new(MemorySink::default());
    let receiver = EntropyReceiver::bind(recv_cfg.clone(), boxed_sink(sink.clone()))
        .await
        .unwrap();
    let bound = receiver.local_addr().unwrap();
    let recv_handle = tokio::spawn(async move { receiver.accept_one().await });

    let mut send_cfg = recv_cfg.clone();
    send_cfg.peer_endpoint = bound;
    let source = boxed_source(StaticSnapshot::new(snapshot.clone()));
    let sent = EntropySender::push(send_cfg, source).await.unwrap();

    let total = recv_handle.await.unwrap().unwrap();
    assert_eq!(total, snapshot.len());
    assert_eq!(sent, snapshot.len());

    // Re-fetching the sink contents must equal the original
    // snapshot byte-for-byte.
    let got = sink.snapshot();
    assert_eq!(got, snapshot);
}

#[tokio::test]
async fn encrypted_roundtrip_with_bundled_fixtures() {
    // Build a 1.5x-buffer payload to exercise the chunk loop.
    let mut snapshot = Vec::with_capacity(384);
    for i in 0u32..384 {
        snapshot.push(u8::try_from(i % 251).unwrap());
    }
    let recv_cfg = cfg(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        true,
    );

    let sink = Arc::new(MemorySink::default());
    let receiver = EntropyReceiver::bind(recv_cfg.clone(), boxed_sink(sink.clone()))
        .await
        .unwrap();
    let bound = receiver.local_addr().unwrap();
    let recv_handle = tokio::spawn(async move { receiver.accept_one().await });

    let mut send_cfg = recv_cfg.clone();
    send_cfg.peer_endpoint = bound;
    let source = boxed_source(StaticSnapshot::new(snapshot.clone()));
    let sent = EntropySender::push(send_cfg, source).await.unwrap();

    let total = recv_handle.await.unwrap().unwrap();
    assert_eq!(total, snapshot.len());
    assert_eq!(sent, snapshot.len());

    let got = sink.snapshot();
    assert_eq!(got, snapshot);
}

#[tokio::test]
async fn empty_snapshot_roundtrips() {
    let recv_cfg = cfg(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        false,
    );
    let sink = Arc::new(MemorySink::default());
    let receiver = EntropyReceiver::bind(recv_cfg.clone(), boxed_sink(sink.clone()))
        .await
        .unwrap();
    let bound = receiver.local_addr().unwrap();
    let recv_handle = tokio::spawn(async move { receiver.accept_one().await });

    let mut send_cfg = recv_cfg.clone();
    send_cfg.peer_endpoint = bound;
    let source = boxed_source(StaticSnapshot::new(Vec::new()));
    let sent = EntropySender::push(send_cfg, source).await.unwrap();

    let total = recv_handle.await.unwrap().unwrap();
    assert_eq!(sent, 0);
    assert_eq!(total, 0);
    assert!(sink.snapshot().is_empty());
}

/// Tampered ciphertext: the receiver decodes a chunk whose AES
/// trailing block has been bit-flipped. PKCS#7 unpad rejects it.
#[tokio::test]
async fn tampered_ciphertext_is_rejected() {
    use dynomite::entropy::send::encrypt_chunk;
    use dynomite::entropy::util::load_material;
    use dynomite::entropy::{
        NegotiationHeader, SnapshotHeader, ENTROPY_COMMAND_SEND, ENTROPY_MAGIC,
    };

    let recv_cfg = cfg(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        true,
    );
    let sink = Arc::new(MemorySink::default());
    let receiver = EntropyReceiver::bind(recv_cfg.clone(), boxed_sink(sink.clone()))
        .await
        .unwrap();
    let bound = receiver.local_addr().unwrap();
    let recv_handle = tokio::spawn(async move { receiver.accept_one().await });

    let material = load_material(&bundled_key_path(), &bundled_iv_path()).unwrap();
    let plaintext = b"some snapshot bytes".to_vec();

    let mut stream = TcpStream::connect(bound).await.unwrap();
    let neg = NegotiationHeader {
        magic: ENTROPY_MAGIC,
        command: ENTROPY_COMMAND_SEND,
        header_size: u32::try_from(recv_cfg.header_size).unwrap(),
        buffer_size: u32::try_from(recv_cfg.buffer_size).unwrap(),
        cipher_size: u32::try_from(recv_cfg.buffer_size + 16).unwrap(),
    };
    stream.write_all(&neg.to_wire()).await.unwrap();
    let snap = SnapshotHeader {
        total_len: u32::try_from(plaintext.len()).unwrap(),
        encrypt_flag: 1,
    };
    stream
        .write_all(&snap.to_wire(recv_cfg.header_size).unwrap())
        .await
        .unwrap();

    let mut ct = encrypt_chunk(&plaintext, &material).unwrap();
    // Flip a bit in the final ciphertext block.
    *ct.last_mut().unwrap() ^= 0x01;
    stream
        .write_all(&u32::try_from(ct.len()).unwrap().to_be_bytes())
        .await
        .unwrap();
    stream.write_all(&ct).await.unwrap();
    let _ = stream.shutdown().await;

    let res = recv_handle.await.unwrap();
    let err = res.expect_err("tampered ciphertext must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("crypto") || msg.contains("padding"),
        "expected crypto/padding error, got {msg}"
    );

    // Sink must remain empty when decryption fails.
    assert!(sink.snapshot().is_empty());
}

/// Truncated stream: client closes after the snapshot header, before
/// any chunks. The receiver's read_exact on the chunk length prefix
/// must return EOF.
#[tokio::test]
async fn truncated_stream_is_rejected() {
    use dynomite::entropy::{
        NegotiationHeader, SnapshotHeader, ENTROPY_COMMAND_SEND, ENTROPY_MAGIC,
    };

    let recv_cfg = cfg(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        false,
    );
    let sink = Arc::new(MemorySink::default());
    let receiver = EntropyReceiver::bind(recv_cfg.clone(), boxed_sink(sink.clone()))
        .await
        .unwrap();
    let bound = receiver.local_addr().unwrap();
    let recv_handle = tokio::spawn(async move { receiver.accept_one().await });

    let mut stream = TcpStream::connect(bound).await.unwrap();
    let neg = NegotiationHeader {
        magic: ENTROPY_MAGIC,
        command: ENTROPY_COMMAND_SEND,
        header_size: u32::try_from(recv_cfg.header_size).unwrap(),
        buffer_size: u32::try_from(recv_cfg.buffer_size).unwrap(),
        cipher_size: u32::try_from(recv_cfg.buffer_size + 16).unwrap(),
    };
    stream.write_all(&neg.to_wire()).await.unwrap();
    let snap = SnapshotHeader {
        total_len: 4096,
        encrypt_flag: 0,
    };
    stream
        .write_all(&snap.to_wire(recv_cfg.header_size).unwrap())
        .await
        .unwrap();
    let _ = stream.shutdown().await;

    let res = tokio::time::timeout(Duration::from_secs(2), recv_handle)
        .await
        .expect("receiver did not exit within timeout")
        .unwrap();
    let err = res.expect_err("truncated stream must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("io") || msg.contains("eof") || msg.contains("entropy"),
        "expected io/eof error, got {msg}"
    );
    assert!(sink.snapshot().is_empty());
}

/// Bad magic word: receiver rejects the negotiation header
/// immediately and surfaces a Protocol error.
#[tokio::test]
async fn bad_magic_is_rejected() {
    let recv_cfg = cfg(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        false,
    );
    let sink = Arc::new(MemorySink::default());
    let receiver = EntropyReceiver::bind(recv_cfg.clone(), boxed_sink(sink.clone()))
        .await
        .unwrap();
    let bound = receiver.local_addr().unwrap();
    let recv_handle = tokio::spawn(async move { receiver.accept_one().await });

    let mut stream = TcpStream::connect(bound).await.unwrap();
    let mut wire = [0u8; 20];
    wire[0..4].copy_from_slice(&0xdead_beefu32.to_be_bytes());
    wire[4..8].copy_from_slice(&1u32.to_be_bytes());
    wire[8..12].copy_from_slice(&64u32.to_be_bytes());
    wire[12..16].copy_from_slice(&256u32.to_be_bytes());
    wire[16..20].copy_from_slice(&272u32.to_be_bytes());
    stream.write_all(&wire).await.unwrap();
    let _ = stream.shutdown().await;

    let res = tokio::time::timeout(Duration::from_secs(2), recv_handle)
        .await
        .expect("receiver did not exit within timeout")
        .unwrap();
    let err = res.expect_err("bad magic must be rejected");
    assert!(err.to_string().contains("magic"));
}

/// Receiver advertised by `run` accepts a connection and resolves
/// the spawned task on shutdown when aborted.
#[tokio::test]
async fn run_spawns_accept_loop() {
    let recv_cfg = cfg(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        false,
    );
    let sink = Arc::new(MemorySink::default());
    // We can't query the listen addr after `run` consumes the
    // receiver, so bind first to learn the port and then start the
    // accept loop on a separate task.
    let receiver = EntropyReceiver::bind(recv_cfg.clone(), boxed_sink(sink.clone()))
        .await
        .unwrap();
    let bound = receiver.local_addr().unwrap();
    let handle = tokio::spawn(async move { receiver.accept_loop().await });

    let mut send_cfg = recv_cfg;
    send_cfg.peer_endpoint = bound;
    let source = boxed_source(StaticSnapshot::new(b"abc".to_vec()));
    let sent = EntropySender::push(send_cfg, source).await.unwrap();
    assert_eq!(sent, 3);

    // Give the receiver a moment to apply the snapshot and update
    // the sink.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(sink.snapshot(), b"abc");

    handle.abort();
    let _ = handle.await;
}

#[tokio::test]
async fn wrong_key_decryption_rejected() {
    // The receiver is configured with the bundled recon_key.pem,
    // but the sender uses a different key file (a tempfile holding
    // 16 bytes that differ from the C-compatible literal). The
    // receiver must reject the decrypted payload because the
    // padding check will fail.
    use std::io::Write;
    let mut bad_key = tempfile::NamedTempFile::new().unwrap();
    bad_key.write_all(b"deadbeefdeadbeef").unwrap();
    bad_key.flush().unwrap();
    let bad_iv = bundled_iv_path();

    let recv_cfg = cfg(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        true,
    );
    let sink = Arc::new(MemorySink::default());
    let receiver = EntropyReceiver::bind(recv_cfg.clone(), boxed_sink(sink.clone()))
        .await
        .unwrap();
    let bound = receiver.local_addr().unwrap();
    let recv_handle = tokio::spawn(async move { receiver.accept_one().await });

    let mut send_cfg = recv_cfg.clone();
    send_cfg.peer_endpoint = bound;
    send_cfg.key_file = bad_key.path().to_path_buf();
    send_cfg.iv_file = bad_iv;
    let source = boxed_source(StaticSnapshot::new(b"this should not be readable".to_vec()));
    // The sender is allowed to complete (it does not know the
    // receiver's key); the receiver is expected to reject.
    let _ = EntropySender::push(send_cfg, source).await;
    let outcome = recv_handle.await.unwrap();
    assert!(
        outcome.is_err(),
        "receiver should have failed to decrypt with wrong key"
    );
    // The mock sink must not have observed any plaintext.
    assert!(
        sink.snapshot().is_empty(),
        "no plaintext should reach the sink"
    );
}
