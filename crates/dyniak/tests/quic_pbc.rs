//! End-to-end PBC-over-QUIC round-trip integration test.
//!
//! Spins up [`dyniak::serve_pbc_quic`] over a real
//! [`dynomite::net::quic::QuicListener`] bound to an ephemeral
//! UDP port, generates a self-signed certificate via `rcgen`
//! (the same approach the engine's Stage 9 QUIC tests use), then
//! dials the listener with the engine's QUIC client and drives a
//! PBC ping + put + get round-trip. The object stored by the put
//! must come back through the get, proving the PBC framing is
//! transport-agnostic: the byte path is QUIC, the protocol is the
//! same one the TCP and TLS-over-TCP listeners serve.
//!
//! Gated on the `quic` feature; without it the file compiles to
//! an empty module.

#![cfg(feature = "quic")]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use prost::Message as _;
use tempfile::tempdir;

use dyniak::proto::pb::{
    read_frame, write_frame, Frame, MessageCode, RpbContent, RpbGetReq, RpbGetResp, RpbPutReq,
    RpbPutResp,
};
use dyniak::serve_pbc_quic;
use dynomite::embed::hooks::{BoxFuture, DatastoreError, Protocol};
use dynomite::embed::Datastore;
use dynomite::msg::{Msg, MsgType};
use dynomite::net::quic::{connect, QuicConfig, QuicListener};

/// Object table keyed by `(bucket, key)`.
type ObjectMap = HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>;

/// Minimal in-memory [`Datastore`] that actually persists the Riak
/// K/V layer so a put followed by a get returns the stored object.
///
/// The [`dynomite::embed::MemoryDatastore`] reports `riak_get` /
/// `riak_put` as unsupported (it only tracks dispatch counts), so
/// this test ships its own map-backed store to exercise the full
/// round-trip without dragging in the `noxu` storage engine.
#[derive(Default)]
struct MapStore {
    inner: Mutex<ObjectMap>,
}

impl Datastore for MapStore {
    fn protocol(&self) -> Protocol {
        Protocol::Custom
    }

    fn dispatch(&self, req: Msg) -> BoxFuture<'_, Result<Msg, DatastoreError>> {
        Box::pin(async move {
            let mut rsp = Msg::new(req.id(), MsgType::Unknown, false);
            rsp.set_parent_id(req.id());
            Ok(rsp)
        })
    }

    fn riak_put<'a>(
        &'a self,
        bucket: &'a [u8],
        key: &'a [u8],
        value: &'a [u8],
        _indexes: &'a [(Vec<u8>, Vec<u8>)],
    ) -> BoxFuture<'a, Result<(), DatastoreError>> {
        Box::pin(async move {
            self.inner
                .lock()
                .insert((bucket.to_vec(), key.to_vec()), value.to_vec());
            Ok(())
        })
    }

    fn riak_get<'a>(
        &'a self,
        bucket: &'a [u8],
        key: &'a [u8],
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, DatastoreError>> {
        Box::pin(async move {
            Ok(self
                .inner
                .lock()
                .get(&(bucket.to_vec(), key.to_vec()))
                .cloned())
        })
    }
}

/// Generate a fresh self-signed cert+key pair on disk and return
/// the owning tempdir plus the two paths.
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

/// Per-operation deadline: QUIC handshake plus the round-trip is
/// comfortably under a second on loopback, but the driver wakes on
/// a coarse timer so a generous bound keeps the test from flaking
/// under load.
const OP_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_pbc_ping_put_get_round_trip() {
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

    let store = Arc::new(MapStore::default());
    let ds: Arc<dyn Datastore> = store.clone();
    let server = tokio::spawn(async move { serve_pbc_quic(listener, ds).await });

    // Dial the listener and split the transport into read / write
    // halves so the PBC framer can drive both directions.
    let client_cfg = QuicConfig::client_insecure();
    let transport = connect(server_addr, client_cfg)
        .await
        .expect("client connect");
    let (mut reader, mut writer) = tokio::io::split(transport);

    // ---- ping ----
    tokio::time::timeout(
        OP_TIMEOUT,
        write_frame(
            &mut writer,
            &Frame::new(MessageCode::PingReq.as_u8(), Vec::new()),
        ),
    )
    .await
    .expect("ping write timed out")
    .expect("ping write");
    let ping_resp = tokio::time::timeout(OP_TIMEOUT, read_frame(&mut reader))
        .await
        .expect("ping read timed out")
        .expect("ping read");
    assert_eq!(ping_resp.code, MessageCode::PingResp.as_u8());
    assert!(ping_resp.body.is_empty());

    // ---- put ----
    let put = RpbPutReq {
        bucket: b"users".to_vec(),
        key: Some(b"alice".to_vec()),
        content: Some(RpbContent {
            value: b"hello-quic".to_vec(),
            ..RpbContent::default()
        }),
        ..RpbPutReq::default()
    };
    tokio::time::timeout(
        OP_TIMEOUT,
        write_frame(
            &mut writer,
            &Frame::new(MessageCode::PutReq.as_u8(), put.encode_to_vec()),
        ),
    )
    .await
    .expect("put write timed out")
    .expect("put write");
    let put_resp = tokio::time::timeout(OP_TIMEOUT, read_frame(&mut reader))
        .await
        .expect("put read timed out")
        .expect("put read");
    assert_eq!(put_resp.code, MessageCode::PutResp.as_u8());
    let _ = RpbPutResp::decode(put_resp.body.as_slice()).expect("decode put resp");

    // ---- get ----
    let get = RpbGetReq {
        bucket: b"users".to_vec(),
        key: b"alice".to_vec(),
        ..RpbGetReq::default()
    };
    tokio::time::timeout(
        OP_TIMEOUT,
        write_frame(
            &mut writer,
            &Frame::new(MessageCode::GetReq.as_u8(), get.encode_to_vec()),
        ),
    )
    .await
    .expect("get write timed out")
    .expect("get write");
    let get_resp = tokio::time::timeout(OP_TIMEOUT, read_frame(&mut reader))
        .await
        .expect("get read timed out")
        .expect("get read");
    assert_eq!(get_resp.code, MessageCode::GetResp.as_u8());
    let body = RpbGetResp::decode(get_resp.body.as_slice()).expect("decode get resp");
    assert_eq!(body.content.len(), 1);
    assert_eq!(
        body.content[0].value,
        b"hello-quic".to_vec(),
        "object stored over QUIC must round-trip back through get"
    );

    // The single K/V op that reaches the substrate's dispatch
    // counter per request is Put and Get (Ping replies directly),
    // so the map must now hold exactly one object.
    assert_eq!(store.inner.lock().len(), 1);

    drop(reader);
    drop(writer);
    server.abort();
    let _ = server.await;
}
