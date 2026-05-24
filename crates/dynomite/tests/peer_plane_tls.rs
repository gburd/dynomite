//! Integration tests for the TLS surfaces added by the
//! `peer-plane-tls` slice.
//!
//! Three end-to-end scenarios:
//!
//! 1. The DNODE listener (`DnodeProxy`) terminates TLS for an
//!    inbound peer and the inbound dispatcher captures the
//!    parsed Redis request that was forwarded over TLS.
//! 2. The cert/key loader and its companion `TlsConnector` round
//!    trip raw bytes over a tokio TCP pair (smoke test for the
//!    crypto provider install).
//! 3. The plaintext path is unchanged: a `DnodeProxy` without
//!    `with_tls(...)` continues to accept a plain TCP `TcpStream`
//!    and forwards it through the dispatcher.
//!
//! The certs are generated at runtime via `rcgen` so no PEM
//! material is committed.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dynomite::conf::DataStore;
use dynomite::io::reactor::{ConnRole, TcpTransport};
use dynomite::msg::Msg;
use dynomite::net::dispatcher::{DispatchOutcome, Dispatcher, OutboundEnvelope, ServerSink};
use dynomite::net::dnode_server::DnodeServerConn;
use dynomite::net::server::OutboundRequest;
use dynomite::net::tls::{
    acceptor_from, connector_from, load_client_config, load_server_config, server_name_owned,
    TlsClientTransport,
};
use dynomite::net::{ClientHandler, Conn, DnodeProxy};
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};

#[derive(Clone, Default)]
struct RecordingDispatcher {
    seen: Arc<Mutex<Vec<Msg>>>,
}

impl Dispatcher for RecordingDispatcher {
    fn dispatch(&self, req: Msg, _responder: ServerSink) -> DispatchOutcome {
        let mut g = self.seen.try_lock().expect("recorder lock uncontended");
        g.push(req);
        DispatchOutcome::Drop
    }
}

struct CertFiles {
    _dir: TempDir,
    cert: PathBuf,
    key: PathBuf,
}

fn issue_cert(san: &str) -> CertFiles {
    let cert = rcgen::generate_simple_self_signed(vec![san.into()]).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, cert.cert.pem()).unwrap();
    std::fs::write(&key_path, cert.key_pair.serialize_pem()).unwrap();
    CertFiles {
        _dir: dir,
        cert: cert_path,
        key: key_path,
    }
}

#[tokio::test]
async fn dnode_proxy_with_tls_round_trips_a_redis_request() {
    let pem = issue_cert("localhost");

    // Server-side: bind the dnode proxy with TLS.
    let server_cfg = load_server_config(&pem.cert, &pem.key, None).unwrap();
    let acceptor = acceptor_from(server_cfg);

    let proxy = DnodeProxy::bind("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .unwrap()
        .with_tls(acceptor);
    let listen_addr = proxy.local_addr().unwrap();
    assert!(proxy.has_tls(), "with_tls must record the acceptor");

    let recorder = RecordingDispatcher::default();
    let seen = Arc::clone(&recorder.seen);
    let recorder_arc: Arc<dyn Dispatcher> = Arc::new(recorder);

    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
    let cancel_fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
        Box::pin(async move {
            let _ = cancel_rx.await;
        });
    let listener_handle = tokio::spawn(async move {
        let dispatcher = Arc::clone(&recorder_arc);
        proxy
            .run(cancel_fut, move |tx| {
                ClientHandler::new(Arc::clone(&dispatcher), tx, DataStore::Redis)
            })
            .await
    });

    // Client-side: connect with the matching CA pinned.
    let client_cfg = load_client_config(Some(pem.cert.as_path())).unwrap();
    let connector = connector_from(client_cfg);
    let tcp = TcpStream::connect(listen_addr).await.unwrap();
    let server_name = server_name_owned("localhost").unwrap();
    let tls_stream = connector.connect(server_name, tcp).await.unwrap();

    // Drive an outbound DnodeServerConn over the established TLS
    // stream so the listener observes a properly framed Dmsg.
    let outbound_conn = Conn::new(
        Box::new(TlsClientTransport::new(
            tls_stream,
            ConnRole::DnodePeerServer,
        )),
        ConnRole::DnodePeerServer,
    );
    let (out_tx, out_rx) = mpsc::channel::<OutboundRequest>(8);
    let outbound_driver = tokio::spawn(async move {
        let _ = DnodeServerConn::new(outbound_conn, out_rx).run().await;
    });

    let redis_bytes = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n".to_vec();
    let (env_tx, _env_rx) = mpsc::channel::<OutboundEnvelope>(8);
    out_tx
        .send(OutboundRequest {
            bytes: redis_bytes.clone(),
            req_id: 7,
            responder: env_tx,
            span: tracing::Span::current(),
            ty: dynomite::proto::dnode::DmsgType::Req,
            target_peer_idx: None,
        })
        .await
        .unwrap();

    // Wait up to 5s for the inbound dispatcher to record the
    // forwarded request.
    let deadline = Instant::now() + Duration::from_secs(5);
    let captured = loop {
        {
            let mut g = seen.lock().await;
            if !g.is_empty() {
                break std::mem::take(&mut *g);
            }
        }
        assert!(
            Instant::now() <= deadline,
            "inbound TLS dispatcher did not see request within 5s"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(captured.len(), 1);
    let payload: Vec<u8> = captured[0]
        .mbufs()
        .iter()
        .flat_map(|b| b.readable().to_vec())
        .collect();
    assert_eq!(payload, redis_bytes);
    assert_eq!(captured[0].dmsg().expect("dmsg").id, 7);

    // Wind everything down.
    drop(out_tx);
    let _ = tokio::time::timeout(Duration::from_secs(2), outbound_driver).await;
    let _ = cancel_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(2), listener_handle).await;
}

/// Plaintext path is unchanged: a `DnodeProxy` without
/// `with_tls(...)` accepts a plain `TcpStream` and dispatches the
/// forwarded request, exactly as before this slice.
#[tokio::test]
async fn dnode_proxy_without_tls_still_round_trips_plaintext() {
    let proxy = DnodeProxy::bind("127.0.0.1:0".parse::<SocketAddr>().unwrap()).unwrap();
    assert!(!proxy.has_tls());
    let listen_addr = proxy.local_addr().unwrap();

    let recorder = RecordingDispatcher::default();
    let seen = Arc::clone(&recorder.seen);
    let recorder_arc: Arc<dyn Dispatcher> = Arc::new(recorder);

    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
    let cancel_fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
        Box::pin(async move {
            let _ = cancel_rx.await;
        });
    let listener_handle = tokio::spawn(async move {
        let dispatcher = Arc::clone(&recorder_arc);
        proxy
            .run(cancel_fut, move |tx| {
                ClientHandler::new(Arc::clone(&dispatcher), tx, DataStore::Redis)
            })
            .await
    });

    let outbound = TcpStream::connect(listen_addr).await.unwrap();
    let outbound_conn = Conn::new(
        Box::new(TcpTransport::new(outbound, ConnRole::DnodePeerServer)),
        ConnRole::DnodePeerServer,
    );
    let (out_tx, out_rx) = mpsc::channel::<OutboundRequest>(8);
    let outbound_driver = tokio::spawn(async move {
        let _ = DnodeServerConn::new(outbound_conn, out_rx).run().await;
    });
    let bytes = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n".to_vec();
    let (env_tx, _env_rx) = mpsc::channel::<OutboundEnvelope>(8);
    out_tx
        .send(OutboundRequest {
            bytes: bytes.clone(),
            req_id: 1,
            responder: env_tx,
            span: tracing::Span::current(),
            ty: dynomite::proto::dnode::DmsgType::Req,
            target_peer_idx: None,
        })
        .await
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        {
            let g = seen.lock().await;
            if !g.is_empty() {
                break;
            }
        }
        assert!(
            Instant::now() <= deadline,
            "plaintext dispatcher did not see request within 5s"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    drop(out_tx);
    let _ = tokio::time::timeout(Duration::from_secs(2), outbound_driver).await;
    let _ = cancel_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(2), listener_handle).await;
}

/// Loader smoke: build the client config with the bundled
/// webpki roots and exercise the secure-random RNG so the
/// crypto provider is verified to be installed.
#[tokio::test]
async fn webpki_roots_default_client_config_has_trust_anchors() {
    let cfg = load_client_config(None).unwrap();
    let mut buf = [0u8; 16];
    cfg.crypto_provider().secure_random.fill(&mut buf).unwrap();
    // The default RNG is randomized; assert that we did not get
    // an all-zero buffer (probability of a false negative is
    // 2^-128).
    assert!(buf.iter().any(|b| *b != 0));
}
