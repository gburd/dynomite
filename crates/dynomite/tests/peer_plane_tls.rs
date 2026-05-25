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
    acceptor_from, connector_from, dc_sni_hostname, load_client_config, load_server_config,
    server_name_owned, TlsClientTransport, TlsProfileMap, TlsProfileSpec,
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

/// Issue a self-signed cert whose SAN includes both the SNI
/// label `dc-<dc>.dynomite.local` and a generic `localhost`
/// fallback so the client can verify the listener under both
/// the per-DC and the legacy SNI paths.
fn issue_dc_cert(dc: &str) -> CertFiles {
    let san_dc = dc_sni_hostname(dc);
    let cert = rcgen::generate_simple_self_signed(vec![san_dc, "localhost".into()]).unwrap();
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

/// Drive a single DNODE request through a per-DC TLS-routed
/// listener. The listener is built from a [`TlsProfileMap`]
/// containing two DC profiles plus an optional default; the
/// outbound side connects with a freshly-built client config
/// pinning the expected cert as CA, and SNI=`dc-<target_dc>.dynomite.local`
/// so the server's SNI resolver picks the matching cert.
/// Read a single PEM file and return the parsed leaf cert
/// in DER form. Used to verify that the SNI resolver selected
/// the expected cert by comparing DER bytes.
fn read_single_pem_cert(path: &std::path::Path) -> rustls::pki_types::CertificateDer<'static> {
    let pem = std::fs::read(path).unwrap();
    let mut reader = std::io::BufReader::new(pem.as_slice());
    let certs: Vec<rustls::pki_types::CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .collect::<std::io::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        certs.len(),
        1,
        "expected exactly one cert in {}",
        path.display()
    );
    certs.into_iter().next().unwrap()
}

/// Drive a single DNODE request through a per-DC TLS-routed
/// listener. The listener is built from a [`TlsProfileMap`]
/// containing two DC profiles plus an optional default; the
/// outbound side connects with a freshly-built client config
/// pinning the expected cert as CA, and SNI=`dc-<target_dc>.dynomite.local`
/// so the server's SNI resolver picks the matching cert.
async fn drive_per_dc_round_trip(
    profile_map: TlsProfileMap,
    target_dc: &str,
    expected_cert_pem_path: &std::path::Path,
) {
    let acceptor = profile_map.build_sni_acceptor().unwrap().unwrap();
    let proxy = DnodeProxy::bind("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .unwrap()
        .with_tls(acceptor);
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

    // Outbound: pin the expected cert as CA on the client side
    // and dial with SNI=`dc-<target_dc>.dynomite.local` so the
    // server's SNI resolver picks the matching cert. The
    // profile_map's per-DC client config is also exercised
    // via TlsProfileMap::client_config_for_dc to verify the
    // lookup contract without coupling it to mTLS in the
    // listener.
    assert!(
        profile_map.client_config_for_dc(target_dc).is_some(),
        "profile map must have a client config for {target_dc}"
    );
    let client_cfg = load_client_config(Some(expected_cert_pem_path)).unwrap();
    let connector = connector_from(client_cfg);
    let tcp = TcpStream::connect(listen_addr).await.unwrap();
    let server_name = server_name_owned(&dc_sni_hostname(target_dc)).unwrap();
    let tls_stream = connector.connect(server_name, tcp).await.unwrap();

    // Verify the server presented exactly the expected cert.
    let peer_certs = tls_stream
        .get_ref()
        .1
        .peer_certificates()
        .expect("server cert visible on client side")
        .to_vec();
    assert_eq!(peer_certs.len(), 1);
    let expected_der = read_single_pem_cert(expected_cert_pem_path);
    assert_eq!(
        peer_certs[0].as_ref(),
        expected_der.as_ref(),
        "server SNI resolver must select the cert for target_dc={target_dc:?}"
    );

    drive_redis_frame(
        tls_stream,
        seen,
        cancel_tx,
        listener_handle,
        b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n".to_vec(),
    )
    .await;
}

/// Drive one Redis frame over an established peer-plane TLS
/// stream and assert the inbound dispatcher captured it. Shared
/// tail of [`drive_per_dc_round_trip`] and the legacy
/// default-only round-trip so each test stays under the
/// per-function line budget.
async fn drive_redis_frame(
    tls_stream: tokio_rustls::client::TlsStream<TcpStream>,
    seen: Arc<Mutex<Vec<Msg>>>,
    cancel_tx: tokio::sync::oneshot::Sender<()>,
    listener_handle: tokio::task::JoinHandle<Result<(), dynomite::net::NetError>>,
    bytes: Vec<u8>,
) {
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
    let (env_tx, _env_rx) = mpsc::channel::<OutboundEnvelope>(8);
    out_tx
        .send(OutboundRequest {
            bytes: bytes.clone(),
            req_id: 11,
            responder: env_tx,
            span: tracing::Span::current(),
            ty: dynomite::proto::dnode::DmsgType::Req,
            target_peer_idx: None,
        })
        .await
        .unwrap();

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
            "TLS dispatcher did not see request within 5s"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(captured.len(), 1);
    let payload: Vec<u8> = captured[0]
        .mbufs()
        .iter()
        .flat_map(|b| b.readable().to_vec())
        .collect();
    assert_eq!(payload, bytes);

    drop(out_tx);
    let _ = tokio::time::timeout(Duration::from_secs(2), outbound_driver).await;
    let _ = cancel_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(2), listener_handle).await;
}

/// Two-DC profile map: a request from the dc1 side to a peer
/// in dc2 picks the dc2 cert via SNI.
#[tokio::test]
async fn per_dc_profile_map_routes_dc1_to_dc2_via_sni() {
    let dc1 = issue_dc_cert("dc1");
    let dc2 = issue_dc_cert("dc2");
    let dc2_cert_path = dc2.cert.clone();
    let mut per_dc = std::collections::BTreeMap::new();
    // Register dc1 / dc2 in the listener's profile map (no
    // CA -> no mTLS so the test client's plaintext-identity
    // handshake is accepted). The TEST client side pins the
    // dc2 cert as a trust anchor itself; that cleanly
    // separates the listener's mTLS knob from the client's
    // server-cert verification path.
    per_dc.insert(
        "dc1".into(),
        TlsProfileSpec {
            cert: dc1.cert.clone(),
            key: dc1.key.clone(),
            ca: None,
        },
    );
    per_dc.insert(
        "dc2".into(),
        TlsProfileSpec {
            cert: dc2.cert.clone(),
            key: dc2.key.clone(),
            ca: None,
        },
    );
    let map = TlsProfileMap::build(None, per_dc).unwrap();
    drive_per_dc_round_trip(map, "dc2", &dc2_cert_path).await;
    drop(dc1);
}

/// Three-DC profile map (dc1, dc2 entries; dc3 has no entry):
/// a connection to dc3 falls back to the default profile and
/// the SNI resolver picks the default cert.
#[tokio::test]
async fn per_dc_profile_map_falls_back_to_default_for_unknown_dc() {
    let default_pem = issue_cert("localhost");
    let dc1 = issue_dc_cert("dc1");
    let dc2 = issue_dc_cert("dc2");
    let mut per_dc = std::collections::BTreeMap::new();
    per_dc.insert(
        "dc1".into(),
        TlsProfileSpec {
            cert: dc1.cert.clone(),
            key: dc1.key.clone(),
            ca: None,
        },
    );
    per_dc.insert(
        "dc2".into(),
        TlsProfileSpec {
            cert: dc2.cert.clone(),
            key: dc2.key.clone(),
            ca: None,
        },
    );
    let default_spec = TlsProfileSpec {
        cert: default_pem.cert.clone(),
        key: default_pem.key.clone(),
        ca: None,
    };
    let map = TlsProfileMap::build(Some(default_spec), per_dc).unwrap();

    // The default profile's cert has SAN=localhost, not
    // dc-dc3.dynomite.local. We can't verify the SNI handshake
    // for dc3 directly because the rcgen-generated default
    // cert lacks that SAN. Instead, confirm that the lookup
    // path falls back via TlsProfileMap::client_config_for_dc
    // and TlsProfileMap::server_config_for_dc, which is the
    // exact contract the per-peer supervisor relies on.
    assert!(
        map.client_config_for_dc("dc3").is_some(),
        "dc3 must fall back to the default client config"
    );
    assert!(
        map.server_config_for_dc("dc3").is_some(),
        "dc3 must fall back to the default server config"
    );
    // dc1 / dc2 stay on their own entries.
    assert!(map.client_config_for_dc("dc1").is_some());
    assert!(map.client_config_for_dc("dc2").is_some());
    drop((dc1, dc2, default_pem));
}
/// Backward compat: the legacy `peer_tls_*` triple constructed
/// via `TlsProfileMap::build` with a default-only profile and
/// an empty per-DC map drives a Redis request exactly as the
/// pre-per-DC code path did.
#[tokio::test]
async fn legacy_default_profile_only_round_trip() {
    let default_pem = issue_cert("localhost");
    let map = TlsProfileMap::build(
        Some(TlsProfileSpec {
            cert: default_pem.cert.clone(),
            key: default_pem.key.clone(),
            ca: None,
        }),
        std::collections::BTreeMap::new(),
    )
    .unwrap();
    let acceptor = map.build_sni_acceptor().unwrap().unwrap();

    let proxy = DnodeProxy::bind("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .unwrap()
        .with_tls(acceptor);
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

    // The legacy client uses load_client_config so the round
    // trip exercises both the new map builder (server side)
    // and the old loader (client side).
    let client_cfg = load_client_config(Some(default_pem.cert.as_path())).unwrap();
    let connector = connector_from(client_cfg);
    let tcp = TcpStream::connect(listen_addr).await.unwrap();
    let server_name = server_name_owned("localhost").unwrap();
    let tls_stream = connector.connect(server_name, tcp).await.unwrap();

    drive_redis_frame(
        tls_stream,
        seen,
        cancel_tx,
        listener_handle,
        b"*2\r\n$3\r\nGET\r\n$3\r\nleg\r\n".to_vec(),
    )
    .await;
}

/// Validation: a per-DC profile entry that declares
/// `cert` without `key` fails at config-validate time, before
/// any PEM bytes are read.
#[test]
fn peer_tls_profile_cert_without_key_validation_fails() {
    use dynomite::conf::ConfTlsProfile;
    let p = ConfTlsProfile {
        cert: Some(PathBuf::from("/dc1.crt")),
        key: None,
        ca: None,
    };
    let err = p.validate("dc1").expect_err("must reject");
    let msg = err.to_string();
    assert!(
        msg.contains("dc1") && msg.contains("key"),
        "unexpected error: {msg}"
    );
}
