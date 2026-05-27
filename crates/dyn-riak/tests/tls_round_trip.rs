//! TLS round-trip integration tests for the Riak PBC and HTTP
//! gateways.
//!
//! Three tests:
//!
//! * `pbc_ping_round_trips_over_tls` -- the PBC server terminates
//!   TLS via `dyn_riak::serve_pbc_tls`, the client connects via
//!   `tokio_rustls::TlsConnector`, sends an `RpbPingReq` and
//!   asserts the framed `RpbPingResp` comes back.
//! * `http_ping_round_trips_over_tls` -- the HTTP gateway
//!   terminates TLS via `dyn_riak::serve_http_tls`, a manual
//!   tokio-rustls client issues `GET /ping` and asserts the
//!   server replies `200 OK`. We send / receive raw HTTP/1.1 to
//!   avoid pulling a hyper client into the TLS test surface.
//! * `pbc_ping_round_trips_plaintext_when_tls_unset` -- guards
//!   the unchanged plaintext path.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use prost::Message as _;
use rustls::pki_types::ServerName;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use dyn_riak::proto::pb::{read_frame, write_frame, Frame, MessageCode, RpbPingResp};
use dyn_riak::{serve_http, serve_http_tls, serve_pbc, serve_pbc_tls};
use dynomite::embed::{Datastore, MemoryDatastore};
use dynomite::net::tls::{acceptor_from, connector_from, load_client_config, load_server_config};

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
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
    CertFiles {
        _dir: dir,
        cert: cert_path,
        key: key_path,
    }
}

fn build_acceptor(pem: &CertFiles) -> TlsAcceptor {
    let cfg = load_server_config(&pem.cert, &pem.key, None).unwrap();
    acceptor_from(cfg)
}

fn build_connector(pem: &CertFiles) -> TlsConnector {
    let cfg = load_client_config(Some(pem.cert.as_path())).unwrap();
    connector_from(cfg)
}

#[tokio::test]
async fn pbc_ping_round_trips_over_tls() {
    let pem = issue_cert("localhost");
    let acceptor = build_acceptor(&pem);
    let connector = build_connector(&pem);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
    let server = tokio::spawn(async move { serve_pbc_tls(listener, ds, acceptor).await });

    // Brief delay so the accept loop is in select before we
    // hammer it.
    tokio::time::sleep(Duration::from_millis(10)).await;

    let tcp = TcpStream::connect(addr).await.unwrap();
    let server_name = ServerName::try_from("localhost").unwrap();
    let mut tls = connector.connect(server_name, tcp).await.unwrap();

    let frame = Frame::new(MessageCode::PingReq.as_u8(), Vec::new());
    write_frame(&mut tls, &frame).await.unwrap();
    let resp = read_frame(&mut tls).await.unwrap();
    assert_eq!(resp.code, MessageCode::PingResp.as_u8());
    assert!(RpbPingResp::decode(resp.body.as_slice()).is_ok());

    drop(tls);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn pbc_ping_round_trips_plaintext_when_tls_unset() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
    let server = tokio::spawn(async move { serve_pbc(listener, ds).await });
    tokio::time::sleep(Duration::from_millis(10)).await;

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let frame = Frame::new(MessageCode::PingReq.as_u8(), Vec::new());
    write_frame(&mut stream, &frame).await.unwrap();
    let resp = read_frame(&mut stream).await.unwrap();
    assert_eq!(resp.code, MessageCode::PingResp.as_u8());

    drop(stream);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn http_ping_round_trips_over_tls() {
    let pem = issue_cert("localhost");
    let acceptor = build_acceptor(&pem);
    let connector = build_connector(&pem);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
    let server = tokio::spawn(async move { serve_http_tls(listener, ds, acceptor).await });
    tokio::time::sleep(Duration::from_millis(10)).await;

    let tcp = TcpStream::connect(addr).await.unwrap();
    let server_name = ServerName::try_from("localhost").unwrap();
    let mut tls = connector.connect(server_name, tcp).await.unwrap();

    // Issue a minimal HTTP/1.1 GET /ping request directly. Using
    // a hand-rolled request avoids dragging hyper-rustls into the
    // dyn-riak test surface; the wire shape is a textbook GET.
    let req = b"GET /ping HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    tls.write_all(req).await.unwrap();
    tls.flush().await.unwrap();

    let mut buf = Vec::with_capacity(256);
    let mut chunk = [0u8; 256];
    loop {
        let n = tls.read(&mut chunk).await.unwrap();
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") && buf.ends_with(b"OK") {
            break;
        }
        if buf.len() > 4096 {
            break;
        }
    }
    let head = std::str::from_utf8(&buf).unwrap_or("");
    assert!(
        head.starts_with("HTTP/1.1 200"),
        "expected 200 status, got: {head:?}"
    );
    assert!(head.ends_with("OK"), "expected OK body, got: {head:?}");

    drop(tls);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn http_ping_round_trips_plaintext_when_tls_unset() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
    let server = tokio::spawn(async move { serve_http(listener, ds).await });
    tokio::time::sleep(Duration::from_millis(10)).await;

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = b"GET /ping HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    stream.write_all(req).await.unwrap();
    stream.flush().await.unwrap();
    let mut buf = Vec::with_capacity(256);
    let mut chunk = [0u8; 256];
    loop {
        let n = stream.read(&mut chunk).await.unwrap();
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() > 4096 {
            break;
        }
    }
    let head = std::str::from_utf8(&buf).unwrap_or("");
    assert!(head.starts_with("HTTP/1.1 200"), "got: {head:?}");

    drop(stream);
    server.abort();
    let _ = server.await;
}
