//! Server-variant coverage for the Riak HTTP gateway entry points.
//!
//! [`dyniak::serve_http`], `serve_http_with_search`,
//! `serve_http_with_wasm`, and `serve_http_tls` are exercised by
//! other integration tests; this file covers the remaining combined
//! and TLS-terminating variants -- `serve_http_with_search_and_wasm`,
//! `serve_http_tls_with_search`, `serve_http_tls_with_wasm`, and
//! `serve_http_tls_with_search_and_wasm` -- with a single `GET /ping`
//! round-trip each, so every gateway accept loop has a smoke test.
//!
//! Each variant is gated on the Cargo features it requires; without
//! them the corresponding test compiles away.

// Every test in this file needs at least one of `search` / `wasm`;
// without either the file would be a bag of unused helpers.
#![cfg(any(feature = "search", feature = "wasm"))]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::ServerName;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector};

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
    acceptor_from(load_server_config(&pem.cert, &pem.key, None).unwrap())
}

fn build_connector(pem: &CertFiles) -> TlsConnector {
    connector_from(load_client_config(Some(pem.cert.as_path())).unwrap())
}

fn ds() -> Arc<dyn Datastore> {
    Arc::new(MemoryDatastore::new())
}

/// Drive a plaintext `GET /ping` and assert a 200 response.
async fn assert_plaintext_ping(addr: std::net::SocketAddr) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(b"GET /ping HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    stream.flush().await.unwrap();
    let head = read_response(&mut stream).await;
    assert!(head.starts_with("HTTP/1.1 200"), "got: {head:?}");
}

/// Drive a `GET /ping` over TLS and assert a 200 response.
async fn assert_tls_ping(addr: std::net::SocketAddr, connector: &TlsConnector) {
    let tcp = TcpStream::connect(addr).await.unwrap();
    let name = ServerName::try_from("localhost").unwrap();
    let mut tls = connector.connect(name, tcp).await.unwrap();
    tls.write_all(b"GET /ping HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    tls.flush().await.unwrap();
    let head = read_response(&mut tls).await;
    assert!(head.starts_with("HTTP/1.1 200"), "got: {head:?}");
}

async fn read_response<S: AsyncReadExt + Unpin>(s: &mut S) -> String {
    let mut buf = Vec::with_capacity(256);
    let mut chunk = [0u8; 256];
    loop {
        let n = s.read(&mut chunk).await.unwrap();
        if n == 0 || buf.len() > 4096 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") && buf.ends_with(b"OK") {
            break;
        }
    }
    String::from_utf8_lossy(&buf).into_owned()
}

#[cfg(all(feature = "search", feature = "wasm"))]
#[tokio::test]
async fn serve_http_with_search_and_wasm_serves_ping() {
    use dyniak::mapreduce::WasmModuleStore;
    use dyniak::serve_http_with_search_and_wasm;
    use dynomite_search::VectorRegistry;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let registry = Arc::new(VectorRegistry::new());
    let store = Arc::new(WasmModuleStore::new().expect("wasm store"));
    let server = tokio::spawn(async move {
        let _ = serve_http_with_search_and_wasm(listener, ds(), registry, store).await;
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_plaintext_ping(addr).await;
    server.abort();
    let _ = server.await;
}

#[cfg(feature = "search")]
#[tokio::test]
async fn serve_http_tls_with_search_serves_ping() {
    use dyniak::serve_http_tls_with_search;
    use dynomite_search::VectorRegistry;

    let pem = issue_cert("localhost");
    let acceptor = build_acceptor(&pem);
    let connector = build_connector(&pem);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let registry = Arc::new(VectorRegistry::new());
    let server = tokio::spawn(async move {
        let _ = serve_http_tls_with_search(listener, ds(), registry, acceptor).await;
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_tls_ping(addr, &connector).await;
    server.abort();
    let _ = server.await;
}

#[cfg(feature = "wasm")]
#[tokio::test]
async fn serve_http_tls_with_wasm_serves_ping() {
    use dyniak::mapreduce::WasmModuleStore;
    use dyniak::serve_http_tls_with_wasm;

    let pem = issue_cert("localhost");
    let acceptor = build_acceptor(&pem);
    let connector = build_connector(&pem);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let store = Arc::new(WasmModuleStore::new().expect("wasm store"));
    let server = tokio::spawn(async move {
        let _ = serve_http_tls_with_wasm(listener, ds(), store, acceptor).await;
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_tls_ping(addr, &connector).await;
    server.abort();
    let _ = server.await;
}

#[cfg(all(feature = "search", feature = "wasm"))]
#[tokio::test]
async fn serve_http_tls_with_search_and_wasm_serves_ping() {
    use dyniak::mapreduce::WasmModuleStore;
    use dyniak::serve_http_tls_with_search_and_wasm;
    use dynomite_search::VectorRegistry;

    let pem = issue_cert("localhost");
    let acceptor = build_acceptor(&pem);
    let connector = build_connector(&pem);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let registry = Arc::new(VectorRegistry::new());
    let store = Arc::new(WasmModuleStore::new().expect("wasm store"));
    let server = tokio::spawn(async move {
        let _ =
            serve_http_tls_with_search_and_wasm(listener, ds(), registry, store, acceptor).await;
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_tls_ping(addr, &connector).await;
    server.abort();
    let _ = server.await;
}
