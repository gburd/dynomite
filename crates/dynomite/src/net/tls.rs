//! TLS helpers for the peer plane and the Riak gateways.
//!
//! Two small surfaces:
//!
//! * [`load_server_config`] reads PEM cert + key from disk and
//!   returns an [`Arc<rustls::ServerConfig>`] suitable for
//!   wrapping [`tokio_rustls::TlsAcceptor`]. When a CA path is
//!   given, client certificates are verified against that CA
//!   (mTLS); when it is `None`, client cert verification is
//!   disabled.
//! * [`load_client_config`] builds an [`Arc<rustls::ClientConfig>`]
//!   suitable for [`tokio_rustls::TlsConnector`]. When a CA path
//!   is given the root store is loaded from that file; otherwise
//!   the bundled `webpki_roots` Mozilla bundle is used.
//!
//! Two thin newtypes wrap an established TLS stream and expose
//! the [`crate::io::reactor::Transport`] interface so the rest of
//! the network stack stays unchanged:
//!
//! * [`TlsServerTransport`] wraps the inbound side of a TLS
//!   connection ([`tokio_rustls::server::TlsStream<TcpStream>`]).
//! * [`TlsClientTransport`] wraps the outbound side
//!   ([`tokio_rustls::client::TlsStream<TcpStream>`]).
//!
//! Mismatched config (cert without key or key without cert) is
//! caught by the conf validator (see
//! [`crate::conf::ConfPool::validate`]); this module assumes the
//! caller has already cross-checked.
//!
//! # Examples
//!
//! ```no_run
//! use std::path::PathBuf;
//! use dynomite::net::tls::{load_client_config, load_server_config};
//!
//! let cert = PathBuf::from("/etc/dynomite/peer.crt");
//! let key = PathBuf::from("/etc/dynomite/peer.key");
//! let _server = load_server_config(&cert, &key, None).unwrap();
//! let _client = load_client_config(None).unwrap();
//! ```
//!
//! # Provider selection
//!
//! `rustls` 0.23 requires an installed [crypto provider]. This
//! module installs the `ring` provider as the process default the
//! first time it is called, via a `OnceLock`. The install is
//! idempotent and the lock is local to this module so callers do
//! not have to think about it.
//!
//! [crypto provider]: rustls::crypto::CryptoProvider

use std::fs::File;
use std::io::{self, BufReader};
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::io::reactor::{ConnRole, Transport};

/// Errors raised by the TLS loaders.
#[derive(Debug, Error)]
pub enum TlsError {
    /// Failed to open or read a PEM file.
    #[error("tls: io reading {path}: {source}")]
    Io {
        /// Path that failed.
        path: String,
        /// Underlying I/O error.
        #[source]
        source: io::Error,
    },
    /// PEM file did not contain a usable certificate or key.
    #[error("tls: no usable {kind} found in {path}")]
    NoMaterial {
        /// Either `"certificate"` or `"private key"`.
        kind: &'static str,
        /// Path that came up empty.
        path: String,
    },
    /// `rustls` rejected the supplied material.
    #[error("tls: rustls rejected configuration: {0}")]
    Rustls(String),
}

/// One-time installer for the rustls process-default crypto
/// provider. Selecting `ring` keeps us off `aws-lc-rs` (whose
/// build-time C dependency fails on the project's nix shell) and
/// matches the `quiche` transport's bundled provider.
fn ensure_provider_installed() {
    static INSTALL: OnceLock<()> = OnceLock::new();
    INSTALL.get_or_init(|| {
        // Ignore the result: another thread or another caller in
        // the same process may have installed a provider already
        // (the test harness, for example, links every binary into
        // one process). The provider we install is the same
        // either way.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let file = File::open(path).map_err(|e| TlsError::Io {
        path: path.display().to_string(),
        source: e,
    })?;
    let mut reader = BufReader::new(file);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .collect::<io::Result<Vec<_>>>()
        .map_err(|e| TlsError::Io {
            path: path.display().to_string(),
            source: e,
        })?;
    if certs.is_empty() {
        return Err(TlsError::NoMaterial {
            kind: "certificate",
            path: path.display().to_string(),
        });
    }
    Ok(certs)
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsError> {
    let file = File::open(path).map_err(|e| TlsError::Io {
        path: path.display().to_string(),
        source: e,
    })?;
    let mut reader = BufReader::new(file);
    let key = rustls_pemfile::private_key(&mut reader).map_err(|e| TlsError::Io {
        path: path.display().to_string(),
        source: e,
    })?;
    key.ok_or_else(|| TlsError::NoMaterial {
        kind: "private key",
        path: path.display().to_string(),
    })
}

/// Build a [`ServerConfig`] from PEM cert + key.
///
/// When `client_ca` is `Some(p)`, every accepted connection must
/// present a certificate signed by a CA from that PEM bundle
/// (mutual TLS). When `None`, client certificates are not
/// requested and the server accepts plaintext authentication.
///
/// # Errors
/// Returns [`TlsError`] if any file is missing, malformed, or
/// rejected by rustls.
pub fn load_server_config(
    cert_path: &Path,
    key_path: &Path,
    client_ca: Option<&Path>,
) -> Result<Arc<ServerConfig>, TlsError> {
    ensure_provider_installed();
    let certs = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;

    let builder = ServerConfig::builder();
    let cfg = if let Some(ca_path) = client_ca {
        let ca_certs = load_certs(ca_path)?;
        let mut roots = RootCertStore::empty();
        for c in ca_certs {
            roots
                .add(c)
                .map_err(|e| TlsError::Rustls(format!("ca add: {e}")))?;
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|e| TlsError::Rustls(format!("client verifier: {e}")))?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
            .map_err(|e| TlsError::Rustls(e.to_string()))?
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| TlsError::Rustls(e.to_string()))?
    };
    Ok(Arc::new(cfg))
}

/// Build a [`ClientConfig`] for outbound TLS.
///
/// When `ca_path` is `Some(p)`, the supplied PEM bundle is the
/// only trust anchor. When `None`, the Mozilla bundle from
/// [`webpki_roots`] is loaded; this is appropriate for clusters
/// whose peer certs chain to a public CA, and is the conservative
/// default for outbound calls in tests where the operator has
/// not pinned a CA.
///
/// # Errors
/// Returns [`TlsError`] if a CA file is missing or malformed.
pub fn load_client_config(ca_path: Option<&Path>) -> Result<Arc<ClientConfig>, TlsError> {
    ensure_provider_installed();
    let mut roots = RootCertStore::empty();
    if let Some(p) = ca_path {
        let ca_certs = load_certs(p)?;
        for c in ca_certs {
            roots
                .add(c)
                .map_err(|e| TlsError::Rustls(format!("ca add: {e}")))?;
        }
    } else {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }
    let cfg = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(cfg))
}

/// Convenience wrapper that adapts a [`ServerConfig`] into a
/// [`TlsAcceptor`].
#[must_use]
pub fn acceptor_from(server_config: Arc<ServerConfig>) -> TlsAcceptor {
    TlsAcceptor::from(server_config)
}

/// Convenience wrapper that adapts a [`ClientConfig`] into a
/// [`TlsConnector`].
#[must_use]
pub fn connector_from(client_config: Arc<ClientConfig>) -> TlsConnector {
    TlsConnector::from(client_config)
}

/// Parse a [`ServerName`] from a host string.
///
/// # Errors
/// Returns [`TlsError::Rustls`] when the input is not a valid
/// DNS name or IP literal.
pub fn server_name_owned(host: &str) -> Result<ServerName<'static>, TlsError> {
    ServerName::try_from(host.to_string())
        .map_err(|e| TlsError::Rustls(format!("server name: {e}")))
}

/// [`Transport`] wrapping a server-side TLS stream over a TCP
/// connection.
#[derive(Debug)]
pub struct TlsServerTransport {
    inner: tokio_rustls::server::TlsStream<TcpStream>,
    role: ConnRole,
    peer_addr: Option<SocketAddr>,
}

impl TlsServerTransport {
    /// Wrap an established server-side TLS stream.
    #[must_use]
    pub fn new(stream: tokio_rustls::server::TlsStream<TcpStream>, role: ConnRole) -> Self {
        let peer_addr = stream.get_ref().0.peer_addr().ok();
        Self {
            inner: stream,
            role,
            peer_addr,
        }
    }
}

impl Transport for TlsServerTransport {
    fn role(&self) -> ConnRole {
        self.role
    }
    fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer_addr
    }
}

impl AsyncRead for TlsServerTransport {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for TlsServerTransport {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// [`Transport`] wrapping a client-side TLS stream over a TCP
/// connection.
#[derive(Debug)]
pub struct TlsClientTransport {
    inner: tokio_rustls::client::TlsStream<TcpStream>,
    role: ConnRole,
    peer_addr: Option<SocketAddr>,
}

impl TlsClientTransport {
    /// Wrap an established client-side TLS stream.
    #[must_use]
    pub fn new(stream: tokio_rustls::client::TlsStream<TcpStream>, role: ConnRole) -> Self {
        let peer_addr = stream.get_ref().0.peer_addr().ok();
        Self {
            inner: stream,
            role,
            peer_addr,
        }
    }
}

impl Transport for TlsClientTransport {
    fn role(&self) -> ConnRole {
        self.role
    }
    fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer_addr
    }
}

impl AsyncRead for TlsClientTransport {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for TlsClientTransport {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn write_pem(dir: &TempDir, name: &str, body: &str) -> std::path::PathBuf {
        let p = dir.path().join(name);
        let mut f = File::create(&p).unwrap();
        f.write_all(body.as_bytes()).unwrap();
        p
    }

    fn issue_self_signed() -> (String, String) {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        (cert.cert.pem(), cert.key_pair.serialize_pem())
    }

    #[test]
    fn load_server_config_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let (cert_pem, key_pem) = issue_self_signed();
        let cert = write_pem(&dir, "cert.pem", &cert_pem);
        let key = write_pem(&dir, "key.pem", &key_pem);
        let cfg = load_server_config(&cert, &key, None).unwrap();
        assert!(Arc::strong_count(&cfg) >= 1);
    }

    #[test]
    fn load_server_config_rejects_missing_cert() {
        let dir = tempfile::tempdir().unwrap();
        let bogus = dir.path().join("missing.pem");
        let key = write_pem(&dir, "key.pem", "");
        let err = load_server_config(&bogus, &key, None).expect_err("missing");
        assert!(matches!(err, TlsError::Io { .. }), "got {err:?}");
    }

    #[test]
    fn load_server_config_rejects_empty_cert_file() {
        let dir = tempfile::tempdir().unwrap();
        let cert = write_pem(&dir, "cert.pem", "");
        let key = write_pem(&dir, "key.pem", "");
        let err = load_server_config(&cert, &key, None).expect_err("empty");
        assert!(matches!(
            err,
            TlsError::NoMaterial {
                kind: "certificate",
                ..
            }
        ));
    }

    #[test]
    fn load_client_config_with_webpki_default() {
        let cfg = load_client_config(None).unwrap();
        assert!(Arc::strong_count(&cfg) >= 1);
    }

    #[test]
    fn server_name_owned_accepts_dns_label() {
        assert!(server_name_owned("localhost").is_ok());
    }
}
