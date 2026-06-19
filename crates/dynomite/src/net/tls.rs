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

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufReader};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

use parking_lot::RwLock;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::server::{ClientHello, ResolvesServerCert, WebPkiClientVerifier};
use rustls::sign::CertifiedKey;
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

/// SNI label the peer plane uses to route handshakes to the
/// matching per-DC profile.
///
/// Both ends of a peer-plane TLS handshake set the SNI to
/// `dc-<peer-dc>.dynomite.local`; the listener's SNI resolver
/// (see [`TlsProfileMap::build_sni_acceptor`]) parses this label
/// to pick the certificate.
///
/// # Examples
///
/// ```
/// use dynomite::net::tls::dc_sni_hostname;
/// assert_eq!(dc_sni_hostname("dc1"), "dc-dc1.dynomite.local");
/// ```
#[must_use]
pub fn dc_sni_hostname(dc: &str) -> String {
    format!("dc-{dc}.dynomite.local")
}

/// Inverse of [`dc_sni_hostname`]: extract the DC name from an
/// SNI label that follows the `dc-<dc>.dynomite.local` shape, or
/// return `None` if the label does not match.
fn dc_from_sni_label(name: &str) -> Option<&str> {
    name.strip_prefix("dc-")
        .and_then(|rest| rest.strip_suffix(".dynomite.local"))
        .filter(|dc| !dc.is_empty())
}

/// PEM material for one TLS profile.
///
/// Used by [`TlsProfileMap::build`] to assemble per-DC server
/// and client configs from on-disk paths. `cert` and `key` are
/// required; `ca` is optional and, when present, pins the
/// trust anchor for both directions and turns the listener
/// into a mutual-TLS deployment.
#[derive(Debug, Clone)]
pub struct TlsProfileSpec {
    /// PEM certificate path.
    pub cert: PathBuf,
    /// PEM private-key path matching [`Self::cert`].
    pub key: PathBuf,
    /// Optional PEM CA bundle.
    pub ca: Option<PathBuf>,
}

/// Bundle of precompiled rustls configs for the peer plane,
/// keyed by datacenter name plus an optional default profile
/// used as a fallback for any DC without an explicit entry.
///
/// The map is built once at startup by
/// [`TlsProfileMap::build`] and shared (cheaply, every member
/// is an `Arc` under the hood) across the dnode listener and
/// every per-peer outbound supervisor. Lookups are O(log n) in
/// the number of DCs.
///
/// # Examples
///
/// ```no_run
/// use std::collections::BTreeMap;
/// use std::path::PathBuf;
/// use dynomite::net::tls::{TlsProfileMap, TlsProfileSpec};
///
/// let mut per_dc = BTreeMap::new();
/// per_dc.insert(
///     "dc1".to_string(),
///     TlsProfileSpec {
///         cert: PathBuf::from("/etc/dynomite/dc1.pem"),
///         key: PathBuf::from("/etc/dynomite/dc1.key"),
///         ca: None,
///     },
/// );
/// let map = TlsProfileMap::build(None, per_dc).unwrap();
/// assert!(map.client_config_for_dc("dc1").is_some());
/// assert!(map.client_config_for_dc("dc-without-profile").is_none());
/// ```
#[derive(Clone, Default)]
pub struct TlsProfileMap {
    per_dc_server: BTreeMap<String, Arc<ServerConfig>>,
    per_dc_client: BTreeMap<String, Arc<ClientConfig>>,
    per_dc_certified: BTreeMap<String, Arc<CertifiedKey>>,
    default_server: Option<Arc<ServerConfig>>,
    default_client: Option<Arc<ClientConfig>>,
    default_certified: Option<Arc<CertifiedKey>>,
    /// Combined CA cert chain (DER) across every profile that
    /// supplied a CA bundle. Used by
    /// [`Self::build_sni_acceptor`] to assemble a single client
    /// verifier that trusts any configured CA.
    combined_ca_certs: Vec<CertificateDer<'static>>,
    has_any_client_ca: bool,
}

impl std::fmt::Debug for TlsProfileMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsProfileMap")
            .field("per_dc", &self.per_dc_server.keys().collect::<Vec<_>>())
            .field("has_default", &self.default_server.is_some())
            .field("has_any_client_ca", &self.has_any_client_ca)
            .finish_non_exhaustive()
    }
}

impl TlsProfileMap {
    /// Build a map from a default profile (the legacy
    /// `peer_tls_*` triple) plus a `dc -> TlsProfileSpec` map.
    ///
    /// Either argument may be empty: a `None` `default` plus
    /// an empty `per_dc` produces an empty map (peer plane runs
    /// plaintext).
    ///
    /// # Errors
    /// Returns the first [`TlsError`] from a failing PEM load.
    pub fn build(
        default: Option<TlsProfileSpec>,
        per_dc: BTreeMap<String, TlsProfileSpec>,
    ) -> Result<Self, TlsError> {
        ensure_provider_installed();
        let provider = rustls::crypto::CryptoProvider::get_default()
            .cloned()
            .unwrap_or_else(|| Arc::new(rustls::crypto::ring::default_provider()));

        let mut map = Self::default();

        if let Some(spec) = default {
            let server_cfg = load_server_config(&spec.cert, &spec.key, spec.ca.as_deref())?;
            let client_cfg = load_client_config(spec.ca.as_deref())?;
            let certified = load_certified_key(&spec.cert, &spec.key, provider.as_ref())?;
            if let Some(ca_path) = spec.ca.as_deref() {
                map.combined_ca_certs.extend(load_certs(ca_path)?);
                map.has_any_client_ca = true;
            }
            map.default_server = Some(server_cfg);
            map.default_client = Some(client_cfg);
            map.default_certified = Some(certified);
        }

        for (dc, spec) in per_dc {
            let server_cfg = load_server_config(&spec.cert, &spec.key, spec.ca.as_deref())?;
            let client_cfg = load_client_config(spec.ca.as_deref())?;
            let certified = load_certified_key(&spec.cert, &spec.key, provider.as_ref())?;
            if let Some(ca_path) = spec.ca.as_deref() {
                map.combined_ca_certs.extend(load_certs(ca_path)?);
                map.has_any_client_ca = true;
            }
            map.per_dc_server.insert(dc.clone(), server_cfg);
            map.per_dc_client.insert(dc.clone(), client_cfg);
            map.per_dc_certified.insert(dc, certified);
        }

        Ok(map)
    }

    /// True when no profile (default or per-DC) is configured.
    /// In this state the peer plane runs plaintext.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.default_server.is_none() && self.per_dc_server.is_empty()
    }

    /// Server config to use for a connection negotiated with a
    /// peer in `dc`. Returns the per-DC entry if present,
    /// otherwise the default profile, otherwise `None`.
    #[must_use]
    pub fn server_config_for_dc(&self, dc: &str) -> Option<Arc<ServerConfig>> {
        self.per_dc_server
            .get(dc)
            .cloned()
            .or_else(|| self.default_server.clone())
    }

    /// Client config to use when dialing a peer in `dc`.
    /// Returns the per-DC entry if present, otherwise the
    /// default profile, otherwise `None`.
    #[must_use]
    pub fn client_config_for_dc(&self, dc: &str) -> Option<Arc<ClientConfig>> {
        self.per_dc_client
            .get(dc)
            .cloned()
            .or_else(|| self.default_client.clone())
    }

    /// Default server config (the legacy / fallback profile).
    #[must_use]
    pub fn default_server_config(&self) -> Option<Arc<ServerConfig>> {
        self.default_server.clone()
    }

    /// Default client config (the legacy / fallback profile).
    #[must_use]
    pub fn default_client_config(&self) -> Option<Arc<ClientConfig>> {
        self.default_client.clone()
    }

    /// True when at least one configured profile carries a CA
    /// bundle. When set, the SNI listener requires every
    /// inbound peer to present a certificate signed by one of
    /// the configured CAs (mTLS).
    #[must_use]
    pub fn requires_client_auth(&self) -> bool {
        self.has_any_client_ca
    }

    /// Names of the DCs with explicit per-DC entries (sorted).
    #[must_use]
    pub fn dc_names(&self) -> Vec<String> {
        self.per_dc_certified.keys().cloned().collect()
    }

    /// Build a single [`tokio_rustls::TlsAcceptor`] whose
    /// `ServerConfig` picks the certificate by SNI hostname
    /// (`dc-<dc-name>.dynomite.local`) and falls back to the
    /// default profile when SNI is missing or does not match.
    ///
    /// Returns `None` when [`Self::is_empty`] is true.
    ///
    /// # Errors
    /// Returns [`TlsError::Rustls`] when rustls rejects the
    /// assembled root store / verifier (e.g. a malformed CA
    /// certificate that slipped through the loader).
    pub fn build_sni_acceptor(&self) -> Result<Option<tokio_rustls::TlsAcceptor>, TlsError> {
        if self.is_empty() {
            return Ok(None);
        }
        ensure_provider_installed();
        let resolver = DcSniResolver {
            by_dc: self.per_dc_certified.clone(),
            default: self.default_certified.clone(),
        };
        let builder = ServerConfig::builder();
        let cfg = if self.has_any_client_ca {
            // Combine every CA bundle (default + per-DC) into a
            // single root store for client verification. This
            // keeps the listener's mTLS check uniform across
            // SNI-routed certs; an inbound peer chains to any
            // of the configured CAs.
            let mut roots = RootCertStore::empty();
            self.populate_combined_ca_roots(&mut roots)?;
            let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .map_err(|e| TlsError::Rustls(format!("client verifier: {e}")))?;
            builder
                .with_client_cert_verifier(verifier)
                .with_cert_resolver(Arc::new(resolver))
        } else {
            builder
                .with_no_client_auth()
                .with_cert_resolver(Arc::new(resolver))
        };
        Ok(Some(tokio_rustls::TlsAcceptor::from(Arc::new(cfg))))
    }

    /// Populate a [`RootCertStore`] with the CAs from every
    /// per-DC entry plus the default profile. Used by
    /// [`Self::build_sni_acceptor`] when at least one profile
    /// carries a CA.
    fn populate_combined_ca_roots(&self, roots: &mut RootCertStore) -> Result<(), TlsError> {
        for cert in &self.combined_ca_certs {
            roots
                .add(cert.clone())
                .map_err(|e| TlsError::Rustls(format!("ca add: {e}")))?;
        }
        Ok(())
    }
}

/// Custom rustls cert resolver: maps SNI of the shape
/// `dc-<dc-name>.dynomite.local` to a per-DC `CertifiedKey`,
/// falling back to the default profile when the SNI is missing
/// or does not match.
#[derive(Debug)]
struct DcSniResolver {
    by_dc: BTreeMap<String, Arc<CertifiedKey>>,
    default: Option<Arc<CertifiedKey>>,
}

impl ResolvesServerCert for DcSniResolver {
    fn resolve(&self, hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        if let Some(name) = hello.server_name() {
            if let Some(dc) = dc_from_sni_label(name) {
                if let Some(ck) = self.by_dc.get(dc) {
                    return Some(ck.clone());
                }
            }
        }
        self.default.clone()
    }
}

/// SNI resolver that reads its certified-key map from a shared
/// [`SharedTlsProfiles`]. Every handshake re-borrows the inner
/// [`TlsProfileMap`] via the read lock, so a SIGHUP-driven
/// [`SharedTlsProfiles::replace`] takes effect on the next
/// inbound connection without rebuilding the [`TlsAcceptor`].
#[derive(Debug)]
struct ReloadingDcSniResolver {
    profiles: Arc<RwLock<TlsProfileMap>>,
}

impl ResolvesServerCert for ReloadingDcSniResolver {
    fn resolve(&self, hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        let profiles = self.profiles.read();
        if let Some(name) = hello.server_name() {
            if let Some(dc) = dc_from_sni_label(name) {
                if let Some(ck) = profiles.per_dc_certified.get(dc) {
                    return Some(ck.clone());
                }
            }
        }
        profiles.default_certified.clone()
    }
}

/// Reloadable wrapper around [`TlsProfileMap`].
///
/// Holds an [`Arc<parking_lot::RwLock<TlsProfileMap>>`] so the
/// inbound listener (via [`Self::build_sni_acceptor`]) and every
/// outbound peer supervisor can pick up cert / key / CA changes
/// without rebinding sockets or rebuilding their
/// [`tokio_rustls::TlsAcceptor`]. The resolver returned by
/// [`Self::build_sni_acceptor`] reads the inner map on every
/// handshake.
///
/// `Clone` is `Arc`-cheap.
///
/// # Examples
///
/// ```
/// use std::collections::BTreeMap;
/// use dynomite::net::tls::{SharedTlsProfiles, TlsProfileMap};
/// let map = TlsProfileMap::build(None, BTreeMap::new()).unwrap();
/// let shared = SharedTlsProfiles::from_map(map);
/// assert!(shared.is_empty());
/// ```
#[derive(Clone, Debug, Default)]
pub struct SharedTlsProfiles {
    inner: Arc<RwLock<TlsProfileMap>>,
}

impl SharedTlsProfiles {
    /// Wrap an existing [`TlsProfileMap`] in a shared cell.
    #[must_use]
    pub fn from_map(map: TlsProfileMap) -> Self {
        Self {
            inner: Arc::new(RwLock::new(map)),
        }
    }

    /// Atomically replace the inner profile map.
    ///
    /// Subsequent handshakes (and outbound dials that consult
    /// [`Self::client_config_for_dc`]) observe the new material;
    /// already-negotiated TLS sessions are not affected.
    pub fn replace(&self, map: TlsProfileMap) {
        *self.inner.write() = map;
    }

    /// True when the wrapped map is empty (peer plane plaintext).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }

    /// Per-DC client config, with the legacy default as fallback.
    /// Reads the inner map at call time.
    #[must_use]
    pub fn client_config_for_dc(&self, dc: &str) -> Option<Arc<ClientConfig>> {
        self.inner.read().client_config_for_dc(dc)
    }

    /// True when at least one wrapped profile pins a CA bundle.
    #[must_use]
    pub fn requires_client_auth(&self) -> bool {
        self.inner.read().requires_client_auth()
    }

    /// Names of the DCs with explicit per-DC entries (sorted).
    #[must_use]
    pub fn dc_names(&self) -> Vec<String> {
        self.inner.read().dc_names()
    }

    /// Build a SIGHUP-aware [`tokio_rustls::TlsAcceptor`].
    ///
    /// The acceptor's underlying [`ServerConfig`] holds a
    /// resolver that re-reads the wrapped
    /// [`Arc<parking_lot::RwLock<TlsProfileMap>>`] on every
    /// handshake, so [`Self::replace`] takes effect on the next
    /// inbound connection without rebinding the listener.
    ///
    /// Returns `None` when the inner map is empty (caller stays
    /// plaintext).
    ///
    /// # Errors
    /// Returns [`TlsError::Rustls`] when rustls rejects the
    /// assembled root store or the verifier (e.g. a CA cert
    /// the loader missed).
    pub fn build_sni_acceptor(&self) -> Result<Option<TlsAcceptor>, TlsError> {
        if self.is_empty() {
            return Ok(None);
        }
        ensure_provider_installed();
        let resolver = ReloadingDcSniResolver {
            profiles: self.inner.clone(),
        };
        let has_any_client_ca = self.inner.read().has_any_client_ca;
        let builder = ServerConfig::builder();
        let cfg = if has_any_client_ca {
            let mut roots = RootCertStore::empty();
            self.inner.read().populate_combined_ca_roots(&mut roots)?;
            let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .map_err(|e| TlsError::Rustls(format!("client verifier: {e}")))?;
            builder
                .with_client_cert_verifier(verifier)
                .with_cert_resolver(Arc::new(resolver))
        } else {
            builder
                .with_no_client_auth()
                .with_cert_resolver(Arc::new(resolver))
        };
        Ok(Some(TlsAcceptor::from(Arc::new(cfg))))
    }
}

fn load_certified_key(
    cert_path: &Path,
    key_path: &Path,
    provider: &rustls::crypto::CryptoProvider,
) -> Result<Arc<CertifiedKey>, TlsError> {
    let certs = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;
    let ck = CertifiedKey::from_der(certs, key, provider)
        .map_err(|e| TlsError::Rustls(format!("certified key: {e}")))?;
    Ok(Arc::new(ck))
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
        (cert.cert.pem(), cert.signing_key.serialize_pem())
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

    fn write_self_signed(dir: &TempDir, prefix: &str) -> (std::path::PathBuf, std::path::PathBuf) {
        let (cert_pem, key_pem) = issue_self_signed();
        (
            write_pem(dir, &format!("{prefix}-cert.pem"), &cert_pem),
            write_pem(dir, &format!("{prefix}-key.pem"), &key_pem),
        )
    }

    #[test]
    fn dc_sni_hostname_round_trips() {
        assert_eq!(dc_sni_hostname("dc1"), "dc-dc1.dynomite.local");
        assert_eq!(dc_from_sni_label("dc-dc1.dynomite.local"), Some("dc1"));
        assert_eq!(dc_from_sni_label("localhost"), None);
        assert_eq!(dc_from_sni_label("dc-.dynomite.local"), None);
        assert_eq!(dc_from_sni_label("dc-dc1.example.com"), None);
    }

    #[test]
    fn tls_profile_map_empty_is_empty() {
        let map = TlsProfileMap::build(None, BTreeMap::new()).unwrap();
        assert!(map.is_empty());
        assert!(map.client_config_for_dc("dc1").is_none());
        assert!(map.server_config_for_dc("dc1").is_none());
        assert!(!map.requires_client_auth());
        assert!(map.build_sni_acceptor().unwrap().is_none());
    }

    #[test]
    fn tls_profile_map_default_only_falls_back() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_self_signed(&dir, "default");
        let map = TlsProfileMap::build(
            Some(TlsProfileSpec {
                cert,
                key,
                ca: None,
            }),
            BTreeMap::new(),
        )
        .unwrap();
        assert!(!map.is_empty());
        // Any DC name resolves to the default.
        assert!(map.client_config_for_dc("dc1").is_some());
        assert!(map.server_config_for_dc("dc-without-profile").is_some());
        assert!(map.default_client_config().is_some());
        assert!(map.build_sni_acceptor().unwrap().is_some());
    }

    #[test]
    fn tls_profile_map_per_dc_overrides_default() {
        let dir = tempfile::tempdir().unwrap();
        let (def_cert, def_key) = write_self_signed(&dir, "default");
        let (dc1_cert, dc1_key) = write_self_signed(&dir, "dc1");
        let mut per_dc = BTreeMap::new();
        per_dc.insert(
            "dc1".into(),
            TlsProfileSpec {
                cert: dc1_cert,
                key: dc1_key,
                ca: None,
            },
        );
        let map = TlsProfileMap::build(
            Some(TlsProfileSpec {
                cert: def_cert,
                key: def_key,
                ca: None,
            }),
            per_dc,
        )
        .unwrap();
        // dc1 must hit its own entry.
        let dc1 = map.client_config_for_dc("dc1").unwrap();
        // Distinct DC must fall back to the default.
        let other = map.client_config_for_dc("other-dc").unwrap();
        assert!(
            !Arc::ptr_eq(&dc1, &other),
            "per-DC entry must differ from the default fallback"
        );
        assert_eq!(map.dc_names(), vec!["dc1".to_string()]);
    }

    #[test]
    fn tls_profile_map_per_dc_only_no_default() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_self_signed(&dir, "dc2");
        let mut per_dc = BTreeMap::new();
        per_dc.insert(
            "dc2".into(),
            TlsProfileSpec {
                cert,
                key,
                ca: None,
            },
        );
        let map = TlsProfileMap::build(None, per_dc).unwrap();
        assert!(map.client_config_for_dc("dc2").is_some());
        // No default: an unknown DC returns None and the
        // caller falls back to plaintext.
        assert!(map.client_config_for_dc("dc3").is_none());
        assert!(map.server_config_for_dc("dc3").is_none());
    }

    #[test]
    fn tls_profile_map_propagates_load_error() {
        let dir = tempfile::tempdir().unwrap();
        // Cert path that does not exist.
        let bogus = dir.path().join("missing.pem");
        let mut per_dc = BTreeMap::new();
        per_dc.insert(
            "dc1".into(),
            TlsProfileSpec {
                cert: bogus.clone(),
                key: bogus,
                ca: None,
            },
        );
        let err = TlsProfileMap::build(None, per_dc).expect_err("missing");
        assert!(matches!(err, TlsError::Io { .. }), "got {err:?}");
    }

    #[test]
    fn load_private_key_rejects_empty_key_file() {
        let dir = tempfile::tempdir().unwrap();
        let (cert_pem, _key_pem) = issue_self_signed();
        let cert = write_pem(&dir, "cert.pem", &cert_pem);
        // A well-formed PEM that holds a CERTIFICATE but no private
        // key parses cleanly yet yields NoMaterial for the key.
        let key = write_pem(&dir, "key.pem", &cert_pem);
        let err = load_server_config(&cert, &key, None).expect_err("no key");
        assert!(
            matches!(
                err,
                TlsError::NoMaterial {
                    kind: "private key",
                    ..
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn server_name_owned_rejects_invalid_label() {
        let err = server_name_owned("not a valid host").expect_err("invalid");
        assert!(matches!(err, TlsError::Rustls(_)), "got {err:?}");
    }

    #[test]
    fn load_client_config_with_pinned_ca() {
        // A self-signed cert doubles as its own CA bundle; the
        // client config loads it as the sole trust anchor.
        let dir = tempfile::tempdir().unwrap();
        let (cert_pem, _key_pem) = issue_self_signed();
        let ca = write_pem(&dir, "ca.pem", &cert_pem);
        let cfg = load_client_config(Some(&ca)).unwrap();
        assert!(Arc::strong_count(&cfg) >= 1);
    }

    #[test]
    fn load_server_config_with_client_ca_enables_mtls() {
        // Supplying a client CA exercises the mTLS branch of
        // load_server_config (WebPkiClientVerifier).
        let dir = tempfile::tempdir().unwrap();
        let (cert_pem, key_pem) = issue_self_signed();
        let cert = write_pem(&dir, "cert.pem", &cert_pem);
        let key = write_pem(&dir, "key.pem", &key_pem);
        let ca = write_pem(&dir, "ca.pem", &cert_pem);
        let cfg = load_server_config(&cert, &key, Some(&ca)).unwrap();
        assert!(Arc::strong_count(&cfg) >= 1);
    }

    #[test]
    fn acceptor_and_connector_adapters_build() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_self_signed(&dir, "adapt");
        let server = load_server_config(&cert, &key, None).unwrap();
        let client = load_client_config(None).unwrap();
        let _acceptor = acceptor_from(server);
        let _connector = connector_from(client);
    }

    #[test]
    fn tls_profile_map_debug_lists_state() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_self_signed(&dir, "dbg");
        let map = TlsProfileMap::build(
            Some(TlsProfileSpec {
                cert,
                key,
                ca: None,
            }),
            BTreeMap::new(),
        )
        .unwrap();
        let s = format!("{map:?}");
        assert!(s.contains("TlsProfileMap"), "debug: {s}");
        assert!(s.contains("has_default"), "debug: {s}");
        assert!(map.default_server_config().is_some());
    }

    #[test]
    fn mtls_profile_map_requires_client_auth_and_builds_acceptor() {
        // A profile carrying a CA flips requires_client_auth and
        // drives the mTLS branch of build_sni_acceptor plus
        // populate_combined_ca_roots, for both the default and
        // per-DC slots.
        let dir = tempfile::tempdir().unwrap();
        let (def_cert_pem, def_key_pem) = issue_self_signed();
        let (dc_cert_pem, dc_key_pem) = issue_self_signed();
        let def_cert = write_pem(&dir, "def-cert.pem", &def_cert_pem);
        let def_key = write_pem(&dir, "def-key.pem", &def_key_pem);
        let def_ca = write_pem(&dir, "def-ca.pem", &def_cert_pem);
        let dc_cert = write_pem(&dir, "dc-cert.pem", &dc_cert_pem);
        let dc_key = write_pem(&dir, "dc-key.pem", &dc_key_pem);
        let dc_ca = write_pem(&dir, "dc-ca.pem", &dc_cert_pem);
        let mut per_dc = BTreeMap::new();
        per_dc.insert(
            "dc1".into(),
            TlsProfileSpec {
                cert: dc_cert,
                key: dc_key,
                ca: Some(dc_ca),
            },
        );
        let map = TlsProfileMap::build(
            Some(TlsProfileSpec {
                cert: def_cert,
                key: def_key,
                ca: Some(def_ca),
            }),
            per_dc,
        )
        .unwrap();
        assert!(map.requires_client_auth());
        let acceptor = map.build_sni_acceptor().unwrap();
        assert!(acceptor.is_some(), "mTLS map must yield an acceptor");
    }

    #[test]
    fn shared_tls_profiles_surface_and_reload() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_self_signed(&dir, "shared");
        let map = TlsProfileMap::build(
            Some(TlsProfileSpec {
                cert,
                key,
                ca: None,
            }),
            BTreeMap::new(),
        )
        .unwrap();
        let shared = SharedTlsProfiles::from_map(map);
        assert!(!shared.is_empty());
        assert!(shared.client_config_for_dc("anything").is_some());
        assert!(!shared.requires_client_auth());
        assert!(shared.dc_names().is_empty());
        // Non-empty, no-CA map yields a non-mTLS SNI acceptor.
        assert!(shared.build_sni_acceptor().unwrap().is_some());

        // Replacing with an empty map flips is_empty and drops the
        // acceptor.
        shared.replace(TlsProfileMap::build(None, BTreeMap::new()).unwrap());
        assert!(shared.is_empty());
        assert!(shared.build_sni_acceptor().unwrap().is_none());
        assert!(shared.client_config_for_dc("dc1").is_none());
    }

    #[test]
    fn shared_tls_profiles_mtls_acceptor() {
        // A CA-bearing profile drives the mTLS branch of
        // SharedTlsProfiles::build_sni_acceptor and
        // requires_client_auth, plus the per-DC dc_names path.
        let dir = tempfile::tempdir().unwrap();
        let (cert_pem, key_pem) = issue_self_signed();
        let cert = write_pem(&dir, "m-cert.pem", &cert_pem);
        let key = write_pem(&dir, "m-key.pem", &key_pem);
        let ca = write_pem(&dir, "m-ca.pem", &cert_pem);
        let mut per_dc = BTreeMap::new();
        per_dc.insert(
            "dcm".into(),
            TlsProfileSpec {
                cert,
                key,
                ca: Some(ca),
            },
        );
        let map = TlsProfileMap::build(None, per_dc).unwrap();
        let shared = SharedTlsProfiles::from_map(map);
        assert!(shared.requires_client_auth());
        assert_eq!(shared.dc_names(), vec!["dcm".to_string()]);
        assert!(shared.build_sni_acceptor().unwrap().is_some());
    }
}
