//! DNODE_PEER_PROXY listener.
//!
//! Listens for inbound peer connections from other Dynomite nodes
//! and spawns a [`crate::net::dnode_client::dnode_client_loop`] task
//! per accepted socket. When configured with a
//! [`tokio_rustls::TlsAcceptor`] (via [`DnodeProxy::with_tls`])
//! every accepted socket is upgraded to TLS before handoff.
//!
//! # Examples
//!
//! ```no_run
//! use dynomite::net::DnodeProxy;
//!
//! let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
//! let listener = DnodeProxy::bind(addr).unwrap();
//! let _ = listener.local_addr();
//! ```

use std::io;
use std::net::SocketAddr;

use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;
use tracing::Instrument as _;

use crate::io::reactor::{ConnRole, TcpTransport, Transport};
use crate::net::client::ClientHandler;
use crate::net::conn::Conn;
use crate::net::dnode_client::dnode_client_loop;
use crate::net::listener::{bind_dual_stack, BindOptions};
use crate::net::tls::TlsServerTransport;
use crate::net::NetError;

/// DNODE_PEER_PROXY listener.
pub struct DnodeProxy {
    listener: TcpListener,
    tls_acceptor: Option<TlsAcceptor>,
}

impl DnodeProxy {
    /// Bind a peer-listener to the given address.
    ///
    /// # Errors
    /// Forwarded from the underlying socket calls.
    pub fn bind<A: Into<SocketAddr>>(addr: A) -> Result<Self, NetError> {
        let listener = bind_dual_stack(addr.into(), BindOptions::default())?;
        Ok(Self {
            listener,
            tls_acceptor: None,
        })
    }

    /// Attach a TLS acceptor; every accepted peer is wrapped via
    /// [`TlsAcceptor::accept`] before being handed off to the
    /// per-peer driver. When the acceptor is unset (the default)
    /// the listener serves plaintext TCP, matching the historical
    /// behaviour.
    #[must_use]
    pub fn with_tls(mut self, acceptor: TlsAcceptor) -> Self {
        self.tls_acceptor = Some(acceptor);
        self
    }

    /// True when the listener is configured with a TLS acceptor.
    #[must_use]
    pub fn has_tls(&self) -> bool {
        self.tls_acceptor.is_some()
    }

    /// Local address of the listener.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Drive the accept loop. The supplied `handler_factory` is
    /// called once per accepted peer; it receives the
    /// per-connection responder sender (the matching half of the
    /// channel the inbound driver reads from) and returns the
    /// [`ClientHandler`] the per-peer loop should use.
    ///
    /// # Errors
    /// Forwarded from the listener accept call.
    #[tracing::instrument(
        name = "dnode_proxy.run",
        skip_all,
        fields(
            local = self.listener.local_addr().map_or_else(|_| String::from("?"), |a| a.to_string()),
        ),
    )]
    pub async fn run<F>(
        self,
        cancel: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
        mut handler_factory: F,
    ) -> Result<(), NetError>
    where
        F: FnMut(
                tokio::sync::mpsc::Sender<crate::net::dispatcher::OutboundEnvelope>,
            ) -> ClientHandler
            + Send,
    {
        let mut cancel = cancel;
        let mut peers: Vec<JoinHandle<Result<(), NetError>>> = Vec::new();
        let tls_acceptor = self.tls_acceptor.clone();
        loop {
            tokio::select! {
                () = &mut cancel => break,
                res = self.listener.accept() => {
                    let (sock, peer) = res?;
                    let role = ConnRole::DnodePeerClient;
                    let transport: Box<dyn Transport> = if let Some(acc) = tls_acceptor.as_ref() {
                        match acc.accept(sock).await {
                            Ok(tls) => Box::new(TlsServerTransport::new(tls, role)),
                            Err(e) => {
                                tracing::warn!(?peer, error = %e, "dnode_proxy tls handshake failed; dropping");
                                continue;
                            }
                        }
                    } else {
                        Box::new(TcpTransport::new(sock, role))
                    };
                    let conn = Conn::new(transport, role);
                    let (tx, rx) = tokio::sync::mpsc::channel(64);
                    let handler = handler_factory(tx);
                    tracing::debug!(?peer, "dnode_proxy accepted peer");
                    let accept_span = tracing::info_span!(
                        "dnode_client.accept",
                        peer = %peer,
                    );
                    let h = tokio::spawn(
                        async move {
                            dnode_client_loop(conn, handler, rx).await
                        }
                        .instrument(accept_span),
                    );
                    peers.push(h);
                }
            }
            peers.retain(|h| !h.is_finished());
        }
        for h in peers {
            let _ = h.await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bind_returns_local_addr() {
        let l = DnodeProxy::bind("127.0.0.1:0".parse::<SocketAddr>().unwrap()).unwrap();
        assert!(l.local_addr().unwrap().ip().is_loopback());
        assert!(!l.has_tls());
    }

    #[tokio::test]
    async fn with_tls_attaches_acceptor() {
        let l = DnodeProxy::bind("127.0.0.1:0".parse::<SocketAddr>().unwrap()).unwrap();
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("c.pem"), cert.cert.pem()).unwrap();
        std::fs::write(dir.path().join("k.pem"), cert.key_pair.serialize_pem()).unwrap();
        let cfg = crate::net::tls::load_server_config(
            &dir.path().join("c.pem"),
            &dir.path().join("k.pem"),
            None,
        )
        .unwrap();
        let l = l.with_tls(crate::net::tls::acceptor_from(cfg));
        assert!(l.has_tls());
    }
}
