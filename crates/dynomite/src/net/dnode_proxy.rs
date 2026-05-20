//! DNODE_PEER_PROXY listener.
//!
//! Listens for inbound peer connections from other Dynomite nodes
//! and spawns a [`crate::net::dnode_client::dnode_client_loop`] task
//! per accepted socket.
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

use crate::io::reactor::{ConnRole, TcpTransport};
use crate::net::client::ClientHandler;
use crate::net::conn::Conn;
use crate::net::dnode_client::dnode_client_loop;
use crate::net::listener::{bind_dual_stack, BindOptions};
use crate::net::NetError;

/// DNODE_PEER_PROXY listener.
pub struct DnodeProxy {
    listener: TcpListener,
}

impl DnodeProxy {
    /// Bind a peer-listener to the given address.
    ///
    /// # Errors
    /// Forwarded from the underlying socket calls.
    pub fn bind<A: Into<SocketAddr>>(addr: A) -> Result<Self, NetError> {
        let listener = bind_dual_stack(addr.into(), BindOptions::default())?;
        Ok(Self { listener })
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
        loop {
            tokio::select! {
                () = &mut cancel => break,
                res = self.listener.accept() => {
                    let (sock, peer) = res?;
                    let role = ConnRole::DnodePeerClient;
                    let transport = Box::new(TcpTransport::new(sock, role));
                    let conn = Conn::new(transport, role);
                    let (tx, rx) = tokio::sync::mpsc::channel(64);
                    let handler = handler_factory(tx);
                    tracing::debug!(?peer, "dnode_proxy accepted peer");
                    let h = tokio::spawn(async move {
                        dnode_client_loop(conn, handler, rx).await
                    });
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
    }
}
