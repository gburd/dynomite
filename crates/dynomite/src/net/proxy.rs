//! PROXY listener.
//!
//! Listens for client connections on the configured `listen:` port
//! and spawns a CLIENT FSM per accepted socket. [`Proxy`] owns a
//! [`tokio::net::TcpListener`] and a per-listener [`Dispatcher`]
//! reference; calling [`Proxy::run`] enters an accept-loop that
//! drives a fresh `tokio::spawn` for every incoming socket.
//!
//! # Examples
//!
//! ```no_run
//! use dynomite::net::{NoopDispatcher, Proxy};
//! use std::sync::Arc;
//!
//! let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
//! let proxy = Proxy::bind(addr, Arc::new(NoopDispatcher)).unwrap();
//! let _handle = proxy.local_addr();
//! ```

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::conf::DataStore;
use crate::io::reactor::{ConnRole, TcpTransport};
use crate::net::client::{client_loop, ClientHandler};
use crate::net::conn::Conn;
use crate::net::dispatcher::Dispatcher;
use crate::net::listener::{bind_dual_stack, BindOptions};
use crate::net::NetError;

/// PROXY listener.
pub struct Proxy {
    listener: TcpListener,
    dispatcher: Arc<dyn Dispatcher>,
    data_store: DataStore,
    response_capacity: usize,
}

impl Proxy {
    /// Bind a proxy listener to the given address.
    ///
    /// Uses [`crate::net::listener::bind_dual_stack`] to honor v4 +
    /// v6 wildcard semantics. The dispatcher is invoked for every
    /// fully-parsed request from any accepted client.
    ///
    /// # Errors
    /// Forwarded from the underlying socket calls.
    pub fn bind<A: Into<SocketAddr>>(
        addr: A,
        dispatcher: Arc<dyn Dispatcher>,
    ) -> Result<Self, NetError> {
        let listener = bind_dual_stack(addr.into(), BindOptions::default())?;
        Ok(Self {
            listener,
            dispatcher,
            data_store: DataStore::Redis,
            response_capacity: 64,
        })
    }

    /// Override the datastore the per-client FSMs will parse.
    /// Defaults to [`DataStore::Redis`].
    #[must_use]
    pub fn with_data_store(mut self, ds: DataStore) -> Self {
        self.data_store = ds;
        self
    }

    /// Override the response-channel capacity per client.
    #[must_use]
    pub fn with_response_capacity(mut self, n: usize) -> Self {
        self.response_capacity = n.max(1);
        self
    }

    /// Local address of the listener.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Borrow the bound listener so callers can extract the
    /// fd-level socket handle when needed.
    pub fn listener(&self) -> &TcpListener {
        &self.listener
    }

    /// Drive the accept loop until the listener returns an error
    /// or the supplied cancel future resolves.
    ///
    /// Each accepted socket is wrapped in a [`Conn`] tagged
    /// [`ConnRole::Client`] and handed to a per-task client loop.
    ///
    /// # Errors
    /// Forwarded from the listener accept call.
    pub async fn run(
        self,
        cancel: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
    ) -> Result<(), NetError> {
        let mut cancel = cancel;
        let mut clients: Vec<JoinHandle<Result<(), NetError>>> = Vec::new();
        loop {
            let accept = self.listener.accept();
            tokio::select! {
                () = &mut cancel => break,
                res = accept => {
                    let (sock, peer) = res?;
                    // Match the latency expectation of the
                    // datastore engines: Redis and memcache both
                    // assume the upstream proxy disables Nagle so
                    // small Redis requests fly without batching.
                    // Errors here are non-fatal: a peer that
                    // disconnected before the option could be
                    // applied is fine.
                    let _ = sock.set_nodelay(true);
                    let role = ConnRole::Client;
                    let transport = Box::new(TcpTransport::new(sock, role));
                    let conn = Conn::new(transport, role);
                    let dispatcher = Arc::clone(&self.dispatcher);
                    let cap = self.response_capacity;
                    let ds = self.data_store;
                    tracing::debug!(?peer, "proxy accepted client");
                    let handle = tokio::spawn(async move {
                        let (tx, rx) = mpsc::channel(cap);
                        let handler = ClientHandler::new(dispatcher, tx, ds);
                        client_loop(conn, handler, rx).await
                    });
                    clients.push(handle);
                }
            }
            // Drain finished tasks opportunistically.
            clients.retain(|h| !h.is_finished());
        }
        for h in clients {
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
        let proxy = Proxy::bind(
            "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
            Arc::new(crate::net::NoopDispatcher),
        )
        .unwrap();
        assert!(proxy.local_addr().unwrap().ip().is_loopback());
    }

    #[tokio::test]
    async fn run_exits_on_cancel() {
        let proxy = Proxy::bind(
            "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
            Arc::new(crate::net::NoopDispatcher),
        )
        .unwrap();
        proxy.run(Box::pin(async {})).await.unwrap();
    }
}
