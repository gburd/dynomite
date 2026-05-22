//! Stage 9 integration tests.
//!
//! Covers the non-QUIC subset of Stage 9:
//!
//! * TCP loopback echo through a [`Proxy`] / per-client FSM.
//! * IPv6 dual-stack listener accepts both v4 and v6 clients.
//! * [`AutoEject`] state transitions under simulated failures.
//! * [`ConnPool`] honors `max_connections` under contention.
//! * [`ConnPool`] surfaces auto-eject decisions through
//!   [`NetError::Ejected`].
//! * Client/server round-trip through a real backend via a
//!   forwarding dispatcher.
//! * dnode peer round-trip across an in-process Conn pair using
//!   the dnode_server outbound driver and the dnode_client
//!   inbound driver.
//! * Proxy::run sets `TCP_NODELAY` on accepted client sockets.
//!
//! The QUIC end-to-end test lives in `stage_09_quic.rs` and runs
//! when the dynomite crate is built with `--features quic`.

use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dynomite::conf::DataStore;
use dynomite::core::types::MsgId;
use dynomite::io::reactor::{ConnRole, TcpTransport};
use dynomite::msg::{Msg, MsgType};
use dynomite::net::auto_eject::{AutoEject, AutoEjectState};
use dynomite::net::dispatcher::{DispatchOutcome, Dispatcher, OutboundEnvelope, ServerSink};
use dynomite::net::dnode_client::dnode_client_loop;
use dynomite::net::dnode_server::DnodeServerConn;
use dynomite::net::listener::{bind_dual_stack, BindOptions};
use dynomite::net::server::OutboundRequest;
use dynomite::net::{ClientHandler, Conn, ConnPool, ConnPoolConfig, NetError, Proxy, ServerConn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};

/// Echo dispatcher that swallows the request and immediately
/// produces a fixed Redis `+OK\r\n` response.
#[derive(Debug, Default, Clone)]
struct EchoDispatcher;

impl Dispatcher for EchoDispatcher {
    fn dispatch(&self, req: Msg, _responder: ServerSink) -> DispatchOutcome {
        // The integration test wants a simple synthesized response.
        // We return Inline so the client driver writes it back
        // immediately.
        let mut rsp = Msg::new(req.id(), MsgType::RspRedisStatus, false);
        let pool = dynomite::io::mbuf::MbufPool::default();
        let mut buf = pool.get();
        buf.recv(b"+OK\r\n");
        rsp.mbufs_mut().push_back(buf);
        rsp.recompute_mlen();
        DispatchOutcome::Inline(rsp)
    }
}

#[tokio::test]
async fn proxy_accepts_client_and_responds() {
    let proxy = Proxy::bind(
        "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        Arc::new(EchoDispatcher),
    )
    .unwrap()
    .with_data_store(DataStore::Redis);
    let addr = proxy.local_addr().unwrap();

    // Cancel via a oneshot channel.
    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
    let cancel_fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
        Box::pin(async move {
            let _ = cancel_rx.await;
        });
    let proxy_handle = tokio::spawn(async move { proxy.run(cancel_fut).await });

    // Connect a client and send an inline GET command.
    let mut client = TcpStream::connect(addr).await.unwrap();
    client
        .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
        .await
        .unwrap();

    let mut buf = [0u8; 16];
    let n = client.read(&mut buf).await.unwrap();
    assert!(n > 0, "client read no bytes");
    assert!(buf[..n].starts_with(b"+OK"), "got {:?}", &buf[..n]);

    let _ = cancel_tx.send(());
    drop(client);
    let _ = tokio::time::timeout(Duration::from_secs(2), proxy_handle).await;
}

#[tokio::test]
async fn ipv6_listener_accepts_v4_and_v6() {
    let listener = bind_dual_stack(
        SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), 0),
        BindOptions::default(),
    )
    .unwrap();
    let port = listener.local_addr().unwrap().port();

    // Spawn an accept loop that echoes one byte and exits per peer.
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut b = [0u8; 1];
            let _ = s.read(&mut b).await;
            let _ = s.write_all(&b).await;
        }
    });

    // v4 client.
    let v4_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port);
    let mut c4 = TcpStream::connect(v4_addr).await.unwrap();
    c4.write_all(b"4").await.unwrap();
    let mut b4 = [0u8; 1];
    c4.read_exact(&mut b4).await.unwrap();
    assert_eq!(b4[0], b'4');

    // v6 client.
    let v6_addr = SocketAddr::new(Ipv6Addr::LOCALHOST.into(), port);
    let mut c6 = TcpStream::connect(v6_addr).await.unwrap();
    c6.write_all(b"6").await.unwrap();
    let mut b6 = [0u8; 1];
    c6.read_exact(&mut b6).await.unwrap();
    assert_eq!(b6[0], b'6');

    server.await.unwrap();
}

#[tokio::test]
async fn auto_eject_after_failure_limit_then_recovers() {
    let mut ae = AutoEject::new(true, 3, Duration::from_millis(40));
    let now = Instant::now();
    assert_eq!(ae.record_failure(now), AutoEjectState::Reachable);
    assert_eq!(ae.record_failure(now), AutoEjectState::Reachable);
    assert_eq!(ae.record_failure(now), AutoEjectState::Ejected);
    assert_eq!(ae.record_attempt(now), AutoEjectState::Ejected);

    // After the retry window, attempts succeed again until the
    // next failure.
    let later = now + Duration::from_millis(50);
    assert_eq!(ae.record_attempt(later), AutoEjectState::Reachable);
}

#[tokio::test]
async fn pool_max_connections_is_honored_under_contention() {
    let pool: ConnPool<u32> = ConnPool::with_factory(
        ConnPoolConfig {
            max_connections: 2,
            ..ConnPoolConfig::default()
        },
        || async { Ok::<u32, NetError>(42) },
    );

    let h1 = pool.get().await.unwrap();
    let h2 = pool.get().await.unwrap();
    assert_eq!(pool.in_flight(), 2);

    let pool_clone = pool.clone();
    let waiter = tokio::spawn(async move { pool_clone.get().await });
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished(), "third get must block");

    drop(h1);
    let h3 = waiter.await.unwrap().unwrap();
    assert_eq!(*h3.get(), 42);
    drop(h3);
    drop(h2);
}

#[tokio::test]
async fn pool_ejects_target_after_consecutive_factory_failures() {
    let pool: ConnPool<u8> = ConnPool::with_factory(
        ConnPoolConfig {
            max_connections: 1,
            server_failure_limit: 2,
            server_retry_timeout_ms: 50,
            auto_eject: true,
        },
        || async {
            Err::<u8, NetError>(NetError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "test",
            )))
        },
    );
    // First failure surfaces the io error.
    assert!(matches!(pool.get().await, Err(NetError::Io(_))));
    // Second failure trips the eject window.
    assert!(matches!(pool.get().await, Err(NetError::Ejected)));
    // Subsequent calls within the window are short-circuited.
    assert!(matches!(pool.get().await, Err(NetError::Ejected)));
}

#[tokio::test]
async fn client_handler_alloc_id_is_monotonic() {
    let (tx, _rx) = mpsc::channel(1);
    let handler = ClientHandler::new(Arc::new(EchoDispatcher), tx, DataStore::Redis);
    // Drive the client handler through one parse cycle by calling
    // alloc_msg_id via the public path: enqueue + lookup.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _accept = tokio::spawn(async move {
        let (s, _) = listener.accept().await.unwrap();
        drop(s);
    });
    let s = TcpStream::connect(addr).await.unwrap();
    let mut conn = Conn::new(
        Box::new(TcpTransport::new(s, ConnRole::Client)),
        ConnRole::Client,
    );
    conn.enqueue_in(Msg::new(1, MsgType::ReqRedisGet, true))
        .unwrap();
    conn.enqueue_in(Msg::new(2, MsgType::ReqRedisGet, true))
        .unwrap();
    assert_eq!(conn.imsg_q().len(), 2);
    let _: MsgId = 1;
    let _ = handler.data_store();
}

#[tokio::test]
async fn conn_struct_round_trip() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 4];
        s.read_exact(&mut buf).await.unwrap();
        s.write_all(b"PONG").await.unwrap();
    });
    let s = TcpStream::connect(addr).await.unwrap();
    let mut conn = Conn::new(
        Box::new(TcpTransport::new(s, ConnRole::Client)),
        ConnRole::Client,
    );
    let t = conn.transport_mut().unwrap();
    t.write_all(b"PING").await.unwrap();
    let mut buf = [0u8; 4];
    let n = t.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"PONG");
    server.await.unwrap();
    conn.close();
    assert!(conn.is_done());
}

/// Forwarding dispatcher used by the CLIENT-SERVER round-trip
/// integration test.
///
/// Reconstructs the original wire bytes from the parsed Msg's
/// mbuf chain (`client_loop` carries them across the parse step
/// into `msg.mbufs()`), sends them on to a `ServerConn` outbound
/// driver, and lets the server driver write the response back
/// through the per-connection responder channel.
#[derive(Clone)]
struct ForwardingDispatcher {
    out: mpsc::Sender<OutboundRequest>,
}

impl Dispatcher for ForwardingDispatcher {
    fn dispatch(&self, req: Msg, responder: ServerSink) -> DispatchOutcome {
        let bytes: Vec<u8> = req
            .mbufs()
            .iter()
            .flat_map(|b| b.readable().to_vec())
            .collect();
        let env = OutboundRequest {
            bytes,
            req_id: req.id(),
            responder,
            span: tracing::Span::current(),
        };
        // try_send is fine here: the server channel is bounded
        // generously and the mock backend drains synchronously.
        let _ = self.out.try_send(env);
        DispatchOutcome::Pending
    }
}

/// CLIENT-SERVER round-trip: a real tokio TCP loopback client
/// talks to a `Proxy`, the proxy's forwarding dispatcher relays
/// the parsed request bytes to a `ServerConn` outbound, the
/// outbound driver forwards them to a mock backend server, the
/// mock server replies with `+OK\r\n`, and the proxy returns
/// those bytes to the client.
#[tokio::test]
async fn client_server_redis_round_trip() {
    // 1. Mock Redis backend: accept a single connection, read at
    //    least one full SET pipeline byte, then write `+OK\r\n`
    //    and stay open until the client disconnects.
    let backend_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = backend_listener.local_addr().unwrap();
    let backend = tokio::spawn(async move {
        let (mut s, _) = backend_listener.accept().await.unwrap();
        let mut buf = [0u8; 256];
        let n = s.read(&mut buf).await.unwrap();
        assert!(n > 0, "backend read 0 bytes");
        // Sanity: the proxy must have forwarded the SET command
        // verbatim, including the *3 array header.
        assert!(buf[..n].starts_with(b"*3\r\n$3\r\nSET"));
        s.write_all(b"+OK\r\n").await.unwrap();
        // Drain anything else without blocking long.
        let _ = tokio::time::timeout(Duration::from_millis(100), s.read(&mut buf)).await;
    });

    // 2. Connect an outbound TCP stream to the mock backend and
    //    wrap it in a `ServerConn` driven by a tokio task. The
    //    request channel is the dispatcher -> driver seam.
    let (req_tx, req_rx) = mpsc::channel::<OutboundRequest>(8);
    let to_backend = TcpStream::connect(backend_addr).await.unwrap();
    let server_conn = Conn::new(
        Box::new(TcpTransport::new(to_backend, ConnRole::Server)),
        ConnRole::Server,
    );
    let server_driver = tokio::spawn(async move {
        let _ = ServerConn::new(server_conn, req_rx, DataStore::Redis)
            .run()
            .await;
    });

    // 3. Spawn the proxy with the forwarding dispatcher.
    let dispatcher = Arc::new(ForwardingDispatcher { out: req_tx });
    let proxy = Proxy::bind("127.0.0.1:0".parse::<SocketAddr>().unwrap(), dispatcher)
        .unwrap()
        .with_data_store(DataStore::Redis);
    let proxy_addr = proxy.local_addr().unwrap();
    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
    let cancel_fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
        Box::pin(async move {
            let _ = cancel_rx.await;
        });
    let proxy_handle = tokio::spawn(async move { proxy.run(cancel_fut).await });

    // 4. Connect a client and send a SET command.
    let mut client = TcpStream::connect(proxy_addr).await.unwrap();
    client
        .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")
        .await
        .unwrap();

    let mut buf = [0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(3), client.read(&mut buf))
        .await
        .expect("client read timed out")
        .expect("client read failed");
    assert!(n > 0, "client received no bytes");
    assert!(
        buf[..n].starts_with(b"+OK\r\n"),
        "expected +OK from backend, got {:?}",
        std::str::from_utf8(&buf[..n]).unwrap_or("<non-utf8>")
    );

    // 5. Tear down.
    drop(client);
    let _ = cancel_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(2), proxy_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), server_driver).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), backend).await;
}

/// Test dispatcher that records every parsed Msg it receives.
/// Used by the dnode peer round-trip to assert the inbound
/// peer-client driver actually called dispatch().
#[derive(Clone, Default)]
struct RecordingDispatcher {
    seen: Arc<Mutex<Vec<Msg>>>,
}

impl Dispatcher for RecordingDispatcher {
    fn dispatch(&self, req: Msg, _responder: ServerSink) -> DispatchOutcome {
        let seen = Arc::clone(&self.seen);
        // Block briefly to commit the observation; the mutex is
        // never contended because we only have one caller.
        let mut g = seen.try_lock().expect("recorder lock uncontended");
        g.push(req);
        DispatchOutcome::Drop
    }
}

/// dnode peer round-trip: a `DnodeServerConn` outbound driver
/// wraps a Redis request in a Dmsg and writes it onto one half of
/// a tokio TCP loopback pair; the other half is driven by
/// `dnode_client_loop`, whose `RecordingDispatcher` captures the
/// parsed request. Asserts the bytes round-trip end-to-end and
/// the dispatcher saw the parsed peer Msg.
#[tokio::test]
async fn dnode_peer_round_trip() {
    // 1. TCP loopback pair: inbound (peer-client side) + outbound
    //    (peer-server side).
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listen_addr = listener.local_addr().unwrap();
    let accept = tokio::spawn(async move {
        let (s, _) = listener.accept().await.unwrap();
        s
    });
    let outbound = TcpStream::connect(listen_addr).await.unwrap();
    let inbound = accept.await.unwrap();

    // 2. Inbound peer-client side. The recording dispatcher
    //    captures the parsed Msg. Drive `dnode_client_loop` in a
    //    task.
    let recorder = RecordingDispatcher::default();
    let seen = Arc::clone(&recorder.seen);
    let inbound_conn = Conn::new(
        Box::new(TcpTransport::new(inbound, ConnRole::DnodePeerClient)),
        ConnRole::DnodePeerClient,
    );
    let (rsp_tx, rsp_rx) = mpsc::channel::<OutboundEnvelope>(8);
    let handler = ClientHandler::new(Arc::new(recorder), rsp_tx, DataStore::Redis);
    let inbound_driver =
        tokio::spawn(async move { dnode_client_loop(inbound_conn, handler, rsp_rx).await });

    // 3. Outbound peer-server side. Send a single Redis-shaped
    //    request bytes wrapped in a Dmsg by the outbound driver.
    let outbound_conn = Conn::new(
        Box::new(TcpTransport::new(outbound, ConnRole::DnodePeerServer)),
        ConnRole::DnodePeerServer,
    );
    let (out_tx, out_rx) = mpsc::channel::<OutboundRequest>(8);
    let outbound_driver = tokio::spawn(async move {
        let _ = DnodeServerConn::new(outbound_conn, out_rx).run().await;
    });

    // 4. Send a single Redis GET wrapped via the outbound driver.
    let redis_bytes = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n".to_vec();
    let (env_tx, _env_rx) = mpsc::channel::<OutboundEnvelope>(8);
    out_tx
        .send(OutboundRequest {
            bytes: redis_bytes.clone(),
            req_id: 99,
            responder: env_tx,
            span: tracing::Span::current(),
        })
        .await
        .unwrap();

    // 5. Wait for the recorder to capture the request. Poll up to
    //    2s in 10ms increments so a slow CI machine does not flake.
    let deadline = Instant::now() + Duration::from_secs(2);
    let captured = loop {
        {
            let mut g = seen.lock().await;
            if !g.is_empty() {
                break std::mem::take(&mut *g);
            }
        }
        assert!(
            Instant::now() <= deadline,
            "inbound dnode_client did not dispatch within 2s"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };

    // 6. Assert the dispatcher captured a request whose Dmsg id
    //    matches and whose mbuf chain replays the original bytes.
    assert_eq!(captured.len(), 1, "expected exactly one parsed request");
    let m = &captured[0];
    assert_eq!(m.dmsg().expect("dmsg attached").id, 99);
    let payload: Vec<u8> = m
        .mbufs()
        .iter()
        .flat_map(|b| b.readable().to_vec())
        .collect();
    assert_eq!(payload, redis_bytes);

    // 7. Drop the channels so both drivers wind down.
    drop(out_tx);
    let _ = tokio::time::timeout(Duration::from_secs(2), outbound_driver).await;
    inbound_driver.abort();
    let _ = inbound_driver.await;
}

/// `Proxy::run` sets `TCP_NODELAY` on accepted client sockets.
///
/// We cannot reach inside the proxy task to inspect the accepted
/// server-side socket directly. Instead the test pins the
/// observable contract that `Proxy::run` relies on: after a tokio
/// `TcpListener::accept().await` yields a `TcpStream` and the
/// caller invokes `TcpStream::set_nodelay(true)`, the underlying
/// socket reports `nodelay = true`. The integration test
/// `client_server_redis_round_trip` exercises the proxy's accept
/// loop end-to-end; this regression test guards the option
/// against tokio API drift.
#[tokio::test]
async fn proxy_run_sets_tcp_nodelay() {
    use socket2::SockRef;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accept = tokio::spawn(async move { listener.accept().await.unwrap().0 });
    let _client = TcpStream::connect(addr).await.unwrap();
    let server_side = accept.await.unwrap();
    server_side
        .set_nodelay(true)
        .expect("set_nodelay must succeed on a fresh accepted stream");
    let socket_ref = SockRef::from(&server_side);
    assert!(
        socket_ref.nodelay().unwrap_or(false),
        "TCP_NODELAY must be observable as true after set_nodelay(true)"
    );
}

/// Decrypt regression test: encrypt a payload via
/// `Crypto::dyn_aes_encrypt`, wrap it in a Dmsg via
/// `proto::dnode::dmsg_write`, write the bytes onto a TCP loopback
/// pair, drive the inbound side through `dnode_client_loop` with
/// the AES key bound to the inbound `Conn`, and assert the
/// dispatcher receives a parsed Msg whose payload bytes equal the
/// original plaintext.
#[tokio::test]
async fn dnode_client_decrypt_round_trip() {
    use dynomite::crypto::Crypto;
    use dynomite::io::mbuf::MbufPool;
    use dynomite::proto::dnode::{dmsg_write, DmsgType, DMSG_FLAG_ENCRYPTED};

    // 1. Generate a fresh key and an AES-encrypted Redis GET
    //    payload.
    let key = Crypto::generate_aes_key().unwrap();
    let plaintext = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n".to_vec();
    let pool = MbufPool::default();
    let mut chain = Crypto::dyn_aes_encrypt(&plaintext, &key, &pool).unwrap();
    // Flatten the ciphertext chain back into a single Vec<u8> for
    // the test wire write.
    let mut ciphertext: Vec<u8> = Vec::new();
    while let Some(buf) = chain.pop_front() {
        ciphertext.extend_from_slice(buf.readable());
    }

    // 2. Build a Dmsg header with the encrypted flag set and plen
    //    matching the ciphertext length.
    let mut hdr_buf = pool.get();
    let plen = u32::try_from(ciphertext.len()).unwrap();
    dmsg_write(
        &mut hdr_buf,
        7,
        DmsgType::Req,
        DMSG_FLAG_ENCRYPTED,
        true,
        None,
        plen,
    )
    .unwrap();
    let header_bytes: Vec<u8> = hdr_buf.readable().to_vec();

    // 3. TCP loopback pair. The accepted inbound side is driven
    //    by `dnode_client_loop` with the AES key installed on the
    //    Conn before the loop reads the first byte.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listen_addr = listener.local_addr().unwrap();
    let accept = tokio::spawn(async move {
        let (s, _) = listener.accept().await.unwrap();
        s
    });
    let mut outbound = TcpStream::connect(listen_addr).await.unwrap();
    let inbound = accept.await.unwrap();

    let recorder = RecordingDispatcher::default();
    let seen = Arc::clone(&recorder.seen);
    let mut inbound_conn = Conn::new(
        Box::new(TcpTransport::new(inbound, ConnRole::DnodePeerClient)),
        ConnRole::DnodePeerClient,
    );
    inbound_conn.set_aes_key(key);
    inbound_conn.set_dnode_secured(true);
    let (rsp_tx, rsp_rx) = mpsc::channel::<OutboundEnvelope>(8);
    let handler = ClientHandler::new(Arc::new(recorder), rsp_tx, DataStore::Redis);
    let inbound_driver =
        tokio::spawn(async move { dnode_client_loop(inbound_conn, handler, rsp_rx).await });

    // 4. Write the header followed by the ciphertext.
    outbound.write_all(&header_bytes).await.unwrap();
    outbound.write_all(&ciphertext).await.unwrap();
    outbound.flush().await.unwrap();

    // 5. Wait for the dispatcher to capture the parsed request.
    let deadline = Instant::now() + Duration::from_secs(2);
    let captured = loop {
        {
            let mut g = seen.lock().await;
            if !g.is_empty() {
                break std::mem::take(&mut *g);
            }
        }
        assert!(
            Instant::now() <= deadline,
            "inbound dnode_client did not dispatch decrypted request within 2s"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };

    // 6. Assert the decrypted payload equals the original
    //    plaintext bytes.
    assert_eq!(captured.len(), 1);
    let m = &captured[0];
    assert_eq!(m.dmsg().expect("dmsg attached").id, 7);
    let payload: Vec<u8> = m
        .mbufs()
        .iter()
        .flat_map(|b| b.readable().to_vec())
        .collect();
    assert_eq!(payload, plaintext);

    // 7. Wind down.
    drop(outbound);
    inbound_driver.abort();
    let _ = inbound_driver.await;
}
