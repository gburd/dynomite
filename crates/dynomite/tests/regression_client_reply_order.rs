//! Regression: the client driver wrote each reply as it arrived from the
//! dispatcher, so pipelined requests whose replies finished out of order
//! (a slow replica for one, a fast one for the next, a timeout before a
//! success) reached the client in the wrong order: `GET a; PING` answered
//! `+PONG` then the value, and a request that never completed was skipped
//! altogether. Redis clients match replies to requests by position, so
//! every later reply on the connection went to the wrong caller. Replies
//! must be released in request order, as the C engine does by sending the
//! head of the connection's outstanding queue only once it is done.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dynomite::conf::DataStore;
use dynomite::msg::{Msg, MsgType};
use dynomite::net::dispatcher::{DispatchOutcome, Dispatcher, OutboundEnvelope, ServerSink};
use dynomite::net::Proxy;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Holds every request's responder and answers them all, in REVERSE
/// arrival order, once `expect` requests have arrived. Each reply is the
/// request's own key echoed as a bulk string, so order is visible.
struct ReverseDispatcher {
    expect: usize,
    held: Mutex<Vec<(Msg, ServerSink)>>,
}

impl Dispatcher for ReverseDispatcher {
    fn dispatch(&self, req: Msg, responder: ServerSink) -> DispatchOutcome {
        let mut held = self.held.lock();
        held.push((req, responder));
        if held.len() == self.expect {
            let batch: Vec<(Msg, ServerSink)> = held.drain(..).rev().collect();
            tokio::spawn(async move {
                for (req, tx) in batch {
                    let key = req
                        .keys()
                        .first()
                        .map(|k| k.key().to_vec())
                        .unwrap_or_default();
                    let mut body = format!("${}\r\n", key.len()).into_bytes();
                    body.extend_from_slice(&key);
                    body.extend_from_slice(b"\r\n");
                    let mut rsp = Msg::new(req.id(), MsgType::RspRedisBulk, false);
                    rsp.append_bytes(&dynomite::io::mbuf::MbufPool::default(), &body);
                    let _ = tx
                        .send(OutboundEnvelope {
                            req_id: req.id(),
                            rsp,
                            span: tracing::Span::current(),
                            source_peer_idx: None,
                        })
                        .await;
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            });
        }
        DispatchOutcome::Pending
    }
}

#[tokio::test]
async fn pipelined_replies_reach_the_client_in_request_order() {
    let keys = ["k0", "k1", "k2", "k3", "k4"];
    let dispatcher = Arc::new(ReverseDispatcher {
        expect: keys.len(),
        held: Mutex::new(Vec::new()),
    });
    let proxy = Proxy::bind("127.0.0.1:0".parse::<SocketAddr>().unwrap(), dispatcher)
        .unwrap()
        .with_data_store(DataStore::Valkey);
    let addr = proxy.local_addr().unwrap();
    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
    let cancel: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
        Box::pin(async move {
            let _ = cancel_rx.await;
        });
    let handle = tokio::spawn(async move { proxy.run(cancel).await });

    let mut client = TcpStream::connect(addr).await.unwrap();
    let mut pipeline = Vec::new();
    for k in keys {
        pipeline.extend_from_slice(format!("*2\r\n$3\r\nGET\r\n$2\r\n{k}\r\n").as_bytes());
    }
    client.write_all(&pipeline).await.unwrap();

    let want: Vec<u8> = keys
        .iter()
        .flat_map(|k| format!("$2\r\n{k}\r\n").into_bytes())
        .collect();
    let mut got = Vec::new();
    let mut buf = [0u8; 256];
    while got.len() < want.len() {
        let n = tokio::time::timeout(Duration::from_secs(5), client.read(&mut buf))
            .await
            .expect("replies within 5s")
            .unwrap();
        assert!(
            n > 0,
            "connection closed early; got {:?}",
            String::from_utf8_lossy(&got)
        );
        got.extend_from_slice(&buf[..n]);
    }
    assert_eq!(
        String::from_utf8_lossy(&got),
        String::from_utf8_lossy(&want)
    );
    let _ = cancel_tx.send(());
    let _ = handle.await;
}
