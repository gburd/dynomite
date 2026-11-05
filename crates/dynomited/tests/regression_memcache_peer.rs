//! A memcache pool serves keys a peer owns.
//!
//! Regression: the dnode listener parsed every request a peer forwarded
//! as RESP, whatever the pool's datastore. In a memcache rack a `set` for
//! a key another node owned reached that node as a memcache frame, failed
//! the RESP parse, and closed the peer link; the client got
//! `-ERR Peer: ...` (a RESP error on a memcache connection) and the key
//! was never stored. Two `dynomited` nodes share one rack, each with an
//! in-test memcache stand-in; every key must be stored, on its owner.

#[path = "support/port.rs"]
mod port;

use std::collections::HashMap;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use port::pick_port;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

type Store = Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>;

/// Serve `set <k> <f> <e> <n>\r\n<data>\r\n` and `get <k>\r\n` from
/// `store`: enough of the memcache text protocol for this test.
async fn fake_memcached(listener: TcpListener, store: Store) {
    loop {
        let Ok((mut s, _)) = listener.accept().await else {
            return;
        };
        let store = store.clone();
        tokio::spawn(async move {
            let mut acc: Vec<u8> = Vec::new();
            let mut buf = [0u8; 4096];
            loop {
                match s.read(&mut buf).await {
                    Ok(n) if n > 0 => acc.extend_from_slice(&buf[..n]),
                    _ => return,
                }
                while let Some(eol) = acc.windows(2).position(|w| w == b"\r\n") {
                    let line = String::from_utf8_lossy(&acc[..eol]).into_owned();
                    let parts: Vec<&str> = line.split(' ').collect();
                    let reply: Vec<u8> = match parts.first().copied() {
                        Some("set") if parts.len() >= 5 => {
                            let n: usize = parts[4].parse().unwrap_or(0);
                            let need = eol + 2 + n + 2;
                            if acc.len() < need {
                                break;
                            }
                            let data = acc[eol + 2..eol + 2 + n].to_vec();
                            store
                                .lock()
                                .unwrap()
                                .insert(parts[1].as_bytes().to_vec(), data);
                            acc.drain(..need);
                            b"STORED\r\n".to_vec()
                        }
                        Some("get") if parts.len() >= 2 => {
                            acc.drain(..eol + 2);
                            let mut r = Vec::new();
                            for k in &parts[1..] {
                                if let Some(v) = store.lock().unwrap().get(k.as_bytes()) {
                                    r.extend_from_slice(
                                        format!("VALUE {k} 0 {}\r\n", v.len()).as_bytes(),
                                    );
                                    r.extend_from_slice(v);
                                    r.extend_from_slice(b"\r\n");
                                }
                            }
                            r.extend_from_slice(b"END\r\n");
                            r
                        }
                        _ => {
                            acc.drain(..eol + 2);
                            b"ERROR\r\n".to_vec()
                        }
                    };
                    if s.write_all(&reply).await.is_err() {
                        return;
                    }
                }
            }
        });
    }
}

struct Node {
    child: Child,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn wait_for_listen(port: u16, deadline: Instant) -> bool {
    while Instant::now() < deadline {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memcache_set_reaches_the_owning_peer() {
    let dir = tempfile::tempdir().unwrap();
    let tokens = ["0", "2147483648"];
    let dyn_ports = [pick_port(), pick_port()];
    let listen_ports = [pick_port(), pick_port()];
    let mut stores: Vec<Store> = Vec::new();
    let mut nodes = Vec::new();
    for i in 0..2 {
        let store: Store = Arc::default();
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mc_port = l.local_addr().unwrap().port();
        tokio::spawn(fake_memcached(l, store.clone()));
        stores.push(store);
        let j = 1 - i;
        let conf = format!(
            "dyn_o_mite:\n  datacenter: dc1\n  rack: rack1\n  listen: 127.0.0.1:{}\n  \
             dyn_listen: 127.0.0.1:{}\n  stats_listen: 127.0.0.1:{}\n  tokens: '{}'\n  \
             servers:\n  - 127.0.0.1:{mc_port}:1\n  data_store: 1\n  \
             dyn_seed_provider: simple_provider\n  dyn_seeds:\n  - 127.0.0.1:{}:rack1:dc1:{}\n",
            listen_ports[i],
            dyn_ports[i],
            pick_port(),
            tokens[i],
            dyn_ports[j],
            tokens[j],
        );
        let path = dir.path().join(format!("n{i}.yml"));
        std::fs::write(&path, conf).unwrap();
        let child = Command::new(assert_cmd::cargo::cargo_bin("dynomited"))
            .arg("-c")
            .arg(&path)
            .arg("-p")
            .arg(dir.path().join(format!("n{i}.pid")))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn dynomited");
        nodes.push(Node { child });
    }
    for p in listen_ports {
        assert!(
            wait_for_listen(p, Instant::now() + Duration::from_secs(30)),
            "dynomited did not bind {p}"
        );
    }
    let mut c = TcpStream::connect(("127.0.0.1", listen_ports[0]))
        .await
        .unwrap();
    let mut buf = [0u8; 256];
    let keys: Vec<String> = (0..16).map(|i| format!("key:{i}")).collect();
    for k in &keys {
        // The peer link may still be coming up: retry until stored.
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            c.write_all(format!("set {k} 0 0 1\r\nv\r\n").as_bytes())
                .await
                .unwrap();
            let n = tokio::time::timeout(Duration::from_secs(5), c.read(&mut buf))
                .await
                .expect("reply")
                .unwrap();
            if &buf[..n] == b"STORED\r\n" {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "set {k}: {:?}",
                String::from_utf8_lossy(&buf[..n])
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    let held: Vec<usize> = stores.iter().map(|s| s.lock().unwrap().len()).collect();
    assert_eq!(
        held.iter().sum::<usize>(),
        keys.len(),
        "stored per node: {held:?}"
    );
    assert!(held.iter().all(|&n| n > 0), "both nodes own keys: {held:?}");
    drop(nodes);
}
