//! Stage 2 regression and property tests for the I/O substrate.
//!
//! Covers the deliverables called out in PLAN.md Stage 2:
//!
//! * `mbuf` split / append round-trip via [`proptest`].
//! * `mbuf` pool recycling without re-allocation.
//! * `cbuf` SPSC FIFO ordering.
//! * `reactor` end-to-end TCP echo through a chained mbuf via the
//!   [`Transport`](dynomite::io::reactor::Transport) abstraction.

use dynomite::io::cbuf::CBuf;
use dynomite::io::mbuf::{Mbuf, MbufPool, MbufQueue, MBUF_ESIZE, MBUF_SIZE};
use dynomite::io::reactor::{ConnRole, TcpTransport, Transport};
use proptest::prelude::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// Property: splitting an mbuf at every valid offset and appending
// the tail back yields the original byte sequence.
proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    #[test]
    fn split_off_then_append_reconstructs(
        bytes in proptest::collection::vec(any::<u8>(), 0..(MBUF_SIZE - MBUF_ESIZE)),
        cut_raw in 0usize..(2 * (MBUF_SIZE - MBUF_ESIZE))
    ) {
        let pool = MbufPool::default();
        let mut head = pool.get();
        let n = head.recv(&bytes);
        prop_assert_eq!(n, bytes.len());
        let original = bytes.clone();
        if cut_raw > head.len() {
            // k > len: the C contract is to refuse. We reproduce it
            // by returning None and leaving the source untouched.
            prop_assert!(head.split_off(cut_raw, &pool).is_none());
            prop_assert_eq!(head.readable(), original.as_slice());
        } else {
            let cut = cut_raw;
            let tail = head.split_off(cut, &pool).expect("split within len");
            prop_assert_eq!(head.len(), cut);
            prop_assert_eq!(head.len() + tail.len(), original.len());
            let mut concat = Vec::with_capacity(original.len());
            concat.extend_from_slice(head.readable());
            concat.extend_from_slice(tail.readable());
            prop_assert_eq!(concat, original);
        }
    }
}

#[test]
fn pool_recycles_chunks_without_reallocation() {
    const N: usize = 8;
    let pool = MbufPool::default();

    // First wave: forces N fresh allocations.
    let mut taken: Vec<Mbuf> = (0..N).map(|_| pool.get()).collect();
    assert_eq!(pool.total_allocated(), N as u64);

    // Return them. Free list now holds N entries.
    for b in taken.drain(..) {
        pool.put(b);
    }
    assert_eq!(pool.free_count(), N);

    // Second wave: pulls from the free list. The total-allocated
    // counter is the canonical signal of recycle vs allocate; it
    // stays at N if every chunk came from the free list.
    let _second: Vec<Mbuf> = (0..N).map(|_| pool.get()).collect();
    assert_eq!(pool.total_allocated(), N as u64);
}

#[test]
fn cbuf_spsc_fifo_ordering() {
    const N: u32 = 64;
    let q: CBuf<u32> = CBuf::new(N as usize);
    for i in 0..N {
        q.push(i).unwrap();
    }
    assert!(q.is_full());
    assert_eq!(q.len(), N as usize);
    for i in 0..N {
        assert_eq!(q.pop(), Some(i));
    }
    assert!(q.is_empty());
}

/// End-to-end: a TCP echo loop driven through [`TcpTransport`], with
/// the server side staging the read through a chained mbuf and writing
/// it back out of the chain.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reactor_echoes_mbuf_chain() {
    const PAYLOAD_LEN: usize = 8192;
    let payload: Vec<u8> = (0..PAYLOAD_LEN)
        .map(|i| u8::try_from(i % 251).unwrap())
        .collect();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let payload_for_check = payload.clone();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut transport = TcpTransport::new(stream, ConnRole::Server);
        assert_eq!(transport.role(), ConnRole::Server);

        // Use small chunks so the payload spans multiple mbufs.
        let pool = MbufPool::new(512, 64);
        let mut chain = MbufQueue::new();
        let mut received = 0usize;

        while received < PAYLOAD_LEN {
            let mut buf = pool.get();
            let cap = buf.remaining();
            let mut read_buf = vec![0u8; cap];
            let n = transport.read(&mut read_buf).await.unwrap();
            assert!(n > 0, "premature EOF");
            buf.recv(&read_buf[..n]);
            received += n;
            chain.push_back(buf);
        }
        assert_eq!(received, PAYLOAD_LEN);
        assert_eq!(chain.total_len(), PAYLOAD_LEN);

        // Echo back, draining each chunk fully.
        while let Some(buf) = chain.pop_front() {
            transport.write_all(buf.readable()).await.unwrap();
            pool.put(buf);
        }
        transport.flush().await.unwrap();
        transport.shutdown().await.unwrap();
    });

    let client_stream = TcpStream::connect(addr).await.unwrap();
    let mut client = TcpTransport::new(client_stream, ConnRole::Client);
    assert_eq!(client.role(), ConnRole::Client);
    assert!(client.peer_addr().is_some());

    client.write_all(&payload).await.unwrap();
    client.flush().await.unwrap();

    let mut echoed = vec![0u8; PAYLOAD_LEN];
    client.read_exact(&mut echoed).await.unwrap();
    assert_eq!(echoed, payload_for_check);

    server.await.unwrap();
}
