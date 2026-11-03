//! Regression: requests and replies larger than one read or one mbuf.
//!
//! Two faults truncated large values:
//! * the client parse loop drained the bytes of an incomplete request on
//!   `Again`, so any request spanning two reads (about 4 KiB) lost its head
//!   and the connection closed on the garbage that followed;
//! * every receive path copied a parsed frame into a single mbuf with one
//!   `Mbuf::recv`, which keeps at most the chunk size (16 KiB), so larger
//!   frames were silently cut short and the request hung.
//!
//! The C engine stores 5 MiB values (the `func_test.py` XLarge case).

use dynomite::io::mbuf::{MbufPool, MBUF_SIZE};
use dynomite::msg::{Msg, MsgType};

#[test]
fn append_bytes_keeps_every_byte_across_chunks() {
    let pool = MbufPool::default();
    for len in [
        0,
        1,
        MBUF_SIZE - 1,
        MBUF_SIZE,
        MBUF_SIZE + 1,
        5 * 1024 * 1024,
    ] {
        let payload: Vec<u8> = (0..len).map(|i| u8::try_from(i % 251).unwrap()).collect();
        let mut m = Msg::new(1, MsgType::Unknown, true);
        m.append_bytes(&pool, &payload);
        let back: Vec<u8> = m
            .mbufs()
            .iter()
            .flat_map(|b| b.readable().to_vec())
            .collect();
        assert_eq!(back.len(), len, "len {len}");
        assert_eq!(back, payload, "len {len}");
        assert_eq!(m.mlen() as usize, len);
    }
}
