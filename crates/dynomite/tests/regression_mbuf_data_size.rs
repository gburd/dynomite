//! Regression: the per-chunk data size and the redis key-length bound.
//!
//! A chunk of configured size `mbuf_size` reserves a 64-byte header and
//! a 16-byte trailing extra region, so the writable region is
//! `mbuf_size - MBUF_HSIZE` and `mbuf_data_size()` (writable plus extra)
//! is `mbuf_size + MBUF_ESIZE - MBUF_HSIZE`. The redis request parser
//! rejects a key whose length is `>= mbuf_data_size()` for the
//! configured `mbuf_size`, not for the 512000-byte maximum.

use dynomite::io::mbuf::{
    mbuf_data_size, mbuf_init, MbufPool, MBUF_ESIZE, MBUF_HSIZE, MBUF_MAX_SIZE, MBUF_MIN_SIZE,
    MBUF_SIZE,
};
use dynomite::msg::{Msg, MsgParseResult, MsgType};
use dynomite::proto::redis::redis_parse_req;

/// Reference formula: `mbuf_offset = mbuf_size + MBUF_ESIZE - MBUF_HSIZE`.
fn c_data_size(mbuf_size: usize) -> usize {
    mbuf_size + 16 - 64
}

fn get_with_key_len(n: usize) -> MsgParseResult {
    let mut wire = format!("*2\r\n$3\r\nGET\r\n${n}\r\n").into_bytes();
    wire.extend(std::iter::repeat_n(b'k', n));
    wire.extend_from_slice(b"\r\n");
    let mut m = Msg::new(1, MsgType::Unknown, true);
    redis_parse_req(&mut m, &wire)
}

#[test]
fn regression_pool_data_size_matches_reference_formula() {
    assert_eq!(MBUF_HSIZE, 64);
    assert_eq!(MBUF_ESIZE, 16);
    assert_eq!(c_data_size(MBUF_SIZE), 16336);
    for size in [MBUF_MIN_SIZE, 1024, MBUF_SIZE, 65536, MBUF_MAX_SIZE] {
        let pool = MbufPool::new(size, 0);
        assert_eq!(pool.chunk_size(), size);
        assert_eq!(pool.data_size(), c_data_size(size), "size {size}");
        let buf = pool.get();
        assert_eq!(buf.data_size(), c_data_size(size), "size {size}");
        // Writable region: end - start = mbuf_offset - MBUF_ESIZE.
        assert_eq!(buf.remaining(), size - 64, "size {size}");
        assert_eq!(buf.capacity(), size - 64, "size {size}");
    }
    assert_eq!(MbufPool::new(MBUF_SIZE, 0).get().remaining(), 16320);
}

/// One test owns every check that reads or writes the process-wide
/// configured mbuf size, so parallel test threads never observe a
/// transient value.
#[test]
fn regression_redis_key_len_bound_follows_configured_mbuf_size() {
    assert_eq!(mbuf_data_size(), c_data_size(MBUF_SIZE));
    assert_eq!(MbufPool::default().get().remaining(), 16320);
    let ds = mbuf_data_size();
    assert_eq!(get_with_key_len(ds - 1), MsgParseResult::Ok);
    assert_eq!(get_with_key_len(ds), MsgParseResult::Error);
    // Between the data size and the 512000-byte maximum: rejected.
    assert_eq!(get_with_key_len(100_000), MsgParseResult::Error);

    for size in [MBUF_MIN_SIZE, 1024, 65536] {
        mbuf_init(size);
        assert_eq!(mbuf_data_size(), c_data_size(size));
        assert_eq!(MbufPool::default().chunk_size(), size);
        assert_eq!(MbufPool::default().data_size(), c_data_size(size));
        let ds = c_data_size(size);
        assert_eq!(get_with_key_len(ds - 1), MsgParseResult::Ok, "size {size}");
        assert_eq!(get_with_key_len(ds), MsgParseResult::Error, "size {size}");
    }
    mbuf_init(MBUF_SIZE);
    assert_eq!(mbuf_data_size(), c_data_size(MBUF_SIZE));
}
