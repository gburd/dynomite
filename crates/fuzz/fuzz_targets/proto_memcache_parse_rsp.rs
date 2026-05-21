#![no_main]
//! Fuzz harness for `dynomite::proto::memcache::memcache_parse_rsp`.
use libfuzzer_sys::fuzz_target;

use dynomite::msg::{Msg, MsgType};
use dynomite::proto::memcache::memcache_parse_rsp;

fuzz_target!(|data: &[u8]| {
    let mut msg = Msg::new(0, MsgType::Unknown, false);
    let _ = memcache_parse_rsp(&mut msg, data);
});
