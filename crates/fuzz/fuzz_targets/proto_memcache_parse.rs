#![no_main]
//! Fuzz harness for `dynomite::proto::memcache::memcache_parse_req`.
use libfuzzer_sys::fuzz_target;

use dynomite::msg::{Msg, MsgType};
use dynomite::proto::memcache::memcache_parse_req;

fuzz_target!(|data: &[u8]| {
    let mut msg = Msg::new(0, MsgType::Unknown, true);
    let _ = memcache_parse_req(&mut msg, data);
});
