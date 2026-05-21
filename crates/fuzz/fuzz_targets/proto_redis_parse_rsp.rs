#![no_main]
//! Fuzz harness for `dynomite::proto::redis::redis_parse_rsp`.
//!
//! Drives arbitrary bytes through the Redis (RESP) response parser
//! and asserts the parser does not panic.
use libfuzzer_sys::fuzz_target;

use dynomite::msg::{Msg, MsgType};
use dynomite::proto::redis::redis_parse_rsp;

fuzz_target!(|data: &[u8]| {
    let mut msg = Msg::new(0, MsgType::Unknown, false);
    let _ = redis_parse_rsp(&mut msg, data);
});
