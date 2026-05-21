#![no_main]
//! Fuzz harness for `dynomite::proto::redis::redis_parse_req`.
//!
//! Drives arbitrary bytes through the Redis (RESP) request parser
//! and asserts the parser does not panic. Any return value is
//! acceptable; only panics constitute a finding.
use libfuzzer_sys::fuzz_target;

use dynomite::msg::{Msg, MsgType};
use dynomite::proto::redis::redis_parse_req;

fuzz_target!(|data: &[u8]| {
    let mut msg = Msg::new(0, MsgType::Unknown, true);
    let _ = redis_parse_req(&mut msg, data);
});
