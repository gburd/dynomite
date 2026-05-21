#![no_main]
//! Fuzz harness for the DNODE inter-peer header parser.
//!
//! Drives arbitrary bytes through `DnodeParser::step` and accepts
//! every well-defined return code; the only finding that fails the
//! corpus is a panic.
use libfuzzer_sys::fuzz_target;

use dynomite::proto::dnode::{DnodeParser, ParseStep};

fuzz_target!(|data: &[u8]| {
    let mut parser = DnodeParser::new();
    match parser.step(data) {
        ParseStep::HeaderDone { .. } | ParseStep::NeedMore { .. } | ParseStep::Error { .. } => {}
    }
});
