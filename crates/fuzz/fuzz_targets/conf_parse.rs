#![no_main]
//! Fuzz harness for `dynomite::conf::Config::parse_str`.
//!
//! Treats arbitrary input bytes as candidate UTF-8 (lossy
//! conversion) and feeds them to the YAML configuration loader.
//! The parser must return either a [`Config`](dynomite::conf::Config)
//! or a `ConfError`; anything else (panic, abort, allocation
//! overflow) is a finding.
use libfuzzer_sys::fuzz_target;

use dynomite::conf::Config;

fuzz_target!(|data: &[u8]| {
    let s = String::from_utf8_lossy(data);
    let _ = Config::parse_str(&s);
});
