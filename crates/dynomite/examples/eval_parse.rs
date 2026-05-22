use dynomite::msg::{Msg, MsgParseResult, MsgType};
use dynomite::proto::redis::redis_parse_req;

fn try_parse(label: &str, bytes: &[u8]) {
    let mut m = Msg::new(1, MsgType::Unknown, true);
    let r = redis_parse_req(&mut m, bytes);
    println!("{label}: result={r:?} ty={:?} keys={} parser_pos={}",
             m.ty(), m.keys().len(), m.parser_pos());
}

fn main() {
    try_parse("PING", b"*1\r\n$4\r\nPING\r\n");
    try_parse("SET", b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    // "return 1" is 8 bytes, "return KEYS[1]" is 14 bytes; lengths fixed.
    try_parse("EVAL_0keys", b"*3\r\n$4\r\nEVAL\r\n$8\r\nreturn 1\r\n$1\r\n0\r\n");
    try_parse("EVAL_1key", b"*4\r\n$4\r\nEVAL\r\n$14\r\nreturn KEYS[1]\r\n$1\r\n1\r\n$3\r\nfoo\r\n");
    // EVALSHA hash + 0 keys.
    try_parse("EVALSHA", b"*3\r\n$7\r\nEVALSHA\r\n$40\r\n0123456789abcdef0123456789abcdef01234567\r\n$1\r\n0\r\n");
}
