//! Per-protocol, per-command-class parser throughput benches.
//!
//! Run with `cargo bench --bench parsers`. The harness exercises
//! the Redis (RESP) and Memcache parsers across representative
//! command classes at five payload sizes (16 / 64 / 256 / 1024 /
//! 8192 bytes). Each iteration uses [`Bencher::iter_batched`] so
//! the per-iteration setup (`Msg::new`) is excluded from the timed
//! region and every iteration sees a freshly initialised parser
//! state machine.
//!
//! The corresponding baseline JSON files live at
//! `crates/dynomite/benches/baseline/parsers.json`. CI fails the
//! bench gate if any case regresses by more than 10% against that
//! baseline; see `docs/book/src/operations/benchmarks.md`.

#![allow(missing_docs)]

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use dynomite::msg::{Msg, MsgType};
use dynomite::proto::memcache::{memcache_parse_req, memcache_parse_rsp};
use dynomite::proto::redis::{redis_parse_req, redis_parse_rsp};

const PAYLOAD_SIZES: [usize; 5] = [16, 64, 256, 1024, 8192];

fn redis_set(payload: usize) -> Vec<u8> {
    let value = vec![b'x'; payload];
    let mut buf = Vec::new();
    buf.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$");
    buf.extend_from_slice(payload.to_string().as_bytes());
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(&value);
    buf.extend_from_slice(b"\r\n");
    buf
}

fn redis_get() -> Vec<u8> {
    b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n".to_vec()
}

fn redis_mget(n_keys: usize) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(format!("*{}\r\n$4\r\nMGET\r\n", n_keys + 1).as_bytes());
    for i in 0..n_keys {
        let key = format!("k{i:04}");
        buf.extend_from_slice(format!("${}\r\n{}\r\n", key.len(), key).as_bytes());
    }
    buf
}

fn redis_mset(n_keys: usize, payload: usize) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(format!("*{}\r\n$4\r\nMSET\r\n", 1 + 2 * n_keys).as_bytes());
    for i in 0..n_keys {
        let key = format!("k{i:04}");
        buf.extend_from_slice(format!("${}\r\n{}\r\n", key.len(), key).as_bytes());
        buf.extend_from_slice(format!("${payload}\r\n").as_bytes());
        buf.extend_from_slice(&vec![b'v'; payload]);
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

fn redis_hset(payload: usize) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(b"*4\r\n$4\r\nHSET\r\n$3\r\ntbl\r\n$5\r\nfield\r\n$");
    buf.extend_from_slice(payload.to_string().as_bytes());
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(&vec![b'v'; payload]);
    buf.extend_from_slice(b"\r\n");
    buf
}

fn redis_zadd() -> Vec<u8> {
    b"*4\r\n$4\r\nZADD\r\n$3\r\nzkn\r\n$3\r\n1.0\r\n$6\r\nmember\r\n".to_vec()
}

fn redis_eval(payload: usize) -> Vec<u8> {
    let script = vec![b's'; payload.max(1)];
    let mut buf = Vec::new();
    buf.extend_from_slice(format!("*3\r\n$4\r\nEVAL\r\n${}\r\n", script.len()).as_bytes());
    buf.extend_from_slice(&script);
    buf.extend_from_slice(b"\r\n$1\r\n0\r\n");
    buf
}

fn memcache_get_cmd(n_keys: usize) -> Vec<u8> {
    let mut buf = Vec::from(b"get".as_slice());
    for i in 0..n_keys {
        buf.extend_from_slice(format!(" k{i:04}").as_bytes());
    }
    buf.extend_from_slice(b"\r\n");
    buf
}

fn memcache_set_cmd(payload: usize) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(format!("set foo 0 0 {payload}\r\n").as_bytes());
    buf.extend_from_slice(&vec![b'x'; payload]);
    buf.extend_from_slice(b"\r\n");
    buf
}

fn memcache_cas_cmd(payload: usize) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(format!("cas foo 0 0 {payload} 12345\r\n").as_bytes());
    buf.extend_from_slice(&vec![b'x'; payload]);
    buf.extend_from_slice(b"\r\n");
    buf
}

fn run_redis_req(c: &mut Criterion) {
    let mut group = c.benchmark_group("redis_parse_req");
    for &p in &PAYLOAD_SIZES {
        let inputs: [(&str, Vec<u8>); 7] = [
            ("set", redis_set(p)),
            ("get", redis_get()),
            ("mget", redis_mget(8.min(1 + p / 8))),
            ("mset", redis_mset(4, p / 4)),
            ("hset", redis_hset(p)),
            ("zadd", redis_zadd()),
            ("eval", redis_eval(p / 4)),
        ];
        for (label, bytes) in inputs {
            let id = BenchmarkId::new(label, p);
            group.throughput(Throughput::Bytes(bytes.len() as u64));
            group.bench_with_input(id, &bytes, |b, bytes| {
                b.iter_batched(
                    || Msg::new(0, MsgType::Unknown, true),
                    |mut msg| {
                        let _ = redis_parse_req(&mut msg, black_box(bytes));
                    },
                    BatchSize::SmallInput,
                );
            });
        }
    }
    group.finish();
}

fn run_redis_rsp(c: &mut Criterion) {
    let mut group = c.benchmark_group("redis_parse_rsp");
    for &p in &PAYLOAD_SIZES {
        let mut bulk = Vec::new();
        bulk.extend_from_slice(format!("${p}\r\n").as_bytes());
        bulk.extend_from_slice(&vec![b'y'; p]);
        bulk.extend_from_slice(b"\r\n");
        group.throughput(Throughput::Bytes(bulk.len() as u64));
        group.bench_with_input(BenchmarkId::new("bulk", p), &bulk, |b, bytes| {
            b.iter_batched(
                || Msg::new(0, MsgType::Unknown, false),
                |mut msg| {
                    let _ = redis_parse_rsp(&mut msg, black_box(bytes));
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn run_memcache_req(c: &mut Criterion) {
    let mut group = c.benchmark_group("memcache_parse_req");
    for &p in &PAYLOAD_SIZES {
        let inputs: [(&str, Vec<u8>); 3] = [
            ("get", memcache_get_cmd(8.min(1 + p / 8))),
            ("set", memcache_set_cmd(p)),
            ("cas", memcache_cas_cmd(p)),
        ];
        for (label, bytes) in inputs {
            group.throughput(Throughput::Bytes(bytes.len() as u64));
            group.bench_with_input(BenchmarkId::new(label, p), &bytes, |b, bytes| {
                b.iter_batched(
                    || Msg::new(0, MsgType::Unknown, true),
                    |mut msg| {
                        let _ = memcache_parse_req(&mut msg, black_box(bytes));
                    },
                    BatchSize::SmallInput,
                );
            });
        }
    }
    group.finish();
}

fn run_memcache_rsp(c: &mut Criterion) {
    let mut group = c.benchmark_group("memcache_parse_rsp");
    for &p in &PAYLOAD_SIZES {
        let mut value = Vec::new();
        value.extend_from_slice(format!("VALUE foo 0 {p}\r\n").as_bytes());
        value.extend_from_slice(&vec![b'x'; p]);
        value.extend_from_slice(b"\r\nEND\r\n");
        group.throughput(Throughput::Bytes(value.len() as u64));
        group.bench_with_input(BenchmarkId::new("value", p), &value, |b, bytes| {
            b.iter_batched(
                || Msg::new(0, MsgType::Unknown, false),
                |mut msg| {
                    let _ = memcache_parse_rsp(&mut msg, black_box(bytes));
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    run_redis_req,
    run_redis_rsp,
    run_memcache_req,
    run_memcache_rsp,
);
criterion_main!(benches);
