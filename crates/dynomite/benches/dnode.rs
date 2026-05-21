//! DNODE peer-protocol encode + parse benches.
//!
//! Run with `cargo bench --bench dnode`. The harness exercises the
//! header writer (`dmsg_write`) followed by the parser
//! (`DnodeParser::step`) over a payload-size sweep
//! (64 / 256 / 1024 / 4096 bytes).
//!
//! The corresponding baseline JSON file lives at
//! `crates/dynomite/benches/baseline/dnode.json`.

#![allow(missing_docs)]

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use dynomite::io::mbuf::MbufPool;
use dynomite::proto::dnode::{dmsg_write, DmsgType, DnodeParser};

const PLEN_SIZES: [u32; 4] = [64, 256, 1024, 4096];

fn encode(c: &mut Criterion) {
    let pool = MbufPool::default();
    let mut group = c.benchmark_group("dnode_encode");
    for &p in &PLEN_SIZES {
        group.throughput(Throughput::Bytes(u64::from(p)));
        group.bench_with_input(BenchmarkId::from_parameter(p), &p, |b, &plen| {
            b.iter_with_setup(
                || pool.get(),
                |mut mb| {
                    dmsg_write(&mut mb, 1, DmsgType::Req, 0, true, None, black_box(plen)).unwrap();
                    pool.put(mb);
                },
            );
        });
    }
    group.finish();
}

fn parse(c: &mut Criterion) {
    let pool = MbufPool::default();
    let mut group = c.benchmark_group("dnode_parse");
    for &p in &PLEN_SIZES {
        // Pre-build the header bytes once.
        let mut mb = pool.get();
        dmsg_write(&mut mb, 1, DmsgType::Req, 0, true, None, p).unwrap();
        let header: Vec<u8> = mb.readable().to_vec();
        pool.put(mb);
        group.throughput(Throughput::Bytes(header.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(p), &header, |b, header| {
            b.iter(|| {
                let mut parser = DnodeParser::new();
                let _ = parser.step(black_box(header));
            });
        });
    }
    group.finish();
}

criterion_group!(benches, encode, parse);
criterion_main!(benches);
