//! End-to-end served CRDT (data-type) integration tests for the
//! Map and HyperLogLog types.
//!
//! Gated on the `noxu` feature; without it the file compiles to
//! nothing (the served CRDT path is backed by `NoxuDatastore`).
//!
//! Mirrors `crdt_register_flag_round_trip.rs`: starts a real
//! `serve_pbc` listener backed by [`NoxuDatastore`], drives
//! `DtUpdateReq` / `DtFetchReq` over TCP, and asserts the served CRDT
//! path converges:
//!
//! * a map with a register field and an incremented counter field
//!   reads back both fields with the right values;
//! * an HLL fed a batch of distinct items reads back a cardinality
//!   estimate near the true count.

#![cfg(feature = "noxu")]

use std::sync::Arc;

use prost::Message as _;
use tokio::net::{TcpListener, TcpStream};

use dyniak::datastore::NoxuDatastore;
use dyniak::proto::pb::{
    read_frame, write_frame, CounterOp, DtFetchReq, DtFetchResp, DtOp, DtUpdateReq, DtUpdateResp,
    DtValue, Frame, HllOp, MapField, MapOp, MapUpdate, MessageCode, RegisterOp, ScalarOp,
    MAP_FIELD_TYPE_COUNTER, MAP_FIELD_TYPE_REGISTER,
};
use dyniak::serve_pbc;
use dynomite::embed::Datastore;
use tempfile::TempDir;

async fn spawn_server() -> (TempDir, std::net::SocketAddr) {
    let dir = TempDir::new().expect("tempdir");
    let noxu = NoxuDatastore::open_in(dir.path()).expect("open noxu");
    let ds: Arc<dyn Datastore> = Arc::new(noxu);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let _ = serve_pbc(listener, ds).await;
    });
    (dir, addr)
}

fn map_update_op(bucket: &[u8], key: &[u8], op: MapOp) -> DtUpdateReq {
    DtUpdateReq {
        bucket: bucket.to_vec(),
        key: Some(key.to_vec()),
        r#type: b"maps".to_vec(),
        op: Some(DtOp {
            map_op: Some(Box::new(op)),
            ..DtOp::default()
        }),
        ..DtUpdateReq::default()
    }
}

async fn dt_update_map(
    stream: &mut TcpStream,
    req: DtUpdateReq,
) -> Option<Box<dyniak::proto::pb::MapValue>> {
    let (mut r, mut w) = tokio::io::split(stream);
    write_frame(
        &mut w,
        &Frame::new(MessageCode::DtUpdateReq.as_u8(), req.encode_to_vec()),
    )
    .await
    .expect("send dt_update");
    let resp = read_frame(&mut r).await.expect("recv dt_update");
    assert_eq!(
        resp.code,
        MessageCode::DtUpdateResp.as_u8(),
        "dt_update must return DtUpdateResp, got code {}",
        resp.code
    );
    DtUpdateResp::decode(resp.body.as_slice())
        .expect("decode")
        .map_value
}

async fn dt_fetch_map(stream: &mut TcpStream, bucket: &[u8], key: &[u8]) -> Option<DtValue> {
    let req = DtFetchReq {
        bucket: bucket.to_vec(),
        key: key.to_vec(),
        r#type: b"maps".to_vec(),
        ..DtFetchReq::default()
    };
    let (mut r, mut w) = tokio::io::split(stream);
    write_frame(
        &mut w,
        &Frame::new(MessageCode::DtFetchReq.as_u8(), req.encode_to_vec()),
    )
    .await
    .expect("send dt_fetch");
    let resp = read_frame(&mut r).await.expect("recv dt_fetch");
    assert_eq!(resp.code, MessageCode::DtFetchResp.as_u8());
    DtFetchResp::decode(resp.body.as_slice())
        .expect("decode")
        .value
}

async fn dt_update_hll(
    stream: &mut TcpStream,
    bucket: &[u8],
    key: &[u8],
    items: &[&[u8]],
) -> Option<u64> {
    let req = DtUpdateReq {
        bucket: bucket.to_vec(),
        key: Some(key.to_vec()),
        r#type: b"hlls".to_vec(),
        op: Some(DtOp {
            hll_op: Some(HllOp {
                add_value: items.iter().map(|i| i.to_vec()).collect(),
            }),
            ..DtOp::default()
        }),
        ..DtUpdateReq::default()
    };
    let (mut r, mut w) = tokio::io::split(stream);
    write_frame(
        &mut w,
        &Frame::new(MessageCode::DtUpdateReq.as_u8(), req.encode_to_vec()),
    )
    .await
    .expect("send dt_update");
    let resp = read_frame(&mut r).await.expect("recv dt_update");
    assert_eq!(resp.code, MessageCode::DtUpdateResp.as_u8());
    DtUpdateResp::decode(resp.body.as_slice())
        .expect("decode")
        .hll_value
}

async fn dt_fetch_hll(stream: &mut TcpStream, bucket: &[u8], key: &[u8]) -> Option<u64> {
    let req = DtFetchReq {
        bucket: bucket.to_vec(),
        key: key.to_vec(),
        r#type: b"hlls".to_vec(),
        ..DtFetchReq::default()
    };
    let (mut r, mut w) = tokio::io::split(stream);
    write_frame(
        &mut w,
        &Frame::new(MessageCode::DtFetchReq.as_u8(), req.encode_to_vec()),
    )
    .await
    .expect("send dt_fetch");
    let resp = read_frame(&mut r).await.expect("recv dt_fetch");
    assert_eq!(resp.code, MessageCode::DtFetchResp.as_u8());
    DtFetchResp::decode(resp.body.as_slice())
        .expect("decode")
        .value
        .and_then(|v| v.hll_value)
}

#[tokio::test]
async fn map_register_and_counter_fields_read_back_after_update() {
    let (_dir, addr) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.expect("connect");

    let op = MapOp {
        updates: vec![
            MapUpdate {
                field: Some(MapField {
                    name: b"name".to_vec(),
                    field_type: MAP_FIELD_TYPE_REGISTER,
                }),
                op: Some(ScalarOp {
                    register_op: Some(RegisterOp {
                        value: b"alice".to_vec(),
                        ts_micros: Some(1),
                    }),
                    ..ScalarOp::default()
                }),
            },
            MapUpdate {
                field: Some(MapField {
                    name: b"hits".to_vec(),
                    field_type: MAP_FIELD_TYPE_COUNTER,
                }),
                op: Some(ScalarOp {
                    counter_op: Some(CounterOp { increment: Some(3) }),
                    ..ScalarOp::default()
                }),
            },
        ],
        removes: vec![],
    };

    let updated = dt_update_map(&mut s, map_update_op(b"profiles", b"bob", op))
        .await
        .expect("map value present");
    assert_eq!(updated.entries.len(), 2);

    let fetched = dt_fetch_map(&mut s, b"profiles", b"bob")
        .await
        .expect("dt value present")
        .map_value
        .expect("map value present");
    assert_eq!(fetched.entries.len(), 2);

    let register_entry = fetched
        .entries
        .iter()
        .find(|e| e.field.as_ref().is_some_and(|f| f.name == b"name"))
        .expect("register field present");
    assert_eq!(
        register_entry
            .value
            .as_ref()
            .and_then(|v| v.register_value.clone()),
        Some(b"alice".to_vec())
    );

    let counter_entry = fetched
        .entries
        .iter()
        .find(|e| e.field.as_ref().is_some_and(|f| f.name == b"hits"))
        .expect("counter field present");
    assert_eq!(
        counter_entry.value.as_ref().and_then(|v| v.counter_value),
        Some(3)
    );
}

#[tokio::test]
async fn map_counter_field_accumulates_across_updates() {
    let (_dir, addr) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.expect("connect");

    let counter_update = |delta: i64| MapOp {
        updates: vec![MapUpdate {
            field: Some(MapField {
                name: b"hits".to_vec(),
                field_type: MAP_FIELD_TYPE_COUNTER,
            }),
            op: Some(ScalarOp {
                counter_op: Some(CounterOp {
                    increment: Some(delta),
                }),
                ..ScalarOp::default()
            }),
        }],
        removes: vec![],
    };

    dt_update_map(&mut s, map_update_op(b"stats", b"page", counter_update(2))).await;
    let updated = dt_update_map(&mut s, map_update_op(b"stats", b"page", counter_update(5)))
        .await
        .expect("map value present");
    let counter_value = updated
        .entries
        .iter()
        .find(|e| e.field.as_ref().is_some_and(|f| f.name == b"hits"))
        .and_then(|e| e.value.as_ref())
        .and_then(|v| v.counter_value);
    assert_eq!(counter_value, Some(7));
}

#[tokio::test]
async fn hll_add_batch_reads_back_near_the_true_cardinality() {
    let (_dir, addr) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.expect("connect");

    let items: Vec<Vec<u8>> = (0u32..500).map(|i| i.to_be_bytes().to_vec()).collect();
    let item_refs: Vec<&[u8]> = items.iter().map(Vec::as_slice).collect();

    let updated = dt_update_hll(&mut s, b"analytics", b"visitors", &item_refs)
        .await
        .expect("hll value present");
    assert!(
        (450..=550).contains(&updated),
        "updated cardinality {updated} not near 500"
    );

    let fetched = dt_fetch_hll(&mut s, b"analytics", b"visitors")
        .await
        .expect("hll value present");
    assert_eq!(fetched, updated);
}

#[tokio::test]
async fn hll_repeated_adds_converge_via_merge() {
    let (_dir, addr) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.expect("connect");

    let batch_a: Vec<Vec<u8>> = (0u32..200).map(|i| i.to_be_bytes().to_vec()).collect();
    let batch_b: Vec<Vec<u8>> = (100u32..300).map(|i| i.to_be_bytes().to_vec()).collect();
    let refs_a: Vec<&[u8]> = batch_a.iter().map(Vec::as_slice).collect();
    let refs_b: Vec<&[u8]> = batch_b.iter().map(Vec::as_slice).collect();

    dt_update_hll(&mut s, b"analytics", b"union", &refs_a).await;
    let updated = dt_update_hll(&mut s, b"analytics", b"union", &refs_b)
        .await
        .expect("hll value present");
    // Union of [0, 300) is 300 distinct items; the two adds together
    // must estimate near the union, not just the second batch alone.
    assert!(
        (250..=350).contains(&updated),
        "unioned cardinality {updated} not near 300"
    );
}
