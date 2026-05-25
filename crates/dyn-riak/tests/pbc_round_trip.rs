//! End-to-end PBC round-trip integration tests.
//!
//! Spins up [`dyn_riak::serve_pbc`] over a real `tokio::net::TcpListener`
//! bound to localhost, then drives requests over a
//! [`tokio::net::TcpStream`] client. Two top-level tests live here:
//!
//! * [`pbc_ping_put_get_del_round_trip`] -- the original v0.0.1
//!   slice's exchange (ping, put, get, del). Asserts that every K/V
//!   op surfaces through to the substrate's `dispatch_count`.
//! * [`pbc_new_ops_round_trip`] -- the v0.0.2 op slice
//!   (server-info, get-bucket, set-bucket, list-buckets, list-keys,
//!   2i index). Asserts the responses for each new op and that the
//!   substrate's `dispatch_count` stays at zero because the new ops
//!   either reply directly (server-info, bucket-props) or surface
//!   `RpbErrorResp` (list-* and index, which the `MemoryDatastore`
//!   does not implement).
//!
//! The tests deliberately use [`dynomite::embed::MemoryDatastore`]
//! rather than [`dyn_riak::datastore::NoxuDatastore`] so they run
//! without the lamdb checkout next door.

use std::sync::Arc;
use std::time::Duration;

use prost::Message as _;
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use dyn_riak::error::RiakError;
use dyn_riak::proto::pb::{
    read_frame, write_frame, Frame, MessageCode, RpbBucketProps, RpbDelReq, RpbErrorResp,
    RpbGetBucketReq, RpbGetBucketResp, RpbGetReq, RpbGetResp, RpbGetServerInfoResp, RpbIndexReq,
    RpbIndexResp, RpbListBucketsReq, RpbListBucketsResp, RpbListKeysReq, RpbListKeysResp,
    RpbPutReq, RpbPutResp, RpbServerInfoReq, RpbSetBucketReq, RpbSetBucketResp,
    INDEX_QUERY_TYPE_EQ,
};
use dyn_riak::serve_pbc;
use dynomite::embed::{Datastore, MemoryDatastore};

type Reader = ReadHalf<TcpStream>;
type Writer = WriteHalf<TcpStream>;

struct Harness {
    ds: Arc<MemoryDatastore>,
    server: JoinHandle<Result<(), RiakError>>,
    reader: Reader,
    writer: Writer,
}

impl Harness {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");

        let ds = Arc::new(MemoryDatastore::new());
        let ds_for_server: Arc<dyn Datastore> = ds.clone();
        let server = tokio::spawn(async move { serve_pbc(listener, ds_for_server).await });

        // Give tokio a chance to enter the accept loop.
        tokio::time::sleep(Duration::from_millis(10)).await;

        let stream = TcpStream::connect(addr).await.expect("connect");
        let (reader, writer) = tokio::io::split(stream);
        Self {
            ds,
            server,
            reader,
            writer,
        }
    }

    async fn shutdown(self) {
        drop(self.reader);
        drop(self.writer);
        // serve_pbc keeps looping; aborting the spawned task is the
        // standard graceful-shutdown shape for this test.
        self.server.abort();
        let _ = self.server.await;
    }
}

#[tokio::test]
async fn pbc_ping_put_get_del_round_trip() {
    let mut h = Harness::start().await;

    exchange_ping(&mut h.reader, &mut h.writer).await;
    exchange_put(&mut h.reader, &mut h.writer).await;
    exchange_get(&mut h.reader, &mut h.writer).await;
    exchange_del(&mut h.reader, &mut h.writer).await;

    // The substrate's accounting should have ticked once per
    // K/V op (Put, Get, Del). Ping does not reach the datastore.
    assert_eq!(h.ds.dispatch_count(), 3);

    h.shutdown().await;
}

#[tokio::test]
async fn pbc_new_ops_round_trip() {
    let mut h = Harness::start().await;

    exchange_server_info(&mut h.reader, &mut h.writer).await;
    exchange_get_bucket(&mut h.reader, &mut h.writer).await;
    exchange_set_bucket(&mut h.reader, &mut h.writer).await;
    exchange_list_buckets_empty(&mut h.reader, &mut h.writer).await;
    exchange_list_keys_empty(&mut h.reader, &mut h.writer).await;
    exchange_index_unsupported(&mut h.reader, &mut h.writer).await;

    // None of the new ops route through the substrate dispatch
    // path: server-info and bucket-props get/set reply directly,
    // list-buckets / list-keys stream from MemoryDatastore's
    // listing index without going through `dispatch`, and the
    // unsupported 2i op short-circuits with an error frame.
    assert_eq!(h.ds.dispatch_count(), 0);

    h.shutdown().await;
}

#[tokio::test]
async fn pbc_list_keys_streams_multiple_frames() {
    // 1000 keys -> 4 chunked frames (3 full of 256 + 1 partial of
    // 232) + 1 empty done=true terminator = 5 frames total.
    let mut h = Harness::start().await;

    for i in 0..1000u16 {
        h.ds.insert(b"users", format!("k{i:04}").as_bytes());
    }

    let req = RpbListKeysReq {
        bucket: b"users".to_vec(),
        ..RpbListKeysReq::default()
    };
    write_req(&mut h.writer, MessageCode::ListKeysReq, req.encode_to_vec()).await;

    let mut frames = Vec::new();
    loop {
        let f = read_frame(&mut h.reader).await.expect("read list frame");
        assert_eq!(f.code, MessageCode::ListKeysResp.as_u8());
        let resp = RpbListKeysResp::decode(f.body.as_slice()).expect("decode list-keys");
        let done = resp.done == Some(true);
        frames.push(resp);
        if done {
            break;
        }
    }
    assert_eq!(frames.len(), 5, "expected 4 chunks plus a terminator");
    let total_keys: usize = frames.iter().map(|f| f.keys.len()).sum();
    assert_eq!(total_keys, 1000);
    let last = frames.last().expect("final frame");
    assert!(last.keys.is_empty());
    assert_eq!(last.done, Some(true));

    h.shutdown().await;
}

#[tokio::test]
async fn pbc_index_streams_chunks_against_scripted_datastore() {
    // Wire path: drive `RpbIndexReq` against a NoxuDatastore-style
    // mock whose `riak_index_eq` returns 1000 keys. The stream
    // must arrive as 4 key-bearing chunks plus one body-less
    // terminator (mirrors the list-keys chunking precedent).
    use dynomite::embed::hooks::{BoxFuture, DatastoreError, Protocol};
    use dynomite::msg::{Msg, MsgType};

    struct ScriptedIndex {
        keys: Vec<Vec<u8>>,
    }
    impl Datastore for ScriptedIndex {
        fn protocol(&self) -> Protocol {
            Protocol::Custom
        }
        fn dispatch(&self, req: Msg) -> BoxFuture<'_, Result<Msg, DatastoreError>> {
            Box::pin(async move {
                let mut rsp = Msg::new(req.id(), MsgType::Unknown, false);
                rsp.set_parent_id(req.id());
                Ok(rsp)
            })
        }
        fn riak_index_eq<'a>(
            &'a self,
            _bucket: &'a [u8],
            _index_name: &'a [u8],
            _value: &'a [u8],
        ) -> BoxFuture<'a, Result<Vec<Vec<u8>>, DatastoreError>> {
            let keys = self.keys.clone();
            Box::pin(async move { Ok(keys) })
        }
    }

    let mut keys = Vec::new();
    for i in 0..1000u16 {
        keys.push(format!("k{i:04}").as_bytes().to_vec());
    }
    let ds: Arc<dyn Datastore> = Arc::new(ScriptedIndex { keys });
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(serve_pbc(listener, ds));
    tokio::time::sleep(Duration::from_millis(10)).await;

    let stream = TcpStream::connect(addr).await.expect("connect");
    let (mut r, mut w) = tokio::io::split(stream);

    let req = RpbIndexReq {
        bucket: b"users".to_vec(),
        index: b"age_int".to_vec(),
        qtype: INDEX_QUERY_TYPE_EQ,
        key: Some(b"42".to_vec()),
        ..RpbIndexReq::default()
    };
    write_frame(
        &mut w,
        &Frame::new(MessageCode::IndexReq.as_u8(), req.encode_to_vec()),
    )
    .await
    .expect("write");

    let mut frames = Vec::new();
    loop {
        let f = read_frame(&mut r).await.expect("frame");
        assert_eq!(f.code, MessageCode::IndexResp.as_u8());
        let resp = RpbIndexResp::decode(f.body.as_slice()).expect("decode");
        let done = resp.done == Some(true);
        frames.push(resp);
        if done {
            break;
        }
    }
    assert_eq!(frames.len(), 5, "4 chunks plus a terminator");
    let total_keys: usize = frames.iter().map(|f| f.keys.len()).sum();
    assert_eq!(total_keys, 1000);
    let last = frames.last().expect("last");
    assert!(last.keys.is_empty());
    assert_eq!(last.done, Some(true));

    drop(r);
    drop(w);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn pbc_index_first_frame_decodes_for_old_clients() {
    // Backwards compat: a client that ignores the `done` flag and
    // reads exactly one frame still observes a usable first chunk.
    use dynomite::embed::hooks::{BoxFuture, DatastoreError, Protocol};
    use dynomite::msg::{Msg, MsgType};

    struct ScriptedIndex {
        keys: Vec<Vec<u8>>,
    }
    impl Datastore for ScriptedIndex {
        fn protocol(&self) -> Protocol {
            Protocol::Custom
        }
        fn dispatch(&self, req: Msg) -> BoxFuture<'_, Result<Msg, DatastoreError>> {
            Box::pin(async move {
                let mut rsp = Msg::new(req.id(), MsgType::Unknown, false);
                rsp.set_parent_id(req.id());
                Ok(rsp)
            })
        }
        fn riak_index_eq<'a>(
            &'a self,
            _bucket: &'a [u8],
            _index_name: &'a [u8],
            _value: &'a [u8],
        ) -> BoxFuture<'a, Result<Vec<Vec<u8>>, DatastoreError>> {
            let keys = self.keys.clone();
            Box::pin(async move { Ok(keys) })
        }
    }

    let mut keys = Vec::new();
    for i in 0..600u16 {
        keys.push(format!("k{i:04}").as_bytes().to_vec());
    }
    let ds: Arc<dyn Datastore> = Arc::new(ScriptedIndex { keys });
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(serve_pbc(listener, ds));
    tokio::time::sleep(Duration::from_millis(10)).await;

    let stream = TcpStream::connect(addr).await.expect("connect");
    let (mut r, mut w) = tokio::io::split(stream);
    let req = RpbIndexReq {
        bucket: b"users".to_vec(),
        index: b"x".to_vec(),
        qtype: INDEX_QUERY_TYPE_EQ,
        key: Some(b"v".to_vec()),
        ..RpbIndexReq::default()
    };
    write_frame(
        &mut w,
        &Frame::new(MessageCode::IndexReq.as_u8(), req.encode_to_vec()),
    )
    .await
    .expect("write");
    let f = read_frame(&mut r).await.expect("frame");
    assert_eq!(f.code, MessageCode::IndexResp.as_u8());
    let parsed = RpbIndexResp::decode(f.body.as_slice()).expect("decode");
    assert_eq!(parsed.keys.len(), 256);
    assert_eq!(parsed.done, Some(false));

    drop(r);
    drop(w);
    server.abort();
    let _ = server.await;
}

async fn write_req(w: &mut Writer, code: MessageCode, body: Vec<u8>) {
    write_frame(w, &Frame::new(code.as_u8(), body))
        .await
        .expect("write req");
}

async fn read_resp(r: &mut Reader, expected: MessageCode) -> Frame {
    let frame = read_frame(r).await.expect("read resp");
    assert_eq!(
        frame.code,
        expected.as_u8(),
        "unexpected response code: got {}, want {}",
        frame.code,
        expected.as_u8()
    );
    frame
}

async fn exchange_ping(r: &mut Reader, w: &mut Writer) {
    write_req(w, MessageCode::PingReq, Vec::new()).await;
    let resp = read_resp(r, MessageCode::PingResp).await;
    assert!(resp.body.is_empty());
}

async fn exchange_put(r: &mut Reader, w: &mut Writer) {
    let req = RpbPutReq {
        bucket: b"users".to_vec(),
        key: Some(b"alice".to_vec()),
        value: b"hello".to_vec(),
        ..RpbPutReq::default()
    };
    write_req(w, MessageCode::PutReq, req.encode_to_vec()).await;
    let resp = read_resp(r, MessageCode::PutResp).await;
    let parsed = RpbPutResp::decode(resp.body.as_slice()).expect("decode put");
    assert!(parsed.content.is_empty());
    assert!(parsed.vclock.is_none());
}

async fn exchange_get(r: &mut Reader, w: &mut Writer) {
    let req = RpbGetReq {
        bucket: b"users".to_vec(),
        key: b"alice".to_vec(),
        ..RpbGetReq::default()
    };
    write_req(w, MessageCode::GetReq, req.encode_to_vec()).await;
    let resp = read_resp(r, MessageCode::GetResp).await;
    let parsed = RpbGetResp::decode(resp.body.as_slice()).expect("decode get");
    // The MemoryDatastore-backed server replies with an empty
    // content list; full sibling materialisation lands with the
    // RiakObject schema in a follow-up slice.
    assert!(parsed.content.is_empty());
}

async fn exchange_del(r: &mut Reader, w: &mut Writer) {
    let req = RpbDelReq {
        bucket: b"users".to_vec(),
        key: b"alice".to_vec(),
        ..RpbDelReq::default()
    };
    write_req(w, MessageCode::DelReq, req.encode_to_vec()).await;
    let resp = read_resp(r, MessageCode::DelResp).await;
    assert!(resp.body.is_empty());
}

async fn exchange_server_info(r: &mut Reader, w: &mut Writer) {
    let req = RpbServerInfoReq::default();
    write_req(w, MessageCode::ServerInfoReq, req.encode_to_vec()).await;
    let resp = read_resp(r, MessageCode::GetServerInfoResp).await;
    let info = RpbGetServerInfoResp::decode(resp.body.as_slice()).expect("decode info");
    assert!(info.node.is_some(), "node must be advertised");
    let version = info.server_version.expect("version present");
    let version_str = std::str::from_utf8(&version).expect("ascii version");
    assert!(
        version_str.starts_with("dyn-riak "),
        "version string must be branded: got {version_str}"
    );
}

async fn exchange_get_bucket(r: &mut Reader, w: &mut Writer) {
    let req = RpbGetBucketReq {
        bucket: b"users".to_vec(),
        ..RpbGetBucketReq::default()
    };
    write_req(w, MessageCode::GetBucketReq, req.encode_to_vec()).await;
    let resp = read_resp(r, MessageCode::GetBucketResp).await;
    let parsed = RpbGetBucketResp::decode(resp.body.as_slice()).expect("decode get-bucket");
    let props = parsed.props.expect("props present");
    assert_eq!(props.n_val, Some(3));
    assert_eq!(props.allow_mult, Some(false));
}

async fn exchange_set_bucket(r: &mut Reader, w: &mut Writer) {
    let req = RpbSetBucketReq {
        bucket: b"users".to_vec(),
        props: Some(RpbBucketProps {
            n_val: Some(5),
            allow_mult: Some(true),
            ..RpbBucketProps::default()
        }),
        ..RpbSetBucketReq::default()
    };
    write_req(w, MessageCode::SetBucketReq, req.encode_to_vec()).await;
    let resp = read_resp(r, MessageCode::SetBucketResp).await;
    let _ = RpbSetBucketResp::decode(resp.body.as_slice()).expect("decode set-bucket");
}

async fn exchange_list_buckets_empty(r: &mut Reader, w: &mut Writer) {
    // MemoryDatastore with no inserts streams a single done=true
    // empty frame.
    let req = RpbListBucketsReq::default();
    write_req(w, MessageCode::ListBucketsReq, req.encode_to_vec()).await;
    let resp = read_resp(r, MessageCode::ListBucketsResp).await;
    let parsed = RpbListBucketsResp::decode(resp.body.as_slice()).expect("decode list-buckets");
    assert_eq!(parsed.done, Some(true));
    assert!(parsed.buckets.is_empty());
}

async fn exchange_list_keys_empty(r: &mut Reader, w: &mut Writer) {
    let req = RpbListKeysReq {
        bucket: b"users".to_vec(),
        ..RpbListKeysReq::default()
    };
    write_req(w, MessageCode::ListKeysReq, req.encode_to_vec()).await;
    let resp = read_resp(r, MessageCode::ListKeysResp).await;
    let parsed = RpbListKeysResp::decode(resp.body.as_slice()).expect("decode list-keys");
    assert_eq!(parsed.done, Some(true));
    assert!(parsed.keys.is_empty());
}

async fn exchange_index_unsupported(r: &mut Reader, w: &mut Writer) {
    let req = RpbIndexReq {
        bucket: b"users".to_vec(),
        index: b"city_bin".to_vec(),
        qtype: INDEX_QUERY_TYPE_EQ,
        key: Some(b"seattle".to_vec()),
        ..RpbIndexReq::default()
    };
    write_req(w, MessageCode::IndexReq, req.encode_to_vec()).await;
    let resp = read_resp(r, MessageCode::ErrorResp).await;
    let err = RpbErrorResp::decode(resp.body.as_slice()).expect("decode err");
    assert!(!err.errmsg.is_empty());
}
