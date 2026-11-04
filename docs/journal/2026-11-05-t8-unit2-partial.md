# 2026-11-05 T8 unit 2 (partial): repair-script request + replica execution

Branch: work/t8 (worktree /scratch/dyn-wt/t8).

## Landed

* bbb0201 `proto::redis::repair::request`: `ScriptRequest` (encode /
  decode of the `DYNOMITE.REPAIR-SCRIPT` RESP array; `from_request`
  reproduces post_parse_msg + post_parse_optional_args field/value/opt
  extraction from proto_cmd_info), `cmd_info` (the 13 rows' repair
  columns), `ScriptRun` (replays a logic.rs script one datastore command
  at a time), `encode_command` / `encode_reply` / `parse_reply`.
* 4994046 dynomited `run_one_backend_conn`: recognises the request,
  drains pending pipelined replies, runs the script on the same
  connection, replies with the script's RESP. Test
  `crates/dynomited/tests/repair_script_backend.rs` (real valkey-server).

## One connection per node to its datastore

`backend_supervisor` keeps exactly one connection (server.rs ~579); the
library `net::server::ServerConn` has no production caller (only
tests/stage_09_net.rs), so only the dynomited driver needed the change.

## Not done (next units)

1. Coordinator rewrite: stamp ts (ms) at client receive when
   `is_read_repairs_enabled()` and replace the forwarded bytes with
   `ScriptRequest::from_request(..).encode()` in
   `ClusterDispatcher::dispatch` / `dispatch_replicas`.
2. The peer side must accept the request: `dnode_client` parses peer
   payloads with `redis_parse_req`; `DYNOMITE.REPAIR-SCRIPT` is not in
   `commands::lookup`, so it will fail to parse. Needs a lookup entry
   (routing LocalNodeOnly, key = 3rd element) or a pre-parse check.
3. The in-process `local_datastore` hook path (embed `Datastore`) does
   not interpret script requests.
4. Coalescer: find_most_updated_rsp, repair write, metadata stripping,
   fallback to checksum on stripped replies; remove
   `schedule_read_repair` (vote-count repair, DEV-R1).
5. Cleanup scripts after all replicas answer a delete.
6. Runtime flag (msg/mod.rs OnceLock -> AtomicBool) and the two stats
   endpoints; un-ignore func_test_safe_quorum_read_repair; DEV-R1
   concurrency scenario; stateright LWW model; ledger rows.

## Note (not a deviation)

C sets rewrite_with_ts_possible=false for arguments split across mbufs
and sends the original command; Rust holds parsed arguments
contiguously, so the rewrite always applies.
