# Fork review: Netflix/dynomite

Survey of every meaningful fork of `Netflix/dynomite` against our
Rust port. 834 forks total; 22 pushed since upstream's last
commit (2024-05-20); only ~6 carry distinct work. Findings
sorted by potential impact on the Rust port.

## Summary table

| Fork                | Status         | Theme                          | Already in our port? |
|---------------------|----------------|--------------------------------|----------------------|
| `lampmanyao`        | 1 ahead        | **Redis AUTH (requirepass)**   | **NO - real gap**    |
| `DynomiteDB:topology`     | 71 ahead | full multithreading rewrite    | tokio handles it     |
| `DynomiteDB:ssl`/`:ssl2`  | 4-2 ahead | TLS over TCP for peer plane    | partial (QUIC only)  |
| `DynomiteDB:entropy`      | 22 ahead | anti-entropy reconciliation    | yes (entropy module) |
| `DynomiteDB:cryptowork`   | 1 ahead  | crypto refactor (no globals)   | yes (Rust ownership) |
| `DynomiteDB:memcacheFix`  | 1 ahead  | memcache parser corrections    | yes (we tested it)   |
| `DynomiteDB:perf`         | 5 ahead  | latency timing instrumentation | partial (stats hist) |
| `DynomiteDB:data_recon`   | 2 ahead  | mmap'd data file               | no (and not needed)  |
| `DynomiteDB:fix-info-json`| 0 ahead  | (merged upstream)              | yes                  |
| `DynomiteDB:hotfix/gossip_crash` | 0 ahead | (merged upstream)         | n/a (gossip deferred) |
| `elijah:master`     | 3 ahead, 450 behind | HGETALL/HKEYS routing     | yes (merged upstream)|
| `isakrubin`         | 2 ahead        | C `volatile struct` build fix  | n/a (Rust)           |
| `noullove`          | 1 ahead        | CI workflow tweak              | n/a                  |
| `ipapapa`           | 9 ahead        | MongoDB support (2015)         | n/a (out of scope)   |

## Gap analysis

### 1. Redis AUTH — `lampmanyao/dynomite`  **(P1 - real gap)**

Adds a `redis_requirepass: <password>` pool config option. When
set, every server-side connection sends `AUTH <password>` as its
first command after connect; the response `+OK\r\n` is consumed
silently before user traffic flows.

Our `crates/dynomited/src/server.rs::backend_supervisor` opens the
TCP connection but does not authenticate. Any password-protected
Redis (which is the default for production deployments) will
reject every subsequent command with `NOAUTH Authentication
required`.

**What to add**:
- `ConfPool::requirepass: Option<String>` (or per-`ConfServer`).
- In `backend_supervisor` after the TCP handshake but before
  draining `rx`, write `*2\r\n$4\r\nAUTH\r\n$<n>\r\n<pw>\r\n`,
  read one reply line, error out on non-`+OK`.
- Same on the dnode peer plane if peers themselves require auth
  (`redis_requirepass` is per-server, not per-peer in the C code).
- Memcache equivalent: `binary AUTH` is more involved; defer.

**Estimated effort**: ~80 LOC + 2 unit tests + 1 integration test
spawning `redis-server --requirepass`. Half a day.

### 2. TCP+TLS for the peer plane — `DynomiteDB:ssl`  **(P2 - future)**

Our QUIC transport already provides TLS 1.3 end-to-end. Operators
who can't enable QUIC (UDP blocked, kernel-level QUIC unavailable,
peers on different transport versions) will still want encrypted
TCP for cross-DC peer traffic. The DynomiteDB:ssl branch added
~700 LOC of OpenSSL plumbing for exactly this.

**What to add (not now)**: a `tokio-rustls` wrapper transport in
`crate::io::reactor` next to `TcpTransport`, plus a config option
`secure_server_option` (the C config field name, currently parsed
but unused) that selects plain / tls / quic.

**Estimated effort**: 2-3 days of careful work.

### 3. Multithreading — `DynomiteDB:topology`  **(no action)**

71 commits, 79 files, ~3000 LOC. Reorganizes the C engine to use
per-peer pthread contexts, spinlocks for connection / message /
mbuf queues, and a thread-local datastore connection. Tokio's
multi-threaded scheduler gives us this with no code; our
`backend_supervisor` and `peer_supervisor` already run on the
shared executor. The branch's other valuable bits (separate
"topology" struct for DC > rack > node hierarchy; "peers have
only one connection") are already in our pass-2 design.

**Verdict**: zero action required; we're better off than the
fork.

### 4. Static-state crypto refactor — `DynomiteDB:cryptowork`  **(no action)**

Removes static globals (`aes_cipher`, `aes_key`, `aes_encrypt_ctx`)
in favor of per-operation contexts. Our Rust crypto module
already passes `Crypto` / cipher state by ownership rather than
relying on file-scope statics, so we have this fix structurally.

The fork also fixes a `unsigned char file_name[]` -> `char file_name[]`
sign-mismatch bug for `fopen(3)`. Our port uses `&Path` so the bug
cannot occur.

### 5. Memcache parser fixes — `DynomiteDB:memcacheFix`  **(no action)**

Adds:
- empty-key detection in `SW_KEY` (`keylen == 0` -> error)
- `narg++` increments at command-name and key boundaries
- `enomem` error label for OOM during parse
- `memcache_post_coalesce` actually appends `END\r\n` for
  fragmented multi-key requests

Our `crates/dynomite/src/proto/memcache/parser.rs` already
implements:
- the empty-key check (`keylen == 0` branch in `SW_KEY`)
- `narg` accounting via the parser state machine
- OOM is impossible by construction (Vec growth panics; tested
  via `crates/fuzz/`)
- `memcache_post_coalesce` is implemented in `coalesce.rs:77`

**Verdict**: confirmed parity, no action.

### 6. Hash-command routing — `elijah:master`  **(no action)**

Three hotfixes for `HGETALL`, `HKEYS`, `HSCAN` to use
`ROUTING_TOKEN_OWNER_LOCAL_RACK_ONLY` instead of
`ROUTING_LOCAL_NODE_ONLY` (which would otherwise return stale data
on the local node when the key's owner is another rack-local
peer). All three were merged into `Netflix/dynomite/dev` and made
it into our port:

```
$ grep -B1 -A4 '"hgetall"' crates/dynomite/src/proto/redis/commands.rs
b"hgetall" => (MsgType::ReqRedisHgetall, true, false, RoutingOverride::TokenOwnerLocalRackOnly),
```

### 7. C-language build break — `isakrubin`  **(n/a)**

Fixes a `volatile struct C2G_InQ` vs `extern struct C2G_InQ`
declaration ambiguity that newer GCC rejects. Rust's type system
makes this category of error unrepresentable.

### 8. CI / docs / packaging — `noullove`, `ramezrafla`, `kalangi07`, `thaingo`, `andypeng2015`, `rinhtrinh1511`, `rprevot`, `SAY-5`, `jparap`, `ochinchina`  **(n/a)**

All recent forks with `ahead=0..1` are CI workflow tweaks,
README edits, or out-of-tree config samples. Nothing for us
to port.

### 9. MongoDB protocol — `ipapapa`  **(out of scope)**

Ioannis Papapanagiotou (one of the original Dynomite authors)
added a MongoDB backend in 2015. The work was never merged to
upstream and the protocol layer would be a substantial new
module. Our scope is Redis + memcache to match upstream.

## Recommendation

**Do**:
1. Add Redis AUTH support (`redis_requirepass`). This is a real
   functional gap.
2. Defer TCP+TLS until QUIC adoption is observed; track as a
   pass-3+ feature.

**Don't**:
3. Anything else from the surveyed forks. Every other concrete
   change is either (a) already in our port via upstream merges,
   (b) made obsolete by Rust+Tokio, or (c) out of our scope.

## Methodology footnote

Survey method: GitHub `forks` API for the full list, then
compare API for divergence vs `Netflix:dev`, then `git clone`
per fork for code review. 4 forks (compared via the API)
were `ahead=0,behind>0` (passive mirrors with stale upstream
state); the rest covered above.
