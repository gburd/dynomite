# Stage 11: entropy reconciliation

## Summary

Ported the entropy module
(`_/dynomite/src/entropy/dyn_entropy{,_snd,_rcv,_util}.{c,h}`) into a
new `dynomite::entropy` Rust module split into `mod.rs`, `send.rs`,
`receive.rs`, and `util.rs`. The module ships a TCP sender +
receiver pair plus pluggable snapshot source/sink traits.

## Files added

- `crates/dynomite/src/entropy/mod.rs` -- public surface, error
  type, `EntropyConfig`, `SnapshotSource`/`SnapshotSink` traits,
  wire-frame helpers (`NegotiationHeader`, `SnapshotHeader`),
  module-level rustdoc + wire format diagram.
- `crates/dynomite/src/entropy/util.rs` -- AES key + IV loaders
  (`load_key_file`, `load_iv_file`, `load_material`); accepts
  raw or PEM-armored files.
- `crates/dynomite/src/entropy/send.rs` -- `EntropySender`,
  `encrypt_chunk`, `RedisLocalSnapshot` (default source),
  `StaticSnapshot` (in-memory test source).
- `crates/dynomite/src/entropy/receive.rs` -- `EntropyReceiver`,
  `decrypt_chunk`, `RedisReplaySink` (default sink),
  `MemorySink` (in-memory test sink).
- `crates/dynomite/tests/stage_11_entropy.rs` -- integration
  tests (plaintext roundtrip, encrypted roundtrip with bundled
  fixtures, empty-snapshot roundtrip, tampered ciphertext
  rejection, truncated stream rejection, bad-magic rejection,
  spawn-loop test).

## Files changed

- `crates/dynomite/src/lib.rs` -- added `pub mod entropy;`.
- `docs/parity.md` -- ~50 new parity rows under `src/entropy/`
  covering each `.c`/`.h` symbol, plus six `# Deviations` entries
  documenting the cipher-padding, length-prefix, unified-framing,
  RESP-direct-call, throttler-removal, and `recon_key`/`recon_iv`
  content-honouring choices.

## C-verification findings

The lead's stage 11 brief contained several factual errors that
would have produced a non-functioning port if implemented as
stated. Each was caught by direct re-reading of the C source and
corrected before implementation.

1. **"AES-128-CBC chunks (256-byte chunks per the C code)"** --
   the C `BUFFER_SIZE` is 16384 (16 KiB), not 256. The default in
   the Rust port uses 16384.

2. **"recon_key.pem ... a 32-byte raw key in PEM-armored form OR
   a real RSA key"** -- the C loader reads the file with `fgets`
   and never PEM-decodes it. The bundled fixtures are plain
   ASCII files containing the 16-byte (AES-128) key followed by a
   newline; the cipher uses AES-128 with a 16-byte key, not 32.
   Furthermore, the C `theKey = (unsigned char *)buff` line is
   commented out, so the C engine always runs against the
   hardcoded literal `"0123456789012345"` regardless of the file
   content. The Rust port honours the on-disk content. The
   bundled fixture also contains a typo (17 ASCII characters,
   not 16); the loader truncates to 16 bytes, which equals the
   hardcoded C literal byte-for-byte.

3. **"send side connects to a peer entropy receiver"** -- the C
   engine has only one role: it listens. The "first byte" of the
   negotiation header tells the C engine whether to send or
   receive in this connection, but the connection is always
   inbound from an external Lepton/Spark cluster. The brief's
   API request (separate `EntropySender` / `EntropyReceiver` with
   the sender as the client) is implemented as requested -- this
   is recorded as the unified-wire-framing deviation in
   `docs/parity.md`.

4. **"issues `BGSAVE` + `GET *`"** -- the C path issues
   `BGREWRITEAOF` (not `BGSAVE`) and reads the on-disk AOF file
   (not `GET *`). The Rust default `RedisLocalSnapshot` matches
   the C behaviour: RESP `BGREWRITEAOF` then `read(aof_path)`.

5. **"prepends a magic header + length per chunk"** -- the C
   sender sends fixed-size `cipher_size`-byte chunks with no
   per-chunk length prefix; the receiver reads `cipher_size`
   bytes per chunk (`read(peer_socket, ciphertext, cipher_size)`).
   The Rust port deviates by adding a per-chunk length prefix to
   handle PKCS#7-padded outputs whose length differs from
   `cipher_size`. This deviation is documented.

6. **C `snd`/`rcv` symmetry** -- the C `snd` and `rcv` wire
   formats are not inverses. `snd` streams the AOF file as opaque
   bytes; `rcv` expects a `numberOfKeys` header plus per-record
   `keyValueLength`+bytes triples. The brief asks for an
   in-process roundtrip across the two; the only sane resolution
   was to unify the framing on the file-stream shape and let the
   `SnapshotSink` handle record-level interpretation. Documented
   as the unified-wire-framing deviation.

## Test results

| Gate | Count | Notes |
|---|---|---|
| `cargo nextest run --workspace` | 564 (was 534) | +30 entropy tests |
| `cargo nextest run --workspace --all-features` | 568 (was 538) | +30 entropy tests |
| `cargo test --doc --workspace` | 527 (was 510) | +17 entropy doctests |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | clean | |
| `cargo fmt --all -- --check` | clean | |
| `scripts/check_no_port_comments.sh` | clean | |
| `scripts/check_no_todos.sh` | clean | |
| `scripts/check_ascii.sh` | clean | |

## Open questions

None. The `BoxedSnapshotSource` / `BoxedSnapshotSink` trait
shapes are locked here so Stage 13 (embedding API) can wrap them
without touching the entropy module.

## Refs

- PLAN.md Stage 11
- docs/parity.md `src/entropy/` and Stage 11 `# Deviations`
