# Response to the 2026-07-23 external review

Date: 2026-07-23
Subject: `~/Downloads/dynomite-rust-review.md`, reviewing release 1.4.1.

The review is high quality and largely correct. It cleanly separates two
kinds of concern: (a) concrete engineering findings that code can fix
now, and (b) maturity properties that are purchased only with production
hours, fleet scale, and independent adversarial scrutiny -- which no code
change can manufacture. This entry records how each actionable finding
was handled. The (b) items are acknowledged as accurate and are not
"addressed" by code because they cannot be; they are positioning /
process items for the maintainer.

## Actionable findings addressed in code

### 1. DynToken saturation -> hard error (review section 4, top priority)

Fixed. `dynomited::server::token_component_to_dyn` previously saturated a
token above `u32::MAX` to `u32::MAX`, which would silently place a node
at a DIFFERENT ring position than configured -- the worst outcome for a
drop-in replacement of a live C cluster whose tokens exceed u32
(Cassandra-style random tokens over the murmur space routinely do). It
now returns a hard `ServerError::BadConfig` and the node refuses to
start. A wrong ring placement is worse than a clear error. Tests
`token_component_above_u32_is_rejected` and
`token_component_at_u32_max_is_accepted`. Parity.md deviation updated.
Arbitrary-precision (`u128`+) token support remains a tracked parity row.

### 2. Peer-plane crypto: authenticated AEAD alongside C-compat CBC (section 4)

Addressed at the primitive layer. The inherited AES-128-CBC with the key
reused as the IV (deterministic, unauthenticated) is kept verbatim for C
interop (byte-pinned test). Added AES-256-GCM primitives
(`crypto::aes::encrypt_to_vec_aead` / `decrypt_to_vec_aead`): full
32-byte key, fresh random 96-bit nonce per message, 128-bit tag,
non-deterministic and tamper/wrong-key detecting. New dependency
`aes-gcm` 0.10 (RustCrypto, MSRV 1.56, same family as `aes`/`cbc`) added
to PLAN.md's crate list. Tests: round-trip, non-determinism,
tamper-detect, wrong-key-reject, too-short-reject.

Not-yet-done and tracked: negotiating the cipher mode in the DNODE
handshake so a cluster defaults to AEAD and falls back to CBC only when a
C peer is present. That is a wire-protocol change with interop and DST
implications; the primitive is in place so the negotiation is a
contained follow-up rather than a rushed peer-protocol edit. Recorded as
parity.md deviation D-crypto.

### 3. Honesty at the README / versioning layer (sections 6, 7)

Addressed. The README now states the Riak non-goals (no `riak_repl`
realtime cross-DC replication, no `riak_ensemble` strong consistency, FT
search is RediSearch-shaped and per-node not Yokozuna/Solr) at the top
level rather than only in the Dyniak chapter, states that mixed C/Rust
clusters are not a tested configuration (whole-cluster replacement only),
and reconciles the "in-progress port" framing with the release cadence
(the version reflects internal milestone cadence, not field-proven
maturity).

### 4. Deferred parity rows audit (section 4)

Audited. See the audit note appended to docs/parity.md; any row still
marked deferred/omitted-for-stage at 1.4.1 is either genuinely complete
in a later matrix row or explicitly re-justified.

## Findings acknowledged but NOT code-fixable this session

* Crate-name collision (`dynomite` import vs softprops' DynamoDB mapper):
  real, but renaming the published import is a breaking ecosystem change
  that needs the maintainer's decision, not a unilateral edit. Noted for
  the maintainer.
* C-interop test rig (mixed C/Rust cluster, DNODE + gossip conformance
  against the v0.6.22 binary): a substantial harness; the README now
  disclaims mixed clusters in the interim.
* Head-to-head benchmarks vs the C engine on identical hardware:
  legitimate table stakes for the parity claim; a benchmark rig is its
  own effort.
* Big-int tokens; distributed FT search fan-out; PBC message coverage
  matrix vs riak_pb; testing against mainstream Riak client libraries:
  each a tracked feature/verification effort.
* Maturity items -- external Jepsen, security audit, multi-week 25-100
  node soaks, power-fail/torn-write matrix for Noxu, a second maintainer,
  a 0.x / production-candidate version designation: these are process and
  time, correctly identified by the review as unearnable by code. They
  are the maintainer's to schedule.

## Bottom line

The two findings the review said to fix before any further release --
token saturation and the crypto inheritance -- are addressed (saturation
fully; crypto at the primitive layer with the negotiation tracked). The
maturity gap the review describes is real and is not claimed to be
closed here.
