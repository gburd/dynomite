# SWIM + Lifeguard membership prototype (behind a flag)

Date: 2026-07-05
Branch: `proto/swim` (off `main` @ `2edfd65`)
Worktree: `/home/gburd/ws/wt-swim`

## Goal

Prototype SWIM + Lifeguard membership / failure detection as an
opt-in ALTERNATIVE to the current Dynamo-style gossip + phi-accrual,
validated by a DST model so it is ready if the large-scale churn
test shows the current gossip struggling. SWIM is aimed at faster
convergence and far fewer false-positive peer-downs as clusters grow
and churn. The existing phi-accrual path is left completely intact;
SWIM is opt-in until the churn data justifies a default cutover.

## What was built

### 1. Pure state machine + I/O shell: `crates/dynomite/src/cluster/swim.rs`

Two-layer per the DST discipline:

* **`SwimState`** -- the pure, synchronous, deterministic protocol
  logic. No tokio, no sockets, no wall clock; it takes explicit
  inputs (a probe outcome, a piggybacked membership update, a logical
  tick) and returns explicit `(peer_idx, PeerState)` transitions.
  This is exactly the shape the model checker drives, and it is why
  the accuracy / completeness invariants can be model-checked
  deterministically. It implements:
  * Probe / ping-req outcomes (`ProbeResult::{Acked, IndirectAcked,
    Failed}`): `Acked` clears suspicion and decays nack score;
    `IndirectAcked` (target alive but our direct path failed) raises
    the Lifeguard nack score and keeps the target alive; `Failed`
    begins / reinforces suspicion.
  * Suspect -> confirm state machine with **incarnation numbers**:
    every node owns a monotone incarnation; learning it is suspected
    makes it bump its incarnation and re-broadcast `Alive`, which
    overrides the suspicion everywhere (refutation). A dead node
    cannot refute, so a genuine death confirms.
  * **Infection-style dissemination** merge (`on_update`): higher
    incarnation always wins; at equal incarnation the worse belief
    wins (Dead > Suspect > Alive) so suspicion spreads; a same-rank
    Suspect merge accumulates the dogpile suspector count.
  * **Lifeguard**: self-awareness (nack score dilates the local
    suspicion timeout -- a slow observer waits longer before
    convicting anyone), dogpile (more independent suspectors shrink
    the confirm deadline), and buddy-system nack (the indirect-ack
    path feeds the nack score).

* **`SwimHandler`** -- the tokio-facing I/O shell. Owns `SwimState`
  behind a `Mutex`, converts wall time into logical protocol ticks,
  and reconciles transitions onto the shared `ServerPool`. Its
  `evaluate(now)` returns the SAME `Vec<(u32, PeerState)>` shape as
  `GossipHandler::evaluate`, so dispatch and hinted handoff are
  unchanged regardless of which backend is selected.

The `Status::{Alive, Suspect, Dead}` -> `PeerState` projection maps
Alive -> Normal and both Suspect and Dead -> Down (a suspect is
pulled from routing immediately; a refutation restores it to Normal,
exactly as phi-accrual's transient down/up does). The DST accuracy
invariant is about the *confirmed-dead* status, not the transient
suspect window.

### 2. The flag: `membership: swim | gossip` (default `gossip`)

* New `conf::Membership` enum (mirrors `conf::Transport`), field
  `membership: Option<Membership>` on `ConfPool`, defaulted to
  `Membership::Gossip` in `apply_hinted_handoff_defaults`.
* Wiring in `crates/dynomited/src/server.rs` at the gossip-handler
  construction point: `match conf_pool.membership` selects the
  backend. `gossip` keeps the phi-accrual handler as the live
  mutator (unchanged). `swim` constructs a `SwimHandler` and logs the
  selection. The phi-accrual path is not ripped out.

### 3. Downstream unchanged

`SwimState`/`SwimHandler` emit the same `(peer_idx, PeerState)`
transitions the engine already consumes, wired to the same
`PeerState` on the same `ServerPool.peers()` table. Dispatch and
hinted handoff see no difference.

## DST + Elle gate

### DST model: `crates/model-tests/src/swim.rs` (in `scripts/model.sh`)

Two shapes, both self-contained (per the crate's no-production-dep
doctrine; the model re-expresses the decision logic exactly as
`gossip.rs` does):

* **`SwimDissemination`** (stateright `Model`, exhaustive BFS):
  infection-style dissemination with refutation over a connected node
  set. Asserts CONVERGENCE (all nodes agree at quiescence) and that
  refutation / suspicion each reach all nodes. Mirrors the `gossip.rs`
  convergence model with the SWIM merge precedence.

* **`sim`** (deterministic seeded simulations, 64 seeds each) driving
  the re-expressed SWIM + Lifeguard detector over a lossy/delaying
  network with genuinely-dead and merely-slow nodes and slow
  observers. The four required invariants:
  * **Completeness** (`completeness_dead_node_confirmed_by_all`): a
    genuinely-dead node is eventually confirmed dead by every live
    observer.
  * **Accuracy / low false-positive**
    (`accuracy_slow_node_not_permanently_dead`): a merely-slow-but-
    alive node is NEVER permanently confirmed dead (incarnation
    refutation + Lifeguard dilation prevent it): 0 dead observers at
    the final tick across all seeds.
  * **Comparative** (`comparative_fewer_false_positives_than_fixed_timeout`):
    under the IDENTICAL slow-node schedule (same seeds, same drop
    rates), SWIM + Lifeguard produces strictly FEWER false positives
    than a naive fixed-timeout detector. SWIM: zero peak false-
    positive convictions; naive fixed-timeout: strictly more.
  * **Negative control** (`negative_control_no_refutation_causes_false_death`):
    with incarnation refutation DISABLED, the checker CATCHES a
    live-but-slow node being falsely and permanently declared dead.
    This proves the accuracy invariant has teeth: if disabling
    refutation did NOT reproduce a false death, the accuracy
    assertion would be vacuous.
  * Plus a sanity floor (`healthy_node_never_declared_dead`): a
    healthy node is never declared dead under either detector.

`scripts/model.sh` runs `cargo test -p model-tests`, which picks up
the new `swim` module automatically (23 model tests total, 7 SWIM).

### hegel property tests: `crates/dynomite/tests/swim_property.rs`

* `incarnation_monotonicity_refutes_stale_suspicions` +
  `higher_incarnation_suspicion_takes_effect`: incarnation
  monotonicity -- a refutation at incarnation `r` makes any stale
  suspicion at incarnation `< r` a no-op, while a suspicion at
  incarnation `> r` still applies (refutation is not permanent).
* `confirm_deadline_math_is_monotone`: the suspect -> confirm timeout
  math -- deadline is >= 1 period past the suspicion start, never
  shortened by nack-score dilation, never lengthened by dogpile
  suspectors.
* `dissemination_converges_on_connected_graph`: pushing one node's
  fact round-robin over a fully-connected set of state machines
  converges (every node agrees); an Alive fact pins Normal
  everywhere.

### Elle / consistency-harness

Elle list-append does not apply to membership (there is no
per-key record history to check). Membership correctness is
validated by the DST model + the property tests, as documented in
the brief. This is recorded here deliberately, not skipped.

## Gates (all green)

* `cargo build -p dynomite-engine --all-targets --locked` -- ok
* `cargo build -p dynomite-engine` (no openblas features) -- ok
* `cargo build -p model-tests --locked` -- ok
* `cargo nextest run -p dynomite-engine` -- 1330 passed, 1 skipped
* `cargo test -p model-tests` (`scripts/model.sh`) -- 23 passed
  (7 SWIM)
* `cargo test --doc -p dynomite-engine` (swim + enums) -- ok
* `cargo clippy -p dynomite-engine -p model-tests --all-targets
  -- -D warnings` -- clean
* `cargo fmt -p dynomite-engine -p model-tests -- --check` -- clean
* `cargo build -p dynomited` + `cargo clippy -p dynomited` -- clean

## Tests added

* 8 unit tests in `swim.rs` (state-machine behaviour).
* 4 hegel property tests (256 cases each).
* 7 DST model tests (2 stateright BFS + 5 seeded simulations).
* 4 conf enum tests (`Membership` parse / default / YAML round-trip).
* Total: 23 new tests.

## Honest limitations (what a production cutover from phi-accrual needs)

1. **The SWIM probe loop is not wired into the run loop.** The
   `SwimHandler` is constructed when `membership: swim` is selected
   and logs the selection, but the tokio task that actually sends
   PINGs, waits for acks with a timeout, fans out PING-REQ indirect
   probes to `k` members, and piggybacks membership updates on the
   ack traffic is NOT yet implemented. Until it is, the phi-accrual
   loop remains the live `PeerState` mutator even under
   `membership: swim`. The state machine and its inputs
   (`on_probe` / `on_update` / `evaluate`) are complete and tested;
   the socket driver that feeds them is the main cutover gap.

2. **No dnode wire format for SWIM messages.** SWIM PING / ACK /
   PING-REQ frames and the piggybacked update list need a dnode
   message encoding + parser (and a fuzz target) before the wire
   protocol is production-safe.

3. **Membership bootstrap / join is unmodelled.** The state machine
   assumes a known member set at construction. Integration with the
   seeds provider, join / leave, and IP-replacement handling
   (which the gossip path handles today) is not done.

4. **`k` (indirect_probes) and the suspicion-timeout constants are
   defaults, not tuned.** The Lifeguard paper scales the suspicion
   timeout logarithmically with cluster size; the prototype uses a
   fixed base. The large-scale churn test should drive the tuning
   before a default cutover.

5. **Metrics / events not wired.** The gossip handler emits
   `peer_state_transitions_total`, `peer_state_current`, and
   `ClusterEvent::{PeerUp, PeerDown}`. `SwimHandler` reconciles
   transitions but does not yet emit these; parity is needed before
   cutover so operators see the same observability.

6. **The DST simulations model dissemination as an idealised
   connected broadcast per period.** Real SWIM dissemination is
   piggybacked and gossip-bounded; the model's convergence claim is
   about the merge logic, not the dissemination latency. The
   stateright `SwimDissemination` model covers the merge / refutation
   convergence over an arbitrary connected push schedule, which is
   the correctness-relevant part; end-to-end dissemination latency
   under load is a benchmark question, not a DST invariant.

Given (1)-(6), SWIM here is a qualified prototype: the protocol
decision logic is real, deterministically tested, and gated, and the
flag is in place -- but it is not yet a drop-in replacement for the
phi-accrual production path. It is ready to promote when the churn
data justifies building the probe loop and wire format.
