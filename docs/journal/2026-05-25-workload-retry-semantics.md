# 2026-05-25 - Workload-driver retry semantics (P3-2.7)

## Summary

The chaos workload driver
(`scripts/chaos-multi-host/workload-driver.py`) used to count
every raised exception as a failure. That conflated three very
different operational classes:

* transient gossip churn surfacing as `NoTargets` for a few
  hundred milliseconds after a peer-state transition,
* a peer-channel-full or socket-reset transient that one
  reconnect would clear,
* genuine data unavailability that the operator needs to see in
  the report.

Pass-3 redis-mode results showed a 14-point drop in success
rate (99.17% -> 85.31%); the morning triage (`docs/post-chaos-
queue.md` Tier 2 P3-2.7) called for a real client-SDK-style
retry layer in the driver so the next chaos pass can separate
"the cluster wobbled but recovered" from "the cluster was
genuinely down".

This commit adds that layer.

## Behaviour change (highlighted)

**Default behaviour is changing.** Before this commit, every
exception raised by the workload driver counted as one failure
in the per-DC NDJSON log. After this commit, the driver retries
once on `NoTargets` (default `--retry-on=NoTargets:1,Timeout:0`)
before counting the request as a failure. Operators reading the
historical pass-1, pass-2, and pass-3 reports against the
upcoming pass-4 results need to know that a drop in `failures`
does not necessarily mean the cluster got more reliable: it
might just mean the driver became more tolerant. The new
`retries` field in the NDJSON log makes the difference visible.

To restore the old behaviour exactly, pass `--retry-on=`
(empty string) on the workload-driver CLI or set
`RETRY_POLICY=""` in the coordinator's environment.

## Design

### CLI

```
python3 workload-driver.py ... --retry-on=NoTargets:1,Timeout:0
```

`--retry-on` takes a comma-separated list of `Class[:N]` entries.
`N` is the per-call retry budget for that error class; missing
`:N` defaults to `:1`. An entry of `Class:0` registers the class
as recoverable (so the dispatcher reports it under that class
rather than as `Unknown`) but never actually retries it. Empty
spec disables every retry.

Recoverable classes:

| Class             | Sources                                          |
|-------------------|--------------------------------------------------|
| `NoTargets`       | RESP `-DYNOMITE: ...`; memcache `SERVER_ERROR ... no quorum`; Riak `RpbErrorResp` whose `errmsg` matches "NoTargets" / "no quorum" |
| `Timeout`         | `socket.timeout` from a recv; Riak `errmsg` containing "timeout" |
| `Closed`          | `ConnectionError` from the hand-rolled readers; `OSError` (ECONNRESET, EPIPE) |
| `WrongConnection` | RESP `-NOAUTH ...` (clears after reconnecting)   |

Anything that does not match a known shape lands in the synthetic
`Unknown` bucket and is never retried.

### Retry loop

```
run_with_retry(fn, conn, mode, policy, retries, cls_name) -> (op, err_class)
```

`run_with_retry` invokes `fn(conn)`. On exception it classifies
into one of the classes above, closes the socket, consumes one
unit from the per-call budget, and retries. Budgets are
per-call (each workload op gets a fresh copy) so a busy class
cannot starve other classes' retries within the same per-second
flush window.

Each retry consumed is recorded into the `retries` counter
keyed by `<workload-class>/<error-class>`. The per-second NDJSON
window flushes the counter alongside `counts` and `failures`
and resets it.

### NDJSON

```json
{
  "ts": 1748150000.0,
  "label": "dc-floki",
  "mode": "redis",
  "elapsed": 10.0,
  "counts": { "strings/SET": 12345, "...": 0 },
  "failures": { "strings/NoTargets": 3 },
  "retries":  { "strings/NoTargets": 12, "strings/Timeout": 2 }
}
```

The `retries` field is new. `counts` and `failures` keep their
existing shape; the only externally visible change to
`failures` is that the second segment of the key is now the
semantic class (`NoTargets`, `Timeout`, ...) rather than the
Python exception type name (`RespError`, `ConnectionError`,
...). The report generator already aggregated these
opaquely so its output continues to make sense.

`generate-report.py` learned a new "Retry summary" section and
a `total retries` column in the per-DC throughput table. A
high retry-with-low-failure DC is healthy (the cluster wobbled
but recovered). A high retry-and-high-failure DC is the signal
the operator wants out of the report: the cluster is
genuinely past its budget.

### Coordinator

`scripts/chaos-multi-host/coordinator.sh` defaults to
`RETRY_POLICY=NoTargets:1,Timeout:0` and threads the value into
each workload-driver invocation as `--retry-on=$RETRY_POLICY`.
Operators can override per pass:

```bash
# match operator-typical client SDK behaviour (the default)
RETRY_POLICY="NoTargets:1,Timeout:0" \
  scripts/chaos-multi-host/coordinator.sh

# pre-2026-05-25 behaviour (every error is a failure)
RETRY_POLICY="" \
  scripts/chaos-multi-host/coordinator.sh

# more aggressive: retry NoTargets up to 3x and Timeout once
RETRY_POLICY="NoTargets:3,Timeout:1" \
  scripts/chaos-multi-host/coordinator.sh
```

`RETRY_POLICY=""` is honoured by the parameter-substitution
form `${RETRY_POLICY-...}` (no colon), which preserves an
empty string rather than falling back to the default.

## Tests

35 Python unittest cases total (8 pre-existing PBC encoding
tests plus 27 new for the retry layer):

* `_RetryPolicyParseTests` (8): empty spec, default spec,
  missing budget defaults to 1, full 4-class spec, unknown
  class rejected, negative budget rejected, non-integer
  rejected, whitespace-tolerant.
* `_ClassifyErrorTests` (12): every recoverable class for
  every mode, plus the negative cases (an unrelated RESP
  error / memcache SERVER_ERROR / Riak errmsg returns
  Unknown).
* `_RunWithRetryTests` (8): the five cases the brief enumerates
  (first-try success, NoTargets-once-then-success, NoTargets
  twice with budget 1 fails, Timeout forever with budget 0
  fails, unmapped error counts as failure with no retry) plus
  three edge cases (empty policy, retry budget resets per call,
  mixed Timeout-then-NoTargets exhausts independent budgets).

Run via `python3 scripts/chaos-multi-host/workload-driver.py
--self-test`.

## Verification

* `python3 scripts/chaos-multi-host/workload-driver.py --self-test`:
  35 OK.
* `python3 -c "import ast; ast.parse(open(...).read())"` on
  workload-driver.py and generate-report.py: clean.
* `bash -n scripts/chaos-multi-host/coordinator.sh`: clean.
* Smoke: synthetic NDJSON fed through `generate-report.py`
  produces the new "Retry summary" section and the
  `total retries` column populates correctly.
* CLI smoke: `--retry-on=BogusClass:1` rejected with a clear
  error message (exit 2); `--retry-on=` accepted and the
  driver runs against a (closed) backend producing the
  expected stream of `Closed` failures.
* `bash scripts/check_ascii.sh` and `bash scripts/check_no_todos.sh`:
  clean.
* `cargo build --workspace --all-targets --locked`: clean (no
  Rust touched, but the workspace still compiles).

## Files touched

```
scripts/chaos-multi-host/workload-driver.py    (+472, -16)
scripts/chaos-multi-host/coordinator.sh        (+15,  -1)
scripts/chaos-multi-host/generate-report.py    (+33,  -8)
docs/operations/chaos.md                       (+59,   0)
docs/journal/2026-05-25-workload-retry-semantics.md  (new)
```

## Open questions / next steps

* Pass-4 design: re-run a 30-minute redis chaos with the new
  driver and compare the `retries` field against the earlier
  pass-3 `failures` aggregate to quantify the gossip-churn vs
  genuine-unavailability split. That measurement was the
  motivating goal of P3-2.7 and is the next concrete deliverable.
* Memcache and Riak modes have classifier coverage but the
  retry layer has not been smoke-tested end-to-end against a
  live cluster in those modes; do that as part of pass-4 too.
* Adding a `dispatcher_dropped_request` metric on the dynomited
  side (paired with the driver's `retries` counter) would make
  the cause attribution unambiguous; that is a separate item
  and out of scope here.
