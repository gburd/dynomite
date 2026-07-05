#!/usr/bin/env bash
# Consistency gate (Elle-style) -- part of the required merge gate for
# any distributed-behaviour change (AGENTS.md Section 6.5).
#
# Two things run here:
#   1. A self-test that proves the checker has teeth: a clean
#      list-append history PASSES; a history with an injected
#      lost-update and one with a dirty read both FAIL. If the
#      checker ever stops catching those, the gate itself is broken
#      and we fail loudly.
#   2. The checker over every committed golden history under
#      tests/fixtures/consistency/*.jsonl (recorded from real-code
#      runs -- the built binary behind its wire API, e.g. the EC2
#      distributed runs or the local dyniak harness). Any anomaly of
#      a covered class fails the build.
#
# A change to the distributed path is expected to ADD a golden
# history captured from the real code exercising it, so this gate
# has something to check. A change with no golden history and no
# self-test regression is a warning, not a silent pass.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
CHECK="$ROOT/scripts/consistency/elle_check.py"
FIX="$ROOT/crates/dynomited/tests/fixtures/consistency"
TMP="$(mktemp -d /scratch/elle-selftest.XXXXXX 2>/dev/null || mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "consistency-gate: FAIL -- $*" >&2; exit 1; }

echo "==> consistency gate (Elle-style, AGENTS.md 6.5)"

# 1. Self-test: the checker must PASS clean and FAIL the two seeded
#    anomalies. This proves the gate is not vacuous.
python3 - > "$TMP/clean.jsonl" <<'PY'
import json
r=[{"index":0,"process":0,"type":"ok","time_ns":1,"value":[["append",0,1]]},
   {"index":1,"process":0,"type":"ok","time_ns":2,"value":[["append",0,2]]},
   {"index":2,"process":0,"type":"ok","time_ns":3,"value":[["r",0,[1,2]]]}]
print("\n".join(json.dumps(x) for x in r))
PY
python3 - > "$TMP/lost.jsonl" <<'PY'
import json
r=[{"index":0,"process":0,"type":"ok","time_ns":1,"value":[["r",0,[1,2]]]},
   {"index":1,"process":0,"type":"ok","time_ns":2,"value":[["r",0,[1]]]}]
print("\n".join(json.dumps(x) for x in r))
PY
python3 - > "$TMP/dirty.jsonl" <<'PY'
import json
r=[{"index":0,"process":0,"type":"fail","time_ns":1,"value":[["append",0,7]]},
   {"index":1,"process":1,"type":"ok","time_ns":2,"value":[["r",0,[7]]]}]
print("\n".join(json.dumps(x) for x in r))
PY

python3 "$CHECK" "$TMP/clean.jsonl" >/dev/null 2>&1 || fail "checker self-test: clean history did NOT pass"
python3 "$CHECK" "$TMP/lost.jsonl"  >/dev/null 2>&1 && fail "checker self-test: lost-update was NOT caught (gate has no teeth)"
python3 "$CHECK" "$TMP/dirty.jsonl" >/dev/null 2>&1 && fail "checker self-test: dirty read was NOT caught (gate has no teeth)"
echo "   checker self-test OK (clean passes; lost-update + dirty-read caught)"

# 2. Golden histories captured from real-code runs.
shopt -s nullglob
golden=("$FIX"/*.jsonl)
if [ ${#golden[@]} -eq 0 ]; then
  echo "   note: no golden histories in $FIX yet; add one captured from a"
  echo "   real distributed run (scripts/consistency/txn_history_workload.py)"
  echo "   when landing a distributed-path change (AGENTS.md 6.5)."
else
  for h in "${golden[@]}"; do
    echo "   checking $(basename "$h")"
    python3 "$CHECK" "$h" || fail "anomaly in golden history $(basename "$h")"
  done
fi
echo "consistency-gate: OK"
