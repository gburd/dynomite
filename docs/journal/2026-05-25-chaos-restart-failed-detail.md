# 2026-05-25 - chaos: capture restart-failure detail

Stage: post-chaos queue P3-1.3.
Branch: stage/p3-1.3-restart-failed-detail.

## Motivation

Pass-3 redis mode showed 73 (arnold) + 76 (nuc) `restart_failed`
events in the chaos-events ndjson even though bug #11 (recovery
debounce) and bug #12 (kill-stale-before-restart) are both in
place. The injector was emitting an opaque
`event restart_failed "{\"reason\":\"start-host.sh-nonzero\"}"`
and discarding the actual stderr/stdout, forcing per-host log
scraping to triage.

## Change

`scripts/chaos-multi-host/chaos-injector.sh`:

1. Added `json_string_escape` helper that encodes a multi-line
   blob into a single JSON string field. Backslashes, double
   quotes, and tabs are escaped inline; newlines are routed
   through a vertical-tab placeholder (forbidden in our log
   payloads) and rewritten to literal backslash-n, so the
   resulting blob is safe to splice between double quotes in
   ndjson.
2. Added `file_sha256` helper that picks `sha256sum`,
   `shasum -a 256`, or `sha256` depending on host (Linux vs
   FreeBSD). Returns the literal "unknown" if no hasher is
   present, so the JSON payload remains parseable.
3. Refactored `restart_dynomited` to capture
   `start-host.sh`'s exit code, `tail -n 50` the combined
   stdout+stderr log on failure, and emit
   `{"reason":"start-host.sh-nonzero","rc":<int>,"tail":"<blob>"}`.
4. The function now takes an optional failure-event-name
   argument; the recovery branch in the main loop calls
   `restart_dynomited recovery_restart_failed` so a recovery
   restart that fails is distinguishable from a scheduled-kill
   restart in the event stream.
5. Replicated the same pattern on the `redis_bounce` path,
   emitting `redis_bounce_failed` with rc + tail when the
   bounce restart fails.
6. Emit `start_args_fingerprint` once at injector start with
   the SHA-256 of `$RUN/start-args` plus the mode, so we can
   correlate event-stream tails with the exact argument set
   that produced them across multi-mode pass-3 runs.

The ndjson event format is unchanged: every emitted line is
still a single JSON object on a single line; embedded newlines
in the `tail` field are JSON-escaped (`\n`), not literal.

## Tests

* `bash -n scripts/chaos-multi-host/chaos-injector.sh`: clean.
* `shellcheck scripts/chaos-multi-host/chaos-injector.sh`:
  clean (rc 0, no warnings).
* Project hygiene: `scripts/check_no_todos.sh`,
  `scripts/check_no_port_comments.sh`,
  `scripts/check_ascii.sh`: all green.

### Smoke recipe

The smoke script lives at `/tmp/chaos-smoke.sh` (not committed;
it pokes at the injector with mktemp scratch dirs). Reproduce
locally with:

```sh
SMOKE_ROOT=$(mktemp -d /tmp/chaos-smoke-XXXXXX)
mkdir -p "$SMOKE_ROOT"/{run,logs,src/scripts/chaos-multi-host}

# Stub start-host.sh that always fails with a known marker.
cat > "$SMOKE_ROOT/src/scripts/chaos-multi-host/start-host.sh" <<'STUB'
#!/usr/bin/env bash
echo "[stub start-host] dc=$1 tokens=$2"
echo "FATAL: deliberate test failure" >&2
exit 7
STUB
chmod +x "$SMOKE_ROOT/src/scripts/chaos-multi-host/start-host.sh"

# Minimal start-args.
cat > "$SMOKE_ROOT/run/start-args" <<'ARGS'
TOKENS="111111"
SEEDS="seed-host:18101:rack0:dc0:111111"
DATASTORE_PORT=17100
DYN_LISTEN_PORT=18101
CLIENT_LISTEN_PORT=18102
STATS_LISTEN_PORT=22222
MODE=redis
ARGS
```

Source the helper definitions out of the injector with awk
ranges (`/^stamp\(\)/,/^}/`, `/^event\(\)/,/^}/`,
`/^json_string_escape\(\)/,/^}/`, `/^file_sha256\(\)/,/^}/`,
`/^kill_stale_dynomited\(\)/,/^}/`,
`/^restart_dynomited\(\)/,/^}/`), set `DC_NAME`, `ROOT`,
`RUN`, `LOGS`, `EVENTS`, source `start-args`, then call
`restart_dynomited` and
`restart_dynomited recovery_restart_failed`.

Validation:

```python
import json
for ln in open("logs/chaos-events-smoke.ndjson"):
    obj = json.loads(ln)
fail = [json.loads(ln) for ln in open(...)
        if "_failed" in json.loads(ln)["kind"]]
assert any(e["kind"] == "restart_failed" for e in fail)
assert any(e["kind"] == "recovery_restart_failed" for e in fail)
for e in fail:
    assert e["detail"]["rc"] == 7
    assert "FATAL: deliberate test failure" in e["detail"]["tail"]
```

### Smoke output (annotated)

```
{"kind":"injector_start", ...}
{"kind":"start_args_fingerprint",
 "detail":{"file":".../run/start-args",
           "sha256":"048ca853cd6cf25840add030f26252419d6bab47f604904118adf607dc66709b",
           "mode":"redis"}}
{"kind":"restart","detail":{"reason":"sigkill"}}
{"kind":"restart_failed",
 "detail":{"reason":"start-host.sh-nonzero","rc":7,
           "tail":"[stub start-host] dc=smoke tokens=111111\n
                   FATAL: deliberate test failure"}}
{"kind":"restart","detail":{"reason":"sigkill"}}
{"kind":"recovery_restart_failed",
 "detail":{"reason":"start-host.sh-nonzero","rc":7,
           "tail":"...two iterations concatenated..."}}
```

Six lines, all parse as JSON, all carry the expected detail
shape.

## Deployment

The chaos infrastructure on remote hosts is rsync'd to
`/scratch/dynomite-chaos/src` at each mode launch. This change
takes effect at the next mode (riak mode is queued and will
pick it up). I deliberately did not push to a running mode's
filesystem.

## Open follow-ups

* Once a pass-4 run lands with these enriched events, mine
  the new `tail` fields to classify the residual 11% failure
  rate and feed the categorisation back into the queue under
  P3-1.3 part 2.
* Consider adding `event start_host_log_path` so a long failure
  tail that exceeds 50 lines can be retrieved out-of-band; for
  now 50 lines covers every observed `start-host.sh` failure.
