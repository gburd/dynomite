# 2026-05-26 - chaos-injector MODE leak bug

## Symptom

Pass-5 memcache mode on `dc-floki` produced **0 successful operations
across all 2 hours** of the run.  The workload-driver logged
`Closed`/`Timeout` errors continuously; dynomited's backend supervisor
hit a tight loop logging `protocol parse error: Error` reconnecting to
the local backend on `127.0.0.1:17100`.

The other hosts (`dc-arnold`, `dc-meh`) had working memcache traffic,
which is what made the floki failure stand out.

Aggregate pass-5 memcache: 2,128,965 ok / 863,950 fail / 71.13%
success.  Floki contributed all of the failures.

## Root cause

`scripts/chaos-multi-host/coordinator.sh` writes the per-host
`start-args` file via a single-quoted heredoc:

    cat > /scratch/dynomite-chaos/run/start-args <<'__CHAOS_ARGS_END__'
    MODE='$MODE'
    TOKENS='$tokens'
    SEEDS=\$(cat /scratch/dynomite-chaos/run/seeds.yml)
    ...
    __CHAOS_ARGS_END__

The single-quoted heredoc was used so the `$(cat ...)` payload for
`SEEDS` would be evaluated lazily on the remote host (when
`chaos-injector.sh` sources start-args), not at write time on the
coordinator.  Side effect: every other `$MODE`, `$tokens`, `$DATASTORE_PORT`,
... became literal text in the file.  When the chaos-injector sourced
start-args, MODE was assigned the 5-byte string `$MODE` instead of
`memcache`.

The chaos-injector's case statements then fell to defaults.  The
visible artifact in the events ndjson:

    {"kind":"fault_process_redis_bounce","detail":{
        "id":"container:dyn-chaos-memcached-dc-floki",
        "mode":"redis"
    }}

The `id` correctly shows the memcached container is what the injector
identified for bouncing, but `mode` is `redis` because the injector
fell back to its `MODE="${MODE:-redis}"` default after the literal
`$MODE` failed to match anything useful in downstream code paths.

When the bounce path then invoked `start-host.sh` with `MODE=redis`,
that helper killed the running memcached container and brought up a
fresh `redis-server` on port 17100.  dynomited stayed configured for
`data_store=1` (memcache), so every subsequent backend connection saw
a redis server speaking RESP and the memcache parser surfaced
`protocol parse error: Error`.

The chaos-injector then bounced the backend several more times, and
because the heredoc bug was racy with how `MODE` happened to be set in
the injector process's environment between iterations, some bounces
correctly used `mode=memcache` (5 of 9 in the memcache window were
`redis`, 4 of 9 were `memcache`).  Net result: floki's backend
flip-flopped between redis and memcached for two hours.

## Why we did not catch this earlier

* Pass-1 / pass-2 / pass-4 were redis-only.  The bug masked itself:
  `MODE` defaulting to `redis` happened to be correct.
* Pass-3 memcache and riak modes both died at startup before any
  chaos cycles ran (different bugs since fixed: missing memcached on
  nuc; missing `--features riak` binary on nuc).  No bounce events
  fired, so the MODE-leak path was not exercised.
* Pass-5 memcache mode is the **first multi-host run where chaos
  bounces actually hit a non-redis backend**, and the bug surfaced
  immediately.

## Fix

Switch the heredoc from single-quoted to unquoted so `MODE`, `tokens`,
and the port variables expand at write time on the coordinator:

    cat > /scratch/dynomite-chaos/run/start-args <<__CHAOS_ARGS_END__
    MODE='$MODE'
    TOKENS='$tokens'
    SEEDS=\$(cat /scratch/dynomite-chaos/run/seeds.yml)
    ...
    __CHAOS_ARGS_END__

The `\$(cat ...)` for SEEDS keeps the leading backslash so it stays
literal in the file (preserving the original deferred-evaluation
intent for the multi-line seeds payload).  Verified locally:

    $ MODE=memcache tokens=1234
    $ cat > /tmp/test-start-args <<__CHAOS_ARGS_END__
    MODE='$MODE'
    TOKENS='$tokens'
    __CHAOS_ARGS_END__
    $ ( . /tmp/test-start-args; echo MODE=$MODE TOKENS=$TOKENS )
    MODE=memcache TOKENS=1234

Single-line change; no behavioral diff for the redis-only passes
(MODE was `redis` either way).

## Test gap

We have unit-level coverage of `chaos-injector.sh` via
`scripts/chaos-multi-host/test_*.sh` but no test that asserts the
sourced `start-args` actually carries a non-default MODE.  Adding one
in a follow-up: `test_coordinator_start_args.sh` writes start-args
through the coordinator's helper (refactored to be testable), sources
it in a subshell, and asserts `[ "$MODE" = memcache ]` for a
memcache-mode invocation.

## Impact on pass-5

The pass-5 memcache report has been generated as-is and committed,
with this journal entry linked from the report's "Known issues"
section.  Pass-5 riak mode is still running (started 21:13 UTC).
The same bug almost certainly affects it too because:

* Riak mode also runs `data_store=0` with redis as the backing store,
  so the literal-mode default to `redis` matches the backend by
  accident.
* But Riak PBC traffic goes through a different listener
  (`--riak-pbc-listen=22244`) which the chaos bounce loop does not
  touch.  The riak-mode workload-driver hits PBC, not the redis
  backend.

So riak mode's success rate may end up looking close to redis mode's
even though the bounce events were also misclassified.  The numbers
will tell us, and we will document the bug's blast radius in the
pass-5 riak report.

## Pass-6 plan

* Re-run pass-6 redis + memcache + riak with this fix in place.
* Default config: `MODE_FAULTS=process` (today's behavior unchanged),
  retry policy now includes backoff (separate change in flight).
* Expect memcache success rate to come up to redis levels (90%+).

## References

* `scripts/chaos-multi-host/coordinator.sh` (one-line fix)
* `dist/chaos-reports/v0.1.0/multi-host-pass-5-memcache.md` (the
  buggy run's report)
* `dist/chaos-reports/v0.1.0/multi-host-pass-5-redis.md` (the clean
  run's report; was unaffected by the bug)
