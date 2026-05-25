# Chaos teardown timeout-and-continue

Date: 2026-05-25
Stage: post-chaos queue P3-1.2
Branch: stage/p3-1.2-teardown-timeout

## Problem

During pass-3 redis mode, the coordinator's `teardown()` blocked
for ~5 hours on the dc-nuc SSH command. The connection to nuc
goes via `ProxyJump=arnold`; the chaos injector was in the
middle of bouncing dynomited on arnold, which left the proxy
hop wedged. Teardown's plain `ssh` invocation has no timeout, so
the entire pass-3 pipeline stalled until a human killed it.

The next two modes (memcache, riak) never started.

## Fix

`scripts/chaos-multi-host/coordinator.sh` `teardown()` is now
written as an explicit per-host sequence rather than a
spec-driven loop, so each step can be wrapped independently:

* Each remote SSH is wrapped in
  `timeout --signal=KILL 60s ...`. On timeout the function logs
  a `WARN dc-X teardown timed out after 60s; continuing` line
  and proceeds to the next host. Per-host failures are
  non-fatal; the coordinator's only obligation here is to free
  the next mode.
* For dc-nuc the handler tries a LAN-direct SSH first
  (`NUC_DIRECT_SSH`, 30s) and falls back to the ProxyJump path
  (`NUC_SSH`, 60s) only if the direct attempt fails. The
  LAN-direct path bypasses arnold entirely, so a chaos-frozen
  arnold no longer blocks nuc teardown.
* Both rsync log copy-backs (arnold, nuc) are also wrapped
  (60s) for the same reason.

A short readme-style table at the top of `teardown()`
documents the budgets and the on-timeout behavior for each
step.

## NUC_DIRECT_SSH

Defined alongside `NUC_SSH`:

```sh
NUC_DIRECT_SSH=(env SSH_AUTH_SOCK="" ssh "${SSH_BASE_OPTS[@]}" "gburd@$NUC_LAN_IP")
```

`$NUC_LAN_IP` is `192.168.1.61`. Whether floki can reach this
address depends on whether the local Tailscale node has subnet
routes for arnold's LAN; if not, the direct attempt fails fast
(no route / connection refused) and the ProxyJump fallback
kicks in.

## Roll-out

The chaos infrastructure on remote hosts is rsync'd from the
working tree to `/scratch/dynomite-chaos/src` at the start of
each mode by `bootstrap_remote_src`. The fix therefore becomes
active for the next mode launched after this lands on `main`.
The currently-running pass-3 (memcache mode in flight, riak
mode queued) will pick up the fix automatically when riak mode
bootstraps. No action required against the running cluster.

## Verification

```
bash -n scripts/chaos-multi-host/coordinator.sh
shellcheck scripts/chaos-multi-host/coordinator.sh
bash scripts/check_no_todos.sh
bash scripts/check_no_port_comments.sh
bash scripts/check_ascii.sh
cargo build --workspace --all-targets --locked
cargo fmt -p dynomite -p dynomited -p dyn-hash-tool \
          -p dyn-encoding -p dyn-riak -p dyn-admin -- --check
```

shellcheck reports the two pre-existing `MEH_SSH` /
`MEH_RSYNC_E` SC2034 warnings (pass-3 deferred meh, see
`be47df6`) and is otherwise clean. One SC2016
(`expressions don't expand in single quotes`) on the new
`local remote_cmd='...'` block is intentionally suppressed
with a `# shellcheck disable=SC2016` directive: the shell
snippet is single-quoted on purpose so `$f` and
`$(cat ...)` evaluate on the remote host.

## Follow-ups

None. P3-1.2 is closed by this commit.
