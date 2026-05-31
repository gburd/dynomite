# Chaos coordinator: bash-stdin runner pattern + meh re-add

Date: 2026-05-25
Stage: post-chaos queue P3-1.4
Branch: chaos-shell-runner-refactor

## Problem

Pass-3 attempted to add meh as a 4th chaos host. meh's user
login shell is fish, and the coordinator's SSH callsites
passed each remote command as a single string argument:

    "${runner[@]}" "<cmd-string>"

For arnold (bash) and nuc (FreeBSD bash) that string is parsed
by the remote login shell as a normal command line. For meh,
fish reparses the string under fish syntax and mangles
bash-only constructs:

* env-prefix assignments (`MODE='redis' /bin/bash ...`) parse
  as a fish command-not-found because fish has no env-prefix
  syntax.
* `$!` in `echo $! > workload.pid` is a fish empty-event-
  expansion error, not the bash background-pid sigil.
* multi-line strings interpolated via SSH arg get re-tokenised
  by fish at every newline.

The pass-3 attempt was rolled back, leaving meh defined in
coordinator.sh but excluded from execution. P3-1.4 captured
the work to refactor the runner pattern.

## Fix

Refactor every SSH callsite from string-arg to bash-stdin:

    "${runner[@]}" bash -s <<EOF
    <command body>
    EOF

`bash -s` reads the script body from stdin. The remote login
shell still parses `bash -s` as the command to exec, but those
two tokens contain no shell metacharacters so every shell
(bash, fish, sh, csh) handles them correctly. The body that
arrives on stdin is interpreted by bash regardless of the
operator's login shell.

Variable interpolation still happens on the local side at
heredoc-expansion time (`<<EOF` unquoted). Remote-only `$vars`
must be escaped as `\$var`. Static bodies use `<<'EOF'`.

### Callsites converted (8)

* `start_host` start-host.sh invocation, plus its two nested
  file-write heredocs (seeds.yml, start-args)
* `start_workload` nohup workload-driver.py
* `start_injector` nohup chaos-injector.sh
* `teardown` per-host kill-and-cleanup snippet (4 callsites:
  floki-local, arnold, nuc-direct, nuc-proxyjump, meh)
* `bootstrap_remote_src` mkdir step
* `src_check` directory-existence probe (newly factored helper)

### File-write heredocs

Two of the start_host writes (seeds.yml, start-args) used the
old "remote cat with stdin heredoc" form. Under bash-stdin
that becomes a nested heredoc inside the outer script body:

    cat > /scratch/dynomite-chaos/run/seeds.yml <<'__CHAOS_SEEDS_END__'
    $seeds_str
    __CHAOS_SEEDS_END__

The outer `<<EOF` is unquoted so `$seeds_str` expands locally
before the body is sent. The inner `<<'__CHAOS_SEEDS_END__'`
is literal-quoted so remote bash writes the seed bytes
verbatim with no further expansion. Unique inner terminators
keep the parse unambiguous.

### MEH_SSH

The previous workaround prepended `bash -lc` inside MEH_SSH:

    MEH_SSH=(env SSH_AUTH_SOCK="" ssh ... meh bash -lc)

Removed under the new pattern. MEH_SSH is now identical in
shape to ARNOLD_SSH (`ssh ... meh`); the coordinator appends
`bash -s` at every callsite.

### Teardown timeout

Every teardown SSH remains wrapped in
`timeout --signal=KILL <budget> ... bash -s <<<"$remote_cmd"`.
The 60s budget from commit `3a98675` is preserved; only the
shape of the inner ssh invocation changed. The `<<<` here-
string form is convenient because `$remote_cmd` is a multi-
line variable populated once per teardown and fed identically
to every host.

## Re-adding meh

* `bootstrap_remote_src dc-meh meh "$MEH_RSYNC_E" "${MEH_SSH[@]}"`
* `src_check meh "${MEH_SSH[@]}"`
* `start_host dc-meh "$TOKENS_MEH" "$(meh_seeds)" "${MEH_SSH[@]}"`
* `start_workload dc-meh /bin/bash "${MEH_SSH[@]}"`
* `start_injector dc-meh /bin/bash "${MEH_SSH[@]}"`
* teardown dc-meh and rsync meh logs

The 4-way token split (floki=0, arnold=1G, nuc=2G, meh=3G)
was already defined; it's now actually used.

## HOSTS_OVERRIDE knob

Added a comma-separated host filter:

    HOSTS_OVERRIDE=meh bash scripts/chaos-multi-host/coordinator.sh

Default (unset/empty) means all four hosts run, preserving the
existing pass-3 entrypoints. Each per-host action is gated by
`if host_enabled <name>; then ... fi`. Host names are bare
(`floki`, `arnold`, `nuc`, `meh`); the dc-prefix is internal.

## Smoke test

New operator-only script `scripts/chaos-multi-host/smoke-
coordinator.sh`:

* Runs a 30-second mock cycle in redis mode (smallest blast
  radius)
* Across all four hosts by default; override via
  HOSTS_OVERRIDE
* Asserts every enabled host produced a workload.ndjson with
  at least MIN_OPS (default 100) lines
* Cleanup is the coordinator's existing teardown trap

CI does not run this; the chaos hosts are not in the CI
network. Recipe:

    # All four hosts (post-refactor sanity check)
    bash scripts/chaos-multi-host/smoke-coordinator.sh

    # meh only (validates the bash-stdin pattern against fish)
    HOSTS_OVERRIDE=meh \
        bash scripts/chaos-multi-host/smoke-coordinator.sh

    # 3-host legacy topology (regression check)
    HOSTS_OVERRIDE=floki,arnold,nuc \
        bash scripts/chaos-multi-host/smoke-coordinator.sh

## Verification

    bash -n scripts/chaos-multi-host/coordinator.sh
    bash -n scripts/chaos-multi-host/smoke-coordinator.sh
    shellcheck scripts/chaos-multi-host/coordinator.sh
    shellcheck scripts/chaos-multi-host/smoke-coordinator.sh
    bash scripts/check_no_todos.sh
    bash scripts/check_no_port_comments.sh
    bash scripts/check_ascii.sh
    cargo build --workspace --all-targets --locked
    cargo fmt -p dynomite -p dynomited -p dyn-hash-tool \
              -p dyn-encoding -p dyniak -p dyn-admin -- --check

shellcheck is clean for both scripts. The previous pass-3
SC2034 warnings on `MEH_SSH` / `MEH_RSYNC_E` are gone now that
the variables are referenced.

## Notes

* `start_workload`'s `bash_path` argument was historically
  unused; the new pattern doesn't need it either. Renamed to
  `_bash_path` to silence shellcheck's unused-variable lint
  while preserving the calling convention so the four
  start_workload callsites stay symmetric with the four
  start_injector callsites (where bash_path IS used).
* The local-floki path now uses an empty array `LOCAL_RUN=()`
  as its runner. `"${LOCAL_RUN[@]}" bash -s <<EOF` expands to
  `bash -s <<EOF`, firing the local shell. This unifies the
  local and remote dispatch shapes; the only callsite that
  still hard-codes `bash` is `start_floki`, which directly
  invokes start-host.sh in the foreground for build/copy
  reasons.
* The teardown's `remote_cmd` variable is now populated from
  a single literal-quoted heredoc rather than a single-quoted
  string literal. This dropped the prior
  `# shellcheck disable=SC2016` directive: the new form is a
  here-string which shellcheck recognises correctly.

## Roll-out

The chaos infrastructure on remote hosts is rsync'd from the
working tree to `/scratch/dynomite-chaos/src` at the start of
each mode by `bootstrap_remote_src`. The fix becomes active
for the next mode launched after this lands on `main`. There
is no running pass-3 at the moment of this commit; the next
operator-initiated run will pick up the refactor.

## Follow-ups

None for P3-1.4. P3-2.* tier-2 items (per-mode report
generation, retry semantics, gossip-driven oscillation) are
unaffected.
