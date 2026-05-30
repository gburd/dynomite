#!/usr/bin/env bash
#
# Test nuc bootstrap path end-to-end (Issue B).
#
# Asserts that against a live nuc:
#   1. ssh -J arnold nuc returns 0 for a trivial command;
#   2. rsync via NUC_RSYNC_E succeeds OR the tar-pipe
#      fallback succeeds;
#   3. bootstrap_remote_src against nuc returns 0 end-to-end.
#
# This test requires:
#   * SSH access to arnold and to nuc through arnold;
#   * /scratch/dynomite-chaos to exist on nuc (or be creatable);
#   * the local repo at $REPO is sane.
#
# It is read-only on the local side and only writes to
# /scratch/dynomite-chaos/src on nuc (which the chaos rig
# writes to anyway). Skips with a clear message when SSH to
# arnold/nuc is not available.
#
# Run with:
#   bash scripts/chaos-multi-host/test_nuc_bootstrap.sh
#
# Exit codes:
#   0  all asserts passed
#   1  any assert failed
#   77 environment cannot run the test (treated as skip)

set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
SCRIPT_DIR="$REPO/scripts/chaos-multi-host"

SSH_KEY="$HOME/.ssh/id_ed25519"
SSH_BASE_OPTS=(-o IdentitiesOnly=yes -i "$SSH_KEY"
               -o ControlMaster=no -o ControlPath=none
               -o StrictHostKeyChecking=accept-new
               -o ServerAliveInterval=30
               -o ConnectTimeout=15
               -o BatchMode=yes)

ARNOLD_SSH=(env SSH_AUTH_SOCK="" ssh "${SSH_BASE_OPTS[@]}" arnold)
NUC_SSH=(env SSH_AUTH_SOCK="" ssh "${SSH_BASE_OPTS[@]}" -o ProxyJump=arnold gburd@nuc)
NUC_RSYNC_E="ssh ${SSH_BASE_OPTS[*]} -o ProxyJump=arnold"

pass=0
fail=0

assert_ok() {
    local name="$1"; shift
    if "$@"; then
        echo "PASS: $name"
        pass=$((pass + 1))
    else
        local rc=$?
        echo "FAIL: $name (rc=$rc)"
        fail=$((fail + 1))
    fi
}

# Step 0: probe arnold; if we can't reach arnold, skip the
# whole suite (env-issue, not a code regression).
echo "==> probing arnold reachability"
if ! timeout --signal=KILL 20s "${ARNOLD_SSH[@]}" true >/dev/null 2>&1; then
    echo "SKIP: cannot ssh arnold (env unavailable)"
    exit 77
fi

echo "==> probing nuc reachability via ProxyJump"
if ! timeout --signal=KILL 30s "${NUC_SSH[@]}" true >/dev/null 2>&1; then
    echo "SKIP: cannot ssh nuc via arnold (env unavailable)"
    exit 77
fi

# Test 1: ssh -J arnold nuc returns 0 for trivial cmd.
test_ssh_proxyjump() {
    timeout --signal=KILL 30s "${NUC_SSH[@]}" bash -s <<'EOF' >/dev/null 2>&1
echo alive
EOF
}
assert_ok "ssh -J arnold nuc echo alive" test_ssh_proxyjump

# Test 2: rsync via NUC_RSYNC_E. We rsync a tiny throwaway
# tree (just this script) into a sandbox under
# /scratch/dynomite-chaos/test/. Don't touch the actual src
# tree.
test_rsync_via_proxyjump() {
    "${NUC_SSH[@]}" bash -s <<'EOF' >/dev/null 2>&1
mkdir -p /scratch/dynomite-chaos/test/nuc-rsync-probe
EOF
    timeout --signal=KILL 60s rsync -az -e "$NUC_RSYNC_E" \
        "$0" \
        gburd@nuc:/scratch/dynomite-chaos/test/nuc-rsync-probe/test_nuc_bootstrap.sh \
        >/dev/null 2>&1
}
# Don't fail the suite on rsync alone -- that's exactly the
# Pass-7 symptom we're working around. Record the result.
if test_rsync_via_proxyjump; then
    echo "PASS: rsync via NUC_RSYNC_E"
    pass=$((pass + 1))
    rsync_ok=1
else
    echo "INFO: rsync via NUC_RSYNC_E failed (expected on degraded ProxyJump tunnels)"
    rsync_ok=0
fi

# Test 3: tar-pipe fallback. This is what
# bootstrap_remote_src now falls back to when rsync fails.
test_tar_pipe_fallback() {
    "${NUC_SSH[@]}" bash -s <<'EOF' >/dev/null 2>&1
mkdir -p /scratch/dynomite-chaos/test/nuc-tar-probe
EOF
    tar -cf - -C "$(dirname "$0")" "$(basename "$0")" \
        | timeout --signal=KILL 60s "${NUC_SSH[@]}" \
              bash -c 'tar -xf - -C /scratch/dynomite-chaos/test/nuc-tar-probe' \
              >/dev/null 2>&1
}
assert_ok "tar | ssh fallback" test_tar_pipe_fallback

# Test 4: bootstrap_remote_src end-to-end. Source the function
# from a stub so we exercise the same code path the
# coordinator does. We extract the function via a small
# wrapper that defines REPO and a no-op log.
test_bootstrap_remote_src() {
    local tmp; tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' RETURN
    cat > "$tmp/wrapper.sh" <<WRAP
#!/usr/bin/env bash
set -euo pipefail
REPO="$REPO"
log() { printf 'wrapper-log: %s\n' "\$*"; }
# Pull the bootstrap_remote_src function out of coordinator.sh
# by sourcing the file with a guard that exits BEFORE any
# top-level work runs. The coordinator runs unconditionally
# at file scope; we cannot just source it. Instead, extract
# the function block with sed.
SCRIPT_DIR="$SCRIPT_DIR"
# Use awk to extract from "bootstrap_remote_src() {" through
# the matching closing brace at column 0 (the bash style).
awk '/^bootstrap_remote_src\(\) \{/{found=1} found{print} found && /^}\$/{exit}' \
    "\$SCRIPT_DIR/coordinator.sh" > "$tmp/fn.sh"
. "$tmp/fn.sh"
SSH_BASE_OPTS=(${SSH_BASE_OPTS[@]@Q})
NUC_SSH=(env SSH_AUTH_SOCK="" ssh "\${SSH_BASE_OPTS[@]}" -o ProxyJump=arnold gburd@nuc)
NUC_RSYNC_E="ssh \${SSH_BASE_OPTS[*]} -o ProxyJump=arnold"
bootstrap_remote_src dc-nuc gburd@nuc "\$NUC_RSYNC_E" no "\${NUC_SSH[@]}"
WRAP
    chmod +x "$tmp/wrapper.sh"
    bash "$tmp/wrapper.sh" >/dev/null 2>&1
}
assert_ok "bootstrap_remote_src dc-nuc end-to-end" test_bootstrap_remote_src

echo
echo "==> results: $pass pass, $fail fail (rsync_ok=$rsync_ok)"
if [ "$fail" -gt 0 ]; then
    exit 1
fi
exit 0
