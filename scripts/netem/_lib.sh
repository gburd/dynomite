#!/usr/bin/env bash
# scripts/netem/_lib.sh - shared helpers for chaos injectors.
#
# Sourced by every script in this directory. Provides:
#   * cap_check: assert CAP_NET_ADMIN (or root). On failure
#     emits a JSON skip notice and exits 0 so the chaos test
#     can treat the injector as a no-op.
#   * tc_dev: pick the loopback device (the chaos test runs
#     entirely on lo).
#   * with_qdisc / clear_qdisc: idempotent qdisc add / remove.
#
# The injectors are POSIX sh apart from `[[`; the chaos
# harness invokes them via `bash`, so bash-isms are fine.

set -euo pipefail

NETEM_DEV="${NETEM_DEV:-lo}"

emit_skip() {
    local reason="$1"
    printf '{"status":"skip","reason":"%s"}\n' "$reason"
    exit 0
}

cap_check() {
    if ! command -v tc >/dev/null 2>&1; then
        emit_skip "tc-not-on-PATH"
    fi
    if [ "$(id -u)" -ne 0 ]; then
        # capsh is the canonical way to check CAP_NET_ADMIN
        # without running tc and parsing failure modes.
        if command -v capsh >/dev/null 2>&1; then
            if ! capsh --has-p=cap_net_admin >/dev/null 2>&1; then
                emit_skip "no-cap-net-admin"
            fi
        else
            # Best effort: try a no-op `tc qdisc show` and fall
            # back to skip if it fails.
            if ! tc qdisc show dev "$NETEM_DEV" >/dev/null 2>&1; then
                emit_skip "no-cap-net-admin"
            fi
        fi
    fi
}

clear_qdisc() {
    local dev="${1:-$NETEM_DEV}"
    tc qdisc del dev "$dev" root 2>/dev/null || true
}

with_qdisc_root() {
    local dev="$1"
    shift
    clear_qdisc "$dev"
    tc qdisc add dev "$dev" root "$@"
}
