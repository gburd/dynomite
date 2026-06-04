#!/usr/bin/env bash
#
# Self-test for scripts/chaos-multi-host/driver-spec.sh.
#
# Sources the helper and asserts compute_driver_specs / the
# pidfile mapping produce the expected per-mode fan-out. The
# unified case is the load-bearing one: it must emit exactly two
# specs (redis + riak) with the QPS split so the total is
# preserved, and distinct driver-<api>.pid pidfiles.
#
# Run from anywhere:
#
#     bash scripts/chaos-multi-host/test_driver_spec.sh

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./driver-spec.sh
. "$HERE/driver-spec.sh"

PASS=0
FAIL=0

fail() {
    FAIL=$((FAIL + 1))
    printf 'FAIL %s\n' "$*"
}

ok() {
    PASS=$((PASS + 1))
    printf 'OK   %s\n' "$*"
}

assert_eq() {
    local expected="$1" actual="$2" name="$3"
    if [ "$expected" = "$actual" ]; then
        ok "$name"
    else
        fail "$name: expected [$expected] got [$actual]"
    fi
}

# ---- redis (single driver, legacy layout) ----
specs="$(compute_driver_specs redis 200 18102 18202 21800)"
line_count="$(printf '%s\n' "$specs" | grep -c .)"
assert_eq 1 "$line_count" "redis emits one spec"
assert_eq "$(printf '\t200\t--mode redis')" "$specs" "redis spec shape"

# ---- memcache (single driver) ----
specs="$(compute_driver_specs memcache 200 18102 18202 21800)"
assert_eq "$(printf '\t200\t--mode memcache')" "$specs" "memcache spec shape"

# ---- riak (single driver, dials PBC) ----
specs="$(compute_driver_specs riak 200 18102 18202 21800)"
assert_eq "$(printf '\t200\t--mode riak --riak-pbc-port 21800')" "$specs" \
    "riak spec shape"

# ---- differential (single driver, dual fan-out flags) ----
specs="$(compute_driver_specs differential 200 18102 18202 21800)"
assert_eq \
    "$(printf '\t200\t--mode differential --rust-port 18102 --c-port 18202')" \
    "$specs" "differential spec shape"

# ---- unified (two drivers, QPS split, distinct pidfiles) ----
specs="$(compute_driver_specs unified 200 18102 18202 21800)"
line_count="$(printf '%s\n' "$specs" | grep -c .)"
assert_eq 2 "$line_count" "unified emits two specs"

# Parse the two specs.
redis_spec="$(printf '%s\n' "$specs" | sed -n '1p')"
riak_spec="$(printf '%s\n' "$specs" | sed -n '2p')"

redis_suffix="$(printf '%s' "$redis_spec" | cut -f1)"
redis_qps="$(printf '%s' "$redis_spec" | cut -f2)"
redis_flags="$(printf '%s' "$redis_spec" | cut -f3)"
riak_suffix="$(printf '%s' "$riak_spec" | cut -f1)"
riak_qps="$(printf '%s' "$riak_spec" | cut -f2)"
riak_flags="$(printf '%s' "$riak_spec" | cut -f3)"

assert_eq "-redis" "$redis_suffix" "unified first spec is redis"
assert_eq "-riak" "$riak_suffix" "unified second spec is riak"
assert_eq "100" "$redis_qps" "unified redis qps half"
assert_eq "100" "$riak_qps" "unified riak qps half"
assert_eq "--mode redis --noxu-compat" "$redis_flags" "unified redis flags"
assert_eq "--mode riak --riak-pbc-port 21800" "$riak_flags" "unified riak flags"

# QPS must sum to the configured total even when odd.
specs_odd="$(compute_driver_specs unified 201 18102 18202 21800)"
sum=0
while IFS=$'\t' read -r _suffix q _flags; do
    [ -z "$q" ] && continue
    sum=$(( sum + q ))
done <<<"$specs_odd"
assert_eq 201 "$sum" "unified odd qps preserved (no load dropped)"

# ---- pidfile mapping ----
assert_eq "/run/workload.pid" "$(driver_pidfile_for "" /run)" \
    "empty suffix maps to workload.pid"
assert_eq "/run/driver-redis.pid" "$(driver_pidfile_for "-redis" /run)" \
    "redis suffix maps to driver-redis.pid"
assert_eq "/run/driver-riak.pid" "$(driver_pidfile_for "-riak" /run)" \
    "riak suffix maps to driver-riak.pid"

printf '\n==> summary: PASS=%d FAIL=%d\n' "$PASS" "$FAIL"
if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
exit 0
