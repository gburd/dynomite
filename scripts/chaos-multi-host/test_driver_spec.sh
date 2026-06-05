#!/usr/bin/env bash
#
# Self-test for scripts/chaos-multi-host/driver-spec.sh.
#
# Sources the helper and asserts compute_driver_specs / the
# pidfile mapping produce the expected per-mode fan-out. The
# combined case is the load-bearing one: it must emit exactly
# three specs (redis + memcache + riak) with the QPS split so
# the total is preserved, distinct band-shifted ports, and no
# --noxu-compat anywhere.
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

# ---- combined (three drivers, QPS split three ways, bands) ----
specs="$(compute_driver_specs combined 200 18102 18202 21800)"
line_count="$(printf '%s\n' "$specs" | grep -c .)"
assert_eq 3 "$line_count" "combined emits three specs"

# Parse the three specs.
redis_spec="$(printf '%s\n' "$specs" | sed -n '1p')"
memcache_spec="$(printf '%s\n' "$specs" | sed -n '2p')"
riak_spec="$(printf '%s\n' "$specs" | sed -n '3p')"

redis_suffix="$(printf '%s' "$redis_spec" | cut -f1)"
redis_qps="$(printf '%s' "$redis_spec" | cut -f2)"
redis_flags="$(printf '%s' "$redis_spec" | cut -f3)"
memcache_suffix="$(printf '%s' "$memcache_spec" | cut -f1)"
memcache_qps="$(printf '%s' "$memcache_spec" | cut -f2)"
memcache_flags="$(printf '%s' "$memcache_spec" | cut -f3)"
riak_suffix="$(printf '%s' "$riak_spec" | cut -f1)"
riak_qps="$(printf '%s' "$riak_spec" | cut -f2)"
riak_flags="$(printf '%s' "$riak_spec" | cut -f3)"

assert_eq "-redis" "$redis_suffix" "combined first spec is redis"
assert_eq "-memcache" "$memcache_suffix" "combined second spec is memcache"
assert_eq "-riak" "$riak_suffix" "combined third spec is riak"

# QPS split three ways (200/3 = 66, 66, remainder 68).
assert_eq "66" "$redis_qps" "combined redis qps third"
assert_eq "66" "$memcache_qps" "combined memcache qps third"
assert_eq "68" "$riak_qps" "combined riak qps remainder"

# Each driver dials its own band: redis +0, memcache +1000, riak
# PBC +2000.
assert_eq "--mode redis --port 18102" "$redis_flags" "combined redis flags"
assert_eq "--mode memcache --port 19102" "$memcache_flags" \
    "combined memcache flags"
assert_eq "--mode riak --riak-pbc-port 23800" "$riak_flags" \
    "combined riak flags"

# No --noxu-compat anywhere in the combined fan-out.
if printf '%s' "$specs" | grep -q -- '--noxu-compat'; then
    fail "combined specs must not carry --noxu-compat"
else
    ok "combined specs free of --noxu-compat"
fi

# QPS must sum to the configured total even when indivisible.
sum=0
while IFS=$'\t' read -r _suffix q _flags; do
    [ -z "$q" ] && continue
    sum=$(( sum + q ))
done <<<"$specs"
assert_eq 200 "$sum" "combined qps preserved (no load dropped)"

specs_odd="$(compute_driver_specs combined 100 18102 18202 21800)"
sum=0
while IFS=$'\t' read -r _suffix q _flags; do
    [ -z "$q" ] && continue
    sum=$(( sum + q ))
done <<<"$specs_odd"
assert_eq 100 "$sum" "combined qps=100 preserved (no load dropped)"

# No mode anywhere should emit --noxu-compat.
for m in redis memcache riak differential combined; do
    allspecs="$(compute_driver_specs "$m" 200 18102 18202 21800)"
    if printf '%s' "$allspecs" | grep -q -- '--noxu-compat'; then
        fail "$m specs must not carry --noxu-compat"
    fi
done
ok "no mode emits --noxu-compat"

# ---- pidfile mapping ----
assert_eq "/run/workload.pid" "$(driver_pidfile_for "" /run)" \
    "empty suffix maps to workload.pid"
assert_eq "/run/driver-redis.pid" "$(driver_pidfile_for "-redis" /run)" \
    "redis suffix maps to driver-redis.pid"
assert_eq "/run/driver-riak.pid" "$(driver_pidfile_for "-riak" /run)" \
    "riak suffix maps to driver-riak.pid"
assert_eq "/run/driver-memcache.pid" "$(driver_pidfile_for "-memcache" /run)" \
    "memcache suffix maps to driver-memcache.pid"

printf '\n==> summary: PASS=%d FAIL=%d\n' "$PASS" "$FAIL"
if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
exit 0
