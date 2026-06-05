#!/usr/bin/env bash
#
# Workload-driver fan-out helper for the multi-host chaos
# coordinator.
#
# The coordinator launches one workload-driver.py per host for
# most modes, but MODE=combined runs THREE independent dynomited
# instances per host (one per backend: redis, memcache, riak)
# on distinct port bands, and therefore THREE workload drivers,
# one per instance, each in its native protocol. compute_driver_specs
# centralises the per-mode mapping so the launch path stays a
# simple loop over the emitted specs.
#
# The file is sourceable: when sourced (BASH_SOURCE != $0) it
# defines compute_driver_specs and returns without side effects.
# test_driver_spec.sh sources it directly.

# Combined-mode port-band offsets. Each host runs three
# dynomited instances; the redis instance keeps the base ports,
# the memcache instance is shifted +1000, and the riak instance
# is shifted +2000 on every port (client/dyn/stats/backend/PBC).
# start-host.sh derives the same offsets from the INSTANCE env;
# the constants are duplicated here only so compute_driver_specs
# can point each driver at the right band without an extra
# argument.
COMBINED_OFFSET_REDIS=0
COMBINED_OFFSET_MEMCACHE=1000
COMBINED_OFFSET_RIAK=2000

# compute_driver_specs MODE QPS CLIENT_PORT CLIENT_PORT_C RIAK_PBC_PORT
#
# Emits one driver spec per line. Each spec is three
# tab-separated fields:
#
#   <api_suffix>\t<qps>\t<mode_flags>
#
# api_suffix is the empty string for single-driver modes; the
# driver then writes workload-<label>.ndjson and records its pid
# in workload.pid (the legacy layout, untouched). For
# MODE=combined three specs are emitted with api_suffix "-redis",
# "-memcache", and "-riak"; the configured QPS is split three ways
# (remainder to the Riak driver) so the total offered load is
# unchanged, and each driver writes workload-<label>-<api>.ndjson
# with a distinct driver-<api>.pid pidfile. Each combined driver
# dials its own port band (redis +0, memcache +1000, riak +2000).
compute_driver_specs() {
    local mode="$1"
    local qps="$2"
    local client_port="$3"
    local client_port_c="$4"
    local riak_pbc_port="$5"

    case "$mode" in
        combined)
            # Split the offered load three ways so the protocols
            # sum to the configured QPS. The remainder goes to
            # the Riak driver so an indivisible QPS is never
            # silently dropped.
            local redis_qps=$(( qps / 3 ))
            local memcache_qps=$(( qps / 3 ))
            local riak_qps=$(( qps - redis_qps - memcache_qps ))
            local redis_client=$(( client_port + COMBINED_OFFSET_REDIS ))
            local memcache_client=$(( client_port + COMBINED_OFFSET_MEMCACHE ))
            local riak_band_pbc=$(( riak_pbc_port + COMBINED_OFFSET_RIAK ))
            printf '%s\t%s\t%s\n' "-redis" "$redis_qps" \
                "--mode redis --port $redis_client"
            printf '%s\t%s\t%s\n' "-memcache" "$memcache_qps" \
                "--mode memcache --port $memcache_client"
            printf '%s\t%s\t%s\n' "-riak" "$riak_qps" \
                "--mode riak --riak-pbc-port $riak_band_pbc"
            ;;
        riak)
            printf '%s\t%s\t%s\n' "" "$qps" \
                "--mode riak --riak-pbc-port $riak_pbc_port"
            ;;
        differential)
            printf '%s\t%s\t%s\n' "" "$qps" \
                "--mode differential --rust-port $client_port --c-port $client_port_c"
            ;;
        *)
            printf '%s\t%s\t%s\n' "" "$qps" "--mode $mode"
            ;;
    esac
}

# driver_pidfile_for API_SUFFIX RUN_DIR
#
# Map an api_suffix to its pidfile path. The empty suffix keeps
# the legacy workload.pid name so existing teardown / status
# tooling for non-combined modes is unaffected; suffixed drivers
# get driver-<api>.pid (driver-redis.pid, driver-riak.pid).
driver_pidfile_for() {
    local api_suffix="$1"
    local run_dir="$2"
    if [ -z "$api_suffix" ]; then
        printf '%s/workload.pid' "$run_dir"
    else
        printf '%s/driver%s.pid' "$run_dir" "$api_suffix"
    fi
}
