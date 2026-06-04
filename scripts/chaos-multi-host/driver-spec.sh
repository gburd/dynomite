#!/usr/bin/env bash
#
# Workload-driver fan-out helper for the multi-host chaos
# coordinator.
#
# The coordinator launches one workload-driver.py per host for
# most modes, but MODE=unified drives a SINGLE shared in-process
# Noxu datastore through TWO client wire protocols at once: a
# Redis RESP (plus RediSearch FT.*) driver against the engine's
# client_listen and a Riak PBC driver against the riak.pbc_listen
# port. compute_driver_specs centralises the per-mode mapping so
# the launch path stays a simple loop over the emitted specs.
#
# The file is sourceable: when sourced (BASH_SOURCE != $0) it
# defines compute_driver_specs and returns without side effects.
# test_driver_spec.sh sources it directly.

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
# MODE=unified two specs are emitted with api_suffix "-redis" and
# "-riak"; the configured QPS is split evenly between them so the
# total offered load is unchanged, and each driver writes
# workload-<label>-<api>.ndjson with a distinct
# driver-<api>.pid pidfile.
compute_driver_specs() {
    local mode="$1"
    local qps="$2"
    local client_port="$3"
    local client_port_c="$4"
    local riak_pbc_port="$5"

    case "$mode" in
        unified)
            # Split the offered load so the two protocols sum to
            # the configured QPS. The remainder goes to the Riak
            # driver so an odd QPS is never silently dropped.
            local redis_qps=$(( qps / 2 ))
            local riak_qps=$(( qps - redis_qps ))
            printf '%s\t%s\t%s\n' "-redis" "$redis_qps" "--mode redis --noxu-compat"
            printf '%s\t%s\t%s\n' "-riak" "$riak_qps" \
                "--mode riak --riak-pbc-port $riak_pbc_port"
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
# tooling for non-unified modes is unaffected; suffixed drivers
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
