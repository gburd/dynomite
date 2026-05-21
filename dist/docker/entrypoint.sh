#!/bin/sh
# Container entrypoint for the dynomited image.
#
# Optionally launches a local redis-server on 22122 (matching
# the C reference image) before handing control to dynomited.
# Set DYNOMITE_BACKEND=none to skip the redis launch when
# running against an external backend.
set -eu

if [ "${DYNOMITE_BACKEND:-redis}" = "redis" ]; then
    redis-server \
        --bind 127.0.0.1 \
        --port 22122 \
        --daemonize no \
        --save "" \
        --appendonly no \
        --protected-mode no &
    REDIS_PID=$!
    trap 'kill -TERM "$REDIS_PID" 2>/dev/null || true' INT TERM
fi

exec /usr/local/sbin/dynomited \
    -c "${DYNOMITED_CONF:-/etc/dynomite/dynomite.yml}" \
    "$@"
