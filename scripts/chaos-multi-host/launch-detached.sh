#!/usr/bin/env bash
#
# Launch the chaos coordinator fully detached from the calling
# shell so that even an aggressive process-group SIGKILL on the
# parent (e.g. pi-tool's process supervisor) cannot take it down.
#
# Uses `setsid` to start a new session, redirects all I/O so
# closing the controlling terminal does not propagate SIGHUP,
# and writes the resulting pid to a caller-specified file.
#
# Usage:
#   RUN_ID=foo CHAOS_DURATION_SECS=7200 \
#     scripts/chaos-multi-host/launch-detached.sh \
#       /tmp/chaos.log /tmp/chaos.pid

set -euo pipefail

LOG_FILE="${1:?log file path required}"
PID_FILE="${2:?pid file path required}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# setsid runs the coordinator in its own session: no controlling
# terminal, no inherited process group, immune to terminal SIGHUP
# and parent-shell exit. The trailing `&` puts it in background;
# `disown` removes it from the shell's job control so even an
# explicit `kill %%` from a wrapper cannot find it.
setsid bash -c "
    exec >>'$LOG_FILE' 2>&1 </dev/null
    echo \"\$\$\" > '$PID_FILE'
    bash '$SCRIPT_DIR/coordinator.sh'
" </dev/null >/dev/null 2>&1 &
DISOWN_PID=$!
disown $DISOWN_PID 2>/dev/null || true

# Give setsid a moment to write the pid file with the actual
# session leader pid (which is NOT the same as $!).
for i in $(seq 1 20); do
    if [ -f "$PID_FILE" ]; then
        break
    fi
    sleep 0.1
done

if [ -f "$PID_FILE" ]; then
    echo "coordinator detached, session leader pid=$(cat $PID_FILE), log=$LOG_FILE"
else
    echo "WARNING: coordinator pid file not written; check $LOG_FILE"
    exit 1
fi
