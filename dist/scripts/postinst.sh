#!/bin/sh
# Post-install hook for deb / rpm packages of the Rust port of
# dynomite. Invoked by the package manager after the binary and
# config files have been laid down on the filesystem.
#
# Idempotent: running the same hook twice is safe and a no-op
# the second time.
set -eu

DYNOMITE_USER=${DYNOMITE_USER:-dynomite}
DYNOMITE_GROUP=${DYNOMITE_GROUP:-dynomite}
DYNOMITE_HOME=${DYNOMITE_HOME:-/var/lib/dynomite}

# Create the system user if missing. Both deb and rpm ship
# `useradd`/`groupadd`, but their flags differ slightly; we use
# the conservative subset.
if ! getent group "$DYNOMITE_GROUP" >/dev/null 2>&1; then
    groupadd --system "$DYNOMITE_GROUP"
fi
if ! getent passwd "$DYNOMITE_USER" >/dev/null 2>&1; then
    useradd --system \
        --gid "$DYNOMITE_GROUP" \
        --home "$DYNOMITE_HOME" \
        --shell /usr/sbin/nologin \
        --comment "Dynomite Rust server" \
        "$DYNOMITE_USER"
fi

# Runtime directories. Created before the unit starts so the
# pidfile path under /var/run/dynomite resolves on boot.
for dir in /etc/dynomite /var/run/dynomite /var/lib/dynomite /var/log/dynomite; do
    install -d -o "$DYNOMITE_USER" -g "$DYNOMITE_GROUP" -m 0750 "$dir"
done

# Reload systemd so the new unit is picked up. The unit is NOT
# enabled or started here; the operator opts in explicitly.
if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload >/dev/null 2>&1 || true
fi

exit 0
