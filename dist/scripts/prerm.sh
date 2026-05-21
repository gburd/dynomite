#!/bin/sh
# Pre-removal hook for deb / rpm packages of the Rust port of
# dynomite. Stops the unit if it is running so the package
# manager is not racing systemd while the binary is being
# unlinked.
set -eu

if command -v systemctl >/dev/null 2>&1; then
    if systemctl is-active --quiet dynomited.service 2>/dev/null; then
        systemctl stop dynomited.service >/dev/null 2>&1 || true
    fi
    if systemctl is-enabled --quiet dynomited.service 2>/dev/null; then
        systemctl disable dynomited.service >/dev/null 2>&1 || true
    fi
fi

# We deliberately leave the dynomite system user, /etc/dynomite,
# /var/lib/dynomite, /var/log/dynomite, and /var/run/dynomite
# in place: they may contain operator-supplied state that
# survives a package removal. A separate `purge` hook (deb only)
# is the right place to scrub those, and is intentionally not
# provided here.

exit 0
