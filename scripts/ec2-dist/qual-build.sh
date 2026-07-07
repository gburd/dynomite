#!/usr/bin/env bash
# Node-side build: Rust dynomited (--no-default-features --features riak)
# + the C Netflix dynomite reference, for whatever arch this node is.
# No `set -e`: failures are handled explicitly so a `pkill` returning
# nonzero (no process to kill) does not abort the script.
ARCH=$(uname -m)
echo "BUILD_BEGIN $ARCH $(date -u +%H:%M:%S)"

sudo dnf install -y -q gcc gcc-c++ make cmake clang openssl-devel perl git \
  autoconf automake libtool pkgconfig 2>&1 | tail -1

# Rust toolchain
if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --default-toolchain 1.95.0 --profile minimal 2>&1 | tail -2
fi
. "$HOME/.cargo/env"

# Rust build
BD="$HOME/build-$(date +%s)"
mkdir -p "$BD" && tar xzf ~/src.tgz -C "$BD"
ln -sfn "$BD" ~/buildcur
cd ~/buildcur || { echo "BUILD_DONE (no src)"; exit 1; }
cargo build --release -p dynomited --no-default-features --features riak 2>&1 | tail -4
if [ -f target/release/dynomited ]; then
  sudo pkill -9 -x dynomited 2>/dev/null
  sleep 1
  cp target/release/dynomited ~/dynomited
  echo "RUST_READY $ARCH $(md5sum ~/dynomited | cut -d' ' -f1)"
else
  echo "RUST_FAILED $ARCH"
fi

# C build
CD="$HOME/cbuild-$(date +%s)"
mkdir -p "$CD" && tar xzf ~/cref.tgz -C "$CD"
cd "$CD" || { echo "BUILD_DONE (no cref)"; exit 1; }
export CFLAGS="-fcommon -Wno-implicit-function-declaration -Wno-error"
autoreconf -fvi >/dev/null 2>&1
./configure >/dev/null 2>&1
make -j4 2>&1 | tail -4
if [ -f src/dynomite ]; then
  cp src/dynomite ~/dynomite-c
  echo "C_READY $ARCH $(~/dynomite-c --version 2>&1 | head -1)"
else
  echo "C_FAILED $ARCH"
fi

echo "BUILD_DONE $ARCH $(date -u +%H:%M:%S)"
