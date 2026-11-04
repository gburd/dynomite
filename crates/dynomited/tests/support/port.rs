//! TCP port reservation shared by the test binaries that spawn
//! `dynomited` or `valkey-server` child processes.

use std::net::TcpListener;

/// Reserve a TCP port for a child process to bind.
///
/// Ports come from below the kernel's ephemeral range (32768 and up on
/// Linux), so no port-0 bind or outgoing connection elsewhere on the host
/// can take one between this check and the child's own bind. Taking an
/// ephemeral port and releasing it lost that race under load: the node
/// failed to bind and the readiness probe connected to whichever process
/// held the port instead. Each test process starts at a pid-derived offset
/// so parallel test processes walk different ports.
pub fn pick_port() -> u16 {
    use std::sync::atomic::{AtomicU32, Ordering};
    const LO: u32 = 10_000;
    const SPAN: u32 = 22_000;
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let seed = std::process::id().wrapping_mul(2_654_435_761) % SPAN;
    for _ in 0..SPAN {
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let port = u16::try_from(LO + (seed + n) % SPAN).expect("invariant: below 32000");
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
    panic!("no free TCP port in {LO}..{}", LO + SPAN);
}
