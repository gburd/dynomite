# Chaos failure injectors

These scripts are invoked by the Stage 16 chaos test
(`crates/dynomite/tests/stage_16_chaos.rs`). They are also
runnable by hand for ad-hoc reproduction of a specific
failure mode.

| Script              | Purpose                                              | Requirements             |
|---------------------|------------------------------------------------------|--------------------------|
| `partition_dc.sh`   | Drop 100% of traffic between two ports               | `tc` + `CAP_NET_ADMIN`   |
| `slow_peer.sh`      | 200 ms one-way delay on a port                       | `tc` + `CAP_NET_ADMIN`   |
| `flap.sh`           | 1-second on/off connectivity flaps                   | `tc` + `CAP_NET_ADMIN`   |
| `gc_pause.sh`       | SIGSTOP a child PID for N seconds, then SIGCONT      | permission to signal PID |
| `clock_skew.sh`     | Launch a command under faketime with a skew offset   | `faketime` (libfaketime) |

Every script emits a single-line JSON status object on
stdout (and skip notices on stderr) so the chaos harness can
parse the outcome deterministically. When prerequisites are
missing the script prints `{"status":"skip", "reason": ...}`
and exits 0; the harness counts the skip and decrements the
expected coverage axis rather than failing the run.

The shared `_lib.sh` provides the capability check and
loopback-qdisc helpers. The chaos test runs entirely on the
loopback device so all `tc` operations target `lo`. Override
with `NETEM_DEV=eth0 ./partition_dc.sh ...` for ad-hoc use
on a real interface.
