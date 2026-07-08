# 45-node qualification: DC_ONE "37.9%" root cause -- SG rule quota, not a product bug

Date: 2026-07-08
Run: dyn-qual-20260707-172759

## Symptom

The full-qualification matrix phase kept failing. DC_ONE scored a
deterministic ~37.9% (identical across four nodes in different regions
and both architectures) while DC_QUORUM scored 100% on the same nodes.
Rust returned `-Dynomite: Failed to achieve Quorum` for most GET/SET/DEL
under DC_ONE; C returned the correct value.

## Investigation (each step ruled out a hypothesis)

1. NOT a launch race / stale binary. A clean, uncontested launch on one
   node (poll-until-process-gone, then launch, then wait-for-bind) came
   up every time. The earlier intermittent EADDRINUSE was (a) a real
   product bug -- the stats listener lacked SO_REUSEADDR, fixed in
   c1ab4be -- and (b) my own manual probes contending with the running
   orchestrator for the same node (self-inflicted; the discipline note
   about not measuring a system you are perturbing applies).

2. NOT stale backend data. FLUSHALL between consistency levels did not
   change the 37.9%.

3. NOT gossip convergence timing. After 692s uptime (11+ min, well past
   convergence) remote-routed keys STILL failed; only locally-routed
   keys (the node's own token range) succeeded. Deterministic, not
   timing.

4. The real signal: direct `valkey-cli -p 9102 SET <k>` on the Rust
   proxy succeeded ONLY for keys whose replica is the local node;
   every key routing to a remote replica returned "Failed to achieve
   Quorum". The dispatcher's fan-out could not send to the remote
   replica (`fanout_send` -> `peer_backends` channel try_send, receiver
   = peer supervisor, which was NOT connected), so `sent + hinted == 0`
   -> no_quorum_error. Correct behaviour given the input.

5. Root cause: the peer supervisors logged "peer connect timed out;
   retrying" for the cross-region peers. From use1, a TCP probe to a
   remote node's public :9101 was BLOCKED. The peers were UP (41/45
   Rust listeners bound); the dnode port was not REACHABLE cross-region.

6. Definitive: the per-region security group holds exactly 60 ingress
   rules (the default RulesPerSecurityGroup quota) covering only 20
   distinct /32 IPs (20 IPs x 3 port-groups: 22, 8087-9102, 22222-22223
   = 60 rules). The 45-node topology needs all 45 peer public IPs
   authorized in every region's SG -> 45 x 3 = 135 rules, more than
   double the quota. `authorize-security-group-ingress` failed silently
   for the last 25 nodes (the `|| true` swallowed
   RulesPerSecurityGroupLimitExceeded). So 25 of 45 nodes were
   unreachable on the dnode port, the full mesh never formed, and any
   DC_ONE key whose replica lived on an unreachable node failed. DC_QUORUM
   passed because its quorum could be met among the reachable in-region
   replicas.

## Conclusion

This is a HARNESS INFRASTRUCTURE LIMIT, not a Dynomite/Rust defect. The
product behaves correctly: a DC_ONE write fans to its local-DC replicas
and, when none can be reached, returns the no-quorum error rather than
silently losing the write. The two earlier full-ring runs that scored
100% did so when the mesh happened to be reachable (fewer nodes / a
wider manual SG state).

## Fix

Use an AWS managed prefix list per region containing all 45 node /32s.
A prefix list is referenced by a single SG rule per port-group, so the
whole mesh costs 3 rules instead of 135, staying far under the 60-rule
quota. deploy-mixed.sh creates/updates the prefix list as nodes come up
and authorizes the SG against the prefix list rather than per-IP.

## Product bug that WAS found and fixed here

c1ab4be: StatsServer::bind used a plain TcpListener::bind (no
SO_REUSEADDR) while every other listener sets it; a fast restart failed
on the stats port and aborted the whole server build. Real defect,
regression-tested, shipped.
