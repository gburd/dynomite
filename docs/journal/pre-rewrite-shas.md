# Pre-rewrite SHAs (for archaeology)

The v0.1.0 release runs `git filter-repo` to normalise every
commit's author + committer to `Greg Burd <greg@burd.me>` per
AGENTS.md Section 14a. This file records the pre-rewrite tip
SHAs so a future archaeologist can reconstruct the original
attribution chain.

| ref | pre-rewrite SHA | note |
|---|---|---|
| main HEAD | `72cfb89` | Stage 16 chaos hour report |
| v0.0.1 | (rewritten) | first crates.io release |
| v0.0.2 | (rewritten) | search-extract + perf wins |
| v0.0.3 | (rewritten) | ITC + FT.SUG* + dyniak-bench |

Distinct pre-rewrite authors (14 total):

- Dynomite Port Lead <agent@dynomite.local>
- dynomite-stage6-fix <dev@dynomite.local>
- Greg Burd <greg@burd.me>
- Reviewer (stage-2) <review-stage-2@local>
- Stage 12 Worker <agent@dynomite.local>
- stage-1-worker <stage-1-worker@dynomite.local>
- Stage 2 worker <agent@dynomite.local>
- Stage 3 worker <agent@dynomite.local>
- Stage 4 Worker <stage4@dynomite.local>
- Stage 5 Worker <agent-stage-5@dynomite.local>
- Stage 5 Worker <stage5@dynomite.local>
- Stage 7 Worker <stage7@worker.local>
- stage-9-net <stage-9-net@dynomite.local>
- stage-worker <stage@dynomite.local>

All collapse to a single `Greg Burd <greg@burd.me>` line in the
post-rewrite git log; the original commits remain in git's
reflog for ~90 days and are recoverable from any clone made
before the v0.1.0 push.

Total pre-rewrite commit count on main: 419.
