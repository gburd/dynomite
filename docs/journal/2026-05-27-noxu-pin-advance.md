# 2026-05-27 - Noxu pin advances from 74b739b -> 9076650

## Summary

Path-deps follow `~/ws/lamdb` working tree, so simply rebuilding
the workspace updates our effective Noxu version. Lamdb advanced
from `74b739b` (v2.0.0 GA) to `9076650`
(`fix(noxu-persist): allow read-only reopen of an existing entity
store`).

User confirmed "New Noxu release to use v2.0.0", and we are
already past the GA release into the patch series.

## Tests after rebuild

- 431 dyn-riak tests pass
- 82 dynomited tests under `--features riak` pass
- No Cargo.lock churn (path-dep commits are not stored in the
  lockfile)
- Cargo build clean across the workspace

## Note

The "Noxu version" we report is therefore "lamdb HEAD as of build
time". This has advantages (always picks up bug fixes) and
disadvantages (build outputs are not reproducible across days
without pinning lamdb to a sha).

For a release tag we should freeze lamdb to a sha and document
that sha in the release notes. Pre-release we keep the path-dep
lazy-follow behavior.

## References

- `docs/journal/2026-05-26-noxu-2-0-0-rc1.md` - the original bump
  to GA.
- `~/ws/lamdb` `9076650` - current effective Noxu HEAD.
