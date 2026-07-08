# Release process

This page documents the procedure the project lead executes
to cut a tagged release. Workers do not run any of these
steps; they are the manual final-step closure that follows
Stage 16 review approval.

## Pre-flight checklist

Run these before touching git history. Every item must be
green.

```bash
nix develop
git status                                  # clean tree
scripts/check.sh                            # full local CI gate
scripts/check_clean.sh                      # cleanup-sweep
cargo public-api --diff-git-checkouts main  # no SemVer drift
mdbook build docs/book                      # docs render clean
```

Then the chaos test (the gate the C engine never had):

```bash
sudo -E env "PATH=$PATH" \
    cargo nextest run \
        --release --workspace \
        --features chaos \
        --test stage_16_chaos \
        --no-capture
```

The 1-hour run must report `STATUS: PASS` with zero invariant
violations. Curate its `target/chaos/<run-id>/report.md` into
`dist/chaos-reports/v0.1.0/report.md` and commit:

```bash
mkdir -p dist/chaos-reports/v0.1.0
cp target/chaos/<run-id>/report.md dist/chaos-reports/v0.1.0/
git add dist/chaos-reports/v0.1.0/report.md
git commit -s -m "release(chaos): v0.1.0 chaos run report"
```

## Author history rewrite

AGENTS.md Sections 14 / 14a require every commit on `main` to
be authored by `Greg Burd <greg@burd.me>`. The rewrite is the
last commit-mutating operation before the tag.

```bash
# 1. Snapshot the pre-rewrite SHAs for archaeology.
git log --format='%H %an <%ae>' main \
    > docs/journal/pre-rewrite-shas.md
git add docs/journal/pre-rewrite-shas.md
git commit -s -m "docs(journal): archive pre-rewrite SHAs"

# 2. Rewrite author + committer across the entire branch.
#    git-filter-repo is the supported tool; --force is needed
#    because we are operating in-place.
git filter-repo --force --commit-callback '
    commit.author_name = b"Greg Burd"
    commit.author_email = b"greg@burd.me"
    commit.committer_name = b"Greg Burd"
    commit.committer_email = b"greg@burd.me"
'

# 3. Verify a single line of authorship history.
git log --format='%an <%ae>' | sort -u
# expected: Greg Burd <greg@burd.me>
```

After the rewrite, no further commits land on `main` until
the tag is pushed.

## Signed tag

```bash
git tag -a -s v0.1.0 -m "dynomite Rust port v0.1.0

First production tag of the Rust dynomite engine.
See CHANGELOG.md for the per-stage summary."

git tag --verify v0.1.0
```

## Push to GitHub and Forgejo

The project ships dual-platform CI (AGENTS.md Section 14b);
the tag must reach both forges before the release is
considered shipped.

```bash
git push origin main
git push origin v0.1.0

# Codeberg / Forgejo mirror: the remote is named `forgejo`
# in the lead's git config.
git push forgejo main
git push forgejo v0.1.0
```

Verify both runners pass on the tag SHA:

* `https://github.com/gburd/dynomite/actions`
* `https://codeberg.org/gregburd/dynomite/actions`

## Container image

After the tag is pushed, build and tag the Docker image:

```bash
docker build -f dist/docker/Dockerfile \
    -t dynomite:0.1.0 \
    -t dynomite:latest \
    .
```

Push the tagged images to the registry of choice; the
project does not prescribe one.

## Post-release

* Open `[Unreleased]` section in `CHANGELOG.md` for the
  next iteration.
* Bump `workspace.package.version` in `Cargo.toml` to
  `0.2.0-dev` (or the next planned minor).
* Open the Stage-after-16 follow-up tickets enumerated in
  `docs/coverage-deviations.md` and `docs/journal/blocked.md`.
