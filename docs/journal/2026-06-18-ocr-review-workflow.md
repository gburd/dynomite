# 2026-06-18 Open Code Review (OCR) AI PR review workflow

## What this adds

A GitHub Actions workflow, `.github/workflows/ocr-review.yml`, that runs
the Open Code Review (`ocr`) tool against a pull request diff and posts
AI-generated review comments. It is **additive** to the existing CI
(`.github/workflows/ci.yml`, which runs `scripts/check.sh`) and does NOT
gate merges. OCR comments are advisory.

Files:

- `.github/workflows/ocr-review.yml` -- the workflow.
- `.github/ocr/litellm.yaml` -- LiteLLM proxy config (Bedrock bridge).
- `.github/ocr/rule.json` -- per-file-glob review rules encoding the
  Dynomite conventions from AGENTS.md.

## What OCR is

`ocr` (`@alibaba-group/open-code-review`) is an LLM-driven reviewer. It
reads the diff and surrounding file content from git refs (it does not
need the working tree), asks an LLM to review each changed hunk against
a rule set, and emits JSON. The workflow turns that JSON into inline PR
review comments via the auto `GITHUB_TOKEN`.

The model is Claude Opus on AWS Bedrock. OCR speaks the OpenAI chat
protocol, so the workflow starts a **LiteLLM** proxy on `127.0.0.1:4000`
for the life of the job and points OCR at it; LiteLLM bridges to Bedrock.
The proxy and its master key never leave the runner.

## How to trigger it

- **Automatically**: any `pull_request` event (opened, synchronize,
  reopened, ready_for_review). Drafts are reviewed too.
- **On demand**: comment `/open-code-review` (or `@open-code-review`) on
  a PR.
- **Manually**: `workflow_dispatch` with the PR number as input.

Concurrency is keyed per PR with `cancel-in-progress`, so a new push
supersedes the running review and saves Bedrock spend.

## Repo variables it needs (no secrets)

Set as repo **variables** (Settings -> Secrets and variables -> Actions
-> Variables). Already set on `gburd/dynomite`:

- `AWS_REGION` -- e.g. `us-east-1`.
- `AWS_ROLE_ARN` -- IAM role assumed via GitHub OIDC; must grant
  `bedrock:InvokeModel*`.
- `OCR_BEDROCK_MODEL` -- LiteLLM model string for the Opus inference
  profile, e.g. `bedrock/converse/us.anthropic.claude-opus-4-8`.

No static AWS keys. Auth is GitHub OIDC: the `id-token: write`
permission lets the job mint an OIDC token that
`aws-actions/configure-aws-credentials@v6` exchanges for temporary
Bedrock credentials.

## IAM OIDC trust policy

The IAM role in `AWS_ROLE_ARN` must trust the GitHub OIDC provider and
its trust policy `Condition` must allow this repository's subject claim:

```
"token.actions.githubusercontent.com:sub": "repo:gburd/dynomite:*"
```

Without that subject in the trust policy, the
"Configure AWS credentials (OIDC)" step fails to assume the role and the
review cannot run.

## What was dropped from the source

The upstream (gburd/postgres) workflow has a second `pg-history` job that
ties a PR to pgsql-hackers mailing-list and commit history via the Agora
MCP server. That is PostgreSQL-specific; Dynomite has no equivalent
mailing list, so the job and its `pg-history.py` helper are NOT ported.
The Dynomite review stays focused on the diff plus the AGENTS.md /
PLAN.md conventions encoded in `rule.json`.

## Forge scope

OCR is GitHub-only. The Codeberg Forgejo mirror does not run it; no
`.forgejo/` equivalent is added.
