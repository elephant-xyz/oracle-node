# feeder-watchdog

Self-healing, cloud-side watchdog for the property-first seed-feeder — one
dedicated Lambda + EventBridge schedule per county.

## What it is

The seed-feeder occasionally stops self-requeuing mid-run: its S3 checkpoint
(`feeder-state.json`) stops advancing while its event-source mapping stays
Enabled. The documented recovery is to re-send the same feeder message, which
resumes from the checkpoint (idempotent). This module automates that recovery.

An EventBridge rule fires the Lambda every 5 minutes. On each invocation it reads
the checkpoint and, if the county is not yet exhausted and the checkpoint has
gone stale, re-sends the byte-identical feeder message baked in at deploy time.

## Why (vs the laptop bash watchdog)

The previous watchdog was a `nohup` bash loop on an operator's laptop — it died
if the machine slept, lost Wi-Fi, or was closed overnight. This runs entirely in
AWS (Lambda + EventBridge), so recovery no longer depends on a laptop staying
awake, and the logic ships in the repo instead of living only in someone's shell.

## Files

- `index.mjs` — the handler. Uses the AWS SDK v3 bundled in `nodejs22.x` (no
  `node_modules`). Reads `STATE_BUCKET`/`STATE_KEY`, `FEEDER_QUEUE_URL`,
  `FEEDER_MESSAGE`, `STALE_SECONDS` from the environment.
- `package.json` — minimal ESM marker, no dependencies.
- `deploy.sh` — parameterized, idempotent, per-county deploy.

## Usage

Requires `AWS_PROFILE` and `AWS_REGION` in the environment, plus node >= 22,
python3, zip, and AWS CLI v2.

Preview the resolved plan (makes no changes):

```bash
AWS_PROFILE=elephant-oracle-node AWS_REGION=us-east-1 \
  bash workflow/lambdas/feeder-watchdog/deploy.sh \
    --dry-run \
    --county Orange \
    --job-id orange-property-first-seed-all-20260702 \
    --stale-seconds 1200
```

Deploy for Orange:

```bash
AWS_PROFILE=elephant-oracle-node AWS_REGION=us-east-1 \
  bash workflow/lambdas/feeder-watchdog/deploy.sh \
    --county Orange \
    --job-id orange-property-first-seed-all-20260702 \
    --stale-seconds 1200
```

Deploy for any other county (multi-word names allowed):

```bash
AWS_PROFILE=elephant-oracle-node AWS_REGION=us-east-1 \
  bash workflow/lambdas/feeder-watchdog/deploy.sh \
    --county "Palm Beach" \
    --job-id palm-beach-property-first-seed-all-20260702
```

By default the sender is `scripts/send-<county-key>-seed-feeder.mjs`, where
`county-key` is the county lowercased with spaces turned into hyphens
(`Palm Beach` -> `palm-beach`). Override with `--sender-script <path>`.

## STALE_SECONDS guidance

`--stale-seconds` controls how old the checkpoint must be before a re-send. The
default is 900. Raise it (e.g. `1200`) when a county is backpressure-gated: the
feeder legitimately pauses while the workflow queue drains, and a too-low
threshold would treat those expected pauses as stalls and fire spurious
re-sends. Set it comfortably above the longest expected healthy pause.

## Per-county isolation and cleanup

Each county gets its own role, Lambda, and rule:

- Lambda `<county-key>-feeder-watchdog`
- Role `<county-key>-feeder-watchdog-role`
- Rule `<county-key>-feeder-watchdog-schedule`

Roles are never shared across counties — that keeps lifecycles independent and
avoids the AccessDenied seen when reusing another county's role.

After a run finishes (`sourceExhausted: true`), tear the county's watchdog down:

```bash
aws events remove-targets --rule <county-key>-feeder-watchdog-schedule --ids 1
aws events delete-rule --name <county-key>-feeder-watchdog-schedule
aws lambda delete-function --function-name <county-key>-feeder-watchdog
aws iam delete-role-policy --role-name <county-key>-feeder-watchdog-role --policy-name watchdog-perms
aws iam detach-role-policy --role-name <county-key>-feeder-watchdog-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
aws iam delete-role --role-name <county-key>-feeder-watchdog-role
```
