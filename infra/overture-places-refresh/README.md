# Overture places monthly refresh

This stack runs Lee's Overture places refresh in ECS Fargate through Step
Functions. It reuses the existing query-db loader and Filebase publisher.

The EventBridge Scheduler schedule is always created with `State: DISABLED`.
The scheduled input also has `publishApproved:false`, so deploying this template
cannot silently publish or start monthly spend.

## Manual validation

Start with a read-only STAC/Neon plan:

```sh
aws stepfunctions start-execution \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --name "lee-plan-$(date -u +%Y%m%dT%H%M%SZ)" \
  --input '{"county":"lee","countyFips":"12071","boundarySource":"tiger/tl_2024_us_county","dryRun":true,"publishApproved":false,"costCeilingUsd":10}'
```

If the plan identifies a newer release, run extraction, validation, load, and
full-current export without changing Filebase/IPNS:

```sh
aws stepfunctions start-execution \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --name "lee-validate-$(date -u +%Y%m%dT%H%M%SZ)" \
  --input '{"county":"lee","countyFips":"12071","boundarySource":"tiger/tl_2024_us_county","dryRun":false,"publishApproved":false,"costCeilingUsd":10}'
```

Use `releaseOverride` only for a deliberate replay:

```json
{"releaseOverride":"2026-08-19.0"}
```

Publication requires `publishApproved:true` on a separate manual execution.
Do not use that value until the code is merged and the validation-only run has
succeeded.

## Enable the monthly schedule after approval

Enabling requires an explicit input change as well as changing Scheduler state.
First save the deployed target, then change only its input:

```sh
aws scheduler get-schedule \
  --name oracle-overture-places-refresh-monthly \
  --query Target > /tmp/overture-places-target.json

jq '.Input = "{\"county\":\"lee\",\"countyFips\":\"12071\",\"boundarySource\":\"tiger/tl_2024_us_county\",\"dryRun\":false,\"publishApproved\":true,\"costCeilingUsd\":10}"' \
  /tmp/overture-places-target.json > /tmp/overture-places-target-approved.json

aws scheduler update-schedule \
  --name oracle-overture-places-refresh-monthly \
  --schedule-expression 'cron(0 8 2 * ? *)' \
  --flexible-time-window '{"Mode":"OFF"}' \
  --state ENABLED \
  --target file:///tmp/overture-places-target-approved.json
```

This enable action is intentionally outside deployment. It requires separate
operator approval after the manual workflow has passed.
