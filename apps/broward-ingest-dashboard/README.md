# Broward persistent ingestion dashboard

`apps/broward-ingest-dashboard` is an isolated Vite application with a Vercel
Function at `GET /api/status`. The function reads only aggregate control tables
in Neon. The browser bundle has no database library, database query, credential,
folio, owner, address, source payload, raw error, or artifact path.

The design replaces the VM-local dashboard's filesystem and `/proc` dependency:

```text
ingestion writer
  -> Neon aggregate heartbeat/checkpoint tables
  -> pooled server-side Vercel Function
  -> no-store aggregate JSON
  -> auto-refreshing browser dashboard
```

The existing durable recovery ledger remains authoritative for recovery:

- `ingest_control.broward_appraisal_chunks`
- `ingest_control.broward_appraisal_terminal_items`
- `ingest_control.broward_appraisal_completed_items`
- `ingest_control.broward_appraisal_gates`
- `ingest_control.broward_appraisal_events`
- durable `broward_appraiser` properties after verified load commits

This package adds a small public-safe projection rather than replacing that
ledger. Nothing here starts, stops, deploys, or publishes the ingestion.

## Local mock mode

No secrets or local downloads are needed:

```bash
cd apps/broward-ingest-dashboard
npm ci
npm run dev:mock
```

Open `http://localhost:4783`. The page labels the values as mock data. Production
mock mode fails closed in the API even if `DASHBOARD_MOCK_MODE=true` is set.

Checks are also secret-free:

```bash
npm run typecheck
npm run test
npm run build
```

## Database contract

Apply the migration with the direct connection string:

```bash
cd apps/broward-ingest-dashboard
DATABASE_URL_UNPOOLED='postgresql://…direct Neon host…' npm run db:migrate
```

The migration script reads only `DATABASE_URL_UNPOOLED` and rejects a
`-pooler` host. It never prints the URL.

`ingest_control.broward_ingest_status` has exactly one fixed pipeline row:

| Field | Meaning |
| --- | --- |
| `pipeline_key` | Fixed `broward-appraisal` key |
| `denominator_count` | Fixed county denominator, 534,309 |
| `attempted_count` | Unique county seed rows represented by the checkpoint |
| `succeeded_count` | Verified durable successes |
| `source_miss_count` | Durable terminal source misses |
| `source_failure_count` | Cumulative retryable source failure attempts |
| `transform_failure_count` | Cumulative transform failure attempts |
| `load_failure_count` | Cumulative load failure attempts |
| `phase` | Fixed operational phase enum |
| `started_at` / `heartbeat_at` | Writer timing only |
| `stale_after_seconds` | Freshness threshold; defaults to 180 seconds |
| `throughput_window_seconds` | Duration represented by recent throughput |
| `throughput_attempted_count` | Attempts observed in that window |

Constraints require `succeeded + source misses <= attempted <= denominator`.
Completion is `(succeeded + source misses) / denominator`; retryable failure
attempts do not advance it.

`ingest_control.broward_ingest_category_coverage` stores only a constrained
Lexicon category key and succeeded count. The key must match
`^[A-Za-z][A-Za-z0-9]{0,63}$`. Do not store source use-code text or other labels.
The API repeats this validation and combines invalid values as `Other`.

The migration also installs the security-invoker function
`ingest_control.record_broward_ingest_status(...)`. One call atomically replaces
the heartbeat, counters, recent throughput, and complete category snapshot.

## Durable-recovery integration

Integrate after the durable-recovery work is committed; do not edit the active
agent's worktree. Call `record_broward_ingest_status` on the recovery's existing
database client:

1. On startup, write `pilot` or `full`.
2. Before/after long stages, write `capturing`, `transforming`, `loading`, or
   `verifying`. A chunk should heartbeat more often than the 180-second stale
   threshold.
3. In the same transaction that records a verified chunk, replace the dashboard
   snapshot.
4. Write `paused` before a controlled shutdown, `failed` after a handled fatal
   failure, and `complete` only after the full seed scan reconciles.

Use these mappings from the existing ledger:

| Dashboard argument | Durable source |
| --- | --- |
| attempted | Unique/capped checkpoint coverage maintained by the recovery; never use an uncapped retry total |
| succeeded | Current completed-item count after exact logical-row verification; visible property count must be at least this value |
| source misses | Current terminal-item count |
| source failures | Sum of `broward_appraisal_events` where stage is `source_error` |
| transform failures | Sum where stage is `transform_error` |
| load failures | Sum where stage is `load_error` |
| recent attempted | Verified chunk attempts committed inside the selected window |
| categories | Cumulative `state.usageTypes` / verified Lexicon usage aggregates |

The call shape is:

```sql
SELECT ingest_control.record_broward_ingest_status(
  $1::text,        -- phase
  $2::bigint,      -- attempted
  $3::bigint,      -- succeeded
  $4::bigint,      -- source misses
  $5::bigint,      -- retryable source failures
  $6::bigint,      -- transform failures
  $7::bigint,      -- load failures
  $8::integer,     -- throughput window seconds
  $9::bigint,      -- attempted in window
  $10::jsonb       -- {"Residential": 123, ...}; aggregate keys/counts only
);
```

Never pass folios, hashes, addresses, owner/contact data, errors, source
payloads, database identity, or paths to this function. The Vercel API queries
only the two dashboard tables, so a separate read-only database role can be
limited to:

```sql
GRANT USAGE ON SCHEMA ingest_control TO dashboard_reader;
GRANT SELECT ON
  ingest_control.broward_ingest_status,
  ingest_control.broward_ingest_category_coverage
TO dashboard_reader;
```

## Vercel setup

No deployment is required to build or test this package.

1. In Vercel, import the `oracle-node` repository as a new project.
2. Set **Root Directory** to `apps/broward-ingest-dashboard`.
3. Keep framework preset **Vite**. The repository supplies:
   - install command: `npm ci`
   - build command: `npm run build`
   - output directory: `dist`
4. Add `DATABASE_URL` to Preview and Production as a server-side environment
   variable. It must be the Neon pooled URL whose host contains `-pooler`.
5. Do not add a `VITE_DATABASE_URL` variable. Every `VITE_*` variable is public
   browser configuration.
6. Keep `DATABASE_URL_UNPOOLED` out of the deployed app. Use it only in the
   trusted migration environment for `npm run db:migrate`.
7. Leave `DASHBOARD_MOCK_MODE` unset in Production. It may be `true` in Preview
   only when an explicitly fake preview is desired.
8. Deploy only after the migration and ingestion heartbeat integration are in
   place. A missing aggregate row or database outage returns HTTP 503 with a
   fixed `offline` response.

The runtime validates that `DATABASE_URL` is pooled, creates a module-scoped
`pg` pool with at most two clients, and attaches its lifecycle to Vercel Fluid
Compute. Responses use `Cache-Control: no-store`.

## Authentication and deployment protection

This is an operational dashboard, not a public-data product. Enable **Vercel
Authentication** under Project Settings → Deployment Protection.

- On Pro/Enterprise, select **All Deployments** so production domains and
  previews both require a Vercel account with project access.
- Standard Protection does **not** protect the production domain. On Hobby,
  keep this project preview/non-production only or put it behind an
  authenticated gateway; do not publish an unprotected production URL.
- Do not create public Shareable Links or long-lived protection bypass tokens.
- Give dashboard viewers read-only project access and use a read-only Neon role
  when operationally possible.

Vercel protection must cover the whole project, including `/api/status`; hiding
only the HTML page would leave aggregate operations data directly reachable.

## Status semantics

- `online`: heartbeat age is within the configured threshold.
- `stale`: a heartbeat exists but is older than the threshold.
- `offline`: progress exists without a heartbeat, or phase is `paused`/`failed`.
- `not started`: the migration initialized the row but no attempt/heartbeat was
  recorded.
- `complete`: durable successes plus terminal misses equal the denominator.
- ETA is shown only for `online` state and positive recent throughput.
- A failed API request sets the browser badge and banner to `offline` while
  retaining previously rendered values as visibly outdated.
