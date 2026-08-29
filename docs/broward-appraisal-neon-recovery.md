# Broward appraisal Neon recovery

Date: 2026-08-29  
Scope: official GIS seed → live BCPA capture → warm query-data-only transform
→ isolated Neon load. No AWS, permits, or publication.

## Mandatory target proof and current blocker

The recovery fails before control-table DDL or data loading unless PostgreSQL
server metadata matches all of:

- project: `raspy-frost-51580436`;
- an independently verified branch ID assigned to `broward-ingest`;
- an independently verified primary endpoint ID assigned to that branch;
- an endpoint ID outside the known production `ep-mute-leaf` prefix.

PostgreSQL exposes the immutable IDs through `neon.project_id`,
`neon.branch_id`, and `neon.endpoint_id`; it does not expose the human-readable
branch label. Therefore both IDs are required on every recovery and dashboard
command, and they must come from authenticated Neon branch metadata rather than
being copied from the connection being checked. The recovery denies the known
production endpoint prefix unconditionally. Neon permits one primary
read-write endpoint per branch, so the independently mapped endpoint and branch
ID are the write-isolation boundary.

Read-only preflight on 2026-08-29 found:

- PostgreSQL IDs: project `raspy-frost-51580436`, branch
  `br-old-cloud-aqz2hqjl`, endpoint `ep-still-flower-aq04hhgg`;
- `properties`: 0 `broward_appraiser` rows;
- `parcels`: 0 Broward/source/jurisdiction rows;
- `addresses`: 0 Broward/source rows;
- existing appraisal sources were Miami-Dade, Palm Beach, Lee, Orange, Santa
  Clara, Chester, and a 50-row Hillsborough pilot.

The pooled and unpooled variables resolve to the same endpoint identifier,
database, and role. Commands never print either connection string.

Neither Neon CLI nor Vercel CLI is authenticated in the recovery VM, and no
Neon API credential is present. Consequently the observed branch and endpoint
IDs cannot yet be independently mapped to the human-readable
`broward-ingest` branch. **No database DDL, pilot load, full load, or dashboard
has been started.** Supply authenticated read-only Neon metadata proving that
mapping before using any ID in a write-capable command. The observed IDs above
are evidence of the unresolved current connection, not approved command
arguments.

The persistent dashboard migration is subject to the same gate. Do not run it
and do not configure or deploy the Vercel project until authenticated Neon
metadata proves that both the direct migration URL and pooled runtime URL belong
to the same branch explicitly named `broward-ingest`.

## Required query-db patch

Use a fresh checkout at `/tmp/elephant-query-db`:

```bash
git clone https://github.com/elephant-xyz/elephant-query-db.git /tmp/elephant-query-db
cd /tmp/elephant-query-db
sha256sum /workspace/docs/patches/elephant-query-db-broward-local-loader.patch
git apply --check /workspace/docs/patches/elephant-query-db-broward-local-loader.patch
git am /workspace/docs/patches/elephant-query-db-broward-local-loader.patch
npm install
npm run typecheck
npm run test
```

Required SHA-256:
`bca216a3c68339abe31dca1b34ff524d4df23606972684837bc268ebae40c083`.
The patch applies to query-db base `15187e2` and creates two commits. The
verified suite is 30 test files / 398 tests.

The Broward county transform checkout is
`/tmp/Counties-trasform-scripts/broward/scripts`. It is never uploaded.

## Reboot-safe checkpoint

`scripts/recover-broward-appraisal-to-neon.mjs` treats every local path as a
cache:

1. If absent, it rebuilds `downloads/broward/broward.csv` from the official
   BCPA ArcGIS layer.
2. It refuses to continue unless the seed is exactly 534,309 valid, distinct
   folios and computes an ordered SHA-256 seed signature.
3. It reads visible folios from `properties` but skips a seed only when its
   one-way hash is present in
   `ingest_control.broward_appraisal_completed_items`. A property without that
   completion hash is replayed idempotently because the loader commits logical
   tables separately and may have been interrupted after the property commit.
4. It tracks confirmed GIS-only appraiser misses as one-way SHA-256 seed-key
   hashes in `ingest_control.broward_appraisal_terminal_items`.
5. It captures and runs the integrated process-warm query-data-only transform
   with at most four pipelines in flight.
6. For each bounded chunk, it validates the non-publication marker, gives the
   patched query-db loader canonical stable local URIs, and derives every
   expected logical `(table, source_record_key)` before loading.
7. After the loader returns, it verifies the exact property count, distinct
   folio count, and every prepared logical source key in Neon.
8. Only then does one transaction commit the loaded seed-key hashes and an
   aggregate row in `ingest_control.broward_appraisal_chunks`. Source,
   transform, and load attempts are aggregate-only events. Existing chunk
   signatures must match the rebuilt seed.
9. Verified captures, classified ZIPs, canonical loader copies, and stage files
   are deleted. No local capture is required for resume.

The logical tables have unique `(source_system, source_record_key)` indexes.
If a VM dies after a table merge but before the aggregate chunk checkpoint,
the next run reprocesses any property without a completed seed-key hash.
Source-key upserts make that replay idempotent rather than a duplicate insert.
Canonical `source_artifact_uri` paths are stable across reboots.

Result journals used by this recovery omit folios and free-text errors. They
contain only chunk row index, stage status, duration, usage aggregate, and a
fixed failure class. Dashboard and tmux output are aggregate-only.

## Pilot gate

The pilot uses the curated 25 folios first and deterministic official-seed
fallbacks to replace any source folio that has disappeared. It stops only when
the isolated branch contains exactly 50 `broward_appraiser` properties and 50
distinct folios:

```bash
npm run broward:recover -- \
  --pilot \
  --expected-branch-id '<verified-broward-ingest-branch-id>' \
  --expected-endpoint-id '<verified-broward-ingest-endpoint-id>' \
  --concurrency 4 \
  --chunk-size 50
```

After exact reconciliation, the recovery commits a `pilot-50` row in
`ingest_control.broward_appraisal_gates`. Full mode refuses to start unless
that gate matches the current seed, branch, and endpoint.

## Full run and dashboard

List existing sessions before creating either tmux session:

```bash
tmux -f /exec-daemon/tmux.portal.conf ls
```

Start or reuse the full recovery session:

```bash
SESSION_NAME=broward-neon-recovery
tmux -f /exec-daemon/tmux.portal.conf has-session -t "=$SESSION_NAME" 2>/dev/null ||
  tmux -f /exec-daemon/tmux.portal.conf new-session -d -s "$SESSION_NAME" -c /workspace
tmux -f /exec-daemon/tmux.portal.conf send-keys -t "$SESSION_NAME:0.0" \
  "npm run broward:recover -- --full --expected-branch-id '<verified-broward-ingest-branch-id>' --expected-endpoint-id '<verified-broward-ingest-endpoint-id>' --concurrency 4 --chunk-size 100" C-m
```

Start the aggregate-only dashboard on Cloud Agent preview port 47832:

```bash
SESSION_NAME=broward-neon-recovery-dashboard
tmux -f /exec-daemon/tmux.portal.conf has-session -t "=$SESSION_NAME" 2>/dev/null ||
  tmux -f /exec-daemon/tmux.portal.conf new-session -d -s "$SESSION_NAME" -c /workspace
tmux -f /exec-daemon/tmux.portal.conf send-keys -t "$SESSION_NAME:0.0" \
  "npm run broward:recovery-dashboard -- --host 0.0.0.0 --port 47832 --expected-branch-id '<verified-broward-ingest-branch-id>' --expected-endpoint-id '<verified-broward-ingest-endpoint-id>'" C-m
```

Open `http://127.0.0.1:47832/` inside the VM or preview port 47832. The API is
`GET /api/status`; liveness is `GET /healthz`. It reports only visible
properties, distinct folios, verified completion hashes, terminal source
misses, prepared/committed row counts, source/transform/load failure attempts,
recent verified throughput, and advisory-lock process state.

Resume uses the identical full command. Never reset or edit Neon source rows,
terminal hashes, or chunk records. A changed official seed signature fails
closed. Permit collection and all publishing remain outside this run.

The persistent Vercel dashboard is documented separately in
`apps/broward-ingest-dashboard/README.md`. Its migration must be applied to the
verified branch before recovery starts. Recovery then projects aggregate status
only after verified mode transitions and committed chunks. Durable completion
and terminal hashes, aggregate events, chunk rows, and gates remain the recovery
source of truth; dashboard status never authorizes skipping a seed.
