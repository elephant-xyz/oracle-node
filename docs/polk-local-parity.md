# Polk local parity runbook

Polk parity uses the official property-appraiser bulk files as the property and
permit denominator, then adds source-evidenced permit details, Sunbiz corporate
records, BBB contractor profiles, and boundary-clipped Overture places.

## Verified 2026-08-31 snapshot

- Appraisal: 438,612 distinct Polk parcels/properties.
- Permits: 531,344 official bulk permit rows.
- Sunbiz: 346,604 exact-Polk-ZIP registrations; 92,813 properties have an exact
  normalized street, city, and ZIP match.
- BBB: 723 complete roofing/HVAC/solar observations resolve to 714 loader
  identities; 403 profiles contain licence evidence, and one exact licence links
  a BBB contractor to two certified Polk permit details.
- Overture Places release `2026-08-19.0`: 30,079 boundary-clipped places and
  30,079 distinct GERS IDs.
- The isolated `pr-201-polk-parity` query-database branch reconciled appraisal,
  permits, Sunbiz, BBB, and Overture to timestamped
  `oracle_dataset_coverage` rows.
- The published Overture places parquet contains 30,079 rows and is registered
  in `catalog/published-counties.json`.

The authoritative local receipts are under `tmp/polk/`. Generated data is
ignored by Git; the scripts and tests in this repository are the reproducible
contract.

## Reproduce the enrichment stages

Prepare and run a network-backed permit pilot:

```bash
npm run polk:permits:enrich -- --stage candidates --network
npm run polk:permits:enrich -- --stage enrich --network
```

The source registry certifies adapters for Polk County Accela, Lakeland iMS,
Lake Wales CitizenLink, and partial Winter Haven eSuite history. Haines City
iWorQ remains adapter-pending. Other municipal rows remain fail-closed where no
anonymous detail request contract has been verified. Do not infer contractors
or licences from bulk descriptions.

County-scale adapter runs are resumable and write one atomic JSONL part per
batch. Generate the complete requestable candidate set, then run with a
conservative per-source request interval:

```bash
npm run polk:permits:enrich -- --stage candidates
npm run polk:permits:enrich -- \
  --stage enrich \
  --network \
  --concurrency 12 \
  --batch-size 100 \
  --delay-ms 250 \
  --attempts 3 \
  --retry-delay-ms 2000 \
  --output tmp/polk/permits/enriched-permits-full.jsonl \
  --receipt tmp/polk/permits/enrichment-receipt-full.json
```

The checkpoint and deterministic part directory default beside the output. A
rerun validates and reuses complete parts; `--reset-checkpoint` is required to
discard them. If a portal throttles or has a transient outage, lower
`--concurrency`, increase `--delay-ms`, and pass `--redrive-errors`; successful
rows in each part remain untouched while only exhausted fetch failures retry.
Use `--include-partial` only for an explicitly scoped Winter Haven historical
run. The official adapter-ready denominator is 230,221 rows, of which 230,114
currently have requestable permit numbers. The remaining 107 rows and the
301,123 rows without certified anonymous adapters remain explicit source
limitations and do not justify fabricated detail evidence.

Transform and match the exact-ZIP Sunbiz slice:

```bash
npm run polk:sunbiz -- --stage filter --source <sunbiz-corporate-file>
npm run polk:sunbiz -- --stage transform-match
```

Run the complete BBB trade harvest and permit-licence CRM match:

```bash
npm run polk:bbb -- --mode harvest --headless true
npm run polk:bbb -- --mode match
```

For a faster operator-controlled run, `--trade-concurrency` accepts `1` through
`3`; the delay, retry, challenge-check, navigation-timeout, and profile-subpage
flags are also forwarded to the shared BBB harvester. To redrive one failed
trade without repeating complete uncapped work, pass `--trades solar` (or another
comma-separated subset); omitted trades must already have complete receipts. A
capped harvest is never marked complete. CRM matches require an exact normalized
licence found in both a certified permit detail and a harvested BBB profile.

Extract and prepare Overture Places:

```bash
npm run polk:overture:probe
npm run polk:overture:extract
npm run polk:overture:publication -- --execute-export
```

Publication must retain the source/licence gate, the 30,079-row count, and the
public-business-contact approval recorded by the receipt. Never use the
bbox-only count as the county denominator.

## Query-database handoff

Create query-database bulk-stage CSVs:

```bash
npm run polk:neon:stage -- --track appraisal
npm run polk:neon:stage -- --track permits
```

Use a direct, isolated Lakebase Postgres branch connection for bulk COPY and
long merges. Reuse the emitted permanent stage-table name after interruption;
the query-database checkpoint skips tables that already committed. Do not run
two loaders against the same stage table.

After all five tracks are loaded, create a read-only reconciliation receipt:

```bash
npm run polk:neon:reconcile -- --mode reconcile --from-neon --release 2026-08-19.0
```

The receipt passes only when every local count matches a timestamped coverage
row, and Overture also matches direct current-row, distinct-GERS, extraction,
and licence observations.

## Final verification

```bash
npm run typecheck
npm run test
npm run format:check
npm run polk:status
```

`npm run polk:status` intentionally keeps permit enrichment blocked while
official rows belong to an unknown agency or an agency without a certified
anonymous adapter. This is an evidence gap, not a reason to manufacture
contractor data.
