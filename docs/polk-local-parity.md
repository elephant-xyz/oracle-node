# Polk local parity runbook

Polk parity uses the official property-appraiser bulk files as the property and
permit denominator, then adds source-evidenced permit details, Sunbiz corporate
records, BBB contractor profiles, and boundary-clipped Overture places.

## Verified 2026-09-04 snapshot

- Appraisal: 438,612 distinct Polk parcels/properties.
- Permits: 1,128,608 unique rows in Neon and the public permit table:
  531,344 property-appraiser bulk rows, 489,363 Polk County Accela
  permit/application rows, and 107,901 Lakeland ArcGIS rows. The 597,264 portal
  rows remain property-unmatched rather than using inferred parcel/address
  links. Another 1,928 Accela contractor-license rows are explicitly excluded
  from property improvements.
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
- The published permit Parquet contains 1,128,608 unique permit IDs at
  `oracle-permit-table-polk`; its gateway range response reconciles to
  233,645,424 bytes and begins with `PAR1`.

The authoritative local receipts are under `tmp/polk/`. Generated data is
ignored by Git; the scripts and tests in this repository are the reproducible
contract.

## Reproduce the enrichment stages

The current decision is **GO WITH DOCUMENTED GAPS** for verified bulk/list
ingestion, publication, and selective detail enrichment. It remains **STOP**
for a countywide detail-page run or a claim of complete jurisdiction history. Read
`docs/polk-permit-source-audit.md` before making any permit network request.
That audit enumerates every county/municipal/delegated jurisdiction, official
bulk and list alternatives, predecessor boundaries, CAPTCHA/manual blockers,
bounded pilot results, safe concurrency, and the unresolved historical gaps.
Do not treat the commands below as authorization for an unbounded detail run.

Harvest and strictly verify the preferred Lakeland ArcGIS list. The harvester
pins the maximum source `OBJECTID`, uses keyset pagination, commits
content-addressed parts, and preserves every unmatched permit with a null
property relation:

```bash
npm run polk:permits:lakeland -- --stage probe
npm run polk:permits:lakeland -- \
  --stage harvest \
  --approve-scale \
  --page-size 2000
npm run polk:permits:lakeland -- \
  --stage verify \
  --page-size 2000
```

Harvest the anonymous Polk County Accela list by non-overlapping monthly CSV
exports. Annual current-year searches can stall, while monthly windows remain
bounded and resumable. The end date is explicit so a resumed checkpoint cannot
silently change scope:

```bash
npm run polk:permits:county-list -- \
  --stage probe \
  --start-date 2003-01-01 \
  --end-date 2026-09-03
npm run polk:permits:county-list -- \
  --stage harvest \
  --approve-scale \
  --start-date 2003-01-01 \
  --end-date 2026-09-03 \
  --window-months 1 \
  --timeout-ms 30000 \
  --window-timeout-ms 90000 \
  --attempts 2 \
  --delay-ms 5000
npm run polk:permits:county-list -- \
  --stage verify \
  --start-date 2003-01-01 \
  --end-date 2026-09-03 \
  --window-months 1
```

The Accela list CSV contains complete search results beyond the `100+` first
page and retains temporary application rows that do not yet have detail links.
Use one session at a time with five seconds between monthly windows. The
whole-window deadline bounds all four session requests together; the
per-request timeout alone is not a complete runtime bound. The list does not
prove predecessor/Hansen completeness. The verified 2003-01-01 through
2026-09-03 run contains 491,695 source rows, 404 exact duplicates, and 491,291
unique records; permit/application dates begin 2005-12-20. Use it to replace
the countywide 69-hour detail pass; enrich only selected records needing
contractor, inspection, or document fields.

Prepare and run a network-backed permit pilot:

```bash
npm run polk:permits:enrich -- \
  --stage candidates \
  --agency "POLK COUNTY" \
  --limit 25 \
  --input tmp/polk/permit-audit/pilot-candidates.jsonl
npm run polk:permits:enrich -- \
  --stage enrich \
  --network \
  --input tmp/polk/permit-audit/pilot-candidates.jsonl \
  --output tmp/polk/permit-audit/pilot-results.jsonl \
  --receipt tmp/polk/permit-audit/pilot-receipt.json \
  --state-dir tmp/polk/permit-audit/pilot.parts \
  --checkpoint tmp/polk/permit-audit/pilot.checkpoint.json \
  --concurrency 1 \
  --batch-size 25 \
  --delay-ms 1000 \
  --timeout-ms 30000
```

The source registry records all named bulk agencies plus delegated Highland
Park and Hillcrest Heights. It certifies request contracts for Polk County
Accela, Lakeland iMS, Lake Wales CitizenLink, and partial Winter Haven eSuite
history; it does not certify countywide historical completeness. Haines City,
Davenport, and Lake Hamilton iWorQ remain CAPTCHA- or credential-blocked.
Bartow and Frostproof require official records exports. Other portals remain
fail-closed until bounded historical pilots pass. Do not infer contractors,
licences, parcels, addresses, or issuing agencies.

County-scale adapter runs are resumable and write one atomic JSONL part per
batch. Before any resume, verify every committed part without network access:

```bash
npm run polk:permits:enrich -- \
  --stage verify \
  --input tmp/polk/permits/adapter-candidates.jsonl \
  --output tmp/polk/permits/enriched-permits-full.jsonl \
  --state-dir tmp/polk/permits/enriched-permits-full.parts \
  --checkpoint tmp/polk/permits/enriched-permits-full.checkpoint.json \
  --batch-size 100
```

The verification rejects incompatible input fingerprints, changed batch
contracts, corrupt/gapped chunks, and checkpoints ahead of committed state.
The source audit now records GO WITH DOCUMENTED GAPS, so an operator may
generate the complete requestable candidate set and run:

```bash
npm run polk:permits:enrich -- --stage candidates
npm run polk:permits:enrich -- \
  --stage enrich \
  --network \
  --approve-scale \
  --concurrency 3 \
  --batch-size 100 \
  --delay-ms 1000 \
  --attempts 3 \
  --retry-delay-ms 2000 \
  --timeout-ms 30000 \
  --output tmp/polk/permits/enriched-permits-full.jsonl \
  --receipt tmp/polk/permits/enrichment-receipt-full.json
```

The checkpoint and deterministic part directory default beside the output. A
rerun validates and reuses complete parts. `--reset-checkpoint` refuses to
delete committed parts; use new output/state paths for a genuinely new run.
Never reset the existing Polk run. More than 100 candidates also require the
explicit `--approve-scale` flag under the documented GO WITH DOCUMENTED GAPS
decision. One portal has at most one request in flight; `--concurrency` allows
different sources to proceed independently. Duplicate agency/permit pairs
share a request while preserving all source rows.

Redrive is a terminating stage: it can repair existing `fetch_error` records
but cannot admit untouched parts. Successful rows remain unchanged.

```bash
npm run polk:permits:enrich -- \
  --stage redrive \
  --network \
  --input tmp/polk/permits/adapter-candidates.jsonl \
  --output tmp/polk/permits/enriched-permits-full.jsonl \
  --state-dir tmp/polk/permits/enriched-permits-full.parts \
  --checkpoint tmp/polk/permits/enriched-permits-full.checkpoint.json \
  --batch-size 100 \
  --concurrency 1 \
  --delay-ms 1000
```

Use `--include-partial` only for an explicitly scoped Winter Haven historical
run. The official adapter-ready denominator is 230,221 rows, of which 230,114
currently have requestable permit numbers. The remaining 107 rows and the
301,123 rows without certified anonymous adapters remain explicit source
limitations and do not justify fabricated detail evidence.

```bash
npm run polk:permits:enrich -- \
  --stage candidates \
  --winter-haven-historical \
  --limit 25 \
  --input tmp/polk/permit-audit/winter-haven-candidates.jsonl
npm run polk:permits:enrich -- \
  --stage enrich \
  --network \
  --include-partial \
  --input tmp/polk/permit-audit/winter-haven-candidates.jsonl \
  --output tmp/polk/permit-audit/winter-haven-results.jsonl \
  --state-dir tmp/polk/permit-audit/winter-haven.parts \
  --checkpoint tmp/polk/permit-audit/winter-haven.checkpoint.json \
  --batch-size 25 \
  --concurrency 1
```

The historical filter accepts only the legacy `YYYY-NNNNNNNN` identifiers
served by eSuite. The official city boundary is eSuite/ProjectDox for
2025-and-older history and COWH Accela for 2026+ applications and records.
Current `WH26-*` search and detail routes redirect to COWH login. They remain
unsupported until an account is explicitly authorized or the City supplies a
native export; an identifier echoed in a login return URL is not permit
evidence.

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

Normalize verified portal lists into bounded, content-addressed
`normalized-jsonl` parts before loading them separately from the CAMA stage:

```bash
npm run polk:permits:normalize
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

## Permit-table publication

Export and validate the combined permit table from the same reconciled Neon
branch, then publish the single Parquet under its dedicated bucket and IPNS
label. The publication receipt at
`tmp/polk/permits/permit-table-publication-receipt.json` must record the
reconciled row count, immutable CID, gateway range response, and `PAR1` magic
bytes before coverage is regenerated.

The verified publication is:

- rows/distinct permit IDs: 1,128,608 / 1,128,608;
- immutable CID: `QmNUR5qhE7Mv7j5Ugn1qsLU5deoq2js8cHD4noRdgwYHnE`;
- IPNS label: `oracle-permit-table-polk`;
- stable URL:
  `https://ipfs.filebase.io/ipns/k51qzi5uqu5dly93m4h8vv3rfhaolyo0i2njowirgv1kvanplaseuf6af7e4my`.

After the receipt exists, rerun `npm run polk:publication:prepare --
--materialize`. The generated dataset coverage links the permit track to the
permit-table CID instead of incorrectly linking unmatched portal rows to the
property-family CID.

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
