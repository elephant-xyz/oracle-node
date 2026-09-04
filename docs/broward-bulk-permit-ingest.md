# Broward bulk-first permit ingestion

Date: 2026-08-31  
Publication: disabled  
Database target: isolated `broward-ingest` Neon branch

## Why this path exists

Broward County Property Appraiser parcel responses do not contain a
Hillsborough-style embedded permit array. That does not mean Broward lacks
bulk permit sources. Permit custody is municipal, and some custodians publish
official ArcGIS layers that can replace one-browser-search-per-property
discovery for their jurisdiction.

The first implemented source is the City of Fort Lauderdale
[Building Permits FeatureServer](https://gis.fortlauderdale.gov/server/rest/services/BuildingPermits/FeatureServer/0).
The anonymous layer reported **204,760 source rows** on 2026-08-31. It exposes
permit/case identity, type, status, description, submission and approval
dates, BCPA parcel ID, work location, applicant, contractor name/license,
estimated cost, certificate-of-occupancy fields, use class, source update
timestamps, and point geometry.

This layer is Fort Lauderdale coverage only. It is not a countywide permit
denominator.

## Source identity caveat

The FeatureServer's visible `PERMITID` is truncated and repeated. The first
100-row live pilot contained 12 repeated `PERMITID` labels. Distinct records
had different complete `CASEKEY` values.

The loader therefore:

- treats `CASEKEY` as the primary source identity;
- falls back to ArcGIS `GlobalID`, then `OBJECTID`, only when `CASEKEY` is
  absent;
- keeps the displayed `PERMITID` as a source field, not a uniqueness claim;
- converts `CASEKEY` to the Accela `capID1/capID2/capID3` tuple; and
- matches that tuple to prior LauderBuild portal rows so bulk data enriches
  existing records instead of duplicating them.

## Durable capture and load contract

`scripts/run-broward-bulk-permit-ingest.mjs`:

1. requests the complete uncapped ArcGIS object-ID list;
2. records the full count and SHA-256 of ordered IDs;
3. freezes an optional pilot subset;
4. fetches exact object-ID chunks with no offset pagination;
5. fails if the source omits, duplicates, or truncates requested IDs;
6. writes mode-0600 raw, normalized, and invalid private artifacts;
7. normalizes BCPA folios as 12-character strings with letters preserved;
8. loads exact-folio matches and retains unmatched permits with null property
   links;
9. commits each chunk and receipt in one Neon transaction; and
10. reconciles source, normalized, invalid, duplicate, roofing, matched, and
    unmatched counts.

The bulk loader shares the existing permit writer lock. Existing Accela
detail payloads take precedence over bulk source payloads, while list-level
status, contractor, cost, date, and provenance fields are merged into
`more_details`.

Private raw files contain public source fields that are not intended for the
published query table. Normalized database payloads omit owner mailing fields
and contractor phone numbers.

## Verified pilot

Capture-only pilot:

```bash
npm run broward:permits:run-bulk -- \
  --job-id broward-permits-ftl-casekey-capture-pilot-20260831 \
  --source fort-lauderdale \
  --limit 100 \
  --chunk-size 50 \
  --load false \
  --output-dir downloads/broward/permit-bulk/fort-lauderdale-casekey-capture-pilot
```

Result: 100 source rows, 100 normalized records, 100 unique `CASEKEY`
identities, zero invalid rows, six roofing classifications, and two exact raw
chunk receipts.

Neon pilot:

```bash
npm run broward:permits:run-bulk -- \
  --job-id broward-permits-ftl-casekey-neon-pilot-20260831 \
  --source fort-lauderdale \
  --limit 100 \
  --chunk-size 50 \
  --load true \
  --output-dir downloads/broward/permit-bulk/fort-lauderdale-casekey-neon-pilot
```

Result: 100/100 committed in two chunks; 76 exact BCPA property matches and 24
unmatched permits retained. The 1,603 pre-existing Fort Lauderdale portal
payloads remained intact.

## Full run — completed

```bash
npm run broward:permits:run-bulk -- \
  --job-id broward-permits-ftl-bulk-full-20260831 \
  --source fort-lauderdale \
  --chunk-size 1000 \
  --load true \
  --output-dir downloads/broward/permit-bulk/fort-lauderdale-full
```

The full job completed in the persistent `broward-ftl-bulk-permits` session.
Local truth is
`downloads/broward/permit-bulk/fort-lauderdale-full/manifest.private.json`;
durable truth is in
`ingest_control.broward_bulk_permit_runs` and
`ingest_control.broward_bulk_permit_chunks`.

Final reconciliation:

| Metric                                     |     Count |
| ------------------------------------------ | --------: |
| ArcGIS source rows                         |   204,760 |
| Normalized source rows                     |   204,760 |
| Unique `CASEKEY` logical permits           |   204,751 |
| Duplicate source rows                      |         9 |
| Invalid source rows                        |         0 |
| Conservative roofing classifications       |    16,613 |
| Source rows with exact BCPA property match |   159,584 |
| Source rows retained unmatched             |    45,176 |
| Durable chunks                             | 205 / 205 |

After logical deduplication, Neon contains 204,751 bulk-backed rows: 159,580
linked and 45,171 unlinked. Of 1,603 pre-existing rich LauderBuild portal
rows, 264 matched bulk `CASEKEY` values and were enriched in place; all 1,603
portal payloads were preserved. Fort Lauderdale now has 206,090 total logical
rows when the older portal-only records are included.

The FeatureServer publishes only 12,907 distinct displayed `PERMITID` values
across those logical rows because the field is truncated. `CASEKEY` is
therefore the durable identity. Full public permit-number recovery requires
optional Accela detail enrichment and is not inferred from the truncated
field.

## Remaining municipalities

Verified but not yet generalized into this runner:

- Broward HCED/POSSE ArcGIS: 7,359 daily-refreshed county permits/approvals;
- Miramar FY2019: 7,870 archived permit rows; and
- Miramar FY2020: 7,066 archived permit rows.

For municipalities without a complete official feed, use vendor-wide
date-window pagination where anonymous search supports it. Maintain a
per-jurisdiction rate limit and split dense windows until every reported
result is paginated. Reuse persistent portal sessions only as a fallback.
Login and CAPTCHA routes remain no-request outcomes.
