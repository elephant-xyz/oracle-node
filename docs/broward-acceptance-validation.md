# Broward county-ingestion acceptance validation

Date: 2026-08-28
County: Broward County, Florida (`12011`)
Appraisal category result: **PASS**
Shared multi-category result: **INCOMPLETE — PERMIT ACCEPTANCE NOT PASSED**

## Acceptance criteria

| Criterion | Evidence | Result |
| --- | --- | --- |
| Every data category has an official source or documented unavailability | [`broward-source-availability.md`](./broward-source-availability.md) covers appraisal, GIS, all 32 permit jurisdictions, Sunbiz, BBB, and deferred tax/recorder sources | SOURCE DISCOVERY PASS |
| Approximately 50 valid, deduplicated, diverse appraisal parcels | Exactly 50 unique folios; 20 usage types, 3 property types, and four polygon-complexity buckets | APPRAISAL PASS |
| Appraisal pilot prepared and ingested using Oracle | 50 live BCPA captures and 50 transformed Oracle artifacts | APPRAISAL PASS |
| Existing appraisal validations pass | Elephant CLI Lexicon validation: 50/50 | APPRAISAL PASS |
| Appraisal schema, completeness, geometry, failure behavior, and counts checked | Appraisal acceptance audit reports every check true | APPRAISAL PASS |
| Appraisal pilot queryable and accurate through Donphan | Donphan property tools ran against the local 50-row property Parquet | APPRAISAL PASS |
| Permit data ingested, validated, reconciled, and queryable | The bounded local permit pilot normalized and queried 73 historical BCS rows, but 30 current jurisdiction routes remain blocked and the ignored 50-row sample was not present in the isolated worktree | **NOT PASSED** |

The prior unqualified `PASS` described only the appraisal category. It did not
satisfy the shared acceptance criteria, which require permit ingestion,
validation, reconciliation, and queryability as well. Source discovery for all
32 permit jurisdictions is necessary evidence, but it is not permit ingestion.
The current permit pilot and exact blockers are documented in
[`broward-permit-acceptance-pilot.md`](./broward-permit-acceptance-pilot.md).

## Pilot diversity

- Seed rows / unique folios: **50 / 50**
- Transformed `property_usage_type` values: **20**
- Transformed `property_type` values: **3**
- Official geometry type: **Polygon** (the BCPA layer publishes only
  `esriGeometryPolygon`)
- Geometry complexity: small, medium, large, and very-large
- Geometry files / relationships: **50 / 50**
- Polygon vertices checked: **23,933**
- Valid Broward coordinates: **50 / 50**
- Alphanumeric condo folio retained: `504108BJ0140`

The selection manifest is
`downloads/broward/broward-validation-sample-50.json`. Geometry-type diversity
is bounded by the official source: there is no second geometry type to select.
The pilot therefore covers the full available type plus four complexity
buckets.

## Appraisal Oracle validation and reconciliation

`scripts/audit-broward-validation-sample.mjs` independently checked:

- seed count and deduplication;
- one non-empty BCPA capture per folio;
- one successful transform and Lexicon validation per folio;
- matching source/transformed parcel identifiers;
- `county_name: "Broward"` and non-empty situs address on all 50;
- transformed polygon coordinate ranges and parcel-to-geometry relationships;
- official geometry type and vertex-complexity coverage;
- property usage/type diversity;
- malformed-folio and empty-envelope rejection;
- query-table and Donphan counts.

Result:

```text
seed=50
unique_folios=50
captures=50
transforms=50
lexicon_validations=50
valid_geometry_artifacts=50
query_rows=50
donphan_rows=50
```

The full-run failure evidence at audit time contained seven expected GIS-only
source misses and zero transform errors. Empty BCPA parcel lists remain hard
failures, not successful skips.

## Appraisal Donphan verification

The local query table uses Donphan's stable 37-column property schema. It was
queried with the actual `elephant-mcp` property tools at commit
`0d61c83f9e166c9da5c5945641a2e322949dd8c1`:

- `getPropertyQuerySchema({ county: "Broward" })` returned 37 columns.
- `SELECT count(*), count(DISTINCT parcel_identifier)` returned `50 / 50`.
- Grouping by `property_usage_type` returned 20 categories totaling 50 rows.
- All 50 coordinates were inside the expected South Florida range.
- Exact lookup for `504108BJ0140` returned:
  - type `Unit`;
  - usage `Residential`;
  - city `PLANTATION`;
  - ZIP `33324`.

This verifies **appraisal** queryability and one known-source accuracy case through the same
DuckDB-backed `queryProperties` path used by the Donphan/uxie agent. The Parquet
and verification evidence remain private under
`downloads/broward/pilot-query/`; nothing was published.

## Permit acceptance status

The local property-first permit pilot resolved BCPA situs evidence for all 25
rows in the checked-in appraisal subset, reconciled 26 source outcomes, and
wrote 73 unique permit/application rows in query-db's 20-column permit-table
shape. Donphan's actual `getPermitQuerySchema`, `getPermitCoverage`, and
`queryPermits` handlers returned 20 columns and 73/73 distinct query rows.

That is a successful bounded orchestration pilot, not full permit acceptance:

- the positive records are historical BCS-held Lauderdale-by-the-Sea records,
  while Citizenserve/CAP Government is the current source;
- the current BMSD attempt was an explicit valid-parcel zero result;
- only 2 of 32 current jurisdiction routes are implemented in this branch;
- 24 source-unavailable outcomes were preserved for the 25-row run; and
- the actual ignored 50-row sample artifact was absent from this fresh
  isolated worktree, so its preserved 25-row subset was used for live
  execution.

Shared county acceptance remains incomplete until the exact blockers in the
permit pilot report are closed.

## Appraisal full-run gate

The first county run was paused at its atomic checkpoint while these criteria
were completed. After the machine audit passed, it resumed from
`nextRowIndex=2524` without reprocessing prior artifacts. The first post-resume
checkpoint reached row 2,600 with no new source failures and zero transform
errors.

The active appraisal ingestion remains separate and was not stopped or
modified. Sunbiz/BBB enrichment, database loading, and public publication also
remain downstream gates. Nothing in the appraisal result or source matrix
claims those datasets, or complete current permit coverage, have passed shared
county acceptance.
