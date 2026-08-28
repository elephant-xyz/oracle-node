# Broward county-ingestion acceptance validation

Date: 2026-08-28  
County: Broward County, Florida (`12011`)  
Result: **PASS**

## Acceptance criteria

| Criterion                                                               | Evidence                                                                                                                                                                | Result |
| ----------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------ |
| Every data category has an official source or documented unavailability | [`broward-source-availability.md`](./broward-source-availability.md) covers appraisal, GIS, all 33 permit jurisdictions, Sunbiz, BBB, and deferred tax/recorder sources | PASS   |
| Approximately 50 valid, deduplicated, diverse parcels                   | Exactly 50 unique folios; 20 usage types, 3 property types, and four polygon-complexity buckets                                                                         | PASS   |
| Pilot prepared and ingested using Oracle                                | 50 live BCPA captures and 50 transformed Oracle artifacts                                                                                                               | PASS   |
| Existing automated validations pass                                     | Elephant CLI Lexicon validation: 50/50                                                                                                                                  | PASS   |
| Schema, completeness, geometry, failure behavior, and counts checked    | Acceptance audit reports every check true                                                                                                                               | PASS   |
| Pilot is queryable and accurate through Donphan                         | Donphan `getPropertyQuerySchema` and `queryProperties` ran against the local 50-row Parquet                                                                             | PASS   |

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

## Oracle validation and reconciliation

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

## Donphan verification

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

This verifies queryability and one known-source accuracy case through the same
DuckDB-backed `queryProperties` path used by the Donphan/uxie agent. The Parquet
and verification evidence remain private under
`downloads/broward/pilot-query/`; nothing was published.

## Full-run gate

The first county run was paused at its atomic checkpoint while these criteria
were completed. With this document and the machine audit both passing, the
appraisal run may resume from `nextRowIndex` without reprocessing prior
artifacts.

Permit ingestion, Sunbiz/BBB enrichment, database loading, and public
publication remain separate downstream gates. The source matrix records their
official sources, but this appraisal acceptance result does not claim those
datasets have already been ingested.
