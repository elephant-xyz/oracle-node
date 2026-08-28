# Broward appraisal local prepare → transform → ingest

Date: 2026-08-28
County key: `broward`
FIPS: `12011`

Local-only run. No AWS enqueue, no Restate, no publish.

## Pilot result

`elephant-cli prepare` against `multi-request-flows/Broward.json` fetched all
**25/25** pilot folios from the live BCPA JSON API. The corrected Broward
extractor passed all gates:

| Gate                   |                Result |
| ---------------------- | --------------------: |
| Prepare captures       |                 25/25 |
| County transforms      |                 25/25 |
| CLI Lexicon validation |                 25/25 |
| `address.county_name`  |       Broward (25/25) |
| Condo folio letters    | kept (`504108BJ0140`) |

The verified schema cache contains 65 CID-addressed schemas (156,575 bytes).
The full repository test suite passed 698/698 and `npm run typecheck` passed.

Command:

```bash
git clone --depth 1 https://github.com/elephant-xyz/Counties-trasform-scripts.git \
  ../Counties-trasform-scripts

node scripts/build-broward-seed.mjs --pilot

node scripts/prefetch-elephant-schema-cache.mjs

node scripts/validate-broward-appraisal.mjs \
  --seed downloads/broward/broward-pilot.csv \
  --scripts ../Counties-trasform-scripts/broward/scripts \
  --output downloads/broward/appraisal-validation-v2
```

## Corrections required by live data

The first local prepare failed CLI validation: `content-type` must be exactly
`application/json` (no `charset`), and JSON bodies must use the `json` field
rather than `body`. `multi-request-flows/Broward.json` now matches that
contract. Posted payload for folio `474135010090`:

```json
{ "folioNumber": "474135010090", "taxyear": "", "action": "CURRENT", "use": "" }
```

Prepare writes `{folio}.json` as a multi-request wrapper:

```json
{
  "input": {
    "source_http_request": {},
    "response": { "d": { "parcelInfok__BackingField": [] } }
  }
}
```

The corrected transform now:

- unwraps `input.response` in every Broward mapping script;
- fails loudly on an empty parcel list or unmapped use code;
- maps family labels such as `04 - Condominium` to the family's `04-01` row;
- retains the POST JSON body and content-type in source provenance;
- emits property-to-structure and property-to-utility relationships;
- rounds acreage-derived `lot_area_sqft` to the integer required by Lexicon.

The transform fix is committed locally as `5130a7f` on
`cursor/broward-live-capture-fix-2cd0`. GitHub rejected the bot's push to
`Counties-trasform-scripts` with HTTP 403, so a maintainer still needs to push
or cherry-pick that commit. The full local run uses this exact local commit.

## Pilot usage types

| Live use code                                | Count | `property_usage_type`    | Permit-first? |
| -------------------------------------------- | ----: | ------------------------ | ------------- |
| `01-01 Single Family`                        |     8 | `Residential`            | no            |
| `01-04 Townhome`                             |     2 | `Residential`            | no            |
| `01-05 Single Family Zero Lot Line`          |     1 | `Residential`            | no            |
| `03-01 Multi-family 10 to 49 units`          |     1 | `Residential`            | no            |
| `04 - Condominium`                           |     4 | `Residential`            | no            |
| `08 - Multi-family - less than 10 units`     |     1 | `Residential`            | no            |
| `10-01 Vacant Commercial`                    |     1 | `Commercial`             | yes           |
| `12-02 Mixed store and office`               |     1 | `Commercial`             | yes           |
| `48-04 Warehouse - Metal`                    |     1 | `Warehouse`              | yes           |
| `52 - Cropland soil capability class II`     |     1 | `CroplandClass2`         | no            |
| `63 - Grazing land soil capability class IV` |     2 | `GrazingLand`            | no            |
| `94` / `94-01` right-of-way                  |     2 | `TransportationTerminal` | no            |

Do **not** keep Lee's default `PROPERTY_FIRST_PERMIT_ELIGIBLE_USAGE_TYPES`
list blindly. For Broward, the pilot's permit-priority labels are
`Commercial` and `Warehouse`. Revisit the permit configuration after the full
appraisal run inventories every emitted usage type; office/industrial/retail
subtypes exist in the mapping table but were not in this 25.

Every patched `data/address.json` has `county_name: "Broward"`. The extractor
does not write `county_jurisdiction` on the transformed address; that field
stays on the seed `unnormalized_address.json`.

Condo `504108BJ0140` kept its letters in `property.parcel_identifier` and
resolved situs `958 MOCKINGBIRD LANE # 513 PLANTATION, FL 33324`.

## Full local ingestion

The native ArcGIS JSON export returned 556,178 parcel feature IDs and produced
**534,309 unique canonical folios** after deduplication. Native ArcGIS JSON is
converted to GeoJSON because the service rejects valid high-vertex polygons
when asked to serialize them directly as GeoJSON.

The full appraisal run started at `2026-08-28T06:06:48Z`, with four isolated
transform workers. It is resumable from
`downloads/broward/full-ingestion/state.json`. Captures are gzip-compressed and
artifacts are sharded by folio prefix under a private mode-0700 directory.

```bash
node scripts/build-broward-seed.mjs --page-size 50 --concurrency 4

node scripts/ingest-broward-appraisal-local.mjs \
  --seed downloads/broward/broward.csv \
  --scripts ../Counties-trasform-scripts/broward/scripts \
  --output downloads/broward/full-ingestion \
  --concurrency 4
```

The first 364 rows produced 361 valid transformed artifacts, zero transform
errors, and three expected fail-closed appraiser misses. Verified misses
include GIS-only folios `474131010000` and `474131AC0000`. A full-run artifact
also passed CLI Lexicon validation independently.

## Remaining after appraisal drain

- Reconcile 534,309 seed folios against successful artifacts and source misses.
- Load to Postgres/Neon after a `DATABASE_URL` is available; this environment
  currently has no local Postgres binary or configured database URL.
- Broward BCS POSSE permit adapter and city-vendor routing.
- Privacy review before any public publication; transformed appraisal records
  contain public owner data.
