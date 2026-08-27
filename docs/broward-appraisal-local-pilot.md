# Broward appraisal local prepare → transform

Date: 2026-08-27
County key: `broward`
FIPS: `12011`

Local-only run. No AWS enqueue, no Restate, no publish.

## Result

`elephant-cli prepare` against `multi-request-flows/Broward.json` fetched all
**25/25** pilot folios from the live BCPA JSON API. The published
`Counties-trasform-scripts/broward` extractor then produced
`data/property.json` for **16/25** parcels. The other nine crashed on
family-level `useCode` labels.

A local matcher patch (`--apply-use-code-fix`) recovered **25/25**
transforms. That patch is not in `Counties-trasform-scripts` yet; apply it
there before any AWS transform worker run.

| Gate | Published scripts | Local use-code fix |
| --- | ---: | ---: |
| Prepare captures | 25/25 | 25/25 |
| County transform | 16/25 | 25/25 |
| `address.county_name` | Broward | Broward |
| Condo folio letters | kept (`504108BJ0140`) | kept |
| CLI Lexicon validate | skipped (no schema cache) | skipped |

Command:

```bash
git clone --depth 1 https://github.com/elephant-xyz/Counties-trasform-scripts.git \
  ../Counties-trasform-scripts

node scripts/build-broward-seed.mjs --pilot

node scripts/validate-broward-appraisal.mjs \
  --seed downloads/broward/broward-pilot.csv \
  --scripts ../Counties-trasform-scripts/broward/scripts \
  --skip-validate
```

Re-run transforms only, with the family-label matcher:

```bash
node scripts/validate-broward-appraisal.mjs \
  --seed downloads/broward/broward-pilot.csv \
  --scripts ../Counties-trasform-scripts/broward/scripts \
  --skip-prepare --skip-validate --apply-use-code-fix
```

## Prepare

The first local prepare failed CLI validation: `content-type` must be exactly
`application/json` (no `charset`), and JSON bodies must use the `json` field
rather than `body`. `multi-request-flows/Broward.json` now matches that
contract. Posted payload for folio `474135010090`:

```json
{"folioNumber":"474135010090","taxyear":"","action":"CURRENT","use":""}
```

Prepare writes `{folio}.json` as a multi-request wrapper:

```json
{ "input": { "source_http_request": {}, "response": { "d": { "parcelInfok__BackingField": [] } } } }
```

The published extractor reads `input.json` → `d.parcelInfok__BackingField` at
the document root. The local harness unwraps `input.response` before transform
so the existing scripts can run. **AWS prepare will feed the wrapper
unchanged.** `Counties-trasform-scripts/broward` must unwrap
`input.response` (Rock Island already does this for `ParcelFeature`) or every
parcel will hit `No parcel info found in input.json` and `process.exit(0)`.

Empty envelopes still fail closed in the harness via
`requireParcelRecords`.

## Published-script failures (9)

All 25 captures had a non-empty parcel list. Transform crashed in
`data_extractor.js` at `propertyMapping.property_type` when the live label was
a **family** (`04 - Condominium`) instead of a subtype (`04-01 ...`).

| Folio | Live `useCode` | City |
| --- | --- | --- |
| `504108BJ0140` | `04 - Condominium` | PLANTATION |
| `494108AK1220` | `04 - Condominium` | TAMARAC |
| `484201BA0050` | `04 - Condominium` | DEERFIELD BEACH |
| `494123BJ0010` | `04 - Condominium` | LAUDERHILL |
| `504209091840` | `08 - Multi-family - less than 10 units` | FORT LAUDERDALE |
| `474134000012` | `52 - Cropland soil capability class II` | UNINCORPORATED |
| `474135010091` | `63 - Grazing land soil capability class IV` | PARKLAND |
| `504026140250` | `63 - Grazing land soil capability class IV` | SOUTHWEST RANCHES |
| `514106100100` | `94 - Right-of-way, streets, roads, irrigation channel, ditch, etc.` | COOPER CITY |

The local fix prefers an exact `NN-NN` token, then includes-match, then the
family's `-01` row. Example: `04 - Condominium` → `04-01 CONDOMINIUM -
RESIDENTIAL` (`Residential` / `Unit` / `Condominium`). PR that matcher into
`Counties-trasform-scripts/broward/scripts/data_extractor.js` and fail with
`errorOut` when nothing matches.

## Transformed usage types (patched run)

| Live use code | Count | `property_usage_type` | Permit-first? |
| --- | ---: | --- | --- |
| `01-01 Single Family` | 8 | `Residential` | no |
| `01-04 Townhome` | 2 | `Residential` | no |
| `01-05 Single Family Zero Lot Line` | 1 | `Residential` | no |
| `03-01 Multi-family 10 to 49 units` | 1 | `Residential` | no |
| `04 - Condominium` | 4 | `Residential` | no |
| `08 - Multi-family - less than 10 units` | 1 | `Residential` | no |
| `10-01 Vacant Commercial` | 1 | `Commercial` | yes |
| `12-02 Mixed store and office` | 1 | `Commercial` | yes |
| `48-04 Warehouse - Metal` | 1 | `Warehouse` | yes |
| `52 - Cropland soil capability class II` | 1 | `CroplandClass2` | no |
| `63 - Grazing land soil capability class IV` | 2 | `GrazingLand` | no |
| `94` / `94-01` right-of-way | 2 | `TransportationTerminal` | no |

Do **not** keep Lee's default `PROPERTY_FIRST_PERMIT_ELIGIBLE_USAGE_TYPES`
list blindly. For Broward, the pilot's permit-priority labels are
`Commercial` and `Warehouse`. Expand from a larger sample before a
full-county run; office/industrial/retail subtypes exist in the mapping
table but were not in this 25.

Every patched `data/address.json` has `county_name: "Broward"`. The extractor
does not write `county_jurisdiction` on the transformed address; that field
stays on the seed `unnormalized_address.json`.

Condo `504108BJ0140` kept its letters in `property.parcel_identifier` and
resolved situs `958 MOCKINGBIRD LANE # 513 PLANTATION, FL 33324`.

## Not done

- Lexicon `validate` (schema cache / IPFS gateways not populated here).
- PR the use-code matcher and multi-request unwrap into
  `Counties-trasform-scripts`.
- Broward BCS POSSE permit adapter and city-vendor routing.
- AWS prepare queue, seed staging, enqueue, or publish.
