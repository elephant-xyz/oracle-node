# Rock Island appraisal source gaps

Date: 2026-08-14
Scope: 25 source-null site addresses and the assessment-class dictionary
Production database changes: 19 E911 addresses and the class mapping applied

## Result

- Exact null-address scope: **25 folios**
- Authoritative E911 site addresses found by exact county key: **19**
- Not found after exact-key official-source checks: **6**
- Conflicting authoritative addresses: **0**
- Official county class definitions found: **22 codes**
- Canonical properties receiving an authoritative class label: **65,653**
- Canonical properties changing from `Unknown` to a non-unknown usage: **63,123**
- Canonical properties deliberately remaining `Unknown`: **2,683**

The private evidence package remains immutable with `apply=false`. A separate
explicit apply command consumed it after review; the package itself was not
rewritten.

## Applied continuation

The approved EC2 PostgreSQL host was used only while the separate
geometry/Illinois worker was idle. The source-scoped backfill:

- inserted and linked **19** exact-folio E911 site-address rows;
- left the **6** not-found folios unaddressed;
- attached official class labels to **65,653** properties;
- changed **63,123** usage values from `Unknown`;
- produced **4,602** commercial/industrial usages;
- deliberately retained **2,683** `Unknown` usages;
- retained raw class codes and complete mapping provenance in
  `source_payload.classification`;
- excluded owner, mailing, and tax-bill addresses.

Final database proof:

- properties: `65,806`
- supported site addresses: `65,800`
- explicit source-null site addresses: `6`
- exact found/not-found folios: `19 / 6`
- address conflicts, parcel/property/address orphans, and duplicate
  parcel/property/address folios: all `0`
- idempotent rerun changes: `0` addresses and `0` properties
- geometry components/rings: `66,516 / 66,560`, unchanged across the final
  rerun
- permits / linked permits: `24,786 / 118`, unchanged
- Illinois corporate registrations/addresses:
  `1,981,254 / 1,969,007`, unchanged

Reversible checkpoint:

- schema: `ri_address_class_address_class_20260814`
- original address rows: `0`
- original property rows: `65,806`
- target folios: `25`
- final aggregate manifest:
  `/srv/ingest/private/rock-island/address-class-20260814/final-idempotent-rerun.json`
- rollback SQL:
  `/srv/ingest/private/rock-island/address-class-20260814/final-idempotent-rerun.json.rollback.sql`
- both files mode: `0600`

## Exact 25-folio scope

The immutable corrected public query table is:

- CID:
  [`QmQnm6W2Ye9GH3oD6SUswHrQCMegnpGbhRFgipitYW6zCc`](https://ipfs.filebase.io/ipfs/QmQnm6W2Ye9GH3oD6SUswHrQCMegnpGbhRFgipitYW6zCc)
- bytes: `20,344,635`
- SHA-256: `730bf5b251489f646bc7516d4794b91419364b51ffaa677470c342e772347dfa`
- rows/distinct folios: `65,806 / 65,806`
- non-null `address_street`: `65,781`
- null `address_street`: `25`

Those exact 25 rows are:

| Folio        | RICO_PARCE  | Result    | Official site address                       | Exact evidence                                                              |
| ------------ | ----------- | --------- | ------------------------------------------- | --------------------------------------------------------------------------- |
| `0436100005` | `05159-1`   | found     | `1107 S HIGH ST, PORT BYRON IL 61275`       | Parcel `3805` → E911 point `6610`                                           |
| `0831449003` | `089419`    | not found | —                                           | Exact E911 key returned 0 rows                                              |
| `0831449018` | `089424`    | not found | —                                           | Exact E911 key returned 0 rows                                              |
| `0834120022` | `081645`    | found     | `530 37TH ST, MOLINE IL 61265`              | Parcel `25853` → E911 point `28492`                                         |
| `0919106035` | `065553`    | found     | `298 ISLAND AV, EAST MOLINE IL 61244`       | Parcel `65932` → E911 point `8230`                                          |
| `1532102015` | `161034`    | found     | `9101 141ST ST W, TAYLOR RIDGE IL 61284`    | Parcel `44890` → E911 point `845`                                           |
| `1601301027` | `1052-A`    | found     | `1800 9TH 1/2 ST, ROCK ISLAND IL 61201`     | Parcel `65933` → E911 point `50055`                                         |
| `1602412002` | `102183`    | found     | `1824 22ND ST, ROCK ISLAND IL 61201`        | Parcel `47897` → E911 point `43192`                                         |
| `1602429005` | `102168`    | found     | `2019 17TH ST, ROCK ISLAND IL 61201`        | Validated 2026-08-03 parcel `53407` → E911 point `49775`                    |
| `1602429006` | `102169-1`  | not found | —                                           | Exact E911 key returned 0 rows; PIN is absent from the current parcel layer |
| `1612122002` | `104742`    | found     | `2618 29TH 1/2 ST CT, ROCK ISLAND IL 61201` | Parcel `55913` → E911 point `47183`                                         |
| `1614114001` | `102595-B`  | found     | `3902 14TH ST, ROCK ISLAND IL 61201`        | Parcel `54910` → E911 point `48051`                                         |
| `1614201026` | `103891-84` | found     | `10 HAWTHORNE RD, ROCK ISLAND IL 61201`     | Parcel `51905` → E911 point `48055`                                         |
| `1701111005` | `0714381`   | found     | `443 35TH AV, EAST MOLINE IL 61244`         | Parcel `57416` → E911 point `18322`                                         |
| `1702125007` | `0714206`   | found     | `5409 19TH AV, MOLINE IL 61265`             | Parcel `12324` → E911 point `18804`                                         |
| `1703114018` | `0712961`   | found     | `1511 37TH ST, MOLINE IL 61265`             | Parcel `10322` → E911 point `18129`                                         |
| `1707301004` | `104280`    | found     | `4417 37TH AV, ROCK ISLAND IL 61201`        | Parcel `54409` → E911 point `50872`                                         |
| `1707301009` | `104274`    | found     | `3714 44TH ST, ROCK ISLAND IL 61201`        | Parcel `54408` → E911 point `50805`                                         |
| `1707301014` | `104284`    | found     | `4420 37TH AV, ROCK ISLAND IL 61201`        | Parcel `54410` → E911 point `50880`                                         |
| `1708107010` | `089236`    | found     | `2704 11TH ST CT, MOLINE IL 61265`          | Parcel `20843` → E911 point `35683`                                         |
| `1709203010` | `0712994`   | found     | `2913 28TH AV A, MOLINE IL 61265`           | Parcel `17336` → E911 point `23402`                                         |
| `1712409027` | `0714517`   | found     | `3410 78TH ST CT, MOLINE IL 61265`          | Parcel `14831` → E911 point `23930`                                         |
| `1723424030` | `121988`    | not found | —                                           | Exact E911 key returned 0 rows                                              |
| `1726300042` | `1274-7`    | not found | —                                           | Exact E911 key returned 0 rows                                              |
| `2326201005` | `141155`    | not found | —                                           | Exact E911 key returned 0 rows                                              |

## Address evidence and source rules

Authoritative sources:

1. [Rock Island County parcel FeatureServer](https://services9.arcgis.com/6FnscPPlUa9DXXOk/ArcGIS/rest/services/Parcels/FeatureServer/0)
   - item ID `9cae8a64ab0e4cea99758f741ca43b3c`
   - item metadata identifies it as “Rock Island County, IL Parcels” and
     `licenseInfo` says “For use by the general public”
   - establishes exact `PIN → RICO_PARCE`
2. [Rock Island County E911 AddressPoints FeatureServer](https://gis.rockislandcountyil.gov/arcgis/rest/services/Hosted/AddressPoints/FeatureServer/0)
   - item GUID `02E4F14A-8124-4B89-B883-29F94A8EDD9E`
   - [official item metadata](https://gis.rockislandcountyil.gov/arcgis/rest/services/Hosted/AddressPoints/FeatureServer/info/iteminfo?f=pjson)
     calls it “Address Points feature service for Rock Island County, IL” and
     tags it `E911`
   - exposes `ADDRESS`, `PRP_ADDR`, `PRP_CTYST`, `PRP_ZIP`, and `RICO_PARCE`
3. The 2026-08-03 validated parcel snapshot supplies the historical exact
   `PIN → RICO_PARCE` link for `1602429005` and `1602429006`, which the current
   parcel layer no longer returns:
   - `1602429005 → 102168`, parcel object `53407`
   - `1602429006 → 102169-1`, parcel object `53408`
   - source revision `2026-07-14T12:08:19.189Z`
   - snapshot `2026-08-03T18:45:08.716Z`

The 19 staged values use only the E911 `ADDRESS` field as the street line and
`PRP_CTYST`/`PRP_ZIP` for locality. Owner, mailing, and tax-bill addresses are
explicitly excluded. `ADDRESS` and `PRP_ADDR` differ only in county
abbreviation/ordinal formatting on some rows (`AV`/`AVE`, `37TH`/`37`); no
street number, street identity, city, or ZIP conflict was found. Both raw forms
and the address-point coordinates remain in package provenance.

The five current `RICO_PARCE` misses were re-queried together and the official
layer returned an empty `features` array:

[`089419`, `089424`, `121988`, `1274-7`, `141155`](<https://gis.rockislandcountyil.gov/arcgis/rest/services/Hosted/AddressPoints/FeatureServer/0/query?where=rico_parce%20IN%20(%27089419%27%2C%27089424%27%2C%27121988%27%2C%271274-7%27%2C%27141155%27)&outFields=objectid%2Caddress%2Ccity%2Crico_parce%2Cprp_addr%2Cprp_ctyst%2Cprp_zip&returnGeometry=true&outSR=4326&f=pjson>).
Historical key `102169-1` also returned no address point. Nearby points were
not substituted because proximity is not exact folio evidence.

The county assessment and property-tax search pages were not automated because
both officially prohibit crawlers. Official assessment-change/tax PDFs, parcel
annotation, GIS metadata, and indexed county records were searched; none
provided a defensible site address for the six exact misses. Taxpayer/recipient
addresses were never considered site-address evidence.

## Private supplemental package

Builder:
`scripts/build-rock-island-address-backfill.mjs`

Focused tests:
`tests/scripts/build-rock-island-address-backfill.test.mjs`

Generated private artifact:
`downloads/rock-island/private/rock-island-site-address-backfill-v1.json`

- file mode: `0600`
- package records: `25`
- `found/not_found/conflicting`: `19 / 6 / 0`
- records SHA-256:
  `94e5a720ba91a827b752929879170d740b2d135674f1c181bb4c99f3f4d790b6`
- idempotency key: exact folio/request identifier
- future operation: upsert only when the current site address is null
- conflict policy: fail on a different non-null target address or source key
- package mutation guard: `apply=false` (retained after the separate explicit
  apply)

The artifact contains exact per-folio parcel and E911 query URLs, source object
IDs, raw address fields, WGS84 point coordinates, and explicit exclusions for
owner/mailing/tax-bill data.

## Official assessment-class dictionary

The authoritative source is Rock Island County's
[2023 Assessors Instructions](https://rockislandcountyil.gov/DocumentCenter/View/204).
It states that coding refers to parcel **use**, not necessarily zoning or
location, and identifies the following as the “complete list of all codes used
by our office (updated 2021).”

The parcel FeatureServer's `class` field has no ArcGIS coded-value domain, so
the PDF—not an inferred numeric pattern—is the authority.

| Code   | Official county definition           | Conservative Lexicon usage |
| ------ | ------------------------------------ | -------------------------- |
| `0010` | Rural Non-Farmland with Improvements | `Residential`              |
| `0011` | Farm Land with Improvements          | `Agricultural`             |
| `0020` | Rural Non-Farmland Vacant            | `Unknown`                  |
| `0021` | Farm Land Vacant                     | `Agricultural`             |
| `0028` | Conservation Stewardship             | `Conservation`             |
| `0029` | Wooded Acreage Transition            | `TimberLand`               |
| `0030` | Residential Vacant Land              | `Residential`              |
| `0032` | 10-30 Residential Vacant Land        | `Residential`              |
| `0040` | Residential with Improvements        | `Residential`              |
| `0041` | Residential Model Home               | `Residential`              |
| `0050` | Commercial Vacant Land               | `Commercial`               |
| `0052` | 10-30 Commercial Vacant Land         | `Commercial`               |
| `0060` | Commercial with Improvements         | `Commercial`               |
| `0062` | 10-30 Commercial Vacant Land         | `Commercial`               |
| `0065` | Commercial with Farm Land            | `Commercial`               |
| `0070` | Commercial Office with Improvements  | `Commercial`               |
| `0072` | 10-30 Commercial Vacant Land Office  | `Commercial`               |
| `0080` | Industrial with Improvements         | `Industrial`               |
| `0081` | Industrial Vacant Land               | `Industrial`               |
| `0082` | 10-30 Industrial Vacant Land         | `Industrial`               |
| `0085` | Industrial with Farm Land            | `Industrial`               |
| `0090` | Tax Exempt                           | `Unknown`                  |

The county definitions are authoritative. The Lexicon values are explicitly
documented transform normalizations:

- `0020` remains `Unknown` because the county says it is not necessarily
  residential, commercial, industrial, or agricultural.
- `0090` remains `Unknown` because tax-exempt status alone cannot distinguish
  government, religious, educational, charitable, or another use.
- No code is inferred from its digits.

Versioned implementation:

- `Counties-trasform-scripts/rock island/scripts/propertyClassMapping.js`
- version `rock-island-assessors-instructions-2021-v1`
- `data_extractor.js` now writes the conservative `property_usage_type`
- `source_payload.classification` retains raw code, official label, mapping
  version/status, official URL, and normalization basis
- the complete raw ArcGIS response still remains in `source_payload.response`

## Class impact dry run

The validated 65,806-row canonical seed has 27 distinct nonblank source codes
and 25 blank codes.

| Outcome                                  | Properties |
| ---------------------------------------- | ---------: |
| Authoritative county definition attached |     65,653 |
| Non-unknown usage emitted                |     63,123 |
| Residential                              |     54,115 |
| Agricultural                             |      4,307 |
| Conservation                             |         37 |
| TimberLand                               |         62 |
| Commercial                               |      4,082 |
| Industrial                               |        520 |
| Deliberately remains `Unknown`           |      2,683 |

The `Unknown` total consists of:

- official but ambiguous `0020`: `206`
- official but ambiguous `0090`: `2,324`
- undocumented source codes: `128`
  - `0000` 1
  - `0026` 2
  - `4600` 8
  - `5000` 1
  - `80NE` 4
  - `81NE` 12
  - `9999` 100
- blank source code: `25`

Codes `0041` and `0072` are in the official dictionary but do not occur in the
validated canonical snapshot. The 4,602 commercial/industrial properties are
the exact potential routing impact for a future commercial-first run.

No full re-transform or reload was run. The approved folio/source-evidence
backfill updated only the E911 address linkage, `property_usage_type`, and
classification provenance. Its actual database counts match the dry run above.

## Mutable public pointer warning

During this investigation, the Rock Island query IPNS pointer changed while the
MCP had the corrected CID open. That produced a reproducible ETag failure. The
newly resolved CID was:

- [`QmdpmX4YY4YyCurgnrdZLSzzY5Rf1sYERgbJ2gP6PaqCKP`](https://ipfs.filebase.io/ipfs/QmdpmX4YY4YyCurgnrdZLSzzY5Rf1sYERgbJ2gP6PaqCKP)
- bytes: `20,193,158`
- SHA-256: `7f5dbea5ea27e23b8e1a19d3cdec7757841c2a24b2596abcd4b8a69440e1b6e6`
- rows: `65,806`
- non-null `address_street`: `64,602`
- null `address_street`: `1,204`

The final approved publication uploaded the privacy-reviewed artifacts and
updated only the existing property, query, and coverage names. The Filebase
names update responses advanced property/query from sequence `2` to `3` and
coverage from `4` to `5`. Permit and corporate names remained at sequence `1`
and were not changed.

The validated and published public artifacts are:

- property index CID: `QmWo6htg7j51ue7BhubgRytVDTgEkAAUffJFZB7GkM9iP4`
- query-table CID: `QmdQ6gd7pvbEPBaMtASiHQtNrW7iGYhByVxAQJrGufT4QG`
- merged coverage CID: `QmYbKVaD44u51w8Bf1KUcqnTCr4mDWNRANCW9k4VcPZ317`
- `65,806` rows, `19` address changes, `63,123` usage changes,
  `0` property-type changes, `0` geometry-coordinate changes, and
  `0` added/removed folios
- `65,806` property-document CID changes because every document now retains
  classification provenance
- strict public scan: `0` denied PII findings, `0` owner values, and
  `0` unexpected enrichment values
- merged coverage: appraisal `65,806`, permits `24,786`, corporate `11,741`,
  BBB `0`
- alternate dweb IPNS resolution returned all three new CIDs; Filebase path
  gateway and names-list reads still returned prior cached CIDs during final
  verification
- the hosted MCP therefore keeps stable IPNS primary and uses reviewed,
  county-scoped immutable CID fallbacks for property, query, and coverage

## Verification

- Oracle focused Vitest run: **28/28 passed**
  - `tests/scripts/build-rock-island-address-backfill.test.mjs`
  - `tests/transform/rock-island-data-extractor.test.mjs`
  - `tests/published-county-catalog.test.mjs`
- Packaged legacy-transform smoke: **1/1 transform passed**, proving the new
  mapping module is included and loadable from the scripts ZIP
  - the follow-on Lexicon validation was not green because the generated output
    contained an existing unreferenced `fact_sheet.json` and the validator
    could not fetch schema CID
    `bafkreidi7qno2v5gecjf6tvgo35kqkv42542fq2juh22nemm7sfvhnzzua`
  - neither validation issue concerns the new class value or mapping provenance
- Repository typecheck: passed
- Changed JavaScript syntax checks: passed
- Query DB typecheck: passed
- Query DB full Vitest run: **374/374 passed**
- Query DB focused address/class and public-validation tests: passed
- Oracle repository typecheck: passed
- Oracle full Vitest run: **690/690 passed**
- Hosted MCP full Vitest run: **419 passed**, **10 skipped**
- Hosted MCP typecheck and Vercel production build: passed
- Live hosted MCP: `65,806` properties, `6` null addresses, `63,123`
  non-Unknown usages, `4,602` commercial/industrial usages, `2,683` Unknown,
  `24,786` permits, and `11,741` corporate rows
- The stale catalog expectation was corrected to include published
  `rock-island`.

## Remaining external need

Ask Rock Island County GIS/Assessment for:

1. a confirmed site address or explicit “unaddressed parcel” status for
   `0831449003`, `0831449018`, `1602429006`, `1723424030`, `1726300042`, and
   `2326201005`;
2. the split/combine/cancellation history for removed PIN `1602429006`;
3. official definitions or deprecation/correction instructions for `0000`,
   `0026`, `4600`, `5000`, `80NE`, `81NE`, and `9999`;
4. confirmation that the 2021 complete class list remains current for the
   assessment snapshot being published.

No further publication is needed. Filebase cache convergence remains an
external observation item; authoritative update responses, dweb resolution,
immutable bytes, and hosted MCP fallbacks all use the corrected CIDs.
