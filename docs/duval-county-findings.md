# Duval County, FL — source discovery and transform reuse findings

**County key:** `duval`<br>
**FIPS:** `12031`<br>
**Florida DOR county number:** `26`<br>
**Scope:** source discovery and an approximately 50-parcel local pilot<br>
**Discovery date:** 2026-09-01

## 1. Appraiser portal

| Item               | Detail                                                                                         |
| ------------------ | ---------------------------------------------------------------------------------------------- |
| Official office    | [Duval County Property Appraiser](https://www.jacksonville.gov/departments/property-appraiser) |
| Property search    | [paopropertysearch.coj.net](https://paopropertysearch.coj.net/)                                |
| Detail URL         | `https://paopropertysearch.coj.net/Basic/Detail.aspx?RE=<identifier>`                          |
| Data downloads     | [Data Offerings](https://www.jacksonville.gov/departments/property-appraiser/data-offerings)   |
| Access mode        | Direct public HTML GET for a known RE number; search itself is ASP.NET WebForms                |
| Authentication     | None observed for search or detail pages                                                       |
| Local reachability | Timeout from non-US locations                                                                  |
| US reachability    | Railway US egress returns the search redirect and complete detail HTML                         |

The detail page is the authoritative structure source. It exposes owners, situs and mailing
addresses, certified and in-progress values, exemptions, sales, land and legal rows, extra
features, one or more buildings, building areas, year built, exterior wall, roof structure,
roof covering, utilities, traversing data, and property-record-card links.

No browser is required when the canonical detail URL is already known. A direct GET produced
complete HTML for all three smoke parcels when called with the exact DOR NAL identifier. Task 5
must apply the same exact-string rule to the remaining seed and quarantine empty responses.

## 2. Parcel identifier

Duval calls its parcel key the **RE Number**.

- The canonical display form is ten digits formatted `######-####`, for example
  `096925-0000`.
- The detail endpoint accepts the undashed ten-digit form.
- Some indexed URLs contain an alias suffix such as `/1000` or `R`; the detail page still
  returns the canonical ten-digit RE Number. Preserve the source identifier as text and
  normalize from the returned page, never by numeric coercion.
- The DOR NAL `PARCEL_ID` remains a string and includes a terminal `R` for these real-estate
  records. DOR PIN `PARCELNO` matches it byte-for-byte. For the smoke set, removing the terminal
  `R` yields the ten-digit detail identifier, and adding a dash after digit six yields the
  canonical display form. A live GET using either the exact DOR value or the ten-digit form
  returned a non-empty page whose canonical RE Number matched in all three cases.

The source archive was the official 2026 preliminary NAL ZIP. It contained 404,023 data rows in
`NAL26F20260827.txt`; the downloaded ZIP was 42,254,989 bytes with SHA-256
`71999eff5639e2bccca961f03c6554174c25c3a11dbdff68008e17f5b05509f6`.

| DOR NAL/PIN identifier | `DOR_UC` | `PA_UC` | Canonical RE Number | COJ property use             |
| ---------------------- | -------- | ------- | ------------------- | ---------------------------- |
| `0969250000R`          | `001`    | `00`    | `096925-0000`       | `0100 Single Family`         |
| `0901770592R`          | `004`    | `00`    | `090177-0592`       | `0400 Residential Condo`     |
| `1230290100R`          | `027`    | `91`    | `123029-0100`       | `2791 Vehicle Show Sale/New` |

This resolves the Task 3 identifier question for the smoke set. Task 5 still repeats the
resolution check for at least five rows from the final deterministic sample.

## 3. Bulk, GIS, and sales sources

The official Data Offerings page publishes monthly county-native downloads for:

- real-estate tax roll (Access and pipe-delimited text);
- GIS shapefiles;
- sales (fixed-format or pipe-delimited text);
- tangible-personal-property roll.

The page listed August 2026 real-estate, GIS, and sales artifacts during discovery. The linked
asset host was not reliable enough to select as the pilot dependency: exact asset URLs returned
HTTP 503 from the laptop and HTTP 404 from Railway US egress. The published real-estate layout
also has `2005-thru-2019` in its filename, so it is not evidence that current county bulk data
contains building-element fields.

The safe pilot route therefore remains:

1. **Florida DOR NAL/SDF/PIN** for the parcel universe, use codes, sales, and geometry.
2. **COJ detail HTML** for building-level roof and exterior-wall facts.

Known DOR baseline:

- NAL 2026P: 404,023 rows.
- SDF 2026P: 64,532 rows.
- PIN 2026F: 405,716 features.
- PIN CRS: EPSG:2881, reprojected to EPSG:4326.
- Duval geometry assertion: latitude `30.103–30.586`, longitude `-82.05–-81.318`.

County-native downloads remain useful fallback inputs if their asset delivery stabilizes, but
they do not currently eliminate the per-parcel structure capture.

## 4. Usage-type vocabulary

The DOR `DOR_UC` and COJ property-use values are distinct classification systems, not alternate
zero-padding of one code. For all three smoke parcels, the COJ code is reconstructed as the last
two digits of `DOR_UC` followed by the two-character county `PA_UC`: `001 + 00 → 0100`,
`004 + 00 → 0400`, and `027 + 91 → 2791`. This rule is verified for the smoke set, not yet
asserted for every Duval subtype. Task 5 uses `DOR_UC` for seed stratification, preserves `PA_UC`,
and retains the resolved COJ value as a separate source field after capture. The pilot must
include at least:

| DOR `DOR_UC` band | Pilot coverage                                               |
| ----------------- | ------------------------------------------------------------ |
| `000–009`         | vacant, single-family, mobile home, multifamily, condominium |
| `010–039`         | commercial                                                   |
| `040–049`         | industrial                                                   |
| `050–069`         | agricultural/timber                                          |
| `070–079`         | institutional                                                |
| `080–089`         | government                                                   |

Never derive one vocabulary by padding, truncating, or numerically coercing the other.

## 5. Permit portals

Duval is consolidated with Jacksonville, but the three beach cities and Baldwin retain
separate permitting surfaces.

| Jurisdiction       | Portal                                                                                                               | Platform                                              | Discovery result                                                                                                                                                                |
| ------------------ | -------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Jacksonville       | [JaxEPICS](https://jaxepics.coj.net/Search/AdvancedSearch)                                                           | Custom Angular application                            | Public search and permit-view routes exist; submission requires a profile. Parcel-search fields and detail payload require adapter discovery.                                   |
| Jacksonville Beach | [COAST](https://jakb-egov.aspgov.com/Click2GovBP/index.html)                                                         | CentralSquare Click2Gov                               | Public historical search, no login; recommends Parcel ID/RE Number and exposes fees, inspections, and plan-review status. Roofing applications are explicitly supported.        |
| Atlantic Beach     | [BS&A Online](https://bsaonline.com/?uid=3261) and [eTRAKiT](https://atlb-trk.aspgov.com/eTRAKiT/Search/permit.aspx) | BS&A intake; CentralSquare eTRAKiT historical search  | BS&A is the current application surface. eTRAKiT exposes historical parcel/address, type, contractor, status, and inspection search but was WAF-blocked from the research host. |
| Neptune Beach      | [Planning & Community Development](https://www.nbfl.gov/planning-community-development)                              | Staff/paper records; online system announced for 2026 | No current public permit-search endpoint confirmed. Request records from the city; application includes an RE Parcel Number.                                                    |
| Baldwin            | [Applications & Agreement Forms](https://baldwinfl.govoffice2.com/?SEC=989BD37A-6DA4-49F8-A566-4D52BFF1E945)         | Staff/form intake                                     | No public permit-search endpoint confirmed. The town publishes permit forms and a roof-inspection affidavit.                                                                    |

The deterministic discovery helper produced a false positive for Jacksonville by classifying
the general city homepage as BS&A. Manual verification overrides it with JaxEPICS. Jacksonville
Beach and Atlantic Beach use different, verified products; Atlantic Beach additionally retains
eTRAKiT for historical search. Neptune Beach and Baldwin remain documented gaps rather than
invented adapters.

`certify.mjs` was rerun on 2026-09-01 from Railway's `us-west2` region using its static-HTML
HTTP/vendor-marker checks (no browser automation). This kept the Jacksonville reachability
constraint from becoming a false unreachable result: **2/5 PASS, 1 REVIEW, 0 unreachable,
2 SKIPPED**. Jacksonville
Beach (Click2Gov) and Atlantic Beach (BS&A) passed. This certification covered the BS&A registry
surface, not the separate eTRAKiT history UI. JaxEPICS was reachable but remains REVIEW because
the static Angular shell does not identify a known vendor or expose permit evidence. Neptune
Beach and Baldwin were intentionally skipped because no public portal was confirmed.

Roof-permit detectability:

- Jacksonville Beach: explicit CivicPlus roofing intake; historical COAST records are searchable.
- Baldwin: explicit roof-inspection affidavit, but no bulk/public search.
- Atlantic Beach: eTRAKiT exposes permit type, subtype, description, contractor, inspections, and
  RE Number; verify roof classification against live records from an allowed host.
- Jacksonville: likely detectable from permit type/description, to verify against guest-search
  records; official instructions confirm address search but not RE Number search.
- Neptune Beach: a roofing application exists, but issued records remain unavailable until
  requested or the announced online system lands.

## 6. Source feasibility

The COJ detail benchmark ran on 2026-09-01 from Railway `us-west2` against four representative
detail pages, 12 requests at each concurrency and 36 requests total. Each request used curl with
redirects enabled, a 30-second timeout, a browser-style user agent, and required HTTP 200 with a
non-empty body:

| Concurrency | Failures |     p50 |     p95 |
| ----------- | -------: | ------: | ------: |
| 1           |     0/12 | 0.490 s | 0.696 s |
| 2           |     0/12 | 0.499 s | 0.796 s |
| 4           |     0/12 | 0.470 s | 0.770 s |

Concurrency 2 is conservative for the 50-parcel pilot. Extrapolating each measured p95 across
404,023 parcels gives 78.1 HTTP hours at concurrency 1, 44.7 at concurrency 2, and 21.6 at
concurrency 4. These are planning estimates, not measured full-county runtimes. Concurrency 1
fails the 48-hour gate, while concurrency 2 is too close once transformation and retries are
included. Concurrency 4 provisionally passes, but a larger bounded soak is required before
approving the full county.

| Source                   | Recommended mode                                                 |
| ------------------------ | ---------------------------------------------------------------- |
| DOR NAL/SDF/PIN          | Bulk download and local seed build                               |
| COJ property detail      | Seed-driven direct HTML capture from US egress                   |
| Jacksonville JaxEPICS    | Adapter discovery; do not assume the SPA API                     |
| Jacksonville Beach COAST | Public permit adapter candidate                                  |
| Atlantic Beach BS&A      | Public permit adapter candidate                                  |
| Neptune Beach / Baldwin  | Documented gap or records request                                |
| Sunbiz                   | Existing statewide bulk ingest, ZIP-scoped                       |
| BBB                      | Existing category/profile harvest after permit contractors exist |
| Overture                 | Existing county-boundary extract                                 |

## 7. Transform reuse gate

The existing `Counties-trasform-scripts/duval/scripts/` package was run against fresh COJ HTML
from Railway US egress:

| Parcel        | Type                        | Result                                                                                                         |
| ------------- | --------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `096925-0000` | Single-family               | Transform completed; 113 artifacts; brick and architectural asphalt shingle mapped.                            |
| `090177-0592` | Condominium                 | Transform completed; 69 artifacts; roof/wall are null because the source page says “No information available.” |
| `123029-0100` | Commercial, three buildings | Transform completed; 112 artifacts; built-up and standing-seam roof values mapped across buildings.            |

The companion [machine-readable field inventory](./duval-transform-field-inventory.json)
preserves every non-empty multi-cell HTML table row and every leaf in `data/*.json` for the three
captures. It contains 263 raw rows, 985 column-aware value pairs, and 679 normalized output-field
entries. Every entry has a machine-readable coverage status; every gap has an `(a)`, `(b)`, or
`(c)` classification and a resolution where applicable. Table headers are explicitly marked
`not_a_fact`; ordinal row keys are `row_container`, with each sibling fact classified under
`value_pairs`. The two non-table boundaries—address headers and Traversing Data—are also classified.
The privacy boundary deliberately excludes free-form owner/address values while preserving the
labels needed for the reuse decision.

The label/field diff produced these classifications:

| Class                         | Source evidence                                                                                                | Decision                                                                                                                                        |
| ----------------------------- | -------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| (a) extractor bug             | `15 Concrete Blk` and space/period variants for roof and structure materials                                   | Fixed six double-escaped regexes; regression covers concrete block, C.B., built-up roof, concrete tile, wood truss, wood rafter, and bar joist. |
| (a) extractor bug             | Building-area `Effective Area` was present while `adjustable_area_sq_ft` remained null                         | Fixed `layoutMapping.js` to map total and per-floor effective area; regression covers the current table layout.                                 |
| (a) extractor bug             | Condo `Beds` and `Baths` were in the condo table, outside `gridBuildingAttributes`                             | Fixed a single-building fallback without leaking counts across commercial buildings; regressions cover unequal condo counts and explicit zero.  |
| (b) page section not captured | Situs/mailing values live in non-table page headers and were null in the isolated inventory run                | No parser repair: the production transform contract receives `unnormalized_address` from capture input. Task 5 must supply it.                  |
| (c) no safe lexicon home      | Land-row `Front`/`Depth` can repeat across components with different unit semantics                            | Preserve in raw evidence; do not guess parcel width/length.                                                                                     |
| (c) no safe lexicon home      | `Traversing Data` is local property-card sketch notation without a documented CRS or stable geospatial meaning | Preserve in raw evidence; do not publish it as parcel geometry. DOR PIN remains authoritative geometry.                                         |
| source absence                | Condo roof/wall rows explicitly say `No information available`                                                 | Keep mapped values null; this is not an extractor defect.                                                                                       |

The current markup still matches the January extractor, so a rewrite is not justified. The
targeted repairs above close the class-(a) gaps found by this smoke set. The inventory is explicitly
the pre-repair evidence snapshot; post-repair behavior is locked by three discoverable Node tests,
run with `npm install && npm test` from `duval/scripts/`.

Outcome: **reuse with a targeted repair**. Full schema and completeness validation remains
Task 7; schema-cache prefetch is not part of discovery. The transformed address writes
`county_name: "Duval"`; Task 5 must also put `county_jurisdiction: "Duval"` in the capture input.

## 8. Pilot parcel set

Task 5 wrote `downloads/duval/duval.csv` (404,023 keyed rows, 0 unkeyed, 0 duplicate
groups) and `downloads/duval/pilot-seed-50.csv` (gitignored). The 50 identifiers are also
recorded in [duval-pilot-seed-50.json](./duval-pilot-seed-50.json). Selection is deterministic:
the three Task 3 smoke parcels first, then one in-range-geometry row per DOR_UC band, then
edge cases, then fill. Rebuild with `npm run duval-seed -- --skip-spot-check`.

The 2026P portal zip currently unpacks `NAL26P202601.csv` (SHA-256
`4ae67aa7550d9d9051c44f01a52ab335897af4959e9625e9ae1e007d77521691`, 29,080,532 bytes) rather
than the August `NAL26F20260827.txt` observed during Task 3. Row count still matches the
published 404,023. PIN centroids reproject inside lat 30.104–30.580, lon -82.047–-81.381.
The PIN sidecar `duval_2026pin.shp.xml` has `<accconst>None</accconst><useconst>None</useconst>`;
see [duval-pin-publication-rights.json](./duval-pin-publication-rights.json).

Live COJ detail pages echoed canonical RE Numbers for the three smoke parcels plus
`000016-0100` (vacant) and `000001-0005` (timber) from the rebuilt ordered sample.
Laptop Brazil still cannot reach COJ; those extra checks used a US fetch path.

| DOR parcel id | DOR_UC | Band               | Notes                        |
| ------------- | ------ | ------------------ | ---------------------------- |
| 0969250000R   | 001    | single family      | Task 3 smoke                 |
| 0901770592R   | 004    | condo              | Task 3 smoke                 |
| 1230290100R   | 027    | commercial         | Task 3 smoke, multi-building |
| 0000160100R   | 000    | vacant residential | Live COJ spot-check          |
| 0000060040R   | 002    | mobile home        |                              |
| 0000320020R   | 008    | multi-family       |                              |
| 0000900100R   | 040    | industrial         |                              |
| 0000010005R   | 055    | agricultural       | Live COJ spot-check          |
| 0000510000R   | 076    | institutional      |                              |
| 0000100015R   | 080    | government         |                              |

After smoke and one row per band, remaining slots fill in `PARCEL_ID` order, so the 50 is
heavy on low-folio section land (ZIP 32234) for agricultural and government. Full list:
[duval-pilot-seed-50.json](./duval-pilot-seed-50.json).

## 9. Additional sources

| Category         | Source and decision                                                                                         |
| ---------------- | ----------------------------------------------------------------------------------------------------------- |
| Sales/deeds      | DOR SDF for bulk sales; COJ sales history links to `oncore.duvalclerk.com` for book/page documents.         |
| Parcel geometry  | DOR PIN joined into the seed; county monthly GIS remains a fallback.                                        |
| Sunbiz           | Available statewide; county extract uses ZIP prefix `322` plus exact ZIP `32099`. Not re-run in this pilot. |
| BBB              | Available nationally; defer harvesting until permit contractor names are available.                         |
| Overture places  | Available through the existing county-boundary extraction workflow.                                         |
| Tax collector    | Out of scope. Payments live at `taxcollector.coj.net` / `duval.county-taxes.com`; values come from COJ/DOR. |
| Recorder         | Out of scope as a harvest. OnCore remains the per-sale deed-image link.                                     |
| Code enforcement | No unified countywide source identified across all five permit jurisdictions.                               |

Publication rights: DOR republishes NAL, SDF, and PIN on the Data Portal in compliance with
chapter 119, Florida Statutes. Posted rolls exclude confidential records such as social security
numbers and owners exempt under s. 119.071. The Duval PIN sidecar
`duval_2026pin.shp.xml` records FGDC `accconst`/`useconst` as `None`.

## 10. Source gaps

Every in-scope catalog category now ends at an official URL or a written unavailability reason
in [duval-sources.yaml](./duval-sources.yaml). The remaining gaps are real and scoped:

| Gap                               | Decision                                                                      |
| --------------------------------- | ----------------------------------------------------------------------------- |
| County-native bulk assets 503/404 | Fallback only. Pilot seed uses DOR NAL/SDF/PIN.                               |
| Roof/wall fields not in DOR NAL   | Per-parcel COJ HTML capture remains mandatory.                                |
| JaxEPICS parcel search / SPA API  | Adapter discovery in a later permit task; guest address search is documented. |
| Atlantic Beach eTRAKiT WAF        | Catalogued as historical search; certify covered BS&A only.                   |
| Neptune Beach and Baldwin         | No public historical-search endpoint. `certify.mjs` SKIPPED by design.        |
| Sunbiz / BBB / Overture           | Available; not re-run during this appraisal capture.                          |
| Code enforcement                  | Unavailable as a unified countywide source.                                   |
| Tax collector / full recorder     | Out of scope for this pilot.                                                  |

`certify.mjs` from Railway US-West remains the reachability evidence (2 PASS, 1 REVIEW,
0 unreachable, 2 SKIPPED). A Brazil laptop re-run can report UNREACHABLE for geo-restricted
portals and is not a catalog defect.

## 11. Risks

- COJ timed out from the tested Brazil connection while Railway US succeeded; pilot capture must
  run from proven US egress.
- County-native bulk asset links are currently unreliable even though the catalog page loads.
- Identifier aliases (`/1000`, `R`) can produce silent misses if coerced or guessed.
- The three small municipalities fragment permit coverage.
- A 36-request benchmark is sufficient for the pilot decision, not for full-county load
  approval.

## 12. Completed pilot stages

1. Built the DOR seed and selected 50 diverse, deduplicated parcels.
2. Derived Sunbiz ZIP prefixes from NAL `PHY_ZIPCD`.
3. Spot-checked five seed identifiers against the live COJ detail endpoint.
4. Ran the portable pilot on Railway US egress at concurrency 2.
5. Completed schema, completeness, geometry, and count reconciliation.
6. Exported the pilot query table and verified it through Donphan.

## 13. Pilot certification and full-ingest readiness (2026-09-02)

The appraisal pilot is certified. Starting the full-county ingest is a **NO-GO until the
operational prerequisites below are complete and concurrency 4 passes a larger bounded soak**.
This is an infrastructure/readiness hold, not a source, identifier, capture, transform, schema,
geometry, or Donphan defect.

| Check               | Result                                                                                                     |
| ------------------- | ---------------------------------------------------------------------------------------------------------- |
| Seed rows           | 50 unique parcel identifiers from a reconciled 404,023-row DOR seed                                        |
| Diversity           | All ten planned `DOR_UC` bands; ten observed transform usage types                                         |
| Geometry            | **50/50** centroids inside the Duval bbox                                                                  |
| Capture + transform | **50/50 success**                                                                                          |
| Lexicon schema      | **50/50 pass**                                                                                             |
| Completeness        | **10.8% mean labeled-field coverage** after subtracting known page chrome; see methodology note below      |
| Required artifacts  | `property.json`, `address.json`, `property_seed.json`, and `input.html` present for all successful parcels |
| Tax / owner         | **50/50** tax and **50/50** owner coverage                                                                 |
| Failures            | **0**                                                                                                      |
| Reconciled counts   | `seedRows == attempted == success + failures` (`50 == 50 == 50 + 0`)                                       |
| Manifest            | `downloads/duval/pilot-run/pilot-manifest.json`                                                            |
| Donphan             | 50 rows; use-code distribution and named parcel values agree with the manifest and COJ evidence            |

The completeness number is the conservative labeled-field heuristic documented in
[duval-appraisal-transform-validation.md](./duval-appraisal-transform-validation.md), not the
Hillsborough artifact-completeness gate. `@elephant-xyz/cli` 1.58.1 does not export
`mirrorValidate`; all 50 transforms nevertheless passed lexicon validation.

### Full-county runtime estimate

The Railway `us-west2` probe measured p50 **0.470 seconds** and p95 **0.770 seconds** at
concurrency 4 with no failures. For 404,023 parcels:

- p50 estimate: `404,023 × 0.470 ÷ 4 = 47,473 seconds` = **13.2 hours**
- p95 estimate: `404,023 × 0.770 ÷ 4 = 77,775 seconds` = **21.6 hours**

Both estimates clear the 48-hour gate, leaving 26.4 hours of p95 headroom for transformation,
retries, and orchestration. They are planning estimates from a 36-request benchmark, not a
full-county load test. Concurrency 2 is not acceptable for the full run because its measured-p95
estimate is 44.7 hours before transformation or retries. Concurrency 4 becomes the safe setting
only after a larger bounded soak confirms the portal tolerates it.

### Prerequisites before changing the decision to GO

1. Confirm production access with `AWS_PROFILE=elephant-oracle-node`.
2. Create the per-county prepare queue with
   `./scripts/create-county-prepare-queue.sh duval`.
3. Stage the reconciled seed at `s3://counties-seeds/duval.csv`.
4. Implement and smoke-test a county-scoped `duval` permit adapter across the fragmented permit
   jurisdictions documented in this report.
5. Implement the county-scoped lookup described by the onboarding contract, then explicitly set
   `PROPERTY_FIRST_PERMIT_ELIGIBLE_USAGE_TYPES_DUVAL=AutoSalesRepair,Industrial` from the
   permit-priority usage types observed in the pilot transform output. The current transform and
   permit-harvest workers read only the unsuffixed `PROPERTY_FIRST_PERMIT_ELIGIBLE_USAGE_TYPES`,
   so setting `_DUVAL` has no effect yet; using the bare variable on the shared process would leak
   Duval policy into concurrent counties. Do not inherit the Lee-vocabulary default, and revisit
   the pilot-derived list when broader Duval usage types are observed.
6. Run a bounded concurrency-4 soak from production-equivalent US egress and retain its
   throughput, failure, throttling, and retry evidence.
7. Complete the human-approved catalog/query-table publication and IPNS map update. The pilot
   intentionally did not publish owner-bearing artifacts to public IPFS.
