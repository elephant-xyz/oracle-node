# Broward County, FL — Source Discovery Findings

Consolidated source-discovery findings for Broward County. Machine-readable
registry: [`broward-sources.yaml`](./broward-sources.yaml).

**County:** Broward County, FL. **FIPS 12011.** **County key:** `broward`.
**DOR county number:** 16 (not FIPS).

**Intake (2026-08-27):** pilot (~25 parcels) then full county if the pilot is
clean. Sources same as Lee: appraisal + permits + Florida Sunbiz + BBB.
Public publish only after a full-county run. This sandbox runs
`elephant-cli prepare` and transform locally, without AWS.

A transform already exists at `Counties-trasform-scripts/broward/scripts/`
(`data_extractor.js` + owner/structure/layout/utility mapping) and reads
`input.json` → `d.parcelInfok__BackingField`. **Reuse it.** Validate on fresh
captures before scaling.

GIS working denominator **556,178** parcels (2026-08-27).

> ## CRITICAL — four things that will silently empty the county
>
> 1. **Folio is text, 12 characters, alphanumeric.** Condos include letters
>    (`504108BJ0140`). Never coerce `parcel_id` to a number. GIS `FOLIO` length
>    is 14; live values are typically 12.
> 2. **Do not send dashed folios** (`474135-01-0090`) to
>    `getParcelInformation` — the API returns a typed envelope with
>    `parcelInfok__BackingField: null`. Use the undashed 12-character folio.
> 3. **`taxyear` must be `""` (empty), not `"CURRENT"`.** Passing `CURRENT` as
>    the tax year returns the same empty envelope. `action` is `CURRENT`.
> 4. **Fail loud on a null/empty `parcelInfok__BackingField`.** GIS has folios
>    the appraiser does not (verified: `474131010000` → empty). A silent skip
>    would look like a clean run.

## 1. Appraiser portal (property)

- **Source:** Broward County Property Appraiser (BCPA), Marty Kiar.
- **Public SPA:** `https://web.bcpa.net/BcpaClient/#/Record-Search`
  (AngularJS + `search.aspx`).
- **REAL data API** (plain POST, JSON, no login, no CAPTCHA, no Cloudflare on
  the JSON endpoints; session cookie optional):
  - Search: `POST https://web.bcpa.net/BcpaClient/search.aspx/GetData`
    body `{"value":"<q>","cities":"","orderBy":"NAME","pageNumber":"1","pageCount":"10","arrayOfValues":"","selectedFromList":"false","totalCount":"Y"}`
    → `d.resultListk__BackingField[]` with `folioNumber`, `ownerName1`,
    `siteAddress1`.
  - Autocomplete: `POST …/search.aspx/PopulateInput`.
  - **Detail (prepare target):** `POST …/search.aspx/getParcelInformation`
    body `{"folioNumber":"<12-char>","taxyear":"","action":"CURRENT","use":""}`
    → `d.parcelInfok__BackingField[0]` — same shape the existing Broward
    transform already parses (owner, situs, mailing, use code, values, sales
    1–5, land calc, picture path, millage, exemptions).
- **No Browser Flow required** for appraisal capture. This is the
  Palm-Beach-style plain-HTTP path.
- **Images:** `picturePath` on the parcel record (transform already emits
  property image files).
- **Map:** `https://gisweb-adapters.bcpa.net/bcpawebmap_ex_new_web/bcpawebmap.aspx?FOLIO=<folio>`
  (GIS layer itself is folio + geometry only).
- **Access:** public. Not geo-blocked from a US IP. Commercial bulk / remote
  electronic access is fee-contracted (`web.bcpa.net/InfoBroward`); the
  per-parcel JSON used by the public search page is the scrape target.
- **Probe (2026-08-27):** 40 GIS-spread folios → 40/40 non-empty details,
  mean **0.40 s**. Ten sequential details: p50 **0.35 s**, min 0.16, max 0.38.
  Empty-id `999999999999` returns null (usable as a hard error).

## 2. Parcel identifier

- **Official name:** Folio / Property ID.
- **Appraiser format:** 12-character string, digits **or** letters. Display
  grouping in the SPA is `XXXXXX-XX-XXXX` (6-2-4) for digit-only folios;
  **do not send that dashed form to the API.**
- **GIS `FOLIO`:** same 12-character key. Verified match:
  GIS `474135010090` = API `folioNumber` `474135010090`.
- **Permit BCS:** same page accepts **Folio Number** (maxlength 12, example
  `0215-01-0130`) **or** **Parcel ID** (maxlength 12, example `504215010130`).
  Confirm which BCS field accepts the undashed PA folio during the permit
  adapter; start with `ParcelID` = PA `folioNumber`.
- **Verified examples:**
  - `474135010090` — 10-01 Vacant Commercial, LOXAHATCHEE ROAD, Parkland
  - `504201090030` — 01-01 Single Family, 19 ROYAL PALM DRIVE
  - `504108BJ0140` — 04 Condominium (letters in folio), Plantation

## 3. Permit portal(s)

Permits are **municipal and fragmented**. County BCS (POSSE) covers
unincorporated plus a county-level search; cities run their own vendors.
Per the Lee/PB pattern, permits are **on-demand, not bulk**.

| Jurisdiction                   | Portal / vendor                        | URL                                                                                  | Probe                                                             |
| ------------------------------ | -------------------------------------- | ------------------------------------------------------------------------------------ | ----------------------------------------------------------------- |
| Unincorporated / county search | POSSE / Amanda (BCS)                   | `https://dpepp.broward.org/BCS/Default.aspx?PossePresentation=ParcelSearchByAddress` | 200, folio+parcel+address fields                                  |
| Hollywood                      | Accela Citizen Access                  | `https://aca-prod.accela.com/hollywood/default.aspx`                                 | 200                                                               |
| Plantation                     | Accela Citizen Access                  | `https://aca.plantation.org/CitizenAccess/`                                          | 200                                                               |
| Fort Lauderdale                | Accela (`FTL`, LauderBuild)            | `https://aca-prod.accela.com/FTL/Default.aspx`                                       | 200 landing; CapHome Error.aspx — needs module/tab during adapter |
| Coral Springs                  | CentralSquare eTRAKiT                  | `https://etrakit.coralsprings.gov/eTRAKiT/Search/permit.aspx`                        | 200, folio search, reCAPTCHA on search                            |
| Pompano Beach                  | Click2Gov                              | `https://c2g.pompanobeachfl.gov/Click2GovBP/selectpermit.html`                       | 200, parcel number search                                         |
| Davie                          | eSuite                                 | `https://esuite.davie-fl.gov/eSuite.Permits/WelcomePage.aspx`                        | 200, public address search                                        |
| Pembroke Pines                 | Tyler EnerGov / Civic Access           | `https://pembrokepinesfl-energovweb.tylerhost.net/apps/selfservice`                  | 200 (redirect from `ppines.com/developmenthub`)                   |
| County ePermits OneStop        | apply/review hub, not a harvest search | `https://www.broward.org/epermits`                                                   | 200                                                               |

Weston and Sunrise official department pages 403'd from this egress. Catalog
rows for the remaining cities are `needs-review` until the certify pass.

**County BCS** is the first permit adapter to build for the pilot (search by
folio/parcel, POSSE session). Accela cities can reuse the Lee Accela harvester
with a per-agency code (`hollywood`, `FTL`, Plantation host).

## 4. Bulk data sources / seed

Preference used:

1. **BCPA GIS parcels (seed identity + geometry).**
   `https://gisweb-adapters.bcpa.net/arcgis/rest/services/BCPA_EXTERNAL_JAN26/MapServer/16`
   - 556,178 features, `maxRecordCount` 1000, `outSR=4326` polygons.
   - Public fields: `FOLIO` (+ shape metrics). Owner/address/value were
     removed from public REST in the 2026-06-06 migration to
     `gisweb-adapters.bcpa.net`. **Do not use this layer for appraisal facts.**
   - Seed `parcel_id` = `FOLIO` as text. Attach `parcel_polygon` / lon / lat
     from the geometry (transform already reads seed CSV geometry).
2. **Florida DOR NAL / GIS shapefiles** — public portal
   `https://floridarevenue.com/property/Pages/DataPortal_RequestAssessmentRollGISData.aspx`
   (path `Tax Roll Data Files` / `Map Data`). Fallback if GIS paging is
   awkward; NAL also supplies DOR use codes for commercial-first sort.
3. **County PA bulk** — fee / contract (`InfoBroward`). Not used.

No `s3://counties-seeds/broward.csv` was assumed; this run will stage
`data/seeds/broward.csv` from GIS (full) and `broward-pilot.csv` (~25 rows).

## 5. Usage-type vocabulary

BCPA `useCode` looks like `01-01 Single Family`, `10-01 Vacant Commercial`,
`04 - Condominium`, `48-04 Warehouse - Metal`. The existing transform maps
these via `propertyUseCodeMappings` onto lexicon `property_usage_type`.

From a 40-parcel GIS-spread probe: 01 residential, 03/08 multi-family, 04
condo, 09 common elements, 12 mixed store/office, 48 warehouse, 63 ag, 94
ROW. **Permit-eligible override must not stay on Lee vocabulary.** Collect
the actual transform `property_usage_type` values at validation, then set
`PROPERTY_FIRST_PERMIT_ELIGIBLE_USAGE_TYPES` (commercial / industrial /
mixed). Residential and ROW should skip.

## 6. Additional data sources

| Source                      | URL                                                         | Bulk?                       | In scope?                                                        |
| --------------------------- | ----------------------------------------------------------- | --------------------------- | ---------------------------------------------------------------- |
| Sunbiz (FL statewide)       | `https://dos.fl.gov/sunbiz/other-services/data-downloads/`  | yes                         | **yes** (reuse Lee; Broward ZIP prefixes only)                   |
| BBB                         | national category harvest                                   | yes                         | **yes** (filter to Broward area)                                 |
| Tax collector               | `https://browardtax.org/`                                   | unknown                     | discovery only                                                   |
| Recorder / official records | `https://officialrecords.broward.org/`                      | 10-day FTP images           | Cloudflare on the search UI; out of ingest unless later approved |
| County GIS (planning)       | `https://gis.browardcountyfl.org/` / `broward.org/Planning` | parcels are **fee from PA** | geometry taken from BCPA GIS instead                             |
| Code enforcement            | BCS `ParcelSearchForEnforcement`                            | on-demand                   | not in this run                                                  |

Broward ZIP prefixes for Sunbiz (not exhaustive): `33004`, `33009`,
`33019`–`33029`, `33060`–`33076`, `33301`–`33334`, `33351`.

## 7. Source feasibility

| Source                           | Records          | Probe                                                                 | Safe conc. (start) | Est. elapsed                                          | Mode                                                                |
| -------------------------------- | ---------------- | --------------------------------------------------------------------- | ------------------ | ----------------------------------------------------- | ------------------------------------------------------------------- |
| Appraisal `getParcelInformation` | 556,178          | p50 ~0.35–0.40 s, 0 empties in 40 spread (1 empty in a geographic 10) | 2                  | ~31 h at conc 2; ~15 h at conc 4; **~54 h at conc 1** | per-parcel HTTP; **48-hour gate: do not run full county at conc 1** |
| GIS seed page                    | 557 pages × 1000 | 0.20–0.28 s/page                                                      | 1–2                | ~3 min                                                | bulk artifact (FOLIO + polygon)                                     |
| County BCS permits               | unknown / parcel | POSSE HTML, session                                                   | 1–2                | TBD in adapter probe                                  | runtime per eligible parcel                                         |
| City Accela / Tyler / eTRAKiT    | fragmented       | mixed; Coral Springs has reCAPTCHA                                    | 1–2                | TBD; likely **>48 h if all cities**                   | on-demand eligible parcels; ask before prefetching every city       |

Full-county appraisal at measured latency is under 48 hours only with
concurrency ≥ 2. Permits across 32 cities are the likely >48 h source —
keep them property-first / eligible-only unless the operator chooses
otherwise after the pilot timings.

## 8. Risks

- **Empty GIS-only folios** — treat as `dead` / gone-at-source, counted, not
  silent success.
- **Alphanumeric condos** — any numeric parser or zero-pad-to-12-digits
  logic will drop `BJ`/`AK` folios.
- **GetData 500** if `orderBy` is omitted (empty string). Use `NAME`.
- **Coral Springs eTRAKiT reCAPTCHA** — browser or skip that city until
  solved.
- **Fort Lauderdale / Weston / Sunrise** — city marketing sites 403 from
  datacenter-like egress; Accela `FTL` host itself is reachable.
- **Recorder Cloudflare** — do not use curl-only for official records.
- **PA commercial-use policy** — InfoBroward fees apply to contracted bulk
  dumps; per-parcel public search JSON is what the website itself calls.

## 9. Pilot folio set (~25)

Chosen from the GIS-spread + geographic probes so the pilot hits commercial
(permit path), residential (skip), condo-with-letters, unincorporated, and
ROW:

```
474135010090  10-01 Vacant Commercial     PARKLAND
494209060010  48-04 Warehouse - Metal     FORT LAUDERDALE
494318013550  12-02 Mixed store/office    LAUDERDALE BY THE SEA
484109030410  01-01 Single Family         CORAL SPRINGS
494212072320  01-01 Single Family         FORT LAUDERDALE
504201090030  01-01 Single Family         (UrbanKit / residential)
503912010490  01-01 Single Family         WESTON
513914101320  01-01 Single Family         PEMBROKE PINES
514111160200  01-01 Single Family         HOLLYWOOD
494119160090  01-01 Single Family         SUNRISE
494109050270  01-05 Zero Lot Line         TAMARAC
504118051290  01-04 Townhome              DAVIE
494202352310  01-04 Townhome              POMPANO BEACH
504108BJ0140  04 Condominium              PLANTATION
494108AK1220  04 Condominium              TAMARAC
484201BA0050  04 Condominium              DEERFIELD BEACH
494123BJ0010  04 Condominium              LAUDERHILL
504209091840  08 Multi-family <10         FORT LAUDERDALE
514207022070  03-01 Multi-family 10-49    HOLLYWOOD
474135010091  63 Grazing                  PARKLAND
504026140250  63 Grazing                  SOUTHWEST RANCHES
474134000012  52 Cropland                 UNINCORPORATED
514106100100  94 ROW                      COOPER CITY
514123070029  94-01 ROW                   MIRAMAR
484230301500  01-01 Single Family         COCONUT CREEK
```

## 10. Appraisal onboarding increment (2026-08-27)

Landed in `oracle-node`:

- `multi-request-flows/Broward.json` POSTs
  `getParcelInformation` with CLI-valid `content-type: application/json` and
  a `json` body (`taxyear: ""`, `folioNumber` templated from
  `request_identifier`).
- `scripts/broward-folio.mjs` preserves the canonical 12-character
  alphanumeric folio. `scripts/build-broward-seed.mjs` pages the BCPA GIS
  layer and emits the columns required by prepare, including `county=Broward`
  so workflow routing selects the Broward prepare queue.
- `scripts/capture-broward-parcel.mjs` provides a fail-closed source probe.
  `scripts/enqueue-broward-appraisal-property-first-from-seed.mjs` and
  `scripts/send-broward-seed-feeder.mjs` provide the pilot/direct and
  backpressure-aware enqueue paths without Orange PID normalization.

Live source verification from this environment:

- `build-broward-seed.mjs --pilot` wrote 25 unique rows with no invalid
  folios. The seed retained `504108BJ0140` and set every seed county to
  `Broward`.
- Commercial `474135010090` returned one parcel record with use code
  `10-01 Vacant Commercial`; alphanumeric condo `504108BJ0140` returned one
  record with `04 - Condominium`; residential `484109030410` returned one
  record with `01-01 Single Family`.
- Invalid folio `999999999999` returned a null parcel list and the capture
  command exited nonzero through `requireParcelRecords`, as required.
- Local `elephant-cli prepare` fetched **25/25** pilot folios. The published
  Broward extractor then transformed **16/25**; the other nine crashed on
  family-level use codes such as `04 - Condominium`. A local matcher patch
  recovered **25/25**. Details:
  [`broward-appraisal-local-pilot.md`](./broward-appraisal-local-pilot.md).
- The focused Broward tests and `npm run typecheck` passed.

This environment has no AWS credentials and no Restate/Postgres stack. No
pilot messages were enqueued. Local prepare/transform does not replace an
AWS smoke: before that, PR the use-code matcher and multi-request unwrap
into `Counties-trasform-scripts/broward`, run
`scripts/create-county-prepare-queue.sh broward`, deploy so `Broward.json`
is uploaded to S3, and stage the seed.

Remaining gates are that transform-script PR, the Broward BCS permit
adapter and city-vendor routing, and a clean AWS 25-parcel smoke. Do not
start the full-county run or publish until those gates are clean.
