# Pinellas County, FL — Source Discovery Findings

Consolidated source-discovery findings for Pinellas County, produced by the
`county-discovery` stage. Mirrors the Lee County reference
([`lee-county-findings.md`](./lee-county-findings.md)); the machine-readable registry is
[`pinellas-sources.yaml`](./pinellas-sources.yaml).

**County:** Pinellas County, FL. **FIPS 12103.** GIS tax-parcel polygons ≈ **311,582**
(`egis.pinellas.gov` PCPA Tax Parcels layer). PublicWebGIS Parcels layer also exposes
owner / use-code / value / site-address attributes for seed selection.

A transform already exists at `Counties-trasform-scripts/pinellas/scripts/`
(`data_extractor.js` + owner/structure/layout/utility mapping) targeting
`https://www.pcpao.gov/property-details`. **Reuse it, do not rebuild** — but
**per-field validation is owed at `validate-county-transform`**, and seed IDs must
use **STRAP** (see critical note below). Prepare is **plain HTTP**, not a browser
flow: `multi-request-flows/Pinellas.json` GETs the print URL with a Chrome UA.
Local `elephant-cli prepare` of mixed STRAPs returns populated print HTML.
Multi-request capture files are `{STRAP}.json`; the CLI copies those to
`input.json`, so local ingest unwraps `PropertyPrint.response` into `input.html`
before the existing Pinellas scripts run (`scripts/run-pinellas-local-ingest.mjs`).
Print page IDs (`land_info`, `sale_history`, `structure_1`, `permit_data`,
`value_history`) differ from the Drupal `property-details` tables (`#pacel_no`,
`tblParcelInformation`). `Counties-trasform-scripts/pinellas/scripts/printHtml.js`
plus `data_extractor.js` now fall back to print span labels and those table ids.
Local re-transform (2026-08-27) of mixed captures produced `property_usage_type`
`Residential` (SFR + vacant lot) and `RetailStore` (store), with sales/tax/permit
rows present. Mapping modules (`ownerMapping.js`, `structureMapping.js`) still
prefer Drupal `#pacel_no` / `structural_*` panels — structure/layout/utility
coverage on print HTML is still incomplete.

**Pilot seed (2026-08-27):** `data/seeds/pinellas-pilot.csv` — **50 unique STRAPs**,
16 use groups, 11 complex polygons / 39 simple, WGS84 `parcel_polygon`. Built from
PublicWebGIS Parcels (`USE_CODE` quotas) by `scripts/build-pinellas-pilot-seed.mjs`.
First 5 STRAPs returned populated PCPAO print HTML (hard-fail on empty lookup).
This is the **pilot** input of record, not the full ~311k county roll.

Findings live in `oracle-node/docs/` (this checkout’s convention; Lee/Orange already
live here). A copy belongs in `Counties-trasform-scripts/pinellas/docs/` once `gh`
can open a PR.

> ## CRITICAL — STRAP vs PARCELID
>
> Pinellas has **two different 18-digit identifiers** for the same polygon:
>
> | Field | Example | Meaning |
> |---|---|---|
> | **STRAP** | `162805389030000430` | RANGE-TOWNSHIP-SECTION-SUB-BLOCK-LOT, no punctuation. Matches PCPAO **download files** and the working `s=` lookup. |
> | **PARCELID** | `052816389030000430` | SECTION-TOWNSHIP-RANGE-SUB-BLOCK-LOT. GIS metadata says this matches the PAO **website search** / tax viewer. |
> | Display | `05-28-16-38903-000-0430` | Punctuated PARCELID shown on the print page. |
>
> Live probe (2026-08-27): `GET /property/detail/print?is_print=1&s=<STRAP>` returned a
> **full** record (owner Frobose, 3400 Rugby Ct, Palm Harbor, use 0110, values, building,
> county-sourced permits). The same URL with **PARCELID** returned an **empty
> placeholder** (0 buildings, “No Sales on Record”).
>
> **Seed `parcel_id` must be STRAP.** Using PARCELID as `s=` will silently produce
> empty HTML and fail transform completeness.

Egress for this discovery: US (Miami, FL). Not geo-blocked on PCPAO / Accela / GIS.

## 1. Appraiser portal (property)

- **Source:** Pinellas County Property Appraiser (PCPAO).
- **Public UI:** `https://www.pcpao.gov/` (Drupal 7). Search home + property-details SPA
  chrome. Interactive detail: `https://www.pcpao.gov/property-details?s=<STRAP>`.
- **Best scrape artifact (plain HTTP, Chrome UA):**
  `GET https://www.pcpao.gov/property/detail/print?is_print=1&s=<STRAP>`
  - Curl with a browser User-Agent: **200**, ~0.4s, ~52 KB HTML for a typical SFR.
  - Bare curl (no UA) to some PCPAO paths returns **403**. Send a Chrome UA.
  - Print HTML includes: parcel number, owner(s), property use, site + mailing
    addresses, legal, tax district, year built, living/gross SF, living units,
    buildings, homestead status, just/assessed/taxable values + history, millage,
    sales history, land (dimensions, method, value), building structural elements
    + subareas, extra features, and a **permit table** (county-sourced, incomplete).
  - Drupal interactive page (`property-details`) is a shell plus
    `jquery.property_detail_new.js`; `#pacel_no` and tables (`tblParcelInformation`,
    `tblSalesHistory`, `tblLandInformation`, `tblExtraFeatures`, `tblPermit`,
    `tblValueHistory`, `tblExemptions`) are the transform’s current selectors.
    Playwright opened the page (200, no CAPTCHA) but the print URL is the reliable
    curl-complete artifact.
- **Hidden DAL:** `https://www.pcpao.gov/dal/…` (PHP). Confirmed download endpoint
  `POST/GET /dal/databasefile/downloadDatabaseFile` (Playwright click on
  `RP_ALL_SITE_ADDRESSES` CSV produced `RP_ALL_SITE_ADDRESSES_csv.zip`). Other
  `/dal/search/*` guesses 404/500 — do not invent DAL routes; prefer print HTML +
  published bulk files.
- **Access:** public, no login. No Cloudflare on PCPAO. UA-sensitive (403 without
  browser UA). Not geo-blocked from US. Google Translate widget pollutes `body`
  innerText; parse named tables / print HTML, not raw `body` text.
- **Refresh:** seed-driven per-parcel print/HTML + nightly bulk CSVs (PCPAO stamps
  files “Aug 27, 02:1x AM” on the downloads page).

## 2. Parcel identifier

- Official name: **STRAP** (PCPAO: “an 18 digit number assigned to each parcel of
  land and each living unit within a condominium or cooperative”).
- Format: RANGE + TOWNSHIP + SECTION + SUBDIVISION + BLOCK + LOT, **digits only**,
  length **18**.
- Punctuated display is PARCELID-ordered (`SS-TT-RR-SUBBB-BLK-LOT`).
- **Appraiser vs GIS vs permits:** STRAP for PCPAO downloads and `s=` print lookup;
  PARCELID for some GIS / tax-viewer searches; Accela search is primarily
  **address**, with parcel as a secondary field. Permit record numbers on the
  appraisal print page use prefixes `PER-H-CB`, `PER-H-CW`, `EBP-` (unincorporated
  / county Accela-style).
- **Do not treat STRAP and PARCELID as interchangeable.**

## 3. Permit portal(s)

Permits are **municipal and fragmented**. Unincorporated Pinellas uses county
**Accela Citizen Access**. Incorporated cities run their own vendors. PCPAO’s
appraisal print page already lists some permits (“received from the County and
Cities”) and warns the table is incomplete.

| Jurisdiction | Vendor / portal | URL | Probe (2026-08-27) | Status |
|---|---|---|---|---|
| Unincorporated Pinellas County | Accela ACA, agency `PINELLAS`, module `Building` | `https://aca-prod.accela.com/PINELLAS/default.aspx` · CapHome `…/Cap/CapHome.aspx?TabName=Home&module=Building` | Playwright + curl **200**, no Cloudflare. Guest search allowed; results default to last 2 years (change start date). Search-by-address recommended; parcel supported in General Search. | discovered |
| City of Clearwater | Accela ACA, agency `CLEARWATER` (also advertised as ePermit) | `https://aca-prod.accela.com/CLEARWATER/Default.aspx` | Playwright **200**. `epermit.myclearwater.com` timed out from this egress. | discovered |
| City of Largo | Tyler EnerGov Civic Access | `https://cityoflargofl-energovweb.tylerhost.net/apps/selfservice#/home` | Playwright **200** | discovered |
| City of Pinellas Park | Tyler Portico / EnerGov | `https://pinellasparkfl.tylerportico.com/navigator/public/selections/navigator?parentId=5996` | Playwright **200** | discovered |
| City of Dunedin | Tyler Enterprise Permitting & Licensing (CSS) | Dunedin Citizen Self Service (linked from `dunedin.gov` Permits & Inspections) | Official page confirmed; CSS URL not latency-probed this run | discovered (URL certify next) |
| City of Tarpon Springs | Click2Gov / aspgov | `https://tarp-egov.aspgov.com/Click2GovBP/index.html` | Playwright **200**; search by application #, address, **parcel number**, or name | discovered |
| City of St. Petersburg | City site + ProjectDox ePlan (`stpetersburg-fl-us.avolvecloud.com`) | `https://www.stpete.org/business/building_permitting/building_permits.php` | Playwright **200** on city page. Historical lookup vs application portal still needs a public search-by-parcel certification. | needs-review |
| Remaining municipalities (Belleair, Belleair Beach, Belleair Bluffs, Belleair Shore, Gulfport, Indian Rocks Beach, Indian Shores, Kenneth City, Madeira Beach, North Redington Beach, Oldsmar, Redington Beach, Redington Shores, Safety Harbor, Seminole, South Pasadena, St. Pete Beach, Treasure Island) | mixed / some may use county Accela | — | Catalog rows seeded `needs-review`. Web sources: Seminole reportedly CitizenServe; some beach towns / Safety Harbor / Oldsmar may defer to county Building Services. | needs-review |

**Pilot recommendation:** harvest county Accela (`PINELLAS`) + reuse appraisal-page
permit rows for the ~50-parcel sample. Do **not** bulk-harvest every municipal
portal before the appraisal pilot is certified. One adapter per vendor via
`county-permit-adapter` (Accela already exists for Lee).

## 4. Bulk data sources (seed + geometry)

| Source | URL | What it gives | Access |
|---|---|---|---|
| PCPAO raw database files | `https://www.pcpao.gov/tools-data/data-downloads/raw-database-files` | 30 `RP_*` / `RP_OS_*` tables (CSV/JSON/XLSX/XML), refreshed ~nightly. Includes site addresses, owners, building, land, legal, sales, permits, structural elements, extra features, millage. **STRAP** is the join key. | Drupal buttons → `/dal/databasefile/downloadDatabaseFile` → zip. Playwright confirmed. |
| PCPAO shapefiles | `https://www.pcpao.gov/tools-data/maps-gis/shape-files` | Parcel polygons + label points. NAD 1983 HARN StatePlane Florida West ft (EPSG:2882). Fields: PARCELNO (DOR mapping), PARCEL_ID (DOR NAL/SDF), **STRAP** (PAO downloads), **PARCELID** (PAO website). | Browser download (page 403 to bare curl). |
| PCPA GIS REST | `https://egis.pinellas.gov/pcpagis/rest/services/PcpaBaseMap/BaseMapParcelAerials/MapServer/157` | Tax Parcels polygons, **count 311,584**, maxRecordCount 15,000. | Plain JSON, no auth. |
| PublicWebGIS Parcels | `https://egis.pinellas.gov/gis/rest/services/PublicWebGIS/Parcels/MapServer/1` | Owner, USE_CODE, values, site address, acres, polygon. maxRecordCount 1,000. | Plain JSON, no auth. **Best mixed-type seed picker.** |
| Accela GIS parcels | `https://egis.pinellas.gov/gis/rest/services/Accela/AccelaAddressParcel/MapServer/1` | Parcel layer count **437,499** (units/addresses vs tax polygons). | Plain JSON. |
| Open Data portal | `https://egis.pinellas.gov/apps/egis/apps.html` | County GIS apps / open data. | 200. |
| Advanced Search | `https://www.pcpao.gov/content/advanced-search` | Filtered export by use code (pilot seed helper). | Drupal UI. |

**Seed source for the 10–50 parcel pilot:** PublicWebGIS query by `USE_CODE` (and
geometry ring-count) → emit STRAP + PARCELID + use + city + polygon WGS84, then
dedupe on STRAP.

## 5. Usage-type vocabulary

Official list: `https://www.pcpao.gov/learn-about/use-codes` (4-digit `PROPERTY_USE`
/ GIS `USE_CODE`). Not zoning.

Commercial / industrial (permit-harvest eligibility, Lee pattern):

- **Vacant commercial / industrial:** 1000, 1035, 1090, 4000, 4090
- **Commercial:** 11xx–39xx (stores, office, marina, restaurant, lodging, etc.)
- **Industrial:** 41xx–49xx (light/heavy manufacturing, warehouse, mini-storage)
- **Institutional / gov:** 70xx–89xx
- **Residential (not default permit-priority):** 0000–09xx, 0110 SFR, 0430 condo, 082x multiplex

Probed GIS examples (one each): vacant residential 0000, SFR 0110, condo 0430,
vacant commercial 1000, office 1730, vacant industrial 4000, light manufacturing
4120, church 7153, vacant county gov 8012 — all returned polygons.

## 6. Additional data sources

Lee’s Oracle baseline is four feeds: **appraisal, permits, Sunbiz, BBB**. Everything
else below is a real county site we found, then parked because this pilot does not
need it. “Unavailable” here means **out of scope**, not “the website is missing.”

| What | Why we skipped it for this pilot |
|---|---|
| Tax collector bills | PCPAO already has values. The tax site is one-parcel-at-a-time, not a downloadable roll. |
| Clerk / official records (deeds) | Deed *images* sit behind a bot wall. Book/page is already on the PCPAO print page. |
| Code enforcement | Accela has a Code Enforce module. That is complaints/cases, not building permits. |
| City business tax (BTR) | City license receipts, not property data. |
| FEMA / flood maps | Linked from the print page; not appraisal or permit records. |

18 cities still `needs-review` in `pinellas-sources.yaml` because discovery only
opened the big permit portals (county Accela, Clearwater, Largo, Pinellas Park,
Dunedin, Tarpon Springs). Those cities are **not documented as having no source** —
we have not confirmed their URLs yet.

### What this run actually ingested

Local laptop prepare+transform (`scripts/run-pinellas-local-ingest.mjs --all`), not AWS
Oracle. **No-AWS path:** that command *is* the pilot ingest. Re-run anytime:

```bash
node scripts/run-pinellas-local-ingest.mjs --all
```

Use `--skip-validate` to only prepare+transform. Lexicon `validate` needs the local
Kubo gateway (`downloads/pinellas/kubo`, `127.0.0.1:8080`).

**Pilot ingest 2026-08-28 (50 STRAPs, appraisal-only):** Node 24, US egress, Kubo up.
`node scripts/run-pinellas-local-ingest.mjs --all` transformed all 50 STRAPs. A later
re-transform the same day (structured Address fields from print HTML; path-only
`source_http_request.url`) reached **50/50 lexicon validates**. Loaded into **local
Postgres** `elephant` (`jurisdiction_key=pinellas_appraiser`, **50 distinct
folios**, 0 orphan properties). **Neon not loaded** — no `DATABASE_URL` in
`elephant-query-db/.env.local`. Published query-table + coverage IPNS
(`oracle-query-table-pinellas`, `oracle-dataset-coverage-pinellas`) and catalog
entry. Structures/layouts/utilities remain 0 on print HTML (known mapping gap).

| Source | In this local run? | Count / artifact |
|---|---|---|
| PCPAO print HTML (appraisal) | **Yes** | **50/50** seed STRAPs → `downloads/pinellas/local-ingest/<STRAP>/transformed.zip` |
| GIS parcel polygons | **Yes (seed only)** | All 50 rows in `data/seeds/pinellas-pilot.csv` (`parcel_polygon`) |
| County / city building permits | **No** | Catalogued; no permit adapter run |
| Sunbiz | **No** | In scope later; statewide zip not downloaded |
| BBB | **No** | In scope later; not harvested |
| Tax / clerk / code / BTR / FEMA | **No** | Out of pilot (table above) |

### Full-county local ingest (2026-08-31)

Operator scope: **all tax parcels, appraisal only, this machine through public Filebase/IPFS**. AWS Step Functions / seed feeder are **out of this run**. Permits / Sunbiz / BBB are out of this run.

**Seed of record (generated, not committed):** `data/seeds/pinellas.csv` via `scripts/build-pinellas-full-seed.mjs` from PCPA GIS tax parcels layer 157 (not the Accela address layer). Snapshot 2026-08-31T17:01:21Z: GIS count **311,582**, unique 18-digit STRAPs **311,566**, 1 invalid STRAP skipped, FIPS **12103**.

**Local start sequence:**

```bash
node scripts/run-pinellas-local-ingest.mjs \
  --seed data/seeds/pinellas.csv \
  --all \
  --skip-validate \
  --skip-existing \
  --concurrency 4 \
  --fetch-concurrency 8 \
  --fetch-timeout-ms 12000 \
  --scripts downloads/Counties-trasform-scripts/pinellas/scripts \
  --output downloads/pinellas/local-ingest
```

The ingest GETs print HTML with a Chrome UA (12s timeout, 8 in-flight fetches), runs Pinellas scripts in **persistent workers** (no per-parcel Node spawn), and writes `downloads/pinellas/local-ingest/<STRAP>/transformed.zip`. Restart-safe: existing zips are skipped in a pre-scan. Progress is `status.json`. Lexicon validate is skipped for the full roll (already proven 50/50 on the pilot).

Hung print GETs previously stalled the run for hours; `--fetch-timeout-ms` aborts those. Do not jump fetch concurrency past 8 without watching for PCPAO 403s.

When the roll is complete:

```bash
node scripts/publish-pinellas-pilot-to-filebase.mjs \
  --seed data/seeds/pinellas.csv \
  --ingest-dir downloads/pinellas/local-ingest
```

That rebuilds the query-table parquet + coverage snapshot and re-points the **existing** Pinellas IPNS names (`oracle-query-table-pinellas`, `oracle-dataset-coverage-pinellas`). Filebase `S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY` / `FILEBASE_API_TOKEN` are required at publish time.

48-hour gate: print HTML @ 0.28s + local transform. Start at concurrency 2. Do not jump to 8; PCPAO is UA-sensitive.

## 7. Source feasibility

| Source | Volume | Probe | Safe concurrency | Est. full download | Recommended mode |
|---|---|---|---|---|---|
| PCPAO print HTML | ~311k tax parcels | 8 sequential `property-details` ~2.8–3.1s (Playwright, includes wait); print URL ~0.4s curl | Start **2**; PCPAO is UA-sensitive and Drupal sessioned. Do not jump to 8. | Print @ 0.4s × 311k serial ≈ **35 h**; conc 2 + delay ≈ **20–24 h**. Under 48h. | **Pilot:** print HTML for ~50 STRAPs. **Countywide:** prefer **bulk `RP_*` CSVs** for roll attributes + shapefile/REST for geometry; HTML only for fields the transform still needs from the page. |
| PCPAO bulk CSVs | 30 tables, nightly | Download zip via DAL after UI click | 1–2 (large zips) | Minutes–low hours depending on table | **Bulk artifact download** (seed + completeness). |
| GIS REST | 311,582 tax polygons | count + mixed USE_CODE queries succeeded | 1–2 (maxRecord 1k–15k; page with `resultOffset`) | Minutes | **Bulk / seed.** |
| Accela PINELLAS | unknown permit count; search last-2-years default | Portal 200, guest search | Lee Accela used conc ~2–4 | Unknown until a 10–25 record timing pass on CapHome | **Runtime retrieval** for pilot; date-window harvest later if <48h. |
| Municipal permits | 24 cities + county | 5 portals 200; rest uncertified | Vendor-specific | Do not full-download | **Runtime retrieval** per vendor adapter. |
| Sunbiz | statewide quarterly zip | Downloads page 200 in browser | 1 | Hours (zip + Deflate64 unzip) | **Bulk** statewide, ZIP-filter Pinellas. |
| BBB | category crawl | Playwright 200 / curl 403 | 1 with challenge retry | Hours | **Browser harvest** (`bbb-harvest`). |

No source in the Oracle baseline is estimated **above 48 hours** if bulk CSVs + GIS
are used for appraisal. A naive countywide HTML scrape is close to the 48h line;
do not default to it.

## 8. Risks

- **STRAP/PARCELID mix-up** — silent empty pages (highest operational risk).
- **User-Agent 403** on PCPAO and several clerk/tax hosts without a browser UA.
- **Cloudflare** on Sunbiz live search and BBB (curl fails; Playwright/Chromium works).
- **Fragmented permits** — county Accela is not St. Pete / Largo / Clearwater.
- **Appraisal permit table is incomplete** by PCPAO’s own disclaimer.
- **Drupal session / Translate widget** — do not parse `body` innerText.
- **`epermit.myclearwater.com` timed out**; use `aca-prod.accela.com/CLEARWATER`.
- Node on this machine is **v20.19.1**; skills ask for 22+ before ingest. Discovery
  did not need it.
- `gh` GitHub token on this machine is **invalid** — cannot open the
  Counties-trasform-scripts PR until `gh auth login`.

## Public-field contract (pilot)

What we will treat as in-scope public fields for the Pinellas pilot, mapped to
existing transform outputs / bulk columns:

- **Identity:** STRAP (canonical), punctuated parcel number, site address, city, ZIP
- **Owner:** name(s), mailing address
- **Classification:** property use code + description (0110 …)
- **Valuation:** just/market, assessed/SOH cap, county/school/municipal taxable (certified year)
- **Physical:** year built, living/gross SF, stories, structural elements, extra features
- **Sales:** date, price, grantor/grantee, book/page
- **Geometry:** parcel polygon from GIS (reproject from EPSG:2882 → WGS84 for seed)
- **Permits (appraisal-page + county Accela):** number, description, issue date, estimated value
- **Enrichment (FL-wide, not Pinellas-portal):** Sunbiz registrations, BBB profiles

Not in the pilot contract: clerk document images, tax-bill PDF, FEMA certificates,
code-enforcement cases, municipal BTRs.

## Probe evidence

- Script: `oracle-node/scripts/pinellas-discovery-probe.mjs`
- Playwright run: `oracle-node/downloads/pinellas/run-playwright.mjs` (gitignored `downloads/`)
- Print fixture (gitignored): `downloads/pinellas/samples/print-strap-162805389030000430.html`
- Verified SFR: STRAP `162805389030000430` / display `05-28-16-38903-000-0430` /
  3400 Rugby Ct, Palm Harbor (unincorporated), use 0110
