# Orange County, FL — Source Discovery Findings

Consolidated source-discovery findings for Orange County, produced by the
`county-discovery` stage. Mirrors the Lee County reference
([`lee-county-findings.md`](./lee-county-findings.md)); the machine-readable registry is
[`orange-sources.yaml`](./orange-sources.yaml).

**County:** Orange County, FL. **FIPS 12095.** Seed roll `s3://counties-seeds/orange.csv`
= **490,529 parcels** (geometry rides in-seed: `parcel_polygon`, `longitude`, `latitude`,
`building_polygon`). A transform already exists at
`Counties-trasform-scripts/orange/scripts/` (`data_extractor.js` + owner/structure/layout/
utility mapping) — **reuse it, do not rebuild** — but it currently references
`lakecopropappr.com` (Lake County), so **per-field validation is owed at the validate
stage**.

Scope this run: **discovery only.** No live ingest/deploy, no PR, no push.

> ## ⚠️ CRITICAL — two things that will silently break the whole scrape
>
> 1. **14 → 15-digit PID normalization.** OCPA's PID is **15 digits, digits-only**. The
>    seed's `parcel_id` is **14 digits** (a leading zero was stripped by numeric
>    formatting) and returns `[]` against the API. The scrape **must**
>    zero-pad the seed id to 15, call `GetSearchInfoByParcel` to get the **canonical**
>    `parcelId`, then fan out `PRC/*` with that canonical id. Skip this and **all 490,529
>    parcels 404**.
> 2. **`-1` value masking on uncertified tax years.** For the current *uncertified* tax
>    year OCPA returns value fields as **`-1`** (`isCertified:false`). Use the **certified
>    year** (`GetPRCGeneralInfo.prcTaxYear`, currently **2025**) for real dollar values.

## 1. Appraiser portal (property)

- **Source:** Orange County Property Appraiser (OCPA).
- **Public SPA:** `https://ocpaweb.ocpafl.org/parcelsearch` (Angular / ArcGIS).
- **REAL data API** (plain GET, JSON, no auth, no cookie, no anti-bot, not geo-blocked):
  base `https://ocpa-mainsite-afd-standard.azurefd.net/api/`. This is the
  Palm-Beach-style plain-HTTP path — **no Browser Flow required.**
- **Resolve / enumerate:** `api/QuickSearch/GetSearchInfoByParcel?pid=<15-digit>`
  (also `ByAddress` / `ByOwnerName` / `ByInstrumentNumber` / `ByBookPage`).
- **Full record = fan-out of `api/PRC/*` by canonical PID:**
  - `GetPRCGeneralInfo` — owner / address / mailing / `dorCode` + `dorDescription` /
    `prcTaxYear`
  - `GetPRCPropertyValues` — land / building / market / assessed / exemptions / SOH cap
  - `GetPRCPropFeatBldg` (+`Subareas`) — beds / baths / floors / grossArea / livingArea /
    extWall / dateBuilt / estNewCost / bldgValue
  - `GetPRCPropFeatLand` (+`Area`) — zoning / landQty / unitPrice
  - `GetPRCPropFeatLegal` — propertyDescription
  - `GetPRCPropFeatXfob` — extra features
  - `GetPRCSales` (+`Subs`) — saleDate / saleAmt / instrNum / book / page / seller /
    buyer / deedDesc
  - `GetPRCCertifiedTaxes` / `GetPRCNonAdValorem` / `GetPRCTotalTaxes`
  - `GetPRCPortValue` / `IncomeModel` / `Stats`
- **Freshness:** `api/Parcel/GetSystemRefreshDate` (returned **07/01/2026**).
- **Images:** `https://ocpaimages.ocpafl.org/api/Image/GetPIDImage` and `/GetPIDSketch`.
- **Decoys — do NOT use:** `ocpaweb.ocpafl.org/api` returns 406 (decoy);
  `ocpaservices …/ParcelInfoPrinterFriendly.aspx` 404'd (do not rely on it).
- **Access:** plain HTTP JSON, no login / CAPTCHA / Cloudflare. Not geo-blocked.
- **Refresh:** on-demand per-parcel, seed-driven; Structured Archive to S3.

## 2. Parcel identifier

- **OCPA "PID"** = **15 digits, digits-only** (e.g. `272003843803710`). Format is
  Section-Township-Range-Subdivision-Block-Lot.
- **⚠️ Seed mismatch:** the seed's `parcel_id` is **14 digits** (leading zero stripped by
  numeric formatting) and returns `[]` against the API. **Normalization required:**
  zero-pad seed id to 15 → call `GetSearchInfoByParcel` → use the returned canonical
  `parcelId` for all `PRC/*` fan-out.
- **Verified examples:**
  - seed `12027000000001` → pad `012027000000001` → canonical `272001000000001`
    (ORANGE COUNTY BCC, OAK LN, `totalCount:1`)
  - seed `12228145000820` → pad `012228145000820` → canonical `282201145000820`
    (KIM YOUNG AI, 6818 THOUSAND OAKS RD)

## 3. Permit portal(s)

Permits are **municipal and fragmented** across ~13 systems / ~9 vendors. Per the Lee/PB
pattern, permits are **on-demand, NOT bulk** — build adapters via `county-permit-adapter`,
one adapter per vendor.

| Jurisdiction | Portal / vendor | URL | Status |
| --- | --- | --- | --- |
| Orange County (unincorporated) | **OC FastTrack** (custom county ASP.NET) | `https://fasttrack.ocfl.net/OnlineServices/PermitsAllTypes.aspx` | plain 200; search by permit# / address / parcel. **Biggest volume — build first** |
| City of Orlando | **RelayView** | `https://cityoforlandofl-permits.myrelayview.com/` | 403 bot-challenge (needs browser session, **not** geo). **2nd biggest — build first** |
| Apopka | OpenGov | `apopkafl.portal.opengov.com` | discovered |
| Winter Park | **Tyler EnerGov / Civic Access** | `selfservice.cityofwinterpark.org/energov_prod` | discovered |
| Maitland | **Tyler EnerGov** | `maitlandfl-energovpub.tylerhost.net` | discovered |
| Ocoee | SagesGov / Clear Village | `permits.ocoee.org` | discovered |
| Winter Garden | BS&A Online | `bsaonline.com` (uid=3123) | discovered |
| Windermere + Oakland | PDCS LLC | `pdcsllc.com` | discovered |
| Belle Isle | UES / teamues | — | needs-review |
| Bay Lake + Lake Buena Vista | **Accela ACA** (CFTOD) | `ca.rcid.org/CitizenAccess` | discovered (bot-challenge) |
| Eatonville | no online portal | — | none |
| Edgewood | rides Orange County | — | needs-review |

- **Highest-leverage reusable adapter: Tyler EnerGov Civic Access** — 2 Orange munis and
  the most common vendor statewide. Build this adapter to maximize downstream reuse.
- Detail contents (contractors, inspections, fees) and session/bootstrap needs are captured
  per-vendor at the `county-permit-adapter` stage.

## 4. Bulk data sources

- **Seed parcel roll:** `s3://counties-seeds/orange.csv` (**490,529** parcels).
- **GIS parcels (geometry), ArcGIS:**
  `https://ocgis4.ocfl.net/arcgis/rest/services/Public_Base/MapServer/32/query`
  where `PARCEL='<15-digit pid>'` (field `PARCEL`, `returnGeometry=true`).
- Geometry **also rides in the seed CSV** (`parcel_polygon` / `longitude` / `latitude` /
  `building_polygon`) — the transform reads it, same as Palm Beach.

## 5. Usage-type vocabulary

- Standard **4-digit FL DOR use codes**, inline in every record (`dorCode` /
  `dorDescription`, `bldgDorCode`, `landDorCode`).
- **Enumerable via:** `api/PropertyUseSearch/GetDORDescriptionList?dorGroupID=<n>` and
  `GetSearchInfoByPropertyType?useCode=<dorCode>`.
- **Samples:** `0103` Single Fam Class III · `1802` Office Mid-Rise · `2300`
  Financial/Bank · `2700` Auto Dealership · `8900` Municipal · `5001` Ag.
- Commercial/industrial codes (drives **permit-harvest eligibility**) map per the Lee
  mapping.

## 6. Additional data sources

- **Sunbiz** (Florida statewide corporate registrations) — reuse the Lee/PB toolchain
  (`sunbiz-corporate-ingest`). The only new per-county input is the **Orange ZIP-prefix
  list** (collect Orange County ZIPs).
- **BBB** — national / complete set, shared harvester (`bbb-harvest`). No per-county work.
- **Recorder / official records** — Orange County Comptroller (noted as available).

## 7. Source feasibility

- **Appraisal:** plain-HTTP JSON API, no browser, no anti-bot → **full bulk collection
  feasible from AWS us-east-1** (like Palm Beach). Estimate ~490,529 parcels ×
  (1 resolve + ~10 PRC calls). **TODO — benchmark:** measure p50/p95 latency and safe
  concurrency at a pilot before committing to a full-download plan.
- **Permits:** on-demand only (fragmented vendors); no bulk artifact. Adapter throughput
  measured per vendor at the `county-permit-adapter` stage.
- **Sunbiz / BBB:** bulk, already-solved toolchain.

## 8. Risks

- **`-1` value masking** on uncertified tax years → must read the **certified year**
  (`prcTaxYear`) for real dollar values.
- **Seed 14 → 15-digit PID normalization** → must **pad + resolve** or every parcel 404s.
- **Orlando (RelayView) + `rcid.org` (Accela) permit portals** issue **bot challenges** →
  need a browser/residential-proxy session. These are **bot challenges, not geo-blocks.**
- **Non-US local egress** (Serbia) is **irrelevant for production** — scraping runs from
  AWS us-east-1; the OCPA appraiser API is confirmed not geo-blocked.

## Named gaps / owed work (feeds later stages)

- **Transform per-field validation owed** — `Counties-trasform-scripts/orange/scripts/`
  still references `lakecopropappr.com`; prove field-by-field extraction vs OCPA at the
  `validate-county-transform` stage.
- **Permit portals needing a browser session** — Orlando (RelayView) and Bay Lake / Lake
  Buena Vista (Accela `rcid.org`); Belle Isle and Edgewood are `needs-review`.
- **Appraisal throughput benchmark TODO** — p50/p95 + safe concurrency at pilot.
- **Orange ZIP-prefix list** — the one new Sunbiz input to collect.

## Downstream

All sources land in S3 (Structured Archive) and reconcile into the Neon query DB
(`@elephant-xyz/query-db`). Completeness is validated per source via
`validate-county-transform`, `monitoring-county-ingestion`, and `query-db-loading-matching`.

**Out of scope this milestone:** IPFS publishing, on-chain/blockchain-style indexing, MCP
exposure, NEO rewiring, Elephant.xyz UI (separate story, Asana `1215644141347714`).
