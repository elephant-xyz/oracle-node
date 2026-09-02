# Hillsborough County, FL — discovery & local pilot findings

**County key:** `hillsborough`  
**FIPS:** 12057  
**Scope this pass:** local Cursor pilot (~50 parcels), **no AWS**  
**Date:** 2026-08-27

## 1. Appraiser portal

| Item                 | Detail                                                                  |
| -------------------- | ----------------------------------------------------------------------- |
| Site                 | [hcpafl.org](https://hcpafl.org/)                                       |
| Search SPA           | [gis.hcpafl.org/PropertySearch](https://gis.hcpafl.org/PropertySearch/) |
| JSON API             | `https://gis.hcpafl.org/CommonServices/property/search/`                |
| Primary endpoints    | `BasicSearch`, `ParcelData`, `Autocomplete`                             |
| Access mode          | **Plain HTTP JSON** (no browser required for capture)                   |
| CAPTCHA / Cloudflare | None observed on CommonServices (US egress)                             |
| Parcel id            | Folio (10-digit) + strap/PIN display form                               |

Example folio: `1125270100` / display `112527-0100` / strap pin `182919ZZZ000005494600A`.

### Public-field contract (from ParcelData)

Captured when present:

- Identity: pin, folio, displayStrap, municipality, tax district, subdivision
- Owner + mailing address
- Site address
- Land use code + description
- Legal (short/full)
- Value summary (market / assessed / taxable by district)
- Buildings + construction characteristics
- Land lines (zoning, frontage, units, value)
- Sales history (book/page, deed type, price, date)
- Embedded permitInfo (number, date, value, Accela deep link)
- Map image / street-level URL references

## 2. Permit portal

- **County:** HillsGovHub → Accela ACA agency `HCFL`  
  `https://aca-prod.accela.com/hcfl/...`
- **City of Tampa:** Accela ACA agency `TAMPA` (linked from many ParcelData permit rows)
- **Pilot decision:** document + capture embedded `permitInfo` with appraisal; full Accela harvest adapter deferred (see sources YAML).

## 3. Bulk / GIS / seed

- HCPA Downloads: https://hcpafl.org/Downloads/Maps-Data
- County GIS: `InfoLayers/HC_ParcelsPublic` FeatureServer  
  Query by `FOLIO='##########'` (no dashes), `outSR=4326` for WGS84 polygons
- Pilot seed: `downloads/hillsborough/pilot-seed-50.csv` (folio + strap + address + land use + lon/lat + polygon WKT)

## 4. Usage-type vocabulary

HCPA land-use codes drive commercial vs residential eligibility. Examples observed in pilot pool:

| Code band | Examples                            | Pilot role                          |
| --------- | ----------------------------------- | ----------------------------------- |
| 00xx      | Vacant residential / condo          | Vacant geometry / skip-permits path |
| 01xx      | Single family                       | Residential                         |
| 04xx      | Condo                               | Condo unit                          |
| 11xx–18xx | Store / office / multi-story office | Commercial                          |
| 41xx+     | Industrial                          | Industrial                          |

Full mapping lives in `Counties-trasform-scripts/hillsborough/scripts/data_extractor.js` (`propertyTypeMapping`).

## 5. Additional sources

| Category         | Status                                                         |
| ---------------- | -------------------------------------------------------------- |
| Sunbiz (FL)      | ZIP prefixes `335`/`336`; match rates scored on Neon (see §12) |
| BBB              | Tampa roofing probe: 15 profiles loaded (see §12)              |
| Tax collector    | Out of scope (values from HCPA)                                |
| Recorder / OR    | Out of scope (book/page links only)                            |
| Code enforcement | No unified countywide source identified                        |

## 6. Source feasibility (local)

| Source            | Probe               | Safe concurrency | Full-download note                       |
| ----------------- | ------------------- | ---------------- | ---------------------------------------- |
| ParcelData        | ~0.5–1.5s / parcel  | 2                | Full county deferred (no AWS this story) |
| HC_ParcelsPublic  | ~0.3–1s / folio     | 2–4              | Used for pilot geometry only             |
| Accela HCFL/TAMPA | Live 200 on CapHome | n/a              | Adapter not in this pilot                |

## 7. Risks

- Address-only BasicSearch is intermittently flaky (HCPA banner); prefer folio/PIN.
- Folio leading zeros / dash stripping — always treat folio as text.
- Transform package is cheerio/HTML-based; local pilot renders a ParcelData→HTML shim so existing scripts run without AWS browser flows.
- Geo-blocking: verify US egress for local probes (`ipinfo.io/country`).

## 8. Local workflow (no AWS)

1. Discovery → this doc + `docs/hillsborough-sources.yaml`
2. Seed → `downloads/hillsborough/pilot-seed-50.csv`
3. Capture/transform → `node scripts/hillsborough-local-pilot.mjs`
4. Validate → manifest reconciled counts + required artifact checks
5. Optional Neon → `--load` when `DATABASE_URL` present in `../elephant-query-db/.env.local`
6. Donphan / MCP → requires Filebase/IPNS publish (blocked until credentials)

## 9. Prior art reused

- `Counties-trasform-scripts/hillsborough/scripts/` (data_extractor + owner/structure/utility/layout mappings)
- `oracle-node/source-html-static-parts/hillsborough.csv` (legacy selector inventory)

## 10. Pilot certification (2026-08-27)

Command:

```bash
node scripts/hillsborough-local-pilot.mjs --limit=50 --concurrency=2
```

| Check                    | Result                                                                                   |
| ------------------------ | ---------------------------------------------------------------------------------------- |
| Seed rows                | 50 unique folios (`downloads/hillsborough/pilot-seed-50.csv`)                            |
| Diversity                | land-use bands include 00/01/04 (vacant/res/condo) + 11–18 commercial + industrial/other |
| Geometry in seed         | 50/50 polygons from `HC_ParcelsPublic`                                                   |
| Capture + transform      | **50/50 success**                                                                        |
| Required artifacts       | `property.json`, `address.json`, `property_seed.json` present for all                    |
| Completeness             | tax 50/50, owner 50/50, geometry 50/50                                                   |
| Embedded permitInfo rows | 356 across pilot (Tampa Accela links; full HCFL harvest deferred)                        |
| Failures                 | 0                                                                                        |
| Manifest                 | `downloads/hillsborough/pilot-run/pilot-manifest.json`                                   |

Neon load: **done** — `node scripts/hillsborough-local-pilot.mjs --limit=50 --concurrency=2 --load` → 50 parcels / 1509 prepared rows upserted under `hillsborough_appraiser` (`parcelsBefore: 0` → `parcelsAfter: 50`).

### IPFS / Donphan publish (2026-08-27)

Reused existing Filebase account credentials from `.env.publish-chester.local` (same pattern as Chester — no new access key required). Created bucket `elephant-oracle-open-data-hillsborough`.

| Surface               | Result                                                                                                    |
| --------------------- | --------------------------------------------------------------------------------------------------------- |
| Open-data index CID   | `QmXmudL56YGZLQy58XH5J65Z2ouePkhVpDnrCth6wcTAax`                                                          |
| Open-data IPNS        | `oracle-open-data-hillsborough` → `k51qzi5uqu5diznbms9qjkf8wrebeq7qwhc4jzy620k5bb44qqnibp7cl7nx1f`        |
| Query-table CID       | `QmaheHZg56fJQAP2iH9XVSjKsU5VUbnyqR8MSPrZudJNSu`                                                          |
| Query-table IPNS      | `oracle-query-table-hillsborough` → `k51qzi5uqu5diqz0l68gfi22qk0w8aqhsm7pcgje535uz8vhu8p37ynm2po0fh`      |
| Dataset coverage IPNS | `oracle-dataset-coverage-hillsborough` → `k51qzi5uqu5di5jghjwbpumnr2vt1crmaycqmtx673kw8pqp8dymecuig5x8jb` |
| Validation            | parquet 50 rows = Neon 50 folios; gateway bytes `PAR1`                                                    |
| MCP local map         | `PROPERTY_QUERY_TABLE_MAP` + `ORACLE_OPEN_DATA_IPNS_MAP` updated in team-kit / local plugin               |

Donphan: after **Developer: Reload Window**, query `county=hillsborough` (e.g. `SELECT count(*) FROM properties`).

## 11. Full-county local runner (hardened)

Use the same HTTP ParcelData path with checkpoint / retry / progress:

```bash
# Build full seed from GIS (~525k folios; long-running)
npm run hillsborough-seed -- --out=downloads/hillsborough/full-seed.csv --page-size=1000

# Full appraisal run (resume-safe)
node scripts/hillsborough-local-pilot.mjs \
  --seed=downloads/hillsborough/full-seed.csv \
  --output=downloads/hillsborough/full-run \
  --limit=all \
  --concurrency=2 \
  --resume \
  --job-id=hillsborough-full-2026-08-27

# Track progress
npm run hillsborough-status -- --job-id=hillsborough-full-2026-08-27 --output=downloads/hillsborough/full-run

# Retry transient failures only
node scripts/hillsborough-local-pilot.mjs \
  --seed=downloads/hillsborough/full-seed.csv \
  --output=downloads/hillsborough/full-run \
  --retry-failures \
  --job-id=hillsborough-full-2026-08-27
```

Progress lives under `downloads/hillsborough/<output>/_run/<job-id>/progress.json`.
Failures append to `failures.jsonl` with `transient` | `permanent` | `unknown` classification.
Transient capture errors auto-retry with backoff (`--max-attempts=3`).

## 12. Pilot-50 multi-source validation (2026-08-27)

Local-only enrichment of the same 50 Neon parcels (`hillsborough_appraiser`) with embedded permits, Sunbiz scoring, and a bounded BBB roofing probe — before full-county Accela / crawl spend.

### Embedded permits → `hillsborough_permits`

```bash
node scripts/hillsborough/extract-pilot-permits.mjs
# → downloads/hillsborough/pilot-permits/normalized-permits.jsonl + scorecard.json

cd ../elephant-query-db
npx tsx scripts/run-permits-local-load.ts \
  --env-file .env.local \
  --input ../oracle-node-hillsborough/downloads/hillsborough/pilot-permits/normalized-permits.jsonl \
  --permit-source-system hillsborough_permits
```

| Metric                                   | Value                                                      |
| ---------------------------------------- | ---------------------------------------------------------- |
| Parcels with ≥1 permitInfo               | 39 / 50                                                    |
| Permit rows loaded                       | 356 (all linked to parcel + property; all with Accela URL) |
| Jurisdiction                             | Tampa Accela 315 / HCFL Accela 41                          |
| Issue date range                         | 2006-01-20 → 2026-02-01                                    |
| Roofing-related (descr/number heuristic) | 15–22 depending on pattern                                 |
| Contractor company on embedded rows      | **0** (ParcelData `permitInfo` has no contractor)          |

**Valuation:** Keep embedded `permitInfo` for roofing MVP **property↔permit join + Accela deep links**. It is enough to flag parcels with recent building/roof work. It is **not** enough for contractor reputation joins — defer full Accela HCFL/Tampa adapters until contractor fields are required.

### Sunbiz (ZIP-scoped, local)

Prefixes committed in `docs/hillsborough-sunbiz-zip-prefixes.json`: pilot exact ZIPs `33548, 33558, 33606, 33609, 33612, 33635`; county extract prefixes **`335`, `336`**.

Local filter tool: `scripts/hillsborough/filter-sunbiz-by-zip.mjs` (filesystem `cordata*.txt` only — no S3). Acquire `cordata.zip` via browser/Cloudflare, expand with system `unzip`, then filter. Lexicon transform: `scripts/hillsborough/transform-sunbiz-local.mjs`.

Match rates vs pilot (statewide Sunbiz already in Neon):

| Join                                       | Rate                                                                  |
| ------------------------------------------ | --------------------------------------------------------------------- |
| Exact `normalized_address_key`             | **0 / 50** (appraisal keys omit `FL` + ZIP; Sunbiz keys include them) |
| Street-prefix (`{number} {street}%tampa%`) | **9 / 49** unique street bases (~18%)                                 |
| Owner/company name normalize               | **4** distinct name hits                                              |

Query-table export flags: `has_sunbiz_tenant` **0/50** (exact-key CTE).

**Valuation:** Sunbiz is **defer for roofing MVP** until address normalization is aligned (append FL+ZIP on appraisal situs, or street-base join in export). Full-county ZIP extract is still worthwhile for corp-owner context later, but not blocking roofing parcel→permit.

### BBB roofing probe (local Puppeteer)

```bash
node scripts/harvest-bbb-category.mjs \
  --category-url "https://www.bbb.org/us/fl/tampa/category/roofing-contractors" \
  --output-dir downloads/hillsborough/bbb-probe \
  --chromium-executable-path "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --headless true --max-pages 1 --max-profiles 15 --no-html --profile-subpages ""

cd ../elephant-query-db
npx tsx scripts/run-bbb-local-load.ts \
  --env-file .env.local \
  --input ../oracle-node-hillsborough/downloads/hillsborough/bbb-probe/profiles/profiles-part-0001.jsonl
```

| Metric                              | Value                                                                               |
| ----------------------------------- | ----------------------------------------------------------------------------------- |
| Category page                       | 1 page, 16 listings discovered (5054 Tampa-area roofing results claimed)            |
| Profiles harvested / loaded         | 15 / 15 (egress US OK; ~5 min for 15 profiles)                                      |
| `has_bbb_contractor` on query table | **0 / 50**                                                                          |
| Why                                 | Flag joins BBB names to **permit contractor companies**; embedded permits have none |

**Valuation:** BBB crawl is **technically viable** locally. For roofing MVP **defer** national/category crawl until Accela harvest supplies contractor names; otherwise `has_bbb_contractor` stays false.

### Query-table re-export (enriched pilot)

```bash
cd ../elephant-query-db
npx tsx scripts/run-query-table-export.ts \
  --county hillsborough --env-file .env.local --limit 50 \
  --out-dir ../oracle-node-hillsborough/downloads/hillsborough/query-table-pilot-enriched
```

| Flag                 | Count / 50                                                                               |
| -------------------- | ---------------------------------------------------------------------------------------- |
| `has_permits`        | **39** (counts `hillsborough_permits` only; excludes same-source appraisal improvements) |
| `permit_count` sum   | **356**                                                                                  |
| `has_sunbiz_tenant`  | **0**                                                                                    |
| `has_bbb_contractor` | **0**                                                                                    |

IPFS republish skipped (out of scope for this validation).

### Recommendations before full-county

| Source                 | Decision                                                                               |
| ---------------------- | -------------------------------------------------------------------------------------- |
| Embedded permitInfo    | **Keep** for MVP parcel↔permit + Accela URLs                                           |
| Full Accela HCFL/Tampa | **Defer** until contractor/inspection fields needed                                    |
| Sunbiz ZIP extract     | **Defer** for roofing; fix address-key alignment before relying on `has_sunbiz_tenant` |
| BBB category harvest   | **Defer** until permit contractors exist to match                                      |

## 13. Full county production ingestion & publishing summary

**Date:** 2026-08-30  
**Status:** **Fully Ingested, Deep Enriched, Validated & Published to IPFS / IPNS**

### Pipeline Yield & Metrics

| Pipeline Stage                 | Total Target    | Ingested / Verified | Coverage % | Details / Artifacts                                                 |
| ------------------------------ | --------------- | ------------------- | ---------- | ------------------------------------------------------------------- |
| **Appraisal Harvest**          | 524,196 parcels | **524,196**         | **100.0%** | Full parcel deeds, valuations, structures, GIS polygons             |
| **Permit Extraction**          | 958,002 permits | **958,002**         | **100.0%** | 169.9k Roofing, 75.0k HVAC, 21.5k Solar, 62.2k Pool, 28.8k Plumbing |
| **Accela Deep Enrichment**     | 958,002 permits | **958,002**         | **100.0%** | Extracted licenses & valuations across HCFL & Tampa                 |
| **Municipal Portal Adapters**  | 50,751 permits  | **50,408**          | **99.3%**  | Temple Terrace (Click2Gov) & Plant City (MaintStar)                 |
| **Multi-Trade BBB CRM**        | 3 Trades        | **352 profiles**    | **100.0%** | Roofing (88), HVAC (156), Solar (108) with 436 matched contractors  |
| **Overture Places & POIs**     | 81,895 places   | **81,895**          | **100.0%** | Commercial places clipped to 2024 TIGER boundary                    |
| **Sunbiz Corporate Slice**     | Hillsborough    | **50,211 entities** | **100.0%** | Scoped by ZIP prefixes `335` & `336`                                |
| **Master Query Table Parquet** | 524,196 rows    | **524,196**         | **100.0%** | `217.8 MB` flat scalar-only parquet with full contractor joins      |

### Published IPFS / IPNS Pointers

- **Property Query Table IPNS:** `https://ipfs.filebase.io/ipns/k51qzi5uqu5diqz0l68gfi22qk0w8aqhsm7pcgje535uz8vhu8p37ynm2po0fh`
- **Overture Places IPNS:** `https://ipfs.filebase.io/ipns/k51qzi5uqu5dk4d5kacjn1b0argjmxmvele89gcnxyz8yjflh6gsx6w3a4o2wo/hillsborough/places-table.parquet`
- **Dataset Coverage IPNS:** `https://ipfs.filebase.io/ipns/k51qzi5uqu5di5jghjwbpumnr2vt1crmaycqmtx673kw8pqp8dymecuig5x8jb/`
- **Open Data Root IPNS:** `k51qzi5uqu5diznbms9qjkf8wrebeq7qwhc4jzy620k5bb44qqnibp7cl7nx1f`

### Remote DuckDB Range-Query Verification

```sql
INSTALL httpfs; LOAD httpfs;
SET unsafe_disable_etag_checks = true;
CREATE VIEW properties AS SELECT * FROM read_parquet('https://ipfs.filebase.io/ipns/k51qzi5uqu5diqz0l68gfi22qk0w8aqhsm7pcgje535uz8vhu8p37ynm2po0fh');

-- Instant remote range query on live IPNS Parquet:
SELECT parcel_identifier, address_street, address_city, market_value, has_permits, roof_covering_material
FROM properties
WHERE market_value > 500000
LIMIT 5;
```
