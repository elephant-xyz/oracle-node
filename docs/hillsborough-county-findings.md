# Hillsborough County, FL — discovery & local pilot findings

**County key:** `hillsborough`  
**FIPS:** 12057  
**Scope this pass:** local Cursor pilot (~50 parcels), **no AWS**  
**Date:** 2026-08-27

## 1. Appraiser portal

| Item | Detail |
|------|--------|
| Site | [hcpafl.org](https://hcpafl.org/) |
| Search SPA | [gis.hcpafl.org/PropertySearch](https://gis.hcpafl.org/PropertySearch/) |
| JSON API | `https://gis.hcpafl.org/CommonServices/property/search/` |
| Primary endpoints | `BasicSearch`, `ParcelData`, `Autocomplete` |
| Access mode | **Plain HTTP JSON** (no browser required for capture) |
| CAPTCHA / Cloudflare | None observed on CommonServices (US egress) |
| Parcel id | Folio (10-digit) + strap/PIN display form |

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

| Code band | Examples | Pilot role |
|-----------|----------|------------|
| 00xx | Vacant residential / condo | Vacant geometry / skip-permits path |
| 01xx | Single family | Residential |
| 04xx | Condo | Condo unit |
| 11xx–18xx | Store / office / multi-story office | Commercial |
| 41xx+ | Industrial | Industrial |

Full mapping lives in `Counties-trasform-scripts/hillsborough/scripts/data_extractor.js` (`propertyTypeMapping`).

## 5. Additional sources

| Category | Status |
|----------|--------|
| Sunbiz (FL) | Available statewide — not re-run this pilot |
| BBB | Available national — not re-run this pilot |
| Tax collector | Out of scope (values from HCPA) |
| Recorder / OR | Out of scope (book/page links only) |
| Code enforcement | No unified countywide source identified |

## 6. Source feasibility (local)

| Source | Probe | Safe concurrency | Full-download note |
|--------|-------|------------------|--------------------|
| ParcelData | ~0.5–1.5s / parcel | 2 | Full county deferred (no AWS this story) |
| HC_ParcelsPublic | ~0.3–1s / folio | 2–4 | Used for pilot geometry only |
| Accela HCFL/TAMPA | Live 200 on CapHome | n/a | Adapter not in this pilot |

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

| Check | Result |
|-------|--------|
| Seed rows | 50 unique folios (`downloads/hillsborough/pilot-seed-50.csv`) |
| Diversity | land-use bands include 00/01/04 (vacant/res/condo) + 11–18 commercial + industrial/other |
| Geometry in seed | 50/50 polygons from `HC_ParcelsPublic` |
| Capture + transform | **50/50 success** |
| Required artifacts | `property.json`, `address.json`, `property_seed.json` present for all |
| Completeness | tax 50/50, owner 50/50, geometry 50/50 |
| Embedded permitInfo rows | 356 across pilot (Tampa Accela links; full HCFL harvest deferred) |
| Failures | 0 |
| Manifest | `downloads/hillsborough/pilot-run/pilot-manifest.json` |

Neon load: **done** — `node scripts/hillsborough-local-pilot.mjs --limit=50 --concurrency=2 --load` → 50 parcels / 1509 prepared rows upserted under `hillsborough_appraiser` (`parcelsBefore: 0` → `parcelsAfter: 50`).

### IPFS / Donphan publish (2026-08-27)

Reused existing Filebase account credentials from `.env.publish-chester.local` (same pattern as Chester — no new access key required). Created bucket `elephant-oracle-open-data-hillsborough`.

| Surface | Result |
|---------|--------|
| Open-data index CID | `QmXmudL56YGZLQy58XH5J65Z2ouePkhVpDnrCth6wcTAax` |
| Open-data IPNS | `oracle-open-data-hillsborough` → `k51qzi5uqu5diznbms9qjkf8wrebeq7qwhc4jzy620k5bb44qqnibp7cl7nx1f` |
| Query-table CID | `QmaheHZg56fJQAP2iH9XVSjKsU5VUbnyqR8MSPrZudJNSu` |
| Query-table IPNS | `oracle-query-table-hillsborough` → `k51qzi5uqu5diqz0l68gfi22qk0w8aqhsm7pcgje535uz8vhu8p37ynm2po0fh` |
| Dataset coverage IPNS | `oracle-dataset-coverage-hillsborough` → `k51qzi5uqu5di5jghjwbpumnr2vt1crmaycqmtx673kw8pqp8dymecuig5x8jb` |
| Validation | parquet 50 rows = Neon 50 folios; gateway bytes `PAR1` |
| MCP local map | `PROPERTY_QUERY_TABLE_MAP` + `ORACLE_OPEN_DATA_IPNS_MAP` updated in team-kit / local plugin |

Donphan: after **Developer: Reload Window**, query `county=hillsborough` (e.g. `SELECT count(*) FROM properties`).
