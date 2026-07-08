# Miami-Dade County, FL — Source Discovery & Ingestion Findings

Consolidated findings for Miami-Dade County, the second full-county Oracle onboarding
after Lee (reference) and alongside Palm Beach / Orange. Machine-readable registry:
[`miami-dade-sources.yaml`](./miami-dade-sources.yaml).

**Status (2026-07-08):** Appraisal **loaded + reconciled** in Neon (**933,087** distinct
folios / `source_system='miami_dade_appraiser'`). Query-table published to IPFS with
`property_cid` populated (**933,087** rows with CIDs). Bundled `elephant` MCP serves
`miami-dade` via `PROPERTY_QUERY_TABLE_MAP` + `ORACLE_OPEN_DATA_IPNS_MAP`. Sunbiz/BBB
enrichment load in progress on separate track. Permits **not** county-wide bulk — municipal
fragmentation (discovery TBD).

## 1. Appraiser portal (property) — SOLVED

- **Source:** Miami-Dade County Property Appraiser (MDCPA).
- **API:** `https://apps.miamidadepa.gov/PApublicServiceProxy/PaServicesProxy.ashx`
  (plain HTTP POST proxy; browser flow also exists for legacy HTML paths).
- **Transform:** `Counties-trasform-scripts/miami dade/scripts/` (`data_extractor.js` +
  mapping modules). Folder name has a **space** — scripts-manager matches variants
  (`miami dade`, `miami-dade`, `miami_dade`).
- **Seed:** `s3://counties-seeds/miami-dade.csv` (~933,532 rows; reconcile at load by folio).
- **TARGET PARCEL COUNT (AC test): `933,087`** distinct folios in Neon (2026-07-07 reconcile).
- **Parcel id / folio:** MDCPA **folio** (e.g. `0101000000020`). Use **`request_identifier`**
  (folio) as the 1:1 cardinality key — never dedupe on normalized `parcel_identifier` alone.
- **Scripts:**
  - `scripts/enqueue-miami-dade-appraisal-property-first-from-seed.mjs` — pilot / bounded enqueue
  - `scripts/send-miami-dade-seed-feeder.mjs` — full-run seed feeder (requires fixed `--job-id`)
- **Prepare queue:** `elephant-oracle-node-prepare-queue-miami-dade` (derived from seed CSV
  `county` column `"Miami Dade"` → slug `miami-dade`).

## 2. Permit portal(s) — NOT SOLVED (municipal fragmentation)

Miami-Dade permits are **municipal**, not a single county Accela portal like Lee. Expect
the Palm Beach pattern (~30+ jurisdictions) — build adapters via `county-permit-adapter`
after cataloguing portals. Property-first pipeline can run **appraisal-only** via
`PROPERTY_FIRST_PERMIT_ELIGIBLE_USAGE_TYPES=__NONE__` on the transform worker.

## 3. Sunbiz (business registration) — reuse Lee toolchain

- Florida Sunbiz is **statewide** — same `cordata.zip` bulk download as Lee.
- **Miami-Dade ZIP-prefix list:** `scripts/miami-dade-county-zips.json` (principal/mailing
  matching for cordata extract).
- **Scripts:**
  - `scripts/enqueue-miami-dade-sunbiz-extract-all.mjs` — ZIP-sliced cordata extract to S3
  - `scripts/transform-miami-dade-sunbiz-per-cordata.mjs` — per-cordata lexicon transform + upload
  - `scripts/build-miami-dade-sunbiz-manifest.mjs` — manifest for bulk loader
- **Loader:** `appraisal-reload` Fargate task with `SKIP_CLEAR=1` for enrichment-only loads.

## 4. BBB (business reputation) — reuse Lee toolchain

- Same national BBB category harvest as Lee; filter to Miami-Dade area profiles.
- Loader overrides example: `.loader-runs/miami-dade-bbb-load-overrides.json`.

## 5. Index + publish (query table + open data)

| Artifact | IPNS label | Notes |
|----------|------------|-------|
| Query table Parquet | `oracle-query-table-miami-dade` | donphan SQL via `queryProperties` |
| Open-data consolidation | `oracle-open-data-miami-dade` | `getOracleProperty` / NEO shards |

**County slug:** always **`miami-dade`** (hyphen) in MCP maps, export `--county`, and donphan
`county` arg. DB `source_system` is `miami_dade_appraiser` (underscore — automatic from
`appraisalSourceForCounty`).

**Query-table publish gotchas (learned):**

- Export **must** pass `--manifest` from property-consolidation export or every
  `property_cid` is NULL → NEO grid shows N/A.
- Re-publish query-table after open-data manifest exists; validate gate:
  `rowCount == distinct folio in Neon` (933,087).
- Prefer `https://ipfs.filebase.io/ipns/<key>` in MCP map (Filebase gateway); `dweb.link` can
  lag after publish.

**Column coverage gaps (query table, 2026-07-07):**

| Column | Coverage | Notes |
|--------|----------|-------|
| `lot_area_sqft` | ~61% | Partial |
| `livable_floor_area` | **0%** | Loader/transform gap — building sqft not mapped to export columns |
| `exterior_wall_material` | sparse | Do not assume material filters work |
| `hoa_flag` | NULL everywhere | Reserved placeholder (all counties) |

Report these honestly in donphan answers; do not invent acreage/material coverage.

## 6. MCP / donphan smoke tests

After publish + team-kit `mcp.json` maps:

```text
getOracleDatasetInfo { county: "miami-dade" }  → propertyCount ~933087
queryProperties { county: "miami-dade", sql: "SELECT count(*) FROM properties" }  → 933087
queryProperties { county: "miami-dade", sql: "SELECT count(*) FROM properties WHERE address_zip = '33101'" }
getOracleProperty { parcelIdentifier: "0101000000020", county: "miami-dade" }
```

Bundled stdio MCP reads `mcp.json` in the soofi-xyz plugin — **not** Vercel env vars.

## Downstream

All sources land in S3 (Structured Archive) and reconcile into Neon (`elephant-query-db`).
Completeness: `validate-county-transform`, `monitoring-county-ingestion`,
`query-db-loading-matching`, then `county-query-table-publish` + `county-open-data-publish`.
