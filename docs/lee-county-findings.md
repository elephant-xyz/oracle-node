# Lee County, FL — Source Discovery Findings

Consolidated source-discovery findings for Lee County, the reference implementation.
Lee was built incrementally before the `county-discovery` findings-doc convention existed,
so its source metadata was scattered across per-topic docs, browser flows, and scripts.
This document is the single index; the machine-readable registry is
[`lee-county-sources.yaml`](./lee-county-sources.yaml).

All four Oracle source categories are **identified and implemented** for Lee County
(AC: property, permits, Sunbiz, BBB). What this milestone adds is the consolidated,
stored registry of their URLs, access patterns, and refresh methods.

## 1. Appraiser portal (property)

- **Source:** Lee County Property Appraiser (LeePA).
- **URLs:** `https://leepa.org/Display/DisplayParcel.aspx` (parcel detail);
  `http://fieldcards.leepa.org/CurrentCostCard/Folio/<folio>` (cost card).
- **Access:** public browser scrape via elephant-cli Browser Flow v2 + Transform v2;
  no login/CAPTCHA. Flows: `browser-flows/LeeCurated.json`, `browser-flows/LeeCostCard.json`;
  transforms in `transform/lee/`.
- **Refresh:** on-demand per-parcel, seed-driven; Structured Archive to S3.

## 2. Parcel identifier

- **STRAP** (Section-Township-Range-Area-Block-Parcel), 17-digit numeric. Punctuation is
  stripped to form the portal search id (e.g. `02442401000090000`). "Folio" is used by the
  appraiser cost-card endpoint.

## 3. Permit portal

- **Source:** Lee County Accela Citizen Access (ACA), vendor **Accela**.
- **URLs:** `https://aca-prod.accela.com/LEECO/Cap/CapHome.aspx` (General Search);
  `.../Cap/CapDetail.aspx` (detail). Public, no login/CAPTCHA.
- **Access:** Browser Flow v2 + Transform v2. Two strategies — opened-date-window list harvest
  (split windows at >= 100 results) and parcel-by-parcel search. Worker
  `workflow/lambdas/permit-harvest-worker/lee-accela.mjs`; stack `elephant-permit-harvest`.
- **Refresh:** batch backfill 1990 -> present + incremental windows; on-demand parcel search
  also available. **Keep the batched source** — the Neo site sorts by permit counts.
- Detail: [`lee-county-permit-findings.md`](./lee-county-permit-findings.md).

## 4. Bulk data sources — Sunbiz (business registration)

- **Source:** Florida Sunbiz corporate registrations (FL Dept. of State).
- **URLs:** `https://dos.fl.gov/sunbiz/other-services/data-downloads/` (bulk index);
  Data Access Portal `doc/quarterly/cor`; `https://search.sunbiz.org/...` (live search, secondary).
- **Access:** quarterly bulk download (`cordata.zip` ~1.74 GB) via headless Chromium (Cloudflare
  challenge blocks plain curl). Fixed-width ASCII, 1440-char records; matched by Lee-area ZIP
  prefixes across principal/mailing/registered-agent/officer addresses. **Deflate64 gotcha:**
  expand `cordata.zip` locally with system `unzip`, upload fixed-width `.txt` to S3.
- **Refresh:** quarterly bulk extract + lexicon transform.
- Detail: [`sunbiz-bulk-download-runbook.md`](./sunbiz-bulk-download-runbook.md),
  [`sunbiz-lexicon-transform-findings.md`](./sunbiz-lexicon-transform-findings.md).

## 5. Additional source — BBB (business reputation)

- **Source:** Better Business Bureau contractor reputation.
- **URL:** `https://www.bbb.org/us/category/data` (category harvest entry).
- **Access:** Puppeteer (headless Chromium) with a Cloudflare challenge-retry loop;
  crawls category -> profile listings -> profile subpages (customer-reviews, complaints,
  more-info). Output schema `oracle-node.bbb-category-harvest.v1`.
- **Refresh:** on-demand / periodic enrichment. Script `scripts/harvest-bbb-category.mjs`.

## Downstream

All four sources land in S3 (Structured Archive) and reconcile into the Neon query DB
(`@elephant-xyz/query-db`). Completeness is validated per source via
`validate-county-transform`, `monitoring-county-ingestion`, and `query-db-loading-matching`.

**Out of scope (separate story, Asana `1215644141347714`):** IPFS publishing, on-chain/
blockchain-style indexing, MCP exposure, NEO rewiring, Elephant.xyz UI.
