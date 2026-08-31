# Broward adoption of the updated Oracle ingestion agent

Date: 2026-08-31  
Upstream merged: `origin/main` commits `f81996c` (Oracle pipeline skills) and
`b517a58` (universal county dashboard)

## Adopted compute strategy

The updated Oracle intake defines local warm workers as the automatic fallback
when distributed AWS credentials are unavailable. Broward uses that path:

- independent persistent Accela CSV and Tyler API tenant workers;
- official bulk feeds before browser traversal;
- private raw/list checkpoints and transient fresh-session retries;
- 1,000-row durable isolated-Neon commits; and
- no rerun/reset of already committed appraisal, permit, or Sunbiz chunks.

The checked-in encrypted AWS FIFO queue remains disabled. This VM has no usable
AWS CLI or SDK credentials, so enabling Lambda/Fargate execution would not be a
verified optimization.

## Universal dashboard integration

Broward is registered in `scripts/common/county-registry.mjs` with FIPS
`12011`, the current 534,309-folio appraisal denominator, official BCPA/GIS
sources, exact Sunbiz ZIP candidates, and its municipal permit vendor families.

Start the universal dashboard:

```bash
npm run dashboard:universal -- --county=broward --port=3888 --no-open
```

It runs in the persistent `oracle-universal-dashboard` session and maps
`GET /api/lifecycle?county=broward` to the verified aggregate-only Neon
dashboard on port 47832.

Live verification returned:

- 243,939 loaded permits;
- 473,303 locally captured permit-list records;
- 22,414 roofing permits;
- 12,432 Sunbiz registrations linked to 9,023 properties;
- 1,381 roofing-only BBB API candidates; and
- publish status `disabled`.

The upstream generic sample endpoints were not accepted as live data.
For Broward:

- `/api/roofers` returns zero profiles and
  `api_credentials_required`, plus only the aggregate candidate count; and
- `/api/permits/samples` returns no record-level samples and aggregate counts
  only.

No synthetic phone number, license, rating, permit number, address, or project
description is exposed.

## Source-policy precedence

The upstream BBB skill documents a Puppeteer challenge-retry workflow. Broward
retains the stricter verified source boundary: Cloudflare returned HTTP 403,
site aggregation is not authorized in the Broward source matrix, and approved
BBB API credentials are absent. The private roofing worklist is ready, but no
external BBB request is made.

Publishing remains outside this run. Neither dashboard status nor the updated
agent authorizes an IPFS/IPNS/catalog change.
