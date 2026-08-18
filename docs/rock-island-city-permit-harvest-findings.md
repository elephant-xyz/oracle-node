# City of Rock Island permit pilot and private harvest

**Run date:** 2026-08-14
**Jurisdiction:** City of Rock Island only
**Sources:** anonymous Tyler Civic Access search and official city monthly issued-permit reports
**Storage:** local gitignored private staging only
**Not performed:** AWS, EC2, PostgreSQL/Neon, Filebase, IPFS, MCP, deployment, or publication

## Outcome

The newly authorized pilot returned exactly **25 permit records**:

- one exact public Tyler permit-number lookup; and
- 24 deterministic rows from one official monthly issued-permit report.

The official-report path passed the 48-hour throughput gate. A fresh serialized download
of all **112** reports then completed in **203.614 seconds** (about 3.4 minutes), including
the configured 1.5-second delay between live report requests. The source remained healthy,
so the resumable full City-only harvest continued as authorized.

The completed private artifact contains **24,787 unique permit/date records** from
**25,608 parsed source rows**. The 821 extra rows were repeated within the same PDFs,
principally across legacy report page breaks. They were merged by permit number plus issue
date, while alternate source pages, sections, addresses, tax-map values, and valuations
were retained in `raw` variant arrays instead of silently discarded.

The official indexed history spans January 2017 through April 2026. Parsed permit issue
dates span **2017-01-03 through 2026-04-30**.

## Access and policy reconfirmation

- The anonymous Tyler portal still exposed the City of Rock Island tenant and public
  search without login. One exact route-driven search returned HTTP 200 and the requested
  permit. No CAPTCHA, challenge, authentication, or security control was bypassed.
- `https://cityofrockislandil-energovweb.tylerhost.net/robots.txt` still returned HTTP 404. That is not a bulk-use or republication grant.
- The city permit-report index returned HTTP 200 and explicitly describes each linked PDF
  as the list of all permits issued for that month.
- `https://www.rigov.org/robots.txt` does not disallow `/1276/Permit-Reports` or
  `/DocumentCenter/View/...`; it disallows administrative and site-search routes that
  this harvest did not use.
- The city's FOIA page says a records request is not needed for information already
  available on the city website. No separate Civic Access bulk-use or redistribution
  terms were found. Therefore these artifacts remain private and are not approved for
  republication.

## Controlled 25-record pilot

### Tyler record

The exact query `MECH-2026-00095` returned one permit in 5.931 seconds from route
navigation to search API response:

- HTTP 200;
- number `MECH-2026-00095`;
- status `Issued`;
- type `Mechanical - Residential`;
- issue date `2026-06-24`;
- source parcel `1707427012`;
- work location `37 VELIE DR ROCK ISLAND IL 61201`.

The local appraisal seed contains exact PIN `1707427012` and address
`37 VELIE DR, ROCK ISLAND IL 61201`. This is one evidence-backed PIN/address match, not
an inferred match. The search result did not expose a description, valuation, or
contractor, so those fields remain unknown for this portal record.

### Official-report records

The report pilot selected 24 unique rows from the August 2025 official report:

- issue date: 24/24;
- work location: 24/24;
- source `TAX_MAP`: 24/24;
- description/purpose: 24/24;
- valuation: 21/24;
- conservatively recognized contractor business: 24/24;
- explicit appraisal parcel match: 0/24.

`TAX_MAP` is retained as source evidence, but `parcel_identifier` remains null unless a
later local/database matching step proves identity. Owner-labelled rows were not copied.
Contractor text was retained only when it contained clear organization evidence; a
person-looking contractor string is deliberately omitted.

## Full private harvest

- Indexed and processed PDFs: 112 (100% of the official index).
- Parsed source rows: 25,608.
- Unique permit/date records: 24,787.
- Issue date: 24,787 (100.00%).
- Work location: 24,775 (99.95%).
- Source tax map / parcel value: 24,744 (99.83%).
- Description / purpose: 24,718 (99.72%).
- Valuation: 18,704 (75.46%).
- Conservatively recognized contractor business: 20,351 (82.10%).
- Explicitly joined appraisal parcel: 0 (0.00%).

All report rows have status `Issued`, based on the official index's stated report
semantics. Legacy reports use source section codes as record types. The April 2026 report
uses a newer Tyler-generated layout with explicit permit types and ten-digit parcel
values; the parser handles both layouts.

The full artifact does not assert parcel matches. Exact/unique matching to the appraisal
seed or database is intentionally deferred to the coordinated loading stage. The one
Tyler pilot record above was independently checked against the local seed and is reported
separately.

## Private artifacts

Base directory:

`downloads/rock-island/permit-harvest/city-rock-island-2026-08-14/`

- `portal-pilot-records.private.jsonl` — one Tyler permit.
- `pilot-records.private.jsonl` — 24 official-report permits.
- `pilot-combined-summary.json` — combined 25-record provenance and exact seed match evidence.
- `pilot-source-provenance.json` — report URL, document id, hash, bytes, timing, and range.
- `pilot-summary.json` — pilot counts and field coverage.
- `full-records.private.jsonl` — 24,787 merged unique permit/date records.
- `full-source-provenance.json` — provenance and SHA-256 for all 112 PDFs.
- `full-summary.json` — full counts, coverage, and artifact paths.
- `raw/reports/<document-id>.pdf` — 112 resumable source PDFs, mode 0600.

These files exclude owner-labelled rows and contact fields. Addresses, descriptions, and
contractor business candidates remain privacy-restricted private staging fields and are
not publication-approved.

## Implementation

- `scripts/permit-source-adapters/rock-island-monthly-reports.mjs`
  - discovers official monthly report links;
  - extracts fixed-column PDF text with coordinates;
  - supports legacy and April 2026 Tyler report layouts;
  - omits owner/person rows and never infers parcel identity;
  - losslessly merges repeated page-break variants.
- `scripts/harvest-rock-island-monthly-permits.mjs`
  - pilot/full modes;
  - serial downloads with a minimum one-second delay;
  - resumable document-id PDF cache;
  - private mode-0600 JSONL, provenance, and summary output;
  - no database, cloud, queue, or publication path.
- `tests/scripts/rock-island-monthly-permits.test.mjs`
  - index discovery, privacy filtering, legacy/new-layout parsing, duplicate merging,
    business-name conservatism, and CLI safety guards.
- `pdfjs-dist`
  - local deterministic PDF text extraction dependency.

## Verification

- Focused Rock Island permit suites: 10/10 tests passed.
- Direct JSDoc/JavaScript type check for the two new harvester modules: passed.
- Mandated `npm run typecheck`: passed for every configured workspace.
- Mandated `npm run test`: 654/655 tests passed on two full-suite runs. The same unrelated
  downloader EventBridge test exceeded its existing 5-second timeout in both parallel
  full-suite runs. Its dedicated file passed 15/15 immediately afterward, including the
  timed-out case in 2.414 seconds. No Rock Island permit test failed.

## Remaining boundary

The source harvest is complete through the latest report currently indexed by the city.
The official reports cover issued permits, not every application, inspection, fee,
attachment, or lifecycle transition in Civic Access. The portal's total historical record
count and earliest searchable record remain unknown, and the monthly index does not cover
pre-2017 history.

Database loading and parcel matching must remain a separately coordinated step while the
Illinois corporate load is active. No public output should be produced until a field-level
privacy and reuse review explicitly approves it.

## Public-index refresh on 2026-08-14

The official permit-report index was checked again at **2026-08-14T21:46:56.648Z**.
It returned HTTP 200 and still exposed exactly **112** report links. The latest linked
report remains Archive document `20365`, **April 2026**, with parsed issue dates ending
**2026-04-30**.

No May, June, July, or August 2026 report link was present. Therefore:

- newly published reports: **0**;
- new parsed source rows: **0**;
- new unique loader-key rows: **0**;
- new issue-date range: **none**;
- raw PDF downloads: **0**;
- PostgreSQL/Neon loads: **0**;
- AWS, Filebase, IPFS, and MCP actions: **0**.

The existing private package and closed public allowlist remain unchanged. The index is
organized as one slot per calendar month and historically receives one report after a month
closes, but it is currently more than three months behind the April cutoff. Treat the cadence
as **monthly when the City publishes**, not as a guaranteed monthly release. Recheck the
single official index weekly; download and parse only newly linked document ids, then
deduplicate against the existing loader key before producing an append-only local package.
