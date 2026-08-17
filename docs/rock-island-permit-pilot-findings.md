# Rock Island County permit source-certification pilot

**Pilot date:** 2026-08-13
**Authorized jurisdictions:** Moline, City of Rock Island, East Moline, Carbon Cliff
**Authorized activity:** conservative local source probes and reusable local implementation only
**Not authorized:** AWS changes, deployment, queueing, Neon mutation, full harvest, or IPFS publication

This artifact is intentionally permit-specific so concurrent appraisal/onboarding work does
not need to modify the general Rock Island findings or source catalog.

## Outcome

`scripts/permit-source-discovery/certify.mjs docs/rock-island-sources.yaml` passed the
four catalogued portal URLs and skipped the 12 manual/unknown jurisdictions:

- Moline: CentralSquare eTRAKiT, static certification `PASS`.
- City of Rock Island: Tyler EnerGov / Civic Access, static certification `PASS`.
- East Moline: iWorQ, static certification `PASS`.
- Carbon Cliff: the delegated East Moline iWorQ portal, static certification `PASS`.

That certification proves a reachable permit-branded portal and matching vendor. It does
**not** prove that record-level automation is permitted or complete. Applying the stricter
record-source gate produced:

- **City of Rock Island: pilot-certified for explicit anonymous permit-number/address
  lookups.** A local browser-session adapter and normalized JSONL runner were implemented.
- **Moline: not record-source-certified.** The static public search page is readable, but
  local Chrome receives `Forbidden`; no adapter was fabricated.
- **East Moline: not record-source-certified.** `robots.txt` is `User-agent: * /
Disallow: /`; record search stopped at the public landing page.
- **Carbon Cliff: not record-source-certified.** It delegates four permit/inspection
  classes to East Moline and inherits the same iWorQ robots blocker.

No full-scale harvest was started.

## Geographic reach versus verified permit coverage

The four selected appraisal jurisdictions contain **41,857 of 65,806** canonical county
PINs (about **63.6%**):

- Moline: 17,486.
- City of Rock Island: 15,738.
- East Moline: 7,834.
- Carbon Cliff: 799.

This is only potential routing reach. The implemented source currently covers the City of
Rock Island's 15,738 parcels (about 23.9% of the county denominator), and even there a
permit is known only when an explicit public search returns it. No claim is made that every
parcel has a permit, that portal records are complete, or that returned `MainParcel`
identifiers always join to the appraisal PIN.

Carbon Cliff's potential portal reach must not be confused with its actual delegated
scope. Its official page verifies only plumbing, mechanical, building, and electrical
permitting/inspections through East Moline, after Village Hall clearance. The shared East
Moline portal offers additional application classes, but there is no evidence that all of
those classes apply to Carbon Cliff.

## Jurisdiction evidence

### Moline — CentralSquare eTRAKiT

- Official source page:
  `https://www.moline.il.us/1949/Online-Permitting-System`.
- Portal: `https://moli.csqrcloud.com/community-etrakit/`.
- Public static paths:
  - permit search: `/Search/permit.aspx`;
  - property search: `/Search/parcel.aspx`.
- The permit page exposes permit-number and site-address search, with
  begins-with/contains/equals/at-least/at-most operators. Search examples instruct users
  to omit street suffixes.
- The property page advertises Site, APN, site address, geographic type, site number, and
  status criteria.
- Permit-result detail tabs advertise permit info, site info, contacts, fees,
  inspections, chronology, conditions, and reviews.
- The static page returned HTTP 200 and is an ASP.NET/Telerik form. It also loads Google's
  reCAPTCHA script.
- A real local headless Chrome render returned only `Forbidden`. No challenge, CAPTCHA, or
  security control was bypassed.
- `https://moli.csqrcloud.com/robots.txt` returned the body `Forbidden`, not an
  interpretable robots policy.
- Available history, tenant correctness behind the generic “Welcome to Central City”
  greeting, record classes, rate tolerance, and record-level terms remain unknown.

**Decision:** no Moline adapter. The safe fallback is a City of Moline Building/Inspections
records request or an officially supplied bulk export. The official page publishes
`buildingpermits@moline.il.us` for portal assistance.

### City of Rock Island — Tyler EnerGov / Civic Access

- Official source page:
  `https://www.rigov.org/634/Building-Permits-and-Contractor-Registra`.
- Portal:
  `https://cityofrockislandil-energovweb.tylerhost.net/apps/selfservice#/home`.
- The rendered anonymous portal identifies the City of Rock Island tenant and exposes
  `Search Public Records` without login.
- Global public search covers permit, plan, inspection, code case, request, license, and
  project modules. An exact permit-number query returned one permit.
- A representative address query returned one permit, two inspections, and one license;
  the adapter retains only module `2` permit entities.
- Search results expose permit number/type/status, apply/issue/expiration/final dates,
  project, main parcel, work address, and description.
- The public detail route calls `POST api/energov/permits/permitdetail` and exposes permit
  type, work class, status, dates, description, address, parcel, square feet, and value.
- A browser/tenant bootstrap is required. A direct detail API POST without it returned
  HTTP 200 with `Success: false` and `Cannot find tenant information.`
- The portal's `robots.txt` path returned HTTP 404. That is not a grant of bulk-use or
  republication rights.
- Global parcel-keyword search returned zero for one known PIN. The advanced request
  contract contains `PermitCriteria.ParcelNumber`, but a deliberately minimal advanced
  request returned HTTP 500. Parcel-search behavior is therefore **unknown**, not
  certified.

The city also publishes monthly issued-permit PDF reports at
`https://www.rigov.org/1276/Permit-Reports`. The index currently spans January 2017
through April 2026: 112 monthly slots. Reports are organized by permit application type
and are the verified historical fallback, but they cover issued permits rather than the
portal's full application/inspection lifecycle. They also include owner/contractor text.
No report parser should emit names, contact data, or unreviewed free text to public
artifacts.

Observed classes from the official city site/report index include commercial and
residential building, excavation, roofing, demolition, mechanical, plumbing, electrical,
sign, and utility-turn-on permits. The live pilot permit was residential mechanical.

### East Moline — iWorQ

- Official source page:
  `https://www.eastmoline.com/442/Permits-Contractor-Reg-Food-Beverage-Por`.
- Portal:
  `https://eastmolinepermit.portal.iworq.net/portalhome/eastmolinepermit`.
- The public landing page exposes `Search Existing Permits` and advertises status,
  additional-file upload, messaging, and payment after locating a permit.
- Public application links enumerate curb cut, electrical, mechanical, plumbing,
  residential building, commercial building, sign, demolition, and residential roofing.
- The search link resolves to `/EASTMOLINE/permits/600`.
- `robots.txt` explicitly says `User-agent: *` and `Disallow: /`.

**Decision:** no record search, endpoint capture, history claim, or adapter. Exact search
fields, available date range, anonymous detail visibility, sessions, and throughput remain
unknown. Request a municipal export or records response.

### Carbon Cliff — delegated East Moline iWorQ

- Official source page:
  `https://carboncliff.gov/permits-zoning-and-floodplain`.
- Carbon Cliff explicitly contracts East Moline for plumbing, mechanical, building, and
  electrical permits and inspections, after Village Hall clearance.
- The East Moline contractor portal separately labels East Moline and Carbon Cliff
  contractor workflows and links both to the shared permit application portal.
- The delegated existing-permit source is the same East Moline iWorQ portal and therefore
  has the same `Disallow: /` robots blocker.

**Decision:** no record lookup or adapter. The safe fallback is a joint Carbon Cliff /
East Moline records request that asks the custodians to distinguish the issuing
jurisdiction in the export.

## Probe counts, latency, and failures

- Egress country was verified as `US`.
- Portal recertification made one static fetch per configured online portal:
  4 passed, 0 review, 0 unreachable, 12 non-portal rows skipped.
- Moline: one rendered search-page attempt; it returned `Forbidden`. Zero permit records
  were queried.
- East Moline: one rendered landing-page inspection. Zero permit records were queried
  after the robots prohibition was found.
- Carbon Cliff: one official delegation-page inspection plus the shared iWorQ landing
  evidence. Zero permit records were queried.
- City of Rock Island: the ceiling of 10 search attempts was not exceeded. The attempts
  included four distinct representative values, repeat validation of one known permit,
  one empty-control diagnostic, and one deliberately minimal advanced request.
- Four browser-driven HTTP 200 search responses observed after an initialized portal page
  completed in roughly 2.09-2.46 seconds (observed p50 about 2.42 seconds, p95 about
  2.46 seconds). A full route-driven anonymous session plus exact search completed in
  7.85 seconds.
- One known permit normalized to one privacy-restricted local JSONL row. One address
  search exposed four mixed-module entities, of which one was a permit.
- Failures/blockers:
  - Moline rendered `Forbidden`.
  - iWorQ robots disallowed record automation.
  - a direct Tyler API request lacked tenant context;
  - a minimal advanced Tyler parcel request returned HTTP 500;
  - the initial DOM-typeahead runner was intermittent, so the final implementation uses
    the portal's own exact-search hash route and verifies the submitted API keyword.

The final route-driven code was unit-tested after the search-attempt ceiling was reached.
The equivalent route-driven live probe had already returned the expected exact keyword,
HTTP 200, one permit, and module discriminator `2`; no additional live request was made
after the cap.

## All-history and 48-hour gate

- **Rock Island monthly reports:** 112 official monthly slots from 2017 through April
  2026 make a bounded issued-permit backfill plausible, but PDF download/parsing latency,
  layout variance, record count, and privacy redaction were not benchmarked in this phase.
  It is therefore not yet approved as a harvest despite being far smaller than a
  parcel-by-parcel county crawl.
- **Rock Island Civic Access:** earliest searchable record and total permit count remain
  unknown. No full-download elapsed estimate can be defended.
- **Moline, East Moline, Carbon Cliff:** record count, date range, safe concurrency, and
  request rate remain unknown because their access blockers prevented a representative
  record benchmark.

The >48-hour decision gate remains **unresolved** for every full-history source. Before
any harvest, obtain a report/export record count or run a newly approved bounded pilot,
then calculate elapsed time from measured latency, required delay, one-at-a-time
concurrency, and retry overhead. Do not infer feasibility from portal reachability.

## Implementation and local artifacts

Tracked additions:

- `scripts/permit-source-adapters/tyler-civic-access.mjs`
  - reusable rendered Tyler public-search adapter;
  - hard maximum of 10 serialized lookups and minimum 1-second delay;
  - tenant-bootstrap-aware exact route search;
  - deterministic normalized city-permit output compatible with the existing
    `normalized-jsonl` loader contract;
  - contact/applicant/assignee/email/raw-response exclusion.
- `scripts/probe-rock-island-permits.mjs`
  - explicit-query local runner;
  - no default crawl, AWS, queue, database, or publication path;
  - local output mode `0600`.
- `tests/fixtures/rock-island-permits/tyler-search-response.json`
  - synthetic, minimal, no scraped PII.
- `tests/scripts/rock-island-permit-probe.test.mjs`
  - normalizer, permit-module filtering, deterministic JSONL, privacy deny-list, and
    lookup/rate guardrail coverage.

Local gitignored pilot output:

- `downloads/rock-island/permit-pilot/rock-island-city-normalized.jsonl`
  - one allow-listed record assembled from the verified exact live response;
  - not publication-approved and not uploaded.

No worker, state machine, CDK, AWS, sibling query-db, shared Rock Island source catalog,
or general findings file was changed.

## Verification and next safe step

Focused tests:

```bash
npx vitest run \
  tests/scripts/rock-island-permit-probe.test.mjs \
  tests/scripts/permit-source-discovery-vendors.test.mjs
```

The next source step is **not** a harvest. Ask Rock Island for a machine-readable issued
permit export covering pre-2017 and current Tyler history, and ask Moline / East Moline /
Carbon Cliff for jurisdiction-labelled bulk exports plus automation/reuse terms. For a
newly approved one-query Tyler check, use:

```bash
node scripts/probe-rock-island-permits.mjs \
  --query "<known-public-permit-number>" \
  --output downloads/rock-island/permit-pilot/rock-island-city-normalized.jsonl \
  --delay-ms 1500
```

Do not run that command again under this completed pilot window: the authorized Rock
Island search-attempt ceiling has been reached. A new bounded approval should precede
additional live searches.

## Public-source-only discovery refresh — 2026-08-14

This refresh used only official pages, openly linked reports, public meeting archives, and
vendor-neutral documentation. It did not submit a records request, retry the prohibited Moline
portal or East Moline/Carbon Cliff iWorQ record paths, bypass a 403/robots/CAPTCHA/login, or
mutate a database or cloud service.

### City of Rock Island refresh

The official monthly index was checked at **2026-08-14T21:46:56.648Z**. It returned HTTP
200 with the same 112 links and the same latest report: April 2026, document `20365`.
There are no reports after the existing **2026-04-30** cutoff, so there are **0 new source
rows, 0 new unique loader-key rows, and no new issue dates**. No PDF or package was changed.

The index has one calendar slot per month but does not guarantee a release date. Because the
current publication is more than three months behind, the safe refresh cadence is one weekly
index request, followed by serialized downloads only when a new official document id appears.
The detailed refresh result is recorded in
`docs/rock-island-city-permit-harvest-findings.md`.

### Moline — official monthly Archive Center reports found

- Official archive:
  `https://www.moline.il.us/926/Permit-Reports`.
- Vendor/access: CivicPlus Archive Center containing report PDFs exported from the municipal
  permit system; plain anonymous HTTPS, no portal search, login, CAPTCHA, or hidden endpoint.
  The site `robots.txt` does not disallow the archive or report paths.
- History: 163 archive links covering 162 unique month labels from **2012-12 through
  2026-06**. The archive has two May 2017 items (`4042` and `4043`) and no December 2015
  item. The direct printed-permit-number layout actually begins in **2024-10**, producing
  21 reports through 2026-06. The user-requested 2025-01 through 2026-06 interval contains
  18 reports.
- Current-layout fields: permit number, permit type, permit subtype, issue date, contractor
  name, address, and usually job value. October 2024 has no job-value column. Private staging
  keeps address, valuation when printed, and only
  conservatively recognized contractor organizations. The public allowlist excludes address,
  contractor, valuation, parcel, description, people, and contacts.
- Legacy-layout fields: permit code and description, issue date, application year and number,
  contractor/subcontractor rows, township/parcel text, street address, application type,
  permit status, and estimated value. The tested stable key uses only the exact printed permit
  code, application year, application number, and issue date; it never fabricates a modern
  permit number.
- Probe: ten official PDF responses across December 2012, 2016, 2019, 2022, 2024, 2025,
  and 2026 returned HTTP 200. Multi-report probes were serialized with 1.2-second spacing.
  Observed timed responses ranged from 0.323 to 3.161 seconds with no throttles or source
  failures. The source publishes no explicit request-rate limit; retain concurrency 1 and at
  least a one-second delay.
- Parser validation: June 2026 archive item `11988` produced **233 rows**, **233 unique
  loader keys**, no duplicates, and issue dates **2026-06-01 through 2026-06-30**. Its closed
  public allowlist had 233 unique permit numbers, zero schema drift, and zero forbidden keys.
- Current harvest: all 18 requested reports produced **3,648 unique permits** with no duplicate
  loader keys and issue dates **2025-01-02 through 2026-06-30**. The three additional
  direct-number reports from October-December 2024 produced 549 permits, yielding a complete
  load-ready direct-number package of **4,197 records** through 2026-06-30.
- Legacy harvest: the versioned application-identity parsers produced **9,422 unique records**
  from 39 reports covering **2017-01-03 through 2020-04-30**, and **8,980 unique records**
  from 42 reports covering **2021-03-02 through 2024-09-30**. These 18,402 records use only
  printed application year, application number, permit code, and issue date for identity.
  They retain the original legacy identity fields and never fabricate a modern permit number.
- Full supported package: 102 reports produced 22,655 source rows and **22,599 unique loader
  keys** after merging 56 repeated source rows. All issue dates remain inside their archive
  month. Every final loader key is unique. Field coverage is 22,053 work locations, 18,402
  source parcel/township text values, 17,377 descriptions, 18,604 contractor-business values,
  and 22,290 valuations; none of the legacy parcel text was promoted to an explicit PIN.
- Completeness: 67 reports matched their printed total exactly, 29 legacy reports differ by
  one to four records after identity-key deduplication, and six reports have no readable total.
  The manifest records every delta rather than treating printed totals as stable identities.
  The 18 requested 2025-01 through 2026-06 reports have 16 exact totals and two unavailable
  totals; no current report has a mismatch.
- Blocked legacy reports: 48 reports from 2012-12 through 2016-12 remain blocked because sampled
  files contain redacted or merged application identities. Ten compacted reports from 2020-05
  through 2021-02 and isolated July 2022 item `8234` merge identity columns. Item `4042` is
  labelled May 2017 but serves an April 2020 report. April 2018 item `4729` extends into May
  and conflicts with the official May report for the same identity. These **61 reports** were
  excluded without inventing or choosing an identity.
- The prohibited CentralSquare portal was not retried.

Local gitignored probe artifacts:

- `downloads/rock-island/permit-discovery-2026-08-14/moline-2026-06/11988.pdf`;
- `2026-06-records.private.jsonl`;
- `2026-06-records.public-allowlist.jsonl`;
- `summary.json`.

Load-ready local package:

- `downloads/rock-island/permit-harvest/moline-archive-2026-08-14/load-ready.private.jsonl`
  — 22,599 private normalized records, SHA-256
  `08f9eb6b914ff06331a0c81a1b416c9f4ba67b48e73cec5b2e207e26e72293be`;
- `public-allowlist.jsonl` — 22,599 closed-allowlist rows, SHA-256
  `c42914903243819cdd8e149d37d0535315fd45d935858889c8977f61d641ca24`;
- `current-2025-load-ready.private.jsonl` and
  `current-2025-combined.public-allowlist.jsonl` — the requested 3,648-record current-only
  package, kept separate from the combined archive outputs;
- `legacy-2017-2020.*.jsonl`, `legacy-2021-2024.*.jsonl`,
  `current-transition-2024.*.jsonl`, and `current-2025.*.jsonl` — deterministic private and
  public files split by supported era;
- `supported-manifest.json` — index hash, per-report source IDs and PDF hashes, field counts,
  completeness checks, blocked-era inventory, and artifact hashes;
- `raw/reports/<archive-id>.pdf` — resumable owner-only source files for reports actually
  downloaded during the bounded investigation and supported harvest.

Implemented and tested code:

- `scripts/permit-source-adapters/moline-issued-permit-reports.mjs`;
- `scripts/harvest-moline-issued-permits.mjs`;
- `tests/scripts/moline-issued-permit-reports.test.mjs`.
- `tests/scripts/harvest-moline-issued-permits.test.mjs`.

### East Moline — no alternate record export found

- Official pages checked: the building/permit page and the public CivicPlus Agenda Center,
  including 2026 City Council, Committee of the Whole, Planning/Zoning, and zoning-appeal
  documents.
- Vendor: iWorQ for permit operations; CivicPlus only for agendas/minutes.
- Public alternative content: agendas and minutes contain occasional narrative references to
  named developments or a permit being issued, not an issued-permit table, download, or API.
- History/fields: meeting documents are publicly listed, but no recurring permit-record
  history or record schema is published.
- Access/rate: anonymous static agenda PDFs; no record endpoint and therefore no record-source
  rate limit to test.
- Feasibility: not feasible for record-level ingestion. The iWorQ `Disallow: /` path was not
  retried.

### Carbon Cliff — maintained register is not published

- Official sources checked: permit/zoning page, ordinance/document library, and public site
  search results.
- Vendor: village administration plus delegated East Moline iWorQ for building, electrical,
  mechanical, and plumbing work.
- Verified fact: the village code requires the clerk to maintain a register of licenses and
  permits and payments, but the register is not exposed as a report, download, or API.
- History/fields/rate: no open record file, history, schema, or endpoint rate exists to test.
- Feasibility: not feasible from currently published records. The delegated prohibited iWorQ
  path was not retried.

### Unincorporated Rock Island County — official aggregate reports only

- Official sources: Zoning & Building Safety pages and Public Works & Facilities agenda
  packets on `rockislandcountyil.gov`.
- Vendor/access: county-managed permit program with CivicPlus Agenda Center PDF packets,
  anonymous HTTPS.
- Verified report history samples: November 2017, May 2019, May 2022, and February 2024.
  The archive may contain additional months, but a complete continuous range was not proven.
- Fields: monthly counts and fees by permit category, total receipts, net revenue, estimated
  project valuation, fiscal-year comparisons, and a new-residence schedule with permit
  numbers, square footage, valuation, and township totals. It is not a complete row-level
  building-permit register.
- Rate: no published limit; only passive official packet reads were used.
- Feasibility: aggregate trend ingestion and the partial new-residence series are technically
  feasible after archive enumeration. Complete permit-record ingestion is not feasible from
  these packets, so no parser was implemented.

### Silvis — applications only

- Official sources: inspections page and city downloads directory.
- Vendor/access: municipal inspector, static anonymous PDFs.
- Fields/history: blank building, plumbing, mechanical, electrical, and sign applications,
  contractor registration, codes, and fee schedules; no issued-permit rows or history.
- Rate: not applicable because no record endpoint exists.
- Feasibility: no record-level bulk source found.

### Milan — inspector narrative only

- Official sources: village contact page, public meeting media/minutes, and planning pages.
- Vendor/access: municipal Building and Inspections office; anonymous meeting PDFs.
- Fields/history: sampled minutes contain an “Inspector’s Report” and qualitative project
  updates, but the underlying permit report is not attached and no recurring issued-permit
  table or API was found.
- Rate: not applicable to record retrieval.
- Feasibility: meeting-minutes text is unsuitable for deterministic permit ingestion.

### Coal Valley — applications and meeting archive only

- Official sources: Building & Inspections, Building Permits, Documents, board minutes, and
  zoning minutes pages.
- Vendor/access: municipal inspection office; static application PDFs and a public document
  browser.
- Fields/history: application requirements, adopted codes, and meeting records; no issued
  permit register/report/API found.
- Rate: no record endpoint.
- Feasibility: no record-level bulk source found.

### Hampton — shared Silvis intake, no published records

- Official source: the village general-information page and public minutes.
- Vendor/access: contractor registration and permit application materials are sent to the City
  of Silvis; the workflow is office/document based.
- Fields/history: blank intake materials and board minutes only; no issued-permit list, range,
  or API.
- Rate: no record endpoint.
- Feasibility: no record-level bulk source found.

### Port Byron — forms and minutes only

- Official sources: forms/permits page and board-minutes archive.
- Vendor/access: village building inspector; static Wix-hosted PDFs.
- History: linked board minutes span 2020 through 2026, with gaps in the link inventory.
- Fields: building application and fee schedule plus narrative minutes; no recurring issued
  permit rows.
- Rate: no record endpoint.
- Feasibility: no record-level bulk source found.

### Andalusia — county/village process, aggregate evidence only

- Official sources: village minutes/document archive, village ordinances, and Rock Island
  County building application/report packets.
- Vendor/access: village zoning/public-works approval combined with Rock Island County
  building-safety forms and reporting; anonymous static PDFs.
- History/fields: village minutes are published from 2014 through 2026 but do not contain a
  permit register. County packets expose only the aggregate fields and new-residence subset
  described above; county forms explicitly include Andalusia approval where applicable.
- Rate: no complete record endpoint.
- Feasibility: only aggregate/partial county reporting is available; no jurisdiction-complete
  row-level ingestion.

### Rapids City — permit log mentioned but not attached

- Official sources: village information page, code, and May-July 2026 board agendas.
- Vendor/access: in-person village program using Rock Island County fee rates; anonymous static
  agenda PDFs.
- Verified status: permits and payments are accepted only in person. The monthly agendas name
  a recurring “Permit Log Report,” but no log attachment or downloadable record file was found.
- History/fields/rate: the referenced log's range and fields are unknown; no endpoint exists to
  benchmark.
- Feasibility: not feasible unless the village begins attaching the already-referenced log.

### Cordova — official application only

- Official source: `https://villageofcordova.com/government`, replacing the obsolete
  `cordovail.org` pointer.
- Vendor/access: village office; anonymous GoDaddy-hosted application PDF.
- Fields/history: blank building-permit application only; the meetings page publishes a
  schedule, not agendas, minutes, or permit reports.
- Rate: no record endpoint.
- Feasibility: no record-level bulk source found.

### Hillsdale — county-served, aggregate reports only

- Official county source: the zoning page explicitly says Rock Island County maintains zoning
  information for unincorporated county and the Village of Hillsdale.
- Vendor/access: Rock Island County Zoning & Building Safety; county forms and agenda packets.
- Fields/history: no Hillsdale-specific issued-permit list was found. County monthly packets
  contain aggregate activity and a new-residence subset only.
- Rate: no complete record endpoint.
- Feasibility: Hillsdale-specific record ingestion is not feasible from the public packet
  surface.

### Reynolds — no official online permit surface found

- Verified scope: the village spans Rock Island and Mercer Counties, making county attribution
  material.
- Vendor/access/history/fields/rate: no official village permit page, report archive, download,
  or API was found. Third-party directory pages were not treated as record sources.
- Feasibility: unknown and not ingestible from a verified public municipal source.

### Oak Grove — no official online permit surface found

- Rock Island County's official building page says incorporated municipalities manage their
  own permit rules; no official Oak Grove permit page or report archive was found.
- Vendor/access/history/fields/rate: unknown. Third-party summaries were not treated as record
  sources.
- Feasibility: not ingestible from a verified public municipal source.

### Federal aggregate fallback

The U.S. Census Bureau Building Permits Survey is an official public fallback for new
privately owned residential-construction **aggregates**, not individual permits. It publishes
monthly files from January 2000 forward and annual files from 1990 forward, with units and
valuation by structure type at county/place/permit-office levels. Place-level data are
downloadable ASCII files, not a per-permit API. Revised geographic data are generally released
around the 17th-18th workday and annual data in May. This source can support coverage/trend
metrics but cannot replace a municipal record adapter, so no parser was added in this task.

### Refresh verification

- Focused Moline parser/harvester plus published-county catalog suites: 30/30 tests passed.
- Direct JSDoc/JavaScript type check for the Moline parser, harvester, and tests: passed.
- Mandated `npm run typecheck`: passed for every configured workspace.
- The initial continuation run reproduced the unrelated downloader five-second timeout and
  stale published-county expectation described above. The catalog expectation now explicitly
  includes the pre-existing Rock Island row without weakening the exact-list assertion.
- The downloader suite passed 15/15 independently. Final mandated `npm run test` passed
  **690/690**, including downloader 15/15 in the parallel full suite.
- YAML parsing and the 16-entry permit catalog count check: passed.
- Independent artifact verification confirmed all 14 artifact hashes, byte counts, JSONL row
  counts, and mode-0600 permissions across both manifests; 22,599 private and public loader
  keys are unique, all 4,197 modern permit/date pairs are unique, provenance is complete, and
  the public files contain zero forbidden keys.
- Formatting and `git diff --check`: passed.
