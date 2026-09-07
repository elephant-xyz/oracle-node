# Polk permit source audit and pre-scale gate

Verified 2026-09-03. This document is the operational gate for any Polk permit
harvest or detail-enrichment run. An adapter or a successful current-record
pilot does not establish historical completeness.

## Current decision: GO WITH DOCUMENTED GAPS

Proceed with ingestion and publication of the strictly verified CAMA, Polk
County Accela list, and Lakeland ArcGIS artifacts. Use detail adapters only for
selective fields or explicitly bounded redrives. Do not resume the 230,114-row
countywide detail-enrichment run: it is still outside the 48-hour feasibility
gate and cannot close the documented historical/custodian gaps.

- The Property Appraiser's 531,344-row `ftp_permit` file is a nightly,
  tax-valuation-oriented projection, not a complete permit archive. The
  Appraiser explicitly excludes permits that do not affect taxable value, such
  as residential fences, re-roofs, and driveways, and is not the issuing
  custodian.
- The projection has 192,808 rows without an agency, 15,556 Property Appraiser
  pseudo-permit/work rows, 410 rows without permit numbers, and 301,123 rows
  outside currently certified full-adapter agencies.
- The paused default candidate file has 230,114 rows but only 215,422 distinct
  agency/permit pairs. Duplicate source requests must be coalesced while
  retaining each valid input row.
- Polk County's 182,886 CAMA rows alone have a theoretical lower bound of 50.8
  hours at one request per second. The 25-record detail pilot took about 34
  seconds, projecting roughly 69 hours before retries. This exceeds the
  repository's 48-hour source-feasibility gate.
- Several municipalities use a predecessor system, a delegated County portal,
  CAPTCHA, manual custody, or a portal whose online retention is shorter than
  the CAMA date span.

The paused work was not reset:

- Main: 353 of 2,302 chunks verified, representing 35,300 records: 35,156
  enriched, 144 no-detail, and zero fetch failures after local error-taxonomy
  repair.
- Winter Haven legacy: all 191 chunks and 19,015 records verified: 18,671
  enriched, 344 no-detail, and zero fetch failures.
- `--stage verify` validates every contiguous atomic part without making
  network requests or writing new result parts. It recovered 128 valid Winter
  Haven parts hidden by the former rewinding checkpoint counter.

## Preferred acquisition order

1. Retain the nightly Property Appraiser ZIP as a countywide parcel-linked
   baseline, with its omissions stated.
2. Load official bulk/list APIs before detail pages:
   - Lakeland ArcGIS permit layer: 107,901 `TYPE='Permit'` rows, anonymous,
     ordered/paginated, and richer than the 40,202 Lakeland CAMA rows.
   - Polk County GIS permit layer: 74,137 rows, anonymous and paginated, but
     only a map-oriented subset of the 531,344 CAMA rows.
3. Obtain native custodian exports for CAPTCHA, manual, predecessor, and
   portal-retention gaps.
4. Use per-permit detail adapters only for fields missing from bulk/list data,
   such as contractors, licences, inspections, and documents.
5. Preserve every valid permit even when no explicit parcel/address match
   exists. Store it with a null property relation and its issuing jurisdiction;
   never infer a parcel, address, or agency from identifier shape alone.

Official baseline:

- Property Appraiser download:
  <https://www.polkflpa.gov/FTPPage/ftpdefault.aspx?url=%5CAppraisalData>
- Property Appraiser permit schema and omissions:
  <https://www.polkflpa.gov/PCPA_FTP_DATA_HELP/FTP_PERMITFileLayout.html>
- Lakeland ArcGIS permit layer:
  <https://services1.arcgis.com/mcbQY5xNGGGM1vBX/ArcGIS/rest/services/IMS_Projects_Permits/FeatureServer/6>
- Polk County GIS permit layer:
  <https://gis.polk-county.net/hosting/rest/services/TPO/TPO_Parcel_and_Permit_Map/FeatureServer/0>

## Bounded pilot evidence

### ArcGIS list and pagination pilots

Lakeland ArcGIS:

- Count: 107,901 permits.
- Service cap: 16,000 rows; pagination, ordering, distinct, count, and
  statistics are supported.
- Ordered 100-row samples at the beginning, midpoint, and end returned 100
  rows each, strictly increasing `OBJECTID` values, no duplicate IDs, and no
  skipped terminal page.
- Concurrency 1, 2, and 4 returned HTTP 200 with complete 25-row pages. The
  bounded pilot observed no throttling; use 4 as the ceiling until a sustained
  pilot proves more.
- The complete frozen-boundary harvest committed 107,901 records in 54
  content-addressed parts in 32.6 seconds. Verification found 107,901 unique
  `GLOBALID` values and ended at the pinned maximum `OBJECTID` 8,182,195 with
  no count drift. The aggregate SHA-256 is
  `edd6fe968bf55391b37872abc9dbf55c03994c7baf40e1f9fa12f1218e129b93`.
- Permit numbers are also unique in this snapshot. Exact normalized
  reconciliation found 24,983 identifiers shared with Lakeland CAMA, 82,918
  ArcGIS-only identifiers, and 13,614 CAMA-only identifiers. ArcGIS-only and
  unmatched rows remain preserved with null property links.
- Data begins in 2014. Pre-2014 records still require the custodian.

Polk County GIS:

- Count: 74,137 rows.
- Service cap: 10,000 rows; deterministic ordered pagination is supported.
- Beginning, midpoint, and end 100-row samples were strictly ordered with no
  duplicate object IDs. One duplicate permit number occurred in the first
  sample, so permit number alone is not a row identity.
- Concurrency 1, 2, and 4 returned HTTP 200 with complete 25-row pages.
- The layer is substantially smaller than CAMA and contains date anomalies; it
  is corroborating data, not the county denominator.

### Polk County Accela list/export pilots

- The anonymous Building search exposes ASP.NET date controls and a session CSV
  export. The first page reports only `100+`, but the export is not limited to
  that display. A 2026-09-01 one-day search exported all 134 list rows; manual
  pagination revealed the total rising from 100 to 134 after page 10.
- The export preserves rows that have no detail link, including temporary
  application numbers. This avoids silently dropping valid list records.
- A 2010 annual search exported 3,443 rows in 13.3 seconds. A 2025 annual
  search did not finish within 120 seconds and was stopped without committing
  output. Monthly windows are therefore the maximum certified span for the
  resumable historical run.
- Search pilots found no records in 1900-2002 and three license records in
  2003; migrated `BLD-H-*` and `LIC-H-*` families are visible in later
  historical windows. The exact predecessor boundary and pre-online permit
  denominator remain custodian questions.
- A 2026-09-01 through 2026-09-03 pilot exported 452 unique records: 448
  permit/application records and four contractor-licensing records. It
  committed atomically and passed content-digest, schema, window, and identity
  verification.
- A sustained run committed and reverified the first 135 of 285 monthly
  windows: 51,211 source rows, 38 exact duplicate rows, and 51,173 unique
  accessible records through 2014-03-31. At one-second spacing the next source
  session stopped responding, so that interval is not certified for sustained
  use. The process was stopped without resetting its checkpoint.
- After a cooldown, an isolated 2014-04 pilot completed in 8.2 seconds with
  1,721 source rows, one exact duplicate, and 1,720 unique records. The resume
  contract is one session at a time, five seconds between monthly windows,
  30-second per-request timeouts, a 90-second whole-window attempt deadline,
  and two attempts.
- The resumed run completed all 285 windows in 42 minutes 18 seconds without a
  timeout or retry failure. Strict offline verification found 491,695 source
  rows, 404 exact duplicates, and 491,291 unique accessible records: 489,363
  permit/application records and 1,928 licensing records. Permit/application
  dates begin 2005-12-20; the only earlier rows are licensing records beginning
  2003-12-17. A fresh conservative run should reserve 60-90 minutes; a
  compatible completed checkpoint needs no source requests.
- Exact normalized identifier reconciliation found 97,314 County-Accela
  permits shared with the 170,308 distinct permit identifiers among 182,886
  `POLK COUNTY` CAMA rows. Accela contributes 392,049 identifiers absent from
  that County CAMA subset, while 72,993 County CAMA identifiers remain absent
  from Accela. Against all CAMA agencies, 102,513 identifiers overlap.
- The list path replaces full per-record detail scale; details should be
  fetched selectively after list/CAMA reconciliation.

### Detail-adapter pilots

All pilots used one in-flight request per source, a one-second source interval,
30-second request timeouts, two attempts, and atomic 25-record chunks.

- Polk County Accela: 24 enriched, one no-detail, zero fetch failures. The miss
  was numeric permit `91160`, confirming that current Accela detail lookup does
  not cover every predecessor identifier.
- Lakeland iMS: 25 enriched, zero no-detail/fetch failures; 17 exposed
  contractor licence evidence.
- Lake Wales CitizenLink: 25 enriched, zero no-detail/fetch failures after the
  adapter was fixed to carry the exact permit identity proven by the AJAX
  suggestion response into details that omit the number.
- Winter Haven eSuite: the legacy-family attempt produced 18,671 enriched and
  344 no-detail results from 19,015 `YYYY-NNNNNNNN` identifiers. A live ten-record
  failure pilot reproduced ten permanent no-detail results and zero transient
  failures, so 330 old errors were reclassified locally without repeating
  source requests. Strict semantic verification then found and locally
  downgraded 14 legacy false enrichments whose error pages contained no
  page-derived permit evidence.

These pilots certify request contracts, not historical completeness.

## Jurisdiction inventory

The CAMA spans below describe observed projection rows and are not proof that
the current online portal retains the same dates.

### Polk County / unincorporated

- Rows and span: 182,886; 1950-2026.
- Issuer/custodian: Polk County Building Division.
- Sources: current POLKCO Accela, official public-records process, and legacy
  Hansen lookup.
- Historical boundary: the completed anonymous list exposes licensing rows
  from 2003-12-17 and permit/application rows from 2005-12-20, including
  migrated `BLD-H-*` records. Numeric/pre-online Hansen numbers still require
  predecessor lookup, including `%` search behavior.
- Blocker and potentially missing: pre-2005, numeric/pre-online, and
  non-valuation permits; 72,993 distinct County CAMA identifiers do not occur
  in Accela, and the bounded detail pilot missed one numeric record.
- Access: anonymous current search; records fulfillment may require staff,
  payment estimate, or human approval.
- Safe concurrency: one list session at a time with five seconds between
  monthly windows, a 30-second per-request timeout, and a 90-second
  whole-window attempt deadline. One detail request may be in flight with at
  least 1,000 ms between starts.
- Status: partial. The anonymous monthly CSV list path is certified and the
  complete 2003-current-window harvest is strictly verified and reconciled to
  CAMA. Next action: obtain a native/predecessor export for the pre-2005 and
  72,993 CAMA-only identifiers, then reconcile County GIS as corroborating
  evidence. Do not resume the 69-hour full detail pass.

Official sources:
<https://www.polkfl.gov/services/building/permitting/>,
<https://aca-prod.accela.com/POLKCO/Cap/CapHome.aspx?module=Building&TabName=Building>,
and <https://apps.polk-county.net/CMSLookup/ExternalHansenDetail>.

### Auburndale

- Rows and span: 9,810; 1988-2026.
- Issuer/custodian: City of Auburndale Construction Services.
- Source: GovBuilt.
- Blocker: Cloudflare JavaScript/cookies, account-oriented status pages, no
  certified anonymous list/export, and unknown historical retention.
- Potentially missing: up to all 9,810 CAMA rows plus permits excluded from
  CAMA.
- Access: native records export may require staff, payment, or human approval.
- Safe concurrency: zero for unattended scraping.
- Status: unsupported. Next action: request a schema and one-month native
  export before considering browser automation.

Official sources: <https://auburndalefl.com/construction-services/> and
<https://auburndalefl.govbuilt.com/>.

### Bartow

- Rows and span: 7,251; 1960-2026.
- Issuer/custodian: City of Bartow Building Department and City Clerk.
- Blocker: the official site exposes applications/forms but no verified public
  historical search, list API, or export.
- Potentially missing: up to all 7,251 CAMA rows plus non-valuation permits.
- Access: human records fulfillment; staff charges and prepayment may apply.
- Safe concurrency: not applicable.
- Status: unsupported. Next action: request a native permit export and
  predecessor-system/cutover metadata.

Official source: <https://www.cityofbartow.net/159/Building-Department>.

### Davenport

- Rows and span: 10,232; 1997-2026.
- Issuer/custodian: City of Davenport; predecessor records may be County-held.
- Source: iWorQ public list with permit/date/parcel/address/applicant fields.
- Blocker: search submission invokes Google reCAPTCHA; the County-to-iWorQ
  cutover date is not documented.
- Potentially missing: all pre-cutover records and any rows inaccessible behind
  CAPTCHA.
- Access: CAPTCHA/human interaction or official native export; fees may apply.
- Safe concurrency: zero unattended.
- Status: partial. Next action: obtain current iWorQ export plus predecessor
  County export and the official cutover date.

Official sources:
<https://www.mydavenport.org/index.asp?SEC=54C1C62E-BE5B-43DE-AF31-EF135278CEAD>
and <https://portal.iworq.net/DAVENPORT/permits/600>.

### Dundee

- Rows and span: 2,869; 2004-2026.
- Issuer/custodian: Town of Dundee, with recent records also resolving in
  County Accela.
- Blocker: mixed Town/County custody and undocumented delegation/predecessor
  boundaries; no standalone list/export was found.
- Potentially missing: any of the 2,869 rows that do not resolve in POLKCO plus
  non-CAMA permits.
- Access: Town/County records staff may be required.
- Safe concurrency: one only for bounded POLKCO detail pilots.
- Status: partial. Next action: request both custodians' exports and delegation
  dates; pilot 5-10 identifiers per family only after receipt.

Official source:
<https://townofdundee.com/departments/building-services/>.

### Eagle Lake

- Rows and span: 3,295; 1950-2026.
- Issuer: City of Eagle Lake; portal custodian: Polk County Accela.
- Blocker: a recent BR record resolves, but delegation dates, predecessor
  custody, list totals, and historical retention are not certified.
- Potentially missing: old/numeric rows and non-CAMA permits.
- Access: anonymous POLKCO for current records; City records requests may
  require estimates/deposits.
- Safe concurrency: one for detail pilots.
- Status: partial. Next action: obtain delegation dates and stratify a 25-record
  pilot across identifier families/decades before sharing the County adapter.

Official source: <https://www.eaglelakefl.gov/building>.

### Fort Meade

- Rows and span: 1,965; 2002-2026.
- Issuer: City of Fort Meade; plan-review/inspection services are shared with
  Polk County.
- Blocker: a current BR record resolves in POLKCO, but City-held records,
  predecessor boundaries, and online totals are unknown.
- Potentially missing: old/numeric and City-only rows.
- Access: anonymous POLKCO; records requests may require payment.
- Safe concurrency: one for detail pilots.
- Status: partial. Next action: reconcile City and County exports before
  enabling the shared adapter.

Official source:
<https://www.cityoffortmeade.org/departments/building.php>.

### Frostproof

- Rows and span: 1,489; 2004-2026.
- Issuer/custodian: City of Frostproof.
- Blocker: manual/email workflow with no public record search or export.
- Potentially missing: up to all 1,489 CAMA rows plus non-CAMA permits.
- Access: human records request; payment may apply.
- Safe concurrency: not applicable.
- Status: unsupported. Next action: request a native export and schema.

Official source:
<https://cityoffrostproof.com/departments/building/>.

### Haines City

- Rows and span: 24,438; 1978-2026.
- Issuer/custodian: Haines City Development Services.
- Sources: iWorQ current list, JustFOIA, and official archive.
- Blocker: Google invisible reCAPTCHA; missing/invalid tokens return no rows.
- Potentially missing: all records inaccessible to unattended search,
  especially predecessor history.
- Access: CAPTCHA/human action or official records fulfillment; payment may
  apply.
- Safe concurrency: zero unattended.
- Status: partial. Next action: request an iWorQ/archive native export; do not
  automate CAPTCHA.

Official sources: <https://hainescity.com/155/Development-Services-Department>,
<https://haines.portal.iworq.net/HAINES/permits/600>, and
<https://hainescity.com/194/Public-Records>.

### Highland Park

- Independently labeled CAMA rows: none.
- Issuer/custodian: Village authorization followed by Polk County issuance.
- Historical boundary: a 1999 interlocal agreement superseded a 1975
  agreement.
- Blocker: Village authorizations and County-issued permits are split across
  custodians and absorbed into Polk County or agency-null CAMA rows.
- Potentially missing: count cannot be determined from CAMA.
- Access: human records requests may be required.
- Safe concurrency: zero until records are identified.
- Status: partial. Next action: request Village authorization records and
  County permit exports using the agreement dates; never infer these rows from
  geography alone.

Official source:
<https://www.highlandpark-fl.org/Permit_Authorization_Form_VHP.pdf>.

### Hillcrest Heights

- Independently labeled CAMA rows: none.
- Issuer/custodian: municipality with permitting/inspection services assigned
  to Polk County.
- Blocker: no trustworthy current municipal portal and no separable source
  count.
- Potentially missing: count cannot be determined from County/agency-null
  rows.
- Access: County and municipal human records confirmation required.
- Safe concurrency: zero until records are identified.
- Status: partial. Next action: obtain the current agreement and an official
  County export scoped by jurisdiction.

Official directory:
<https://www.polkflpa.gov/permitagencies.aspx>.

### Lake Alfred

- Rows and span: 4,432; 1977-2026.
- Issuer/custodian: City of Lake Alfred.
- Source: COLA Accela.
- Blocker: anonymous date search exists, but the default online window begins
  in 2023 while CAMA reaches 1977; identifier mapping and retention are not
  certified.
- Potentially missing: pre-online/cutover rows and non-CAMA permits.
- Access: anonymous current search; City records fulfillment for history.
- Safe concurrency: start at one.
- Status: partial. Next action: run old/mid/current one-day list pilots and
  obtain a native historical export before enabling an Accela adapter.

Official sources: <https://www.mylakealfred.com/166/Building-Permits> and
<https://aca-prod.accela.com/COLA/Cap/CapHome.aspx?module=Building&TabName=Building>.

### Lake Hamilton

- Rows and span: 2,087; 2005-2026.
- Issuer/custodian: Town of Lake Hamilton; Town Clerk maintains permits.
- Source: iWorQ.
- Blocker: reCAPTCHA and detail lookup requiring both permit number and
  contractor ID; retention is unknown.
- Potentially missing: all records not accessible without those credentials,
  plus non-CAMA permits.
- Access: CAPTCHA/human interaction or official native export.
- Safe concurrency: zero unattended.
- Status: partial. Next action: request the Town's native export rather than
  using the unrelated paid parcel lien-search process.

Official sources:
<https://townoflakehamilton.com/1205/Community-Development> and
<https://townoflakehamilton.portal.iworq.net/LAKEHAMILTON/permits/600>.

### Lake Wales

- Rows and span: 7,133; 1982-2026.
- Issuer/custodian: City of Lake Wales.
- Source: anonymous CitizenLink Public View.
- Blocker: permit/address lookup has no bulk or date-window list, and historical
  retention remains undocumented.
- Potentially missing: permits absent from CAMA and any records outside online
  retention.
- Access: no login for public view; official export may require staff/payment.
- Safe concurrency: one detail request with a 1,000 ms interval.
- Status: partial. Next action: request a native export. The exact-result
  adapter is suitable only for selective enrichment.

Official sources:
<https://www.lakewalesfl.gov/909/Contractor-Online-Portal> and
<https://secure.lakewalesfl.gov/permits/>.

### Lakeland

- CAMA rows/span: 40,202; observed dates include 1960-2026.
- Official ArcGIS rows/span: 107,901; 2014-current.
- Issuer/custodian: City of Lakeland.
- Sources: anonymous ArcGIS list/API and iMS detail search.
- Blocker: ArcGIS does not cover pre-2014 history or every detail field; older
  unavailable iMS records require a public-records request.
- Potentially missing: pre-2014 and non-retained records.
- Access: anonymous API/current iMS; records fulfillment may require payment.
- Safe concurrency: ArcGIS 4 in the bounded pilot; iMS detail concurrency 1
  with a 1,000 ms interval.
- Status: partial. The complete ArcGIS snapshot is ingested and strictly
  verified. Next action: request the pre-2014 export and use iMS only for
  missing detail evidence.

Official sources:
<https://services1.arcgis.com/mcbQY5xNGGGM1vBX/ArcGIS/rest/services/IMS_Projects_Permits/FeatureServer/6>
and <https://ims.lakelandgov.net/ims/Find3?cat=Permits>.

### Mulberry

- Rows and span: 1,720; 1921-2026, with early dates treated as suspect.
- Issuer/custodian: City of Mulberry.
- Source: MULBERRY Accela.
- Blocker: sampled CAMA identifiers did not map reliably to Accela; the default
  portal window is recent and no bulk export was found.
- Potentially missing: predecessor/old identifier families and non-CAMA
  permits.
- Access: anonymous current page; historical export requires City staff.
- Safe concurrency: zero until identifier mapping passes, then start at one.
- Status: partial. Next action: obtain identifier-family mapping and native
  export before adapter enablement.

Official sources:
<https://www.cityofmulberryfl.org/building-department> and
<https://aca-prod.accela.com/MULBERRY/Cap/CapHome.aspx?module=Building&TabName=Building>.

### Polk City

- Rows and span: 1,754; 2004-2026.
- Issuer: Polk City; portal custodian: Polk County Accela.
- Blocker: current records resolve in POLKCO, but historical/delegation
  boundaries and list totals are not certified.
- Potentially missing: old/numeric rows, older monthly records, and non-CAMA
  permits.
- Access: anonymous current portal; records fulfillment may require staff.
- Safe concurrency: one for detail pilots.
- Status: partial. Next action: reconcile City archive/native export against
  County records before sharing the adapter.

Official source: <https://www.mypolkcity.org/building>.

### Winter Haven

- Rows and span: 21,417; 1974-2026.
- Issuer/custodian: City of Winter Haven.
- Sources and official boundary: eSuite/ProjectDox for 2025-and-older permit
  history; COWH Accela for 2026+ applications/status/documents.
- Blocker: the existing adapter supports only 19,015
  `YYYY-NNNNNNNN` identifiers, has 344 permanent no-detail results, and does
  not cover `WH26-*` current records or every older identifier family. CAMA
  contains 682 `WH26-*` rows representing 680 distinct current identifiers.
- The bounded COWH pilot attempted both anonymous Building search and exact
  detail lookup for `WH26-AD-0001`. Both redirected to `Login.aspx`; the page
  states that a free Citizen Access account is required for secured services.
  The exact identifier appeared only in the encoded return URL, not as permit
  evidence.
- Potentially missing: 2,402 non-legacy-family CAMA rows, including the 682
  current rows; detail evidence for 344 legacy source misses; and any non-CAMA
  permits.
- Access: eSuite history is anonymous. COWH current search/detail requires a
  login, and no account credentials are authorized for this run. A native
  records export may require human fulfillment/payment.
- Safe concurrency: one eSuite detail request with a 1,000 ms interval.
- Status: partial. Next action: obtain an authorized COWH account or, preferably,
  a native 2026-current export plus a 2025-and-older native export. Reconcile
  the 344 misses against that evidence. Do not redrive them blindly.

Official sources:
<https://www.mywinterhaven.com/342/Building-Permits-Licenses>,
<https://myinspections.mywinterhaven.com/eSuite.Permits/AdvancedSearchPage/AdvancedSearch.aspx>,
and <https://aca-prod.accela.com/COWH>.

## Operator disposition for inaccessible sources

For this run, authorized automation stops at a login, CAPTCHA, Cloudflare
challenge, paid-record estimate, or staff-fulfilled export. It does not create
accounts, solve CAPTCHAs, submit public-records requests, or infer records from
identifier or geography.

- Auburndale, Bartow, Davenport, Frostproof, Haines City, Lake Hamilton, and
  Mulberry remain pending native custodian exports. No unattended portal
  harvest will be attempted.
- Dundee, Eagle Lake, Fort Meade, Highland Park, Hillcrest Heights, and Polk
  City remain split/delegated custody. Their CAMA rows stay preserved under the
  published agency value, but no additional municipal or County relationship
  is inferred.
- Lake Alfred and Lake Wales remain selective-current-detail sources, not
  historical denominators. Their native historical exports remain follow-up
  inputs.
- Lakeland's anonymous ArcGIS list is the preferred 2014-current source.
  Pre-2014 records stay explicitly partial pending a City export.
- Winter Haven eSuite remains the 2025-and-older selective source. Current
  COWH access and the 344 legacy misses stay explicit blockers pending the
  access evidence below and a native City export.

The resulting coverage status is `partial` or `unsupported` exactly as stated
per jurisdiction above. These source gaps do not authorize dropping CAMA rows:
all valid rows remain in the baseline with their source agency, including null
agency and Property Appraiser provenance rows.

## Non-jurisdiction rows and supplemental approvals

`POLK COUNTY PROPERTY APPRAISER` contributes 15,556 rows but is not a permit
issuer. Its observations remain provenance-bearing CAMA work records and must
not be presented as issuing-jurisdiction permits.

The 192,808 agency-null rows remain valid source rows. Preserve them with
unknown jurisdiction. Do not assign them from permit-number shape, property
location, or municipal polygons without explicit custodian evidence.

FDEP, SWFWMD, FDOT, USACE, health/septic, and similar sources issue separate
state/federal approvals. They are not substitutes for municipal building
permits and are outside this building-permit denominator.

## Runner fixes completed before any resume

- Versioned input fingerprints and compatibility validation prevent reusing
  parts with a changed candidate order, batch size, input, output, or state
  directory.
- Every contiguous committed part is structurally validated. Gaps, corrupt
  statuses, stale identities, and checkpoint claims ahead of disk state fail
  closed.
- Checkpoints are monotonic, recover committed parts after a prior counter
  rewind, and state whether the aggregate output is complete.
- Redrive maps only `fetch_error` records in parts that contain errors.
- Duplicate agency/permit requests share one result while each input row is
  retained.
- One source cannot have overlapping in-flight requests; unrelated sources may
  proceed independently.
- Every request has a configurable timeout. HTTP 404/exact-result misses become
  non-retryable `no_detail`; transient responses remain bounded retries.
- Municipal parsers cannot claim enrichment from an echoed requested ID.
  Winter Haven matches the exact result row; Lake Wales requires an exact AJAX
  suggestion; Accela details must match the requested permit.
- Invalid candidate lines fail before any part is written.
- Part, checkpoint, aggregate, and receipt writes are atomic; a per-state-dir
  run lock prevents concurrent writers.

## Resume gate

Overall decision is **GO WITH DOCUMENTED GAPS** for bulk/list ingestion,
publication, and selective detail enrichment:

1. Lakeland ArcGIS is fully harvested and verified for 2014-current. Pre-2014
   remains partial pending a City export.
2. Polk County Accela completed all 285 monthly windows and verified 491,291
   unique records. The result cap is bypassed through the session CSV export,
   predecessor identifiers are preserved, and Countywide detail scale is
   replaced by selective enrichment.
3. Winter Haven's 2026 COWH source was piloted and confirmed login-protected.
   Its 344 legacy misses remain preserved as `no_detail`, not blindly redriven,
   pending an authorized account or native export.
4. Every named jurisdiction has either bounded source evidence or an explicit
   official-export/manual-access blocker and operator disposition.
5. All 192,808 agency-null rows and 15,556 Property Appraiser pseudo-agency
   rows remain preserved and visibly unassigned.

The decision remains **STOP** for a countywide detail-page run or for any claim
of full historical jurisdiction coverage.

No countywide source should be labeled full until its official source count,
accessible record count, historical boundary, and committed output reconcile.
