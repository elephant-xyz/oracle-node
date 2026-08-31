# Broward Tyler and Citizenserve local permit adapters

Date: 2026-08-29  
Scope: local-only, property-first source certification  
Vendors: Tyler EnerGov/Civic Access and Citizenserve/CAP Government

## Safety and coverage boundary

These adapters do not create a municipal permit harvest. Each invocation
requires exactly one explicit Broward folio or situs address, permits at most
three result pages and ten detail pages, runs serially, and writes only
owner-readable local artifacts. There is no seed-file mode, date-range mode,
AWS client, queue, database load, IPFS path, or publication path.

A Broward folio is exactly 12 undashed alphanumeric characters. It remains a
string end to end; letters are uppercased but never removed, padded, or
converted to a number. Address mode accepts a validated public situs value and
never substitutes an owner mailing address.

The normalized private record excludes owners, applicants, contacts,
assignees, email/phone fields, attachments, payments, and account data. It
retains the work location and description for local matching, so the JSONL is
not publication-approved.

## Implementation

- `scripts/permit-source-adapters/broward-permit-jurisdictions.mjs`
  contains the nine jurisdiction routes, official municipal evidence, portal
  URL, source key, history limitation, supported query fields, and available
  validated appraisal sample.
- `scripts/permit-source-adapters/tyler-civic-access.mjs` retains its existing
  Rock Island-compatible normalizer and adds a bounded detail-backed adapter.
  The rendered public search route bootstraps Tyler tenant context, and the
  adapter verifies the exact keyword, page number, ten-row page size, detail
  entity ID, permit number, and folio before emitting a record.
- `scripts/permit-source-adapters/citizenserve.mjs` uses the public
  Citizenserve form, parses the source-provided 30-row page ranges, follows only
  official permit-detail links, and reconciles list/detail fields. Installation
  `117` serves two towns; permit-type markers prevent one town's records from
  being emitted under the other's source key.
- `scripts/permit-source-adapters/bounded-permit-common.mjs` owns exact query
  validation, the closed normalized contract, conflict-detecting dedupe,
  deterministic JSONL, and atomic mode-0600 checkpoint state.
- `scripts/probe-broward-municipal-permits.mjs` is the only runner. It writes
  `checkpoint.json`, `records.private.jsonl`, and `summary.json` below the
  explicit local output directory.

Search pages are marked complete only after every in-scope detail candidate on
that page is captured. Each detail is checkpointed first. After interruption,
the source page can be read again while already completed detail identities are
skipped. Duplicate records must be byte-equivalent after normalization;
conflicting content for the same source identity fails closed.

Citizenserve currently invokes reCAPTCHA v3 from its ordinary public form
submission. The adapter lets the rendered official page execute that code. It
does not request, inject, replay, solve, or bypass a token. A visible challenge
or login form is a hard stop.

## Tyler vendor-wide optimization (2026-08-31)

`scripts/run-broward-tyler-date-windows.mjs` adds a separate list-first path
for the four anonymous Tyler tenants: Pembroke Pines, Hallandale Beach,
Miramar, and Oakland Park. The original property-first adapter and its bounds
remain available for detail recertification.

The optimized runner:

- bootstraps the public tenant once per invocation;
- clones the complete UI request model and switches it to advanced Permit
  search;
- uses explicit `ApplyDateFrom`/`ApplyDateTo` UTC timestamps;
- requests 100 records per page;
- reuses tenant cookies and required tenant headers;
- reconciles `TotalFound`, `TotalPages`, permit entities, and stable `CaseId`;
- applies both in-page and outer wall-clock request timeouts;
- writes private raw JSON pages and deterministic normalized list JSONL; and
- checkpoints after each complete application-date window.

Two-day live pilots reconciled Pembroke Pines 30, Hallandale Beach 10, Miramar
34, and Oakland Park 9 permits. Full runs use 30-day windows after year-wide
historical probes proved too expensive/unbounded. Oakland Park begins at its
documented `2019-11-01` Tyler boundary. Pembroke Pines begins at the City's
documented 1992 records-request boundary but still does not claim portal
completeness. Hallandale's migrated start remains unknown. Miramar restarts
from 2019, with its separate FY2019/FY2020 official ArcGIS archives retained
as reconciliation evidence.

Completed list inventories load through
`scripts/load-broward-permit-list-to-neon.mjs`. Tyler `CaseId` matches the
existing detail-loader key, so later detail enrichment updates the same row.
Exact 12-character folios link immediately; other permits remain valid
unlinked rows. Loads use the shared permit writer lock and durable 1,000-row
Neon chunks.

Oakland Park completed its documented post-2019 Tyler inventory with 28,946
accessible unique permits and one row reported by `TotalFound` but unavailable
at every supported page size. The missing source row is recorded explicitly.
The 28,946 accessible rows loaded to isolated Neon in 29 durable chunks:
22,991 matched exact Broward folios and 5,955 remain valid unlinked permits.
Tyler retries rebuild the complete tenant browser session after timeout or
HTTP 401 instead of retrying inside expired state.

## Jurisdiction matrix

| Jurisdiction          | Adapter/source                                 | Anonymous record status   | Boundary                                                                                                                                 |
| --------------------- | ---------------------------------------------- | ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| Pembroke Pines        | Tyler Civic Access, City Development HUB       | Enabled                   | Portal completeness is not inferred; the City separately documents a 1992-current records-search request.                                |
| Hallandale Beach      | Tyler EnerGov/Civic Access                     | Enabled from official FAQ | The City explicitly documents anonymous permit/parcel/address global search. Earliest migrated history is unknown.                       |
| Miramar               | Tyler EnerGov/Civic Access                     | Enabled                   | Public-record search is separate from authenticated project management.                                                                  |
| Oakland Park          | Tyler EnerGov/Civic Access                     | Enabled                   | Tyler is post-2019 only. The City directs records before 2019-11-01 to legacy searches/public records.                                   |
| North Lauderdale      | Tyler Enterprise Permitting & Licensing CSS    | **Skipped**               | The official City page says login is required. The runner rejects this jurisdiction before opening Chrome and never accepts credentials. |
| Lauderdale-by-the-Sea | Citizenserve installation 117 / CAP Government | Enabled                   | Current town source only. Historical BCS-held records remain separate county provenance.                                                 |
| Southwest Ranches     | Citizenserve installation 117 / CAP Government | Enabled                   | Building permits only; Town zoning/engineering and external approvals are separate processes.                                            |
| West Park             | Citizenserve installation 261 / CAP Government | Enabled                   | Public search/detail only; account-required application submission is not used.                                                          |
| Wilton Manors         | Citizenserve installation 125                  | Enabled                   | Records/files unavailable in the portal require the City's official records route.                                                       |

“Enabled” means the adapter has an official anonymous search configuration. It
does not certify all-history completeness or permit a full crawl.

## Initial official-source observations

Only existing Broward appraisal validation folios were used:

| Jurisdiction          | Validated folio | Initial observation                                                                                                                                                                                                                                                          |
| --------------------- | --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Pembroke Pines        | `513914101320`  | The final one-page/one-detail runner reported 27 mixed-module results over three pages; page 1 had seven permits and normalized one reconciled detail (`22-08581`). Source-spelled address `470 SW 198 TER` reported 26 mixed-module results and also normalized one detail. |
| Lauderdale-by-the-Sea | `494318013550`  | The final one-page/one-detail runner reported 33 permits over two pages and normalized one reconciled detail (`LBS13-001986`). Source-spelled address `218 E COMMERCIAL BLVD` produced the same 33-record total and one bounded detail.                                      |
| Miramar               | `514123070029`  | The exact folio returned HTTP 200 with a typed empty Tyler result (`0` entities, `0` pages). That is a successful source observation, not proof that the right-of-way parcel has no records outside this portal.                                                             |
| Southwest Ranches     | `504026140250`  | The exact folio returned Citizenserve's explicit `No records found` result (`0` rows, `0` pages). This agricultural parcel is not treated as a positive permit control.                                                                                                      |

The other jurisdictions have no municipality-matched parcel in the checked-in
25-folio validation list. No unrelated parcel was invented or looked up merely
to force a positive result.

Both vendors matched source abbreviations (`TER`, `BLVD`) but returned zero for
the otherwise identical appraisal suffixes (`TERRACE`, `BOULEVARD`). The
adapter deliberately does not issue automatic variant searches: an operator
may submit a source-spelled validated situs address, while folio remains the
preferred stable lookup. A zero-result address variant is never promoted to a
no-permits completeness claim.

Every final live run used `--max-pages 1 --max-details 1`. The positive searches
therefore report both `paginationTruncated` and `detailsTruncated`; those flags
are expected proof that the probe stopped at its approved ceiling, not an
all-record result.

## Local invocation

```bash
CHROME_EXECUTABLE_PATH=/usr/local/bin/google-chrome \
node scripts/probe-broward-municipal-permits.mjs \
  --jurisdiction pembroke_pines \
  --folio 513914101320 \
  --output-dir downloads/broward/permit-probes/pembroke-pines \
  --max-pages 1 \
  --max-details 1
```

The one-page/one-detail values above are appropriate for source
recertification. Raising them remains bounded by hard maxima of three and ten;
it does not authorize bulk use.
