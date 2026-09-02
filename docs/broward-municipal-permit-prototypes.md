# Broward municipal permit vendor-family adapters

Date: 2026-09-01
Scope: bounded local anonymous transports; no cloud harvest, database write,
publication, login, CAPTCHA solution, registration, or records request

## Implemented contract

The implementation separates jurisdiction routing, vendor parsing, transport,
and durable local execution:

- `broward-municipal-config.mjs` records 14 source configurations, official
  evidence, current/legacy boundaries, capabilities, and executable access
  dispositions.
- `broward-municipal-protocols.mjs` normalizes Coconut Creek legacy ASP,
  Click2Gov, Tyler/New World eSuite, SmartGov, and eGovPLUS responses. OpenGov
  GraphQL normalization remains fixture-only while the live app is unhealthy.
- `broward-municipal-transport.mjs` implements same-origin cookie sessions,
  rotating ASP/CSRF state, strict redirects and response sizes, direct HTML
  pagination where certified, and persistent headless Chromium only for
  eSuite's JavaScript autocomplete and ASP.NET postbacks.
- `broward-municipal-core.mjs` owns shared safety invariants: exact string
  queries, alphanumeric folios, serialized requests, hard query/page/result/
  detail limits, identity reconciliation, deterministic dedupe, and resumable
  checkpoints.
- `run-broward-municipal-permit-pilot.mjs` writes owner-only records before
  advancing their checkpoint identities. Summaries contain aggregate counts
  and query kinds/digests, never query values or source rows.

Default ceilings are one CLI query, three result pages, 25 unique references,
three details, at least 1,250 ms between operations, a 30-second request
deadline, a 2 MB response limit, and an exclusive 50-row raw HTML ceiling.
Absolute core ceilings remain three queries, six pages, 50 references, and ten
details.

## Route disposition and bounded pilots

Pilots used existing private appraisal evidence. The table contains aggregate
results only.

| Protocol                 | Jurisdictions                   |                              Live bounded result | Disposition                                                  |
| ------------------------ | ------------------------------- | -----------------------------------------------: | ------------------------------------------------------------ |
| Coconut Creek legacy ASP | Coconut Creek                   |       1 search page, 1 selected detail, 1 record | implemented                                                  |
| Click2Gov                | Margate, Pompano Beach, Tamarac |         each: 1 exact search, 1 detail, 1 record | implemented                                                  |
| Tyler/New World eSuite   | Dania Beach, Davie              |     each: 1 address page, 10 details, 10 records | implemented through persistent isolated headless Chromium    |
| SmartGov                 | Lighthouse Point                | 1 folio search, explicit empty result, 0 details | implemented; positive detail remains fixture-covered         |
| eGovPLUS                 | Lauderhill                      |               1 exact search, 1 detail, 1 record | implemented                                                  |
| OpenGov/ViewPoint        | Lauderdale Lakes                |                  0 requests, `landing_only` skip | blocked: official rendered application reports inaccessible  |
| Gov-Easy / GeoCivix      | Deerfield Beach, Pembroke Park  | Pembroke manual session: 17 pages, 166 list rows | CAPTCHA remains required; unattended transport stays blocked |
| CommunityCore            | Hillsboro Beach                 |                                       0 requests | blocked by reCAPTCHA                                         |
| MGO Connect              | Parkland                        |                                       0 requests | blocked by login                                             |
| Tyler EnerGov            | Sunrise                         |         shared Tyler adapter, tracked separately | implemented                                                  |

Davie eSuite remains an explicitly bounded public-history/status route. The
separate 2026 OAS submission system is login-gated, so eSuite results do not
establish complete new-submission coverage.

Click2Gov exposes segmented parcel fields, but no certified conversion from a
12-character BCPA folio to those tenant-specific segments was found. The
adapter therefore supports exact permit-number and address searches only.

## Privacy and normalization

Every normalized private record retains source system/protocol, official
search and token-free detail URL, stable vendor identity, query kind, source
parcel display, dates/status/type/description/value where present, bounded
inspection outcomes, and conservative roofing classification.

Owner, applicant, contact, personal phone/email, reviewer, inspector,
free-form inspection comments, and payment/fee details are omitted.
Locations and descriptions remain private staging fields.

Roofing is true only when permit type or project description contains the
standalone word `roof` or `roofing`; contractor identity and generic
construction language do not classify a permit as roofing.

## Checkpoint and reconciliation behavior

Checkpoints contain a SHA-256 digest of normalized queries rather than permit
numbers, addresses, or folios. They record jurisdiction, next query/page or
opaque cursor, seen source identities, captured detail identities, and
completion.

The local runner atomically persists a normalized record before checkpointing
that identity. A crash between those writes safely refetches the detail and
accepts only an exact duplicate. Result pages advance only after all permitted
details finish. Conflicting identities, repeated/regressing pages, malformed
cursors, source-row ceilings, cross-origin links, and identity mismatches fail
closed.

Click2Gov, Coconut Creek, and eGovPLUS return client-all result pages; the raw
row ceiling is exclusive so a page at the limit is never claimed complete.
eSuite retains its persistent result page while details open in same-context
pages, allowing numbered ASP.NET postbacks without losing session state.
SmartGov follows only same-origin direct page links and fails if pagination
requires an uncertified script postback.

## Production enumeration boundaries

Two production runners now reuse the certified vendor parsers/transports:

- `run-broward-municipal-record-type-enumeration.mjs` snapshots the complete
  official eSuite or SmartGov type selector, partitions by exact option value,
  requires ten records on every non-terminal page, reconciles SmartGov's
  reported total, verifies replayed pages on resume, and rejects overlap across
  partitions. Duplicate eSuite labels remain separate because their source
  option IDs are distinct. Historical eSuite rows with no issued permit number
  retain the matching public application number and an explicit
  `permit_application` provenance kind; the stable numeric detail ID remains
  the record key and mismatched application/list identities fail closed.
- `build-broward-municipal-property-seed.mjs` derives jurisdiction only from
  the executable BCPA situs registry. It produces exact folio queries for
  Coconut Creek/Lauderhill and deduplicated normalized base-address queries for
  Margate/Pompano Beach/Tamarac. Any target property whose address cannot be
  represented is written to a separate mode-0600 gap ledger by canonical
  property identifier and reason, without retaining the rejected address.
- `run-broward-municipal-property-enumeration.mjs` consumes that immutable
  private seed, requires each client-all page to stay below its exclusive cap,
  captures every detail before advancing the query checkpoint, and preserves
  source-cap/time-out/pagination blockers without treating them as empty. An
  address at or above the cap advances only into a separate unresolved state:
  the mode-0600 deferred-cap ledger stores plan/query hashes, aggregate counts,
  reason, and bounded attempt timing but no address. Terminal progress excludes
  those items. The runner continues the primary seed, then permits at most
  three due deferred retries per later invocation, at 24-hour intervals and
  no more than three observations per item.

The live Pompano Beach and Tamarac address forms expose no permit-type, status,
application-year, or date-range filter that can partition a capped address.
They expose segmented parcel controls, but no BCPA-to-vendor segment mapping
has been certified and the controls cannot be assumed to partition the same
address result set. Capped items therefore remain deferred; no wildcard,
permit-number-prefix, or parcel split is attempted.

All three scripts write mode-0600 files below ignored owner-only directories.
Their console summaries and dashboard projections contain aggregate counts and
allowlisted blocker states only.

| Jurisdiction     | Deterministic boundary                                                                                            | Full-worker gate                                                                                           |
| ---------------- | ----------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| Coconut Creek    | Every exact folio in the reconciled current BCPA Coconut Creek property seed                                      | Seed has zero target-query omissions and a small terminal folio pilot                                      |
| Dania Beach      | Every non-placeholder exact option ID in the official eSuite permit-type selector                                 | Sequential paging reconciles; application-only rows preserve their vendor ID and explicit application kind |
| Davie            | Every legacy eSuite exact type; login-gated 2026 OAS submissions remain excluded                                  | Same as Dania; output retains the explicit OAS boundary                                                    |
| Lauderhill       | Every exact folio in the reconciled current BCPA Lauderhill property seed                                         | Seed has zero target-query omissions and a small terminal folio pilot                                      |
| Lighthouse Point | Every exact value in the official SmartGov type selector                                                          | Positive live list/detail identity plus reported-total/page reconciliation                                 |
| Margate          | 13,535 shared-address queries representing 15,059 properties; 1,450 properties remain in the private gap ledger   | Representable capture may run; full coverage remains partial until the ledger/custodian gap is resolved    |
| Pompano Beach    | 23,900 shared-address queries; 2,961 properties remain in the private gap ledger; exclusive cap stays fail-closed | A positive 5-row address query and all details reconciled below the exclusive 100-row cap                  |
| Sunrise          | EnerGov application-date windows from 1900-01-01 through the run end date                                         | One-day window reconciles; microfilm/records absent from EnerGov remain custodian-only                     |
| Tamarac          | 18,362 shared-address queries representing 19,800 properties; 1,378 properties remain in the private gap ledger   | Representable capture may run; full coverage remains partial until the ledger/custodian gap is resolved    |

The recovery dashboard includes fixed rows for all nine routes. Missing
checkpoints render as no-start with the exact gate above; recent checkpoints
render running, future retry deadlines render cooling down, and only exhausted
query/partition/window denominators render complete.

### eSuite legacy identifier pagination

Dania Beach and Davie initially paused as `incomplete_pagination` even though
the live eSuite grids displayed ten rows and a next-page control. Structural
inspection found no reported total, missing detail links, blank permit cells,
or overlap with preceding pages. The parser had discarded printable linked
identifiers outside its former alphanumeric-and-hyphen pattern: three of ten
Dania page-six rows used punctuation/spacing and two of ten Davie page-one rows
used spaces. Each rejected row had a distinct numeric detail URL ID, a matching
hidden detail ID, and a detail permit/application identifier equal to the list
anchor.

The parser now accepts only bounded printable identifiers containing at least
one alphanumeric character and still requires exact detail identity. Blank,
control-character, conflicting, or inaccessible identities fail closed. A
selected-type replay reconciled Dania's blocked type to 100 unique records over
ten terminal pages and Davie's blocked type to 16 over two terminal pages, with
zero page overlap or missing rows. No page-size change or synthetic subdivision
was needed. Davie's login-gated 2026 OAS boundary remains excluded.

## Remaining software blocker

Lauderdale Lakes OpenGov remains the only actionable route not promoted. The
official landing returns HTTP 200, but its own rendered fallback still says
the permitting application is inaccessible. The fixture parser and cursor
checkpoint contract remain available, while `probeStatus: landing_only`
prevents GraphQL transport construction or requests.

The seven policy/source barriers remain unchanged and fail closed: Coral
Springs, Hillsboro Beach, and Pembroke Park require CAPTCHA; North Lauderdale
and Parkland require login; Deerfield has no anonymous current search and a
CAPTCHA-protected historical route; Sea Ranch Lakes is custodian-only, with
BCS evidence supplemental rather than proof of municipal completeness.

## Pembroke Park manually validated list evidence

A user-authorized browser session with a manually completed Gov-Easy CAPTCHA
was reused in place without refreshing the page or reading, solving, or
persisting CAPTCHA/session material. The `Job Name=ROOF` search reported 166
results across 17 pages. All 17 pages reconciled to 166 list rows and 166 stable
application identities, with zero duplicates, conflicting identities, invalid
rows, or missing permit-number/status/location/job-name fields.

The allow-listed list capture omits owner and contractor names and does not
collect contact, payment, or PDF data. The existing standalone-word roofing
rule classifies 149 of the 166 rows; the remaining 17 stay in the keyword slice
without being relabeled roofing. No detail pages were required for stable list
identity, status, and work location.

This evidence does not change the source policy. Gov-Easy remains
`captcha_required` and blocked for unattended transport. The captured slice is
only the records returned by that exact keyword search; it is not proof of all
Pembroke Park roofing permits, all Pembroke Park permits, historical
completeness, or anonymous access.

## Coral Springs manually authorized capped list evidence

A user-authorized, already-open Chrome tab with a manually completed eTRAKiT
reCAPTCHA is attached in place. The adapter never launches or refreshes a
browser, reads or changes cookies/tokens, or copies the CAPTCHA response. It
allow-lists only list `RECORDID`, permit number/type/status, site address, and
folio; owner and contractor columns, contacts, details, fees, and PDFs are
excluded.

The approved `Permit Type` `Contains` `ROOF` search reports 59,379 matches, but
the Telerik grid exposes only 50 pages of 20 rows (1,000). The exact deployed
contract is an ASP.NET POST with `__VIEWSTATE`, no `__EVENTVALIDATION` input,
and grid command
`__doPostBack('ctl00$cplMain$rgSearchRslts',
'FireCommand:ctl00$cplMain$rgSearchRslts$ctl00;Page;…')`. The capture invokes
the rendered next-page control sequentially, waits at least six seconds between
postbacks, and atomically checkpoints each reconciled page before consuming the
next clipboard envelope. CAPTCHA/session material is never persisted.

No exhaustive partition has been proved:

- The live search-field taxonomy is permit number, site address, permit type,
  owner, contractor, folio, and status. There is no applied, issued, final, or
  other date input; this is direct form evidence, not an inference from result
  columns.
- Only one field/operator/value criterion is accepted per search, so a roofing
  condition cannot be intersected with a permit-number year/range or folio.
- Permit type is free text rather than a complete enumerable taxonomy.
  Exact-type values observed in the capped slice cannot prove that no other
  roofing type exists beyond the cap.
- The page exposes an Excel action, but its all-results/cap semantics are not
  certified and the grid includes owner/contractor columns. It is not invoked
  or treated as a safe bulk contract.
- The permit-number namespace and historical start are not certified.
  Prefix/year partitions would therefore have unproved gaps.

Consequently, even a fully reconciled 1,000-row capture is labeled
`bounded_capped_keyword_slice`, while the executable registry remains
`captcha_required`. The gap plan is (1) property-first exact-folio retrieval
for the finite Coral Springs appraisal property set during future manually
authorized sessions, with missing/duplicate/conflict receipts and no owner
fields; and (2) a public-records bulk request for a non-personal permit export
limited to stable record ID, permit number/type/status, lifecycle dates, site
address, and folio. Property-first coverage is useful for parcel linkage but
cannot by itself prove permits with absent or bad folios; only a reconciled
custodian export can close that residual gap. No request is submitted by this
implementation.
