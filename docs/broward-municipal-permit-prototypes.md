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

| Protocol                 | Jurisdictions                   |                              Live bounded result | Disposition                                                 |
| ------------------------ | ------------------------------- | -----------------------------------------------: | ----------------------------------------------------------- |
| Coconut Creek legacy ASP | Coconut Creek                   |       1 search page, 1 selected detail, 1 record | implemented                                                 |
| Click2Gov                | Margate, Pompano Beach, Tamarac |         each: 1 exact search, 1 detail, 1 record | implemented                                                 |
| Tyler/New World eSuite   | Dania Beach, Davie              |     each: 1 address page, 10 details, 10 records | implemented through persistent isolated headless Chromium   |
| SmartGov                 | Lighthouse Point                | 1 folio search, explicit empty result, 0 details | implemented; positive detail remains fixture-covered        |
| eGovPLUS                 | Lauderhill                      |               1 exact search, 1 detail, 1 record | implemented                                                 |
| OpenGov/ViewPoint        | Lauderdale Lakes                |                  0 requests, `landing_only` skip | blocked: official rendered application reports inaccessible |
| Gov-Easy / GeoCivix      | Deerfield Beach, Pembroke Park  |                                       0 requests | blocked by CAPTCHA or no anonymous current search           |
| CommunityCore            | Hillsboro Beach                 |                                       0 requests | blocked by reCAPTCHA                                        |
| MGO Connect              | Parkland                        |                                       0 requests | blocked by login                                            |
| Tyler EnerGov            | Sunrise                         |         shared Tyler adapter, tracked separately | implemented                                                 |

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
