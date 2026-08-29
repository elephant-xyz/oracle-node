# Broward municipal permit vendor-family prototypes

Date: 2026-08-29  
Scope: local-only source-contract prototypes; no harvest, cloud service, database,
publication, login, CAPTCHA solution, or records request

## What is implemented

The implementation separates jurisdiction routing from reusable protocol logic:

- `broward-municipal-config.mjs` holds 13 city configurations, official
  evidence, current/legacy split routes, capabilities, and executable access
  dispositions.
- `broward-municipal-protocols.mjs` parses bounded Click2Gov, Tyler/New World
  eSuite, SmartGov, eGovPLUS, and fixture-only OpenGov search/detail responses.
- `broward-municipal-core.mjs` owns the cross-vendor safety contract: exact
  string queries, alphanumeric folios, serialized requests, hard query/page/
  result/detail limits, deterministic dedupe, identity reconciliation, and
  resumable checkpoints.

These are protocol prototypes, not a full-harvest command. A future transport
must supply one parsed result page and one parsed detail at a time to
`runBoundedMunicipalCapture`. The runner refuses access-controlled
jurisdictions before invoking either callback.

Default hard ceilings are three queries, three result pages, 25 unique
references, five details, and at least 1,250 ms between source requests.
Absolute code ceilings are three queries, six result pages, 50 references, ten
details, and at least 1,000 ms. The tests inject a no-wait clock; production
callers cannot lower the configured delay.

## Jurisdiction routing

| Protocol | Jurisdictions | Prototype capability | Operational disposition |
| --- | --- | --- | --- |
| Click2Gov | Pompano Beach, Tamarac, Margate | application/address/parcel result parsing; contact-expanded row dedupe; session-token removal; same-session detail parsing | bounded anonymous transport may be added |
| Tyler/New World eSuite | Davie, Dania Beach | permit/address result parsing; numeric permit-id dedupe; numbered-page checkpoint; same-session detail and inspections | bounded anonymous transport may be added |
| Gov-Easy | Deerfield Beach legacy, Pembroke Park | route/capability metadata only | skip: six-digit numeric CAPTCHA |
| GeoCivix | Deerfield Beach current records | split-route metadata | skip: secure/login route |
| SmartGov | Lighthouse Point | advanced-search result, numbered page, public detail, and inspection parsing | anonymous prototype; positive detail still needs certification |
| OpenGov/ViewPoint | Lauderdale Lakes | GraphQL edge/detail fixture parser and opaque-cursor checkpoint support | landing-only skip while the rendered application reports itself inaccessible |
| CommunityCore | Hillsboro Beach | official route metadata | skip: account required |
| MGO Connect | Parkland | official route metadata | skip: account required |
| eGovPLUS | Lauderhill | permit/folio/address results; client-all result contract; public detail and inspection parsing | bounded anonymous transport may be added |
| Official records request | Sunrise | microfilm/building-record route, official request form, and city-clerk fallback | skip: do not send a request |

Davie eSuite is explicitly legacy/public history, not complete 2026 coverage.
The config retains the city's separate Avolve OAS submission route as
login-required supplemental provenance. Deerfield similarly retains both
legacy Gov-Easy and current GeoCivix instead of silently treating either as the
whole history.

## Folio and provenance contract

BCPA folios remain strings. Display dashes/spaces may be removed and letters
uppercased for a submitted 12-character folio, but letters are never stripped,
values are never parsed as numbers, and missing characters are never padded.
For example, `504108-bj-0140` becomes `504108BJ0140`.

Vendor parcel displays are a separate field and are preserved as source text.
This matters because Pompano Click2Gov returned
`9202-06-37-1T1000100HALL`, which is neither a numeric field nor a certified
BCPA folio conversion. The implementation does not invent a crosswalk.

Every normalized row retains:

- jurisdiction-level source system and reusable protocol;
- official search and token-free detail URL;
- stable vendor record id and reconciled permit/application number;
- source parcel display and separately submitted BCPA folio, if any;
- source page and query kind;
- permit dates/status/type, project location/description/value, and bounded
  inspection summaries where exposed.

Owner, applicant, primary-contact, personal email/phone, reviewer, inspector,
free-form inspection comments, and fee-detail fields are deliberately omitted.
Locations and descriptions remain private-staging data and are not approved
for publication.

## Checkpoint and pagination behavior

Checkpoints contain a SHA-256 digest of normalized queries rather than raw
addresses. They record jurisdiction, next query, next numbered page or opaque
cursor, all seen source identities, all captured detail identities, and
completion.

A result page is not advanced until all of its permitted details complete. If
a process stops during details, resume refetches that one page and skips the
already captured identities. Overlap across pages or queries is deduplicated.
Conflicting detail identities or conflicting normalized records fail closed.
Malformed, regressing, repeated, or overlong cursors also fail.

Click2Gov and eGovPLUS currently expose all matching rows in one client-side
page; their raw row caps apply before dedupe. eSuite and SmartGov retain
numbered next-page state. OpenGov fixture support retains the public opaque end
cursor, but no Lauderdale Lakes GraphQL request is currently allowed.

## Bounded official-site observations

Only sources already documented as anonymous were searched. All requests were
serialized and no source response was saved as a private artifact.

### Click2Gov

The Pompano Beach, Tamarac, and Margate official portal landings returned HTTP
200 and exposed the same four search modes: application number, address,
segmented parcel, and name. The form uses a session CSRF token, and permit
detail remains in that anonymous session.

One Pompano city-hall address query (`100 W ATLANTIC`) produced a 1.1 MB
contact-expanded page with more than the prototype's 50-row ceiling. No bulk
detail traversal followed. One exact application already visible in that
bounded result, `99-00007758`, returned one detail:

- source parcel display `9202-06-37-1T1000100HALL`;
- application date `1999-12-27`;
- type `MECHANICAL-STAND ALONE-97`;
- status `CLOSED`;
- valuation `$8,500`.

The fixture uses synthetic values but preserves the observed HTML contract.
No Tamarac or Margate record search was run after their shared protocol landing
was confirmed.

### Tyler/New World eSuite

Davie and Dania Beach landings returned HTTP 200 with public permit type,
permit number, and service-address fields. Contractor login is a separate
control and was not used.

Davie's official address autocomplete returned eight candidates for prefix
`8800`. Selecting the exact public candidate
`8800 SW 36 ST BLD A, DAVIE, FL 33328` returned nine permit references. One
same-session detail, record id `400068` / permit `2026-00004503`, was captured:

- parcel `504129010010`;
- status `Permit Issued` on `2026-06-23`;
- type `E-Fire Alarm`;
- description `INSTALL FIRE ALARM COMMUNICATOR`;
- estimated improvement value `$300`;
- three requestable and four completed inspection rows on the detail page.

Only the allow-listed summary above was retained. Contact, owner, comments,
email, phone, and reviewer content displayed by the page were not retained. No
Dania Beach record search was run.

### Gov-Easy / GeoCivix

Both Gov-Easy client landings returned HTTP 200:

- Deerfield Beach client `dce877e0-e162-4827-a60d-7249ec4e8fe2`;
- Pembroke Park client `d60f9827-2c53-44a4-9037-31e1de2b3f09`, linked by the
  town's official online-permitting page.

Both load `numeric-captcha.js`. The public client generates and validates a
six-digit canvas CAPTCHA before allowing search. No CAPTCHA was solved, no
session flag was set, and no Gov-Easy data endpoint was called.

Deerfield's current GeoCivix URL is `/secure/`; it was recorded as the current
side of the 2025 split and not opened as an anonymous record source.

### SmartGov

Lighthouse Point's landing and advanced search returned HTTP 200. The public
advanced form exposes permits/licenses, application number, type/status/date,
site address, parcel, contact/contractor, and project fields. The prototype
allows only permit number, address, and folio; it does not query people.

One exact city-hall address search (`2200 NE 38TH ST`) passed SmartGov's normal
form validation and returned the source's explicit `No results found`. No
detail request followed. This certifies anonymous search and empty-result
behavior, not a positive Lighthouse Point record.

### OpenGov

Lauderdale Lakes' official OpenGov landing returned HTTP 200, but its own
rendered fallback says the permitting and licensing application is currently
inaccessible. No GraphQL request was made. The checked-in GraphQL fixtures
exercise cursor/detail normalization only; `probeStatus: landing_only`
prevents a live transport call.

### eGovPLUS

Lauderhill's official HTTP-only landing returned HTTP 200 and exposed permit,
permit type, folio, street number/name, and status fields. Its page's example
number returned the explicit `No matching records found`.

One city-hall address query (`5581 W OAKLAND PARK`) returned 56 rows, over the
50-row prototype ceiling, so no broad detail traversal followed. One exact
record from that page, permit `26020017`, returned one detail:

- folio `494123090020`;
- status `Open`;
- type `TNC`;
- application date `2026-02-02`;
- issue date `2026-02-17`;
- applied value `1200`;
- three listed final-inspection requirements.

Owner, applicant, contractor contacts, reviewers, inspectors, notes, and fees
were not retained.

### Explicit no-network routes

No record request or authenticated/challenged source request was attempted:

- Hillsboro Beach CommunityCore: account required;
- Parkland MGO Connect: free account required;
- Deerfield GeoCivix: secure/login route;
- Deerfield and Pembroke Park Gov-Easy: CAPTCHA;
- Sunrise: building records are primarily microfilm and the official page
  directs open-permit/public-record inquiries to
  `BuildingRecords@sunrisefl.gov`.

Sunrise's official building-record page returned the already documented access
denial from this environment. The configuration retains the official 2025
building-record request form and city-clerk custodian page but never sends
email or submits a form.

## Remaining blockers

1. Click2Gov's segmented parcel labels and its source parcel display do not
   establish a BCPA 12-character conversion. Do not auto-split a BCPA folio
   until a city publishes the mapping.
2. eSuite result postbacks and detail URLs are session-bound. A production
   transport needs tenant-specific autocomplete and numbered-postback
   certification before expansion beyond explicit probes.
3. Gov-Easy is blocked by CAPTCHA, and Deerfield's current GeoCivix side is
   login-gated. Completeness requires a custodian-supplied route or records
   request.
4. SmartGov still needs one known positive official Lighthouse Point example
   to certify the detail parser against live markup.
5. OpenGov must remain landing-only until the official Lauderdale Lakes search
   renders normally and an anonymous GraphQL contract can be observed without
   a challenge.
6. CommunityCore and MGO Connect have no certified anonymous record-level
   access.
7. Sunrise is a human records-request workflow with potential retrieval/open-
   permit fees, not an unattended permit endpoint.

No AWS credentials, queues, databases, appraisal ingestion, IPFS, or
publication paths are imported or used by these prototypes.
