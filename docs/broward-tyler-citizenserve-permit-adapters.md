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

| Jurisdiction          | Validated folio | Initial observation                                                                                                                                                                                       |
| --------------------- | --------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Pembroke Pines        | `513914101320`  | Exact Tyler global search returned 27 mixed-module entities over three reported pages; page 1 contained seven permit entities. One public permit detail reconciled the case ID, permit number, and folio. |
| Lauderdale-by-the-Sea | `494318013550`  | Exact Citizenserve parcel search returned 33 permits over two source pages for `218 E COMMERCIAL BLVD`. One direct public detail reconciled permit `LBS13-001986`.                                        |
| Miramar               | `514123070029`  | The appraisal sample provides the situs `PEMBROKE ROAD`; bounded adapter probe remains to be recorded.                                                                                                    |
| Southwest Ranches     | `504026140250`  | The appraisal sample provides the situs `GRIFFIN ROAD`; bounded adapter probe remains to be recorded.                                                                                                     |

The other jurisdictions have no municipality-matched parcel in the checked-in
25-folio validation list. No unrelated parcel was invented or looked up merely
to force a positive result.

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
