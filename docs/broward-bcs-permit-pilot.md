# Broward BCS POSSE property-first permit pilot

Date: 2026-08-28  
Source: Broward County Building Code Services (BCS), Computronix POSSE  
Execution scope: local-only, bounded, anonymous public search

## Custody and coverage boundary

The adapter targets the official
[BCS Parcel/Address search](https://dpepp.broward.org/BCS/Default.aspx?PossePresentation=ParcelSearchByAddress).
It does **not** treat that portal as a countywide municipal permit source.

The official
[Building Code page](https://www.broward.org/Building/Government2Government/Pages/CurrentServiceAgreements.aspx)
states that Broward Building Code regulates the Broward Municipal Services
District (BMSD/unincorporated), the airport, and services for cities under
contract. An official 2022 County Auditor report documents 19 then-current
contract cities, but contracts and municipal custodians change. The BCS search
form's city dropdown is therefore not evidence that BCS is the current permit
custodian for every listed city.

The adapter accepts only parcel records and detail pages actually returned by
BCS. Every row retains the jurisdiction printed on its official detail page.
This supports BMSD/unincorporated records and historical/current contract-city
records held by BCS without making a broader coverage claim. A no-result for a
municipal parcel is not evidence that the parcel has no municipal permits; the
city source remains necessary.

## Bounded validation set

`--pilot` uses exactly five permit-priority folios already present in the
Broward appraisal pilot or 50-parcel acceptance sample:

| Parcel ID     | Appraisal validation usage |
| ------------- | -------------------------- |
| `474135010090` | Commercial                 |
| `494209060010` | Warehouse                  |
| `494318013550` | Commercial                 |
| `474236140090` | OfficeBuilding             |
| `474236140080` | LightManufacturing         |

The adapter enters these values in POSSE's **Parcel ID** field, not its legacy
folio field. Input is exactly 12 alphanumeric characters. Letters are
uppercased but never stripped, padded, dashed, or converted to a number.
Separate live validation with appraisal folio `504108BJ0140` resolved to BCS
parcel object `401961` and returned the explicit valid-parcel/no-permits page,
confirming that the browser submission retains alphanumeric IDs end to end.

Run:

```bash
node scripts/probe-broward-bcs-permits.mjs \
  --pilot \
  --output downloads/broward/bcs-permit-pilot.private.jsonl \
  --summary downloads/broward/bcs-permit-pilot.summary.json
```

Both files are local mode-0600 artifacts. The command imports no AWS client and
does not enqueue, publish, load a database, or start a harvest.

## Source contract and normalized detail

One anonymous browser session is required because POSSE builds encrypted form
state in JavaScript and requires a session cookie. For each parcel the adapter:

1. loads the official search page;
2. writes the exact parcel ID to `ParcelID_23473057_S0`;
3. dispatches POSSE's field-change handler and verifies the unchanged value is
   present in `datachanges`;
4. submits the official Search function;
5. requires an official `ParcelPermitList` URL with a numeric parcel object ID;
6. parses all list rows and counts master applications, permits, and excluded
   plan reviews;
7. fetches each master/permit detail page sequentially in the same anonymous
   session; and
8. reconciles permit number, type, and the displayed legacy folio against the
   submitted parcel before emitting a row.

Normalized local-private rows retain:

- search, parcel-list, detail, and inspection URLs plus POSSE object IDs;
- exact submitted parcel ID, BCS legacy folio, and printed jurisdiction;
- master/permit kind, permit number, type, status, issue/application/expiration
  dates;
- title/description, situs address, legal description, contractor and license;
- building/present/proposed use, value, area, occupancy/construction fields,
  occupant load, and floor-elevation fields where exposed; and
- inspection type, requested/completed dates, result, and detail URL.

Owner name and owner mailing address are deliberately omitted. Contractor,
address, legal-description, and description fields still make the output
private staging; it is not approved for publication.

Plan-review rows are counted in lookup provenance but excluded from normalized
permit JSONL. Their list presence is not silently treated as a separately
issued permit. Inspection detail links are preserved, but the pilot does not
traverse them because the parent permit page already exposes the bounded
inspection history needed here.

## Empty, no-match, and failure behavior

BCS has two materially different zero-result responses:

- A resolved `ParcelPermitList` with **“No permits were found for this
  address.”** is a successful `no_permits` observation. It retains the search
  URL, resolved list URL, submitted parcel ID, and numeric parcel object ID and
  emits no JSONL rows.
- A search response with **“There are no permits that match your criteria.”**
  does not establish a parcel object. The adapter throws and exits nonzero.

Zero rows without the exact valid-parcel marker, mixed rows plus the marker,
unknown row kinds, malformed dates/numbers, missing required detail fields,
source links outside official BCS, identity mismatches, source errors, and
unexpected page titles all fail closed. BCS's literal `mmm dd, yyyy` issue-date
placeholder is normalized to `null`; it is not parsed or invented.

## Access and load limits

Hard limits are intentionally below harvest scale:

- at most **5** unique parcels per process;
- one browser search at a time with at least **1,000 ms** between properties;
- at most **125** list rows per property;
- at most **75** master/permit detail requests per property;
- one detail request at a time with at least **250 ms** between details;
- at most **2 MB** of HTML per detail response; and
- no retries, parallel source requests, login, CAPTCHA handling, or security
  control bypass.

The public search was anonymously reachable from this environment. The page
declares JavaScript and session cookies mandatory. Several nonessential legacy
assets encounter redirect loops and conflicting frame headers, but the POSSE
form and official result/detail pages worked. Broward's official Building Code
page also warns that ePermits may be unavailable during a website upgrade, so
an unexpected response remains a hard source failure rather than a successful
empty result.

The BCS list can be large even for one property. During source-contract
inspection, `494318013550` returned 107 rows: 19 master applications, 54
permits, and 34 plan reviews. The adapter normalizes the 73 master/permit
details and excludes the 34 plan reviews, fitting just below the 75-detail hard
cap. A property above the cap fails before any detail traversal.
