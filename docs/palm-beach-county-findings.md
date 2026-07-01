# Palm Beach County, FL — Source Discovery Findings

Source-discovery findings for Palm Beach County, the second onboarded county after Lee.
Status: **IN PROGRESS** — sources enumerated via NETR; per-portal data inventory
(Playwright probing) and authoritative counts pending a US egress IP + refreshed
`elephant-oracle-node` AWS creds.

Palm Beach is the deliberate stress test for **source discovery** (Soofi, 2026-06-29):
~39 municipalities each with its own permit portal — a mini version of the national
"25k–60k permit sources vs ~3,300 counties" problem.

## Source enumeration (NETR Online directory)

From `https://publicrecords.netronline.com/state/FL/county/palm_beach`:

| Office                    | Official URL                                              | Role                          |
| ------------------------- | --------------------------------------------------------- | ----------------------------- |
| Property Appraiser (PAPA) | https://www.pbcgov.org/PAPA/index.htm                     | appraisal / property (seed)   |
| Tax Collector             | https://pbctax.publicaccessnow.com/PropertyTax.aspx       | tax roll                      |
| Clerk / Recorder          | https://www.mypalmbeachclerk.com/records/official-records | deeds, mortgages, liens       |
| Mapping / GIS             | https://maps.co.palm-beach.fl.us/cwgis/papa.html          | parcel geometry, parcel count |

## 1. Appraiser portal (property) — SOLVED (Playwright-probed, plain-HTTP scrapeable)

- **Source:** Palm Beach County Property Appraiser (PAPA). Canonical host **`pbcpao.gov`**
  (`pbcgov.org/papa` 302-redirects here).
- **Access mode: PLAIN HTTP — no browser, no Cloudflare** (tested from a non-US IP, works).
  Better than Lee (which needed a full browser flow). Two endpoints:
  1. **Autocomplete / search API** —
     `POST https://pbcpao.gov/AutoComplete/SearchAutoComplete`
     body `propertyType=RE&searchText=<query>`, header `X-Requested-With: XMLHttpRequest`.
     Returns JSON `[{ "text": "<owner> | <address>", "pcn": "P:<17-digit PCN>" }]`.
     (`propertyType=TPP` for tangible personal property.)
  2. **Per-parcel detail** —
     `GET https://pbcpao.gov/Property/Details?parcelId=<17-digit PCN>` → full HTML
     (owner, address, values, etc.). Confirmed 200 with real data via plain curl.
  - Search-results page: `GET /MasterSearch/SearchResults?propertyType=RE&searchvalue=<q>`;
    `GET /MasterSearch/MasterSearch?propType=RE&searchvalue=&pcn=<PCN>` 302-redirects to the
    Details page (clean PCN→detail resolver).
  - Site uses Dynatrace (`ruxitagentjs`) RUM only — no bot wall on these endpoints.
- **Other endpoints:** tax roll `pbcpao.gov/tax-roll.htm`; PAPA GIS `gis.pbcgov.org/papagis`.
- **TARGET PARCEL COUNT (AC test number): `659,119` Real Property Parcels (PAPA, 2025)**,
  total market value $513,305,769,954 (Lee's equivalent was 516,848). GIS REST can't
  corroborate (parcel layer is a cached tile service, `returnCountOnly`=1); true AC
  reconciliation = seed/tax-roll row count at load time.
- **TODO:** bulk seed source (tax-roll/GIS download vs autocomplete-walk) for the full
  659k enumeration.

## 2. Parcel identifier — SOLVED

- **PCN = Property Control Number**, **17 digits**. Segments (from `pbcpao.gov/pcn-info.htm`):
  `MM RR TT SS UU BBB LLLL` →
  - digits **1–2** = municipality (**`00` = unincorporated PBC**) — _encodes the permit
    jurisdiction directly, useful for routing._
  - 3–8 range/township/section, 9–10 subdivision, 11–13 block, 14–17 lot.
  - Example: `00424414630111105` (no punctuation) = `00-42-44-14-63-011-1105` (with dashes).
  - In the autocomplete API the PCN comes prefixed `P:` (strip it).

## 3. Permit portal(s) — THE HARD PART — jurisdictions enumerated, portals TODO

- **NOT county-level.** Confirmed via county GIS `PZB/Municipalities` layer:
  **40 jurisdictions = 39 incorporated municipalities + Unincorporated PBC.**
  - South Bay, Palm Beach Shores, Greenacres, Delray Beach, Atlantis, Belle Glade,
    Riviera Beach, Gulf Stream, West Palm Beach, Boca Raton, Palm Springs, Lake Park,
    Loxahatchee Groves, North Palm Beach, South Palm Beach, Palm Beach, Wellington,
    Lake Clarke Shores, Westlake, Tequesta, Golf, Mangonia Park, Palm Beach Gardens,
    Jupiter, Lake Worth Beach, Lantana, Royal Palm Beach, Boynton Beach, Highland Beach,
    Ocean Ridge, Briny Breezes, Glen Ridge, Hypoluxo, Cloud Lake, Manalapan,
    Jupiter Inlet Colony, Pahokee, Juno Beach, Haverhill — + Unincorporated (PBC).
- Permits are **not** in the county GIS REST (PZB folder has only Municipalities +
  Agricultural_Reserve boundaries).
- **Known lead (county-discovery skill):** PBC permits require a Playwright session via
  `iPZB.Building/Session` before its API answers. This is the **county/unincorporated**
  portal; the ~39 municipalities each have their own (Accela / Tyler / ePZB / custom).
- **Fragmentation PROVEN** — 3 jurisdictions probed, 3 different vendors:
  | Jurisdiction | Portal | Vendor | Search |
  |---|---|---|---|
  | Unincorporated PBC | `pbc.gov/ePZB.Admin.WebSPA/` (apply: `pbcgov.com/epzb`) | **ePZB** (county custom; the skill's `iPZB`) | address / PCN |
  | West Palm Beach | `permit-planner.wpb.org` (legacy `onestopshop.wpbgov.com/eGovPlus`, `byb2-egov.aspgov.com/Click2GovBP`) | **Tyler EPL / Civic Access** (migrated 2023) | address / permit # |
  | Boca Raton | Boca eHub via `myboca.us`; `boca-egov.aspgov.com/Click2GovBP` | **Boca eHub + Click2Gov** | address / app # / keyword |
- **Vendor families seen so far:** county ePZB (custom), Tyler EPL/Civic Access,
  Click2Gov (aspgov.com). Expect Accela, CentralSquare, OpenGov, GovAccess across the
  rest → each needs an adapter (reuse the `county-permit-adapter` skill; Lee = Accela).
- **This is the source-discovery deliverable:** the remaining ~37 jurisdictions should be
  discovered + catalogued by the agent (not hand-probed). Recipe per jurisdiction:
  1. find the official permit portal URL (city site → "permits/building" → online search);
  2. classify vendor by URL/page shape; 3. record search-by-parcel/address support +
     session needs; 4. write the row to the source catalog (see `palm-beach-sources.yaml`).

## 4. Sunbiz (business registration) — reuse Lee toolchain

- Florida Sunbiz is **statewide** — same `cordata.zip` bulk download as Lee. Only new work:
  the **Palm Beach ZIP-prefix list** for address matching.

## 5. BBB (business reputation) — reuse Lee toolchain

- Same national BBB category harvest as Lee, filtered to the Palm Beach area.

## Access reality — TESTED from local RS IP (NOT geo-blocked)

Empirically tested 2026-06-29 from a Serbian (RS) egress IP — **no geo-blocking**:

| Source                           | Status from RS IP    | Note                                                                            |
| -------------------------------- | -------------------- | ------------------------------------------------------------------------------- |
| PAPA home (`pbcpao.gov`)         | **200 OK**           | real content served                                                             |
| PAPA parcel detail               | 500 (app error)      | wrong guessed PCN, not a block — reachable                                      |
| Sunbiz **live search**           | 403 "Just a moment…" | **Cloudflare bot challenge** (headless browser solves; same anywhere — NOT geo) |
| Sunbiz **data downloads** (bulk) | **200 OK**           | the path we actually ingest from                                                |
| County GIS REST                  | **200 OK**           | reachable                                                                       |

**Conclusion:** the earlier "US VPN required" assumption was wrong for Palm Beach. The
only friction is **Cloudflare bot challenges** on JS-heavy pages (Sunbiz live search,
likely the permit portal session) → handled by a **headless browser**, exactly as Lee did.
Proxies (`PROXY_FILE` / `PERMIT_HARVEST_PROXY_URL`) remain worth having for the
**Lee-scale run** (rate-limiting / challenge volume per `county-ingest-run`), but are
**not a blocker to start discovery**.

- ~~**AWS** — `elephant-oracle-node` creds~~ — **RESOLVED**: static IAM key, valid
  (account `848665034107`), `counties-seeds/` reachable. No PB seed there yet.

## Next: real local Playwright probe (no proxy needed)

- PAPA: exact detail-page URL pattern + a real PCN (format/digits) via the search flow.
- Permit portals: county/unincorporated `iPZB` session + per-municipality portal discovery
  (candidates so far: `discover.pbcgov.org` 200; `epzb`/`pzbservices` hosts unresolved).
- Sunbiz: headless-browser path for live search; bulk `cordata.zip` for ingest.

## Open questions (carry to Soofi/Mykyta)

- Where does the **source-catalog repository** live (dedicated repo / DB table / skill)?
- Acceptance test for "the agent can discover sources" (Soofi: this is uncertified).
