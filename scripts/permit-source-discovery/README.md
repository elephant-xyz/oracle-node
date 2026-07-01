# permit-source-discovery

Deterministic Node tooling for the **permit-source-discovery agent** — the capability
that discovers and catalogs each US jurisdiction's building-permit portal.

Counties have exactly one property appraiser, but building permits are issued by
municipalities, townships, and boroughs — roughly 25k–60k separate portals
nationwide. Palm Beach County FL alone has 39 municipalities plus unincorporated.
Cataloging them is the bottleneck for permit ingestion.

## The split: LLM agent vs. deterministic helper

This directory is the **deterministic, certifiable core**. It does the parts that
must be reproducible and provable. It is _not_ the whole agent.

| Concern                                                                                                        | Owner          | Why                                                                              |
| -------------------------------------------------------------------------------------------------------------- | -------------- | -------------------------------------------------------------------------------- |
| Find a city's official website from its name; read its "permits" page; disambiguate; follow web-search results | **LLM agent**  | Open-ended judgement, web search, reading prose — not deterministic              |
| Recognize the vendor behind a known portal URL/HTML                                                            | `vendors.mjs`  | A fixed signature table; pure and unit-testable                                  |
| Re-fetch and re-classify every cataloged portal to prove it                                                    | `certify.mjs`  | Reproducible verification — this is what proves "the agent can discover sources" |
| Resolve + classify candidate URLs from common host shapes, flag misses                                         | `discover.mjs` | Cheap deterministic first pass; misses are handed back to the LLM agent          |

The agent supplies verified portal URLs (into the catalog or into `discover.mjs`);
these scripts prove and re-prove them. **They never invent data** — they only
classify what the network actually returns.

## Files

- **`vendors.mjs`** — vendor-signature classifier. `VENDORS` is the seed table of
  permit-software vendors (Accela, Tyler EnerGov/Civic Access, Click2Gov, OpenGov,
  CentralSquare, ePZB, GovAccess). `classifyVendor({url, html})` matches URL
  patterns first (confidence `url`), then HTML markers (confidence `html`), else
  `unknown`. Pure, synchronous, no network. Extend the table as new vendors appear.
- **`certify.mjs`** — the certification tool. Re-fetches every portal in a source
  catalog and checks the detected vendor against the catalog's stated vendor.
- **`discover.mjs`** — best-effort candidate resolution for a list of jurisdictions.

## Usage

Certify an existing catalog (verifies `docs/palm-beach-sources.yaml`):

```bash
node scripts/permit-source-discovery/certify.mjs docs/palm-beach-sources.yaml
```

Prints a per-jurisdiction table:

```
jurisdiction | http | reachable | catalogVendor | detectedVendor | permitEvidence | verifiedVia | verdict
```

and a summary line `(N/total certified, R review, K unreachable, S skipped (needs-review))`.
A row is **PASS** only when the detected vendor matches the catalog AND the page shows
permit-domain evidence (`permitEvidence: yes`) — this is what proves the URL is a live
_permit_ portal of the stated vendor, not merely a reachable vendor-branded host. A
vendor mismatch or missing permit evidence is **REVIEW** (a warning). Catalog rows
with no `portal` (the needs-review small towns) are **SKIPPED** — not probed, not
failed. Exit code is non-zero **only** when a portal is UNREACHABLE, because discovery
is best-effort (challenged or JS-only portals are expected).

**SPA / bot-block Playwright fallback.** Plain `fetch` is the fast primary path. Many
modern permit portals (Civic Access, eHub, OpenGov permitting, Tyler tylerhost/EDEN,
MGO, CityView) are JS single-page apps whose _static_ HTML lacks permit markers, or
they bot-block plain fetch. Certify renders with Playwright (the chromium reused from
the catalog workspace) and re-evaluates when EITHER a reachable HTTP 200 page
classifies to a vendor but shows no static permit evidence, OR the page is a bot
challenge / HTTP 403. After rendering it re-derives reachability (a substantial,
non-challenge DOM is treated as reachable), vendor, and permit evidence. The
`verifiedVia` column reports `static` (passed on plain fetch) or `rendered` (evaluated
after the Playwright render). The browser is launched lazily — at most once, only if
some entry needs it — and closed at the end. If Playwright or the chromium binary is
unavailable, that entry keeps its static result (with a printed note) rather than
failing the run.

Discover candidate portals for new jurisdictions (comma list, print to stdout):

```bash
node scripts/permit-source-discovery/discover.mjs \
  --county "Palm Beach" \
  --jurisdictions "Boynton Beach,Delray Beach,Jupiter,Wellington"
```

Or from a JSON array file, merging into a catalog (existing `discovered` rows are
preserved, never clobbered):

```bash
node scripts/permit-source-discovery/discover.mjs \
  --county "Palm Beach" \
  --jurisdictions ./palm-beach-municipalities.json \
  --out docs/palm-beach-sources.yaml
```

Rows are emitted with `status: discovered` only when a candidate (a) returns HTTP
`200` (a redirect ending in an error page does not count), (b) shows permit-domain
evidence in its body, and (c) classifies to a known vendor. Otherwise the row is
`status: needs-review` — handed to the LLM agent / human, with `candidates_tried`
recorded for context. (This guards against false positives like vendor-branded hosts
that actually serve a non-permit page, e.g. an OpenGov financial transparency portal.)

## Notes

- Plain `fetch` (Node 26 global) is used throughout; no browser is launched.
  Portals that render their vendor only via JS, or that sit behind a bot challenge,
  will classify as `unknown` / report `reachable: challenge` — these are warnings,
  not failures. If a JS-only portal needs rendering, add Playwright at that step;
  it is intentionally kept out of the deterministic path.
- Network concurrency is capped at 4.
- `yaml` (already resolvable in `oracle-node/node_modules`) is used for parsing and
  serializing catalogs.
