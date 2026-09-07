---
description: "Harvest BBB (Better Business Bureau) business profiles by category for contractor reputation and quality enrichment in the elephant query DB, with throughput checks before long crawls. Use when asked to collect BBB profiles, contractor reputation data, or refresh bbb_* tables."
metadata: {"author":"elephant-xyz"}
---
# BBB Harvest

National data source — county-agnostic. Output feeds the `bbb_*` tables in
`elephant-query-db` (contractor reputation/quality scores joined to permits via
contractor names).

## Pinellas local CLI (oracle-node)

**This checkout is `oracle-node`, not the Restate pipeline.** Pinellas BBB harvest runs
here via vanilla Puppeteer (`scripts/harvest-bbb-category.mjs`) — **not** stealth mode,
**not** the BBB API.

### Operator full ingest already running — do not stop

If the operator started `pinellas-bbb-full` or `scripts/pinellas/run-bbb-full-harvest.sh`,
**leave it running**. Do not kill the harvest, restart Chrome, or start competing BBB
jobs. Also leave Accela alone (`pinellas-accela-full`, separate Chrome profile).

| Item | Value |
| --- | --- |
| tmux session | `pinellas-bbb-full` |
| Orchestrator | `scripts/pinellas/run-bbb-full-harvest.sh` → `run-bbb-full-harvest.mjs` |
| Jobs | 6 sequential (Clearwater then St. Pete × roofing/HVAC/solar) |
| Browser | 1 Chrome (`/usr/local/bin/google-chrome`), 1 tab, separate profile from Accela |
| Flags | `--headless true --no-html --profile-subpages none --max-pages 1000 --page-delay-ms 2000 --profile-delay-ms 1500` |
| Output | `downloads/pinellas/bbb-harvest/{clearwater,st-petersburg}/{roofing,hvac,solar}` |
| Logs | `downloads/pinellas/bbb-harvest/logs/` (`runner.log` + per-job logs) |

First job: Clearwater roofing (~4,882 listings). Cities: St. Petersburg + Clearwater
(Tampa excluded). Trades: roofing, HVAC, solar. Pagination uses `?page=N`.

When the operator has already started the full ingest, **do not** block on the 48-hour
gate or refuse the run because `docs/pinellas-county-findings.md` once recommended
API-only — see the operator override dated 2026-09-07 in that doc.

To start a fresh full ingest (operator request only):

```bash
tmux -f /exec-daemon/tmux.portal.conf new-session -d -s pinellas-bbb-full \
  'cd /workspace && scripts/pinellas/run-bbb-full-harvest.sh 2>&1 | tee downloads/pinellas/bbb-harvest/logs/runner.log'
```

Page-1-only sampling (robots-compliant fallback) remains in
`buildPinellasBbbHarvestPlan()`; full pagination is
`buildPinellasBbbFullHarvestJobs()` — see `scripts/pinellas/bbb-harvest-plan.mjs`.

## Pipeline counties — Restate `BbbHarvest`

For onboarded counties on the durable stack, the harvester is the `BbbHarvest` Restate
workflow (key = jobId) in `elephant-pipeline/services/enrichment.ts`, wrapping a local
Puppeteer crawler. `BbbHarvest` is authored per `durable-workflow-builder`; the
parameters and output layout below are its contract, not existing code.

### Run

```bash
curl localhost:8080/restate/send/BbbHarvest/<jobId>/run \
  --json '{"jobId":"<jobId>","categoryUrl":"https://www.bbb.org/us/category/<category>","maxPages":50,"pageDelayMs":2000,"profileDelayMs":1500}'
```

Input parameters (all optional except `categoryUrl` and `jobId` — `jobId` is required
and must match the workflow key in the URL):

- `maxPages` / `maxProfiles` — crawl bounds
- `profileSubpages` (array) — capture extra tabs per profile
- `challengeAttempts` / `challengeCheckIntervalMs` — bot-challenge retry tuning
- `headless: false` (+ a Chromium executable path) — when challenges need a real browser
- `html: false` — skip raw HTML capture (keep HTML by default; raw-first principle)

Each page and profile is its own `ctx.run` step, so the crawl is durable: a crash or
restart resumes exactly where it stopped — no manual start-page bookkeeping. A step that
exhausts retries pauses visibly in the Web UI (`:9070`); fix, then resume.

## Output layout

Under `data/artifacts/enrichment/bbb/<jobId>/` (probes can just use a scratch
subdir there):

- `profiles/profiles-part-NNNN.jsonl` — extracted profile records (chunked)
- `failures/failed-profiles.jsonl` — only classified PERMANENT profile failures are
  recorded here (rather than aborting the crawl); re-run with the failed URLs after a
  challenge-tuning change. TRANSIENT failures (challenge/network) retry inside their
  `ctx.run` and, if retries are exhausted, fail/pause the workflow — never silently
  record a transient failure as done
- `manifest/summary.json` — counters for reconciliation

## Workflow

1. Pick the category relevant to the enrichment goal (e.g. construction/contractor
   categories for permit contractor matching).
   - **Multi-Trade Harvesting**: Expand contractor collection across all high-value building trades:
     - Roofing Contractors (`roofing-contractors`)
     - Solar Energy Contractors (`solar-energy-system-contractors`)
     - Heating and Air Conditioning / HVAC (`heating-and-air-conditioning`)
2. Run a small probe (`maxPages: 2`, scratch output subdir) to confirm challenge handling
   works from the current network; BBB serves bot challenges that the crawler retries
   through, but datacenter IPs may need `headless: false` or a different egress. If
   pages come back 403/blocked, check the egress country (`curl -s ipinfo.io/country`)
   — a US VPN/proxy exit may be required before anything else is worth debugging.
3. Estimate total crawl time from probe latency, page/profile counts, delays, retries,
   and safe concurrency. If the estimate is more than 48 hours, ask whether to download
   BBB artifacts anyway, ingest them into the query DB, or retrieve BBB profile data at
   runtime from the owning app/service. For runtime retrieval, capture the expected API
   or scrape path, cache/freshness needs, and failure behavior. Stop here if the
   operator chose ingest-only or runtime retrieval — do not proceed to step 4.
4. Run the full category with conservative delays; the journal keeps it resumable.
5. Reconcile `summary.json` counts vs profiles parts, retry failures.
6. Load into the `bbb_*` tables per the `query-db-loading-matching` skill.

### 3-Tier Multi-Source CRM Cross-Matching Cascade

When matching BBB contractor profiles to municipal permit records or Sunbiz business entities, apply a strict 3-tier cascade:
1. **Tier 1 — State License Number Match**: Match exact state license strings (e.g. `CCC1328456`, `CAC1815924`). Highest confidence (1.0).
2. **Tier 2 — Standardized Phone Number Match**: Normalize 10-digit phone strings (strip punctuation and country code `+1`). High confidence (0.95).
3. **Tier 3 — Cleaned Business Name Match**: Strip corporate suffixes (`LLC`, `INC`, `CORP`, `SERVICES`, `ROOFING`), trim whitespace, and match normalized names with Jaro-Winkler similarity ≥ 0.90. Medium confidence (0.80).

The crawler module has vitest tests in `elephant-pipeline` — keep them passing if it is
modified. Category lists, run notes, and any
source-specific docs produced for a county or category also get committed and PR'd to
`github.com/elephant-xyz/Counties-trasform-scripts` (`gh pr create`) so they aren't lost.
