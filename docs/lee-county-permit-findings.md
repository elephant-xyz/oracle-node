# Lee County Permit Workflow Findings

Date: 2026-05-25

## Inputs Reviewed

- `lee_county_permit_workflow_report.md`
- `permit-node.zip`
- local `elephant-cli` checkout at `/home/movsiienko/Projects/elephant/elephant-cli`
- local `oracle-node` repository

## Current CLI Capability

- The installed `oracle-node` dependency `@elephant-xyz/cli@1.58.1` does not expose the old developer-local permit helpers (`preparePermits`, `preparePermitPages`, `preparePermitPagesToDirectory`).
- The local `elephant-cli` checkout does have the newer Browser Flow v2 and Transform v2 paths:
  - Browser Flow v2: `prepare(..., { browserFlowVersion: 2, browserFlowZip })`
  - Transform v2: `transform(..., { transformVersion: 2, transformZip, dataGroup })`
- Browser Flow v2 preserves input files and writes a `captures.json` manifest plus named HTML captures under `captures/`.
- Transform v2 expects `parcel.json`, `address.json`, `captures.json`, and named captures. It does not consume legacy root-level HTML directly.

## Lee County Accela Behavior

- The public Lee County Accela portal is reachable without login/CAPTCHA from this environment after the networking issue was resolved.
- Direct permit detail pages are public when the URL includes the full Accela query parameters.
- Do not pass a full URL together with an empty `multiValueQueryString: {}` into CLI prepare. The CLI's URL construction treats the empty query map as authoritative and strips the real query string, which sends Accela to `Error.aspx`.
- For General Search, the default date range can hide old permits. Clearing both Start Date and End Date fields worked better than typing `01/01/1900`; typing the old date caused an Accela `String was not recognized as a valid DateTime` error in one test.

## Local E2E Smoke Result

Using the local-built CLI at `/home/movsiienko/Projects/elephant/elephant-cli/dist/index.js`, Browser Flow v2 successfully:

1. opened `https://aca-prod.accela.com/LEECO/Cap/CapHome.aspx?module=Permitting&TabName=Permitting`,
2. cleared General Search date fields,
3. searched parcel `02442401000090000`,
4. captured the permit list page,
5. found `Showing 1-10 of 100`,
6. extracted 10 detail links from the first result page,
7. opened first permit `ELE2025-02590`,
8. captured the record detail page.

Transform v2 then extracted from the live captures:

- permit number: `ELE2025-02590`
- record type: `Electrical`
- status: `Closed-CC Issued`
- parcel: `02442401000090000`
- work location: `4980 BAYLINE DR NORTH FORT MYERS FL 33917`
- project description: `Lee County Electric Cooperative/Fitness Center Data low voltage`
- estimated job value: `10000`
- estimated square feet: `2856`
- completed inspections: two `Pass` inspections with inspection code, type, ID, inspector, and date.

## What `oracle-node` Needs

To run this as part of `oracle-node`, the smallest useful implementation is:

1. Upgrade or pin `@elephant-xyz/cli` to a version/commit with Browser Flow v2 and Transform v2.
2. Extend the prepare/downloader Lambda to accept a Browser Flow v2 ZIP artifact and pass `browserFlowVersion: 2` + `browserFlowZip` to CLI prepare.
3. Add a Lee County Browser Flow v2 handler package that searches by parcel, clears date fields, paginates result pages, and captures permit list/detail pages.
4. Add a Lee County Transform v2 handler package that extracts permit list metadata, permit detail fields, inspections, applicants/contractors, parcel fields, job values, and document/ePlan links where public.
5. Extend the transform worker to call Transform v2 with `transformVersion: 2` and `transformZip`.
6. Ensure seeds include both `parcel.json` and `address.json` aliases because Transform v2 expects those names.

## `permit-node` Mechanism

There are two related but different artifacts:

1. GitHub `elephant-xyz/permit-node` `main` at `d271c60e23919d2e83bde46260bbab98a899802e`
2. extra `permit-node/local-permit-workflow` bundled inside `permit-node.zip`

The GitHub `permit-node` workflow is not parcel-by-parcel. Its `PreparePermits` Lambda downloads `portal.zip`, calls the old CLI `preparePermits(portalZipPath, { start: today, end: today, output })`, splits the produced permit-link CSV into chunks, and publishes chunk messages. The `PreparePermitPages` Lambda then wraps each CSV chunk in a ZIP and calls old CLI `preparePermitPages(zipFilePath, outputZipPath, { useBrowser: true })`. This is a date-window portal harvester for new/current permit records, not a parcel-seed workflow.

The bundled `local-permit-workflow` is parcel-by-parcel. It loads a seed CSV, normalizes `parcel_id` by stripping punctuation into `searchParcelId`, deduplicates those search IDs, and for each parcel calls old CLI `preparePermits("", { url: portalUrl, parcelId: entry.searchParcelId, start: "01/01/1900", end: today, output })`. It then reads each parcel result ZIP, merges and dedupes detail links, writes `outputs/all-permit-links.csv`, and calls old CLI `preparePermitPagesToDirectory(...)` to expand every discovered permit detail page.

Evidence from the bundled run:

- `workdir-full-run/manifests/workflow.json` shows `seedRows: 79`, `searchedParcels: 79`, `parcelConcurrency: 8`, `pageConcurrency: 8`, and `processedPermitPages: 1025`.
- `permit-node.zip` contains 1,025 expanded permit HTML files under `local-permit-workflow/workdir-full-run/results/permits/`.
- It contains 67 parcel-link ZIPs under `local-permit-workflow/workdir-full-run/parcel-links/`; some of the 79 searched parcels were empty/error/skipped or did not produce ZIPs.

Conclusion: for the “bring as much permit data as possible for our parcel seeds” goal, the useful precedent is the zipped `local-permit-workflow`, not the GitHub `permit-node` main Step Functions workflow. We should preserve its parcel-search strategy, but implement it with Browser Flow v2/Transform v2 instead of the old private `preparePermits` and `preparePermitPages*` helpers.

## Open Follow-Ups

- Decide final data model names for permits/inspections if the canonical data group schema lacks direct permit entities.
- Add broader offline tests using archived HTML from `permit-node.zip`; keep live Lee County smoke tests gated by an environment variable.
- Stage real Sunbiz quarterly corporate data in S3 or find an allowed non-challenged acquisition path.

## Implemented Permit Harvest Worker

The production path now uses a separate low-concurrency stack instead of the main `elephant-oracle-node` stack because the main SAM stack update repackaged unrelated Lambdas and hit existing Lambda size limits.

- Stack: `elephant-permit-harvest`
- Queue: `https://sqs.us-east-1.amazonaws.com/848665034107/elephant-oracle-node-permit-harvest-queue`
- DLQ: `https://sqs.us-east-1.amazonaws.com/848665034107/elephant-permit-harvest-PermitHarvestDeadLetterQueue-y0H4KiJoMez6`
- Worker: `elephant-permit-harvest-PermitHarvestWorkerFunctio-FMP5YTblMFul`
- Output: `s3://elephant-oracle-node-environmentbucket-mmsoo3xbdi80/permit-harvest`
- Current worker artifact: `deployments/permit-harvest-worker/permit-harvest-worker-20260526T093930Z.zip`
- Current worker sizing: 4096 MB memory, 4096 MB `/tmp`, 900s timeout.

Worker message types:

- `lee-permit-list-window`: searches Lee County Accela General Search by opened date range, captures raw list HTML and links JSON, splits dense multi-day windows when Accela reports at least the configured threshold, and enqueues detail batches for final windows.
- `lee-permit-detail-batch`: captures raw permit detail HTML, extracts public permit fields, and writes lexicon-gap-preserving JSON.
- `sunbiz-corporate-address-match`: reads an S3-staged Sunbiz corporate fixed-width text file or ZIP, matches address inputs against principal, mailing, registered-agent, and officer addresses, and writes entity/address match JSON.
- `sunbiz-corporate-zip-extract`: reads an S3-staged Sunbiz corporate fixed-width text file or supported ZIP, extracts every entity whose principal, mailing, registered-agent, or officer ZIP matches a configured ZIP-prefix list, and writes bounded JSONL chunks plus a summary.

Live Lee smoke results:

- Job `lee-smoke-20260525b` captured one day (`2025-05-01`) with 10 list links and 10 detail JSON outputs.
- Example output `permit-harvest/lee-smoke-20260525b/lee/extracted/permits/mec2025-02353-2212e50e478f.json` now has `recordStatus: "Closed-CC Issued"`.
- Old-date smoke `lee-smoke-old-19900101` verified Accela accepts early dates and can return no-results pages cleanly.

Historical Lee backfill launched:

- Job ID: `lee-permit-backfill-20260525`
- Range: `1990-01-01` through `2026-05-25`
- Initial windows: 444 windows of 30 days each
- Split threshold: 100 reported results for multi-day windows
- Detail batch size: 8
- Lambda/event-source concurrency: 4
- Early queue behavior is expected to grow while dense 30-day windows split into 15-day, 7-day, and eventually one-day windows. DLQ remained empty during launch monitoring.

## Sunbiz Findings and Current Path

The report's live Sunbiz address-search URL is:

`https://search.sunbiz.org/Inquiry/corporationsearch/SearchResults?inquiryType=Address&searchTerm={address}`

Direct local `curl` still hits a Cloudflare managed challenge (`cf-mitigated: challenge`, "Performing security verification"), but headless Chromium/Puppeteer now passes the challenge for both the public search site and the official Data Access Portal. A live local Sunbiz address-search smoke for `4980 Bayline` returned `ELECTRIC TOASTERS TOASTMASTERS CLUB INC.` and `LEE INTERLINE, INC.`. A live local Data Access Portal smoke logged in with the public credentials, listed `doc > quarterly > cor`, and confirmed `cordata.zip` is available at about 1.74 GB compressed and `corevent.zip` at about 187 MB compressed.

The robust path is therefore S3-staged official bulk data, not repeated live HTML search scraping:

- Official quarterly corporate data: `doc > quarterly > cor > cordata.zip`
- Official event data: `doc > quarterly > cor > corevent.zip`
- Format: fixed-width ASCII, no headers, corporate record length 1440 characters.
- Useful fields preserved by the parser: document number, entity name, status, filing type, principal/mailing address, file date, FEI, last transaction date, annual reports, registered agent, and up to six officers/addresses.

Sunbiz worker smoke result:

- Sample fixed-width object staged at `permit-harvest/sunbiz-smoke-inputs/sample-cor.txt`.
- Direct Lambda invocation wrote `permit-harvest/sunbiz-smoke-20260525/sunbiz/address-matches/sample-batch-00000.json`.
- It matched `4980 Bayline` to document `790346` and `8010 Summerlin Lakes Dr` to document `L04000013844`.

Updated local E2E after networking was fixed:

- Downloaded real daily corporate file `20260522c.txt` from the Data Access Portal; it contained 3,021 fixed-width corporate records.
- Parsed the file with the repo Sunbiz parser and extracted entities whose principal, mailing, registered-agent, or officer ZIPs matched Lee-area prefixes.
- Result: 87 matched corporate records across 4 local JSONL-style chunks using a 25-record chunk size.
- Sample matched entities included `CRONO CONTRACTING GROUP LLC` at `4312 NW 27TH LN CAPE CORAL 33993` and `ARANDA FLOORING INC` at `26550 CHAPAREL DR BONITA SPRINGS 34135`.

Implementation direction: prefer full official bulk ZIP processing by Lee-area ZIP prefixes over address-by-address Sunbiz HTML searches. This is analogous to the permit date-window strategy: acquire the broad official dataset once, extract all likely Lee County companies by ZIP across principal/mailing/registered-agent/officer addresses, and normalize/link to permit addresses afterwards. Address matching remains useful as a secondary enrichment step for specific permit work locations.

## Sunbiz Quarterly Corporate Run

The official quarterly corporate source was staged at:

`s3://elephant-oracle-node-environmentbucket-mmsoo3xbdi80/permit-harvest/sunbiz-source/quarterly/cor/cordata-2026q2.zip`

Important operational finding: the official `cordata.zip` uses ZIP compression method 9 (Deflate64), which the Lambda worker's `yauzl` ZIP reader cannot stream. The production workaround was to expand the ten `cordata*.txt` entries locally with system `unzip`, upload those fixed-width text files to S3, and process each text object with `sourceFormat: "text"`.

Expanded source prefix:

`s3://elephant-oracle-node-environmentbucket-mmsoo3xbdi80/permit-harvest/sunbiz-source/quarterly/cor/cordata-2026q2-expanded/`

Full extraction job:

- Job ID: `sunbiz-lee-corporate-quarterly-2026q2-expanded`
- Manifest: `s3://elephant-oracle-node-environmentbucket-mmsoo3xbdi80/permit-harvest/sunbiz-lee-corporate-quarterly-2026q2-expanded/sunbiz/corporate-by-zip/manifest.json`
- Records scanned: `12,607,458`
- Invalid records: `0`
- Lee-area ZIP-matched entity records: `379,467`
- Output chunks: `80`

The output preserves the full parsed corporate record, including document number, entity/status/type, principal and mailing addresses, filing dates, FEI, annual report dates, registered agent, and officer names/addresses. The current lexicon does not fully model this yet, so these outputs are intentionally marked as preserved for later lexicon expansion rather than dropped.
