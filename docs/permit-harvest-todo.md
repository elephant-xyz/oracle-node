# Permit Harvest TODO

## Lee County all-permits harvest

- [x] Confirm usable elephant-cli GitHub ref for Browser Flow v2 / Transform v2.
- [x] Add/update dependencies from GitHub instead of npm where needed.
- [x] Add a separate permit-harvest SQS queue and DLQ.
- [x] Add a permit-harvest worker Lambda with low max concurrency.
- [x] Add worker message contracts for:
  - [x] `lee-permit-list-window`
  - [x] `lee-permit-detail-batch`
  - [x] `sunbiz-corporate-address-match`
  - [x] `sunbiz-corporate-zip-extract`
- [x] Implement Lee list-window scraping:
  - [x] validate message input
  - [x] date-window Accela search
  - [x] split dense multi-day windows when reported total is high
  - [x] paginate final windows
  - [x] write raw list HTML and permit-link JSON to S3
  - [x] enqueue detail batches to the separate queue
- [x] Implement Lee permit-detail batch scraping:
  - [x] skip if stable output already exists
  - [x] capture raw HTML
  - [x] parse as much permit data as possible
  - [x] write extracted JSON even for fields not in lexicon
  - [x] write lexicon-gap status in extracted JSON
- [x] Run local/cloud E2E for one Lee window and a few detail pages.
- [x] Typecheck and tests.
- [x] Deploy with `AWS_PROFILE=elephant-oracle-node`.
- [x] Seed/launch Lee historical backfill job.
- [x] Capture queue URL, execution/job prefix, and monitoring commands.
- [ ] Let Lee historical backfill drain; watch DLQ and inspect extracted-permit counts.

## Sunbiz extraction

- [x] Inspect Sunbiz workflow from `lee_county_permit_workflow_report.md`.
- [x] Confirm direct Sunbiz address-search scraping is currently Cloudflare-challenged from this environment.
- [x] Confirm official Sunbiz bulk downloads are fixed-width corporate data, but the download host is also Cloudflare-challenged from this environment.
- [x] Implement Sunbiz bulk corporate address matching from an S3-staged fixed-width text file or ZIP.
- [x] Preserve entity, principal address, mailing address, registered agent, and up to six officer addresses even before lexicon expansion.
- [x] Add tests/typecheck.
- [x] Deploy and smoke-test the Sunbiz worker path with an S3-staged fixed-width sample.
- [x] Re-test after networking fix: headless Chromium can now load Sunbiz address search and log into the Data Access Portal; plain `curl` still hits Cloudflare challenge.
- [x] Run local E2E on a real downloaded daily corporate file (`20260522c.txt`): 3,021 records parsed, 87 Lee-area ZIP matches emitted across 4 local chunks.
- [x] Implement ZIP-prefix bulk extraction message type (`sunbiz-corporate-zip-extract`) to capture all entities whose principal, mailing, registered-agent, or officer ZIPs match Lee-area prefixes.
- [x] Add enqueue helper for ZIP-prefix extraction (`scripts/enqueue-sunbiz-corporate-zip-extract.mjs`) with built-in Lee County ZIP list.
- [x] Stage real quarterly `cordata.zip` in S3. Data Access Portal shows it under `doc > quarterly > cor`, filename `cordata.zip`, size about 1.74 GB compressed.
- [x] Deploy updated permit-harvest worker with 4 GB `/tmp` ephemeral storage and run a cloud smoke against the staged daily file before launching quarterly data.
- [x] Run full Lee-area ZIP extraction against the quarterly corporate dataset.
- [x] Add Sunbiz business-registration model to `../lexicon` and run the extracted quarterly Lee-area records through a lexicon-shaped transform.
- [ ] Build/launch address batches from Lee permit extracted work locations once enough permit details have accumulated.

## Constraints / decisions

- Use separate SQS queue from existing property workflow.
- Keep concurrency low because Lee Accela is slow.
- Prefer stable S3 keys and skip already-done work.
- Do not block extraction on current lexicon coverage; record lexicon gaps for later.
- No DynamoDB FilterExpression.
- Use AWS docs/best practices for AWS service changes.

## Current deployment / runbook

- Separate stack: `elephant-permit-harvest`
- Queue URL: `https://sqs.us-east-1.amazonaws.com/848665034107/elephant-oracle-node-permit-harvest-queue`
- DLQ URL: `https://sqs.us-east-1.amazonaws.com/848665034107/elephant-permit-harvest-PermitHarvestDeadLetterQueue-y0H4KiJoMez6`
- Worker function: `elephant-permit-harvest-PermitHarvestWorkerFunctio-FMP5YTblMFul`
- Output prefix: `s3://elephant-oracle-node-environmentbucket-mmsoo3xbdi80/permit-harvest`
- Current worker artifact: `s3://elephant-oracle-node-environmentbucket-mmsoo3xbdi80/deployments/permit-harvest-worker/permit-harvest-worker-20260526T093930Z.zip`
- Worker sizing: 4096 MB memory, 4096 MB `/tmp`, 900s timeout. The SQS event-source max concurrency and Lambda reserved concurrency are restored to 4 after the Sunbiz quarterly run.
- Lee backfill job ID: `lee-permit-backfill-20260525`
- Lee backfill date range: `1990-01-01` through `2026-05-25`; 444 initial 30-day windows; dense windows split recursively; detail batches use size 8; Lambda/event-source concurrency is capped at 4.
- Latest check on 2026-05-26 05:06 UTC: main queue had 903 visible / 2 in-flight / 0 delayed messages; DLQ had 0; S3 had 922 Lee list artifacts but still 0 detail-batch summaries, 0 raw permit-detail HTML files, and 0 extracted permit JSON files. The worker is healthy, but all recent windows still reported Accela's capped `100` total and were split again, so the run is still in discovery/splitting rather than detail extraction. Among written `links.json` summaries, observed spans were 420 initial 16-30 day windows, 41 narrowed 9-15 day windows, and 1 narrowed 3-4 day window; no terminal 1-day windows had been written yet.
- Deployed update on 2026-05-26 05:17 UTC: list-window messages now enqueue detail batches for captured permit links before splitting dense windows, so first-page permits can be extracted while completeness splitting continues. First confirmation log: `lee_detail_batches_enqueued` for `19900101_19900115` with `batchCount: 2` and `sourceWindowWillSplit: true` at 2026-05-26 05:19 UTC. At that moment SQS had not yet delivered a detail-batch message to Lambda, so extracted permit count was still 0.
- Deployed stronger update on 2026-05-26 05:27 UTC: list-window messages now capture and write discovered permit details inline before splitting, because SQS was still prioritizing existing list-window messages over newly enqueued detail-batch messages. First confirmed inline extraction completed at 2026-05-26 05:31 UTC. Latest check after that update: main queue had 959 visible / 2 in-flight / 0 delayed messages; DLQ had 0; extracted permit JSON count was 25; raw detail HTML count was 25; detail batch summaries count was 4. Sample extracted permit: `TMP199501617`, status `Closed-Old`, work location `4361 BAY BEACH LN`, parcel `034724W10610B0000 *`, description `TEMP CONST TRAILER`.
- Increased concurrency on 2026-05-26 07:00 UTC by setting both Lambda reserved concurrency and the SQS event-source maximum concurrency to 4. Burn-in at 2026-05-26 07:07 UTC: CloudWatch showed actual concurrent executions reaching 4, Lambda `Errors` remained 0, DLQ remained 0, extracted permit JSON count increased from 688 to 762 in about 6.5 minutes, and detail batch summaries increased from 138 to 152. A few individual detail captures timed out at the 90s page wait and were recorded in batch summaries, but those did not fail Lambda invocations; monitor whether that per-record timeout rate rises before increasing beyond 4.

## Regular Lee appraisal prepare queue notes

- Queue URL: `https://sqs.us-east-1.amazonaws.com/848665034107/elephant-oracle-node-prepare-queue-lee`
- Downloader function: `elephant-oracle-node-DownloaderFunction-8GbNMvP3cL1H`
- Event-source mapping UUID: `8629af85-abed-4c92-b850-34b658383da1`
- Raised the Lee prepare event-source `MaximumConcurrency` from 20 to 30, then 40, then 80 on 2026-05-26. AWS account unreserved concurrency was 895 before the 80-concurrency change, and the downloader function has no reserved-concurrency cap.
- Burn-in at 40 around 2026-05-26 08:25 UTC showed 1,261 successes / 11 app-level prepare failures / 0 task timeouts over 10 minutes, with Lambda `Errors` and `Throttles` both 0. ETA at that point was about 2.5 days.
- Burn-in at 80 around 2026-05-26 08:39 UTC showed 1,847 successes / 58 app-level prepare failures / 0 task timeouts over 10 minutes, with Lambda `Errors` and `Throttles` both 0 and actual Lambda concurrency holding at 80. Prepare queue was about 453,306 visible / 61 in-flight; downstream Transform queue had a small backlog of 124 visible / 95 in-flight while SVL/Hash/Upload stayed at 0 visible. At that rate, remaining prepare ETA is about 41 hours, assuming the county site and Transform queue continue to keep up.

## Sunbiz quarterly corporate run notes

- Official quarterly source staged at `s3://elephant-oracle-node-environmentbucket-mmsoo3xbdi80/permit-harvest/sunbiz-source/quarterly/cor/cordata-2026q2.zip` (`1,744,668,408` bytes).
- `cordata.zip` uses ZIP compression method 9 (Deflate64), which `yauzl` cannot stream in Lambda. Workaround used for the production run: expand the 10 text entries locally with `unzip` and stage them under `s3://elephant-oracle-node-environmentbucket-mmsoo3xbdi80/permit-harvest/sunbiz-source/quarterly/cor/cordata-2026q2-expanded/`.
- Full extraction job: `sunbiz-lee-corporate-quarterly-2026q2-expanded`.
- Manifest: `s3://elephant-oracle-node-environmentbucket-mmsoo3xbdi80/permit-harvest/sunbiz-lee-corporate-quarterly-2026q2-expanded/sunbiz/corporate-by-zip/manifest.json`.
- Result: 10 quarterly text entries scanned, `12,607,458` corporate records read, `0` invalid records, `379,467` Lee-area ZIP-matched entity records, `80` JSONL chunks.
- ZIP matching covers principal address, mailing address, registered-agent address, and officer addresses. The output intentionally preserves fields not yet mapped to lexicon so the lexicon can be expanded later.
- Lexicon transform output: `s3://elephant-oracle-node-environmentbucket-mmsoo3xbdi80/permit-harvest/sunbiz-lee-corporate-quarterly-2026q2-expanded/lexicon-transform/business-registration-v1/summary.json`. It transformed all `379,467` matched records into `379,467` company records, `379,467` business registrations, `758,511` registration-address roles, `1,005,170` parties, `641,031` de-duplicated addresses, and `3,906,808` relationship records.

Monitoring commands:

```bash
AWS_PROFILE=elephant-oracle-node AWS_REGION=us-east-1 \
  aws sqs get-queue-attributes \
    --queue-url https://sqs.us-east-1.amazonaws.com/848665034107/elephant-oracle-node-permit-harvest-queue \
    --attribute-names All

AWS_PROFILE=elephant-oracle-node AWS_REGION=us-east-1 \
  aws sqs get-queue-attributes \
    --queue-url https://sqs.us-east-1.amazonaws.com/848665034107/elephant-permit-harvest-PermitHarvestDeadLetterQueue-y0H4KiJoMez6 \
    --attribute-names All

AWS_PROFILE=elephant-oracle-node AWS_REGION=us-east-1 \
  aws logs tail /aws/lambda/elephant-permit-harvest-PermitHarvestWorkerFunctio-FMP5YTblMFul --since 15m --format short

AWS_PROFILE=elephant-oracle-node AWS_REGION=us-east-1 \
  aws s3 ls s3://elephant-oracle-node-environmentbucket-mmsoo3xbdi80/permit-harvest/lee-permit-backfill-20260525/ --recursive
```

Sunbiz bulk enqueue shape once real data is staged:

```bash
AWS_PROFILE=elephant-oracle-node AWS_REGION=us-east-1 \
  node scripts/enqueue-sunbiz-corporate-address-match.mjs \
    --job-id sunbiz-lee-20260525 \
    --corporate-data-s3-uri s3://elephant-oracle-node-environmentbucket-mmsoo3xbdi80/path/to/cordata.zip \
    --source-format zip \
    --address-json ./addresses.json \
    --batch-size 50 \
    --max-matches-per-address 25
```
