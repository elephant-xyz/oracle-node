#!/usr/bin/env node

import { appendFile, mkdir, readFile } from "node:fs/promises";
import { dirname } from "node:path";

import {
  HeadObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";

import {
  buildPermitOutputStem,
  captureLeePermitDetail,
  consoleLogger,
  createBrowser,
  normalizeParcelSearchValue,
  safeKeyPart,
  searchLeePermitParcel,
  shortHash,
} from "../workflow/lambdas/permit-harvest-worker/lee-accela.mjs";

/**
 * @typedef {import("../workflow/lambdas/permit-harvest-worker/lee-accela.mjs").PermitLink} PermitLink
 */

/**
 * @typedef {object} S3UriParts
 * @property {string} bucket - S3 bucket name.
 * @property {string} key - S3 object key or prefix with no trailing slash.
 */

/**
 * @typedef {object} TargetParcelCandidate
 * @property {number | null} rank - Optional manifest rank.
 * @property {string} parcelIdentifier - Raw appraiser/Accela parcel identifier for the parcel-scoped permit search.
 * @property {string} normalizedParcelIdentifier - Punctuation-free parcel identifier sent to Accela.
 * @property {string | null} requestIdentifier - Lee Appraiser folio/request identifier when already known.
 * @property {string | null} bestPermitAddress - Best known property/work-location address.
 * @property {string | null} addressBase - Unit-insensitive address base used by scoped Sunbiz loading.
 * @property {string | null} appraisalOutputS3Uri - Transformed appraisal artifact URI when this parcel has already completed appraisal extraction.
 */

/**
 * @typedef {object} HarvestOptions
 * @property {string | null} manifestPath - Optional scoped manifest containing `candidates`.
 * @property {string | null} parcelIdentifier - Optional single parcel value used when no manifest is supplied.
 * @property {string} statePath - JSONL state file used for resume/skip behavior.
 * @property {string} jobId - Stable output job identifier appended under the output prefix.
 * @property {string} outputPrefix - Base S3 prefix, usually `s3://.../permit-harvest`.
 * @property {number | null} limit - Optional maximum pending candidate count to process.
 * @property {number} maxPages - Maximum Accela result pages to capture per parcel.
 * @property {boolean} skipExisting - Skip detail JSON objects that already exist in S3.
 * @property {boolean} skipCompleted - Skip parcels already marked `parcel_completed` in the state file.
 * @property {boolean} continueOnError - Continue after one parcel fails.
 * @property {string} region - AWS region for S3 writes.
 */

/**
 * @typedef {object} DetailWriteResult
 * @property {string} recordNumber - Permit record number.
 * @property {string | null} extractedJsonS3Uri - Extracted JSON S3 URI, or `null` on error.
 * @property {string | null} rawHtmlS3Uri - Raw detail HTML S3 URI, or `null` on error.
 * @property {boolean} skipped - Whether an existing JSON object was reused.
 * @property {string | null} error - Detail capture/write error when present.
 */

const DEFAULT_OUTPUT_PREFIX =
  "s3://elephant-oracle-node-environmentbucket-mmsoo3xbdi80/permit-harvest";
const DEFAULT_STATE_PATH =
  ".loader-runs/property-first-permits/lee-permit-parcel-state.jsonl";
const DETAIL_CAPTURE_ATTEMPTS = 2;
const DETAIL_RETRY_DELAY_MS = 5000;

/**
 * Run parcel-scoped Lee Accela permit extraction for property-first cohorts.
 *
 * The script consumes a scoped appraisal manifest or one `--parcel`, searches
 * Accela by `Parcel No.` with date fields cleared, writes list/detail artifacts
 * to the same S3 layout consumed by `@elephant-xyz/query-db`, and records a
 * resumable JSONL state line per parcel.
 *
 * @returns {Promise<void>} Resolves after all selected parcel candidates finish.
 */
async function main() {
  const options = parseOptions(process.argv.slice(2));
  const candidates = await readTargetCandidates(options);
  const completed = options.skipCompleted
    ? await readCompletedParcelKeys(options.statePath)
    : new Set();
  const uncompleted = candidates.filter(
    (candidate) =>
      completed.has(candidate.normalizedParcelIdentifier) === false,
  );
  const pending = uncompleted.slice(0, options.limit ?? undefined);
  const output = resolveOutputPrefix(options.outputPrefix, options.jobId);
  const s3 = new S3Client({ region: options.region });

  await mkdir(dirname(options.statePath), { recursive: true });
  console.log(
    JSON.stringify({
      event: "lee_property_first_permit_harvest_started",
      manifestPath: options.manifestPath,
      statePath: options.statePath,
      jobId: options.jobId,
      outputPrefix: output.uri,
      candidateCount: candidates.length,
      completedCandidateCount: candidates.length - uncompleted.length,
      selectedPendingCount: pending.length,
      pendingCount: pending.length,
      maxPages: options.maxPages,
    }),
  );

  const browser = await createBrowser(consoleLogger);
  const failures = [];
  try {
    for (const [candidateIndex, candidate] of pending.entries()) {
      try {
        const stateRecord = await harvestCandidate({
          browser,
          candidate,
          output,
          s3,
          options,
        });
        await appendStateRecord(options.statePath, stateRecord);
        console.log(
          JSON.stringify({
            event:
              stateRecord.event === "parcel_no_permits"
                ? "lee_property_first_permit_harvest_no_permits"
                : "lee_property_first_permit_harvest_completed",
            parcelIdentifier: candidate.parcelIdentifier,
            normalizedParcelIdentifier: candidate.normalizedParcelIdentifier,
            rank: candidate.rank,
            completedCount: candidateIndex + 1,
            pendingCount: pending.length,
            discoveredPermitCount: stateRecord.discoveredPermitCount,
            processedPermitCount: stateRecord.processedPermitCount,
            skippedPermitCount: stateRecord.skippedPermitCount,
            errorPermitCount: stateRecord.errorPermitCount,
          }),
        );
      } catch (caught) {
        const message =
          caught instanceof Error ? caught.message : String(caught);
        failures.push(message);
        await appendStateRecord(options.statePath, {
          event: "parcel_failed",
          failedAt: new Date().toISOString(),
          jobId: options.jobId,
          parcelIdentifier: candidate.parcelIdentifier,
          normalizedParcelIdentifier: candidate.normalizedParcelIdentifier,
          requestIdentifier: candidate.requestIdentifier,
          rank: candidate.rank,
          error: message,
        });
        console.error(
          JSON.stringify({
            event: "lee_property_first_permit_harvest_failed",
            parcelIdentifier: candidate.parcelIdentifier,
            normalizedParcelIdentifier: candidate.normalizedParcelIdentifier,
            rank: candidate.rank,
            error: message,
          }),
        );
        if (options.continueOnError === false) throw caught;
      }
    }
  } finally {
    await browser.close().catch(() => undefined);
  }

  if (failures.length > 0 && options.continueOnError === false) {
    throw new Error(`${String(failures.length)} parcel harvest(s) failed`);
  }
  console.log(
    JSON.stringify({
      event: "lee_property_first_permit_harvest_finished",
      statePath: options.statePath,
      outputPrefix: output.uri,
      processedParcelCount: pending.length,
      failureCount: failures.length,
    }),
  );
}

/**
 * Harvest list pages and detail artifacts for one property candidate.
 *
 * @param {object} params - Candidate harvest inputs.
 * @param {import("puppeteer").Browser} params.browser - Browser instance reused across candidates.
 * @param {TargetParcelCandidate} params.candidate - Property-first parcel candidate.
 * @param {S3UriParts & { uri: string }} params.output - Resolved S3 output prefix.
 * @param {S3Client} params.s3 - S3 client used for artifact writes.
 * @param {HarvestOptions} params.options - CLI options.
 * @returns {Promise<Record<string, unknown>>} JSONL state record for the completed parcel.
 */
async function harvestCandidate({ browser, candidate, output, s3, options }) {
  const searchResult = await searchLeePermitParcel({
    browser,
    parcelIdentifier: candidate.normalizedParcelIdentifier,
    maxPages: options.maxPages,
    logger: consoleLogger,
  });
  if (
    searchResult.reportedTotal !== null &&
    searchResult.permits.length < searchResult.reportedTotal
  ) {
    throw new Error(
      `Parcel ${candidate.parcelIdentifier} captured ${String(searchResult.permits.length)} of ${String(searchResult.reportedTotal)} reported permits; increase --max-pages before loading`,
    );
  }
  const listPrefix = `${output.key}/lee/permit-parcel-searches/${searchResult.searchKey}`;
  for (const page of searchResult.pages) {
    await putTextObject({
      s3,
      bucket: output.bucket,
      key: `${listPrefix}/raw/page-${String(page.pageNumber).padStart(3, "0")}.html`,
      body: page.html,
      contentType: "text/html; charset=utf-8",
    });
  }
  const summaryUri = await putTextObject({
    s3,
    bucket: output.bucket,
    key: `${listPrefix}/links.json`,
    body: JSON.stringify(
      {
        schemaVersion: "permit-harvest.lee-accela.parcel.v1",
        jobId: options.jobId,
        searchKey: searchResult.searchKey,
        parcelIdentifier: candidate.parcelIdentifier,
        normalizedParcelIdentifier: candidate.normalizedParcelIdentifier,
        requestIdentifier: candidate.requestIdentifier,
        appraisalOutputS3Uri: candidate.appraisalOutputS3Uri,
        retrievedAt: new Date().toISOString(),
        reportedTotal: searchResult.reportedTotal,
        discoveredPermitCount: searchResult.permits.length,
        noResults: searchResult.noResults,
        maxPages: options.maxPages,
        permits: searchResult.permits,
      },
      null,
      2,
    ),
    contentType: "application/json; charset=utf-8",
  });
  const detailResults = await writePermitDetails({
    browser,
    candidate,
    output,
    permits: searchResult.permits,
    s3,
    skipExisting: options.skipExisting,
  });
  const detailErrors = detailResults.filter((result) => result.error !== null);
  if (detailErrors.length > 0) {
    const failedRecordNumbers = detailErrors
      .map((result) => result.recordNumber)
      .join(", ");
    throw new Error(
      `Parcel ${candidate.parcelIdentifier} is missing ${String(detailErrors.length)} permit detail artifact(s): ${failedRecordNumbers}`,
    );
  }

  return {
    event:
      searchResult.permits.length === 0
        ? "parcel_no_permits"
        : "parcel_completed",
    completedAt: new Date().toISOString(),
    jobId: options.jobId,
    rank: candidate.rank,
    parcelIdentifier: candidate.parcelIdentifier,
    normalizedParcelIdentifier: candidate.normalizedParcelIdentifier,
    requestIdentifier: candidate.requestIdentifier,
    bestPermitAddress: candidate.bestPermitAddress,
    addressBase: candidate.addressBase,
    appraisalOutputS3Uri: candidate.appraisalOutputS3Uri,
    permitListSummaryS3Uri: summaryUri,
    discoveredPermitCount: searchResult.permits.length,
    reportedTotal: searchResult.reportedTotal,
    noResults: searchResult.noResults,
    processedPermitCount: detailResults.filter(
      (result) => result.skipped === false && result.error === null,
    ).length,
    skippedPermitCount: detailResults.filter((result) => result.skipped).length,
    errorPermitCount: detailResults.filter((result) => result.error !== null)
      .length,
    detailResults,
  };
}

/**
 * Capture and write detail artifacts for permits discovered by one parcel search.
 *
 * @param {object} params - Detail write inputs.
 * @param {import("puppeteer").Browser} params.browser - Browser instance reused for detail pages.
 * @param {TargetParcelCandidate} params.candidate - Source property candidate attached to each output JSON.
 * @param {S3UriParts & { uri: string }} params.output - Resolved S3 output prefix.
 * @param {readonly PermitLink[]} params.permits - Permit links discovered by the parcel search.
 * @param {S3Client} params.s3 - S3 client used for artifact writes.
 * @param {boolean} params.skipExisting - Whether to reuse existing extracted JSON objects.
 * @returns {Promise<readonly DetailWriteResult[]>} Per-permit write results.
 */
async function writePermitDetails({
  browser,
  candidate,
  output,
  permits,
  s3,
  skipExisting,
}) {
  /** @type {DetailWriteResult[]} */
  const results = [];
  for (const permit of permits) {
    const stem = buildPermitOutputStem(permit);
    const jsonKey = `${output.key}/lee/extracted/permits/${stem}.json`;
    const htmlKey = `${output.key}/lee/raw/permit-details/${stem}.html`;
    for (let attempt = 1; attempt <= DETAIL_CAPTURE_ATTEMPTS; attempt += 1) {
      try {
        if (skipExisting && (await objectExists(s3, output.bucket, jsonKey))) {
          results.push({
            recordNumber: permit.recordNumber,
            extractedJsonS3Uri: `s3://${output.bucket}/${jsonKey}`,
            rawHtmlS3Uri: `s3://${output.bucket}/${htmlKey}`,
            skipped: true,
            error: null,
          });
          consoleLogger.info("lee_property_first_detail_skipped_existing", {
            recordNumber: permit.recordNumber,
            jsonKey,
          });
          break;
        }

        const capture = await captureLeePermitDetail({
          browser,
          permit,
          logger: consoleLogger,
        });
        const extraction = withPropertyFirstParcel(
          capture.extraction,
          candidate,
        );
        const htmlUri = await putTextObject({
          s3,
          bucket: output.bucket,
          key: htmlKey,
          body: capture.html,
          contentType: "text/html; charset=utf-8",
        });
        const jsonUri = await putTextObject({
          s3,
          bucket: output.bucket,
          key: jsonKey,
          body: JSON.stringify(
            {
              ...extraction,
              sourceSearchResult: permit,
              rawHtmlS3Uri: htmlUri,
              idempotencyKey: `lee-permit:${shortHash(permit.url)}`,
              propertyFirstTarget: {
                parcelIdentifier: candidate.parcelIdentifier,
                normalizedParcelIdentifier:
                  candidate.normalizedParcelIdentifier,
                requestIdentifier: candidate.requestIdentifier,
                bestPermitAddress: candidate.bestPermitAddress,
                addressBase: candidate.addressBase,
                appraisalOutputS3Uri: candidate.appraisalOutputS3Uri,
              },
              lexiconStatus: {
                mappedToCurrentLexicon: false,
                notes:
                  "Permit, inspection, processing-status, and public document fields are preserved for later lexicon expansion.",
              },
            },
            null,
            2,
          ),
          contentType: "application/json; charset=utf-8",
        });
        results.push({
          recordNumber: permit.recordNumber,
          extractedJsonS3Uri: jsonUri,
          rawHtmlS3Uri: htmlUri,
          skipped: false,
          error: null,
        });
        break;
      } catch (caught) {
        const message =
          caught instanceof Error ? caught.message : String(caught);
        if (attempt < DETAIL_CAPTURE_ATTEMPTS) {
          consoleLogger.warn("lee_property_first_detail_capture_retrying", {
            recordNumber: permit.recordNumber,
            parcelIdentifier: candidate.parcelIdentifier,
            attempt,
            maxAttempts: DETAIL_CAPTURE_ATTEMPTS,
            error: message,
          });
          await delay(DETAIL_RETRY_DELAY_MS);
          continue;
        }
        results.push({
          recordNumber: permit.recordNumber,
          extractedJsonS3Uri: null,
          rawHtmlS3Uri: null,
          skipped: false,
          error: message,
        });
        logDetailCaptureFailure(candidate, permit, message);
        break;
      }
    }
  }
  return results;
}

/**
 * Wait for a fixed amount of time before retrying a transient Accela action.
 *
 * @param {number} milliseconds - Delay duration in milliseconds.
 * @returns {Promise<void>} Resolves after the delay.
 */
function delay(milliseconds) {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

/**
 * Ensure every property-first permit artifact has a top-level parcel identifier
 * for the scoped Neon relinker, even when Accela's detail page is unavailable
 * and the artifact was constructed from search-result evidence. The original
 * detail payload remains otherwise unchanged; when the detail page did not
 * expose a `More Details > Parcel Number` field, the appraiser parcel used for
 * this targeted Accela search is added to `moreDetails` as explicit provenance.
 *
 * @param {Record<string, unknown> & { parcelIdentifier?: string | null, moreDetails?: unknown }} extraction - Captured permit extraction before property-first enrichment.
 * @param {TargetParcelCandidate} candidate - Property-first parcel candidate that produced this permit search.
 * @returns {Record<string, unknown>} Extraction enriched with parcel evidence required by scoped loading/linking.
 */
function withPropertyFirstParcel(extraction, candidate) {
  const existingParcel =
    typeof extraction.parcelIdentifier === "string" &&
    extraction.parcelIdentifier.trim().length > 0
      ? extraction.parcelIdentifier
      : null;
  const moreDetails = isRecord(extraction.moreDetails)
    ? extraction.moreDetails
    : {};
  return {
    ...extraction,
    parcelIdentifier: existingParcel ?? candidate.normalizedParcelIdentifier,
    moreDetails: {
      ...moreDetails,
      ...(typeof moreDetails["Parcel Number"] === "string" &&
      moreDetails["Parcel Number"].trim().length > 0
        ? {}
        : { "Parcel Number": candidate.normalizedParcelIdentifier }),
    },
  };
}

/**
 * Log a terminal permit-detail capture failure after all retry attempts have
 * been exhausted. Unlike explicit Accela error-page fallbacks, generic browser
 * timeouts are not converted into loaded permit rows because doing so would
 * risk filling Neon with low-detail records during a temporary portal outage.
 *
 * @param {TargetParcelCandidate} candidate - Parcel currently being harvested.
 * @param {PermitLink} permit - Permit whose detail capture failed.
 * @param {string} message - Human-readable browser or Accela error message.
 * @returns {void}
 */
function logDetailCaptureFailure(candidate, permit, message) {
  consoleLogger.error("lee_property_first_detail_capture_failed", {
    recordNumber: permit.recordNumber,
    parcelIdentifier: candidate.parcelIdentifier,
    error: message,
  });
}

/**
 * Read target parcel candidates from a manifest or a single `--parcel` value.
 *
 * @param {HarvestOptions} options - Parsed CLI options.
 * @returns {Promise<readonly TargetParcelCandidate[]>} Validated candidates.
 */
async function readTargetCandidates(options) {
  if (options.manifestPath !== null) {
    const parsed = JSON.parse(await readFile(options.manifestPath, "utf8"));
    if (!isRecord(parsed) || Array.isArray(parsed.candidates) === false) {
      throw new Error(
        `Manifest must contain a candidates array: ${options.manifestPath}`,
      );
    }
    const candidates = [];
    for (const [index, value] of parsed.candidates.entries()) {
      const candidate = readTargetCandidate(value, index);
      if (candidate !== null) candidates.push(candidate);
    }
    return dedupeCandidates(candidates);
  }
  if (options.parcelIdentifier !== null) {
    const normalizedParcelIdentifier = normalizeParcelSearchValue(
      options.parcelIdentifier,
    );
    if (normalizedParcelIdentifier === null) {
      throw new Error(`Invalid --parcel value: ${options.parcelIdentifier}`);
    }
    return [
      {
        rank: 1,
        parcelIdentifier: options.parcelIdentifier,
        normalizedParcelIdentifier,
        requestIdentifier: null,
        bestPermitAddress: null,
        addressBase: null,
        appraisalOutputS3Uri: null,
      },
    ];
  }
  throw new Error("Either --manifest or --parcel is required");
}

/**
 * Convert one manifest candidate into a parcel-harvest target.
 *
 * @param {unknown} value - Raw manifest candidate.
 * @param {number} index - Zero-based candidate index used as a fallback rank.
 * @returns {TargetParcelCandidate | null} Candidate when it contains a usable parcel identifier.
 */
function readTargetCandidate(value, index) {
  if (!isRecord(value)) return null;
  const rawParcel = readString(
    value.parcelIdentifier ?? value.parcel_identifier,
  );
  const normalizedParcelIdentifier = normalizeParcelSearchValue(rawParcel);
  if (rawParcel === null || normalizedParcelIdentifier === null) return null;
  return {
    rank: readNumber(value.rank) ?? index + 1,
    parcelIdentifier: rawParcel,
    normalizedParcelIdentifier,
    requestIdentifier: readString(
      value.requestIdentifier ?? value.request_identifier,
    ),
    bestPermitAddress: readString(
      value.bestPermitAddress ?? value.best_permit_address,
    ),
    addressBase: readString(value.addressBase ?? value.address_base),
    appraisalOutputS3Uri: readString(
      value.appraisalOutputS3Uri ?? value.source_artifact_uri,
    ),
  };
}

/**
 * Deduplicate candidates by normalized parcel while preserving first-seen order.
 *
 * @param {readonly TargetParcelCandidate[]} candidates - Candidate list.
 * @returns {readonly TargetParcelCandidate[]} Deduplicated candidates.
 */
function dedupeCandidates(candidates) {
  const seen = new Set();
  const deduped = [];
  for (const candidate of candidates) {
    if (seen.has(candidate.normalizedParcelIdentifier)) continue;
    seen.add(candidate.normalizedParcelIdentifier);
    deduped.push(candidate);
  }
  return deduped;
}

/**
 * Read completed parcel keys from a JSONL state file if it exists.
 *
 * @param {string} statePath - JSONL state file path.
 * @returns {Promise<Set<string>>} Normalized parcel identifiers marked completed.
 */
async function readCompletedParcelKeys(statePath) {
  try {
    const text = await readFile(statePath, "utf8");
    const completed = new Set();
    for (const line of text.split(/\r?\n/)) {
      if (line.trim().length === 0) continue;
      const parsed = JSON.parse(line);
      if (
        !isRecord(parsed) ||
        (parsed.event !== "parcel_completed" &&
          parsed.event !== "parcel_no_permits")
      ) {
        continue;
      }
      const normalizedParcelIdentifier = normalizeParcelSearchValue(
        parsed.normalizedParcelIdentifier ?? parsed.parcelIdentifier,
      );
      if (normalizedParcelIdentifier !== null)
        completed.add(normalizedParcelIdentifier);
    }
    return completed;
  } catch (caught) {
    if (isRecord(caught) && caught.code === "ENOENT") return new Set();
    throw caught;
  }
}

/**
 * Append one JSON-serializable state record to a JSONL file.
 *
 * @param {string} statePath - JSONL state path.
 * @param {Record<string, unknown>} record - State record to append.
 * @returns {Promise<void>} Resolves after the record is written.
 */
async function appendStateRecord(statePath, record) {
  await appendFile(statePath, `${JSON.stringify(record)}\n`, "utf8");
}

/**
 * Parse an S3 URI into bucket and key components.
 *
 * @param {string} uri - S3 URI in `s3://bucket/key` format.
 * @returns {S3UriParts} Parsed parts.
 */
function parseS3Uri(uri) {
  const match = /^s3:\/\/([^/]+)\/(.*)$/i.exec(uri);
  if (!match) throw new Error(`Invalid S3 URI: ${uri}`);
  return { bucket: match[1], key: match[2].replace(/\/$/, "") };
}

/**
 * Append a job id to the base S3 output prefix.
 *
 * @param {string} outputPrefix - Base S3 output prefix.
 * @param {string} jobId - Stable job id.
 * @returns {S3UriParts & { uri: string }} Resolved prefix.
 */
function resolveOutputPrefix(outputPrefix, jobId) {
  const base = outputPrefix.replace(/\/$/, "");
  const uri = `${base}/${safeKeyPart(jobId)}`;
  return { uri, ...parseS3Uri(uri) };
}

/**
 * Write one UTF-8 text object to S3.
 *
 * @param {object} params - Upload parameters.
 * @param {S3Client} params.s3 - S3 client.
 * @param {string} params.bucket - Destination bucket.
 * @param {string} params.key - Destination key.
 * @param {string} params.body - UTF-8 body.
 * @param {string} params.contentType - Object content type.
 * @returns {Promise<string>} S3 URI of the written object.
 */
async function putTextObject({ s3, bucket, key, body, contentType }) {
  await s3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: Buffer.from(body, "utf8"),
      ContentType: contentType,
    }),
  );
  return `s3://${bucket}/${key}`;
}

/**
 * Check whether an S3 object already exists.
 *
 * @param {S3Client} s3 - S3 client.
 * @param {string} bucket - Bucket name.
 * @param {string} key - Object key.
 * @returns {Promise<boolean>} True when the object exists.
 */
async function objectExists(s3, bucket, key) {
  try {
    await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
    return true;
  } catch (caught) {
    if (
      caught instanceof Error &&
      (caught.name === "NotFound" || caught.name === "NoSuchKey")
    ) {
      return false;
    }
    if (
      isRecord(caught) &&
      isRecord(caught.$metadata) &&
      caught.$metadata.httpStatusCode === 404
    ) {
      return false;
    }
    throw caught;
  }
}

/**
 * Parse command-line flags.
 *
 * @param {readonly string[]} args - CLI args after script name.
 * @returns {HarvestOptions} Normalized options.
 */
function parseOptions(args) {
  const values = readCliValues(args);
  const jobId =
    values.get("job-id") ??
    `lee-property-first-${new Date()
      .toISOString()
      .replace(/[^0-9]/g, "")
      .slice(0, 14)}`;
  return {
    manifestPath: values.get("manifest") ?? null,
    parcelIdentifier: values.get("parcel") ?? null,
    statePath: values.get("state") ?? DEFAULT_STATE_PATH,
    jobId,
    outputPrefix: values.get("output-prefix") ?? DEFAULT_OUTPUT_PREFIX,
    limit: parsePositiveInteger(values.get("limit"), "limit"),
    maxPages: parsePositiveInteger(values.get("max-pages"), "max-pages") ?? 50,
    skipExisting: parseBoolean(values.get("skip-existing"), true),
    skipCompleted: parseBoolean(values.get("skip-completed"), true),
    continueOnError: parseBoolean(values.get("continue-on-error"), true),
    region: values.get("region") ?? process.env.AWS_REGION ?? "us-east-1",
  };
}

/**
 * Convert `--key value` and `--key=value` CLI args into a map.
 *
 * @param {readonly string[]} args - Raw CLI args.
 * @returns {Map<string, string>} Parsed key/value map.
 */
function readCliValues(args) {
  const values = new Map();
  for (let index = 0; index < args.length; index += 1) {
    const raw = args[index];
    if (raw === undefined || raw.startsWith("--") === false) continue;
    const equalsIndex = raw.indexOf("=");
    if (equalsIndex > 2) {
      values.set(raw.slice(2, equalsIndex), raw.slice(equalsIndex + 1));
      continue;
    }
    const key = raw.slice(2);
    const next = args[index + 1];
    if (next !== undefined && next.startsWith("--") === false) {
      values.set(key, next);
      index += 1;
    } else {
      values.set(key, "true");
    }
  }
  return values;
}

/**
 * Parse an optional positive integer CLI field.
 *
 * @param {string | undefined} value - Raw field value.
 * @param {string} fieldName - Field name used in errors.
 * @returns {number | null} Parsed positive integer or `null` when omitted.
 */
function parsePositiveInteger(value, fieldName) {
  if (value === undefined || value.trim().length === 0) return null;
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0)
    throw new Error(`Invalid --${fieldName}: ${value}`);
  return parsed;
}

/**
 * Parse an optional boolean CLI field.
 *
 * @param {string | undefined} value - Raw field value.
 * @param {boolean} defaultValue - Default when the field is omitted.
 * @returns {boolean} Parsed boolean.
 */
function parseBoolean(value, defaultValue) {
  if (value === undefined) return defaultValue;
  if (["true", "1", "yes", "y"].includes(value.toLowerCase())) return true;
  if (["false", "0", "no", "n"].includes(value.toLowerCase())) return false;
  throw new Error(`Invalid boolean value: ${value}`);
}

/**
 * Check if an unknown value is a non-null object record.
 *
 * @param {unknown} value - Unknown value.
 * @returns {value is Record<string, unknown>} True for object records.
 */
function isRecord(value) {
  return (
    value !== null &&
    typeof value === "object" &&
    Array.isArray(value) === false
  );
}

/**
 * Read a non-empty string from an unknown value.
 *
 * @param {unknown} value - Unknown value.
 * @returns {string | null} String value, or `null`.
 */
function readString(value) {
  return typeof value === "string" && value.trim().length > 0
    ? value.trim()
    : null;
}

/**
 * Read a finite number from an unknown value.
 *
 * @param {unknown} value - Unknown value.
 * @returns {number | null} Numeric value, or `null`.
 */
function readNumber(value) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((caught) => {
    const message = caught instanceof Error ? caught.message : String(caught);
    console.error(
      JSON.stringify({
        event: "lee_property_first_permit_harvest_failed",
        error: message,
      }),
    );
    process.exitCode = 1;
  });
}
