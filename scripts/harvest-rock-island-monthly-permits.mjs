#!/usr/bin/env node
// @ts-check

import { createHash } from "node:crypto";
import {
  chmod,
  mkdir,
  readFile,
  rename,
  stat,
  writeFile,
} from "node:fs/promises";
import { basename, dirname, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

import {
  dedupeAndSortMonthlyPermits,
  extractPositionedPdfText,
  extractRockIslandReportLinks,
  parseRockIslandMonthlyReport,
  renderMonthlyPermitJsonl,
  REPORT_INDEX_URL,
  REPORT_SOURCE_SYSTEM,
} from "./permit-source-adapters/rock-island-monthly-reports.mjs";

/**
 * @typedef {"pilot" | "full"} HarvestMode
 */

/**
 * @typedef {object} HarvestOptions
 * @property {HarvestMode} mode - Bounded pilot or all-indexed-report harvest.
 * @property {string} outputDirectory - Gitignored private staging directory.
 * @property {number} delayMs - Delay between live PDF downloads.
 * @property {number} maxRecords - Pilot output ceiling; ignored in full mode.
 * @property {string | null} reportId - Optional pilot report selector.
 */

/**
 * @typedef {object} ReportRunProvenance
 * @property {string} documentId - CivicPlus document identifier.
 * @property {string} title - Official report-index title.
 * @property {string} sourceUrl - Official PDF URL.
 * @property {string} localPath - Private local raw PDF path.
 * @property {boolean} reused - Whether a previously downloaded PDF was reused.
 * @property {number} downloadMs - Live download duration, or zero when reused.
 * @property {number} parseMs - PDF extraction and normalization duration.
 * @property {number} byteCount - Raw PDF size.
 * @property {string} sha256 - Raw PDF SHA-256.
 * @property {number} parsedRecordCount - Parsed source rows before cross-report deduplication.
 * @property {string | null} earliestIssueDate - Earliest parsed date.
 * @property {string | null} latestIssueDate - Latest parsed date.
 */

const DEFAULT_OUTPUT_DIRECTORY =
  "downloads/rock-island/permit-harvest/monthly-reports";
const MIN_DELAY_MS = 1_000;
const MAX_PILOT_RECORDS = 25;
const USER_AGENT = "oracle-node-rock-island-permit-harvest/1.0";
const USAGE = `Usage:
  node scripts/harvest-rock-island-monthly-permits.mjs \\
    --mode pilot|full \\
    [--output-dir ${DEFAULT_OUTPUT_DIRECTORY}] \\
    [--delay-ms 1500] [--max-records 25] [--report-id <document-id>]

Safety:
  - Reads only the official City of Rock Island permit-report index and PDFs.
  - Downloads serially with at least 1000 ms between live PDF requests.
  - Pilot mode refuses more than 25 output records.
  - Reuses raw PDFs by document id for resumable local runs.
  - Writes mode 0600 private artifacts under a gitignored downloads directory.
  - Never uses AWS, queues, EC2, PostgreSQL, Neon, IPFS, or MCP publication.
`;

/**
 * Read a required CLI value.
 *
 * @param {readonly string[]} args - Raw CLI arguments.
 * @param {number} index - Current argument index.
 * @param {string} flag - Flag name used in errors.
 * @returns {{value: string, nextIndex: number}} Parsed value and consumed index.
 */
function readFollowingValue(args, index, flag) {
  const value = args[index + 1];
  if (value === undefined || value.startsWith("--")) {
    throw new Error(`${flag} requires a value`);
  }
  return { value, nextIndex: index + 1 };
}

/**
 * Parse a bounded positive integer.
 *
 * @param {string} value - Candidate integer text.
 * @param {string} flag - Flag name.
 * @param {number} minimum - Inclusive minimum.
 * @param {number | null} maximum - Optional inclusive maximum.
 * @returns {number} Validated integer.
 */
function parseInteger(value, flag, minimum, maximum) {
  const parsed = Number(value);
  if (
    !Number.isInteger(parsed) ||
    parsed < minimum ||
    (maximum !== null && parsed > maximum)
  ) {
    const range =
      maximum === null
        ? `at least ${String(minimum)}`
        : `from ${String(minimum)} through ${String(maximum)}`;
    throw new Error(`${flag} must be an integer ${range}`);
  }
  return parsed;
}

/**
 * Parse the local-only harvest CLI.
 *
 * @param {readonly string[]} args - Arguments after the script name.
 * @returns {HarvestOptions | null} Parsed options, or null for help.
 */
export function parseOptions(args) {
  /** @type {HarvestMode | null} */
  let mode = null;
  let outputDirectory = DEFAULT_OUTPUT_DIRECTORY;
  let delayMs = 1_500;
  let maxRecords = MAX_PILOT_RECORDS;
  let reportId = null;

  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index];
    if (argument === "--help" || argument === "-h") return null;
    const readValue = (/** @type {string} */ flag) => {
      const parsed = readFollowingValue(args, index, flag);
      index = parsed.nextIndex;
      return parsed.value;
    };

    if (argument === "--mode" || argument.startsWith("--mode=")) {
      const value =
        argument === "--mode"
          ? readValue("--mode")
          : argument.slice("--mode=".length);
      if (value !== "pilot" && value !== "full") {
        throw new Error("--mode must be pilot or full");
      }
      mode = value;
      continue;
    }
    if (argument === "--output-dir" || argument.startsWith("--output-dir=")) {
      outputDirectory =
        argument === "--output-dir"
          ? readValue("--output-dir")
          : argument.slice("--output-dir=".length);
      continue;
    }
    if (argument === "--delay-ms" || argument.startsWith("--delay-ms=")) {
      const value =
        argument === "--delay-ms"
          ? readValue("--delay-ms")
          : argument.slice("--delay-ms=".length);
      delayMs = parseInteger(value, "--delay-ms", MIN_DELAY_MS, null);
      continue;
    }
    if (argument === "--max-records" || argument.startsWith("--max-records=")) {
      const value =
        argument === "--max-records"
          ? readValue("--max-records")
          : argument.slice("--max-records=".length);
      maxRecords = parseInteger(value, "--max-records", 1, MAX_PILOT_RECORDS);
      continue;
    }
    if (argument === "--report-id" || argument.startsWith("--report-id=")) {
      reportId =
        argument === "--report-id"
          ? readValue("--report-id")
          : argument.slice("--report-id=".length);
      if (!/^\d+$/.test(reportId)) {
        throw new Error("--report-id must be a numeric DocumentCenter id");
      }
      continue;
    }
    throw new Error(`Unknown option: ${argument}`);
  }

  if (mode === null) throw new Error("--mode is required");
  if (outputDirectory.trim().length === 0) {
    throw new Error("--output-dir must not be empty");
  }
  if (mode === "full" && reportId !== null) {
    throw new Error("--report-id is only valid in pilot mode");
  }
  return {
    mode,
    outputDirectory: outputDirectory.trim(),
    delayMs,
    maxRecords,
    reportId,
  };
}

/**
 * Pause between official source requests.
 *
 * @param {number} milliseconds - Delay duration.
 * @returns {Promise<void>} Resolves after the delay.
 */
function delay(milliseconds) {
  return new Promise((resolveDelay) => {
    setTimeout(resolveDelay, milliseconds);
  });
}

/**
 * Fetch one official source with a finite timeout and explicit user agent.
 *
 * @param {string} url - Official HTTPS URL.
 * @param {string} expectedKind - Description used in errors.
 * @returns {Promise<Response>} Successful HTTP response.
 */
async function fetchOfficialSource(url, expectedKind) {
  const response = await fetch(url, {
    headers: {
      "user-agent": USER_AGENT,
      accept:
        expectedKind === "PDF"
          ? "application/pdf"
          : "text/html,application/xhtml+xml",
    },
    signal: AbortSignal.timeout(60_000),
    redirect: "follow",
  });
  if (!response.ok) {
    throw new Error(
      `${expectedKind} request failed with HTTP ${String(response.status)}: ${url}`,
    );
  }
  return response;
}

/**
 * Determine whether a raw PDF already exists and is reusable.
 *
 * @param {string} path - Candidate local PDF path.
 * @returns {Promise<boolean>} True only for a non-empty `%PDF` file.
 */
async function hasReusablePdf(path) {
  try {
    const metadata = await stat(path);
    if (!metadata.isFile() || metadata.size < 5) return false;
    const bytes = await readFile(path);
    return bytes.subarray(0, 4).toString("ascii") === "%PDF";
  } catch (caught) {
    if (
      caught instanceof Error &&
      "code" in caught &&
      caught.code === "ENOENT"
    ) {
      return false;
    }
    throw caught;
  }
}

/**
 * Write a private artifact atomically with owner-only permissions.
 *
 * @param {string} path - Final artifact path.
 * @param {string | Uint8Array} content - Complete artifact content.
 * @returns {Promise<void>} Resolves after replacement.
 */
async function writePrivateFile(path, content) {
  await mkdir(dirname(path), { recursive: true });
  const temporaryPath = `${path}.tmp-${String(process.pid)}`;
  await writeFile(temporaryPath, content, { mode: 0o600 });
  await rename(temporaryPath, path);
  await chmod(path, 0o600);
}

/**
 * Download or reuse one report PDF.
 *
 * @param {{documentId: string, url: string}} report - Official report identity.
 * @param {string} rawDirectory - Private raw-PDF directory.
 * @returns {Promise<{path: string, bytes: Uint8Array, reused: boolean, downloadMs: number}>} Local source artifact.
 */
async function obtainReportPdf(report, rawDirectory) {
  const path = join(rawDirectory, `${report.documentId}.pdf`);
  if (await hasReusablePdf(path)) {
    return {
      path,
      bytes: new Uint8Array(await readFile(path)),
      reused: true,
      downloadMs: 0,
    };
  }

  const started = Date.now();
  const response = await fetchOfficialSource(report.url, "PDF");
  const bytes = new Uint8Array(await response.arrayBuffer());
  if (Buffer.from(bytes.subarray(0, 4)).toString("ascii") !== "%PDF") {
    throw new Error(`Official report did not return PDF bytes: ${report.url}`);
  }
  await writePrivateFile(path, bytes);
  return { path, bytes, reused: false, downloadMs: Date.now() - started };
}

/**
 * Return the minimum and maximum non-null ISO dates.
 *
 * @param {readonly {permit_issue_date: string | null}[]} records - Parsed records.
 * @returns {{earliest: string | null, latest: string | null}} Date coverage.
 */
function readDateCoverage(records) {
  const dates = records
    .map((record) => record.permit_issue_date)
    .filter((value) => value !== null)
    .sort((left, right) => left.localeCompare(right));
  return {
    earliest: dates[0] ?? null,
    latest: dates[dates.length - 1] ?? null,
  };
}

/**
 * Run the bounded or full local official-report harvest.
 *
 * @param {HarvestOptions} options - Validated CLI options.
 * @returns {Promise<void>} Resolves after private artifacts and summary are written.
 */
export async function runHarvest(options) {
  const outputDirectory = resolve(options.outputDirectory);
  const rawDirectory = join(outputDirectory, "raw", "reports");
  await mkdir(rawDirectory, { recursive: true, mode: 0o700 });
  await chmod(outputDirectory, 0o700).catch(() => undefined);
  await chmod(dirname(rawDirectory), 0o700);
  await chmod(rawDirectory, 0o700);

  const runStarted = Date.now();
  const indexStarted = Date.now();
  const indexResponse = await fetchOfficialSource(REPORT_INDEX_URL, "HTML");
  const indexHtml = await indexResponse.text();
  const indexMs = Date.now() - indexStarted;
  const allReports = extractRockIslandReportLinks(indexHtml);
  if (allReports.length === 0) {
    throw new Error("Official permit-report index contained no report links");
  }

  const selectedReports =
    options.mode === "full"
      ? allReports
      : [
          options.reportId === null
            ? allReports[allReports.length - 1]
            : allReports.find(
                (report) => report.documentId === options.reportId,
              ),
        ].filter((report) => report !== undefined);
  if (selectedReports.length === 0) {
    throw new Error(
      `Pilot report id ${String(options.reportId)} was not present on the official index`,
    );
  }

  /** @type {import("./permit-source-adapters/rock-island-monthly-reports.mjs").RockIslandMonthlyPermit[]} */
  const parsedRecords = [];
  /** @type {ReportRunProvenance[]} */
  const reports = [];

  for (const [index, report] of selectedReports.entries()) {
    const obtained = await obtainReportPdf(report, rawDirectory);
    const byteCount = obtained.bytes.byteLength;
    const sha256 = createHash("sha256").update(obtained.bytes).digest("hex");
    const parseStarted = Date.now();
    const pages = await extractPositionedPdfText(obtained.bytes);
    const reportRecords = parseRockIslandMonthlyReport(pages, report);
    const parseMs = Date.now() - parseStarted;
    if (reportRecords.length === 0) {
      throw new Error(
        `No permit rows parsed from official report ${report.documentId}`,
      );
    }
    parsedRecords.push(...reportRecords);
    const coverage = readDateCoverage(reportRecords);
    reports.push({
      documentId: report.documentId,
      title: report.title,
      sourceUrl: report.url,
      localPath: obtained.path,
      reused: obtained.reused,
      downloadMs: obtained.downloadMs,
      parseMs,
      byteCount,
      sha256,
      parsedRecordCount: reportRecords.length,
      earliestIssueDate: coverage.earliest,
      latestIssueDate: coverage.latest,
    });
    if (obtained.reused === false && index < selectedReports.length - 1) {
      await delay(options.delayMs);
    }
  }

  const uniqueRecords = dedupeAndSortMonthlyPermits(parsedRecords);
  const outputRecords =
    options.mode === "pilot"
      ? uniqueRecords.slice(0, options.maxRecords)
      : uniqueRecords;
  const coverage = readDateCoverage(outputRecords);
  const recordsPath = join(
    outputDirectory,
    `${options.mode}-records.private.jsonl`,
  );
  const provenancePath = join(
    outputDirectory,
    `${options.mode}-source-provenance.json`,
  );
  const summaryPath = join(outputDirectory, `${options.mode}-summary.json`);
  await writePrivateFile(recordsPath, renderMonthlyPermitJsonl(outputRecords));
  await writePrivateFile(
    provenancePath,
    `${JSON.stringify(
      {
        sourceSystem: REPORT_SOURCE_SYSTEM,
        indexUrl: REPORT_INDEX_URL,
        indexHttpStatus: indexResponse.status,
        indexFetchedAt: new Date().toISOString(),
        indexSha256: createHash("sha256").update(indexHtml).digest("hex"),
        indexedReportCount: allReports.length,
        selectedReportCount: selectedReports.length,
        serialDelayMs: options.delayMs,
        reports,
      },
      null,
      2,
    )}\n`,
  );
  const summary = {
    event: "rock_island_monthly_permit_harvest_completed",
    mode: options.mode,
    sourceSystem: REPORT_SOURCE_SYSTEM,
    indexedReportCount: allReports.length,
    selectedReportCount: selectedReports.length,
    parsedSourceRecordCount: parsedRecords.length,
    uniqueParsedRecordCount: uniqueRecords.length,
    outputRecordCount: outputRecords.length,
    duplicateRecordCount: parsedRecords.length - uniqueRecords.length,
    recordsWithIssueDate: outputRecords.filter(
      (record) => record.permit_issue_date !== null,
    ).length,
    recordsWithWorkLocation: outputRecords.filter(
      (record) => record.work_location !== null,
    ).length,
    recordsWithSourceTaxMap: outputRecords.filter(
      (record) => record.raw.source_tax_map !== null,
    ).length,
    explicitlyMatchedParcelCount: outputRecords.filter(
      (record) => record.parcel_identifier !== null,
    ).length,
    recordsWithDescription: outputRecords.filter(
      (record) => record.project_description !== null,
    ).length,
    recordsWithValuation: outputRecords.filter(
      (record) => record.raw.project_valuation !== null,
    ).length,
    recordsWithContractorBusiness: outputRecords.filter(
      (record) => record.contractor_business_names.length > 0,
    ).length,
    earliestIssueDate: coverage.earliest,
    latestIssueDate: coverage.latest,
    indexMs,
    elapsedMs: Date.now() - runStarted,
    recordsPath,
    provenancePath,
    summaryPath,
    privacy:
      "Private staging only; owner/person rows and contact fields are excluded. Descriptions, addresses, and contractor business candidates are not publication-approved.",
  };
  await writePrivateFile(summaryPath, `${JSON.stringify(summary, null, 2)}\n`);
  process.stdout.write(`${JSON.stringify(summary)}\n`);
}

/**
 * CLI entry point.
 *
 * @returns {Promise<void>} Resolves after the requested harvest.
 */
export async function main() {
  const options = parseOptions(process.argv.slice(2));
  if (options === null) {
    process.stdout.write(USAGE);
    return;
  }
  await runHarvest(options);
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main().catch((caught) => {
    process.stderr.write(
      `${JSON.stringify({
        event: "rock_island_monthly_permit_harvest_failed",
        script: basename(process.argv[1] ?? "unknown"),
        error: caught instanceof Error ? caught.message : String(caught),
      })}\n`,
    );
    process.exitCode = 1;
  });
}
