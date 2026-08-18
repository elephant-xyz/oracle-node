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
  dedupeMolineIssuedPermits,
  extractMolineReportLinks,
  isCompactedLegacyMolineReportLayout,
  isCurrentMolineReportLayout,
  isLegacyApplicationMolineReportLayout,
  isRotatedLegacyMolineReportLayout,
  MOLINE_REPORT_INDEX_URL,
  MOLINE_REPORT_SOURCE_SYSTEM,
  molinePermitLoaderKey,
  parseMolineIssuedPermitReport,
  readMolinePrintedPermitTotal,
  renderMolinePrivateJsonl,
  renderMolinePublicJsonl,
} from "./permit-source-adapters/moline-issued-permit-reports.mjs";
import { extractPositionedPdfText } from "./permit-source-adapters/rock-island-monthly-reports.mjs";

/**
 * @typedef {"current-2025" | "supported"} HarvestScope
 */

/**
 * @typedef {"blocked-early-identity" | "blocked-metadata-mismatch" | "blocked-date-range-conflict" | "legacy-2017-2020" | "blocked-compacted-2020-2021" | "blocked-compacted-isolated" | "legacy-2021-2024" | "current-transition-2024" | "current-2025"} ArchiveEra
 */

/**
 * @typedef {object} HarvestOptions
 * @property {HarvestScope} scope - Current 18 reports or every supported report era.
 * @property {string} outputDirectory - Local gitignored package directory.
 * @property {number} delayMs - Minimum delay between live report downloads.
 */

/**
 * @typedef {object} ReportProvenance
 * @property {string} archiveId - CivicPlus Archive Center document id.
 * @property {string} reportMonth - Official archive month.
 * @property {string} title - Official archive title.
 * @property {string} sourceUrl - Canonical official report URL.
 * @property {ArchiveEra} era - Reviewed archive era.
 * @property {"current-2024-10" | "current-2024-10-no-value" | "legacy-application-v1" | "legacy-rotated-v2"} parserLayout - Parser selected from source headers.
 * @property {string} localPath - Owner-only local raw PDF path.
 * @property {boolean} reused - Whether the raw source was already staged.
 * @property {number} downloadMs - Live download duration, or zero when reused.
 * @property {number} parseMs - PDF extraction and parsing duration.
 * @property {number} byteCount - Raw PDF bytes.
 * @property {string} sha256 - Raw PDF SHA-256.
 * @property {number} parsedRowCount - Source rows before loader-key deduplication.
 * @property {number} uniqueLoaderKeyCount - Unique loader identities in this report.
 * @property {number} duplicateRowCount - Exact repeated rows, normally report page breaks.
 * @property {number | null} printedPermitTotal - Source's printed report total.
 * @property {number | null} completenessDelta - Unique identities minus printed total.
 * @property {number} outsideArchiveMonthCount - Parsed rows whose issue month differs from the archive label.
 * @property {readonly string[]} issueDateMonths - Distinct issue months printed in the report.
 * @property {string} earliestIssueDate - Earliest parsed issue date.
 * @property {string} latestIssueDate - Latest parsed issue date.
 */

const DEFAULT_OUTPUT_DIRECTORY =
  "downloads/rock-island/permit-harvest/moline-archive-2026-08-14";
const MIN_DELAY_MS = 1_000;
const USER_AGENT = "oracle-node-moline-permit-harvest/1.0";
const LAST_AUTHORIZED_MONTH = "2026-06";
const USAGE = `Usage:
  node scripts/harvest-moline-issued-permits.mjs \\
    --scope current-2025|supported \\
    [--output-dir ${DEFAULT_OUTPUT_DIRECTORY}] [--delay-ms 1200]

Safety:
  - Reads only Moline's official CivicPlus Archive Center index and PDFs.
  - Downloads serially with at least 1000 ms between live report requests.
  - Reuses PDFs by official archive id for deterministic, resumable local runs.
  - Excludes blocked legacy eras instead of inventing application identities.
  - Writes owner-only local artifacts and never touches cloud or database services.
`;

/**
 * Parse one finite integer.
 *
 * @param {string} value - Candidate CLI value.
 * @param {string} flag - Flag used in errors.
 * @param {number} minimum - Inclusive minimum.
 * @returns {number} Valid integer.
 */
function readInteger(value, flag, minimum) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < minimum) {
    throw new Error(
      `${flag} must be an integer of at least ${String(minimum)}`,
    );
  }
  return parsed;
}

/**
 * Parse local-only harvest options.
 *
 * @param {readonly string[]} args - Arguments after the script path.
 * @returns {HarvestOptions | null} Validated options, or null for help.
 */
export function parseOptions(args) {
  /** @type {HarvestScope | null} */
  let scope = null;
  let outputDirectory = DEFAULT_OUTPUT_DIRECTORY;
  let delayMs = 1_200;
  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index];
    if (argument === "--help" || argument === "-h") return null;
    const readValue = (/** @type {string} */ flag) => {
      const value = args[index + 1];
      if (value === undefined || value.startsWith("--")) {
        throw new Error(`${flag} requires a value`);
      }
      index += 1;
      return value;
    };
    if (argument === "--scope" || argument.startsWith("--scope=")) {
      const value =
        argument === "--scope"
          ? readValue("--scope")
          : argument.slice("--scope=".length);
      if (value !== "current-2025" && value !== "supported") {
        throw new Error("--scope must be current-2025 or supported");
      }
      scope = value;
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
      delayMs = readInteger(value, "--delay-ms", MIN_DELAY_MS);
      continue;
    }
    throw new Error(`Unknown option: ${argument}`);
  }
  if (scope === null) throw new Error("--scope is required");
  if (outputDirectory.trim().length === 0) {
    throw new Error("--output-dir must not be empty");
  }
  return { scope, outputDirectory: outputDirectory.trim(), delayMs };
}

/**
 * Keep current-only and full-archive combined artifacts at distinct paths.
 *
 * @param {HarvestScope} scope - Requested harvest scope.
 * @returns {{ privateFileName: string, publicFileName: string }} Deterministic output names.
 */
export function combinedArtifactFileNames(scope) {
  return scope === "supported"
    ? {
        privateFileName: "load-ready.private.jsonl",
        publicFileName: "public-allowlist.jsonl",
      }
    : {
        privateFileName: "current-2025-load-ready.private.jsonl",
        publicFileName: "current-2025-combined.public-allowlist.jsonl",
      };
}

/**
 * Classify one official archive month into a reviewed parsing era.
 *
 * @param {string} reportMonth - Official `YYYY-MM` month.
 * @returns {ArchiveEra} Stable era label.
 */
export function classifyArchiveEra(reportMonth) {
  if (reportMonth < "2017-01") return "blocked-early-identity";
  if (reportMonth <= "2020-04") return "legacy-2017-2020";
  if (reportMonth <= "2021-02") return "blocked-compacted-2020-2021";
  if (reportMonth <= "2024-09") return "legacy-2021-2024";
  if (reportMonth <= "2024-12") return "current-transition-2024";
  return "current-2025";
}

/**
 * Classify one indexed report, including document-specific official metadata defects.
 *
 * Archive item 4042 is labelled May 2017 by the index but serves a split-page April 2020
 * report. It is isolated instead of assigning its rows to either month.
 *
 * @param {{archiveId: string, reportMonth: string}} report - Indexed report.
 * @returns {ArchiveEra} Reviewed report era.
 */
function classifyReportEra(report) {
  if (report.archiveId === "4042") return "blocked-metadata-mismatch";
  if (report.archiveId === "4729") return "blocked-date-range-conflict";
  if (report.archiveId === "8234") return "blocked-compacted-isolated";
  return classifyArchiveEra(report.reportMonth);
}

/**
 * Decide whether one era belongs in the requested scope.
 *
 * @param {ArchiveEra} era - Reviewed era.
 * @param {HarvestScope} scope - Requested local package scope.
 * @returns {boolean} True when the report should be downloaded and parsed.
 */
function isSelectedEra(era, scope) {
  if (scope === "current-2025") return era === "current-2025";
  return (
    era === "legacy-2017-2020" ||
    era === "legacy-2021-2024" ||
    era === "current-transition-2024" ||
    era === "current-2025"
  );
}

/**
 * Sleep between official source requests.
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
 * Fetch one official source with finite timeout and explicit identity.
 *
 * @param {string} url - Official HTTPS URL.
 * @param {"HTML" | "PDF"} kind - Expected source kind.
 * @returns {Promise<Response>} Successful response.
 */
async function fetchOfficialSource(url, kind) {
  const response = await fetch(url, {
    headers: {
      "user-agent": USER_AGENT,
      accept:
        kind === "PDF" ? "application/pdf" : "text/html,application/xhtml+xml",
    },
    redirect: "follow",
    signal: AbortSignal.timeout(60_000),
  });
  if (!response.ok) {
    throw new Error(
      `${kind} request failed with HTTP ${String(response.status)}: ${url}`,
    );
  }
  return response;
}

/**
 * Write an owner-only file atomically.
 *
 * @param {string} path - Final local path.
 * @param {string | Uint8Array} content - Complete file content.
 * @returns {Promise<void>} Resolves after replacement.
 */
async function writePrivateFile(path, content) {
  await mkdir(dirname(path), { recursive: true, mode: 0o700 });
  const temporaryPath = `${path}.tmp-${String(process.pid)}`;
  await writeFile(temporaryPath, content, { mode: 0o600 });
  await rename(temporaryPath, path);
  await chmod(path, 0o600);
}

/**
 * Determine whether a cached raw PDF is reusable.
 *
 * @param {string} path - Candidate local file.
 * @returns {Promise<boolean>} True for a non-empty PDF.
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
 * Download or reuse one official report.
 *
 * @param {{archiveId: string, url: string}} report - Official report identity.
 * @param {string} rawDirectory - Owner-only raw source directory.
 * @returns {Promise<{path: string, bytes: Uint8Array, reused: boolean, downloadMs: number}>} Raw source.
 */
async function obtainReportPdf(report, rawDirectory) {
  const path = join(rawDirectory, `${report.archiveId}.pdf`);
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
 * Return deterministic date coverage.
 *
 * @param {readonly import("./permit-source-adapters/moline-issued-permit-reports.mjs").MolineIssuedPermit[]} records - Parsed records.
 * @returns {{earliest: string, latest: string}} Inclusive coverage.
 */
function readDateCoverage(records) {
  const dates = records.map((record) => record.permit_issue_date).sort();
  const earliest = dates[0];
  const latest = dates[dates.length - 1];
  if (earliest === undefined || latest === undefined) {
    throw new Error("Cannot read coverage from an empty Moline record set");
  }
  return { earliest, latest };
}

/**
 * Assert that public rows contain only reviewed safe keys.
 *
 * @param {readonly Record<string, unknown>[]} rows - Public allowlist rows.
 * @returns {void}
 */
export function assertSafePublicRows(rows) {
  const forbidden =
    /address|contractor|valuation|parcel|description|owner|applicant|contact|person|phone|email/i;
  for (const row of rows) {
    for (const key of Object.keys(row)) {
      if (forbidden.test(key)) {
        throw new Error(`Forbidden Moline public field: ${key}`);
      }
    }
  }
}

/**
 * Describe one completed text artifact.
 *
 * @param {string} path - Local artifact path.
 * @param {string} content - Exact artifact content.
 * @param {number} rowCount - JSONL row count, or zero for JSON documents.
 * @returns {{path: string, byteCount: number, sha256: string, rowCount: number}} Manifest entry.
 */
function describeArtifact(path, content, rowCount) {
  return {
    path,
    byteCount: Buffer.byteLength(content),
    sha256: createHash("sha256").update(content).digest("hex"),
    rowCount,
  };
}

/**
 * Run a resumable local Moline archive harvest.
 *
 * @param {HarvestOptions} options - Validated local-only options.
 * @returns {Promise<void>} Resolves after package and manifest creation.
 */
export async function runHarvest(options) {
  const outputDirectory = resolve(options.outputDirectory);
  const rawDirectory = join(outputDirectory, "raw", "reports");
  await mkdir(rawDirectory, { recursive: true, mode: 0o700 });
  await chmod(outputDirectory, 0o700).catch(() => undefined);
  await chmod(dirname(rawDirectory), 0o700);
  await chmod(rawDirectory, 0o700);

  const runStarted = Date.now();
  const indexResponse = await fetchOfficialSource(
    MOLINE_REPORT_INDEX_URL,
    "HTML",
  );
  const indexHtml = await indexResponse.text();
  const allReports = extractMolineReportLinks(indexHtml).filter(
    (report) => report.reportMonth <= LAST_AUTHORIZED_MONTH,
  );
  if (allReports.length === 0) {
    throw new Error("Official Moline archive contained no reports");
  }
  const selectedReports = allReports.filter((report) =>
    isSelectedEra(classifyReportEra(report), options.scope),
  );
  if (options.scope === "current-2025" && selectedReports.length !== 18) {
    throw new Error(
      `Expected 18 current-layout reports from 2025-01 through 2026-06, found ${String(selectedReports.length)}`,
    );
  }

  /** @type {import("./permit-source-adapters/moline-issued-permit-reports.mjs").MolineIssuedPermit[]} */
  const parsedRecords = [];
  /** @type {ReportProvenance[]} */
  const provenance = [];
  for (const [reportIndex, report] of selectedReports.entries()) {
    const obtained = await obtainReportPdf(report, rawDirectory);
    const rawBuffer = Buffer.from(obtained.bytes);
    const byteCount = rawBuffer.byteLength;
    const sha256 = createHash("sha256").update(rawBuffer).digest("hex");
    const parseStarted = Date.now();
    const pages = await extractPositionedPdfText(obtained.bytes);
    const era = classifyReportEra(report);
    const supportedLayout =
      isCurrentMolineReportLayout(pages) ||
      isLegacyApplicationMolineReportLayout(pages) ||
      isRotatedLegacyMolineReportLayout(pages);
    if (!supportedLayout) {
      const reason = isCompactedLegacyMolineReportLayout(pages)
        ? "compacted identity columns"
        : "unknown layout";
      throw new Error(
        `Selected Moline report ${report.archiveId} has ${reason}`,
      );
    }
    const reportRecords = parseMolineIssuedPermitReport(pages, report);
    const parserLayout = reportRecords[0]?.raw.parser_layout;
    if (parserLayout === undefined) {
      throw new Error(`Moline report ${report.archiveId} parsed no records`);
    }
    const uniqueReportRecords = dedupeMolineIssuedPermits(reportRecords);
    const outsideArchiveMonthCount = reportRecords.filter(
      (record) => record.permit_issue_date.slice(0, 7) !== report.reportMonth,
    ).length;
    const issueDateMonths = [
      ...new Set(
        reportRecords.map((record) => record.permit_issue_date.slice(0, 7)),
      ),
    ].sort();
    const printedPermitTotal = readMolinePrintedPermitTotal(pages);
    const coverage = readDateCoverage(uniqueReportRecords);
    parsedRecords.push(...reportRecords);
    provenance.push({
      archiveId: report.archiveId,
      reportMonth: report.reportMonth,
      title: report.title,
      sourceUrl: report.url,
      era,
      parserLayout,
      localPath: obtained.path,
      reused: obtained.reused,
      downloadMs: obtained.downloadMs,
      parseMs: Date.now() - parseStarted,
      byteCount,
      sha256,
      parsedRowCount: reportRecords.length,
      uniqueLoaderKeyCount: uniqueReportRecords.length,
      duplicateRowCount: reportRecords.length - uniqueReportRecords.length,
      printedPermitTotal,
      completenessDelta:
        printedPermitTotal === null
          ? null
          : uniqueReportRecords.length - printedPermitTotal,
      outsideArchiveMonthCount,
      issueDateMonths,
      earliestIssueDate: coverage.earliest,
      latestIssueDate: coverage.latest,
    });
    if (obtained.reused === false && reportIndex < selectedReports.length - 1) {
      await delay(options.delayMs);
    }
  }

  const uniqueRecords = dedupeMolineIssuedPermits(parsedRecords);
  const publicJsonl = renderMolinePublicJsonl(uniqueRecords);
  /** @type {Record<string, unknown>[]} */
  const parsedPublicRows = publicJsonl
    .trim()
    .split("\n")
    .filter((line) => line.length > 0)
    .map((line) => JSON.parse(line));
  assertSafePublicRows(parsedPublicRows);

  /** @type {Map<ArchiveEra, import("./permit-source-adapters/moline-issued-permit-reports.mjs").MolineIssuedPermit[]>} */
  const recordsByEra = new Map();
  for (const record of uniqueRecords) {
    const era = classifyArchiveEra(record.raw.source_report_month);
    const records = recordsByEra.get(era) ?? [];
    records.push(record);
    recordsByEra.set(era, records);
  }
  /** @type {{path: string, byteCount: number, sha256: string, rowCount: number}[]} */
  const artifacts = [];
  const writeJsonlArtifact = async (
    /** @type {string} */ fileName,
    /** @type {string} */ content,
    /** @type {number} */ rowCount,
  ) => {
    const path = join(outputDirectory, fileName);
    await writePrivateFile(path, content);
    artifacts.push(describeArtifact(path, content, rowCount));
  };
  const privateJsonl = renderMolinePrivateJsonl(uniqueRecords);
  const combinedArtifactNames = combinedArtifactFileNames(options.scope);
  await writeJsonlArtifact(
    combinedArtifactNames.privateFileName,
    privateJsonl,
    uniqueRecords.length,
  );
  await writeJsonlArtifact(
    combinedArtifactNames.publicFileName,
    publicJsonl,
    parsedPublicRows.length,
  );
  /** @type {readonly ArchiveEra[]} */
  const outputEras = [
    "legacy-2017-2020",
    "legacy-2021-2024",
    "current-transition-2024",
    "current-2025",
  ];
  for (const era of outputEras) {
    const eraRecords = recordsByEra.get(era) ?? [];
    if (eraRecords.length === 0) continue;
    await writeJsonlArtifact(
      `${era}.private.jsonl`,
      renderMolinePrivateJsonl(eraRecords),
      eraRecords.length,
    );
    await writeJsonlArtifact(
      `${era}.public-allowlist.jsonl`,
      renderMolinePublicJsonl(eraRecords),
      eraRecords.length,
    );
  }

  const coverage = readDateCoverage(uniqueRecords);
  const blockedReports = allReports
    .map((report) => ({ ...report, era: classifyReportEra(report) }))
    .filter(
      (report) =>
        report.era === "blocked-early-identity" ||
        report.era === "blocked-metadata-mismatch" ||
        report.era === "blocked-date-range-conflict" ||
        report.era === "blocked-compacted-2020-2021" ||
        report.era === "blocked-compacted-isolated",
    );
  const fieldCoverage = {
    recordCount: uniqueRecords.length,
    printedModernPermitNumber: uniqueRecords.filter(
      (record) => record.permit_number !== null,
    ).length,
    stableLegacyApplicationIdentity: uniqueRecords.filter(
      (record) => record.raw.source_application_number !== null,
    ).length,
    issueDate: uniqueRecords.length,
    workLocation: uniqueRecords.filter(
      (record) => record.work_location !== null,
    ).length,
    sourceParcelEvidence: uniqueRecords.filter(
      (record) => record.raw.source_parcel_text !== null,
    ).length,
    projectDescription: uniqueRecords.filter(
      (record) => record.project_description !== null,
    ).length,
    contractorBusiness: uniqueRecords.filter(
      (record) => record.contractor_business_names.length > 0,
    ).length,
    valuation: uniqueRecords.filter(
      (record) => record.raw.project_valuation !== null,
    ).length,
    explicitParcelIdentifier: 0,
  };
  /** @type {Map<ArchiveEra, number>} */
  const eraReportCountMap = new Map();
  for (const report of allReports) {
    const era = classifyReportEra(report);
    eraReportCountMap.set(era, (eraReportCountMap.get(era) ?? 0) + 1);
  }
  const manifest = {
    schemaVersion: "1.0",
    event: "moline_official_permit_archive_package_completed",
    generatedAt: new Date().toISOString(),
    scope: options.scope,
    sourceSystem: MOLINE_REPORT_SOURCE_SYSTEM,
    sourceIndexUrl: MOLINE_REPORT_INDEX_URL,
    sourceIndexHttpStatus: indexResponse.status,
    sourceIndexSha256: createHash("sha256").update(indexHtml).digest("hex"),
    indexedReportCount: allReports.length,
    indexedUniqueMonthCount: new Set(
      allReports.map((report) => report.reportMonth),
    ).size,
    selectedReportCount: selectedReports.length,
    parsedSourceRowCount: parsedRecords.length,
    uniqueLoaderKeyCount: uniqueRecords.length,
    duplicateSourceRowCount: parsedRecords.length - uniqueRecords.length,
    earliestIssueDate: coverage.earliest,
    latestIssueDate: coverage.latest,
    eraReportCounts: Object.fromEntries(eraReportCountMap),
    eraRecordCounts: Object.fromEntries(
      [...recordsByEra.entries()].map(([era, records]) => [
        era,
        records.length,
      ]),
    ),
    reportCompleteness: {
      exact: provenance.filter((report) => report.completenessDelta === 0)
        .length,
      mismatch: provenance.filter(
        (report) =>
          report.completenessDelta !== null && report.completenessDelta !== 0,
      ).length,
      unavailable: provenance.filter(
        (report) => report.completenessDelta === null,
      ).length,
    },
    fieldCoverage,
    privacy:
      "Private JSONL retains official address, parcel-text evidence, description, conservative contractor-business candidates, and valuation. Public allowlist excludes all address, parcel, description, contractor, valuation, person, owner, applicant, and contact fields.",
    identityDecision:
      "Modern records use the printed permit number. Supported legacy records use the exact printed permit-code + application-year + application-number + issue-date composite because the source can issue one application/permit-code combination on multiple dates. No modern permit number is fabricated.",
    blockedReports: blockedReports.map((report) => ({
      archiveId: report.archiveId,
      reportMonth: report.reportMonth,
      title: report.title,
      sourceUrl: report.url,
      era: report.era,
      reason:
        report.era === "blocked-early-identity"
          ? "Representative 2012 and 2015 reports contain redacted or merged official application numbers; the era was stopped before scaling."
          : report.era === "blocked-metadata-mismatch"
            ? "The index labels archive item 4042 as May 2017, but its PDF serves an April 2020 split-page report; month identity is contradictory."
            : report.era === "blocked-date-range-conflict"
              ? "The April 2018 PDF extends into May and conflicts with the May report for the same official identity; no variant was selected."
              : report.era === "blocked-compacted-isolated"
                ? "The July 2022 PDF merges identity columns in embedded text; the report is isolated without heuristic splitting."
                : "Embedded PDF text merges identity columns; the ten-report era was stopped without heuristic splitting.",
    })),
    artifacts,
    reports: provenance,
    elapsedMs: Date.now() - runStarted,
    prohibitedActionsPerformed: [],
  };
  const manifestPath = join(outputDirectory, `${options.scope}-manifest.json`);
  await writePrivateFile(
    manifestPath,
    `${JSON.stringify(manifest, null, 2)}\n`,
  );
  const summary = {
    event: manifest.event,
    scope: options.scope,
    selectedReportCount: manifest.selectedReportCount,
    uniqueLoaderKeyCount: manifest.uniqueLoaderKeyCount,
    earliestIssueDate: manifest.earliestIssueDate,
    latestIssueDate: manifest.latestIssueDate,
    manifestPath,
    artifacts,
    elapsedMs: manifest.elapsedMs,
  };
  process.stdout.write(`${JSON.stringify(summary)}\n`);
}

/**
 * CLI entry point.
 *
 * @returns {Promise<void>} Resolves after local package creation.
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
        event: "moline_official_permit_archive_package_failed",
        script: basename(process.argv[1] ?? "unknown"),
        error: caught instanceof Error ? caught.message : String(caught),
      })}\n`,
    );
    process.exitCode = 1;
  });
}
