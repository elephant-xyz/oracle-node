#!/usr/bin/env node

import { createHash } from "node:crypto";
import { once } from "node:events";
import { createReadStream, createWriteStream } from "node:fs";
import {
  mkdir,
  open,
  readFile,
  readdir,
  rename,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import * as path from "node:path";
import { pathToFileURL } from "node:url";
import { parseArgs } from "node:util";

import { load } from "cheerio";
import { parse as parseCsv } from "csv-parse/sync";

/**
 * @typedef {Record<string, unknown>} JsonObject
 *
 * @typedef {object} PolkAccelaWindow
 * @property {string} startDate Inclusive ISO date.
 * @property {string} endDate Inclusive ISO date.
 *
 * @typedef {object} PolkAccelaListOptions
 * @property {"probe" | "harvest" | "verify"} stage Operation.
 * @property {string} startDate Inclusive history start.
 * @property {string} endDate Inclusive history end.
 * @property {number} windowMonths Number of calendar months per source window.
 * @property {number | null} limitWindows Optional bounded pilot window count.
 * @property {string} output Aggregate normalized JSONL.
 * @property {string} receipt Receipt path.
 * @property {string} stateDirectory Immutable window-part directory.
 * @property {string} checkpoint Checkpoint path.
 * @property {number} timeoutMs Per-request timeout.
 * @property {number} windowTimeoutMs Whole-window attempt timeout.
 * @property {number} attempts Whole-window attempts.
 * @property {number} retryDelayMs Initial retry delay.
 * @property {number} delayMs Delay between completed source windows.
 * @property {boolean} approveScale Explicit full-history approval.
 *
 * @typedef {object} PolkAccelaListRecord
 * @property {"oracle-node.polk-county-accela-list-record.v1"} schemaVersion Record schema.
 * @property {"polk_county_accela_csv"} sourceSystem Source identifier.
 * @property {string} sourceRecordKey Record-number identity.
 * @property {string} sourceUrl Deterministic Accela detail URL.
 * @property {string} retrievedAt Retrieval timestamp.
 * @property {PolkAccelaWindow} sourceWindow Inclusive search window.
 * @property {string} permitNumber Published record number.
 * @property {string | null} recordType Published record type.
 * @property {"permit" | "license" | "other"} recordClass Conservative record classification.
 * @property {string | null} address Published address text.
 * @property {string | null} status Published status.
 * @property {string | null} sourceDate Published list date.
 * @property {string | null} projectName Published project name.
 * @property {string | null} description Published description.
 * @property {string | null} expirationDate Published expiration date.
 * @property {string | null} shortNotes Published short notes.
 * @property {null} parcelIdentifier No parcel is inferred from address.
 * @property {null} propertyMatch No property match is guessed.
 *
 * @typedef {object} PolkAccelaCheckpoint
 * @property {"oracle-node.polk-county-accela-list-checkpoint.v1"} schemaVersion Checkpoint schema.
 * @property {"polk-county-accela-list-contract.v1"} recordContractVersion Record semantics contract.
 * @property {string} portalUrl Pinned portal URL.
 * @property {string} startDate Requested start.
 * @property {string} endDate Requested end.
 * @property {number} windowMonths Window contract.
 * @property {number | null} limitWindows Pilot contract.
 * @property {string} output Aggregate output.
 * @property {string} stateDirectory Part directory.
 * @property {number} requestedWindowCount Number of selected windows.
 * @property {number} completedWindowCount Committed contiguous windows.
 * @property {number} sourceRecordCount Source CSV rows before exact deduplication.
 * @property {number} exactDuplicateRowCount Exact source rows coalesced by record number.
 * @property {number} accessibleRecordCount Committed records.
 * @property {boolean} complete Requested-window completion.
 * @property {string} updatedAt Checkpoint timestamp.
 */

export const POLK_COUNTY_ACCELA_PORTAL_URL =
  "https://aca-prod.accela.com/POLKCO/Cap/CapHome.aspx?module=Building&TabName=Building";
export const POLK_COUNTY_ACCELA_EXPORT_URL =
  "https://aca-prod.accela.com/POLKCO/Export2CSV.ashx";

const CHECKPOINT_SCHEMA = "oracle-node.polk-county-accela-list-checkpoint.v1";
const RECORD_SCHEMA = "oracle-node.polk-county-accela-list-record.v1";
const RECORD_CONTRACT_VERSION = "polk-county-accela-list-contract.v1";
const SEARCH_EVENT_TARGET = "ctl00$PlaceHolderMain$btnNewSearch";
const EXPORT_EVENT_TARGET =
  "ctl00$PlaceHolderMain$dgvPermitList$gdvPermitList$gdvPermitListtop4btnExport";
const START_DATE_CONTROL =
  "ctl00$PlaceHolderMain$generalSearchForm$txtGSStartDate";
const END_DATE_CONTROL = "ctl00$PlaceHolderMain$generalSearchForm$txtGSEndDate";
const EXPECTED_CSV_HEADER = Object.freeze([
  "Record Number",
  "Record Type",
  "Address",
  "Status",
  "Date",
  "Project Name",
  "Description",
  "Expiration Date",
  "Short Notes",
]);
const PART_PATTERN =
  /^window-(\d{4})\.(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})(?:\.r(\d+)\.d(\d+))?\.([a-f0-9]{64})\.jsonl$/;
const USER_AGENT =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/143 Safari/537.36";

/**
 * Test whether an unknown value is a JSON object.
 *
 * @param {unknown} value Candidate value.
 * @returns {value is JsonObject} Whether the value is an object.
 */
function isJsonObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Parse a bounded integer CLI option.
 *
 * @param {string | undefined} raw Raw value.
 * @param {string} name Option name.
 * @param {number} fallback Default.
 * @param {number} maximum Maximum.
 * @returns {number} Parsed integer.
 */
function readPositiveInteger(raw, name, fallback, maximum) {
  if (raw === undefined) return fallback;
  const value = Number.parseInt(raw, 10);
  if (
    !Number.isSafeInteger(value) ||
    value < 1 ||
    value > maximum ||
    String(value) !== raw
  ) {
    throw new Error(`--${name} must be an integer from 1 through ${maximum}`);
  }
  return value;
}

/**
 * Validate and parse an ISO calendar date.
 *
 * @param {string} raw Date text.
 * @param {string} name Option name.
 * @returns {string} Canonical ISO date.
 */
function readIsoDate(raw, name) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    throw new Error(`--${name} must use YYYY-MM-DD`);
  }
  const date = new Date(`${raw}T00:00:00.000Z`);
  if (Number.isNaN(date.getTime()) || date.toISOString().slice(0, 10) !== raw) {
    throw new Error(`--${name} is not a valid calendar date`);
  }
  return raw;
}

/**
 * Parse the Polk County Accela list CLI.
 *
 * @param {readonly string[]} argv Arguments excluding node and script.
 * @returns {PolkAccelaListOptions} Validated options.
 */
export function parsePolkAccelaListOptions(argv) {
  const defaultEndDate = new Date().toISOString().slice(0, 10);
  const { values } = parseArgs({
    args: [...argv],
    options: {
      stage: { type: "string", default: "probe" },
      "start-date": { type: "string", default: "2003-01-01" },
      "end-date": { type: "string", default: defaultEndDate },
      "window-months": { type: "string" },
      "limit-windows": { type: "string" },
      output: {
        type: "string",
        default: "tmp/polk/permits/polk-county-accela-list.jsonl",
      },
      receipt: { type: "string" },
      "state-dir": { type: "string" },
      checkpoint: { type: "string" },
      "timeout-ms": { type: "string" },
      "window-timeout-ms": { type: "string" },
      attempts: { type: "string" },
      "retry-delay-ms": { type: "string" },
      "delay-ms": { type: "string" },
      "approve-scale": { type: "boolean" },
    },
    strict: true,
  });
  if (
    values.stage !== "probe" &&
    values.stage !== "harvest" &&
    values.stage !== "verify"
  ) {
    throw new Error("--stage must be probe, harvest, or verify");
  }
  const startDate = readIsoDate(values["start-date"], "start-date");
  const endDate = readIsoDate(values["end-date"], "end-date");
  if (startDate > endDate) {
    throw new Error("--start-date must not be after --end-date");
  }
  return {
    stage: values.stage,
    startDate,
    endDate,
    windowMonths: readPositiveInteger(
      values["window-months"],
      "window-months",
      1,
      12,
    ),
    limitWindows:
      values["limit-windows"] === undefined
        ? null
        : readPositiveInteger(
            values["limit-windows"],
            "limit-windows",
            1,
            10_000,
          ),
    output: values.output,
    receipt: values.receipt ?? `${values.output}.receipt.json`,
    stateDirectory: values["state-dir"] ?? `${values.output}.parts`,
    checkpoint: values.checkpoint ?? `${values.output}.checkpoint.json`,
    timeoutMs: readPositiveInteger(
      values["timeout-ms"],
      "timeout-ms",
      30_000,
      180_000,
    ),
    windowTimeoutMs: readPositiveInteger(
      values["window-timeout-ms"],
      "window-timeout-ms",
      90_000,
      300_000,
    ),
    attempts: readPositiveInteger(values.attempts, "attempts", 2, 10),
    retryDelayMs: readPositiveInteger(
      values["retry-delay-ms"],
      "retry-delay-ms",
      2_000,
      60_000,
    ),
    delayMs: readPositiveInteger(values["delay-ms"], "delay-ms", 5_000, 60_000),
    approveScale: values["approve-scale"] === true,
  };
}

/**
 * Build deterministic, non-overlapping calendar-month windows.
 *
 * @param {string} startDate Inclusive ISO start.
 * @param {string} endDate Inclusive ISO end.
 * @param {number} windowMonths Number of months per window.
 * @returns {PolkAccelaWindow[]} Ordered windows.
 */
export function buildPolkAccelaWindows(startDate, endDate, windowMonths) {
  const end = new Date(`${endDate}T00:00:00.000Z`);
  let cursor = new Date(`${startDate}T00:00:00.000Z`);
  /** @type {PolkAccelaWindow[]} */
  const windows = [];
  while (cursor <= end) {
    const windowStart = new Date(cursor);
    const next = new Date(
      Date.UTC(cursor.getUTCFullYear(), cursor.getUTCMonth() + windowMonths, 1),
    );
    const nominalEnd = new Date(next.getTime() - 86_400_000);
    const windowEnd = nominalEnd < end ? nominalEnd : end;
    windows.push({
      startDate: windowStart.toISOString().slice(0, 10),
      endDate: windowEnd.toISOString().slice(0, 10),
    });
    cursor = new Date(windowEnd.getTime() + 86_400_000);
  }
  return windows;
}

/**
 * Format an ISO date for Accela's US date controls.
 *
 * @param {string} isoDate ISO date.
 * @returns {string} MM/DD/YYYY.
 */
function toAccelaDate(isoDate) {
  const [year, month, day] = isoDate.split("-");
  return `${month}/${day}/${year}`;
}

/**
 * Return nullable trimmed text.
 *
 * @param {unknown} value Source value.
 * @returns {string | null} Trimmed text.
 */
function sourceText(value) {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

/**
 * Parse an Accela MM/DD/YYYY list date.
 *
 * @param {unknown} value Source value.
 * @returns {string | null} ISO date.
 */
function sourceDate(value) {
  const text = sourceText(value);
  if (text === null) return null;
  const match = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(text);
  if (match === null) return text;
  const [, month, day, year] = match;
  const iso = `${year}-${month.padStart(2, "0")}-${day.padStart(2, "0")}`;
  const parsed = new Date(`${iso}T00:00:00.000Z`);
  return !Number.isNaN(parsed.getTime()) &&
    parsed.toISOString().slice(0, 10) === iso
    ? iso
    : text;
}

/**
 * Conservatively classify one Accela record.
 *
 * @param {string} permitNumber Record number.
 * @param {string | null} recordType Record type.
 * @returns {"permit" | "license" | "other"} Record class.
 */
function classifyAccelaRecord(permitNumber, recordType) {
  if (
    /\bpermit\b/i.test(recordType ?? "") ||
    /^(?:BLD-H-|BC-|BR-|BT-|BPS-|[0-9]{2}TMP-)/i.test(permitNumber)
  ) {
    return "permit";
  }
  if (
    /\blicen[cs](?:e|ing)\b/i.test(recordType ?? "") ||
    /^(?:LIC-|BL-)/i.test(permitNumber)
  ) {
    return "license";
  }
  return "other";
}

/**
 * Build the public detail URL for one exported record number.
 *
 * @param {string} permitNumber Exact source record number.
 * @returns {string} Detail URL.
 */
function buildDetailUrl(permitNumber) {
  const url = new URL("https://aca-prod.accela.com/POLKCO/Cap/CapDetail.aspx");
  url.searchParams.set("Module", "Building");
  url.searchParams.set("TabName", "Building");
  url.searchParams.set("altId", permitNumber);
  return url.toString();
}

/**
 * Parse one Accela CSV export and fail closed on schema changes.
 *
 * @param {string} csv CSV response.
 * @param {PolkAccelaWindow} window Search window.
 * @param {string} retrievedAt Retrieval timestamp.
 * @returns {PolkAccelaListRecord[]} Normalized records.
 */
export function parsePolkAccelaCsv(csv, window, retrievedAt) {
  return parsePolkAccelaCsvExport(csv, window, retrievedAt).records;
}

/**
 * Parse one Accela CSV export with exact-duplicate evidence.
 *
 * Exact duplicate rows are one source record repeated by the portal. A
 * duplicate record number with conflicting fields fails closed.
 *
 * @param {string} csv CSV response.
 * @param {PolkAccelaWindow} window Search window.
 * @param {string} retrievedAt Retrieval timestamp.
 * @returns {{records:PolkAccelaListRecord[],sourceRecordCount:number,exactDuplicateRowCount:number}} Parsed records and source counts.
 */
function parsePolkAccelaCsvExport(csv, window, retrievedAt) {
  const rows = /** @type {string[][]} */ (
    parseCsv(csv, {
      bom: true,
      skip_empty_lines: true,
      relax_column_count: true,
    })
  );
  if (rows.length === 0) {
    return {
      records: [],
      sourceRecordCount: 0,
      exactDuplicateRowCount: 0,
    };
  }
  const header = rows[0]?.map((value) => value.trim()) ?? [];
  if (
    EXPECTED_CSV_HEADER.some((expected, index) => header[index] !== expected)
  ) {
    throw new Error(
      `Polk Accela CSV schema changed: ${JSON.stringify(header)}`,
    );
  }
  const parsed = rows.slice(1).map((row, index) => {
    const permitNumber = sourceText(row[0]);
    if (permitNumber === null) {
      throw new Error(
        `Polk Accela CSV row ${index + 2} is missing Record Number`,
      );
    }
    const recordType = sourceText(row[1]);
    return {
      schemaVersion: RECORD_SCHEMA,
      sourceSystem: "polk_county_accela_csv",
      sourceRecordKey: `polk_accela:${permitNumber.toLowerCase()}`,
      sourceUrl: buildDetailUrl(permitNumber),
      retrievedAt,
      sourceWindow: { ...window },
      permitNumber,
      recordType,
      recordClass: classifyAccelaRecord(permitNumber, recordType),
      address: sourceText(row[2]),
      status: sourceText(row[3]),
      sourceDate: sourceDate(row[4]),
      projectName: sourceText(row[5]),
      description: sourceText(row[6]),
      expirationDate: sourceDate(row[7]),
      shortNotes: sourceText(row[8]),
      parcelIdentifier: null,
      propertyMatch: null,
    };
  });
  const recordsByKey = new Map();
  let exactDuplicateRowCount = 0;
  for (const record of parsed) {
    const existing = recordsByKey.get(record.sourceRecordKey);
    if (existing === undefined) {
      recordsByKey.set(record.sourceRecordKey, record);
      continue;
    }
    if (JSON.stringify(existing) !== JSON.stringify(record)) {
      throw new Error(
        `Polk Accela CSV contains conflicting duplicate record ${record.permitNumber}`,
      );
    }
    exactDuplicateRowCount += 1;
  }
  return {
    records: [...recordsByKey.values()],
    sourceRecordCount: parsed.length,
    exactDuplicateRowCount,
  };
}

/**
 * Parse all successful HTML form controls for an ASP.NET postback.
 *
 * @param {string} html Current page.
 * @returns {URLSearchParams} Form body.
 */
function buildPostbackForm(html) {
  const $ = load(html);
  const params = new URLSearchParams();
  $("input[name]").each((_, element) => {
    const input = $(element);
    const type = (input.attr("type") ?? "text").toLowerCase();
    if (["submit", "button", "image", "file", "reset"].includes(type)) return;
    if (
      ["checkbox", "radio"].includes(type) &&
      input.attr("checked") === undefined
    ) {
      return;
    }
    const name = input.attr("name");
    if (name !== undefined) params.append(name, input.attr("value") ?? "");
  });
  $("select[name]").each((_, element) => {
    const select = $(element);
    const selected = select.find("option[selected]").first();
    const option =
      selected.length > 0 ? selected : select.find("option").first();
    const name = select.attr("name");
    if (name !== undefined) {
      params.append(name, option.attr("value") ?? option.text());
    }
  });
  return params;
}

/**
 * Mutable in-memory cookie jar for one isolated Accela window.
 *
 * @returns {{header:() => string,update:(response:Response) => void}} Jar.
 */
function createCookieJar() {
  const values = new Map();
  return {
    header: () =>
      [...values.entries()]
        .map(([name, value]) => `${name}=${value}`)
        .join("; "),
    update: (response) => {
      for (const raw of response.headers.getSetCookie()) {
        const pair = raw.split(";")[0] ?? "";
        const separator = pair.indexOf("=");
        if (separator > 0) {
          values.set(pair.slice(0, separator), pair.slice(separator + 1));
        }
      }
    },
  };
}

/**
 * Fetch one HTTP response and reject source errors.
 *
 * @param {string} url Request URL.
 * @param {RequestInit} init Request options.
 * @param {number} timeoutMs Timeout.
 * @param {typeof fetch} fetchImpl Injectable fetch.
 * @param {AbortSignal | undefined} outerSignal Whole-window signal.
 * @returns {Promise<Response>} Successful response.
 */
async function fetchResponse(url, init, timeoutMs, fetchImpl, outerSignal) {
  const requestTimeout = AbortSignal.timeout(timeoutMs);
  const response = await fetchImpl(url, {
    ...init,
    signal:
      outerSignal === undefined
        ? requestTimeout
        : AbortSignal.any([outerSignal, requestTimeout]),
  });
  if (!response.ok) {
    throw new Error(`Polk Accela returned HTTP ${response.status}`);
  }
  return response;
}

/**
 * Fetch one isolated date window through Accela's complete CSV export.
 *
 * @param {PolkAccelaWindow} window Date window.
 * @param {PolkAccelaListOptions} options Runtime settings.
 * @param {typeof fetch} fetchImpl Injectable fetch.
 * @param {AbortSignal} windowSignal Whole-window attempt signal.
 * @returns {Promise<{records:PolkAccelaListRecord[],sourceRecordCount:number,exactDuplicateRowCount:number,initialResultSummary:string | null,initialResultAtLeast100:boolean,csvBytes:number}>} Window result.
 */
async function fetchAccelaWindow(window, options, fetchImpl, windowSignal) {
  const jar = createCookieJar();
  const commonHeaders = {
    "user-agent": USER_AGENT,
    "accept-language": "en-US,en;q=0.9",
  };
  const initial = await fetchResponse(
    POLK_COUNTY_ACCELA_PORTAL_URL,
    { headers: commonHeaders },
    options.timeoutMs,
    fetchImpl,
    windowSignal,
  );
  jar.update(initial);
  let html = await initial.text();
  const searchForm = buildPostbackForm(html);
  searchForm.set("__EVENTTARGET", SEARCH_EVENT_TARGET);
  searchForm.set("__EVENTARGUMENT", "");
  searchForm.set(START_DATE_CONTROL, toAccelaDate(window.startDate));
  searchForm.set(END_DATE_CONTROL, toAccelaDate(window.endDate));
  const searchResponse = await fetchResponse(
    POLK_COUNTY_ACCELA_PORTAL_URL,
    {
      method: "POST",
      headers: {
        ...commonHeaders,
        "content-type": "application/x-www-form-urlencoded",
        cookie: jar.header(),
        referer: POLK_COUNTY_ACCELA_PORTAL_URL,
        origin: "https://aca-prod.accela.com",
      },
      body: searchForm,
    },
    options.timeoutMs,
    fetchImpl,
    windowSignal,
  );
  jar.update(searchResponse);
  html = await searchResponse.text();
  const $ = load(html);
  const pageText = $.root().text().replace(/\s+/g, " ");
  if (/error\(s\) occurred on current page|unable to proceed/i.test(pageText)) {
    throw new Error(
      `Polk Accela returned an error page for ${window.startDate}..${window.endDate}`,
    );
  }
  const noResults =
    /Your search returned no results|No records found/i.test(pageText) ||
    /Showing\s+0\s*-\s*0\s+of\s+0/i.test(pageText);
  if (noResults) {
    return {
      records: [],
      sourceRecordCount: 0,
      exactDuplicateRowCount: 0,
      initialResultSummary: null,
      initialResultAtLeast100: false,
      csvBytes: 0,
    };
  }
  const summaryMatch =
    /Showing\s+([0-9,]+\s*-\s*[0-9,]+\s+of\s+([0-9,]+)(\+)?)/i.exec(pageText);
  if ($(`#${EXPORT_EVENT_TARGET.replaceAll("$", "_")}`).length === 0) {
    throw new Error(
      `Polk Accela export control disappeared for ${window.startDate}..${window.endDate}`,
    );
  }
  const exportForm = buildPostbackForm(html);
  exportForm.set("__EVENTTARGET", EXPORT_EVENT_TARGET);
  exportForm.set("__EVENTARGUMENT", "");
  const exportPreparation = await fetchResponse(
    POLK_COUNTY_ACCELA_PORTAL_URL,
    {
      method: "POST",
      headers: {
        ...commonHeaders,
        "content-type": "application/x-www-form-urlencoded",
        cookie: jar.header(),
        referer: POLK_COUNTY_ACCELA_PORTAL_URL,
        origin: "https://aca-prod.accela.com",
      },
      body: exportForm,
    },
    options.timeoutMs,
    fetchImpl,
    windowSignal,
  );
  jar.update(exportPreparation);
  await exportPreparation.text();
  const exportUrl = new URL(POLK_COUNTY_ACCELA_EXPORT_URL);
  exportUrl.searchParams.set("flag", String(Date.now()));
  const csvResponse = await fetchResponse(
    exportUrl.toString(),
    {
      headers: {
        ...commonHeaders,
        cookie: jar.header(),
        referer: POLK_COUNTY_ACCELA_PORTAL_URL,
      },
    },
    options.timeoutMs,
    fetchImpl,
    windowSignal,
  );
  jar.update(csvResponse);
  const contentType = csvResponse.headers.get("content-type") ?? "";
  if (!contentType.toLowerCase().includes("text/csv")) {
    throw new Error(
      `Polk Accela export returned unexpected content type ${contentType}`,
    );
  }
  const csv = await csvResponse.text();
  const parsed = parsePolkAccelaCsvExport(
    csv,
    window,
    new Date().toISOString(),
  );
  return {
    records: parsed.records,
    sourceRecordCount: parsed.sourceRecordCount,
    exactDuplicateRowCount: parsed.exactDuplicateRowCount,
    initialResultSummary: summaryMatch?.[1] ?? null,
    initialResultAtLeast100:
      summaryMatch !== null &&
      (summaryMatch[3] === "+" ||
        Number.parseInt(summaryMatch[2].replaceAll(",", ""), 10) >= 100),
    csvBytes: Buffer.byteLength(csv),
  };
}

/**
 * Retry a complete isolated source window.
 *
 * @param {PolkAccelaWindow} window Date window.
 * @param {PolkAccelaListOptions} options Runtime options.
 * @param {typeof fetch} fetchImpl Fetch implementation.
 * @returns {Promise<{records:PolkAccelaListRecord[],sourceRecordCount:number,exactDuplicateRowCount:number,initialResultSummary:string | null,initialResultAtLeast100:boolean,csvBytes:number}>} Window result.
 */
async function fetchWindowWithRetry(window, options, fetchImpl) {
  /** @type {unknown} */
  let lastError = null;
  for (let attempt = 1; attempt <= options.attempts; attempt += 1) {
    const controller = new AbortController();
    const timeout = setTimeout(() => {
      controller.abort(
        new Error(
          `Polk Accela window attempt timed out after ${options.windowTimeoutMs}ms`,
        ),
      );
    }, options.windowTimeoutMs);
    try {
      return await fetchAccelaWindow(
        window,
        options,
        fetchImpl,
        controller.signal,
      );
    } catch (caught) {
      lastError = caught;
      if (attempt === options.attempts) break;
      await new Promise((resolve) =>
        setTimeout(resolve, options.retryDelayMs * 2 ** (attempt - 1)),
      );
    } finally {
      clearTimeout(timeout);
    }
  }
  throw lastError instanceof Error
    ? lastError
    : new Error("Polk Accela window failed");
}

/**
 * Compute a SHA-256 text digest.
 *
 * @param {string} text Text.
 * @returns {string} Digest.
 */
function sha256Text(text) {
  return createHash("sha256").update(text).digest("hex");
}

/**
 * Atomically replace a UTF-8 file.
 *
 * @param {string} destination Destination.
 * @param {string} text Content.
 * @returns {Promise<void>} Resolves after rename.
 */
async function writeAtomicText(destination, text) {
  await mkdir(path.dirname(destination), { recursive: true });
  const temporary = `${destination}.tmp-${process.pid}-${Date.now()}`;
  try {
    await writeFile(temporary, text, "utf8");
    await rename(temporary, destination);
  } catch (caught) {
    await rm(temporary, { force: true });
    throw caught;
  }
}

/**
 * Commit one content-addressed date window, including empty windows.
 *
 * @param {string} stateDirectory Part directory.
 * @param {number} index Window index.
 * @param {PolkAccelaWindow} window Date window.
 * @param {readonly PolkAccelaListRecord[]} records Records.
 * @param {number} sourceRecordCount Source CSV rows.
 * @param {number} exactDuplicateRowCount Exact duplicate rows.
 * @returns {Promise<string>} Part path.
 */
async function writeWindowPart(
  stateDirectory,
  index,
  window,
  records,
  sourceRecordCount,
  exactDuplicateRowCount,
) {
  const text =
    records.map((record) => JSON.stringify(record)).join("\n") +
    (records.length > 0 ? "\n" : "");
  const digest = sha256Text(text);
  const name = `window-${String(index).padStart(4, "0")}.${window.startDate}_${window.endDate}.r${sourceRecordCount}.d${exactDuplicateRowCount}.${digest}.jsonl`;
  const destination = path.join(stateDirectory, name);
  await writeAtomicText(destination, text);
  return destination;
}

/**
 * Read an optional JSON object.
 *
 * @param {string} filePath JSON path.
 * @returns {Promise<JsonObject | null>} Parsed object.
 */
async function readOptionalJson(filePath) {
  try {
    const value = /** @type {unknown} */ (
      JSON.parse(await readFile(filePath, "utf8"))
    );
    if (!isJsonObject(value)) throw new Error(`${filePath} is not an object`);
    return value;
  } catch (caught) {
    if (
      caught instanceof Error &&
      "code" in caught &&
      /** @type {NodeJS.ErrnoException} */ (caught).code === "ENOENT"
    ) {
      return null;
    }
    throw caught;
  }
}

/**
 * Read and validate a checkpoint.
 *
 * @param {string} checkpointPath Checkpoint path.
 * @returns {Promise<PolkAccelaCheckpoint | null>} Checkpoint.
 */
async function readCheckpoint(checkpointPath) {
  const value = await readOptionalJson(checkpointPath);
  if (value === null) return null;
  if (
    value.schemaVersion !== CHECKPOINT_SCHEMA ||
    value.recordContractVersion !== RECORD_CONTRACT_VERSION ||
    typeof value.portalUrl !== "string" ||
    typeof value.startDate !== "string" ||
    typeof value.endDate !== "string" ||
    !Number.isSafeInteger(value.windowMonths) ||
    (value.limitWindows !== null &&
      !Number.isSafeInteger(value.limitWindows)) ||
    typeof value.output !== "string" ||
    typeof value.stateDirectory !== "string" ||
    !Number.isSafeInteger(value.requestedWindowCount) ||
    !Number.isSafeInteger(value.completedWindowCount) ||
    !Number.isSafeInteger(value.accessibleRecordCount) ||
    (value.sourceRecordCount !== undefined &&
      !Number.isSafeInteger(value.sourceRecordCount)) ||
    (value.exactDuplicateRowCount !== undefined &&
      !Number.isSafeInteger(value.exactDuplicateRowCount)) ||
    typeof value.complete !== "boolean" ||
    typeof value.updatedAt !== "string"
  ) {
    throw new Error(`Invalid Polk Accela checkpoint: ${checkpointPath}`);
  }
  const sourceRecordCount =
    value.sourceRecordCount === undefined
      ? Number(value.accessibleRecordCount)
      : Number(value.sourceRecordCount);
  const exactDuplicateRowCount =
    value.exactDuplicateRowCount === undefined
      ? 0
      : Number(value.exactDuplicateRowCount);
  if (
    sourceRecordCount < Number(value.accessibleRecordCount) ||
    sourceRecordCount - Number(value.accessibleRecordCount) !==
      exactDuplicateRowCount
  ) {
    throw new Error(`Invalid Polk Accela source counts: ${checkpointPath}`);
  }
  return /** @type {PolkAccelaCheckpoint} */ ({
    ...value,
    sourceRecordCount,
    exactDuplicateRowCount,
  });
}

/**
 * Write a monotonic checkpoint.
 *
 * @param {string} checkpointPath Destination.
 * @param {Omit<PolkAccelaCheckpoint, "schemaVersion" | "recordContractVersion" | "sourceRecordCount" | "exactDuplicateRowCount" | "updatedAt"> & {sourceRecordCount?:number,exactDuplicateRowCount?:number}} checkpoint Body.
 * @returns {Promise<void>} Resolves after atomic replacement.
 */
async function writeCheckpoint(checkpointPath, checkpoint) {
  const previous = await readCheckpoint(checkpointPath);
  const sourceRecordCount =
    checkpoint.sourceRecordCount ?? checkpoint.accessibleRecordCount;
  const exactDuplicateRowCount = checkpoint.exactDuplicateRowCount ?? 0;
  if (
    previous !== null &&
    (checkpoint.completedWindowCount < previous.completedWindowCount ||
      checkpoint.accessibleRecordCount < previous.accessibleRecordCount ||
      sourceRecordCount < previous.sourceRecordCount ||
      exactDuplicateRowCount < previous.exactDuplicateRowCount)
  ) {
    throw new Error("Polk Accela checkpoint cannot rewind");
  }
  await writeAtomicText(
    checkpointPath,
    `${JSON.stringify(
      {
        schemaVersion: CHECKPOINT_SCHEMA,
        recordContractVersion: RECORD_CONTRACT_VERSION,
        ...checkpoint,
        sourceRecordCount,
        exactDuplicateRowCount,
        updatedAt: new Date().toISOString(),
      },
      null,
      2,
    )}\n`,
  );
}

/**
 * Assert checkpoint compatibility.
 *
 * @param {PolkAccelaCheckpoint} checkpoint Existing checkpoint.
 * @param {PolkAccelaListOptions} options Options.
 * @param {number} requestedWindowCount Selected windows.
 * @returns {void} Throws on mismatch.
 */
function assertCheckpointCompatible(checkpoint, options, requestedWindowCount) {
  if (
    checkpoint.recordContractVersion !== RECORD_CONTRACT_VERSION ||
    checkpoint.portalUrl !== POLK_COUNTY_ACCELA_PORTAL_URL ||
    checkpoint.startDate !== options.startDate ||
    checkpoint.endDate !== options.endDate ||
    checkpoint.windowMonths !== options.windowMonths ||
    checkpoint.limitWindows !== options.limitWindows ||
    checkpoint.output !== options.output ||
    checkpoint.stateDirectory !== options.stateDirectory ||
    checkpoint.requestedWindowCount !== requestedWindowCount
  ) {
    throw new Error(
      "Polk Accela checkpoint is incompatible; preserve it and use a new output/state directory.",
    );
  }
}

/**
 * Validate a normalized list record.
 *
 * @param {unknown} record Candidate.
 * @param {PolkAccelaWindow} expectedWindow Expected source window.
 * @returns {record is PolkAccelaListRecord} Whether reusable.
 */
function isReusableRecord(record, expectedWindow) {
  return (
    isJsonObject(record) &&
    record.schemaVersion === RECORD_SCHEMA &&
    record.sourceSystem === "polk_county_accela_csv" &&
    typeof record.sourceRecordKey === "string" &&
    typeof record.sourceUrl === "string" &&
    typeof record.retrievedAt === "string" &&
    isJsonObject(record.sourceWindow) &&
    record.sourceWindow.startDate === expectedWindow.startDate &&
    record.sourceWindow.endDate === expectedWindow.endDate &&
    typeof record.permitNumber === "string" &&
    (record.recordClass === "permit" ||
      record.recordClass === "license" ||
      record.recordClass === "other") &&
    record.parcelIdentifier === null &&
    record.propertyMatch === null
  );
}

/**
 * Verify contiguous immutable window parts and global identity uniqueness.
 *
 * @param {string} stateDirectory Part directory.
 * @param {readonly PolkAccelaWindow[]} windows Selected windows.
 * @returns {Promise<{partPaths:string[],recordCount:number,sourceRecordCount:number,exactDuplicateRowCount:number,recordKeys:Set<string>,classCounts:Record<"permit" | "license" | "other",number>}>} Verified state.
 */
async function verifyWindowParts(stateDirectory, windows) {
  let entries;
  try {
    entries = await readdir(stateDirectory);
  } catch (caught) {
    if (
      caught instanceof Error &&
      "code" in caught &&
      /** @type {NodeJS.ErrnoException} */ (caught).code === "ENOENT"
    ) {
      return {
        partPaths: [],
        recordCount: 0,
        sourceRecordCount: 0,
        exactDuplicateRowCount: 0,
        recordKeys: new Set(),
        classCounts: { permit: 0, license: 0, other: 0 },
      };
    }
    throw caught;
  }
  const matched = entries
    .flatMap((entry) => {
      const match = PART_PATTERN.exec(entry);
      return match === null
        ? []
        : [
            {
              entry,
              index: Number.parseInt(match[1], 10),
              startDate: match[2],
              endDate: match[3],
              sourceRecordCount:
                match[4] === undefined ? null : Number.parseInt(match[4], 10),
              exactDuplicateRowCount:
                match[5] === undefined ? null : Number.parseInt(match[5], 10),
              digest: match[6],
            },
          ];
    })
    .sort((left, right) => left.index - right.index);
  /** @type {string[]} */
  const partPaths = [];
  const recordKeys = new Set();
  const classCounts = { permit: 0, license: 0, other: 0 };
  let recordCount = 0;
  let sourceRecordCount = 0;
  let exactDuplicateRowCount = 0;
  for (let position = 0; position < matched.length; position += 1) {
    const part = matched[position];
    const expected = windows[position];
    if (
      part === undefined ||
      expected === undefined ||
      part.index !== position ||
      part.startDate !== expected.startDate ||
      part.endDate !== expected.endDate
    ) {
      throw new Error("Polk Accela window parts are not contiguous");
    }
    const partPath = path.join(stateDirectory, part.entry);
    const text = await readFile(partPath, "utf8");
    if (sha256Text(text) !== part.digest) {
      throw new Error(`Polk Accela part digest mismatch: ${partPath}`);
    }
    const records = text
      .split(/\r?\n/)
      .filter((line) => line.trim().length > 0)
      .map((line) => /** @type {unknown} */ (JSON.parse(line)));
    for (const record of records) {
      if (!isReusableRecord(record, expected)) {
        throw new Error(`Invalid Polk Accela record in ${partPath}`);
      }
      if (recordKeys.has(record.sourceRecordKey)) {
        throw new Error(
          `Duplicate Polk Accela record across windows: ${record.permitNumber}`,
        );
      }
      recordKeys.add(record.sourceRecordKey);
      classCounts[record.recordClass] += 1;
    }
    const partSourceRecordCount = part.sourceRecordCount ?? records.length;
    const partExactDuplicateRowCount = part.exactDuplicateRowCount ?? 0;
    if (
      partSourceRecordCount < records.length ||
      partSourceRecordCount - records.length !== partExactDuplicateRowCount
    ) {
      throw new Error(`Invalid Polk Accela source counts in ${partPath}`);
    }
    recordCount += records.length;
    sourceRecordCount += partSourceRecordCount;
    exactDuplicateRowCount += partExactDuplicateRowCount;
    partPaths.push(partPath);
  }
  return {
    partPaths,
    recordCount,
    sourceRecordCount,
    exactDuplicateRowCount,
    recordKeys,
    classCounts,
  };
}

/**
 * Acquire an exclusive writer lock, removing only a dead-process lock.
 *
 * @param {string} stateDirectory State directory.
 * @returns {Promise<() => Promise<void>>} Release callback.
 */
async function acquireRunLock(stateDirectory) {
  await mkdir(stateDirectory, { recursive: true });
  const lockPath = path.join(stateDirectory, ".run.lock");
  try {
    const handle = await open(lockPath, "wx");
    await handle.writeFile(
      `${JSON.stringify({ pid: process.pid, startedAt: new Date().toISOString() })}\n`,
      "utf8",
    );
    await handle.close();
  } catch (caught) {
    if (
      !(caught instanceof Error) ||
      !("code" in caught) ||
      /** @type {NodeJS.ErrnoException} */ (caught).code !== "EEXIST"
    ) {
      throw caught;
    }
    const existing = await readOptionalJson(lockPath);
    const pid =
      existing !== null && Number.isSafeInteger(existing.pid)
        ? Number(existing.pid)
        : null;
    let active = false;
    if (pid !== null) {
      try {
        process.kill(pid, 0);
        active = true;
      } catch (probeError) {
        if (
          !(probeError instanceof Error) ||
          !("code" in probeError) ||
          /** @type {NodeJS.ErrnoException} */ (probeError).code !== "ESRCH"
        ) {
          throw probeError;
        }
      }
    }
    if (active || pid === null) {
      throw new Error(`Polk Accela state is locked: ${lockPath}`);
    }
    await rm(lockPath);
    return acquireRunLock(stateDirectory);
  }
  return async () => {
    await rm(lockPath, { force: true });
  };
}

/**
 * Assemble immutable window parts.
 *
 * @param {readonly string[]} partPaths Ordered parts.
 * @param {string} output Aggregate output.
 * @returns {Promise<void>} Resolves after atomic rename.
 */
async function assembleOutput(partPaths, output) {
  await mkdir(path.dirname(output), { recursive: true });
  const temporary = `${output}.tmp-${process.pid}-${Date.now()}`;
  const writer = createWriteStream(temporary, { encoding: "utf8" });
  try {
    for (const partPath of partPaths) {
      const text = await readFile(partPath, "utf8");
      if (!writer.write(text)) await once(writer, "drain");
    }
    writer.end();
    await once(writer, "finish");
    await rename(temporary, output);
  } catch (caught) {
    writer.destroy();
    await rm(temporary, { force: true });
    throw caught;
  }
}

/**
 * Hash one file as a stream.
 *
 * @param {string} filePath File.
 * @returns {Promise<string>} SHA-256.
 */
async function sha256File(filePath) {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(filePath)) hash.update(chunk);
  return hash.digest("hex");
}

/**
 * Run the Polk County Accela CSV list workflow.
 *
 * @param {PolkAccelaListOptions} options Validated options.
 * @param {typeof fetch} [fetchImpl] Injectable fetch.
 * @returns {Promise<JsonObject>} Receipt.
 */
export async function runPolkAccelaList(options, fetchImpl = globalThis.fetch) {
  const allWindows = buildPolkAccelaWindows(
    options.startDate,
    options.endDate,
    options.windowMonths,
  );
  const windows =
    options.limitWindows === null
      ? allWindows
      : allWindows.slice(0, options.limitWindows);
  if (options.stage === "probe") {
    return {
      schemaVersion: "oracle-node.polk-county-accela-list-probe.v1",
      probedAt: new Date().toISOString(),
      portalUrl: POLK_COUNTY_ACCELA_PORTAL_URL,
      exportUrl: POLK_COUNTY_ACCELA_EXPORT_URL,
      requestedStartDate: options.startDate,
      requestedEndDate: options.endDate,
      windowMonths: options.windowMonths,
      totalWindowCount: allWindows.length,
      selectedWindowCount: windows.length,
      acquisition:
        "isolated ASP.NET date search followed by complete session CSV export",
      parcelMatchPolicy: "preserve list row; never infer parcel from address",
    };
  }
  if (
    options.stage === "harvest" &&
    windows.length > 3 &&
    !options.approveScale
  ) {
    throw new Error(
      `Polk Accela list harvest has ${windows.length} windows; --approve-scale is required after a documented GO decision.`,
    );
  }
  let checkpoint = await readCheckpoint(options.checkpoint);
  if (checkpoint !== null) {
    assertCheckpointCompatible(checkpoint, options, windows.length);
  } else if (options.stage === "verify") {
    throw new Error("Polk Accela verification requires a checkpoint");
  }
  /** @type {() => Promise<void>} */
  let releaseLock = async () => {};
  if (options.stage === "harvest") {
    releaseLock = await acquireRunLock(options.stateDirectory);
  }
  try {
    const verified = await verifyWindowParts(options.stateDirectory, windows);
    if (
      checkpoint !== null &&
      (checkpoint.completedWindowCount > verified.partPaths.length ||
        checkpoint.accessibleRecordCount > verified.recordCount ||
        checkpoint.sourceRecordCount > verified.sourceRecordCount ||
        checkpoint.exactDuplicateRowCount > verified.exactDuplicateRowCount)
    ) {
      throw new Error(
        "Polk Accela checkpoint claims committed work that did not verify",
      );
    }
    if (options.stage === "verify") {
      return {
        schemaVersion: "oracle-node.polk-county-accela-list-verification.v1",
        verifiedAt: new Date().toISOString(),
        output: options.output,
        checkpoint: options.checkpoint,
        stateDirectory: options.stateDirectory,
        verifiedWindowCount: verified.partPaths.length,
        requestedWindowCount: windows.length,
        verifiedRecordCount: verified.recordCount,
        verifiedSourceRecordCount: verified.sourceRecordCount,
        exactDuplicateRowCount: verified.exactDuplicateRowCount,
        uniqueRecordCount: verified.recordKeys.size,
        classCounts: verified.classCounts,
        complete: verified.partPaths.length === windows.length,
      };
    }
    if (checkpoint === null) {
      await writeCheckpoint(options.checkpoint, {
        portalUrl: POLK_COUNTY_ACCELA_PORTAL_URL,
        startDate: options.startDate,
        endDate: options.endDate,
        windowMonths: options.windowMonths,
        limitWindows: options.limitWindows,
        output: options.output,
        stateDirectory: options.stateDirectory,
        requestedWindowCount: windows.length,
        completedWindowCount: verified.partPaths.length,
        sourceRecordCount: verified.sourceRecordCount,
        exactDuplicateRowCount: verified.exactDuplicateRowCount,
        accessibleRecordCount: verified.recordCount,
        complete: verified.partPaths.length === windows.length,
      });
      checkpoint = await readCheckpoint(options.checkpoint);
    }
    let partPaths = verified.partPaths;
    let recordCount = verified.recordCount;
    let sourceRecordCount = verified.sourceRecordCount;
    let exactDuplicateRowCount = verified.exactDuplicateRowCount;
    let csvBytes = 0;
    let windowsInitiallyAtLeast100 = 0;
    for (let index = partPaths.length; index < windows.length; index += 1) {
      const window = windows[index];
      if (window === undefined) continue;
      const result = await fetchWindowWithRetry(window, options, fetchImpl);
      for (const record of result.records) {
        if (verified.recordKeys.has(record.sourceRecordKey)) {
          throw new Error(
            `Duplicate Polk Accela record across windows: ${record.permitNumber}`,
          );
        }
        verified.recordKeys.add(record.sourceRecordKey);
        verified.classCounts[record.recordClass] += 1;
      }
      const partPath = await writeWindowPart(
        options.stateDirectory,
        index,
        window,
        result.records,
        result.sourceRecordCount,
        result.exactDuplicateRowCount,
      );
      partPaths = [...partPaths, partPath];
      recordCount += result.records.length;
      sourceRecordCount += result.sourceRecordCount;
      exactDuplicateRowCount += result.exactDuplicateRowCount;
      csvBytes += result.csvBytes;
      if (result.initialResultAtLeast100) windowsInitiallyAtLeast100 += 1;
      await writeCheckpoint(options.checkpoint, {
        portalUrl: POLK_COUNTY_ACCELA_PORTAL_URL,
        startDate: options.startDate,
        endDate: options.endDate,
        windowMonths: options.windowMonths,
        limitWindows: options.limitWindows,
        output: options.output,
        stateDirectory: options.stateDirectory,
        requestedWindowCount: windows.length,
        completedWindowCount: partPaths.length,
        sourceRecordCount,
        exactDuplicateRowCount,
        accessibleRecordCount: recordCount,
        complete: partPaths.length === windows.length,
      });
      process.stdout.write(
        `${JSON.stringify({
          event: "polk_accela_list_progress",
          windowIndex: index,
          window,
          windowRecordCount: result.records.length,
          windowSourceRecordCount: result.sourceRecordCount,
          windowExactDuplicateRowCount: result.exactDuplicateRowCount,
          initialResultSummary: result.initialResultSummary,
          completedWindowCount: partPaths.length,
          requestedWindowCount: windows.length,
          accessibleRecordCount: recordCount,
        })}\n`,
      );
      if (index + 1 < windows.length) {
        await new Promise((resolve) => setTimeout(resolve, options.delayMs));
      }
    }
    if (
      partPaths.length !== windows.length ||
      recordCount !== verified.recordKeys.size ||
      sourceRecordCount - recordCount !== exactDuplicateRowCount
    ) {
      throw new Error("Polk Accela final window reconciliation failed");
    }
    await assembleOutput(partPaths, options.output);
    const outputInfo = await stat(options.output);
    const receipt = {
      schemaVersion: "oracle-node.polk-county-accela-list-harvest.v1",
      harvestedAt: new Date().toISOString(),
      portalUrl: POLK_COUNTY_ACCELA_PORTAL_URL,
      exportUrl: POLK_COUNTY_ACCELA_EXPORT_URL,
      startDate: options.startDate,
      endDate: options.endDate,
      windowMonths: options.windowMonths,
      requestedWindowCount: windows.length,
      completedWindowCount: partPaths.length,
      sourceRecordCount,
      exactDuplicateRowCount,
      accessibleRecordCount: recordCount,
      uniqueRecordCount: verified.recordKeys.size,
      classCounts: verified.classCounts,
      csvBytesThisRun: csvBytes,
      windowsInitiallyAtLeast100ThisRun: windowsInitiallyAtLeast100,
      output: options.output,
      outputBytes: outputInfo.size,
      outputSha256: await sha256File(options.output),
      checkpoint: options.checkpoint,
      stateDirectory: options.stateDirectory,
      pilot: options.limitWindows !== null,
      parcelMatchPolicy:
        "unmatched list records preserved; no inferred relation",
      complete: true,
      historicalCoverageComplete: false,
      historicalCoverageReason:
        "This harvest exposes licensing rows from 2003-12-17 and permit/application rows from 2005-12-20; earlier County/Hansen custody and completeness remain unresolved.",
    };
    await writeAtomicText(
      options.receipt,
      `${JSON.stringify(receipt, null, 2)}\n`,
    );
    return receipt;
  } finally {
    await releaseLock();
  }
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  runPolkAccelaList(parsePolkAccelaListOptions(process.argv.slice(2)))
    .then((receipt) => {
      process.stdout.write(`${JSON.stringify(receipt, null, 2)}\n`);
    })
    .catch((caught) => {
      const error = caught instanceof Error ? caught.message : String(caught);
      process.stderr.write(
        `${JSON.stringify({ event: "polk_accela_list_failed", error })}\n`,
      );
      process.exitCode = 1;
    });
}
