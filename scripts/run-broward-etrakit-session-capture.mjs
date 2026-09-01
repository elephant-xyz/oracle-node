#!/usr/bin/env node
// @ts-check

/**
 * Capture a bounded Coral Springs eTRAKiT result slice from an already-open,
 * manually CAPTCHA-authorized Chrome tab.
 *
 * This process never launches a browser, reads cookies, copies CAPTCHA values,
 * refreshes the page, or submits a new search. It attaches to the one visible
 * Chrome window through its existing DevTools console, reads only allow-listed
 * list cells, and invokes the portal's own Telerik `Page` postback command.
 */

import { createHash, randomUUID } from "node:crypto";
import { spawn } from "node:child_process";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

const SOURCE_SYSTEM = "broward_coral_springs_etrakit_permits";
const JURISDICTION = "Coral Springs";
const SEARCH_URL =
  "https://etrakit.coralsprings.gov/eTRAKiT/Search/permit.aspx";
const SEARCH_ORIGIN = "https://etrakit.coralsprings.gov";
const SEARCH_PATH = "/eTRAKiT/Search/permit.aspx";
const SCHEMA_VERSION = "oracle-node.broward-etrakit-list.v1";
const CHECKPOINT_SCHEMA_VERSION =
  "oracle-node.broward-etrakit-capture-checkpoint.v1";
const SUMMARY_SCHEMA_VERSION = "oracle-node.broward-etrakit-capture-summary.v1";

/**
 * @typedef {object} EtrakitCaptureOptions
 * @property {string} outputDirectory - Owner-only ignored artifact directory.
 * @property {number} sourceReportedCount - Operator-observed source count.
 * @property {number} expectedPageCount - Exact exposed page count.
 * @property {number} expectedPageSize - Exact rows per exposed page.
 * @property {number} delayMs - Delay after each source postback.
 * @property {number} consoleDeadlineMs - Deadline for one console exchange.
 * @property {number} pageDeadlineMs - Deadline for one page to become readable.
 *
 * @typedef {object} BrowserListRow
 * @property {string} sourceRecordId - Stable eTRAKiT RECORDID cell.
 * @property {string} permitNumber - Public permit number.
 * @property {string | null} recordType - Public permit type.
 * @property {string | null} status - Public permit status.
 * @property {string | null} address - Public site address.
 * @property {string | null} folio - Public list folio.
 *
 * @typedef {object} EtrakitListRecord
 * @property {"oracle-node.broward-etrakit-list.v1"} schemaVersion
 *   Versioned allow-listed list contract.
 * @property {"broward_coral_springs_etrakit_permits"} sourceSystem
 *   Stable Broward source system.
 * @property {"Coral Springs"} jurisdiction - Issuing jurisdiction.
 * @property {string} sourceRecordId - Stable eTRAKiT RECORDID.
 * @property {string} recordKey - Stable source-system-qualified identity.
 * @property {string} permitNumber - Public permit number.
 * @property {string | null} recordType - Public permit taxonomy.
 * @property {string | null} status - Public permit status.
 * @property {string | null} address - Public site address.
 * @property {string | null} folio - Public permit folio.
 * @property {string} sourceUrl - Token-free official search URL.
 * @property {number[]} sourcePages - One-based exposed pages containing row.
 * @property {boolean} isRoofPermit - Source-query-backed roofing classification.
 * @property {Readonly<{
 *   queryField:"Permit Type",
 *   queryOperator:"Contains",
 *   queryValue:"ROOF",
 *   sourceReportedCount:number,
 *   exposedRecordCap:number,
 *   exposedPageCount:number,
 *   pageSize:number,
 *   completenessBoundary:"bounded_capped_keyword_slice",
 *   countEvidence:"operator_observed_source_result"
 * }>} coverage - Explicit non-completeness provenance.
 *
 * @typedef {object} EtrakitPageReceipt
 * @property {number} page - One-based page.
 * @property {number} rowCount - Reconciled rows read on the page.
 * @property {string} rowDigest - SHA-256 over stable page identities.
 *
 * @typedef {object} EtrakitCaptureCheckpoint
 * @property {"oracle-node.broward-etrakit-capture-checkpoint.v1"} schemaVersion
 *   Checkpoint contract.
 * @property {string} sourceSystem - Stable source system.
 * @property {number} sourceReportedCount - Operator-observed result count.
 * @property {number} expectedPageCount - Exact exposed pages.
 * @property {number} expectedPageSize - Exact exposed page size.
 * @property {Record<string, EtrakitPageReceipt>} completedPages
 *   Page receipts keyed by one-based page number.
 * @property {number} capturedRowCount - Total reconciled page rows.
 * @property {number} uniqueRecordCount - Stable unique records.
 * @property {number} duplicateRecordCount - Exact cross-page duplicates.
 * @property {number} conflictRecordCount - Conflicting stable identities.
 * @property {boolean} completed - Whether every exposed page reconciled.
 * @property {string} updatedAt - Last durable checkpoint timestamp.
 *
 * @typedef {object} BrowserCaptureEnvelope
 * @property {string} nonce - Per-exchange clipboard lineage.
 * @property {Readonly<{
 *   title:string,
 *   origin:string,
 *   path:string,
 *   searchBy:string,
 *   searchOperator:string,
 *   searchValue:string,
 *   pageCount:number,
 *   pageSize:number,
 *   currentPage:number,
 *   rowCount:number,
 *   formMethod:string,
 *   formActionPath:string,
 *   viewStatePresent:boolean,
 *   eventValidationPresent:boolean,
 *   postBackTarget:string,
 *   postBackArgumentPrefix:string
 *   pagerText:string
 * }>} contract - Session-free source contract.
 * @property {BrowserListRow[]} records - Allow-listed current-page records.
 */

/**
 * Parse a fixed-purpose session capture command.
 *
 * @param {readonly string[]} argv - CLI arguments after the script path.
 * @returns {EtrakitCaptureOptions} Validated bounded capture options.
 */
export function parseEtrakitCaptureOptions(argv) {
  /** @type {Map<string, string>} */
  const values = new Map();
  for (let index = 0; index < argv.length; index += 2) {
    const flag = argv[index];
    const value = argv[index + 1];
    if (
      typeof flag !== "string" ||
      !flag.startsWith("--") ||
      typeof value !== "string" ||
      value.startsWith("--")
    ) {
      throw new Error("eTRAKiT capture options must be --flag value pairs");
    }
    values.set(flag.slice(2), value);
  }
  const outputDirectory = values.get("output-dir");
  if (typeof outputDirectory !== "string" || outputDirectory.trim() === "") {
    throw new Error("--output-dir is required");
  }
  return {
    outputDirectory,
    sourceReportedCount: readBoundedInteger(
      values.get("reported-count"),
      "--reported-count",
      1_001,
      10_000_000,
    ),
    expectedPageCount: readBoundedInteger(
      values.get("expected-pages") ?? "50",
      "--expected-pages",
      1,
      50,
    ),
    expectedPageSize: readBoundedInteger(
      values.get("page-size") ?? "20",
      "--page-size",
      1,
      100,
    ),
    delayMs: readBoundedInteger(
      values.get("delay-ms") ?? "2500",
      "--delay-ms",
      1_500,
      60_000,
    ),
    consoleDeadlineMs: readBoundedInteger(
      values.get("console-deadline-ms") ?? "8000",
      "--console-deadline-ms",
      2_000,
      60_000,
    ),
    pageDeadlineMs: readBoundedInteger(
      values.get("page-deadline-ms") ?? "30000",
      "--page-deadline-ms",
      5_000,
      120_000,
    ),
  };
}

/**
 * Read one bounded integer CLI value.
 *
 * @param {string | undefined} value - Candidate value.
 * @param {string} flag - Public-safe option name.
 * @param {number} minimum - Inclusive minimum.
 * @param {number} maximum - Inclusive maximum.
 * @returns {number} Validated integer.
 */
function readBoundedInteger(value, flag, minimum, maximum) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < minimum || parsed > maximum) {
    throw new Error(
      `${flag} must be an integer from ${String(minimum)} through ${String(maximum)}`,
    );
  }
  return parsed;
}

/**
 * Normalize a source list cell without inferring missing values.
 *
 * @param {unknown} value - Candidate browser cell text.
 * @returns {string | null} Collapsed source text.
 */
function optionalText(value) {
  if (typeof value !== "string") return null;
  const normalized = value
    .replace(/\u00a0/gu, " ")
    .replace(/\s+/gu, " ")
    .trim();
  return normalized === "" ? null : normalized;
}

/**
 * Normalize and validate a stable required source value.
 *
 * @param {unknown} value - Candidate browser value.
 * @param {string} fieldName - Public-safe field label.
 * @returns {string} Stable non-empty source text.
 */
function requiredText(value, fieldName) {
  const normalized = optionalText(value);
  if (normalized === null || normalized.length > 500) {
    throw new Error(`eTRAKiT ${fieldName} is missing or invalid`);
  }
  return normalized;
}

/**
 * Build the committed list-only record from one allow-listed browser row.
 *
 * @param {BrowserListRow} row - Current source page row.
 * @param {number} page - One-based source page.
 * @param {EtrakitCaptureOptions} options - Fixed source coverage.
 * @returns {EtrakitListRecord} Stable privacy-minimized inventory row.
 */
export function normalizeEtrakitListRecord(row, page, options) {
  const sourceRecordId = requiredText(
    row.sourceRecordId,
    "source record identity",
  );
  if (!/^[A-Z0-9_:-]+$/iu.test(sourceRecordId)) {
    throw new Error("eTRAKiT source record identity is malformed");
  }
  const permitNumber = requiredText(row.permitNumber, "permit number");
  const recordType = optionalText(row.recordType);
  if (recordType === null || !recordType.toUpperCase().includes("ROOF")) {
    throw new Error("eTRAKiT row does not reconcile to the roofing query");
  }
  return {
    schemaVersion: SCHEMA_VERSION,
    sourceSystem: SOURCE_SYSTEM,
    jurisdiction: JURISDICTION,
    sourceRecordId,
    recordKey: `${SOURCE_SYSTEM}:record:${sourceRecordId}`,
    permitNumber,
    recordType,
    status: optionalText(row.status),
    address: optionalText(row.address),
    folio: optionalText(row.folio),
    sourceUrl: SEARCH_URL,
    sourcePages: [page],
    isRoofPermit: true,
    coverage: Object.freeze({
      queryField: "Permit Type",
      queryOperator: "Contains",
      queryValue: "ROOF",
      sourceReportedCount: options.sourceReportedCount,
      exposedRecordCap: options.expectedPageCount * options.expectedPageSize,
      exposedPageCount: options.expectedPageCount,
      pageSize: options.expectedPageSize,
      completenessBoundary: "bounded_capped_keyword_slice",
      countEvidence: "operator_observed_source_result",
    }),
  };
}

/**
 * Validate the session-free form, search, and pagination contract.
 *
 * @param {BrowserCaptureEnvelope["contract"]} contract - Browser metadata.
 * @param {number} expectedPage - One-based expected current page.
 * @param {EtrakitCaptureOptions} options - Fixed capture limits.
 * @returns {void}
 */
export function validateEtrakitBrowserContract(
  contract,
  expectedPage,
  options,
) {
  if (
    contract.title !== "eTRAKiT" ||
    contract.origin !== SEARCH_ORIGIN ||
    contract.path !== SEARCH_PATH ||
    contract.searchBy !== "Permit_Main.PERMITTYPE" ||
    contract.searchOperator !== "CONTAINS" ||
    contract.searchValue !== "ROOF" ||
    contract.pageCount !== options.expectedPageCount ||
    contract.pageSize !== options.expectedPageSize ||
    contract.currentPage !== expectedPage ||
    contract.rowCount !== options.expectedPageSize ||
    contract.formMethod.toLowerCase() !== "post" ||
    contract.formActionPath !== SEARCH_PATH ||
    contract.viewStatePresent !== true ||
    contract.postBackTarget !== "ctl00$cplMain$rgSearchRslts" ||
    contract.postBackArgumentPrefix !==
      "FireCommand:ctl00$cplMain$rgSearchRslts$ctl00;Page;" ||
    !contract.pagerText.endsWith(
      `Buttons to move Next/Previous page ${String(expectedPage)} of ${String(options.expectedPageCount)}`,
    )
  ) {
    throw new Error(
      "eTRAKiT browser result contract is not the approved slice",
    );
  }
  if (contract.eventValidationPresent !== false) {
    throw new Error(
      "eTRAKiT EventValidation presence changed; recertification required",
    );
  }
}

/**
 * Reconcile one page into a stable record map.
 *
 * Exact duplicate identities merge source-page provenance. Any changed
 * inventory fact for the same stable identity fails closed.
 *
 * @param {Map<string, EtrakitListRecord>} records - Prior durable records.
 * @param {readonly BrowserListRow[]} rows - Current allow-listed source rows.
 * @param {number} page - One-based page.
 * @param {EtrakitCaptureOptions} options - Fixed capture options.
 * @returns {{receipt:EtrakitPageReceipt,duplicateCount:number}} Page receipt.
 */
export function reconcileEtrakitPage(records, rows, page, options) {
  if (rows.length !== options.expectedPageSize) {
    throw new Error(
      "eTRAKiT page row count does not match the exposed contract",
    );
  }
  let duplicateCount = 0;
  /** @type {string[]} */
  const pageKeys = [];
  for (const row of rows) {
    const candidate = normalizeEtrakitListRecord(row, page, options);
    pageKeys.push(candidate.recordKey);
    const existing = records.get(candidate.recordKey);
    if (existing === undefined) {
      records.set(candidate.recordKey, candidate);
      continue;
    }
    const existingWithoutPages = { ...existing, sourcePages: [] };
    const candidateWithoutPages = { ...candidate, sourcePages: [] };
    if (
      stableJson(existingWithoutPages) !== stableJson(candidateWithoutPages)
    ) {
      throw new Error("eTRAKiT stable identity has conflicting list facts");
    }
    duplicateCount += 1;
    records.set(candidate.recordKey, {
      ...existing,
      sourcePages: [...new Set([...existing.sourcePages, page])].sort(
        (left, right) => left - right,
      ),
    });
  }
  pageKeys.sort();
  return {
    receipt: {
      page,
      rowCount: rows.length,
      rowDigest: createHash("sha256").update(pageKeys.join("\n")).digest("hex"),
    },
    duplicateCount,
  };
}

/**
 * Stable JSON serializer used only for conflict checks and durable JSONL.
 *
 * @param {unknown} value - JSON-compatible value.
 * @returns {string} Recursively key-sorted JSON.
 */
function stableJson(value) {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map((entry) => stableJson(entry)).join(",")}]`;
  }
  const record = /** @type {Record<string, unknown>} */ (value);
  return `{${Object.keys(record)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${stableJson(record[key])}`)
    .join(",")}}`;
}

/**
 * Render deterministic private JSONL.
 *
 * @param {Iterable<EtrakitListRecord>} records - Reconciled records.
 * @returns {string} Stable newline-terminated JSONL.
 */
export function renderEtrakitListJsonl(records) {
  const sorted = [...records].sort((left, right) =>
    left.recordKey.localeCompare(right.recordKey),
  );
  return `${sorted.map((record) => stableJson(record)).join("\n")}\n`;
}

/**
 * Run a finite child process without a shell.
 *
 * Captured output is returned only to the caller and is never logged. Error
 * messages contain the fixed executable name, not stdout/stderr or arguments.
 *
 * @param {string} executable - Fixed local utility.
 * @param {readonly string[]} args - Exact argument vector.
 * @param {{input?:string,timeoutMs?:number,maxBytes?:number}} [options]
 *   Process limits.
 * @returns {Promise<string>} Captured UTF-8 stdout.
 */
async function runProcess(
  executable,
  args,
  { input, timeoutMs = 10_000, maxBytes = 1_000_000 } = {},
) {
  return new Promise((resolvePromise, rejectPromise) => {
    const child = spawn(executable, [...args], {
      stdio: ["pipe", "pipe", "pipe"],
    });
    /** @type {Buffer[]} */
    const output = [];
    let outputBytes = 0;
    let settled = false;
    const finish = (
      /** @type {Error | null} */ error,
      /** @type {string | null} */ value,
    ) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (error !== null) rejectPromise(error);
      else resolvePromise(value ?? "");
    };
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      finish(new Error(`${executable} exceeded its finite deadline`), null);
    }, timeoutMs);
    child.stdout.on("data", (chunk) => {
      const buffer = Buffer.from(chunk);
      outputBytes += buffer.length;
      if (outputBytes > maxBytes) {
        child.kill("SIGKILL");
        finish(new Error(`${executable} exceeded its output ceiling`), null);
        return;
      }
      output.push(buffer);
    });
    child.on("error", () => {
      finish(new Error(`${executable} could not be executed`), null);
    });
    child.on("close", (code) => {
      if (code !== 0) {
        finish(new Error(`${executable} exited unsuccessfully`), null);
        return;
      }
      finish(null, Buffer.concat(output).toString("utf8"));
    });
    child.stdin.end(input);
  });
}

/**
 * Return the exact one visible Chrome window without launching another.
 *
 * @returns {Promise<string>} Existing X11 window identifier.
 */
async function requireExistingChromeWindow() {
  const output = await runProcess("xdotool", [
    "search",
    "--onlyvisible",
    "--class",
    "google-chrome",
  ]);
  const identifiers = output
    .split(/\r?\n/u)
    .map((value) => value.trim())
    .filter((value) => /^\d+$/u.test(value));
  if (identifiers.length !== 1) {
    throw new Error("Exactly one existing visible Chrome window is required");
  }
  return /** @type {string} */ (identifiers[0]);
}

/**
 * Activate the existing window and type one console expression.
 *
 * @param {string} windowId - Existing X11 Chrome window.
 * @param {string} expression - Fixed generated JavaScript expression.
 * @returns {Promise<void>} Resolves after Enter is sent.
 */
async function typeConsoleExpression(windowId, expression) {
  await runProcess("xdotool", ["windowactivate", "--sync", windowId]);
  await runProcess("xdotool", [
    "type",
    "--window",
    windowId,
    "--clearmodifiers",
    "--delay",
    "1",
    "--",
    expression,
  ]);
  await runProcess("xdotool", ["key", "--window", windowId, "Return"]);
}

/**
 * Replace the clipboard with a non-sensitive exchange marker.
 *
 * @param {string} marker - Random marker containing no source data.
 * @returns {Promise<void>} Resolves after clipboard ownership is established.
 */
async function setClipboardMarker(marker) {
  await /** @type {Promise<void>} */ (
    new Promise((resolvePromise, rejectPromise) => {
      const child = spawn("xclip", ["-selection", "clipboard"], {
        stdio: ["pipe", "ignore", "ignore"],
      });
      let settled = false;
      const finish = (/** @type {Error | null} */ error) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        if (error === null) resolvePromise();
        else rejectPromise(error);
      };
      const timer = setTimeout(() => {
        /*
         * A live xclip process owns the selection until the browser replaces it.
         * Ownership, rather than process exit, is the successful steady state.
         */
        finish(null);
      }, 100);
      child.on("error", () => {
        finish(new Error("xclip could not establish clipboard ownership"));
      });
      child.stdin.end(marker);
    })
  );
}

/**
 * Read the current clipboard privately.
 *
 * @returns {Promise<string>} Clipboard contents retained only in memory.
 */
async function readClipboard() {
  return runProcess("xclip", ["-selection", "clipboard", "-o"], {
    timeoutMs: 3_000,
    maxBytes: 2_000_000,
  });
}

/**
 * Wait without issuing source requests.
 *
 * @param {number} delayMs - Finite delay.
 * @returns {Promise<void>} Resolves after delay.
 */
async function wait(delayMs) {
  await new Promise((resolvePromise) => setTimeout(resolvePromise, delayMs));
}

/**
 * Execute one console `copy(JSON.stringify(...))` exchange.
 *
 * The marker prevents stale or pre-existing clipboard contents from being
 * interpreted as source output.
 *
 * @param {string} windowId - Existing Chrome window.
 * @param {string} expression - Console expression that includes the nonce.
 * @param {string} nonce - Expected exchange lineage.
 * @param {number} deadlineMs - Finite clipboard deadline.
 * @returns {Promise<Record<string, unknown>>} Parsed exchange object.
 */
async function executeConsoleExchange(windowId, expression, nonce, deadlineMs) {
  const marker = `etrakit-capture-pending:${nonce}`;
  await setClipboardMarker(marker);
  await typeConsoleExpression(windowId, expression);
  const deadlineAt = Date.now() + deadlineMs;
  while (Date.now() < deadlineAt) {
    const contents = await readClipboard();
    if (contents !== marker) {
      let parsed;
      try {
        parsed = /** @type {unknown} */ (JSON.parse(contents));
      } catch {
        throw new Error("eTRAKiT console returned non-JSON data");
      }
      if (
        parsed === null ||
        typeof parsed !== "object" ||
        Array.isArray(parsed) ||
        /** @type {Record<string, unknown>} */ (parsed).nonce !== nonce
      ) {
        throw new Error("eTRAKiT console exchange lineage did not match");
      }
      return /** @type {Record<string, unknown>} */ (parsed);
    }
    await wait(100);
  }
  throw new Error("eTRAKiT console exchange timed out");
}

/**
 * Build the privacy-minimized current-page extraction expression.
 *
 * Cell indexes come from the explicitly reconciled portal header contract:
 * permit number, permit type, status, site address, ignored owner, ignored
 * contractor, RECORDID, and folio.
 *
 * @param {string} nonce - Per-exchange lineage.
 * @returns {string} One-line DevTools console expression.
 */
export function buildEtrakitCaptureExpression(nonce) {
  const encodedNonce = JSON.stringify(nonce);
  return `(()=>{const nonce=${encodedNonce};const clean=v=>{const t=String(v??"").replace(/\\u00a0/g," ").replace(/\\s+/g," ").trim();return t===""?null:t};const grid=document.getElementById("ctl00_cplMain_rgSearchRslts");const rows=[...grid.querySelectorAll(".rgRow,.rgAltRow")];const pagerGrid=grid.cloneNode(true);pagerGrid.querySelectorAll(".rgRow,.rgAltRow").forEach(row=>row.remove());const form=document.getElementById("form1");const records=rows.map(row=>{const c=row.cells;return {sourceRecordId:clean(c[6]?.innerText),permitNumber:clean(c[0]?.innerText),recordType:clean(c[1]?.innerText),status:clean(c[2]?.innerText),address:clean(c[3]?.innerText),folio:clean(c[7]?.innerText)}});copy(JSON.stringify({nonce,contract:{title:document.title,origin:location.origin,path:location.pathname,searchBy:document.getElementById("cplMain_ddSearchBy").value,searchOperator:document.getElementById("cplMain_ddSearchOper").value,searchValue:document.getElementById("cplMain_txtSearchString").value,pageCount:tableView.get_pageCount(),pageSize:tableView.get_pageSize(),currentPage:tableView.get_currentPageIndex()+1,rowCount:rows.length,formMethod:form.method,formActionPath:new URL(form.action).pathname,viewStatePresent:!!form.querySelector('input[name="__VIEWSTATE"]'),eventValidationPresent:!!form.querySelector('input[name="__EVENTVALIDATION"]'),postBackTarget:tableView._owner.UniqueID,postBackArgumentPrefix:"FireCommand:"+tableView._data.UniqueID+";Page;",pagerText:(pagerGrid.innerText||pagerGrid.textContent||"").replace(/\\s+/g," ").trim()},records}))})()`;
}

/**
 * Build one exact Telerik next-page postback expression.
 *
 * The portal's own `tableView.page()` constructs
 * `__doPostBack(gridUniqueId, FireCommand:...;Page;<page>)` using the current
 * document's ViewState. No hidden field value is copied from the browser.
 *
 * @param {string} nonce - Per-command lineage.
 * @param {number} page - One-based expected target page.
 * @returns {string} One-line DevTools console expression.
 */
export function buildEtrakitPageExpression(nonce, page) {
  return `(()=>{const nonce=${JSON.stringify(nonce)};copy(JSON.stringify({nonce,command:"PageNext",page:${String(page)}}));document.getElementById("ctl00_cplMain_rgSearchRslts_ctl00_ctl03_ctl01_btnPageNext").click()})()`;
}

/**
 * Build one browser-resident sequential page pump.
 *
 * Typing a new console expression while Telerik replaces the grid can lose
 * keystrokes. This pump is injected once while the page is stable, invokes
 * only the rendered next button, waits for both the server-rendered pager and
 * Telerik page index, and then copies one allow-listed page envelope. The
 * external writer has the entire conservative delay to durably consume each
 * envelope before the next postback.
 *
 * @param {string} nonce - Stable lineage for this pump.
 * @param {number} firstPage - One-based page currently rendered.
 * @param {number} finalPage - One-based final exposed page.
 * @param {number} delayMs - Conservative delay between source postbacks.
 * @param {number} pageDeadlineMs - Finite page-transition deadline.
 * @returns {string} One-line asynchronous console expression.
 */
export function buildEtrakitPagePumpExpression(
  nonce,
  firstPage,
  finalPage,
  delayMs,
  pageDeadlineMs,
) {
  return `(async()=>{const publish=copy;const nonce=${JSON.stringify(nonce)};const clean=v=>{const t=String(v??"").replace(/\\u00a0/g," ").replace(/\\s+/g," ").trim();return t===""?null:t};const wait=ms=>new Promise(resolve=>setTimeout(resolve,ms));const read=()=>{const grid=document.getElementById("ctl00_cplMain_rgSearchRslts");const rows=[...grid.querySelectorAll(".rgRow,.rgAltRow")];const pagerGrid=grid.cloneNode(true);pagerGrid.querySelectorAll(".rgRow,.rgAltRow").forEach(row=>row.remove());const form=document.getElementById("form1");return {nonce,contract:{title:document.title,origin:location.origin,path:location.pathname,searchBy:document.getElementById("cplMain_ddSearchBy").value,searchOperator:document.getElementById("cplMain_ddSearchOper").value,searchValue:document.getElementById("cplMain_txtSearchString").value,pageCount:tableView.get_pageCount(),pageSize:tableView.get_pageSize(),currentPage:tableView.get_currentPageIndex()+1,rowCount:rows.length,formMethod:form.method,formActionPath:new URL(form.action).pathname,viewStatePresent:!!form.querySelector('input[name="__VIEWSTATE"]'),eventValidationPresent:!!form.querySelector('input[name="__EVENTVALIDATION"]'),postBackTarget:tableView._owner.UniqueID,postBackArgumentPrefix:"FireCommand:"+tableView._data.UniqueID+";Page;",pagerText:(pagerGrid.innerText||pagerGrid.textContent||"").replace(/\\s+/g," ").trim()},records:rows.map(row=>{const c=row.cells;return {sourceRecordId:clean(c[6]?.innerText),permitNumber:clean(c[0]?.innerText),recordType:clean(c[1]?.innerText),status:clean(c[2]?.innerText),address:clean(c[3]?.innerText),folio:clean(c[7]?.innerText)}})}};const ready=page=>{const value=read();return value.contract.currentPage===page&&value.contract.pagerText.endsWith("Buttons to move Next/Previous page "+page+" of "+${String(finalPage)})};for(let page=${String(firstPage)};page<=${String(finalPage)};page++){const deadline=Date.now()+${String(pageDeadlineMs)};while(!ready(page)){if(Date.now()>=deadline)throw new Error("eTRAKiT page transition deadline");await wait(250)}publish(JSON.stringify(read()));if(page<${String(finalPage)}){await wait(${String(delayMs)});document.getElementById("ctl00_cplMain_rgSearchRslts_ctl00_ctl03_ctl01_btnPageNext").click()}}})()`;
}

/**
 * Build a session-free probe proving the active console context.
 *
 * @param {string} nonce - Per-exchange lineage.
 * @returns {string} One-line console expression.
 */
function buildTabProbeExpression(nonce) {
  return `copy(JSON.stringify({nonce:${JSON.stringify(nonce)},title:document.title,origin:location.origin,path:location.pathname}))`;
}

/**
 * Ensure the existing DevTools console is ready without launching or
 * navigating a browser. The first probe reuses current focus; toggles are used
 * only when the console is closed or another browser pane owns focus.
 *
 * @param {string} windowId - Existing Chrome window.
 * @param {number} deadlineMs - Per-probe deadline.
 * @returns {Promise<void>} Resolves only in the approved tab context.
 */
async function ensureConsoleReady(windowId, deadlineMs) {
  for (let attempt = 0; attempt < 3; attempt += 1) {
    if (attempt > 0) {
      await runProcess("xdotool", [
        "key",
        "--window",
        windowId,
        "ctrl+shift+j",
      ]);
      await wait(750);
    }
    const nonce = randomUUID();
    try {
      const result = await executeConsoleExchange(
        windowId,
        buildTabProbeExpression(nonce),
        nonce,
        deadlineMs,
      );
      if (
        result.title === "eTRAKiT" &&
        result.origin === SEARCH_ORIGIN &&
        result.path === SEARCH_PATH
      ) {
        return;
      }
      throw new Error("Existing Chrome tab is not the approved eTRAKiT page");
    } catch (error) {
      if (attempt === 2) throw error;
    }
  }
}

/**
 * Read an optional private JSON object.
 *
 * @param {string} filePath - Owner-only artifact path.
 * @returns {Promise<unknown | undefined>} Parsed value or undefined if absent.
 */
async function readOptionalJson(filePath) {
  try {
    return /** @type {unknown} */ (
      JSON.parse(await readFile(filePath, "utf8"))
    );
  } catch (error) {
    if (error instanceof Error && "code" in error && error.code === "ENOENT") {
      return undefined;
    }
    throw error;
  }
}

/**
 * Read and validate a prior private checkpoint.
 *
 * @param {unknown} value - Parsed optional checkpoint.
 * @param {EtrakitCaptureOptions} options - Current immutable capture options.
 * @returns {EtrakitCaptureCheckpoint | undefined} Validated checkpoint.
 */
export function validateEtrakitCheckpoint(value, options) {
  if (value === undefined) return undefined;
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("eTRAKiT checkpoint is malformed");
  }
  const checkpoint = /** @type {Record<string, unknown>} */ (value);
  if (
    checkpoint.schemaVersion !== CHECKPOINT_SCHEMA_VERSION ||
    checkpoint.sourceSystem !== SOURCE_SYSTEM ||
    checkpoint.sourceReportedCount !== options.sourceReportedCount ||
    checkpoint.expectedPageCount !== options.expectedPageCount ||
    checkpoint.expectedPageSize !== options.expectedPageSize ||
    checkpoint.completedPages === null ||
    typeof checkpoint.completedPages !== "object" ||
    Array.isArray(checkpoint.completedPages) ||
    typeof checkpoint.completed !== "boolean"
  ) {
    throw new Error("eTRAKiT checkpoint lineage does not match");
  }
  return /** @type {EtrakitCaptureCheckpoint} */ (value);
}

/**
 * Read prior normalized private records.
 *
 * @param {string} filePath - Owner-only JSONL path.
 * @returns {Promise<Map<string, EtrakitListRecord>>} Stable record map.
 */
async function readExistingRecords(filePath) {
  /** @type {Map<string, EtrakitListRecord>} */
  const records = new Map();
  let text;
  try {
    text = await readFile(filePath, "utf8");
  } catch (error) {
    if (error instanceof Error && "code" in error && error.code === "ENOENT") {
      return records;
    }
    throw error;
  }
  for (const line of text.split(/\r?\n/u)) {
    if (line.trim() === "") continue;
    const parsed = /** @type {unknown} */ (JSON.parse(line));
    if (
      parsed === null ||
      typeof parsed !== "object" ||
      Array.isArray(parsed)
    ) {
      throw new Error("eTRAKiT private JSONL is malformed");
    }
    const candidate = /** @type {Record<string, unknown>} */ (parsed);
    if (
      candidate.schemaVersion !== SCHEMA_VERSION ||
      candidate.sourceSystem !== SOURCE_SYSTEM ||
      typeof candidate.recordKey !== "string" ||
      records.has(candidate.recordKey)
    ) {
      throw new Error("eTRAKiT private JSONL identity is invalid");
    }
    records.set(candidate.recordKey, /** @type {EtrakitListRecord} */ (parsed));
  }
  return records;
}

/**
 * Atomically replace one owner-only artifact.
 *
 * @param {string} filePath - Final ignored artifact path.
 * @param {string} contents - Complete UTF-8 contents.
 * @returns {Promise<void>} Resolves after atomic replacement.
 */
async function writePrivateFile(filePath, contents) {
  const temporaryPath = `${filePath}.${String(process.pid)}.tmp`;
  await writeFile(temporaryPath, contents, {
    encoding: "utf8",
    mode: 0o600,
  });
  await rename(temporaryPath, filePath);
}

/**
 * Parse and validate one browser capture envelope.
 *
 * @param {Record<string, unknown>} value - Parsed console exchange.
 * @returns {BrowserCaptureEnvelope} Shape-validated envelope.
 */
function readBrowserCaptureEnvelope(value) {
  if (
    value.contract === null ||
    typeof value.contract !== "object" ||
    Array.isArray(value.contract) ||
    !Array.isArray(value.records)
  ) {
    throw new Error("eTRAKiT browser capture envelope is malformed");
  }
  for (const row of value.records) {
    if (row === null || typeof row !== "object" || Array.isArray(row)) {
      throw new Error("eTRAKiT browser list row is malformed");
    }
  }
  return /** @type {BrowserCaptureEnvelope} */ (value);
}

/**
 * Read one expected page, retrying only local console access until the page
 * deadline. Retries never issue a source request.
 *
 * @param {string} windowId - Existing Chrome window.
 * @param {number} expectedPage - One-based page.
 * @param {EtrakitCaptureOptions} options - Capture deadlines and contract.
 * @returns {Promise<BrowserCaptureEnvelope>} Reconciled current page.
 */
async function readExpectedBrowserPage(windowId, expectedPage, options) {
  const deadlineAt = Date.now() + options.pageDeadlineMs;
  while (Date.now() < deadlineAt) {
    const nonce = randomUUID();
    try {
      const raw = await executeConsoleExchange(
        windowId,
        buildEtrakitCaptureExpression(nonce),
        nonce,
        options.consoleDeadlineMs,
      );
      const envelope = readBrowserCaptureEnvelope(raw);
      validateEtrakitBrowserContract(envelope.contract, expectedPage, options);
      return envelope;
    } catch {
      await wait(500);
    }
  }
  throw new Error("eTRAKiT page did not reconcile before its deadline");
}

/**
 * Wait for one exact page envelope from the already-running browser pump.
 *
 * @param {string} marker - Non-sensitive pre-pump clipboard marker.
 * @param {string} nonce - Pump lineage.
 * @param {number} expectedPage - Required next one-based page.
 * @param {number} deadlineMs - Finite local delivery deadline.
 * @returns {Promise<BrowserCaptureEnvelope>} Exact page envelope.
 */
async function waitForPumpedBrowserPage(
  marker,
  nonce,
  expectedPage,
  deadlineMs,
) {
  const deadlineAt = Date.now() + deadlineMs;
  while (Date.now() < deadlineAt) {
    const contents = await readClipboard();
    if (contents !== marker) {
      let parsed;
      try {
        parsed = /** @type {unknown} */ (JSON.parse(contents));
      } catch {
        /*
         * Clipboard ownership is global to the desktop. Ignore unrelated
         * non-JSON values without reading, logging, or persisting them.
         */
        await wait(100);
        continue;
      }
      if (
        parsed !== null &&
        typeof parsed === "object" &&
        !Array.isArray(parsed)
      ) {
        const candidate = /** @type {Record<string, unknown>} */ (parsed);
        if (candidate.nonce === nonce) {
          const envelope = readBrowserCaptureEnvelope(candidate);
          if (envelope.contract.currentPage > expectedPage) {
            throw new Error("eTRAKiT page pump advanced before checkpointing");
          }
          if (envelope.contract.currentPage === expectedPage) return envelope;
        }
      }
    }
    await wait(100);
  }
  throw new Error("eTRAKiT page pump delivery timed out");
}

/**
 * Execute the existing-session, list-only capture.
 *
 * @param {EtrakitCaptureOptions} options - Validated fixed capture options.
 * @returns {Promise<Readonly<{
 *   event:"broward_etrakit_capture_completed",
 *   status:"complete",
 *   reportedRecordCount:number,
 *   exposedRecordCap:number,
 *   pagedRecordCount:number,
 *   uniqueRecordCount:number,
 *   duplicateRecordCount:number,
 *   conflictRecordCount:0,
 *   detailRecordCount:0,
 *   completedPageCount:number,
 *   completenessBoundary:"bounded_capped_keyword_slice",
 *   captchaPrerequisite:"manual_authorization_reused",
 *   registryStatus:"captcha_required"
 * }>>} Aggregate-only capture summary.
 */
export async function runEtrakitSessionCapture(options) {
  const outputDirectory = resolve(options.outputDirectory);
  await mkdir(outputDirectory, { recursive: true, mode: 0o700 });
  const recordsPath = join(outputDirectory, "records.private.jsonl");
  const checkpointPath = join(outputDirectory, "checkpoint.private.json");
  const summaryPath = join(outputDirectory, "summary.private.json");
  const priorCheckpoint = validateEtrakitCheckpoint(
    await readOptionalJson(checkpointPath),
    options,
  );
  const records = await readExistingRecords(recordsPath);
  /** @type {Record<string, EtrakitPageReceipt>} */
  const completedPages = { ...(priorCheckpoint?.completedPages ?? {}) };
  let capturedRowCount = priorCheckpoint?.capturedRowCount ?? 0;
  let duplicateRecordCount = priorCheckpoint?.duplicateRecordCount ?? 0;
  if (priorCheckpoint?.completed === true) {
    throw new Error("eTRAKiT capture is already complete");
  }
  const windowId = await requireExistingChromeWindow();
  await ensureConsoleReady(windowId, options.consoleDeadlineMs);
  const completedPageNumbers = Object.keys(completedPages)
    .map(Number)
    .sort((left, right) => left - right);
  const firstPendingPage = (completedPageNumbers.at(-1) ?? 0) + 1;
  if (
    completedPageNumbers.some((page, index) => page !== index + 1) ||
    firstPendingPage > options.expectedPageCount
  ) {
    throw new Error("eTRAKiT completed pages are not a contiguous prefix");
  }

  const pumpNonce = randomUUID();
  const pumpMarker = `etrakit-capture-pump-pending:${pumpNonce}`;
  await setClipboardMarker(pumpMarker);
  await typeConsoleExpression(
    windowId,
    buildEtrakitPagePumpExpression(
      pumpNonce,
      firstPendingPage,
      options.expectedPageCount,
      options.delayMs,
      options.pageDeadlineMs,
    ),
  );
  for (
    let page = firstPendingPage;
    page <= options.expectedPageCount;
    page += 1
  ) {
    const envelope = await waitForPumpedBrowserPage(
      pumpMarker,
      pumpNonce,
      page,
      options.pageDeadlineMs + options.delayMs,
    );
    validateEtrakitBrowserContract(envelope.contract, page, options);
    const reconciled = reconcileEtrakitPage(
      records,
      envelope.records,
      page,
      options,
    );
    completedPages[String(page)] = reconciled.receipt;
    capturedRowCount += reconciled.receipt.rowCount;
    duplicateRecordCount += reconciled.duplicateCount;
    await writePrivateFile(
      recordsPath,
      renderEtrakitListJsonl(records.values()),
    );
    /** @type {EtrakitCaptureCheckpoint} */
    const checkpoint = {
      schemaVersion: CHECKPOINT_SCHEMA_VERSION,
      sourceSystem: SOURCE_SYSTEM,
      sourceReportedCount: options.sourceReportedCount,
      expectedPageCount: options.expectedPageCount,
      expectedPageSize: options.expectedPageSize,
      completedPages,
      capturedRowCount,
      uniqueRecordCount: records.size,
      duplicateRecordCount,
      conflictRecordCount: 0,
      completed: page === options.expectedPageCount,
      updatedAt: new Date().toISOString(),
    };
    await writePrivateFile(
      checkpointPath,
      `${JSON.stringify(checkpoint, null, 2)}\n`,
    );
  }

  const exposedRecordCap = options.expectedPageCount * options.expectedPageSize;
  if (
    capturedRowCount !== exposedRecordCap ||
    records.size + duplicateRecordCount !== capturedRowCount
  ) {
    throw new Error("eTRAKiT completed capture counts do not reconcile");
  }
  const summary = Object.freeze({
    event: /** @type {const} */ ("broward_etrakit_capture_completed"),
    status: /** @type {const} */ ("complete"),
    reportedRecordCount: options.sourceReportedCount,
    exposedRecordCap,
    pagedRecordCount: capturedRowCount,
    uniqueRecordCount: records.size,
    duplicateRecordCount,
    conflictRecordCount: /** @type {const} */ (0),
    detailRecordCount: /** @type {const} */ (0),
    completedPageCount: options.expectedPageCount,
    completenessBoundary: /** @type {const} */ ("bounded_capped_keyword_slice"),
    captchaPrerequisite: /** @type {const} */ ("manual_authorization_reused"),
    registryStatus: /** @type {const} */ ("captcha_required"),
  });
  await writePrivateFile(
    summaryPath,
    `${JSON.stringify(
      { schemaVersion: SUMMARY_SCHEMA_VERSION, ...summary },
      null,
      2,
    )}\n`,
  );
  return summary;
}

/**
 * Aggregate-only CLI entry point.
 *
 * @returns {Promise<void>} Resolves after all exposed pages are durable.
 */
export async function main() {
  const summary = await runEtrakitSessionCapture(
    parseEtrakitCaptureOptions(process.argv.slice(2)),
  );
  process.stdout.write(`${JSON.stringify(summary)}\n`);
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main().catch((error) => {
    process.stderr.write(
      `${JSON.stringify({
        event: "broward_etrakit_capture_failed",
        message: error instanceof Error ? error.message : "Unknown error",
      })}\n`,
    );
    process.exitCode = 1;
  });
}
