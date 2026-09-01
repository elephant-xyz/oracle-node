import { describe, expect, it } from "vitest";

import {
  buildEtrakitCaptureExpression,
  buildEtrakitPageExpression,
  buildEtrakitPagePumpExpression,
  normalizeEtrakitListRecord,
  parseEtrakitCaptureOptions,
  reconcileEtrakitPage,
  renderEtrakitListJsonl,
  validateEtrakitBrowserContract,
  validateEtrakitCheckpoint,
} from "../../scripts/run-broward-etrakit-session-capture.mjs";

const options = Object.freeze({
  outputDirectory: "downloads/private-coral",
  sourceReportedCount: 59_379,
  expectedPageCount: 50,
  expectedPageSize: 20,
  delayMs: 6_000,
  consoleDeadlineMs: 8_000,
  pageDeadlineMs: 30_000,
});

/**
 * Build one synthetic privacy-minimized browser row.
 *
 * @param {number} index - Stable fixture suffix.
 * @param {Record<string, unknown>} [overrides] - Field overrides.
 * @returns {import("../../scripts/run-broward-etrakit-session-capture.mjs").BrowserListRow}
 *   Synthetic source row.
 */
function browserRow(index, overrides = {}) {
  return /** @type {import("../../scripts/run-broward-etrakit-session-capture.mjs").BrowserListRow} */ ({
    sourceRecordId: `TEST:${String(index).padStart(19, "0")}`,
    permitNumber: `TEST-${String(index).padStart(6, "0")}`,
    recordType: "RESIDENTIAL REROOF",
    status: "Issued",
    address: "SYNTHETIC LOCATION",
    folio: "000000000001",
    ...overrides,
  });
}

/**
 * Build one fully shaped browser contract.
 *
 * @param {Record<string, unknown>} [overrides] - Field overrides.
 * @returns {import("../../scripts/run-broward-etrakit-session-capture.mjs").BrowserCaptureEnvelope["contract"]}
 *   Synthetic source contract.
 */
function browserContract(overrides = {}) {
  return /** @type {import("../../scripts/run-broward-etrakit-session-capture.mjs").BrowserCaptureEnvelope["contract"]} */ ({
    title: "eTRAKiT",
    origin: "https://etrakit.coralsprings.gov",
    path: "/eTRAKiT/Search/permit.aspx",
    searchBy: "Permit_Main.PERMITTYPE",
    searchOperator: "CONTAINS",
    searchValue: "ROOF",
    pageCount: 50,
    pageSize: 20,
    currentPage: 1,
    rowCount: 20,
    formMethod: "post",
    formActionPath: "/eTRAKiT/Search/permit.aspx",
    viewStatePresent: true,
    eventValidationPresent: false,
    postBackTarget: "ctl00$cplMain$rgSearchRslts",
    postBackArgumentPrefix:
      "FireCommand:ctl00$cplMain$rgSearchRslts$ctl00;Page;",
    pagerText:
      "PERMIT # Permit Type STATUS SITE ADDRESS OWNER CONTRACTOR RECORDID FOLIO Buttons to move Next/Previous page 1 of 50",
    ...overrides,
  });
}

describe("Coral Springs existing-session eTRAKiT capture", () => {
  it("parses conservative finite limits and requires a reported denominator", () => {
    expect(
      parseEtrakitCaptureOptions([
        "--output-dir",
        "downloads/private-coral",
        "--reported-count",
        "59379",
        "--delay-ms",
        "6000",
      ]),
    ).toEqual(options);
    expect(() =>
      parseEtrakitCaptureOptions([
        "--output-dir",
        "downloads/private-coral",
      ]),
    ).toThrow("--reported-count");
    expect(() =>
      parseEtrakitCaptureOptions([
        "--output-dir",
        "downloads/private-coral",
        "--reported-count",
        "59379",
        "--delay-ms",
        "1000",
      ]),
    ).toThrow("--delay-ms");
  });

  it("proves the exact ViewState and Telerik postback contract", () => {
    expect(() =>
      validateEtrakitBrowserContract(browserContract(), 1, options),
    ).not.toThrow();
    expect(() =>
      validateEtrakitBrowserContract(
        browserContract({ eventValidationPresent: true }),
        1,
        options,
      ),
    ).toThrow("EventValidation presence changed");
    expect(() =>
      validateEtrakitBrowserContract(
        browserContract({ pagerText: "page 2 of 50" }),
        1,
        options,
      ),
    ).toThrow("approved slice");
  });

  it("normalizes only list inventory and preserves the capped boundary", () => {
    const record = normalizeEtrakitListRecord(browserRow(1), 1, options);
    expect(record).toMatchObject({
      schemaVersion: "oracle-node.broward-etrakit-list.v1",
      sourceSystem: "broward_coral_springs_etrakit_permits",
      jurisdiction: "Coral Springs",
      sourceRecordId: "TEST:0000000000000000001",
      recordKey:
        "broward_coral_springs_etrakit_permits:record:TEST:0000000000000000001",
      sourcePages: [1],
      isRoofPermit: true,
      coverage: {
        sourceReportedCount: 59_379,
        exposedRecordCap: 1_000,
        completenessBoundary: "bounded_capped_keyword_slice",
      },
    });
    expect(JSON.stringify(record)).not.toMatch(
      /captcha|cookie|owner|contractor|contact/iu,
    );
    expect(() =>
      normalizeEtrakitListRecord(
        browserRow(1, { recordType: "PLUMBING" }),
        1,
        options,
      ),
    ).toThrow("roofing query");
  });

  it("reconciles stable identities, exact duplicates, and conflicts", () => {
    const records = new Map();
    const firstPage = Array.from({ length: 20 }, (_value, index) =>
      browserRow(index + 1),
    );
    const first = reconcileEtrakitPage(records, firstPage, 1, options);
    expect(first).toMatchObject({
      receipt: { page: 1, rowCount: 20 },
      duplicateCount: 0,
    });
    expect(records).toHaveLength(20);
    const duplicate = reconcileEtrakitPage(records, firstPage, 2, options);
    expect(duplicate.duplicateCount).toBe(20);
    expect(records.get(firstPage[0]?.sourceRecordId ?? "")?.sourcePages).toBe(
      undefined,
    );
    expect(
      [...records.values()].every(
        (record) =>
          record.sourcePages.length === 2 &&
          record.sourcePages[0] === 1 &&
          record.sourcePages[1] === 2,
      ),
    ).toBe(true);
    expect(() =>
      reconcileEtrakitPage(
        records,
        firstPage.map((row, index) =>
          index === 0 ? { ...row, status: "Different" } : row,
        ),
        3,
        options,
      ),
    ).toThrow("conflicting list facts");
    expect(renderEtrakitListJsonl(records.values()).split("\n")).toHaveLength(
      21,
    );
  });

  it("generates a one-injection pump without browser-launch or session access", () => {
    const capture = buildEtrakitCaptureExpression("safe-nonce");
    const page = buildEtrakitPageExpression("safe-nonce", 2);
    const pump = buildEtrakitPagePumpExpression(
      "safe-nonce",
      1,
      50,
      6_000,
      30_000,
    );
    expect(capture).toContain('input[name="__VIEWSTATE"]');
    expect(page).toContain("btnPageNext");
    expect(pump).toContain("const publish=copy");
    expect(pump).toContain("btnPageNext");
    expect(`${capture}${page}${pump}`).not.toMatch(
      /cookie|g-recaptcha-response|localStorage|sessionStorage|puppeteer|playwright|launch/iu,
    );
  });

  it("accepts only the immutable aggregate checkpoint lineage", () => {
    const checkpoint = {
      schemaVersion: "oracle-node.broward-etrakit-capture-checkpoint.v1",
      sourceSystem: "broward_coral_springs_etrakit_permits",
      sourceReportedCount: 59_379,
      expectedPageCount: 50,
      expectedPageSize: 20,
      completedPages: {},
      capturedRowCount: 0,
      uniqueRecordCount: 0,
      duplicateRecordCount: 0,
      conflictRecordCount: 0,
      completed: false,
      updatedAt: "2026-09-01T00:00:00.000Z",
    };
    expect(validateEtrakitCheckpoint(checkpoint, options)).toEqual(checkpoint);
    expect(() =>
      validateEtrakitCheckpoint(
        { ...checkpoint, sourceReportedCount: 1_000 },
        options,
      ),
    ).toThrow("lineage");
  });
});
