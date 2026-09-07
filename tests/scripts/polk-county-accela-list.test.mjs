import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  buildPolkAccelaWindows,
  parsePolkAccelaCsv,
  parsePolkAccelaListOptions,
  runPolkAccelaList,
} from "../../scripts/polk/polk-county-accela-list.mjs";

/** @type {string[]} */
const temporaryDirectories = [];

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { recursive: true, force: true })),
  );
});

const INITIAL_HTML = `<!doctype html>
  <form id="aspnetForm">
    <input type="hidden" name="__VIEWSTATE" value="initial">
    <input name="ctl00$PlaceHolderMain$generalSearchForm$txtGSStartDate" value="">
    <input name="ctl00$PlaceHolderMain$generalSearchForm$txtGSEndDate" value="">
    <a id="ctl00_PlaceHolderMain_btnNewSearch">Search</a>
  </form>`;

const RESULT_HTML = `<!doctype html>
  <form id="aspnetForm">
    <input type="hidden" name="__VIEWSTATE" value="result">
    <a id="ctl00_PlaceHolderMain_dgvPermitList_gdvPermitList_gdvPermitListtop4btnExport">Download results</a>
    <div>Showing 1-2 of 100+</div>
  </form>`;

const NO_RESULTS_HTML = `<!doctype html>
  <form id="aspnetForm"><input type="hidden" name="__VIEWSTATE" value="none"></form>
  <div>Your search returned no results.</div>`;

/**
 * Build a representative Accela CSV response.
 *
 * @param {string} date Accela date.
 * @returns {string} CSV.
 */
function csvForDate(date) {
  return [
    '"Record Number","Record Type","Address","Status","Date","Project Name","Description","Expiration Date","Short Notes",',
    `"BR-2026-${date.replaceAll("/", "")}","Building Permit","1 MAIN ST","Issued","${date}","","TEST WORK","","",`,
    `"LIC-H-${date.replaceAll("/", "")}","Contractor License","","Active","${date}","","","","",`,
    "",
  ].join("\r\n");
}

/**
 * Create an isolated-session Accela endpoint.
 *
 * @param {ReadonlySet<string>} emptyStartDates MM/DD/YYYY starts returning no results.
 * @returns {typeof fetch} Fetch implementation.
 */
function createAccelaFetch(emptyStartDates = new Set()) {
  let activeDate = "01/01/2026";
  return /** @type {typeof fetch} */ (
    async (input, init = {}) => {
      const url = String(input);
      if (url.includes("Export2CSV.ashx")) {
        return new Response(csvForDate(activeDate), {
          status: 200,
          headers: { "content-type": "text/csv; charset=UTF-8" },
        });
      }
      if ((init.method ?? "GET") === "GET") {
        return new Response(INITIAL_HTML, {
          status: 200,
          headers: { "set-cookie": "ACA_SS_STORE=test; Path=/" },
        });
      }
      const body =
        init.body instanceof URLSearchParams
          ? init.body
          : new URLSearchParams(String(init.body ?? ""));
      const target = body.get("__EVENTTARGET");
      if (target === "ctl00$PlaceHolderMain$btnNewSearch") {
        activeDate =
          body.get("ctl00$PlaceHolderMain$generalSearchForm$txtGSStartDate") ??
          activeDate;
        return new Response(
          emptyStartDates.has(activeDate) ? NO_RESULTS_HTML : RESULT_HTML,
          { status: 200 },
        );
      }
      return new Response(RESULT_HTML, { status: 200 });
    }
  );
}

describe("Polk County Accela CSV list harvest", () => {
  it("builds contiguous calendar windows with a partial final window", () => {
    expect(buildPolkAccelaWindows("2025-11-15", "2026-03-10", 2)).toEqual([
      { startDate: "2025-11-15", endDate: "2025-12-31" },
      { startDate: "2026-01-01", endDate: "2026-02-28" },
      { startDate: "2026-03-01", endDate: "2026-03-10" },
    ]);
  });

  it("parses permit and license rows without inferring property links", () => {
    const records = parsePolkAccelaCsv(
      csvForDate("09/01/2026"),
      { startDate: "2026-09-01", endDate: "2026-09-01" },
      "2026-09-03T00:00:00.000Z",
    );

    expect(records).toHaveLength(2);
    expect(records[0]).toMatchObject({
      permitNumber: "BR-2026-09012026",
      recordClass: "permit",
      sourceDate: "2026-09-01",
      parcelIdentifier: null,
      propertyMatch: null,
    });
    expect(records[1]).toMatchObject({
      recordClass: "license",
      parcelIdentifier: null,
    });
  });

  it("fails closed when the CSV schema changes", () => {
    expect(() =>
      parsePolkAccelaCsv(
        '"Changed","Record Type"\n"x","y"\n',
        { startDate: "2026-09-01", endDate: "2026-09-01" },
        "2026-09-03T00:00:00.000Z",
      ),
    ).toThrow(/schema changed/);
  });

  it("coalesces exact source duplicates but rejects conflicting identities", () => {
    const csv = csvForDate("09/01/2026");
    const duplicateRow = csv.split(/\r?\n/)[1];
    const exactDuplicate = `${csv}${duplicateRow}\r\n`;
    expect(
      parsePolkAccelaCsv(
        exactDuplicate,
        { startDate: "2026-09-01", endDate: "2026-09-01" },
        "2026-09-03T00:00:00.000Z",
      ),
    ).toHaveLength(2);

    const conflictingDuplicate = `${csv}"BR-2026-09012026","Building Permit","2 OTHER ST","Issued","09/01/2026","","OTHER WORK","","",\r\n`;
    expect(() =>
      parsePolkAccelaCsv(
        conflictingDuplicate,
        { startDate: "2026-09-01", endDate: "2026-09-01" },
        "2026-09-03T00:00:00.000Z",
      ),
    ).toThrow(/conflicting duplicate/);
  });

  it("requires approval for more than three source windows", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "polk-accela-scale-"));
    temporaryDirectories.push(directory);
    const options = parsePolkAccelaListOptions([
      "--stage",
      "harvest",
      "--start-date",
      "2026-01-01",
      "--end-date",
      "2026-04-30",
      "--output",
      path.join(directory, "records.jsonl"),
    ]);

    await expect(
      runPolkAccelaList(options, createAccelaFetch()),
    ).rejects.toThrow(/--approve-scale/);
  });

  it("aborts a stalled source session at the whole-window deadline", async () => {
    const directory = await mkdtemp(
      path.join(tmpdir(), "polk-accela-timeout-"),
    );
    temporaryDirectories.push(directory);
    const options = parsePolkAccelaListOptions([
      "--stage",
      "harvest",
      "--start-date",
      "2026-01-01",
      "--end-date",
      "2026-01-31",
      "--output",
      path.join(directory, "records.jsonl"),
      "--window-timeout-ms",
      "5",
      "--attempts",
      "1",
    ]);
    const stalledFetch = /** @type {typeof fetch} */ (
      async (_input, init = {}) =>
        await new Promise((resolve, reject) => {
          const signal = init.signal;
          if (signal === null || signal === undefined) {
            reject(new Error("Expected a request abort signal"));
            return;
          }
          if (signal.aborted) {
            reject(signal.reason);
            return;
          }
          signal.addEventListener("abort", () => reject(signal.reason), {
            once: true,
          });
        })
    );

    await expect(runPolkAccelaList(options, stalledFetch)).rejects.toThrow(
      /window attempt timed out after 5ms/,
    );
  });

  it("harvests empty and populated windows, resumes, and verifies", async () => {
    const directory = await mkdtemp(
      path.join(tmpdir(), "polk-accela-harvest-"),
    );
    temporaryDirectories.push(directory);
    const output = path.join(directory, "records.jsonl");
    const options = parsePolkAccelaListOptions([
      "--stage",
      "harvest",
      "--start-date",
      "2026-01-01",
      "--end-date",
      "2026-02-28",
      "--output",
      output,
      "--delay-ms",
      "1",
    ]);
    const fetchImpl = createAccelaFetch(new Set(["02/01/2026"]));

    const first = await runPolkAccelaList(options, fetchImpl);
    const resumed = await runPolkAccelaList(options, fetchImpl);
    const lines = (await readFile(output, "utf8"))
      .trim()
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line));

    expect(first).toMatchObject({
      requestedWindowCount: 2,
      completedWindowCount: 2,
      sourceRecordCount: 2,
      exactDuplicateRowCount: 0,
      accessibleRecordCount: 2,
      uniqueRecordCount: 2,
      classCounts: { permit: 1, license: 1, other: 0 },
      complete: true,
      historicalCoverageComplete: false,
    });
    expect(resumed.outputSha256).toBe(first.outputSha256);
    expect(lines.every((line) => line.propertyMatch === null)).toBe(true);

    const verification = await runPolkAccelaList(
      { ...options, stage: "verify" },
      fetchImpl,
    );
    expect(verification).toMatchObject({
      verifiedWindowCount: 2,
      verifiedRecordCount: 2,
      uniqueRecordCount: 2,
      complete: true,
    });
  });
});
