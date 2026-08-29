import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { ParquetReader } from "@dsnp/parquetjs";
import { afterEach, describe, expect, it } from "vitest";

import {
  BROWARD_BCS_ADAPTER_KEY,
  BROWARD_PERMIT_JURISDICTIONS,
  resolveBrowardPermitJurisdiction,
  sourcesForBrowardPermitJurisdiction,
} from "../../scripts/broward-permit-jurisdictions.mjs";
import {
  DONPHAN_PERMIT_QUERY_COLUMNS,
  deterministicPermitUuid,
  mapBrowardPermitToDonphanRow,
} from "../../scripts/broward-permit-query-artifact.mjs";
import {
  dedupeBrowardPermitPilotRecords,
  parseBrowardPermitPilotOptions,
  readBrowardPermitPilotFolios,
  runBrowardPermitPilot,
} from "../../scripts/run-broward-permit-pilot.mjs";
import {
  parseDonphanToolResult,
  parseDonphanValidationOptions,
} from "../../scripts/validate-broward-permits-with-donphan.mjs";

const temporaryDirectories = [];

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { force: true, recursive: true })),
  );
});

/**
 * Create an isolated temporary output root.
 *
 * @returns {Promise<string>} Registered temporary directory.
 */
async function createTemporaryDirectory() {
  const directory = await mkdtemp(
    join(tmpdir(), "broward-permit-orchestration-"),
  );
  temporaryDirectories.push(directory);
  return directory;
}

/**
 * Build one complete normalized BCS record accepted by the pilot/query mapper.
 *
 * @param {Partial<ReturnType<typeof permitRecord>>} [overrides] - Field overrides.
 * @returns {{
 * source_system:string,source_url:string,source_object_id:string,
 * source_record_kind:"master"|"permit",record_key:string,
 * parcel_identifier:string,permit_number:string,record_status:string,
 * record_type:string,permit_issue_date:string|null,application_date:string|null,
 * expiration_date:string|null,project_title:string|null,
 * project_description:string|null,job_value:number|null,
 * inspections:{completed_date:string|null}[]
 * }} Complete normalized record.
 */
function permitRecord(overrides = {}) {
  return {
    source_system: "broward_county_bcs_posse_permits",
    source_url:
      "https://dpepp.broward.org/BCS/Default.aspx?PossePresentation=ViewPermit&PosseObjectId=15703657",
    source_object_id: "15703657",
    source_record_kind: "permit",
    record_key: "broward_county_bcs_posse_permits:15703657",
    parcel_identifier: "494318013550",
    permit_number: "04-07545",
    record_status: "Complete",
    record_type: "CALT: BLDG-COMMERCIAL INT. PARTITIONS/ALTER",
    permit_issue_date: "2005-02-17",
    application_date: null,
    expiration_date: null,
    project_title: null,
    project_description: "INTERIOR PARTITIONS",
    job_value: null,
    inspections: [
      { completed_date: "2005-05-04" },
      { completed_date: "2005-05-03" },
    ],
    ...overrides,
  };
}

describe("Broward 32-jurisdiction permit registry", () => {
  it("registers every jurisdiction without treating BCS as countywide", () => {
    expect(BROWARD_PERMIT_JURISDICTIONS).toHaveLength(32);
    expect(
      new Set(BROWARD_PERMIT_JURISDICTIONS.map((entry) => entry.key)).size,
    ).toBe(32);

    const currentBcs = BROWARD_PERMIT_JURISDICTIONS.filter(
      (entry) =>
        entry.primarySource.adapterKey === BROWARD_BCS_ADAPTER_KEY &&
        entry.primarySource.status === "implemented",
    );
    expect(currentBcs.map((entry) => entry.key).sort()).toEqual([
      "lazy-lake",
      "unincorporated-broward",
    ]);

    const lbts = BROWARD_PERMIT_JURISDICTIONS.find(
      (entry) => entry.key === "lauderdale-by-the-sea",
    );
    expect(lbts?.primarySource).toMatchObject({
      adapterKey: "citizenserve-cap",
      status: "adapter_unavailable",
      coverageKind: "current",
    });
    expect(lbts?.supplementalSources).toEqual([
      expect.objectContaining({
        adapterKey: BROWARD_BCS_ADAPTER_KEY,
        status: "implemented",
        coverageKind: "historical",
      }),
    ]);
    expect(
      sourcesForBrowardPermitJurisdiction(
        /** @type {NonNullable<typeof lbts>} */ (lbts),
      ),
    ).toHaveLength(2);
  });

  it("derives routes from BCPA situs city/address and never guesses BCS", () => {
    expect(
      resolveBrowardPermitJurisdiction({
        situsCity: "UNINCORPORATED",
        situsAddress1: "NW 81 STREET",
      }),
    ).toMatchObject({
      method: "situs_city",
      jurisdiction: { key: "unincorporated-broward" },
    });
    expect(
      resolveBrowardPermitJurisdiction({
        situsCity: "",
        situsAddress:
          "218 E COMMERCIAL BOULEVARD, LAUDERDALE-BY-THE-SEA, FL 33308",
      }),
    ).toMatchObject({
      method: "situs_address",
      jurisdiction: { key: "lauderdale-by-the-sea" },
    });
    expect(
      resolveBrowardPermitJurisdiction({
        situsCity: "",
        situsAddress: "100 WESTON ROAD, FL 33301",
      }),
    ).toMatchObject({
      method: "unresolved",
      jurisdiction: null,
    });
  });

  it("labels login and CAPTCHA sources as no-request outcomes", () => {
    expect(
      BROWARD_PERMIT_JURISDICTIONS.find(
        (entry) => entry.key === "coral-springs",
      )?.primarySource.status,
    ).toBe("captcha_required");
    expect(
      BROWARD_PERMIT_JURISDICTIONS.find(
        (entry) => entry.key === "north-lauderdale",
      )?.primarySource.status,
    ).toBe("login_required");
  });
});

describe("Broward permit Donphan normalization", () => {
  it("emits the exact 20-column permit shape without inventing completion", () => {
    const row = mapBrowardPermitToDonphanRow(permitRecord());
    expect(Object.keys(row)).toEqual(DONPHAN_PERMIT_QUERY_COLUMNS);
    expect(row).toMatchObject({
      property_id: "broward:494318013550",
      parcel_identifier: "494318013550",
      permit_number: "04-07545",
      improvement_action: "permit_record",
      permit_issue_date: "2005-02-17",
      final_inspection_date: "2005-05-04",
      completion_date: null,
      source_system: "broward_county_bcs_posse_permits",
      county_name: "Broward",
    });
    expect(row.property_improvement_id).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u,
    );
    expect(deterministicPermitUuid("stable")).toBe(
      deterministicPermitUuid("stable"),
    );
  });

  it("deduplicates exact records and preserves conflicting source identities", () => {
    const record = permitRecord();
    const reconciled = dedupeBrowardPermitPilotRecords([
      record,
      record,
      permitRecord({ record_status: "Cancelled" }),
    ]);
    expect(reconciled.records).toHaveLength(1);
    expect(reconciled.duplicateCount).toBe(1);
    expect(reconciled.conflicts).toEqual([
      {
        recordKey: record.record_key,
        error:
          "Same source record key produced conflicting normalized payloads",
      },
    ]);
  });
});

describe("checkpointed local Broward permit pilot", () => {
  it("reconciles records, explicit empties, unavailable routes, and resume", async () => {
    const root = await createTemporaryDirectory();
    const outputDirectory = join(root, "output");
    const checkpointPath = join(outputDirectory, "checkpoint.json");
    const appraisalCalls = [];
    const adapterCalls = [];
    const appraisalRecords = {
      "474134000012": {
        folioNumber: "474134000012",
        situsCity: "UNINCORPORATED",
        situsAddress1: "NW 81 STREET",
        situsZipCode: "33076",
        useCode: "52 - Cropland soil capability class II",
      },
      "494318013550": {
        folioNumber: "494318013550",
        situsCity: "LAUDERDALE BY THE SEA",
        situsAddress1: "218 E COMMERCIAL BOULEVARD",
        situsZipCode: "33308",
        useCode: "12-02 Mixed store and office",
      },
      "484109030410": {
        folioNumber: "484109030410",
        situsCity: "CORAL SPRINGS",
        situsAddress1: "100 TEST STREET",
        situsZipCode: "33065",
        useCode: "01-01 Single Family",
      },
    };
    const options = {
      folios: ["474134000012", "494318013550", "484109030410"],
      outputDirectory,
      checkpointPath,
      maxAdapterAttempts: 5,
      appraisalDelayMs: 250,
      permitDelayMs: 1_000,
      appraisalTimeoutMs: 30_000,
      fetchAppraisalRecord: async (folio) => {
        appraisalCalls.push(folio);
        return appraisalRecords[folio];
      },
      adapterRunners: {
        [BROWARD_BCS_ADAPTER_KEY]: async (folio) => {
          adapterCalls.push(folio);
          return folio === "474134000012"
            ? {
                status: "no_permits",
                records: [],
                observation: { parcelIdentifier: folio, status: "no_permits" },
              }
            : {
                status: "records",
                records: [permitRecord()],
                observation: {
                  parcelIdentifier: folio,
                  status: "records",
                  normalizedRecordCount: 1,
                },
              };
        },
      },
      sleep: async () => undefined,
    };

    const first = await runBrowardPermitPilot(options);
    expect(first.counters).toEqual({
      sampleParcels: 3,
      appraisalAttempts: 3,
      appraisalResolved: 3,
      jurisdictionResolved: 3,
      jurisdictionUnresolved: 0,
      sourceOutcomes: 4,
      sourceUnavailableOutcomes: 2,
      permitSourceAttempts: 2,
      permitAttemptedParcels: 2,
      explicitNoPermitOutcomes: 1,
      sourceFailures: 0,
      rawPermitRecords: 1,
      duplicatePermitRecords: 0,
      conflictingPermitRecords: 0,
      uniquePermitRecords: 1,
      queryRows: 1,
    });
    expect(first.reconciliation).toMatchObject({
      allInputParcelsTerminal: true,
      allRecordsAccountedFor: true,
      queryRowsMatchUniqueRecords: true,
      allJurisdictionsRegistered: true,
      currentSourceJurisdictionsImplemented: 2,
      currentSourceJurisdictionsBlocked: 30,
    });
    expect(first.acceptance).toEqual({
      localPilotPassed: true,
      countyPermitAcceptancePassed: false,
      reason:
        "Appraisal acceptance and a bounded permit pilot do not establish full permit acceptance while current municipal sources remain unavailable",
    });
    expect(adapterCalls).toEqual(["474134000012", "494318013550"]);

    const reader = await ParquetReader.openFile(first.artifacts.parquet);
    try {
      const cursor = reader.getCursor();
      expect(await cursor.next()).toMatchObject({
        parcel_identifier: "494318013550",
        permit_number: "04-07545",
      });
      expect(await cursor.next()).toBeNull();
    } finally {
      await reader.close();
    }
    const coverage = JSON.parse(
      await readFile(first.artifacts.coverage, "utf8"),
    );
    expect(coverage.registry).toHaveLength(32);
    expect(coverage.parquet).toMatchObject({ rowCount: 1 });

    const resumed = await runBrowardPermitPilot(options);
    expect(resumed.counters).toEqual(first.counters);
    expect(appraisalCalls).toHaveLength(3);
    expect(adapterCalls).toHaveLength(2);
  });

  it("parses validated CSV/manifest inputs and enforces CLI bounds", async () => {
    const root = await createTemporaryDirectory();
    const csvPath = join(root, "sample.csv");
    const manifestPath = join(root, "sample.json");
    await writeFile(
      csvPath,
      "parcel_id,parcel_polygon\n474134000012,\"{\"\"type\"\":\"\"Polygon\"\"}\"\n494318013550,\"{}\"\n",
      "utf8",
    );
    await writeFile(
      manifestPath,
      JSON.stringify({
        parcels: [{ folio: "474134000012" }, { folio: "504108BJ0140" }],
      }),
      "utf8",
    );
    expect(await readBrowardPermitPilotFolios(csvPath)).toEqual([
      "474134000012",
      "494318013550",
    ]);
    expect(await readBrowardPermitPilotFolios(manifestPath)).toEqual([
      "474134000012",
      "504108BJ0140",
    ]);
    expect(parseBrowardPermitPilotOptions(["--pilot"])).toMatchObject({
      inputMode: "pilot",
      maxAdapterAttempts: 5,
      appraisalDelayMs: 300,
      permitDelayMs: 1_000,
    });
    expect(() =>
      parseBrowardPermitPilotOptions([
        "--pilot",
        "--permit-delay-ms",
        "999",
      ]),
    ).toThrow("at least 1000");
    expect(() =>
      parseBrowardPermitPilotOptions([
        "--pilot",
        "--max-adapter-attempts",
        "6",
      ]),
    ).toThrow("from 1 through 5");
  });
});

describe("actual Donphan validation runner contracts", () => {
  it("parses handler content and rejects embedded tool errors", () => {
    expect(
      parseDonphanToolResult(
        { content: [{ type: "text", text: '{"rows":[{"c":1}]}' }] },
        "queryPermits",
      ),
    ).toEqual({ rows: [{ c: 1 }] });
    expect(() =>
      parseDonphanToolResult(
        {
          content: [
            {
              type: "text",
              text: '{"error":"Failed","details":"missing parquet"}',
            },
          ],
        },
        "queryPermits",
      ),
    ).toThrow("missing parquet");
  });

  it("requires explicit local Parquet, Donphan module, and evidence paths", () => {
    expect(
      parseDonphanValidationOptions([
        "--parquet",
        "./permit.parquet",
        "--module",
        "../elephant-mcp/src/tools/permitQuery.ts",
        "--output",
        "./evidence.json",
      ]),
    ).toMatchObject({ county: "Broward" });
    expect(() => parseDonphanValidationOptions(["--parquet", "x"])).toThrow(
      "required",
    );
  });
});
