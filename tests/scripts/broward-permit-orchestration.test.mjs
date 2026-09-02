import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { ParquetReader } from "@dsnp/parquetjs";
import { afterEach, describe, expect, it } from "vitest";

import {
  BROWARD_ACCELA_ADAPTER_KEY,
  BROWARD_BCS_ADAPTER_KEY,
  BROWARD_CITIZENSERVE_ADAPTER_KEY,
  BROWARD_PERMIT_JURISDICTIONS,
  BROWARD_TYLER_CIVIC_ACCESS_ADAPTER_KEY,
  resolveBrowardPermitJurisdiction,
  sourcesForBrowardPermitJurisdiction,
} from "../../scripts/broward-permit-jurisdictions.mjs";
import {
  DONPHAN_PERMIT_QUERY_COLUMNS,
  deterministicPermitUuid,
  mapBrowardPermitToDonphanRow,
} from "../../scripts/broward-permit-query-artifact.mjs";
import {
  buildAccelaPermitUpsertValues,
  buildMunicipalPermitUpsertValues,
  buildPermitUpsertValues,
  parsePermitLoadOptions,
  readNormalizedAccelaPermitRecords,
  readNormalizedMunicipalPermitRecords,
  readNormalizedPermitRecords,
} from "../../scripts/load-broward-permit-pilot-to-neon.mjs";
import {
  dedupeBrowardPermitPilotRecords,
  parseBrowardPermitPilotOptions,
  readBrowardPermitPilotFolios,
  recordBrowardPermitPilotStatus,
  runBrowardPermitPilot,
  verifyBrowardPermitStatusTarget,
} from "../../scripts/run-broward-permit-pilot.mjs";
import {
  failureCooldownDelayMs,
  normalizeMigratedPermitItem,
  parseSupportedPermitOptions,
  processByRouteWithConcurrency,
  readBcsSummaryRecordCount,
  readJurisdictionKeys,
  runNode,
  supportedPermitClientConfig,
} from "../../scripts/run-broward-supported-permit-ingest.mjs";
import {
  parseDonphanToolResult,
  parseDonphanValidationOptions,
} from "../../scripts/validate-broward-permits-with-donphan.mjs";
import { BROWARD_ACCELA_SOURCES } from "../../scripts/permit-source-adapters/broward-accela.mjs";
import { BROWARD_MUNICIPAL_PERMIT_JURISDICTIONS } from "../../scripts/permit-source-adapters/broward-municipal-config.mjs";
import { BROWARD_PERMIT_JURISDICTIONS as BROWARD_TYLER_CITIZENSERVE_JURISDICTIONS } from "../../scripts/permit-source-adapters/broward-permit-jurisdictions.mjs";

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

/**
 * Build the complete BCS shape consumed by the Neon permit loader.
 *
 * @returns {Record<string, unknown>} Complete normalized public permit record.
 */
function permitLoadRecord() {
  return {
    ...permitRecord(),
    source_search_url: "https://dpepp.broward.org/BCS/Default.aspx",
    source_list_url:
      "https://dpepp.broward.org/BCS/Default.aspx?PossePresentation=ParcelPermitList",
    source_folio_number: "0123456",
    issuing_jurisdiction: "LAUDERDALE-BY-THE-SEA",
    work_location: "218 E COMMERCIAL BLVD",
    legal_description: "PUBLIC LEGAL DESCRIPTION",
    contractor_name: "PUBLIC CONTRACTOR",
    contractor_license: "PUBLIC-LICENSE",
    building_use: "COMMERCIAL",
    present_use: "OFFICE",
    proposed_use: "OFFICE",
    square_footage: 1_250,
    occupancy_type: "BUSINESS",
    construction_type: "TYPE II",
    occupant_load: 10,
    finish_floor_above_road: 1.5,
    finish_floor_above_sea_level: 8.2,
    is_roof_permit: false,
    inspections: [
      {
        source_url: "https://dpepp.broward.org/BCS/inspection/1",
        source_object_id: "1",
        inspection_type: "BUILDING FINAL",
        requested_date: "2005-05-03",
        result: "Passed",
        completed_date: "2005-05-04",
      },
    ],
    raw: {
      search_method: "ParcelID",
      reference_number: "REF-1",
      list_contractor: "PUBLIC CONTRACTOR",
      detail_page_title: "Permit",
    },
  };
}

describe("Broward permit Neon pilot loader", () => {
  it("builds exact parent-linked permit values and latest inspection evidence", () => {
    const values = buildPermitUpsertValues(
      /** @type {Parameters<typeof buildPermitUpsertValues>[0]} */ (
        permitLoadRecord()
      ),
      {
        propertyId: "11111111-1111-4111-8111-111111111111",
        parcelId: "22222222-2222-4222-8222-222222222222",
      },
    );
    expect(values).toMatchObject({
      propertyId: "11111111-1111-4111-8111-111111111111",
      parcelId: "22222222-2222-4222-8222-222222222222",
      parcelIdentifier: "494318013550",
      permitNumber: "04-07545",
      improvementAction: "permit_record",
      finalInspectionDate: "2005-05-04",
      estimatedSqFt: 1_250,
    });
    expect(values.moreDetails).toMatchObject({
      issuing_jurisdiction: "LAUDERDALE-BY-THE-SEA",
      contractor_license: "PUBLIC-LICENSE",
      source_object_id: "15703657",
    });
    expect(values.sourceRecordHash).toMatch(/^[a-f0-9]{64}$/u);
  });

  it("requires a unique reconciled input and an exact optional count", async () => {
    expect(
      parsePermitLoadOptions([
        "--input",
        "pilot.jsonl",
        "--expected-records",
        "73",
        "--accela-input",
        "accela.jsonl",
        "--expected-accela-records",
        "14",
        "--municipal-input",
        "tyler.jsonl",
        "--municipal-input",
        "citizenserve.jsonl",
        "--expected-municipal-records",
        "17",
      ]),
    ).toEqual({
      inputPath: "pilot.jsonl",
      expectedRecords: 73,
      includeBcs: true,
      accelaInputPath: "accela.jsonl",
      expectedAccelaRecords: 14,
      municipalInputPaths: ["tyler.jsonl", "citizenserve.jsonl"],
      expectedMunicipalRecords: 17,
    });
    const directory = await createTemporaryDirectory();
    const inputPath = join(directory, "permits.private.jsonl");
    const record = permitLoadRecord();
    await writeFile(
      inputPath,
      `${JSON.stringify(record)}\n${JSON.stringify(record)}\n`,
    );
    await expect(readNormalizedPermitRecords(inputPath)).rejects.toThrow(
      /duplicate key/u,
    );
  });

  it("maps bounded Accela records without inventing dates", async () => {
    const record = {
      schemaVersion: "permit-harvest.accela.v1",
      source: "Accela",
      sourceSystem: "broward_plantation_accela_permits",
      jurisdiction: "Plantation",
      retrievedAt: "2026-08-31T06:00:00Z",
      sourceUrl: "https://aca.plantation.org/permit/1",
      recordNumber: "B22-03630",
      recordType: "Building",
      recordStatus: "Closed",
      workLocation: "100 TEST AVE",
      parcelIdentifier: "504108BJ0140",
      sourceParcelIdentifier: "504108BJ0140",
      applicant: null,
      licensedProfessional: null,
      projectDescription: "PUBLIC PROJECT",
      moreDetails: { Type: "Building" },
      moreDetailsRawText: null,
      inspectionsRawText: null,
      completedInspections: [],
      processingStatusRawText: null,
      documentLinks: [],
      relatedLinks: [],
      rawText: "",
      sourceSearchResult: { recordNumber: "B22-03630" },
      idempotencyKey: "broward_plantation_accela_permits:B22-03630",
      provenance: { searchMethod: "parcel" },
    };
    const directory = await createTemporaryDirectory();
    const inputPath = join(directory, "accela.private.jsonl");
    await writeFile(inputPath, `${JSON.stringify(record)}\n`);
    await expect(
      readNormalizedAccelaPermitRecords(inputPath),
    ).resolves.toMatchObject({
      records: [expect.objectContaining({ recordNumber: "B22-03630" })],
      sourceSha256: expect.stringMatching(/^[a-f0-9]{64}$/u),
    });
    const values = buildAccelaPermitUpsertValues(
      /** @type {Parameters<typeof buildAccelaPermitUpsertValues>[0]} */ (
        record
      ),
      {
        propertyId: "11111111-1111-4111-8111-111111111111",
        parcelId: "22222222-2222-4222-8222-222222222222",
      },
    );
    expect(values).toMatchObject({
      sourceSystem: "broward_plantation_accela_permits",
      permitNumber: "B22-03630",
      parcelIdentifier: "504108BJ0140",
      improvementAction: "permit_record",
      permitIssueDate: null,
      applicationReceivedDate: null,
    });
  });

  it("reconciles and maps bounded Tyler/Citizenserve permit records", async () => {
    const record = {
      source_system: "broward_pembroke_pines_tyler_permits",
      source_vendor: "tyler_energov_civic_access",
      source_url: "https://example.test/permit/1",
      source_record_id: "1",
      record_key: "broward_pembroke_pines_tyler_permits:1",
      city: "Pembroke Pines",
      permit_number: "22-08581",
      parcel_identifier: "513914101320",
      work_location: "470 SW 198 TER",
      permit_issue_date: "2022-10-01",
      application_date: "2022-09-01",
      expiration_date: null,
      finalized_date: "2023-01-01",
      record_status: "Complete",
      record_type: "Building",
      work_class: "Alteration",
      project_description: "PUBLIC PROJECT",
      square_feet: 500,
      job_value: 25_000,
      is_roof_permit: false,
      provenance: { query_kind: "folio" },
      raw: { source: "public" },
    };
    const directory = await createTemporaryDirectory();
    const inputPath = join(directory, "municipal.private.jsonl");
    await writeFile(inputPath, `${JSON.stringify(record)}\n`);
    await expect(
      readNormalizedMunicipalPermitRecords([inputPath]),
    ).resolves.toMatchObject({
      records: [expect.objectContaining({ permit_number: "22-08581" })],
      sourceSha256: expect.stringMatching(/^[a-f0-9]{64}$/u),
    });
    const values = buildMunicipalPermitUpsertValues(
      /** @type {Parameters<typeof buildMunicipalPermitUpsertValues>[0]} */ (
        record
      ),
      {
        propertyId: "11111111-1111-4111-8111-111111111111",
        parcelId: "22222222-2222-4222-8222-222222222222",
      },
    );
    expect(values).toMatchObject({
      sourceSystem: "broward_pembroke_pines_tyler_permits",
      permitNumber: "22-08581",
      applicationReceivedDate: "2022-09-01",
      permitIssueDate: "2022-10-01",
      finalInspectionDate: null,
      estimatedJobValue: 25_000,
      estimatedSqFt: 500,
    });
    expect(values.moreDetails).toMatchObject({
      source_vendor: "tyler_energov_civic_access",
      finalized_date: "2023-01-01",
    });
  });
});

describe("Broward supported-route permit ingest", () => {
  it("keeps the control session alive and bounds network-silent queries", () => {
    expect(supportedPermitClientConfig("postgresql://example.test/db")).toEqual(
      expect.objectContaining({
        connectionString: "postgresql://example.test/db",
        keepAlive: true,
        keepAliveInitialDelayMillis: 10_000,
        query_timeout: 120_000,
        statement_timeout: 120_000,
      }),
    );
  });

  it("reconciles the exact BCS child summary field and property identity", () => {
    const summary = {
      event: "broward_bcs_permit_probe_completed",
      sourceSystem: "broward_county_bcs_posse_permits",
      parcelCount: 1,
      roofOnly: false,
      normalizedRecordCount: 7,
      observations: [
        {
          parcelIdentifier: "PRIVATE",
          normalizedRecordCount: 7,
          status: "records",
        },
      ],
    };
    expect(readBcsSummaryRecordCount(summary, "PRIVATE", false)).toBe(7);
    expect(() =>
      readBcsSummaryRecordCount(
        {
          ...summary,
          observations: [
            {
              parcelIdentifier: "DIFFERENT",
              normalizedRecordCount: 7,
            },
          ],
        },
        "PRIVATE",
        false,
      ),
    ).toThrow(/identity changed/u);
    expect(() =>
      readBcsSummaryRecordCount(
        {
          ...summary,
          normalizedRecordCount: 6,
        },
        "PRIVATE",
        false,
      ),
    ).toThrow(/do not reconcile/u);
  });

  it("caps total concurrency and requires a stable job ID", () => {
    expect(
      parseSupportedPermitOptions([
        "--job-id",
        "broward-permits-supported-pilot-20260831",
        "--limit",
        "30",
        "--concurrency",
        "4",
        "--max-attempts",
        "3",
        "--max-items",
        "2",
        "--jurisdictions",
        "unincorporated-broward,lazy-lake",
        "--migrate-from-job",
        "broward-permits-supported-full-20260831",
      ]),
    ).toEqual({
      jobId: "broward-permits-supported-pilot-20260831",
      limit: 30,
      concurrency: 4,
      maxAttempts: 3,
      maxItems: 2,
      workDirectory: "downloads/broward/supported-permit-ingest",
      scope: "all",
      jurisdictionKeys: ["lazy-lake", "unincorporated-broward"],
      migrateFromJobId: "broward-permits-supported-full-20260831",
    });
    expect(() =>
      parseSupportedPermitOptions([
        "--job-id",
        "broward-permits-supported",
        "--concurrency",
        "5",
      ]),
    ).toThrow(/through 4/u);
    expect(() =>
      parseSupportedPermitOptions(["--job-id", "unscoped-run"]),
    ).toThrow(/broward-permits-/u);
    expect(
      parseSupportedPermitOptions([
        "--job-id",
        "broward-permits-roofing-pilot-20260831",
        "--scope",
        "roofing",
      ]).scope,
    ).toBe("roofing");
    expect(() => readJurisdictionKeys("plantation,plantation")).toThrow(
      /unique implemented/u,
    );
    expect(failureCooldownDelayMs(1)).toBe(5 * 60_000);
    expect(failureCooldownDelayMs(5)).toBe(4 * 60 * 60_000);
  });

  it("migrates only exact current seed identities and extends finite failures", () => {
    const candidate = {
      folio: "PRIVATE",
      parcelHash: "a".repeat(64),
      situsAddress: "PRIVATE",
      jurisdictionKey: "lazy-lake",
      adapterKey: BROWARD_BCS_ADAPTER_KEY,
    };
    expect(
      normalizeMigratedPermitItem(
        {
          parcel_hash: candidate.parcelHash,
          jurisdiction_key: candidate.jurisdictionKey,
          adapter_key: candidate.adapterKey,
          status: "failed_exhausted",
          record_count: 0,
          attempt_count: 3,
          error_class: "source_or_load_error",
        },
        new Map([[candidate.parcelHash, candidate]]),
        5,
      ),
    ).toMatchObject({
      status: "failed",
      attemptCount: 3,
      recordCount: 0,
    });
    expect(() =>
      normalizeMigratedPermitItem(
        {
          parcel_hash: "b".repeat(64),
          jurisdiction_key: candidate.jurisdictionKey,
          adapter_key: candidate.adapterKey,
          status: "records",
          record_count: 1,
          attempt_count: 1,
          error_class: null,
        },
        new Map([[candidate.parcelHash, candidate]]),
        5,
      ),
    ).toThrow(/compatible/u);
  });

  it("does not let a waiting same-route item reserve a global worker", async () => {
    const events = [];
    const items = [
      { route: "a", id: 1, delay: 30 },
      { route: "a", id: 2, delay: 1 },
      { route: "b", id: 1, delay: 1 },
      { route: "c", id: 1, delay: 1 },
    ];
    let active = 0;
    let maximumActive = 0;
    const routeActive = new Set();
    await processByRouteWithConcurrency(
      items,
      2,
      (item) => item.route,
      async (item) => {
        expect(routeActive.has(item.route)).toBe(false);
        routeActive.add(item.route);
        active += 1;
        maximumActive = Math.max(maximumActive, active);
        events.push(`start:${item.route}:${String(item.id)}`);
        await new Promise((resolvePromise) =>
          setTimeout(resolvePromise, item.delay),
        );
        events.push(`end:${item.route}:${String(item.id)}`);
        active -= 1;
        routeActive.delete(item.route);
      },
    );
    expect(maximumActive).toBe(2);
    expect(events.indexOf("start:c:1")).toBeLessThan(events.indexOf("end:a:1"));
    expect(events.indexOf("start:a:2")).toBeGreaterThan(
      events.indexOf("end:a:1"),
    );
  });

  it("terminates a probe process group at its hard deadline", async () => {
    const startedAt = Date.now();
    const result = await runNode(
      ["-e", "setTimeout(() => undefined, 60_000)"],
      50,
    );
    expect(result).toMatchObject({
      exitCode: -1,
      timedOut: true,
    });
    expect(Date.now() - startedAt).toBeLessThan(5_000);
  });
});

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
      adapterKey: BROWARD_CITIZENSERVE_ADAPTER_KEY,
      status: "implemented",
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

  it("reconciles every isolated vendor config into the unified registry", () => {
    const unifiedByKey = new Map(
      BROWARD_PERMIT_JURISDICTIONS.map((entry) => [entry.key, entry]),
    );

    expect(Object.keys(BROWARD_ACCELA_SOURCES)).toHaveLength(5);
    for (const source of Object.values(BROWARD_ACCELA_SOURCES)) {
      expect(unifiedByKey.get(source.key)?.primarySource).toMatchObject({
        sourceKey: source.sourceSystem,
        sourceUrl: source.portalUrl,
        adapterKey: BROWARD_ACCELA_ADAPTER_KEY,
        status: "implemented",
      });
    }

    expect(Object.keys(BROWARD_TYLER_CITIZENSERVE_JURISDICTIONS)).toHaveLength(
      10,
    );
    for (const config of Object.values(
      BROWARD_TYLER_CITIZENSERVE_JURISDICTIONS,
    )) {
      const route = unifiedByKey.get(
        config.key.replaceAll("_", "-"),
      )?.primarySource;
      expect(route?.sourceKey).toBe(config.sourceSystem);
      if (config.skipReason !== null) {
        expect(route).toMatchObject({
          adapterKey: BROWARD_TYLER_CIVIC_ACCESS_ADAPTER_KEY,
          status: "login_required",
        });
      } else {
        expect(route).toMatchObject({
          adapterKey:
            config.vendor === "citizenserve"
              ? BROWARD_CITIZENSERVE_ADAPTER_KEY
              : BROWARD_TYLER_CIVIC_ACCESS_ADAPTER_KEY,
          status: "implemented",
        });
      }
    }

    const municipalAdapterKeys = new Map([
      ["coconut_creek", "coconut-creek-permit-status"],
      ["click2gov", "click2gov"],
      ["tyler_esuite", "tyler-esuite"],
      ["tyler_energov", BROWARD_TYLER_CIVIC_ACCESS_ADAPTER_KEY],
      ["gov_easy", "gov-easy"],
      ["smartgov", "granicus-smartgov"],
      ["opengov", "opengov"],
      ["communitycore", "communitycore"],
      ["mgo_connect", "mygovernmentonline"],
      ["egovplus", "egovplus"],
      ["records_request", null],
    ]);
    expect(BROWARD_MUNICIPAL_PERMIT_JURISDICTIONS).toHaveLength(14);
    for (const config of BROWARD_MUNICIPAL_PERMIT_JURISDICTIONS) {
      const jurisdiction = unifiedByKey.get(config.key.replaceAll("_", "-"));
      const routes =
        jurisdiction === undefined
          ? []
          : [jurisdiction.primarySource, ...jurisdiction.supplementalSources];
      expect(routes).toContainEqual(
        expect.objectContaining({
          adapterKey: municipalAdapterKeys.get(config.protocol),
        }),
      );
    }

    expect(
      BROWARD_PERMIT_JURISDICTIONS.filter(
        (entry) => entry.primarySource.status === "implemented",
      ),
    ).toHaveLength(24);
    expect(
      BROWARD_PERMIT_JURISDICTIONS.filter(
        (entry) => entry.primarySource.status === "adapter_unavailable",
      ),
    ).toHaveLength(1);
    expect(
      BROWARD_PERMIT_JURISDICTIONS.filter(
        (entry) => entry.primarySource.status === "login_required",
      ),
    ).toHaveLength(2);
    expect(
      BROWARD_PERMIT_JURISDICTIONS.filter(
        (entry) => entry.primarySource.status === "no_anonymous_search",
      ),
    ).toHaveLength(1);
    expect(
      BROWARD_PERMIT_JURISDICTIONS.filter(
        (entry) => entry.primarySource.status === "captcha_required",
      ),
    ).toHaveLength(3);
    expect(
      BROWARD_PERMIT_JURISDICTIONS.filter(
        (entry) => entry.primarySource.status === "custodian_only",
      ),
    ).toHaveLength(1);
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
    expect(
      BROWARD_PERMIT_JURISDICTIONS.find(
        (entry) => entry.key === "hillsboro-beach",
      )?.primarySource.status,
    ).toBe("captcha_required");
    expect(
      BROWARD_PERMIT_JURISDICTIONS.find(
        (entry) => entry.key === "pembroke-park",
      )?.primarySource,
    ).toMatchObject({
      status: "captcha_required",
      reason: expect.stringMatching(
        /user-authorized validated session.*never anonymous access/iu,
      ),
    });
    expect(
      BROWARD_PERMIT_JURISDICTIONS.find(
        (entry) => entry.key === "deerfield-beach",
      )?.primarySource.status,
    ).toBe("no_anonymous_search");
  });

  it("keeps Sea Ranch BCS evidence supplemental and Sunrise anonymously implemented", () => {
    const seaRanch = BROWARD_PERMIT_JURISDICTIONS.find(
      (entry) => entry.key === "sea-ranch-lakes",
    );
    expect(seaRanch?.primarySource).toMatchObject({
      status: "custodian_only",
      coverageKind: "current",
    });
    expect(seaRanch?.supplementalSources).toEqual([
      expect.objectContaining({
        adapterKey: BROWARD_BCS_ADAPTER_KEY,
        coverageKind: "supplemental",
      }),
    ]);
    expect(
      BROWARD_PERMIT_JURISDICTIONS.find((entry) => entry.key === "sunrise")
        ?.primarySource,
    ).toMatchObject({
      adapterKey: BROWARD_TYLER_CIVIC_ACCESS_ADAPTER_KEY,
      status: "implemented",
    });
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
      474134000012: {
        folioNumber: "474134000012",
        situsCity: "UNINCORPORATED",
        situsAddress1: "NW 81 STREET",
        situsZipCode: "33076",
        useCode: "52 - Cropland soil capability class II",
      },
      494318013550: {
        folioNumber: "494318013550",
        situsCity: "LAUDERDALE BY THE SEA",
        situsAddress1: "218 E COMMERCIAL BOULEVARD",
        situsZipCode: "33308",
        useCode: "12-02 Mixed store and office",
      },
      484109030410: {
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
      currentSourceJurisdictionsImplemented: 24,
      currentSourceJurisdictionsBlocked: 8,
    });
    expect(first.acceptance).toEqual({
      localPilotPassed: true,
      countyPermitAcceptancePassed: false,
      reason:
        "Appraisal acceptance and a bounded permit pilot do not establish full permit acceptance while current municipal sources remain unavailable",
    });
    expect(adapterCalls).toEqual(["474134000012", "494318013550"]);
    const statusWrites = [];
    await recordBrowardPermitPilotStatus(
      /** @type {import("pg").Client} */ ({
        query: (sql, values) => {
          statusWrites.push({ sql, values });
          return Promise.resolve({ rows: [] });
        },
      }),
      first,
    );
    expect(statusWrites).toHaveLength(1);
    expect(statusWrites[0]?.sql).toContain(
      "record_broward_permit_pilot_status",
    );
    expect(statusWrites[0]?.values).toEqual([
      3,
      3,
      3,
      3,
      0,
      4,
      2,
      2,
      2,
      1,
      0,
      1,
      0,
      0,
      1,
      1,
      true,
      true,
      true,
      true,
      false,
      first.generatedAt,
    ]);
    expect(JSON.stringify(statusWrites)).not.toMatch(
      /474134000012|494318013550|situs|address|owner/iu,
    );

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
      'parcel_id,parcel_polygon\n474134000012,"{""type"":""Polygon""}"\n494318013550,"{}"\n',
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
    expect(
      parseBrowardPermitPilotOptions(["--pilot", "--record-neon-status"]),
    ).toMatchObject({
      inputMode: "pilot",
      maxAdapterAttempts: 5,
      appraisalDelayMs: 300,
      permitDelayMs: 1_000,
      recordNeonStatus: true,
    });
    expect(() =>
      parseBrowardPermitPilotOptions(["--pilot", "--permit-delay-ms", "999"]),
    ).toThrow("at least 1000");
    expect(() =>
      parseBrowardPermitPilotOptions([
        "--pilot",
        "--max-adapter-attempts",
        "6",
      ]),
    ).toThrow("from 1 through 5");
  });

  it("verifies the aggregate permit status target read-only", async () => {
    const calls = [];
    const client = {
      query: (sql) => {
        calls.push(sql);
        return Promise.resolve({
          rows: sql.includes("current_setting")
            ? [
                {
                  project_id: "raspy-frost-51580436",
                  branch_id: "br-isolated-broward",
                  endpoint_id: "ep-isolated-broward",
                },
              ]
            : [],
        });
      },
    };

    await verifyBrowardPermitStatusTarget(
      /** @type {import("pg").Client} */ (client),
      {
        expectedBranchId: "br-isolated-broward",
        expectedEndpointId: "ep-isolated-broward",
      },
    );
    expect(calls).toEqual([
      "BEGIN READ ONLY",
      expect.stringContaining("current_setting"),
      "ROLLBACK",
    ]);
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
