import { createHash } from "node:crypto";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import {
  mapPermitListLoadRow,
  normalizePermitListRecord,
  parsePermitListLoadOptions,
  readIncrementalPermitManifest,
  readPermitListRecords,
} from "../../scripts/load-broward-permit-list-to-neon.mjs";

/**
 * Build one complete Accela list artifact row.
 *
 * @param {Record<string, unknown>} [overrides] - Field overrides.
 * @returns {Record<string, unknown>} List record.
 */
function accelaListRecord(overrides = {}) {
  return {
    schemaVersion: "oracle-node.broward-accela-list.v1",
    sourceSystem: "broward_hollywood_accela_permits",
    jurisdiction: "Hollywood",
    recordNumber: "STRUC-ROOF-26-001317",
    sourceUrl:
      "https://aca-prod.accela.com/HOLLYWOOD/Cap/CapDetail.aspx?Module=Building&capID1=REC26&capID2=00000&capID3=00951",
    address: "1740 N 55 AVE",
    description: "Reroofing",
    status: "Plans Received",
    recordType: "Roofing Permit",
    recordKey: "broward_hollywood_accela_permits:permit:STRUC-ROOF-26-001317",
    sourceWindowKeys: ["20260830_20260831"],
    ...overrides,
  };
}

/**
 * Build one complete Tyler list artifact row.
 *
 * @param {Record<string, unknown>} [overrides] - Field overrides.
 * @returns {Record<string, unknown>} List record.
 */
function tylerListRecord(overrides = {}) {
  return {
    source_system: "broward_pembroke_pines_tyler_permits",
    source_url:
      "https://pembrokepinesfl-energovweb.tylerhost.net/apps/selfservice#/permit/case-1",
    city: "Pembroke Pines",
    permit_number: "BUL-200001",
    parcel_identifier: "513914101320",
    work_location: "470 SW 198 TER",
    permit_issue_date: "2020-01-15",
    record_status: "Complete",
    record_type: "Residential Re-Roofing",
    project_description: "Remove and replace roof",
    is_roof_permit: true,
    raw: {
      case_id: "case-1",
      work_class: "RESIDENTIAL RE-ROOF",
      applied_date: "2020-01-10",
      expiration_date: "2020-07-15",
      finalized_date: "2020-02-01",
    },
    ...overrides,
  };
}

/**
 * Build one synthetic municipal terminal-query detail row.
 *
 * @param {Record<string, unknown>} [overrides] - Field overrides.
 * @returns {Record<string, unknown>} Municipal detail record.
 */
function municipalPartialRecord(overrides = {}) {
  return {
    source_system: "broward_margate_click2gov_permits",
    source_protocol: "click2gov",
    source_url: "https://example.invalid/permit/record-1",
    source_search_url: "https://example.invalid/search",
    source_record_id: "record-1",
    record_key: "broward_margate_click2gov_permits:record-1",
    jurisdiction: "Margate",
    permit_number: "SYNTHETIC-1",
    parcel_identifier: "484125010010",
    query_folio: "484125010010",
    work_location: "SYNTHETIC LOCATION",
    application_date: "2025-01-01",
    permit_issue_date: "2025-01-02",
    expiration_date: null,
    record_status: "Issued",
    record_type: "Roofing",
    project_description: "Synthetic roof",
    job_value: null,
    inspections: [],
    is_roof_permit: true,
    raw: { terminal_query_index: 7 },
    ...overrides,
  };
}

/**
 * Build one allow-listed Pembroke Park Gov-Easy list artifact row.
 *
 * Values are synthetic and deliberately omit owner, contractor, contact,
 * payment, CAPTCHA, and browser-session fields.
 *
 * @param {Record<string, unknown>} [overrides] - Field overrides.
 * @returns {Record<string, unknown>} Gov-Easy list record.
 */
function govEasyListRecord(overrides = {}) {
  return {
    schemaVersion: "oracle-node.broward-gov-easy-list.v1",
    sourceSystem: "broward_pembroke_park_gov_easy_permits",
    jurisdiction: "Pembroke Park",
    sourceRecordId: "12345",
    recordKey: "broward_pembroke_park_gov_easy_permits:application:12345",
    permitNumber: "PP-ROOF-2026-0001",
    jobName: "ROOF REPLACEMENT",
    status: "Issued",
    address: "100 SAMPLE ST",
    sourceUrl:
      "https://apps.gov-easy.com/Home/PermitInspection/Search?clientId=d60f9827-2c53-44a4-9037-31e1de2b3f09",
    sourcePage: 1,
    isRoofPermit: true,
    coverage: {
      queryField: "Job Name",
      queryValue: "ROOF",
      sourceReportedCount: 166,
    },
    ...overrides,
  };
}

/**
 * Build one synthetic allow-listed Coral Springs eTRAKiT row.
 *
 * @param {number} index - One-based fixture record.
 * @param {number} page - One-based source page.
 * @param {Record<string, unknown>} [overrides] - Field overrides.
 * @returns {Record<string, unknown>} eTRAKiT list record.
 */
function etrakitListRecord(index, page, overrides = {}) {
  const sourceRecordId = `TEST:${String(index).padStart(19, "0")}`;
  return {
    schemaVersion: "oracle-node.broward-etrakit-list.v1",
    sourceSystem: "broward_coral_springs_etrakit_permits",
    jurisdiction: "Coral Springs",
    sourceRecordId,
    recordKey: `broward_coral_springs_etrakit_permits:record:${sourceRecordId}`,
    permitNumber: `TEST-${String(index).padStart(6, "0")}`,
    recordType: "RESIDENTIAL REROOF",
    status: "Issued",
    address: "SYNTHETIC LOCATION",
    folio: "000000000001",
    sourceUrl: "https://etrakit.coralsprings.gov/eTRAKiT/Search/permit.aspx",
    sourcePages: [page],
    isRoofPermit: true,
    coverage: {
      queryField: "Permit Type",
      queryOperator: "Contains",
      queryValue: "ROOF",
      sourceReportedCount: 59_379,
      exposedRecordCap: 1_000,
      exposedPageCount: 50,
      pageSize: 20,
      completenessBoundary: "bounded_capped_keyword_slice",
      countEvidence: "operator_observed_source_result",
    },
    ...overrides,
  };
}

describe("Broward permit list Neon loading", () => {
  it("parses an immutable chunked load job", () => {
    expect(
      parsePermitListLoadOptions([
        "--job-id",
        "broward-permits-hollywood-list-20260831",
        "--input",
        "normalized-list.private.jsonl",
        "--chunk-size",
        "1000",
      ]),
    ).toEqual({
      jobId: "broward-permits-hollywood-list-20260831",
      inputPath: "normalized-list.private.jsonl",
      chunkSize: 1000,
      incrementalManifestPath: null,
      lockWaitSeconds: 0,
    });
  });

  it("parses strict incremental provenance and a bounded writer wait", () => {
    expect(
      parsePermitListLoadOptions([
        "--job-id",
        "broward-permits-margate-incremental-20260903",
        "--input",
        "incremental.private.jsonl",
        "--incremental-manifest",
        "incremental-manifest.private.json",
        "--lock-wait-seconds",
        "300",
      ]),
    ).toMatchObject({
      incrementalManifestPath: "incremental-manifest.private.json",
      lockWaitSeconds: 300,
    });
    expect(() =>
      parsePermitListLoadOptions([
        "--job-id",
        "broward-permits-margate-incremental-20260903",
        "--input",
        "incremental.private.jsonl",
        "--unknown",
        "unsafe",
      ]),
    ).toThrow(/supported flags/u);
    expect(() =>
      parsePermitListLoadOptions([
        "--job-id",
        "broward-permits-margate-incremental-20260903",
        "--input",
        "incremental.private.jsonl",
        "--incremental-manifest",
        "incremental-manifest.private.json",
        "--chunk-size",
        "1001",
      ]),
    ).toThrow(/cannot exceed 1000/u);
  });

  it("maps Accela list inventory without inventing a parcel", () => {
    const normalized = normalizePermitListRecord(accelaListRecord());
    expect(normalized).toMatchObject({
      sourceSystem: "broward_hollywood_accela_permits",
      sourceRecordKey:
        "broward_hollywood_accela_permits:permit:STRUC-ROOF-26-001317",
      permitNumber: "STRUC-ROOF-26-001317",
      parcelIdentifier: null,
      isRoofPermit: true,
    });
    expect(mapPermitListLoadRow(normalized, undefined)).toMatchObject({
      property_id: null,
      parcel_id: null,
      property_match_method: "unmatched",
      property_match_confidence: "unmatched",
      permit_number: "STRUC-ROOF-26-001317",
    });
  });

  it("loads official Accela CSV inventory with the same detail key", () => {
    const normalized = normalizePermitListRecord({
      schemaVersion: "oracle-node.broward-accela-csv-list.v1",
      sourceSystem: "broward_hollywood_accela_permits",
      jurisdiction: "Hollywood",
      recordNumber: "STRUC-ROOF-25-000185",
      sourceUrl:
        "https://aca-prod.accela.com/HOLLYWOOD/Cap/CapDetail.aspx?altId=STRUC-ROOF-25-000185",
      recordKey: "broward_hollywood_accela_permits:permit:STRUC-ROOF-25-000185",
      recordDate: "2025-01-16",
      recordType: "Roofing Permit",
      projectName: null,
      address: "6751 HARDING ST",
      expirationDate: "2025-09-02",
      status: "Closed - Complete",
      isRoofPermit: true,
      sourceWindowKey: "hollywood:date:20250116_20250116",
    });
    expect(normalized).toMatchObject({
      sourceRecordKey:
        "broward_hollywood_accela_permits:permit:STRUC-ROOF-25-000185",
      applicationDate: null,
      expirationDate: "2025-09-02",
      isRoofPermit: true,
    });
  });

  it("maps Tyler CaseId identity and exact Broward folio parents", () => {
    const normalized = normalizePermitListRecord(tylerListRecord());
    expect(normalized).toMatchObject({
      sourceSystem: "broward_pembroke_pines_tyler_permits",
      sourceRecordKey: "broward_pembroke_pines_tyler_permits:case-1",
      parcelIdentifier: "513914101320",
      applicationDate: "2020-01-10",
      permitIssueDate: "2020-01-15",
      isRoofPermit: true,
    });
    expect(
      mapPermitListLoadRow(normalized, {
        propertyId: "11111111-1111-4111-8111-111111111111",
        parcelId: "22222222-2222-4222-8222-222222222222",
      }),
    ).toMatchObject({
      property_id: "11111111-1111-4111-8111-111111111111",
      parcel_id: "22222222-2222-4222-8222-222222222222",
      property_match_method: "exact_folio",
      property_match_confidence: "exact",
      source_record_key: "broward_pembroke_pines_tyler_permits:case-1",
    });
  });

  it("maps terminal municipal detail rows while preserving unmatched support", () => {
    const normalized = normalizePermitListRecord(municipalPartialRecord());
    expect(normalized).toMatchObject({
      sourceSystem: "broward_margate_click2gov_permits",
      sourceRecordKey: "broward_margate_click2gov_permits:record-1",
      parcelIdentifier: "484125010010",
      applicationDate: "2025-01-01",
      isRoofPermit: true,
      sourcePayload: {
        schema_version: "oracle-node.broward-municipal-partial-list.v1",
      },
    });
    expect(mapPermitListLoadRow(normalized, undefined)).toMatchObject({
      property_id: null,
      property_match_method: "unmatched",
      source_system: "broward_margate_click2gov_permits",
    });
  });

  it("binds a strict incremental manifest to one immutable source list", async () => {
    const directory = await mkdtemp(
      join(tmpdir(), "broward-permit-incremental-"),
    );
    try {
      const inputPath = join(directory, "input.private.jsonl");
      const manifestPath = join(
        directory,
        "incremental-manifest.private.json",
      );
      const inputText = `${JSON.stringify(municipalPartialRecord())}\n`;
      const listSha256 = createHash("sha256")
        .update(inputText)
        .digest("hex");
      await writeFile(inputPath, inputText);
      const input = await readPermitListRecords(inputPath);
      const manifest = {
        schemaVersion:
          "oracle-node.broward-permit-incremental-manifest.v1",
        sourceSystem: "broward_margate_click2gov_permits",
        frozenAt: "2026-09-03T15:34:00.000Z",
        coverageBoundary: "partial_terminal_artifacts",
        checkpointSha256: "a".repeat(64),
        listSha256,
        artifactManifestSha256: "b".repeat(64),
        artifactCount: 1,
        artifactRecordCount: 1,
        eligibleRecordCount: 1,
        excludedCounts: {
          source_cap: 0,
          incomplete: 0,
          deferred: 0,
          undated: 0,
          invalid: 0,
          duplicate: 0,
          unreconciled: 0,
          in_flight: 0,
        },
        priorHighWatermark: null,
        highWatermark: { nextQueryIndex: 8 },
      };
      await writeFile(manifestPath, `${JSON.stringify(manifest)}\n`);
      await expect(
        readIncrementalPermitManifest(manifestPath, input),
      ).resolves.toMatchObject({
        manifest: {
          sourceSystem: "broward_margate_click2gov_permits",
          eligibleRecordCount: 1,
        },
        manifestSha256: expect.stringMatching(/^[a-f0-9]{64}$/u),
      });
      await writeFile(
        manifestPath,
        `${JSON.stringify({ ...manifest, eligibleRecordCount: 2 })}\n`,
      );
      await expect(
        readIncrementalPermitManifest(manifestPath, input),
      ).rejects.toThrow(/does not reconcile/u);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("maps a manually authorized Gov-Easy list row without inferring a parcel", () => {
    const normalized = normalizePermitListRecord(govEasyListRecord());
    expect(normalized).toMatchObject({
      sourceSystem: "broward_pembroke_park_gov_easy_permits",
      sourceRecordKey:
        "broward_pembroke_park_gov_easy_permits:application:12345",
      permitNumber: "PP-ROOF-2026-0001",
      parcelIdentifier: null,
      recordType: null,
      description: "ROOF REPLACEMENT",
      isRoofPermit: true,
      sourcePayload: {
        schema_version: "oracle-node.broward-gov-easy-list.v1",
        coverage: {
          queryField: "Job Name",
          queryValue: "ROOF",
          sourceReportedCount: 166,
        },
      },
    });
    expect(mapPermitListLoadRow(normalized, undefined)).toMatchObject({
      property_id: null,
      parcel_id: null,
      property_match_method: "unmatched",
      property_match_confidence: "unmatched",
      source_system: "broward_pembroke_park_gov_easy_permits",
      more_details: {
        list_inventory: true,
        is_roof_permit: true,
      },
    });
  });

  it("preserves conservative non-roof classification inside the ROOF keyword slice", () => {
    const normalized = normalizePermitListRecord(
      govEasyListRecord({
        jobName: "REROOF",
        isRoofPermit: false,
      }),
    );
    expect(normalized.isRoofPermit).toBe(false);
    expect(() =>
      normalizePermitListRecord(
        govEasyListRecord({
          coverage: {
            queryField: "Job Name",
            queryValue: "ALL",
            sourceReportedCount: 166,
          },
        }),
      ),
    ).toThrow("Unsupported Broward permit list row");
  });

  it("maps Coral Springs list rows with exact folios and capped coverage", () => {
    const normalized = normalizePermitListRecord(etrakitListRecord(1, 1));
    expect(normalized).toMatchObject({
      sourceSystem: "broward_coral_springs_etrakit_permits",
      sourceRecordKey:
        "broward_coral_springs_etrakit_permits:record:TEST:0000000000000000001",
      parcelIdentifier: "000000000001",
      applicationDate: null,
      permitIssueDate: null,
      isRoofPermit: true,
      sourcePayload: {
        schema_version: "oracle-node.broward-etrakit-list.v1",
        coverage: {
          sourceReportedCount: 59_379,
          exposedRecordCap: 1_000,
          completenessBoundary: "bounded_capped_keyword_slice",
        },
      },
    });
    expect(mapPermitListLoadRow(normalized, undefined)).toMatchObject({
      property_match_method: "unmatched",
      source_http_request: {
        method: "POST",
        access: "manual_captcha_authorized_session",
        payload_persisted: false,
      },
    });
  });

  it("loads Coral Springs only after all 50 exposed pages reconcile", async () => {
    const directory = await mkdtemp(
      join(tmpdir(), "broward-etrakit-list-load-"),
    );
    try {
      const inputPath = join(directory, "input.jsonl");
      const records = Array.from({ length: 1_000 }, (_value, index) =>
        etrakitListRecord(index + 1, Math.floor(index / 20) + 1),
      );
      await writeFile(
        inputPath,
        `${records.map((record) => JSON.stringify(record)).join("\n")}\n`,
      );
      await expect(readPermitListRecords(inputPath)).resolves.toMatchObject({
        records: expect.arrayContaining([
          expect.objectContaining({
            sourceSystem: "broward_coral_springs_etrakit_permits",
          }),
        ]),
        duplicateCount: 0,
      });
      await writeFile(inputPath, `${JSON.stringify(records[0])}\n`);
      await expect(readPermitListRecords(inputPath)).rejects.toThrow(
        "not the reconciled 1000-row exposed slice",
      );
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("deduplicates exact input and rejects conflicting source keys", async () => {
    const directory = await mkdtemp(
      join(tmpdir(), "broward-permit-list-load-"),
    );
    try {
      const inputPath = join(directory, "input.jsonl");
      const record = accelaListRecord();
      await writeFile(
        inputPath,
        `${JSON.stringify(record)}\n${JSON.stringify(record)}\n`,
      );
      await expect(readPermitListRecords(inputPath)).resolves.toMatchObject({
        records: [
          expect.objectContaining({ permitNumber: record.recordNumber }),
        ],
        duplicateCount: 1,
        inputSha256: expect.stringMatching(/^[a-f0-9]{64}$/u),
      });
      await writeFile(
        inputPath,
        `${JSON.stringify(record)}\n${JSON.stringify(
          accelaListRecord({ status: "Different" }),
        )}\n`,
      );
      await expect(readPermitListRecords(inputPath)).rejects.toThrow(
        /Conflicting Broward permit list record/u,
      );
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });
});
