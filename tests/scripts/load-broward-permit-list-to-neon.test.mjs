import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import {
  mapPermitListLoadRow,
  normalizePermitListRecord,
  parsePermitListLoadOptions,
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
    recordKey:
      "broward_pembroke_park_gov_easy_permits:application:12345",
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
    });
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
