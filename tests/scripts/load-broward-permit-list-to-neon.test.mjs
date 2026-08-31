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
    recordKey:
      "broward_hollywood_accela_permits:permit:STRUC-ROOF-26-001317",
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
      source_record_key:
        "broward_pembroke_pines_tyler_permits:case-1",
    });
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
        records: [expect.objectContaining({ permitNumber: record.recordNumber })],
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
