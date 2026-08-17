import { describe, expect, it } from "vitest";

import {
  PUBLIC_PERMIT_FIELDS,
  assertPrivateInputSchema,
  buildSafePermitParquetSchema,
  scanPublicPermitRow,
  stableJson,
  toPublicPermitRow,
} from "../../scripts/build-rock-island-safe-permit-publication.mjs";

/**
 * Build one synthetic private record containing fields that must not cross the
 * public transformation boundary.
 *
 * @returns {Record<string, unknown>} Private normalized permit.
 */
function privatePermit() {
  return {
    source_system: "rock_island_city_official_monthly_permit_reports",
    source_url:
      "https://www.rigov.org/DocumentCenter/View/20365/Apr-2026-Permit-Report",
    city: "Rock Island",
    permit_number: "B260001",
    parcel_identifier: "private-pin",
    work_location: "private-address",
    permit_issue_date: "2026-04-30",
    record_status: "Issued",
    record_type: "roof",
    project_description: "private-description",
    contractor_business_names: ["private-contractor"],
    is_roof_permit: true,
    raw: {
      source_document_id: "20365",
      source_report_title: "April",
      raw_text: "private-raw-text",
      project_valuation: 123,
    },
  };
}

describe("Rock Island safe permit publication", () => {
  it("maps only explicitly approved fields", () => {
    const row = toPublicPermitRow(privatePermit());

    expect(Object.keys(row)).toEqual(PUBLIC_PERMIT_FIELDS);
    expect(row).toEqual({
      permit_key: "rock_island_city_official_monthly_permit_reports:B260001",
      source_system: "rock_island_city_official_monthly_permit_reports",
      source_report_document_id: "20365",
      source_report_title: "April",
      source_report_url:
        "https://www.rigov.org/DocumentCenter/View/20365/Apr-2026-Permit-Report",
      permit_number: "B260001",
      permit_issue_date: "2026-04-30",
      record_status: "Issued",
      record_type: "roof",
      city: "Rock Island",
      is_roof_permit: true,
    });
    expect(JSON.stringify(row)).not.toMatch(
      /private-pin|private-address|private-description|private-contractor|private-raw-text|123/,
    );
  });

  it("fails closed when the private input schema changes", () => {
    const drifted = { ...privatePermit(), newly_added_field: "unreviewed" };
    expect(() => assertPrivateInputSchema(drifted)).toThrow(
      /Private permit schema drift/,
    );
  });

  it("rejects unreviewed semantic vocabularies", () => {
    expect(() =>
      toPublicPermitRow({
        ...privatePermit(),
        record_status: "Applicant Name",
      }),
    ).toThrow(/Unapproved public status/);
    expect(() =>
      toPublicPermitRow({ ...privatePermit(), record_type: "Unreviewed Type" }),
    ).toThrow(/Unreviewed permit type/);
  });

  it("finds contact and identity patterns in public values", () => {
    const row = toPublicPermitRow(privatePermit());
    expect(scanPublicPermitRow(row)).toEqual([]);
    expect(
      scanPublicPermitRow({
        ...row,
        source_report_title: "person@example.com 309-555-1212 111-22-3333",
      }),
    ).toEqual(
      expect.arrayContaining([
        "email_value:source_report_title",
        "phone_value:source_report_title",
        "ssn_value:source_report_title",
      ]),
    );
  });

  it("keeps the physical Parquet schema closed and deterministic", () => {
    expect(Object.keys(buildSafePermitParquetSchema().fields)).toEqual(
      PUBLIC_PERMIT_FIELDS,
    );
    expect(stableJson({ b: 2, a: 1 })).toBe('{\n  "b": 2,\n  "a": 1\n}\n');
  });
});
