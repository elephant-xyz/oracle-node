import { describe, expect, it } from "vitest";

import {
  buildCombinedPermitParquetSchema,
  molineCombinedPermitKey,
  toMolineCombinedPermitRow,
} from "../../scripts/build-rock-island-combined-permit-publication.mjs";

function legacyRecord() {
  return {
    source_system: "moline_official_monthly_building_permit_reports",
    source_url: "https://www.moline.il.us/Archive.aspx?ADID=3728",
    city: "Moline",
    permit_number: null,
    parcel_identifier: null,
    work_location: "PRIVATE",
    permit_issue_date: "2017-01-03",
    record_status: "issued",
    record_type: "B: RES SIDING",
    project_description: "PRIVATE",
    contractor_business_names: ["PRIVATE"],
    is_roof_permit: false,
    raw: {
      source_archive_id: "3728",
      source_report_title: "2017-01 Building Permits Issued",
      source_application_year: "17",
      source_application_number: "1",
      source_permit_code: "BRSD",
      source_pages: [2],
      source_reports: [],
    },
  };
}

describe("combined Rock Island permit publication", () => {
  it("maps legacy Moline identity without fabricating a permit number", () => {
    const record = legacyRecord();
    expect(molineCombinedPermitKey(record)).toBe(
      "moline_official_monthly_building_permit_reports:application:17:1:BRSD:issued:2017-01-03",
    );
    expect(toMolineCombinedPermitRow(record)).toEqual({
      permit_key:
        "moline_official_monthly_building_permit_reports:application:17:1:BRSD:issued:2017-01-03",
      source_system: "moline_official_monthly_building_permit_reports",
      source_report_document_id: "3728",
      source_report_title: "2017-01 Building Permits Issued",
      source_report_url: "https://www.moline.il.us/Archive.aspx?ADID=3728",
      permit_number: null,
      permit_issue_date: "2017-01-03",
      record_status: "Issued",
      record_type: "B: RES SIDING",
      city: "Moline",
      is_roof_permit: false,
    });
  });

  it("keeps only the strict City-compatible public schema", () => {
    const row = toMolineCombinedPermitRow(legacyRecord());
    expect(Object.keys(row)).toEqual([
      "permit_key",
      "source_system",
      "source_report_document_id",
      "source_report_title",
      "source_report_url",
      "permit_number",
      "permit_issue_date",
      "record_status",
      "record_type",
      "city",
      "is_roof_permit",
    ]);
    expect(JSON.stringify(row)).not.toMatch(
      /PRIVATE|address|parcel|description|contractor|valuation|raw/,
    );
  });

  it("makes permit_number nullable for official legacy application identity", () => {
    const schema = buildCombinedPermitParquetSchema();
    expect(schema.fields.permit_number?.repetitionType).toBe("OPTIONAL");
    expect(schema.fields.permit_key?.repetitionType).toBe("REQUIRED");
  });
});
