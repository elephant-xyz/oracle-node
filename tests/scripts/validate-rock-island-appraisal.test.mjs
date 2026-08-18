import { describe, expect, it } from "vitest";

import {
  parseCliOptions,
  parseValidationIssues,
  renderSeedRecord,
} from "../../scripts/validate-rock-island-appraisal.mjs";

describe("Rock Island appraisal validation harness", () => {
  it("renders a complete one-row seed without corrupting JSON or geometry", () => {
    const csv = renderSeedRecord(
      ["source_identifier", "multiValueQueryString", "parcel_polygon"],
      {
        source_identifier: "0012345678",
        multiValueQueryString: `{"where":["PIN='0012345678'"]}`,
        parcel_polygon: `{"type":"Polygon","coordinates":[[[-90,41],[-91,41],[-90,41]]]}`,
      },
    );

    expect(csv).toContain("0012345678");
    expect(csv).toContain(`"{""where"":[""PIN='0012345678'""]}"`);
    expect(csv.split("\n")).toHaveLength(3);
  });

  it("parses bounded validation runs", () => {
    expect(
      parseCliOptions([
        "--limit",
        "1",
        "--output",
        "downloads/rock-island/test-output",
      ]),
    ).toMatchObject({
      limit: 1,
      outputDirectory: "downloads/rock-island/test-output",
    });
    expect(() => parseCliOptions(["--limit", "-1"])).toThrow(
      /positive integer/,
    );
  });

  it("preserves distinct validator issues from the error CSV", () => {
    const csv = [
      "error_message,file_path",
      '"Schema CID fetch failed","property.json"',
      '"Unused fact_sheet.json","fact_sheet.json"',
      '"Schema CID fetch failed","lot.json"',
      "",
    ].join("\n");

    expect(parseValidationIssues(csv)).toEqual([
      "Schema CID fetch failed",
      "Unused fact_sheet.json",
    ]);
  });
});
