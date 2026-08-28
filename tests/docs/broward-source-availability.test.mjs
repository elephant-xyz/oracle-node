import { readFile } from "fs/promises";
import { describe, expect, it } from "vitest";
import { parse } from "yaml";

const SOURCE_PATH = new URL("../../docs/broward-sources.yaml", import.meta.url);

describe("Broward official source availability", () => {
  it("documents all 31 municipalities plus unincorporated BMSD", async () => {
    const source = parse(await readFile(SOURCE_PATH, "utf8"));
    const jurisdictions = source.permits.map((item) => item.jurisdiction);

    expect(jurisdictions).toHaveLength(32);
    expect(new Set(jurisdictions).size).toBe(32);
    expect(jurisdictions).toContain("Unincorporated Broward County");
    expect(
      source.permits.every(
        (item) =>
          typeof item.status === "string" && item.status !== "needs-review",
      ),
    ).toBe(true);
    expect(
      source.permit_source_documentation.official_custodian_directory,
    ).toContain("broward.org");
  });

  it("records official or explicitly restricted non-permit categories", async () => {
    const source = parse(await readFile(SOURCE_PATH, "utf8"));

    expect(source.appraisal.detail_api.url).toContain("bcpa.net");
    expect(source.gis.rest).toContain("bcpa.net");
    expect(source.sunbiz.bulk).toContain("dos.fl.gov");
    expect(source.sunbiz.zip_candidates).toContain("33388");
    expect(source.sunbiz.zip_candidates).not.toContain("33070");
    expect(source.bbb.access).toBe("approval-required");
    expect(source.bbb.status).toBe("unavailable-without-approved-api-access");
    expect(source.recorder.bulk).toContain("images and index data");
  });
});
