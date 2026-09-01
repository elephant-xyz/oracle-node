import { describe, expect, it } from "vitest";

import {
  buildRoofingBbbCandidates,
  isRoofingContractorPlaceholder,
  normalizeRoofingContractorLicense,
  normalizeRoofingContractorName,
  parseRoofingBbbWorklistOptions,
} from "../../scripts/build-broward-roofing-bbb-worklist.mjs";

describe("Broward roofing-only BBB worklist", () => {
  it("parses a bounded private worklist command", () => {
    expect(
      parseRoofingBbbWorklistOptions([
        "--output-dir",
        "downloads/broward/bbb-roofing",
        "--limit",
        "100",
      ]),
    ).toEqual({
      outputDirectory: "downloads/broward/bbb-roofing",
      limit: 100,
    });
    expect(() => parseRoofingBbbWorklistOptions(["--limit", "0"])).toThrow(
      /1 through 100000/u,
    );
  });

  it("normalizes business identities and rejects placeholders", () => {
    expect(normalizeRoofingContractorName("Example Roofing, LLC")).toBe(
      "EXAMPLE ROOFING",
    );
    expect(normalizeRoofingContractorLicense(" ccc 1234567 ")).toBe(
      "CCC1234567",
    );
    expect(normalizeRoofingContractorLicense("TBD")).toBeNull();
    expect(isRoofingContractorPlaceholder("OWNER / BUILDER")).toBe(true);
    expect(isRoofingContractorPlaceholder("Example Roofing LLC")).toBe(false);
  });

  it("deduplicates licenses, chooses the most evidenced name, and reconciles exclusions", () => {
    const result = buildRoofingBbbCandidates([
      {
        source_system: "broward_fort_lauderdale_lauderbuild_permits",
        contractor_name: "Example Roofing LLC",
        contractor_license: "CCC1234567",
        permit_date: "2025-02-01",
      },
      {
        source_system: "broward_fort_lauderdale_lauderbuild_permits",
        contractor_name: "EXAMPLE ROOFING, LLC",
        contractor_license: "CCC1234567",
        permit_date: "2026-02-01",
      },
      {
        source_system: "broward_fort_lauderdale_lauderbuild_permits",
        contractor_name: "EXAMPLE ROOFING, LLC",
        contractor_license: "CCC1234567",
        permit_date: "2026-01-01",
      },
      {
        source_system: "broward_hollywood_accela_permits",
        contractor_name: "Name Only Roofs Inc.",
        contractor_license: null,
        permit_date: null,
      },
      {
        source_system: "broward_fort_lauderdale_lauderbuild_permits",
        contractor_name: "TO BE DETERMINED",
        contractor_license: "TBD",
        permit_date: "2025-01-01",
      },
    ]);
    expect(result.excludedPlaceholderPermits).toBe(1);
    expect(result.candidates).toEqual([
      {
        identityKey: "license:CCC1234567",
        contractorName: "EXAMPLE ROOFING, LLC",
        contractorLicense: "CCC1234567",
        roofingPermitCount: 3,
        earliestPermitDate: "2025-02-01",
        latestPermitDate: "2026-02-01",
        sourceSystems: ["broward_fort_lauderdale_lauderbuild_permits"],
      },
      {
        identityKey: "name:NAME ONLY ROOFS",
        contractorName: "Name Only Roofs Inc.",
        contractorLicense: null,
        roofingPermitCount: 1,
        earliestPermitDate: null,
        latestPermitDate: null,
        sourceSystems: ["broward_hollywood_accela_permits"],
      },
    ]);
  });
});
