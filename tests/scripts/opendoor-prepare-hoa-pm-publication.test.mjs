import { describe, expect, it } from "vitest";

import {
  baseCoverage,
  parseOptions,
} from "../../scripts/opendoor/prepare_hoa_pm_publication.mjs";

describe("OpenDoor HOA/PM publication preparation", () => {
  it("uses all ready counties and the current Sunbiz quarter by default", () => {
    const options = parseOptions([]);
    expect(options.quarter).toBe("2026Q3");
    expect(options.counties).toHaveLength(11);
    expect(options.counties).toContain("martin");
  });

  it("builds honest partial appraisal coverage", () => {
    expect(
      baseCoverage("martin", {
        exportedAt: "2026-09-12T01:11:02.503Z",
        rowCount: 22,
        expectedCount: 22,
        publicationScope: {
          schemaVersion: "1.0",
          level: "partial",
          denominatorBasis: "published_subset",
        },
      }),
    ).toEqual({
      county: "martin",
      exportedAt: "2026-09-12T01:11:02.503Z",
      publicationScope: {
        schemaVersion: "1.0",
        level: "partial",
        denominatorBasis: "published_subset",
      },
      datasets: [
        {
          county: "martin",
          source: "appraisal",
          ingested_count: 22,
          expected_count: 22,
          first_loaded_at: "2026-09-12T01:11:02.503Z",
          last_loaded_at: "2026-09-12T01:11:02.503Z",
          cid: null,
          ipns_label: null,
        },
      ],
    });
  });
});
