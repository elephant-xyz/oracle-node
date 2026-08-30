import { describe, expect, it } from "vitest";

import { parseFilterSunbizArgs } from "../../scripts/hillsborough/filter-sunbiz-by-zip.mjs";

describe("filter-sunbiz-by-zip local-only", () => {
  it("rejects s3:// sources", async () => {
    await expect(
      parseFilterSunbizArgs([
        "--source",
        "s3://bucket/cordata0.txt",
        "--zip-prefix",
        "336",
      ]),
    ).rejects.toThrow(/Local-only|filesystem|S3/i);
  });

  it("requires at least one local source", async () => {
    await expect(parseFilterSunbizArgs(["--zip-prefix", "336"])).rejects.toThrow(
      /source/,
    );
  });
});
