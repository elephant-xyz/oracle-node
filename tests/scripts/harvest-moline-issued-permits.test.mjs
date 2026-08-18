import { describe, expect, it } from "vitest";

import {
  assertSafePublicRows,
  classifyArchiveEra,
  combinedArtifactFileNames,
  parseOptions,
} from "../../scripts/harvest-moline-issued-permits.mjs";

describe("Moline official archive harvester", () => {
  it("parses local-only scope and delay options", () => {
    expect(
      parseOptions([
        "--scope",
        "supported",
        "--output-dir",
        "downloads/example",
        "--delay-ms",
        "1500",
      ]),
    ).toEqual({
      scope: "supported",
      outputDirectory: "downloads/example",
      delayMs: 1500,
    });
    expect(() =>
      parseOptions(["--scope", "supported", "--delay-ms", "999"]),
    ).toThrow(/at least 1000/);
    expect(() => parseOptions(["--scope", "all"])).toThrow(
      /current-2025 or supported/,
    );
  });

  it("classifies every reviewed archive boundary explicitly", () => {
    expect(classifyArchiveEra("2012-12")).toBe("blocked-early-identity");
    expect(classifyArchiveEra("2016-12")).toBe("blocked-early-identity");
    expect(classifyArchiveEra("2017-01")).toBe("legacy-2017-2020");
    expect(classifyArchiveEra("2020-04")).toBe("legacy-2017-2020");
    expect(classifyArchiveEra("2020-05")).toBe("blocked-compacted-2020-2021");
    expect(classifyArchiveEra("2021-02")).toBe("blocked-compacted-2020-2021");
    expect(classifyArchiveEra("2021-03")).toBe("legacy-2021-2024");
    expect(classifyArchiveEra("2024-09")).toBe("legacy-2021-2024");
    expect(classifyArchiveEra("2024-10")).toBe("current-transition-2024");
    expect(classifyArchiveEra("2024-12")).toBe("current-transition-2024");
    expect(classifyArchiveEra("2025-01")).toBe("current-2025");
  });

  it("keeps current-only artifacts separate from the full package", () => {
    expect(combinedArtifactFileNames("current-2025")).toEqual({
      privateFileName: "current-2025-load-ready.private.jsonl",
      publicFileName: "current-2025-combined.public-allowlist.jsonl",
    });
    expect(combinedArtifactFileNames("supported")).toEqual({
      privateFileName: "load-ready.private.jsonl",
      publicFileName: "public-allowlist.jsonl",
    });
  });

  it("rejects private source fields from the public allowlist", () => {
    expect(() =>
      assertSafePublicRows([
        {
          permit_key: "safe",
          permit_number: "BP25-000001",
          work_address: "PRIVATE",
        },
      ]),
    ).toThrow(/Forbidden Moline public field: work_address/);
    expect(() =>
      assertSafePublicRows([
        {
          permit_key: "safe",
          source_application_year: "24",
          source_application_number: "100",
          source_permit_code: "BCMR",
        },
      ]),
    ).not.toThrow();
  });
});
