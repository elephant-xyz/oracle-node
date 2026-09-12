import { describe, expect, it } from "vitest";

import {
  artifactPaths,
  parseOptions,
} from "../../scripts/opendoor/publish_filebase_batch.mjs";

describe("OpenDoor Filebase batch publisher", () => {
  it("requires an explicit publication phase", () => {
    expect(() => parseOptions([])).toThrow(/--phase/);
    expect(parseOptions(["--phase", "base"]).phase).toBe("base");
    expect(parseOptions(["--phase", "base"]).cidOnly).toBe(false);
    expect(parseOptions(["--phase", "base", "--cid-only"]).cidOnly).toBe(true);
  });

  it("routes HOA/PM artifacts to distinct labels", () => {
    expect(artifactPaths("martin", "hoa-pm")).toMatchObject({
      county: "martin",
      queryTableIpnsLabel: "oracle-query-table-martin-hoa-pm",
      coverageIpnsLabel: "oracle-dataset-coverage-martin-hoa-pm",
    });
    expect(artifactPaths("martin", "hoa-pm").hoaPmObjectsPath).toContain(
      "/martin/hoa-pm/objects/hoa-pm-objects.jsonl",
    );
  });
});
