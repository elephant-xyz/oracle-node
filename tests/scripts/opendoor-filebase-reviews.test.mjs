import { describe, expect, it } from "vitest";

import {
  integrity,
  parseOptions,
} from "../../scripts/opendoor/prepare_filebase_reviews.mjs";

describe("OpenDoor Filebase publication reviews", () => {
  it("defaults to a non-approved base review", () => {
    expect(parseOptions([])).toMatchObject({
      action: "review",
      phase: "base",
      approvedBy: null,
      approvedAt: null,
      expectedBatchDigest: null,
    });
  });

  it("binds review entries to exact bytes", () => {
    expect(integrity(Buffer.from("PAR1"))).toEqual({
      bytes: 4,
      sha256: "fbc62d3b511368ee275ddc74117d8689b430e1427220e25d30816201d89ca7b6",
    });
  });
});
