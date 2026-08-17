import { describe, expect, it } from "vitest";

import {
  catalogVendorToKeys,
  classifyVendor,
  hasPermitEvidence,
} from "../../scripts/permit-source-discovery/vendors.mjs";

describe("permit source vendor classification", () => {
  it("classifies a CentralSquare eTRAKiT URL", () => {
    expect(
      classifyVendor({
        url: "https://moli.csqrcloud.com/community-etrakit/",
        html: "",
      }),
    ).toMatchObject({
      key: "centralsquare",
      name: "CentralSquare / eHub",
      confidence: "url",
    });
  });

  it("maps an eTRAKiT catalog label to CentralSquare", () => {
    expect(catalogVendorToKeys("CentralSquare eTRAKiT")).toContain(
      "centralsquare",
    );
  });

  it("recognizes a public permit-search page", () => {
    expect(
      hasPermitEvidence("<main><h1>eTRAKiT</h1><a>Search Permit</a></main>"),
    ).toBe(true);
  });
});
