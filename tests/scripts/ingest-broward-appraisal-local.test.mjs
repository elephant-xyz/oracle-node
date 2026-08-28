import { describe, expect, it } from "vitest";

import {
  buildBrowardSeedEntities,
  buildBrowardSourceRequest,
  parseCliOptions,
} from "../../scripts/ingest-broward-appraisal-local.mjs";

describe("Broward local appraisal ingestion", () => {
  it("builds the exact fail-closed BCPA request", () => {
    expect(buildBrowardSourceRequest("504108BJ0140")).toMatchObject({
      method: "POST",
      url: expect.stringContaining("getParcelInformation"),
      headers: {
        "content-type": "application/json",
        accept: expect.any(String),
        "x-requested-with": "XMLHttpRequest",
        origin: "https://web.bcpa.net",
        referer: expect.any(String),
      },
      json: {
        folioNumber: "504108BJ0140",
        taxyear: "",
        action: "CURRENT",
        use: "",
      },
    });
  });

  it("builds county-scoped compatibility seed entities", () => {
    const entities = buildBrowardSeedEntities(
      {
        request_identifier: "504108BJ0140",
        address: "",
        latitude: "26.10864266",
        longitude: "-80.27918202",
      },
      "504108BJ0140",
    );
    expect(entities.propertySeed).toMatchObject({
      request_identifier: "504108BJ0140",
      parcel_id: "504108BJ0140",
      source_http_request: {
        method: "POST",
        headers: { "content-type": "application/json" },
        json: { folioNumber: "504108BJ0140", taxyear: "" },
      },
    });
    expect(entities.unnormalizedAddress).toMatchObject({
      request_identifier: "504108BJ0140",
      county_jurisdiction: "Broward",
      latitude: 26.10864266,
      longitude: -80.27918202,
    });
  });

  it("bounds source concurrency and supports resumable run options", () => {
    expect(
      parseCliOptions([
        "--seed",
        "downloads/broward/test.csv",
        "--concurrency",
        "4",
        "--limit",
        "25",
        "--reset-checkpoint",
      ]),
    ).toMatchObject({
      seedPath: "downloads/broward/test.csv",
      concurrency: 4,
      limit: 25,
      resetCheckpoint: true,
    });
    expect(() => parseCliOptions(["--concurrency", "5"])).toThrow(
      /cannot exceed 4/,
    );
  });
});
