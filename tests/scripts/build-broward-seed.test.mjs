import { describe, expect, it } from "vitest";

import { requireParcelRecords } from "../../scripts/capture-broward-parcel.mjs";
import {
  BROWARD_COUNTY_NAME,
  BROWARD_DETAIL_URL,
  BROWARD_PILOT_FOLIOS,
} from "../../scripts/broward-folio.mjs";
import {
  buildFolioWhere,
  buildObjectIdPageUrl,
  buildPageUrl,
  centroidFromGeometry,
  parseCliOptions,
  SEED_COLUMNS,
  toSeedRow,
} from "../../scripts/build-broward-seed.mjs";

describe("Broward seed builder", () => {
  it("keeps folio as text including condo letters", () => {
    const row = toSeedRow({
      properties: { FOLIO: "504108BJ0140" },
      geometry: {
        type: "Polygon",
        coordinates: [
          [
            [-80.2, 26.1],
            [-80.1, 26.1],
            [-80.1, 26.2],
            [-80.2, 26.2],
            [-80.2, 26.1],
          ],
        ],
      },
    });

    expect(row?.parcel_id).toBe("504108BJ0140");
    expect(row?.source_identifier).toBe("504108BJ0140");
    expect(row?.request_identifier).toBe("504108BJ0140");
    expect(row?.county).toBe(BROWARD_COUNTY_NAME);
    expect(row?.county_fips).toBe("12011");
    expect(row?.method).toBe("POST");
    expect(row?.url).toBe(BROWARD_DETAIL_URL);
    expect(JSON.parse(row?.headers ?? "{}")).toEqual({
      "content-type": "application/json",
    });
    expect(JSON.parse(row?.json ?? "{}")).toEqual({
      folioNumber: "504108BJ0140",
      taxyear: "",
      action: "CURRENT",
      use: "",
    });
    expect(row?.state).toBe("FL");
    expect(SEED_COLUMNS).toContain("request_identifier");
    expect(SEED_COLUMNS).toContain("headers");
    expect(SEED_COLUMNS).toContain("json");
    expect(JSON.parse(row?.parcel_polygon ?? "{}")).toMatchObject({
      type: "Polygon",
    });
  });

  it("rejects unusable folios instead of padding them", () => {
    expect(
      toSeedRow({
        properties: { FOLIO: "" },
        geometry: null,
      }),
    ).toBeUndefined();
  });

  it("computes a centroid from the exterior ring", () => {
    const centroid = centroidFromGeometry({
      type: "Polygon",
      coordinates: [
        [
          [-80, 26],
          [-79, 26],
          [-79, 27],
          [-80, 27],
          [-80, 26],
        ],
      ],
    });
    expect(centroid.longitude).toBe("-79.50000000");
    expect(centroid.latitude).toBe("26.50000000");
  });

  it("builds a GIS IN clause for the pilot folios", () => {
    const where = buildFolioWhere(["474135010090", "504108BJ0140"]);
    expect(where).toBe("FOLIO IN ('474135010090','504108BJ0140')");
    const url = buildPageUrl(where, 0, 25);
    expect(url).toContain("outSR=4326");
    expect(url).toContain("f=geojson");
    expect(url).toContain("orderByFields=OBJECTID");
    expect(buildObjectIdPageUrl([2, 3])).toContain("objectIds=2%2C3");
    expect(parseCliOptions(["--pilot"]).pilot).toBe(true);
    expect(parseCliOptions(["--pilot"]).outputPath).toContain("broward-pilot");
    expect(parseCliOptions(["--concurrency", "8"]).concurrency).toBe(8);
    expect(() => parseCliOptions(["--concurrency", "17"])).toThrow(
      /cannot exceed 16/,
    );
    expect(BROWARD_PILOT_FOLIOS).toHaveLength(25);
  });

  it("fails closed on an empty appraiser envelope", () => {
    expect(() =>
      requireParcelRecords(
        {
          d: {
            parcelInfok__BackingField: null,
          },
        },
        "474131010000",
      ),
    ).toThrow(/no parcelInfok__BackingField/);
  });
});
