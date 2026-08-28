import { describe, expect, it } from "vitest";

import {
  analyzeGeometry,
  parseCliOptions,
  selectCandidates,
} from "../../scripts/build-broward-validation-sample.mjs";

/**
 * @param {string} folio - Test folio.
 * @param {string} usageType - Test usage type.
 * @param {"Polygon" | "MultiPolygon"} type - Geometry type.
 * @param {boolean} originalPilot - Original-pilot flag.
 * @returns {import("../../scripts/build-broward-validation-sample.mjs").Candidate}
 *   Selection candidate.
 */
function candidate(folio, usageType, type, originalPilot = false) {
  return {
    row: { request_identifier: folio },
    folio,
    usageType,
    geometry: {
      type,
      components: type === "MultiPolygon" ? 2 : 1,
      holes: 0,
      vertices: 5,
      vertexBucket: "small",
    },
    originalPilot,
  };
}

describe("Broward validation sample selection", () => {
  it("analyzes Polygon and MultiPolygon complexity", () => {
    expect(
      analyzeGeometry(
        JSON.stringify({
          type: "Polygon",
          coordinates: [
            [
              [-80, 26],
              [-79, 26],
              [-79, 27],
              [-80, 27],
              [-80, 26],
            ],
            [
              [-79.8, 26.2],
              [-79.7, 26.2],
              [-79.8, 26.2],
            ],
          ],
        }),
      ),
    ).toEqual({
      type: "Polygon",
      components: 1,
      holes: 1,
      vertices: 8,
      vertexBucket: "small",
    });
    expect(
      analyzeGeometry(
        JSON.stringify({
          type: "MultiPolygon",
          coordinates: [
            [
              [
                [-80, 26],
                [-79, 26],
                [-80, 26],
              ],
            ],
            [
              [
                [-81, 26],
                [-80, 26],
                [-81, 26],
              ],
            ],
          ],
        }),
      )?.components,
    ).toBe(2);
  });

  it("preserves original pilot rows before adding diverse signatures", () => {
    const selected = selectCandidates(
      [
        candidate("474135010090", "Commercial", "Polygon", true),
        candidate("504108BJ0140", "Residential", "Polygon", true),
        candidate("111111111111", "Residential", "MultiPolygon"),
        candidate("222222222222", "Warehouse", "Polygon"),
      ],
      4,
    );
    expect(selected.map((item) => item.folio)).toEqual([
      "474135010090",
      "504108BJ0140",
      "222222222222",
      "111111111111",
    ]);
  });

  it("parses an exact configurable sample size", () => {
    expect(parseCliOptions(["--size", "50"]).sampleSize).toBe(50);
    expect(() => parseCliOptions(["--size", "0"])).toThrow(/positive integer/);
  });
});
