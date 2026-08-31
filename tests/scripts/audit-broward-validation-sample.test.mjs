import AdmZip from "adm-zip";
import { describe, expect, it } from "vitest";

import {
  auditTransformedGeometry,
  countFailureClasses,
} from "../../scripts/audit-broward-validation-sample.mjs";

describe("Broward 50-parcel acceptance audit", () => {
  it("reconciles valid transformed geometry and its relationship", () => {
    const zip = new AdmZip();
    zip.addFile(
      "data/geometry_parcel_0.json",
      Buffer.from(
        JSON.stringify({
          polygon: [
            { longitude: -80.2, latitude: 26.1 },
            { longitude: -80.1, latitude: 26.1 },
            { longitude: -80.1, latitude: 26.2 },
            { longitude: -80.2, latitude: 26.1 },
          ],
        }),
      ),
    );
    zip.addFile(
      "data/relationship_parcel_has_geometry_parcel_0.json",
      Buffer.from(
        JSON.stringify({
          from: { "/": "./parcel.json" },
          to: { "/": "./geometry_parcel_0.json" },
        }),
      ),
    );

    expect(auditTransformedGeometry(zip)).toEqual({
      valid: true,
      geometryFiles: 1,
      relationshipFiles: 1,
      vertices: 4,
    });
  });

  it("counts source and transform failures separately", () => {
    expect(
      countFailureClasses(
        [
          '{"status":"succeeded"}',
          '{"status":"source_error"}',
          '{"status":"transform_error"}',
          "",
        ].join("\n"),
      ),
    ).toEqual({ sourceErrors: 1, transformErrors: 1 });
  });
});
