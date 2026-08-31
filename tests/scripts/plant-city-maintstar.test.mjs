import { describe, it, expect } from "vitest";
import {
  normalizeMaintStarQuery,
  parseMaintStarRecord,
} from "../../scripts/hillsborough/adapters/plant-city-maintstar.mjs";

describe("Plant City MaintStar Adapter", () => {
  it("normalizes query strings cleanly", () => {
    expect(normalizeMaintStarQuery("  082304255  ")).toBe("082304255");
    expect(normalizeMaintStarQuery("")).toBe("");
  });

  it("parses raw MaintStar JSON record object", () => {
    const raw = {
      id: 450270,
      createdDate: "2026-02-25T13:23:41Z",
      projectNumber: "0226-00823",
      msType: "Solar",
      number: "0226-00823",
      lat: 28.023013419668,
      lng: -82.119135863685,
      type: "Electrical",
      dateVal: "2026-03-17T14:35:34Z",
      datePrefix: "Issued on",
      address: "1004 N VERMONT ST, Plant City, FL 33563-3548",
      status: "Issued / Need NOC",
      description: "INSTALL ROOF MOUNTED SOLAR PANELS",
    };

    const parsed = parseMaintStarRecord(raw);
    expect(parsed).toBeDefined();
    expect(parsed.id).toBe(450270);
    expect(parsed.number).toBe("0226-00823");
    expect(parsed.msType).toBe("Solar");
    expect(parsed.type).toBe("Electrical");
    expect(parsed.status).toBe("Issued / Need NOC");
    expect(parsed.address).toBe("1004 N VERMONT ST, Plant City, FL 33563-3548");
    expect(parsed.lat).toBe(28.023013419668);
    expect(parsed.lng).toBe(-82.119135863685);
    expect(parsed.isRoofPermit).toBe(true);
  });
});
