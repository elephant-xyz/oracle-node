import { readFile } from "fs/promises";
import { describe, expect, it } from "vitest";

const FLOW_PATH = new URL(
  "../../multi-request-flows/Rock_Island.json",
  import.meta.url,
);

describe("Rock Island multi-request appraisal flow", () => {
  it("queries only the approved non-PII ArcGIS field allow-list", async () => {
    const flow = JSON.parse(await readFile(FLOW_PATH, "utf8"));
    const [parcelRequest] = flow.requests;
    const query = parcelRequest.request.multiValueQueryString;
    const outFields = query.outFields.join(",");

    expect(parcelRequest.key).toBe("ParcelFeature");
    expect(parcelRequest.request.method).toBe("GET");
    expect(parcelRequest.request.url).toMatch(
      /services9\.arcgis\.com\/.*\/Parcels\/FeatureServer\/0\/query$/,
    );
    expect(query.where).toEqual(["PIN='{{=it.request_identifier}}'"]);
    expect(query.returnGeometry).toEqual(["true"]);
    expect(query.outSR).toEqual(["4326"]);
    expect(outFields).toContain("PIN");
    expect(outFields).toContain("EAV");
    expect(outFields).not.toMatch(/owner/i);
    expect(outFields).not.toMatch(/taxbill_(?:name|addr|first|last|zip)/i);
  });
});
