import { createRequire } from "node:module";
import { describe, expect, it } from "vitest";

const require = createRequire(import.meta.url);
const {
  extractStrapFromRawHtml,
  buildPinellasSourceHttpRequest,
  parsePinellasAddress,
  toLexiconAddress,
} = require("../../../Counties-trasform-scripts/pinellas/scripts/printHtml.js");

describe("Pinellas print HTML strap extraction", () => {
  it("reads the 18-digit STRAP from print query and map div ids", () => {
    expect(
      extractStrapFromRawHtml(
        `var qs = 'is_print=1&s=152703878580000500'; $("#div-parcel-map152703878580000500")`,
      ),
    ).toBe("152703878580000500");
    expect(
      extractStrapFromRawHtml(
        `<div id="div-parcel-map163131676080040070"></div>`,
      ),
    ).toBe("163131676080040070");
  });

  it("does not treat a dashed PARCELID as a STRAP", () => {
    expect(
      extractStrapFromRawHtml(
        `<label id="pacel_no">03-27-15-87858-000-0500</label>`,
      ),
    ).toBeNull();
  });
});

describe("Pinellas lexicon address parsing", () => {
  it("builds a print source_http_request without a query string", () => {
    const request = buildPinellasSourceHttpRequest("152703878580000500");
    expect(request.url).toBe("https://www.pcpao.gov/property/detail/print");
    expect(request.url).not.toContain("?");
    expect(request.multiValueQueryString).toEqual({
      is_print: ["1"],
      s: ["152703878580000500"],
    });
  });

  it("parses a typical Pinellas site line into structured fields", () => {
    const parsed = parsePinellasAddress(
      "1403 CIRCLE DR\nTARPON SPRINGS, FL 34689",
      { strap: "152703878580000500" },
    );
    expect(parsed).toMatchObject({
      street_number: "1403",
      street_name: "CIRCLE",
      street_suffix_type: "Dr",
      city_name: "TARPON SPRINGS",
      state_code: "FL",
      postal_code: "34689",
      country_code: "US",
      county_name: "Pinellas",
      range: "15",
      township: "27",
      section: "03",
    });
    const record = toLexiconAddress(
      parsed,
      buildPinellasSourceHttpRequest("152703878580000500"),
      "152703878580000500",
    );
    expect(record).not.toHaveProperty("unnormalized_address");
    expect(JSON.stringify(record)).not.toContain("?");
  });

  it("keeps pre/post directionals out of street_name", () => {
    expect(
      parsePinellasAddress("7125 S SHORE DR, SOUTH PASADENA FL 33707"),
    ).toMatchObject({
      street_number: "7125",
      street_pre_directional_text: "S",
      street_name: "SHORE",
      street_suffix_type: "Dr",
      city_name: "SOUTH PASADENA",
      postal_code: "33707",
    });
    expect(
      parsePinellasAddress("636 10TH AVE S, ST PETERSBURG FL 33701"),
    ).toMatchObject({
      street_number: "636",
      street_name: "10TH",
      street_suffix_type: "Ave",
      street_post_directional_text: "S",
      city_name: "ST PETERSBURG",
    });
  });

  it("parses vacant sites without a house number and mailing ZIP+4", () => {
    expect(
      parsePinellasAddress("CHURCH ST\nSAFETY HARBOR, FL 34695"),
    ).toMatchObject({
      street_number: null,
      street_name: "CHURCH",
      street_suffix_type: "St",
      city_name: "SAFETY HARBOR",
      postal_code: "34695",
    });
    expect(
      parsePinellasAddress(
        "1403 CIRCLE DR\nTARPON SPRINGS, FL 34689-2030",
      ),
    ).toMatchObject({
      postal_code: "34689",
      plus_four_postal_code: "2030",
    });
  });

  it("parses out-of-state mailing lines and omits Pinellas county_name", () => {
    const parsed = parsePinellasAddress(
      "4590 N WEST SHAFER DR\nMONTICELLO, IN 47960-7065",
      { strap: "173130464040040220" },
    );
    expect(parsed).toMatchObject({
      street_number: "4590",
      street_pre_directional_text: "N",
      street_name: "WEST SHAFER",
      street_suffix_type: "Dr",
      city_name: "MONTICELLO",
      state_code: "IN",
      postal_code: "47960",
      plus_four_postal_code: "7065",
    });
    const record = toLexiconAddress(
      parsed,
      buildPinellasSourceHttpRequest("173130464040040220"),
      "173130464040040220",
    );
    expect(record.county_name).toBeUndefined();
    expect(record.township).toBeNull();
  });

});

