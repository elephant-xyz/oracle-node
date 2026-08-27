import { describe, expect, it } from "vitest";

import {
  findBrowardPropertyMapping,
  parseBrowardUseCodePrefix,
} from "../../scripts/broward-use-code.mjs";

/** @type {readonly import("../../scripts/broward-use-code.mjs").BrowardUseCodeMapping[]} */
const SAMPLE_MAPPINGS = [
  {
    property_usecode: "01-01 SINGLE FAMILY",
    property_usage_type: "Residential",
    property_type: "Building",
    ownership_estate_type: "FeeSimple",
    structure_form: "SingleFamilyDetached",
    build_status: "Improved",
  },
  {
    property_usecode: "04-01 CONDOMINIUM - RESIDENTIAL",
    property_usage_type: "Residential",
    property_type: "Building",
    ownership_estate_type: "Condominium",
    structure_form: null,
    build_status: "Improved",
  },
  {
    property_usecode: "08-01 DUPLEX WITH GUEST HOUSE",
    property_usage_type: "Residential",
    property_type: "Building",
    ownership_estate_type: "FeeSimple",
    structure_form: "Duplex",
    build_status: "Improved",
  },
  {
    property_usecode: "10-01 VACANT COMMERCIAL",
    property_usage_type: "Commercial",
    property_type: "LandParcel",
    ownership_estate_type: "FeeSimple",
    structure_form: null,
    build_status: "VacantLand",
  },
  {
    property_usecode: "52-01 CROPLAND SOIL CAPABILITY CLASS II",
    property_usage_type: "CroplandClass2",
    property_type: "Building",
    ownership_estate_type: "FeeSimple",
    structure_form: null,
    build_status: "Improved",
  },
  {
    property_usecode: "63-01 GRAZING LAND SOIL CAPABILITY CLASS IV - CATTLE",
    property_usage_type: "GrazingLand",
    property_type: "Building",
    ownership_estate_type: "FeeSimple",
    structure_form: null,
    build_status: "Improved",
  },
  {
    property_usecode: "94-01 RIGHT OF WAY - STREET, ROAD, ETC. - PUBLIC",
    property_usage_type: "TransportationTerminal",
    property_type: "Building",
    ownership_estate_type: "FeeSimple",
    structure_form: null,
    build_status: "Improved",
  },
];

describe("Broward use-code matching", () => {
  it("parses subtype and family prefixes", () => {
    expect(parseBrowardUseCodePrefix("01-01 Single Family")).toEqual({
      family: "01",
      subtype: "01-01",
    });
    expect(parseBrowardUseCodePrefix("04 - Condominium")).toEqual({
      family: "04",
      subtype: undefined,
    });
  });

  it("matches subtype labels and family-level live labels from the pilot", () => {
    expect(
      findBrowardPropertyMapping("01-01 Single Family", SAMPLE_MAPPINGS)
        ?.property_usage_type,
    ).toBe("Residential");
    expect(
      findBrowardPropertyMapping("10-01 Vacant Commercial", SAMPLE_MAPPINGS)
        ?.property_usage_type,
    ).toBe("Commercial");
    expect(
      findBrowardPropertyMapping("04 - Condominium", SAMPLE_MAPPINGS)
        ?.property_usecode,
    ).toBe("04-01 CONDOMINIUM - RESIDENTIAL");
    expect(
      findBrowardPropertyMapping(
        "08 - Multi-family - less than 10 units",
        SAMPLE_MAPPINGS,
      )?.property_usecode,
    ).toBe("08-01 DUPLEX WITH GUEST HOUSE");
    expect(
      findBrowardPropertyMapping(
        "52 - Cropland soil capability class II",
        SAMPLE_MAPPINGS,
      )?.property_usage_type,
    ).toBe("CroplandClass2");
    expect(
      findBrowardPropertyMapping(
        "63 - Grazing land soil capability class IV",
        SAMPLE_MAPPINGS,
      )?.property_usage_type,
    ).toBe("GrazingLand");
    expect(
      findBrowardPropertyMapping(
        "94 - Right-of-way, streets, roads, irrigation channel, ditch, etc.",
        SAMPLE_MAPPINGS,
      )?.property_usage_type,
    ).toBe("TransportationTerminal");
  });
});
