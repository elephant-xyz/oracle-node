import { describe, expect, it } from "vitest";
import {
  collectMatchedZipPrefixes,
  parseS3Uri,
  stableAddressIdentifier,
  toLexiconAddress,
  transformSunbizRecord,
} from "../../scripts/transform-sunbiz-corporate-to-lexicon.mjs";

/**
 * @typedef {object} SunbizAddressFixture
 * @property {string | null} line1 - First source address line.
 * @property {string | null} line2 - Second source address line.
 * @property {string | null} city - Source city value.
 * @property {string | null} state - Source state value.
 * @property {string | null} zip - Source ZIP value.
 * @property {string | null} country - Source country value.
 * @property {string} singleLine - Source single-line address value.
 * @property {string} normalized - Source normalized address value.
 */

/**
 * Build a Sunbiz address fixture with the complete parser output shape.
 *
 * @param {object} fields - Address fields.
 * @param {string} fields.line1 - First source address line.
 * @param {string} fields.city - Source city value.
 * @param {string} fields.state - Source state value.
 * @param {string} fields.zip - Source ZIP value.
 * @param {string} [fields.country] - Source country value.
 * @returns {SunbizAddressFixture} Complete Sunbiz address fixture.
 */
function buildAddress({ line1, city, state, zip, country }) {
  const singleLine = [line1, city, state, zip].filter(Boolean).join(" ");
  return {
    line1,
    line2: null,
    city,
    state,
    zip,
    country: country ?? null,
    singleLine,
    normalized: singleLine.replace(/\./g, "").toUpperCase(),
  };
}

describe("Sunbiz corporate lexicon transform", () => {
  it("maps a Sunbiz corporate record into business-registration entities and relationships", () => {
    const principalAddress = buildAddress({
      line1: "3123 LAFAYETTE ST.",
      city: "FT. MYERS",
      state: "FL",
      zip: "33916",
    });
    const mailingAddress = buildAddress({
      line1: "PO BOX 100",
      city: "FORT MYERS",
      state: "FL",
      zip: "33902",
    });
    const registeredAgentAddress = buildAddress({
      line1: "1201 HAYS ST.",
      city: "TALLAHASSEE",
      state: "FL",
      zip: "32301",
    });
    const officerAddress = buildAddress({
      line1: "3123 LAFAYETTE ST.",
      city: "FORT MYERS",
      state: "FL",
      zip: "33916",
    });

    const record = {
      sourceFileName: "cordata0.txt",
      sourceLineNumber: 42,
      entity: {
        schemaVersion: "permit-harvest.sunbiz-corporate.v1",
        source: "florida-sunbiz-corporate-bulk",
        documentNumber: "N92000000500",
        entityName: "FAITH TEMPLE CHURCH, INC.",
        statusCode: "I",
        status: "INACTIVE",
        filingTypeCode: "DOMNP",
        filingType: "Domestic Non-Profit",
        principalAddress,
        mailingAddress,
        filedDate: "1992-12-01",
        feiNumber: "590000000",
        moreThanSixOfficers: false,
        lastTransactionDate: "2025-01-15",
        stateCountry: "FL",
        annualReports: [
          { year: "2025", date: "2025-01-15" },
          { year: "2024", date: "2024-02-01" },
          { year: null, date: null },
        ],
        registeredAgent: {
          name: "CORPORATION INFORMATION SERVICES INC.",
          type: "C",
          address: registeredAgentAddress,
        },
        officers: [
          {
            ordinal: 1,
            title: "D",
            type: "P",
            name: "JOHNSON LEBERT",
            address: officerAddress,
          },
        ],
        rawRecordLength: 1440,
      },
      matchedAddresses: [
        {
          role: "principalAddress",
          matchedZipPrefix: "33916",
          zip: "33916",
          officerOrdinal: null,
          officerTitle: null,
          officerName: null,
          address: principalAddress,
        },
        {
          role: "officerAddress",
          matchedZipPrefix: "33916",
          zip: "33916",
          officerOrdinal: 1,
          officerTitle: "D",
          officerName: "JOHNSON LEBERT",
          address: officerAddress,
        },
      ],
    };

    const bundle = transformSunbizRecord(record, {
      sourceDataUri: "s3://example-bucket/cordata0.txt",
    });

    expect(bundle.companies).toEqual([
      {
        source_http_request: {
          method: "GET",
          url: "https://dos.fl.gov/sunbiz/other-services/data-downloads/",
        },
        request_identifier: "sunbiz:N92000000500:company",
        name: "FAITH TEMPLE CHURCH, INC.",
      },
    ]);
    expect(bundle.businessRegistrations[0]).toMatchObject({
      request_identifier: "sunbiz:N92000000500:business_registration",
      source_system: "SUNBIZ",
      source_data_uri: "s3://example-bucket/cordata0.txt",
      source_file_name: "cordata0.txt",
      source_line_number: 42,
      document_number: "N92000000500",
      entity_name: "FAITH TEMPLE CHURCH, INC.",
      status_code: "I",
      status: "INACTIVE",
      filing_type_code: "DOMNP",
      filing_type: "Domestic Non-Profit",
      annual_report_1_year: "2025",
      annual_report_1_date: "2025-01-15",
      matched_address_roles: ["officerAddress", "principalAddress"],
      matched_zip_prefixes: ["33916"],
    });
    expect(bundle.businessRegistrationAddresses).toEqual([
      {
        request_identifier:
          "sunbiz:N92000000500:business_registration_address:principal",
        source_system: "SUNBIZ",
        document_number: "N92000000500",
        address_role: "PRINCIPAL",
        matched_zip_prefixes: ["33916"],
      },
      {
        request_identifier:
          "sunbiz:N92000000500:business_registration_address:mailing",
        source_system: "SUNBIZ",
        document_number: "N92000000500",
        address_role: "MAILING",
        matched_zip_prefixes: [],
      },
    ]);
    expect(bundle.businessRegistrationParties).toHaveLength(2);
    expect(bundle.businessRegistrationParties).toContainEqual(
      expect.objectContaining({
        party_role: "REGISTERED_AGENT",
        name: "CORPORATION INFORMATION SERVICES INC.",
        party_type_code: "C",
        matched_zip_prefixes: [],
      }),
    );
    expect(bundle.businessRegistrationParties).toContainEqual(
      expect.objectContaining({
        party_role: "OFFICER",
        name: "JOHNSON LEBERT",
        title: "D",
        officer_ordinal: 1,
        matched_zip_prefixes: ["33916"],
      }),
    );
    expect(bundle.addresses).toContainEqual(
      expect.objectContaining({
        city_name: "FT MYERS",
        postal_code: "33916",
        state_code: "FL",
        unnormalized_address: "3123 LAFAYETTE ST. FT. MYERS FL 33916",
      }),
    );
    expect(
      bundle.relationships
        .map((relationship) => relationship.relationship_type)
        .sort(),
    ).toEqual([
      "business_registration_address_has_address",
      "business_registration_address_has_address",
      "business_registration_has_address",
      "business_registration_has_address",
      "business_registration_has_party",
      "business_registration_has_party",
      "business_registration_party_has_address",
      "business_registration_party_has_address",
      "company_has_business_registration",
    ]);
  });

  it("normalizes address fields and keeps stable address identifiers deterministic", () => {
    const address = buildAddress({
      line1: "4312 NW 27TH LN",
      city: "CAPE CORAL",
      state: "FL",
      zip: "33993-1234",
    });

    expect(toLexiconAddress(address)).toMatchObject({
      request_identifier: stableAddressIdentifier(address),
      city_name: "CAPE CORAL",
      postal_code: "33993",
      plus_four_postal_code: "1234",
      state_code: "FL",
      unnormalized_address: "4312 NW 27TH LN CAPE CORAL FL 33993-1234",
    });
  });

  it("collects matched ZIP prefixes by role and officer ordinal", () => {
    const address = buildAddress({
      line1: "1 MAIN ST",
      city: "FORT MYERS",
      state: "FL",
      zip: "33901",
    });
    const matches = [
      {
        role: "officerAddress",
        matchedZipPrefix: "33901",
        zip: "33901",
        officerOrdinal: 1,
        officerTitle: "MGR",
        officerName: "FIRST OFFICER",
        address,
      },
      {
        role: "officerAddress",
        matchedZipPrefix: "33902",
        zip: "33902",
        officerOrdinal: 2,
        officerTitle: "MGR",
        officerName: "SECOND OFFICER",
        address,
      },
      {
        role: "registeredAgentAddress",
        matchedZipPrefix: "33901",
        zip: "33901",
        officerOrdinal: null,
        officerTitle: null,
        officerName: null,
        address,
      },
    ];

    expect(collectMatchedZipPrefixes(matches, "officerAddress", 1)).toEqual([
      "33901",
    ]);
    expect(
      collectMatchedZipPrefixes(matches, "registeredAgentAddress", null),
    ).toEqual(["33901"]);
  });

  it("parses S3 URIs", () => {
    expect(parseS3Uri("s3://bucket-name/path/to/object.json")).toEqual({
      bucket: "bucket-name",
      key: "path/to/object.json",
    });
  });
});
