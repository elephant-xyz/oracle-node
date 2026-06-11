import { describe, expect, it } from "vitest";
import {
  extractCorporateDataLinesByZip,
  findZipMatchedAddresses,
  matchCorporateDataLines,
  normalizeAddressForMatch,
  normalizeZipPrefixes,
  parseCorporateDataRecord,
  parseSunbizDate,
} from "../../../../workflow/lambdas/permit-harvest-worker/sunbiz-corporate.mjs";

/**
 * @typedef {object} OfficerFixture
 * @property {string} title - Officer title.
 * @property {string} type - Officer type.
 * @property {string} name - Officer name.
 * @property {string} address - Officer street address.
 * @property {string} city - Officer city.
 * @property {string} state - Officer state.
 * @property {string} zip - Officer ZIP.
 */

/**
 * Write a value into a fixed-width character buffer.
 *
 * @param {string[]} buffer - Fixed-width character buffer.
 * @param {number} start - One-based start position.
 * @param {number} length - Fixed field length.
 * @param {string | undefined} value - Field value.
 * @returns {void}
 */
function writeField(buffer, start, length, value) {
  const text = String(value ?? "").padEnd(length, " ").slice(0, length);
  for (let index = 0; index < length; index += 1) {
    buffer[start - 1 + index] = text[index];
  }
}

/**
 * Build a Sunbiz corporate fixed-width row fixture.
 *
 * @param {object} fields - Field values keyed by test-friendly names.
 * @param {string} fields.documentNumber - Corporate document number.
 * @param {string} fields.entityName - Entity name.
 * @param {string} fields.statusCode - Status code.
 * @param {string} fields.filingTypeCode - Filing type code.
 * @param {string} fields.address1 - Principal street address.
 * @param {string} fields.city - Principal city.
 * @param {string} fields.state - Principal state.
 * @param {string} fields.zip - Principal ZIP.
 * @param {string} fields.filedDate - Filed date in YYYYMMDD format.
 * @param {string} fields.feiNumber - FEI number.
 * @param {string} fields.registeredAgentName - Registered agent name.
 * @param {string} fields.registeredAgentAddress - Registered agent street address.
 * @param {string} fields.registeredAgentCity - Registered agent city.
 * @param {string} fields.registeredAgentState - Registered agent state.
 * @param {string} fields.registeredAgentZip - Registered agent ZIP.
 * @param {OfficerFixture[]} officers - Officer fixtures.
 * @returns {string} Fixed-width line.
 */
function buildCorporateLine(fields, officers = []) {
  const buffer = Array.from({ length: 1440 }, () => " ");
  writeField(buffer, 1, 12, fields.documentNumber);
  writeField(buffer, 13, 192, fields.entityName);
  writeField(buffer, 205, 1, fields.statusCode);
  writeField(buffer, 206, 15, fields.filingTypeCode);
  writeField(buffer, 221, 42, fields.address1);
  writeField(buffer, 305, 28, fields.city);
  writeField(buffer, 333, 2, fields.state);
  writeField(buffer, 335, 10, fields.zip);
  writeField(buffer, 473, 8, fields.filedDate);
  writeField(buffer, 481, 14, fields.feiNumber);
  writeField(buffer, 545, 42, fields.registeredAgentName);
  writeField(buffer, 587, 1, "P");
  writeField(buffer, 588, 42, fields.registeredAgentAddress);
  writeField(buffer, 630, 28, fields.registeredAgentCity);
  writeField(buffer, 658, 2, fields.registeredAgentState);
  writeField(buffer, 660, 9, fields.registeredAgentZip);

  const officerOffsets = [
    { title: 669, type: 673, name: 674, address: 716, city: 758, state: 786, zip: 788 },
    { title: 797, type: 801, name: 802, address: 844, city: 886, state: 914, zip: 916 },
  ];
  for (const [index, officer] of officers.entries()) {
    const offsets = officerOffsets[index];
    if (!offsets) continue;
    writeField(buffer, offsets.title, 4, officer.title);
    writeField(buffer, offsets.type, 1, officer.type);
    writeField(buffer, offsets.name, 42, officer.name);
    writeField(buffer, offsets.address, 42, officer.address);
    writeField(buffer, offsets.city, 28, officer.city);
    writeField(buffer, offsets.state, 2, officer.state);
    writeField(buffer, offsets.zip, 9, officer.zip);
  }

  return buffer.join("");
}

describe("sunbiz corporate bulk helpers", () => {
  it("normalizes dates and address text", () => {
    expect(parseSunbizDate("19400124")).toBe("1940-01-24");
    expect(parseSunbizDate("20240231")).toBeNull();
    expect(normalizeAddressForMatch("4980 Bayline Drive, North Fort Myers FL")).toBe(
      "4980 BAYLINE DR N FORT MYERS FL",
    );
  });

  it("parses fixed-width corporate records with addresses, agent, and officers", () => {
    const line = buildCorporateLine(
      {
        documentNumber: "790346",
        entityName: "LEE COUNTY ELECTRIC COOPERATIVE, INC.",
        statusCode: "A",
        filingTypeCode: "DOMNP",
        address1: "4980 BAYLINE DRIVE",
        city: "NORTH FORT MYERS",
        state: "FL",
        zip: "33917",
        filedDate: "19400124",
        feiNumber: "590329555",
        registeredAgentName: "AKIN, RICHARD",
        registeredAgentAddress: "1715 MONROE STREET",
        registeredAgentCity: "FORT MYERS",
        registeredAgentState: "FL",
        registeredAgentZip: "33901",
      },
      [
        {
          title: "P",
          type: "P",
          name: "POWELL, MICHAEL",
          address: "4980 BAYLINE DRIVE",
          city: "NORTH FORT MYERS",
          state: "FL",
          zip: "33917",
        },
      ],
    );

    expect(parseCorporateDataRecord(line)).toMatchObject({
      documentNumber: "790346",
      entityName: "LEE COUNTY ELECTRIC COOPERATIVE, INC.",
      status: "ACTIVE",
      filingType: "Domestic Non-Profit",
      filedDate: "1940-01-24",
      feiNumber: "590329555",
      principalAddress: {
        singleLine: "4980 BAYLINE DRIVE NORTH FORT MYERS FL 33917",
        normalized: "4980 BAYLINE DR N FORT MYERS FL 33917",
      },
      registeredAgent: {
        name: "AKIN, RICHARD",
        address: {
          singleLine: "1715 MONROE STREET FORT MYERS FL 33901",
        },
      },
      officers: [
        {
          ordinal: 1,
          title: "P",
          name: "POWELL, MICHAEL",
          address: {
            normalized: "4980 BAYLINE DR N FORT MYERS FL 33917",
          },
        },
      ],
    });
  });

  it("matches report addresses against principal, registered-agent, and officer addresses", () => {
    const leeElectric = buildCorporateLine(
      {
        documentNumber: "790346",
        entityName: "LEE COUNTY ELECTRIC COOPERATIVE, INC.",
        statusCode: "A",
        filingTypeCode: "DOMNP",
        address1: "4980 BAYLINE DRIVE",
        city: "NORTH FORT MYERS",
        state: "FL",
        zip: "33917",
        filedDate: "19400124",
        feiNumber: "590329555",
        registeredAgentName: "AKIN, RICHARD",
        registeredAgentAddress: "1715 MONROE STREET",
        registeredAgentCity: "FORT MYERS",
        registeredAgentState: "FL",
        registeredAgentZip: "33901",
      },
      [
        {
          title: "P",
          type: "P",
          name: "POWELL, MICHAEL",
          address: "4980 BAYLINE DRIVE",
          city: "NORTH FORT MYERS",
          state: "FL",
          zip: "33917",
        },
      ],
    );
    const summerlin = buildCorporateLine(
      {
        documentNumber: "L04000013844",
        entityName: "GULF COAST CARDIOTHORACIC SURGEONS, P.L.",
        statusCode: "A",
        filingTypeCode: "FLAL",
        address1: "8010 SUMMERLIN LAKES DRIVE",
        city: "FORT MYERS",
        state: "FL",
        zip: "33907",
        filedDate: "20040218",
        feiNumber: "200000000",
        registeredAgentName: "REGISTERED AGENT",
        registeredAgentAddress: "8010 SUMMERLIN LAKES DRIVE",
        registeredAgentCity: "FORT MYERS",
        registeredAgentState: "FL",
        registeredAgentZip: "33907",
      },
      [],
    );

    const summary = matchCorporateDataLines({
      lines: [leeElectric, summerlin],
      sourceFileName: "sample-cor.txt",
      maxMatchesPerAddress: 10,
      addressInputs: [
        {
          inputId: "bayline",
          rawAddress: "4980 Bayline",
          city: "North Fort Myers",
          state: "FL",
        },
        {
          inputId: "monroe-agent",
          rawAddress: "1715 Monroe St Fort Myers FL 33901",
        },
        {
          inputId: "summerlin",
          rawAddress: "8010 Summerlin Lakes Dr",
          zip: "33907",
        },
      ],
    });

    expect(summary).toMatchObject({
      sourceRecordsRead: 2,
      invalidRecordCount: 0,
      totalMatchCount: 3,
      addressResults: [
        {
          inputId: "bayline",
          matchCount: 1,
          matches: [
            {
              entity: { documentNumber: "790346" },
              matchedAddresses: [
                { role: "principalAddress" },
                { role: "officerAddress", officerName: "POWELL, MICHAEL" },
              ],
            },
          ],
        },
        {
          inputId: "monroe-agent",
          matchCount: 1,
          matches: [
            {
              entity: { documentNumber: "790346" },
              matchedAddresses: [{ role: "registeredAgentAddress" }],
            },
          ],
        },
        {
          inputId: "summerlin",
          matchCount: 1,
          matches: [
            {
              entity: { documentNumber: "L04000013844" },
              matchedAddresses: [
                { role: "principalAddress" },
                { role: "registeredAgentAddress" },
              ],
            },
          ],
        },
      ],
    });
  });

  it("extracts corporate records by ZIP-bearing address fields into bounded chunks", async () => {
    const leeElectric = buildCorporateLine(
      {
        documentNumber: "790346",
        entityName: "LEE COUNTY ELECTRIC COOPERATIVE, INC.",
        statusCode: "A",
        filingTypeCode: "DOMNP",
        address1: "4980 BAYLINE DRIVE",
        city: "NORTH FORT MYERS",
        state: "FL",
        zip: "33917-3901",
        filedDate: "19400124",
        feiNumber: "590329555",
        registeredAgentName: "AKIN, RICHARD",
        registeredAgentAddress: "1715 MONROE STREET",
        registeredAgentCity: "FORT MYERS",
        registeredAgentState: "FL",
        registeredAgentZip: "33901",
      },
      [
        {
          title: "P",
          type: "P",
          name: "POWELL, MICHAEL",
          address: "4980 BAYLINE DRIVE",
          city: "NORTH FORT MYERS",
          state: "FL",
          zip: "33917",
        },
      ],
    );
    const summerlin = buildCorporateLine(
      {
        documentNumber: "L04000013844",
        entityName: "GULF COAST CARDIOTHORACIC SURGEONS, P.L.",
        statusCode: "A",
        filingTypeCode: "FLAL",
        address1: "8010 SUMMERLIN LAKES DRIVE",
        city: "FORT MYERS",
        state: "FL",
        zip: "33907",
        filedDate: "20040218",
        feiNumber: "200000000",
        registeredAgentName: "REGISTERED AGENT",
        registeredAgentAddress: "8010 SUMMERLIN LAKES DRIVE",
        registeredAgentCity: "FORT MYERS",
        registeredAgentState: "FL",
        registeredAgentZip: "33907",
      },
      [],
    );
    const miami = buildCorporateLine(
      {
        documentNumber: "P26000027280",
        entityName: "PULIDO CONTRACTORS ROOFING CORP.",
        statusCode: "A",
        filingTypeCode: "DOMP",
        address1: "2823 NW 21 COURT",
        city: "MIAMI",
        state: "FL",
        zip: "33142",
        filedDate: "20260522",
        feiNumber: "000000000",
        registeredAgentName: "REGISTERED AGENT",
        registeredAgentAddress: "2823 NW 21 COURT",
        registeredAgentCity: "MIAMI",
        registeredAgentState: "FL",
        registeredAgentZip: "33142",
      },
      [],
    );
    /** @type {import("../../../../workflow/lambdas/permit-harvest-worker/sunbiz-corporate.mjs").SunbizZipExtractionChunk[]} */
    const emittedChunks = [];

    const summary = await extractCorporateDataLinesByZip({
      lines: [leeElectric, summerlin, miami],
      zipPrefixes: ["33917", "33907", "33917-0000"],
      chunkRecordLimit: 1,
      maxRecords: undefined,
      sourceFileName: "sample-cor.txt",
      onChunk: async (chunk) => {
        emittedChunks.push(chunk);
        return {
          chunkIndex: chunk.chunkIndex,
          recordCount: chunk.recordCount,
          uri: `memory://chunk-${chunk.chunkIndex}`,
        };
      },
    });

    expect(normalizeZipPrefixes(["33917", "33917-0000", "33907"])).toEqual([
      "33917",
      "33907",
    ]);
    expect(summary).toMatchObject({
      sourceRecordsRead: 3,
      invalidRecordCount: 0,
      matchedRecordCount: 2,
      chunkRecordLimit: 1,
      chunks: [
        { chunkIndex: 0, recordCount: 1, uri: "memory://chunk-0" },
        { chunkIndex: 1, recordCount: 1, uri: "memory://chunk-1" },
      ],
    });
    expect(emittedChunks).toHaveLength(2);
    expect(emittedChunks[0]?.records[0]).toMatchObject({
      entity: { documentNumber: "790346" },
      matchedAddresses: [
        { role: "principalAddress", matchedZipPrefix: "33917" },
        { role: "officerAddress", matchedZipPrefix: "33917" },
      ],
    });
    expect(emittedChunks[1]?.records[0]).toMatchObject({
      entity: { documentNumber: "L04000013844" },
      matchedAddresses: [
        { role: "principalAddress", matchedZipPrefix: "33907" },
        { role: "registeredAgentAddress", matchedZipPrefix: "33907" },
      ],
    });

    const record = parseCorporateDataRecord(leeElectric);
    expect(record).not.toBeNull();
    expect(findZipMatchedAddresses(record, ["33901"])).toMatchObject([
      { role: "registeredAgentAddress", zip: "33901" },
    ]);
  });
});
