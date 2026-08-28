/**
 * @typedef {object} CorporateFixtureFields
 * @property {string} documentNumber - Florida corporate document number.
 * @property {string} entityName - Corporate legal name.
 * @property {string} principalAddress - Principal street address.
 * @property {string} principalCity - Principal city.
 * @property {string} principalState - Principal state code.
 * @property {string} principalZip - Principal ZIP or ZIP+4.
 * @property {string | undefined} [registeredAgentName] - Optional registered-agent name.
 * @property {string | undefined} [registeredAgentAddress] - Optional registered-agent street address.
 * @property {string | undefined} [registeredAgentCity] - Optional registered-agent city.
 * @property {string | undefined} [registeredAgentState] - Optional registered-agent state code.
 * @property {string | undefined} [registeredAgentZip] - Optional registered-agent ZIP.
 */

/**
 * Write one value to a one-based fixed-width Sunbiz field.
 *
 * @param {string[]} buffer - Mutable 1,440-character fixture buffer.
 * @param {number} start - One-based Sunbiz field start.
 * @param {number} length - Fixed field width.
 * @param {string | undefined} value - Fixture value.
 * @returns {void}
 */
function writeField(buffer, start, length, value) {
  const text = String(value ?? "")
    .padEnd(length, " ")
    .slice(0, length);
  for (let index = 0; index < length; index += 1) {
    buffer[start - 1 + index] = text[index] ?? " ";
  }
}

/**
 * Build a small parser-faithful corporate row for local extraction tests.
 *
 * The fixture writes only fields needed by these tests. Production parsing is
 * always performed by the shared permit-harvest Sunbiz parser.
 *
 * @param {CorporateFixtureFields} fields - Corporate and address fixture values.
 * @returns {string} One 1,440-character Sunbiz fixed-width row.
 */
export function buildCorporateFixtureLine(fields) {
  const buffer = Array.from({ length: 1440 }, () => " ");
  writeField(buffer, 1, 12, fields.documentNumber);
  writeField(buffer, 13, 192, fields.entityName);
  writeField(buffer, 205, 1, "A");
  writeField(buffer, 206, 15, "DOMP");
  writeField(buffer, 221, 42, fields.principalAddress);
  writeField(buffer, 305, 28, fields.principalCity);
  writeField(buffer, 333, 2, fields.principalState);
  writeField(buffer, 335, 10, fields.principalZip);
  writeField(buffer, 473, 8, "20260102");
  writeField(buffer, 545, 42, fields.registeredAgentName);
  writeField(buffer, 587, 1, fields.registeredAgentName ? "P" : undefined);
  writeField(buffer, 588, 42, fields.registeredAgentAddress);
  writeField(buffer, 630, 28, fields.registeredAgentCity);
  writeField(buffer, 658, 2, fields.registeredAgentState);
  writeField(buffer, 660, 9, fields.registeredAgentZip);
  return buffer.join("");
}

/**
 * Build the bundled five-row Broward reconciliation sample.
 *
 * @returns {string[]} Three candidate rows, one non-candidate row, and one invalid blank row.
 */
export function buildBrowardReconciliationSampleLines() {
  return [
    buildCorporateFixtureLine({
      documentNumber: "P26000000001",
      entityName: "VERIFIED BROWARD SAMPLE INC.",
      principalAddress: "100 EAST LAS OLAS BOULEVARD",
      principalCity: "FORT LAUDERDALE",
      principalState: "FL",
      principalZip: "33301",
      registeredAgentName: "UNRESOLVED AGENT",
      registeredAgentAddress: "200 CROSS COUNTY ROAD",
      registeredAgentCity: "FORT LAUDERDALE",
      registeredAgentState: "FL",
      registeredAgentZip: "33388",
    }),
    buildCorporateFixtureLine({
      documentNumber: "P26000000002",
      entityName: "OUTSIDE CROSS-BOUNDARY SAMPLE INC.",
      principalAddress: "300 NORTHWEST 2 AVENUE",
      principalCity: "MIAMI",
      principalState: "FL",
      principalZip: "33023",
    }),
    buildCorporateFixtureLine({
      documentNumber: "P26000000003",
      entityName: "UNRESOLVED CROSS-BOUNDARY SAMPLE INC.",
      principalAddress: "400 SOUTH DIXIE HIGHWAY",
      principalCity: "HALLANDALE BEACH",
      principalState: "FL",
      principalZip: "33009",
    }),
    buildCorporateFixtureLine({
      documentNumber: "P26000000004",
      entityName: "NON-CANDIDATE SAMPLE INC.",
      principalAddress: "500 BRICKELL AVENUE",
      principalCity: "MIAMI",
      principalState: "FL",
      principalZip: "33131",
    }),
    " ".repeat(1440),
  ];
}
