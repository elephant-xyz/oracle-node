import { GetObjectCommand } from "@aws-sdk/client-s3";
import crypto from "crypto";
import { createReadStream, createWriteStream } from "fs";
import { mkdir, rm, writeFile } from "fs/promises";
import { tmpdir } from "os";
import path from "path";
import readline from "readline";
import { createRequire } from "module";
import { pipeline } from "stream/promises";

const require = createRequire(import.meta.url);
/** @type {typeof import("yauzl")} */
const yauzl = require("yauzl");

/**
 * @typedef {object} Logger
 * @property {(message: string, details?: Record<string, unknown>) => void} info - Emit an informational message.
 * @property {(message: string, details?: Record<string, unknown>) => void} warn - Emit a warning message.
 * @property {(message: string, details?: Record<string, unknown>) => void} error - Emit an error message.
 */

/**
 * @typedef {object} S3UriParts
 * @property {string} bucket - S3 bucket name.
 * @property {string} key - S3 object key.
 */

/**
 * @typedef {object} SunbizAddress
 * @property {string | null} line1 - First street address line.
 * @property {string | null} line2 - Second street address line.
 * @property {string | null} city - City.
 * @property {string | null} state - State abbreviation.
 * @property {string | null} zip - ZIP or ZIP+4.
 * @property {string | null} country - Country code when present.
 * @property {string} singleLine - Collapsed single-line display address.
 * @property {string} normalized - Uppercase punctuation-normalized address used for matching.
 */

/**
 * @typedef {object} SunbizOfficer
 * @property {number} ordinal - One-based officer slot from the fixed-width file.
 * @property {string | null} title - Officer title.
 * @property {string | null} type - Officer type code, usually person or corporation.
 * @property {string | null} name - Officer name.
 * @property {SunbizAddress} address - Officer address.
 */

/**
 * @typedef {object} SunbizCorporateRecord
 * @property {string} schemaVersion - Internal parser schema version.
 * @property {string} source - Source system name.
 * @property {string} documentNumber - Florida document/corporation number.
 * @property {string | null} entityName - Entity legal name.
 * @property {string | null} statusCode - Sunbiz status code.
 * @property {string | null} status - Normalized status label.
 * @property {string | null} filingTypeCode - Sunbiz filing type code.
 * @property {string | null} filingType - Human-readable filing type.
 * @property {SunbizAddress} principalAddress - Principal address of entity.
 * @property {SunbizAddress} mailingAddress - Mailing address of entity.
 * @property {string | null} filedDate - Formation filing date as YYYY-MM-DD when parseable.
 * @property {string | null} feiNumber - Federal Employer Identification Number.
 * @property {boolean} moreThanSixOfficers - True when Sunbiz indicates more officers exist than fit in the fixed-width row.
 * @property {string | null} lastTransactionDate - Last filing date as YYYY-MM-DD when parseable.
 * @property {string | null} stateCountry - State/country code from Sunbiz.
 * @property {Array<{ year: string | null, date: string | null }>} annualReports - Up to three annual report years and dates.
 * @property {{ name: string | null, type: string | null, address: SunbizAddress }} registeredAgent - Registered agent data.
 * @property {SunbizOfficer[]} officers - Up to six officers/directors from the bulk file.
 * @property {number} rawRecordLength - Raw fixed-width line length.
 */

/**
 * @typedef {object} SunbizAddressInput
 * @property {string | undefined} [inputId] - Stable caller-provided identifier for this address.
 * @property {string | undefined} [source] - Optional provenance label, such as a permit record number or S3 key.
 * @property {string} rawAddress - Address text to match against Sunbiz address-bearing fields.
 * @property {string | undefined} [city] - Optional city filter.
 * @property {string | undefined} [state] - Optional state filter.
 * @property {string | undefined} [zip] - Optional ZIP filter.
 */

/**
 * @typedef {object} NormalizedSunbizAddressInput
 * @property {string} inputId - Stable identifier for this address.
 * @property {SunbizAddressInput} original - Original caller-provided address input.
 * @property {string} normalizedQuery - Normalized address query.
 * @property {string[]} queryTokens - Normalized address tokens.
 * @property {string | null} city - Normalized city filter.
 * @property {string | null} state - Normalized state filter.
 * @property {string | null} zip - Normalized ZIP filter.
 */

/**
 * @typedef {object} SunbizMatchedAddress
 * @property {"principalAddress" | "mailingAddress" | "registeredAgentAddress" | "officerAddress"} role - Entity address role that matched.
 * @property {number | null} officerOrdinal - Officer slot when role is officerAddress.
 * @property {string | null} officerTitle - Officer title when role is officerAddress.
 * @property {string | null} officerName - Officer name when role is officerAddress.
 * @property {SunbizAddress} address - Matched Sunbiz address.
 */

/**
 * @typedef {object} SunbizAddressMatch
 * @property {string} sourceFileName - Corporate data source file or zip entry name.
 * @property {number} sourceLineNumber - One-based line number within the source file.
 * @property {SunbizCorporateRecord} entity - Parsed Sunbiz entity record.
 * @property {SunbizMatchedAddress[]} matchedAddresses - Address-bearing fields that matched the input address.
 */

/**
 * @typedef {object} SunbizAddressMatchResult
 * @property {SunbizAddressInput} input - Original input address.
 * @property {string} inputId - Stable input identifier.
 * @property {string} normalizedQuery - Normalized query used for matching.
 * @property {number} matchCount - Number of entity matches returned for this input.
 * @property {boolean} truncated - True when maxMatchesPerAddress stopped collection for this input.
 * @property {SunbizAddressMatch[]} matches - Matching entities.
 */

/**
 * @typedef {object} SunbizCorporateAddressMatchSummary
 * @property {string} schemaVersion - Internal output schema version.
 * @property {string} source - Source system name.
 * @property {string} sourceDataS3Uri - S3 object read by the matcher.
 * @property {"text" | "zip"} sourceFormat - Source object format.
 * @property {string} retrievedAt - ISO timestamp for the extraction run.
 * @property {number} addressInputCount - Number of address inputs.
 * @property {number} sourceRecordsRead - Number of fixed-width rows read.
 * @property {number} invalidRecordCount - Number of rows skipped because they lacked a document number.
 * @property {number} totalMatchCount - Total entity matches across all inputs.
 * @property {number} maxMatchesPerAddress - Per-input match cap.
 * @property {SunbizAddressMatchResult[]} addressResults - Per-input match results.
 * @property {{ mappedToCurrentLexicon: boolean, notes: string }} lexiconStatus - Lexicon mapping status and notes.
 */

/**
 * @typedef {object} SunbizZipMatchedAddress
 * @property {"principalAddress" | "mailingAddress" | "registeredAgentAddress" | "officerAddress"} role - Entity address role whose ZIP matched.
 * @property {string} matchedZipPrefix - Caller-provided ZIP prefix that matched the address.
 * @property {string} zip - Normalized ZIP digits from the Sunbiz address field.
 * @property {number | null} officerOrdinal - Officer slot when role is officerAddress.
 * @property {string | null} officerTitle - Officer title when role is officerAddress.
 * @property {string | null} officerName - Officer name when role is officerAddress.
 * @property {SunbizAddress} address - Matched Sunbiz address.
 */

/**
 * @typedef {object} SunbizZipExtractedRecord
 * @property {string} sourceFileName - Corporate data source file or ZIP entry name.
 * @property {number} sourceLineNumber - One-based line number encountered by the extractor.
 * @property {SunbizCorporateRecord} entity - Parsed Sunbiz entity record.
 * @property {SunbizZipMatchedAddress[]} matchedAddresses - Address-bearing fields with matching ZIP prefixes.
 */

/**
 * @typedef {object} SunbizZipExtractionChunk
 * @property {number} chunkIndex - Zero-based chunk index.
 * @property {number} recordCount - Number of records in this chunk.
 * @property {SunbizZipExtractedRecord[]} records - Extracted Sunbiz records for this chunk.
 */

/**
 * @typedef {object} SunbizZipExtractionChunkReceipt
 * @property {number} chunkIndex - Zero-based chunk index.
 * @property {number} recordCount - Number of records written for this chunk.
 * @property {string} uri - Output URI for this chunk.
 */

/**
 * @typedef {object} SunbizCorporateZipExtractionSummary
 * @property {string} schemaVersion - Internal output schema version.
 * @property {string} source - Source system name.
 * @property {string} sourceDataS3Uri - Source object URI or local-lines marker.
 * @property {"text" | "zip"} sourceFormat - Source object format.
 * @property {string} retrievedAt - ISO timestamp for the extraction run.
 * @property {string[]} zipPrefixes - Normalized ZIP prefixes used for extraction.
 * @property {number} sourceRecordsRead - Number of fixed-width rows read.
 * @property {number} invalidRecordCount - Number of rows skipped because they lacked a document number.
 * @property {number} matchedRecordCount - Number of entity records matched by at least one ZIP-bearing address.
 * @property {number} chunkRecordLimit - Maximum records per emitted chunk.
 * @property {number | null} maxRecords - Optional cap on matched records for smoke runs.
 * @property {boolean} stoppedAfterMaxRecords - True when maxRecords stopped processing early.
 * @property {SunbizZipExtractionChunkReceipt[]} chunks - Output chunk receipts.
 * @property {{ mappedToCurrentLexicon: boolean, notes: string }} lexiconStatus - Lexicon mapping status and notes.
 */

/**
 * @typedef {object} CorporateFieldDefinition
 * @property {keyof CorporateFieldValues} key - Internal field key.
 * @property {number} start - One-based start position from the Sunbiz definition.
 * @property {number} length - Fixed field length.
 */

/**
 * @typedef {object} CorporateFieldValues
 * @property {string} documentNumber
 * @property {string} entityName
 * @property {string} statusCode
 * @property {string} filingTypeCode
 * @property {string} address1
 * @property {string} address2
 * @property {string} city
 * @property {string} state
 * @property {string} zip
 * @property {string} country
 * @property {string} mailAddress1
 * @property {string} mailAddress2
 * @property {string} mailCity
 * @property {string} mailState
 * @property {string} mailZip
 * @property {string} mailCountry
 * @property {string} filedDate
 * @property {string} feiNumber
 * @property {string} moreThanSixOfficersFlag
 * @property {string} lastTransactionDate
 * @property {string} stateCountry
 * @property {string} reportYear1
 * @property {string} reportDate1
 * @property {string} reportYear2
 * @property {string} reportDate2
 * @property {string} reportYear3
 * @property {string} reportDate3
 * @property {string} registeredAgentName
 * @property {string} registeredAgentType
 * @property {string} registeredAgentAddress
 * @property {string} registeredAgentCity
 * @property {string} registeredAgentState
 * @property {string} registeredAgentZip
 */

const CORPORATE_RECORD_SCHEMA_VERSION = "permit-harvest.sunbiz-corporate.v1";
const CORPORATE_ADDRESS_MATCH_SCHEMA_VERSION =
  "permit-harvest.sunbiz-corporate-address-match.v1";
const CORPORATE_ZIP_EXTRACTION_SCHEMA_VERSION =
  "permit-harvest.sunbiz-corporate-zip-extract.v1";

/** @type {CorporateFieldDefinition[]} */
const CORPORATE_FIELDS = [
  { key: "documentNumber", start: 1, length: 12 },
  { key: "entityName", start: 13, length: 192 },
  { key: "statusCode", start: 205, length: 1 },
  { key: "filingTypeCode", start: 206, length: 15 },
  { key: "address1", start: 221, length: 42 },
  { key: "address2", start: 263, length: 42 },
  { key: "city", start: 305, length: 28 },
  { key: "state", start: 333, length: 2 },
  { key: "zip", start: 335, length: 10 },
  { key: "country", start: 345, length: 2 },
  { key: "mailAddress1", start: 347, length: 42 },
  { key: "mailAddress2", start: 389, length: 42 },
  { key: "mailCity", start: 431, length: 28 },
  { key: "mailState", start: 459, length: 2 },
  { key: "mailZip", start: 461, length: 10 },
  { key: "mailCountry", start: 471, length: 2 },
  { key: "filedDate", start: 473, length: 8 },
  { key: "feiNumber", start: 481, length: 14 },
  { key: "moreThanSixOfficersFlag", start: 495, length: 1 },
  { key: "lastTransactionDate", start: 496, length: 8 },
  { key: "stateCountry", start: 504, length: 2 },
  { key: "reportYear1", start: 506, length: 4 },
  { key: "reportDate1", start: 511, length: 8 },
  { key: "reportYear2", start: 519, length: 4 },
  { key: "reportDate2", start: 524, length: 8 },
  { key: "reportYear3", start: 532, length: 4 },
  { key: "reportDate3", start: 537, length: 8 },
  { key: "registeredAgentName", start: 545, length: 42 },
  { key: "registeredAgentType", start: 587, length: 1 },
  { key: "registeredAgentAddress", start: 588, length: 42 },
  { key: "registeredAgentCity", start: 630, length: 28 },
  { key: "registeredAgentState", start: 658, length: 2 },
  { key: "registeredAgentZip", start: 660, length: 9 },
];

/**
 * @typedef {object} OfficerFieldOffsets
 * @property {number} title - One-based title start position.
 * @property {number} type - One-based type start position.
 * @property {number} name - One-based name start position.
 * @property {number} address - One-based address start position.
 * @property {number} city - One-based city start position.
 * @property {number} state - One-based state start position.
 * @property {number} zip - One-based ZIP start position.
 */

/** @type {OfficerFieldOffsets[]} */
const OFFICER_FIELD_OFFSETS = [
  {
    title: 669,
    type: 673,
    name: 674,
    address: 716,
    city: 758,
    state: 786,
    zip: 788,
  },
  {
    title: 797,
    type: 801,
    name: 802,
    address: 844,
    city: 886,
    state: 914,
    zip: 916,
  },
  {
    title: 925,
    type: 929,
    name: 930,
    address: 972,
    city: 1014,
    state: 1042,
    zip: 1044,
  },
  {
    title: 1053,
    type: 1057,
    name: 1058,
    address: 1100,
    city: 1142,
    state: 1170,
    zip: 1172,
  },
  {
    title: 1181,
    type: 1185,
    name: 1186,
    address: 1228,
    city: 1270,
    state: 1298,
    zip: 1300,
  },
  {
    title: 1309,
    type: 1313,
    name: 1314,
    address: 1356,
    city: 1398,
    state: 1426,
    zip: 1428,
  },
];

const STATUS_LABELS = new Map([
  ["A", "ACTIVE"],
  ["I", "INACTIVE"],
]);

const FILING_TYPE_LABELS = new Map([
  ["DOMP", "Domestic Profit"],
  ["DOMNP", "Domestic Non-Profit"],
  ["FORP", "Foreign Profit"],
  ["FORNP", "Foreign Non-Profit"],
  ["DOMLP", "Domestic Limited Partnership"],
  ["FORLP", "Foreign Limited Partnership"],
  ["FLAL", "Florida Limited Liability Company"],
  ["FORL", "Foreign Limited Liability Company"],
  ["NPREG", "Non-Profit Registration"],
  ["TRUST", "Declaration of Trust"],
  ["AGENT", "Designation of Registered Agent"],
]);

/**
 * Collapse whitespace in text and return null for empty strings.
 *
 * @param {unknown} value - Raw value.
 * @returns {string | null} Collapsed text or null.
 */
function cleanText(value) {
  const text = String(value ?? "")
    .replace(/\s+/g, " ")
    .trim();
  return text ? text : null;
}

/**
 * Return a short stable hash for identifiers and temporary filenames.
 *
 * @param {string} value - Value to hash.
 * @returns {string} First twelve SHA-256 hex characters.
 */
export function shortHash(value) {
  return crypto.createHash("sha256").update(value).digest("hex").slice(0, 12);
}

/**
 * Convert arbitrary text into a lower-case S3-safe path segment.
 *
 * @param {string} value - Raw value to normalize.
 * @returns {string} S3-safe key part.
 */
export function safeKeyPart(value) {
  const normalized = value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return normalized || "unknown";
}

/**
 * Parse an S3 URI into bucket and key components.
 *
 * @param {string} uri - S3 URI in `s3://bucket/key` format.
 * @returns {S3UriParts} Parsed bucket and key.
 */
function parseS3Uri(uri) {
  const match = /^s3:\/\/([^/]+)\/(.+)$/i.exec(uri);
  if (!match) throw new Error(`Invalid S3 URI: ${uri}`);
  return { bucket: match[1], key: match[2] };
}

/**
 * Slice and trim a fixed-width field using Sunbiz one-based positions.
 *
 * @param {string} line - Fixed-width line.
 * @param {number} start - One-based start position.
 * @param {number} length - Fixed field length.
 * @returns {string} Trimmed field value.
 */
function readFixedField(line, start, length) {
  return line.slice(start - 1, start - 1 + length).trim();
}

/**
 * Parse a Sunbiz `YYYYMMDD` date into ISO format when possible.
 *
 * @param {string} value - Raw date field.
 * @returns {string | null} ISO date or null.
 */
export function parseSunbizDate(value) {
  const text = value.trim();
  if (!/^\d{8}$/.test(text)) return null;
  const year = Number(text.slice(0, 4));
  const month = Number(text.slice(4, 6));
  const day = Number(text.slice(6, 8));
  const date = new Date(Date.UTC(year, month - 1, day));
  if (
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() !== month - 1 ||
    date.getUTCDate() !== day
  ) {
    return null;
  }
  return date.toISOString().slice(0, 10);
}

/**
 * Normalize address text for broad Sunbiz address matching.
 *
 * @param {string | null | undefined} value - Address text.
 * @returns {string} Uppercase normalized text.
 */
export function normalizeAddressForMatch(value) {
  return String(value ?? "")
    .toUpperCase()
    .replace(/\b(STREET)\b/g, "ST")
    .replace(/\b(AVENUE)\b/g, "AVE")
    .replace(/\b(DRIVE)\b/g, "DR")
    .replace(/\b(ROAD)\b/g, "RD")
    .replace(/\b(BOULEVARD)\b/g, "BLVD")
    .replace(/\b(PARKWAY)\b/g, "PKWY")
    .replace(/\b(LANE)\b/g, "LN")
    .replace(/\b(COURT)\b/g, "CT")
    .replace(/\b(PLACE)\b/g, "PL")
    .replace(/\b(NORTH)\b/g, "N")
    .replace(/\b(SOUTH)\b/g, "S")
    .replace(/\b(EAST)\b/g, "E")
    .replace(/\b(WEST)\b/g, "W")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Build a normalized address object from fixed-width fields.
 *
 * @param {object} fields - Address field values.
 * @param {string | null} fields.line1 - Street line 1.
 * @param {string | null} fields.line2 - Street line 2.
 * @param {string | null} fields.city - City.
 * @param {string | null} fields.state - State.
 * @param {string | null} fields.zip - ZIP.
 * @param {string | null} fields.country - Country.
 * @returns {SunbizAddress} Normalized address.
 */
function buildAddress({ line1, line2, city, state, zip, country }) {
  const singleLine = [line1, line2, city, state, zip, country]
    .filter((item) => item && item.trim())
    .join(" ");
  return {
    line1,
    line2,
    city,
    state,
    zip,
    country,
    singleLine,
    normalized: normalizeAddressForMatch(singleLine),
  };
}

/**
 * Parse a Sunbiz fixed-width corporate data row.
 *
 * @param {string} line - Fixed-width corporate data row.
 * @returns {SunbizCorporateRecord | null} Parsed record, or null when no document number exists.
 */
export function parseCorporateDataRecord(line) {
  /** @type {CorporateFieldValues} */
  const fields = {
    documentNumber: "",
    entityName: "",
    statusCode: "",
    filingTypeCode: "",
    address1: "",
    address2: "",
    city: "",
    state: "",
    zip: "",
    country: "",
    mailAddress1: "",
    mailAddress2: "",
    mailCity: "",
    mailState: "",
    mailZip: "",
    mailCountry: "",
    filedDate: "",
    feiNumber: "",
    moreThanSixOfficersFlag: "",
    lastTransactionDate: "",
    stateCountry: "",
    reportYear1: "",
    reportDate1: "",
    reportYear2: "",
    reportDate2: "",
    reportYear3: "",
    reportDate3: "",
    registeredAgentName: "",
    registeredAgentType: "",
    registeredAgentAddress: "",
    registeredAgentCity: "",
    registeredAgentState: "",
    registeredAgentZip: "",
  };

  for (const definition of CORPORATE_FIELDS) {
    fields[definition.key] = readFixedField(
      line,
      definition.start,
      definition.length,
    );
  }

  const documentNumber = fields.documentNumber.trim();
  if (!documentNumber) return null;

  /** @type {SunbizOfficer[]} */
  const officers = [];
  for (const [index, offsets] of OFFICER_FIELD_OFFSETS.entries()) {
    const title = cleanText(readFixedField(line, offsets.title, 4));
    const type = cleanText(readFixedField(line, offsets.type, 1));
    const name = cleanText(readFixedField(line, offsets.name, 42));
    const address = buildAddress({
      line1: cleanText(readFixedField(line, offsets.address, 42)),
      line2: null,
      city: cleanText(readFixedField(line, offsets.city, 28)),
      state: cleanText(readFixedField(line, offsets.state, 2)),
      zip: cleanText(readFixedField(line, offsets.zip, 9)),
      country: null,
    });
    if (title || type || name || address.singleLine) {
      officers.push({ ordinal: index + 1, title, type, name, address });
    }
  }

  const statusCode = cleanText(fields.statusCode);
  const filingTypeCode = cleanText(fields.filingTypeCode);
  return {
    schemaVersion: CORPORATE_RECORD_SCHEMA_VERSION,
    source: "florida-sunbiz-corporate-bulk",
    documentNumber,
    entityName: cleanText(fields.entityName),
    statusCode,
    status: statusCode ? (STATUS_LABELS.get(statusCode) ?? statusCode) : null,
    filingTypeCode,
    filingType: filingTypeCode
      ? (FILING_TYPE_LABELS.get(filingTypeCode) ?? filingTypeCode)
      : null,
    principalAddress: buildAddress({
      line1: cleanText(fields.address1),
      line2: cleanText(fields.address2),
      city: cleanText(fields.city),
      state: cleanText(fields.state),
      zip: cleanText(fields.zip),
      country: cleanText(fields.country),
    }),
    mailingAddress: buildAddress({
      line1: cleanText(fields.mailAddress1),
      line2: cleanText(fields.mailAddress2),
      city: cleanText(fields.mailCity),
      state: cleanText(fields.mailState),
      zip: cleanText(fields.mailZip),
      country: cleanText(fields.mailCountry),
    }),
    filedDate: parseSunbizDate(fields.filedDate),
    feiNumber: cleanText(fields.feiNumber),
    moreThanSixOfficers: fields.moreThanSixOfficersFlag.toUpperCase() === "Y",
    lastTransactionDate: parseSunbizDate(fields.lastTransactionDate),
    stateCountry: cleanText(fields.stateCountry),
    annualReports: [
      {
        year: cleanText(fields.reportYear1),
        date: parseSunbizDate(fields.reportDate1),
      },
      {
        year: cleanText(fields.reportYear2),
        date: parseSunbizDate(fields.reportDate2),
      },
      {
        year: cleanText(fields.reportYear3),
        date: parseSunbizDate(fields.reportDate3),
      },
    ],
    registeredAgent: {
      name: cleanText(fields.registeredAgentName),
      type: cleanText(fields.registeredAgentType),
      address: buildAddress({
        line1: cleanText(fields.registeredAgentAddress),
        line2: null,
        city: cleanText(fields.registeredAgentCity),
        state: cleanText(fields.registeredAgentState),
        zip: cleanText(fields.registeredAgentZip),
        country: null,
      }),
    },
    officers,
    rawRecordLength: line.length,
  };
}

/**
 * Normalize and validate a caller-provided address input.
 *
 * @param {SunbizAddressInput} input - Address input.
 * @returns {NormalizedSunbizAddressInput} Normalized address input.
 */
function normalizeAddressInput(input) {
  const normalizedQuery = normalizeAddressForMatch(input.rawAddress);
  if (!normalizedQuery) {
    throw new Error("Sunbiz address input requires non-empty rawAddress");
  }
  const inputId = input.inputId?.trim() || shortHash(input.rawAddress);
  return {
    inputId,
    original: input,
    normalizedQuery,
    queryTokens: normalizedQuery.split(" ").filter(Boolean),
    city: input.city ? normalizeAddressForMatch(input.city) : null,
    state: input.state ? normalizeAddressForMatch(input.state) : null,
    zip: input.zip ? normalizeAddressForMatch(input.zip) : null,
  };
}

/**
 * Return true when all query tokens appear in order in the candidate text.
 *
 * @param {string[]} queryTokens - Query tokens.
 * @param {string[]} candidateTokens - Candidate tokens.
 * @returns {boolean} True when query tokens are present in order.
 */
function tokensAppearInOrder(queryTokens, candidateTokens) {
  let cursor = 0;
  for (const token of candidateTokens) {
    if (token === queryTokens[cursor]) cursor += 1;
    if (cursor === queryTokens.length) return true;
  }
  return queryTokens.length === 0;
}

/**
 * Determine whether one normalized input matches a Sunbiz address.
 *
 * @param {NormalizedSunbizAddressInput} input - Normalized input address.
 * @param {SunbizAddress} address - Candidate Sunbiz address.
 * @returns {boolean} True when the candidate address matches the input.
 */
export function isSunbizAddressMatch(input, address) {
  if (!address.normalized) return false;
  if (
    input.city &&
    !normalizeAddressForMatch(address.city).includes(input.city)
  ) {
    return false;
  }
  if (input.state && normalizeAddressForMatch(address.state) !== input.state) {
    return false;
  }
  if (input.zip) {
    const candidateZip = normalizeAddressForMatch(address.zip);
    if (!candidateZip.startsWith(input.zip.slice(0, 5))) return false;
  }
  if (address.normalized.includes(input.normalizedQuery)) return true;
  return tokensAppearInOrder(input.queryTokens, address.normalized.split(" "));
}

/**
 * Find all address-bearing fields on an entity that match an input address.
 *
 * @param {SunbizCorporateRecord} record - Parsed Sunbiz corporate record.
 * @param {NormalizedSunbizAddressInput} input - Normalized address input.
 * @returns {SunbizMatchedAddress[]} Matched addresses.
 */
function findMatchedAddresses(record, input) {
  /** @type {SunbizMatchedAddress[]} */
  const matches = [];
  if (isSunbizAddressMatch(input, record.principalAddress)) {
    matches.push({
      role: "principalAddress",
      officerOrdinal: null,
      officerTitle: null,
      officerName: null,
      address: record.principalAddress,
    });
  }
  if (isSunbizAddressMatch(input, record.mailingAddress)) {
    matches.push({
      role: "mailingAddress",
      officerOrdinal: null,
      officerTitle: null,
      officerName: null,
      address: record.mailingAddress,
    });
  }
  if (isSunbizAddressMatch(input, record.registeredAgent.address)) {
    matches.push({
      role: "registeredAgentAddress",
      officerOrdinal: null,
      officerTitle: null,
      officerName: record.registeredAgent.name,
      address: record.registeredAgent.address,
    });
  }
  for (const officer of record.officers) {
    if (isSunbizAddressMatch(input, officer.address)) {
      matches.push({
        role: "officerAddress",
        officerOrdinal: officer.ordinal,
        officerTitle: officer.title,
        officerName: officer.name,
        address: officer.address,
      });
    }
  }
  return matches;
}

/**
 * Normalize ZIP prefixes for broad ZIP-based Sunbiz extraction.
 *
 * @param {string[]} zipPrefixes - Raw caller-provided ZIP prefixes.
 * @returns {string[]} Unique digit-only ZIP prefixes.
 */
export function normalizeZipPrefixes(zipPrefixes) {
  const normalized = [
    ...new Set(
      zipPrefixes
        .map((zipPrefix) => zipPrefix.replace(/\D+/g, "").slice(0, 5))
        .filter(Boolean),
    ),
  ];
  if (normalized.length === 0) {
    throw new Error(
      "At least one ZIP prefix is required for Sunbiz extraction",
    );
  }
  return normalized;
}

/**
 * Normalize a ZIP value from a Sunbiz address field to digits.
 *
 * @param {string | null | undefined} zip - Raw ZIP or ZIP+4.
 * @returns {string} Digit-only ZIP value.
 */
function normalizeZipDigits(zip) {
  return String(zip ?? "").replace(/\D+/g, "");
}

/**
 * Return the matching ZIP prefix for an address, when any prefix matches.
 *
 * @param {SunbizAddress} address - Candidate address.
 * @param {string[]} zipPrefixes - Normalized ZIP prefixes.
 * @returns {{ zip: string, matchedZipPrefix: string } | null} Matched ZIP metadata.
 */
function getAddressZipMatch(address, zipPrefixes) {
  const zip = normalizeZipDigits(address.zip);
  if (!zip) return null;
  const matchedZipPrefix = zipPrefixes.find((zipPrefix) =>
    zip.startsWith(zipPrefix),
  );
  return matchedZipPrefix ? { zip, matchedZipPrefix } : null;
}

/**
 * Find all address-bearing fields on an entity whose ZIP matches a target prefix.
 *
 * @param {SunbizCorporateRecord} record - Parsed Sunbiz corporate record.
 * @param {string[]} zipPrefixes - Normalized ZIP prefixes.
 * @returns {SunbizZipMatchedAddress[]} Matched ZIP-bearing addresses.
 */
export function findZipMatchedAddresses(record, zipPrefixes) {
  /** @type {SunbizZipMatchedAddress[]} */
  const matches = [];

  const principalAddressMatch = getAddressZipMatch(
    record.principalAddress,
    zipPrefixes,
  );
  if (principalAddressMatch) {
    matches.push({
      role: "principalAddress",
      matchedZipPrefix: principalAddressMatch.matchedZipPrefix,
      zip: principalAddressMatch.zip,
      officerOrdinal: null,
      officerTitle: null,
      officerName: null,
      address: record.principalAddress,
    });
  }

  const mailingAddressMatch = getAddressZipMatch(
    record.mailingAddress,
    zipPrefixes,
  );
  if (mailingAddressMatch) {
    matches.push({
      role: "mailingAddress",
      matchedZipPrefix: mailingAddressMatch.matchedZipPrefix,
      zip: mailingAddressMatch.zip,
      officerOrdinal: null,
      officerTitle: null,
      officerName: null,
      address: record.mailingAddress,
    });
  }

  const registeredAgentAddressMatch = getAddressZipMatch(
    record.registeredAgent.address,
    zipPrefixes,
  );
  if (registeredAgentAddressMatch) {
    matches.push({
      role: "registeredAgentAddress",
      matchedZipPrefix: registeredAgentAddressMatch.matchedZipPrefix,
      zip: registeredAgentAddressMatch.zip,
      officerOrdinal: null,
      officerTitle: null,
      officerName: record.registeredAgent.name,
      address: record.registeredAgent.address,
    });
  }

  for (const officer of record.officers) {
    const officerAddressMatch = getAddressZipMatch(
      officer.address,
      zipPrefixes,
    );
    if (!officerAddressMatch) continue;
    matches.push({
      role: "officerAddress",
      matchedZipPrefix: officerAddressMatch.matchedZipPrefix,
      zip: officerAddressMatch.zip,
      officerOrdinal: officer.ordinal,
      officerTitle: officer.title,
      officerName: officer.name,
      address: officer.address,
    });
  }

  return matches;
}

/**
 * Extract Sunbiz corporate records from fixed-width lines by ZIP prefix and emit bounded chunks.
 *
 * @param {object} params - Extraction parameters.
 * @param {Iterable<string> | AsyncIterable<string>} params.lines - Corporate data lines.
 * @param {string[]} params.zipPrefixes - ZIP prefixes to match against principal, mailing, registered-agent, and officer addresses.
 * @param {number} params.chunkRecordLimit - Maximum matched records per emitted chunk.
 * @param {number | null | undefined} params.maxRecords - Optional cap on matched records for smoke runs.
 * @param {string} params.sourceFileName - Source file name for result provenance.
 * @param {string | undefined} [params.sourceDataS3Uri] - Source URI for summary provenance.
 * @param {"text" | "zip" | undefined} [params.sourceFormat] - Source format for summary provenance.
 * @param {(chunk: SunbizZipExtractionChunk) => Promise<SunbizZipExtractionChunkReceipt>} params.onChunk - Async chunk sink.
 * @returns {Promise<SunbizCorporateZipExtractionSummary>} Extraction summary.
 */
export async function extractCorporateDataLinesByZip({
  lines,
  zipPrefixes,
  chunkRecordLimit,
  maxRecords,
  sourceFileName,
  sourceDataS3Uri = "local-lines",
  sourceFormat = "text",
  onChunk,
}) {
  const normalizedZipPrefixes = normalizeZipPrefixes(zipPrefixes);
  if (!Number.isFinite(chunkRecordLimit) || chunkRecordLimit <= 0) {
    throw new Error("chunkRecordLimit must be a positive number");
  }
  const normalizedChunkRecordLimit = Math.floor(chunkRecordLimit);
  const normalizedMaxRecords =
    typeof maxRecords === "number" &&
    Number.isFinite(maxRecords) &&
    maxRecords > 0
      ? Math.floor(maxRecords)
      : null;

  let sourceRecordsRead = 0;
  let invalidRecordCount = 0;
  let matchedRecordCount = 0;
  let chunkIndex = 0;
  let stoppedAfterMaxRecords = false;
  /** @type {SunbizZipExtractedRecord[]} */
  let chunkRecords = [];
  /** @type {SunbizZipExtractionChunkReceipt[]} */
  const chunks = [];

  /**
   * Emit the current chunk to the caller-provided chunk sink.
   *
   * @returns {Promise<void>} Resolves after the chunk is written.
   */
  async function flushChunk() {
    if (chunkRecords.length === 0) return;
    const recordCount = chunkRecords.length;
    const receipt = await onChunk({
      chunkIndex,
      recordCount,
      records: chunkRecords,
    });
    chunks.push(receipt);
    chunkRecords = [];
    chunkIndex += 1;
  }

  for await (const line of lines) {
    if (
      normalizedMaxRecords !== null &&
      matchedRecordCount >= normalizedMaxRecords
    ) {
      stoppedAfterMaxRecords = true;
      break;
    }
    sourceRecordsRead += 1;
    const record = parseCorporateDataRecord(line);
    if (!record) {
      invalidRecordCount += 1;
      continue;
    }
    const matchedAddresses = findZipMatchedAddresses(
      record,
      normalizedZipPrefixes,
    );
    if (matchedAddresses.length === 0) continue;
    matchedRecordCount += 1;
    chunkRecords.push({
      sourceFileName,
      sourceLineNumber: sourceRecordsRead,
      entity: record,
      matchedAddresses,
    });
    if (chunkRecords.length >= normalizedChunkRecordLimit) {
      await flushChunk();
    }
  }

  await flushChunk();

  return {
    schemaVersion: CORPORATE_ZIP_EXTRACTION_SCHEMA_VERSION,
    source: "florida-sunbiz-corporate-bulk",
    sourceDataS3Uri,
    sourceFormat,
    retrievedAt: new Date().toISOString(),
    zipPrefixes: normalizedZipPrefixes,
    sourceRecordsRead,
    invalidRecordCount,
    matchedRecordCount,
    chunkRecordLimit: normalizedChunkRecordLimit,
    maxRecords: normalizedMaxRecords,
    stoppedAfterMaxRecords,
    chunks,
    lexiconStatus: {
      mappedToCurrentLexicon: false,
      notes:
        "Sunbiz entity, registered-agent, officer, and address records are preserved for later lexicon expansion.",
    },
  };
}

/**
 * Match Sunbiz corporate fixed-width lines against address inputs.
 *
 * @param {object} params - Matching parameters.
 * @param {Iterable<string>} params.lines - Corporate data lines.
 * @param {SunbizAddressInput[]} params.addressInputs - Address inputs.
 * @param {number} params.maxMatchesPerAddress - Per-input match cap.
 * @param {string} params.sourceFileName - Source file name for result provenance.
 * @returns {SunbizCorporateAddressMatchSummary} Match summary.
 */
export function matchCorporateDataLines({
  lines,
  addressInputs,
  maxMatchesPerAddress,
  sourceFileName,
}) {
  const targets = addressInputs.map(normalizeAddressInput);
  /** @type {Map<string, SunbizAddressMatch[]>} */
  const matchesByInputId = new Map(
    targets.map((target) => [
      target.inputId,
      /** @type {SunbizAddressMatch[]} */ ([]),
    ]),
  );
  /** @type {Set<string>} */
  const truncatedInputIds = new Set();
  let sourceRecordsRead = 0;
  let invalidRecordCount = 0;

  for (const line of lines) {
    sourceRecordsRead += 1;
    const record = parseCorporateDataRecord(line);
    if (!record) {
      invalidRecordCount += 1;
      continue;
    }

    for (const target of targets) {
      const existing = matchesByInputId.get(target.inputId) ?? [];
      if (existing.length >= maxMatchesPerAddress) {
        truncatedInputIds.add(target.inputId);
        continue;
      }
      const matchedAddresses = findMatchedAddresses(record, target);
      if (matchedAddresses.length === 0) continue;
      existing.push({
        sourceFileName,
        sourceLineNumber: sourceRecordsRead,
        entity: record,
        matchedAddresses,
      });
      matchesByInputId.set(target.inputId, existing);
    }
  }

  const addressResults = targets.map((target) => {
    const matches = matchesByInputId.get(target.inputId) ?? [];
    return {
      input: target.original,
      inputId: target.inputId,
      normalizedQuery: target.normalizedQuery,
      matchCount: matches.length,
      truncated: truncatedInputIds.has(target.inputId),
      matches,
    };
  });

  return {
    schemaVersion: CORPORATE_ADDRESS_MATCH_SCHEMA_VERSION,
    source: "florida-sunbiz-corporate-bulk",
    sourceDataS3Uri: "local-lines",
    sourceFormat: "text",
    retrievedAt: new Date().toISOString(),
    addressInputCount: addressInputs.length,
    sourceRecordsRead,
    invalidRecordCount,
    totalMatchCount: addressResults.reduce(
      (total, result) => total + result.matchCount,
      0,
    ),
    maxMatchesPerAddress,
    addressResults,
    lexiconStatus: {
      mappedToCurrentLexicon: false,
      notes:
        "Sunbiz entity, registered-agent, officer, and address matches are preserved for later lexicon expansion.",
    },
  };
}

/**
 * Convert an SDK response body into a local temporary file path.
 *
 * @param {unknown} body - AWS SDK streaming body.
 * @param {string} destinationPath - Destination file path.
 * @returns {Promise<void>} Resolves when the file is written.
 */
async function writeBodyToFile(body, destinationPath) {
  if (!body || typeof body !== "object") {
    throw new Error("S3 object response did not include a readable Body");
  }
  if ("pipe" in body && typeof body.pipe === "function") {
    await pipeline(
      /** @type {NodeJS.ReadableStream} */ (body),
      createWriteStream(destinationPath),
    );
    return;
  }
  if (
    "transformToByteArray" in body &&
    typeof body.transformToByteArray === "function"
  ) {
    const bytes = await body.transformToByteArray();
    await writeFile(destinationPath, Buffer.from(bytes));
    return;
  }
  throw new Error("Unsupported S3 Body type for Sunbiz source object");
}

/**
 * Download an S3 object to `/tmp` for fixed-width or ZIP streaming.
 *
 * @param {object} params - Download parameters.
 * @param {import("@aws-sdk/client-s3").S3Client} params.s3 - S3 client.
 * @param {string} params.sourceDataS3Uri - Source object URI.
 * @param {"text" | "zip"} params.sourceFormat - Source format.
 * @returns {Promise<string>} Temporary file path.
 */
async function downloadSourceToTempFile({ s3, sourceDataS3Uri, sourceFormat }) {
  const { bucket, key } = parseS3Uri(sourceDataS3Uri);
  const extension = sourceFormat === "zip" ? "zip" : "txt";
  const directory = path.join(tmpdir(), "permit-harvest-sunbiz");
  await mkdir(directory, { recursive: true });
  const filePath = path.join(
    directory,
    `${safeKeyPart(path.basename(key))}-${shortHash(sourceDataS3Uri)}.${extension}`,
  );
  const response = await s3.send(
    new GetObjectCommand({ Bucket: bucket, Key: key }),
  );
  await writeBodyToFile(response.Body, filePath);
  return filePath;
}

/**
 * Open a ZIP file with yauzl.
 *
 * @param {string} filePath - Local ZIP file path.
 * @returns {Promise<import("yauzl").ZipFile>} Open ZIP file.
 */
function openZipFile(filePath) {
  return new Promise((resolve, reject) => {
    yauzl.open(filePath, { lazyEntries: true }, (error, zipFile) => {
      if (error) {
        reject(error);
        return;
      }
      if (!zipFile) {
        reject(new Error(`Failed to open ZIP file: ${filePath}`));
        return;
      }
      resolve(zipFile);
    });
  });
}

/**
 * Process all text lines in a local text file.
 *
 * @param {string} filePath - Local text file path.
 * @param {(line: string, sourceFileName: string) => void} onLine - Line handler.
 * @returns {Promise<void>} Resolves after all lines are processed.
 */
async function processTextFileLines(filePath, onLine) {
  const input = createReadStream(filePath);
  const reader = readline.createInterface({ input, crlfDelay: Infinity });
  for await (const line of reader) {
    onLine(line, path.basename(filePath));
  }
}

/**
 * Process all text lines in a local text file with an async line handler.
 *
 * @param {string} filePath - Local text file path.
 * @param {(line: string, sourceFileName: string) => Promise<boolean>} onLine - Async line handler that returns false to stop early.
 * @returns {Promise<void>} Resolves after all lines are processed or the handler stops early.
 */
async function processTextFileLinesAsync(filePath, onLine) {
  const input = createReadStream(filePath);
  const reader = readline.createInterface({ input, crlfDelay: Infinity });
  for await (const line of reader) {
    const shouldContinue = await onLine(line, path.basename(filePath));
    if (!shouldContinue) break;
  }
}

/**
 * Process all `.txt` entries in a local ZIP file.
 *
 * @param {string} filePath - Local ZIP file path.
 * @param {(line: string, sourceFileName: string) => void} onLine - Line handler.
 * @returns {Promise<void>} Resolves after all ZIP text entries are processed.
 */
async function processZipTextLines(filePath, onLine) {
  const zipFile = await openZipFile(filePath);
  await new Promise((resolve, reject) => {
    zipFile.on("error", reject);
    zipFile.on("end", resolve);
    zipFile.on("entry", (entry) => {
      if (/\/$/.test(entry.fileName) || !/\.txt$/i.test(entry.fileName)) {
        zipFile.readEntry();
        return;
      }
      zipFile.openReadStream(entry, (error, stream) => {
        if (error) {
          reject(error);
          return;
        }
        if (!stream) {
          reject(new Error(`Failed to read ZIP entry: ${entry.fileName}`));
          return;
        }
        stream.on("error", reject);
        const reader = readline.createInterface({
          input: stream,
          crlfDelay: Infinity,
        });
        reader.on("line", (line) => onLine(line, entry.fileName));
        reader.on("close", () => zipFile.readEntry());
      });
    });
    zipFile.readEntry();
  });
}

/**
 * Process all `.txt` entries in a local ZIP file with an async line handler.
 *
 * @param {string} filePath - Local ZIP file path.
 * @param {(line: string, sourceFileName: string) => Promise<boolean>} onLine - Async line handler that returns false to stop early.
 * @returns {Promise<void>} Resolves after all ZIP text entries are processed or the handler stops early.
 */
async function processZipTextLinesAsync(filePath, onLine) {
  const zipFile = await openZipFile(filePath);
  await new Promise((resolve, reject) => {
    let finished = false;

    /**
     * Resolve or reject the surrounding promise exactly once.
     *
     * @param {unknown} error - Optional error value.
     * @returns {void}
     */
    function finish(error) {
      if (finished) return;
      finished = true;
      if (error) reject(error);
      else resolve(undefined);
    }

    zipFile.on("error", finish);
    zipFile.on("end", () => finish(undefined));
    zipFile.on("entry", (entry) => {
      if (/\/$/.test(entry.fileName) || !/\.txt$/i.test(entry.fileName)) {
        zipFile.readEntry();
        return;
      }
      zipFile.openReadStream(entry, (error, stream) => {
        if (error) {
          finish(error);
          return;
        }
        if (!stream) {
          finish(new Error(`Failed to read ZIP entry: ${entry.fileName}`));
          return;
        }
        stream.on("error", finish);
        void (async () => {
          const reader = readline.createInterface({
            input: stream,
            crlfDelay: Infinity,
          });
          for await (const line of reader) {
            const shouldContinue = await onLine(line, entry.fileName);
            if (!shouldContinue) {
              zipFile.close();
              finish(undefined);
              return;
            }
          }
          zipFile.readEntry();
        })().catch(finish);
      });
    });
    zipFile.readEntry();
  });
}

/**
 * Match a Sunbiz corporate fixed-width S3 object against address inputs.
 *
 * @param {object} params - Matching parameters.
 * @param {import("@aws-sdk/client-s3").S3Client} params.s3 - S3 client.
 * @param {string} params.sourceDataS3Uri - S3 URI for a fixed-width text file or ZIP containing text files.
 * @param {"text" | "zip"} params.sourceFormat - Source object format.
 * @param {SunbizAddressInput[]} params.addressInputs - Addresses to match.
 * @param {number} params.maxMatchesPerAddress - Per-address match cap.
 * @param {Logger} params.logger - Structured logger.
 * @returns {Promise<SunbizCorporateAddressMatchSummary>} Match summary.
 */
export async function matchCorporateDataS3Object({
  s3,
  sourceDataS3Uri,
  sourceFormat,
  addressInputs,
  maxMatchesPerAddress,
  logger,
}) {
  const targets = addressInputs.map(normalizeAddressInput);
  /** @type {Map<string, SunbizAddressMatch[]>} */
  const matchesByInputId = new Map(
    targets.map((target) => [
      target.inputId,
      /** @type {SunbizAddressMatch[]} */ ([]),
    ]),
  );
  /** @type {Set<string>} */
  const truncatedInputIds = new Set();
  let sourceRecordsRead = 0;
  let invalidRecordCount = 0;
  const filePath = await downloadSourceToTempFile({
    s3,
    sourceDataS3Uri,
    sourceFormat,
  });

  /**
   * Process one fixed-width line from a text file or ZIP entry.
   *
   * @param {string} line - Raw fixed-width row.
   * @param {string} sourceFileName - Source file name or ZIP entry name.
   * @returns {void}
   */
  const onLine = (line, sourceFileName) => {
    sourceRecordsRead += 1;
    const record = parseCorporateDataRecord(line);
    if (!record) {
      invalidRecordCount += 1;
      return;
    }

    for (const target of targets) {
      const existing = matchesByInputId.get(target.inputId) ?? [];
      if (existing.length >= maxMatchesPerAddress) {
        truncatedInputIds.add(target.inputId);
        continue;
      }
      const matchedAddresses = findMatchedAddresses(record, target);
      if (matchedAddresses.length === 0) continue;
      existing.push({
        sourceFileName,
        sourceLineNumber: sourceRecordsRead,
        entity: record,
        matchedAddresses,
      });
      matchesByInputId.set(target.inputId, existing);
    }
  };

  try {
    logger.info("sunbiz_corporate_match_source_open", {
      sourceDataS3Uri,
      sourceFormat,
      addressInputCount: addressInputs.length,
    });
    if (sourceFormat === "zip") {
      await processZipTextLines(filePath, onLine);
    } else {
      await processTextFileLines(filePath, onLine);
    }
  } finally {
    await rm(filePath, { force: true }).catch(() => undefined);
  }

  const addressResults = targets.map((target) => {
    const matches = matchesByInputId.get(target.inputId) ?? [];
    return {
      input: target.original,
      inputId: target.inputId,
      normalizedQuery: target.normalizedQuery,
      matchCount: matches.length,
      truncated: truncatedInputIds.has(target.inputId),
      matches,
    };
  });
  const totalMatchCount = addressResults.reduce(
    (total, result) => total + result.matchCount,
    0,
  );
  logger.info("sunbiz_corporate_match_source_complete", {
    sourceDataS3Uri,
    sourceFormat,
    sourceRecordsRead,
    invalidRecordCount,
    totalMatchCount,
  });

  return {
    schemaVersion: CORPORATE_ADDRESS_MATCH_SCHEMA_VERSION,
    source: "florida-sunbiz-corporate-bulk",
    sourceDataS3Uri,
    sourceFormat,
    retrievedAt: new Date().toISOString(),
    addressInputCount: addressInputs.length,
    sourceRecordsRead,
    invalidRecordCount,
    totalMatchCount,
    maxMatchesPerAddress,
    addressResults,
    lexiconStatus: {
      mappedToCurrentLexicon: false,
      notes:
        "Sunbiz entity, registered-agent, officer, and address matches are preserved for later lexicon expansion.",
    },
  };
}

/**
 * Extract Sunbiz corporate records from an S3 fixed-width text or ZIP object by ZIP prefix.
 *
 * @param {object} params - Extraction parameters.
 * @param {import("@aws-sdk/client-s3").S3Client} params.s3 - S3 client.
 * @param {string} params.sourceDataS3Uri - S3 URI for a fixed-width text file or ZIP containing text files.
 * @param {"text" | "zip"} params.sourceFormat - Source object format.
 * @param {string[]} params.zipPrefixes - ZIP prefixes to match against principal, mailing, registered-agent, and officer addresses.
 * @param {number} params.chunkRecordLimit - Maximum matched records per emitted chunk.
 * @param {number | null | undefined} params.maxRecords - Optional cap on matched records for smoke runs.
 * @param {(chunk: SunbizZipExtractionChunk) => Promise<SunbizZipExtractionChunkReceipt>} params.onChunk - Async chunk sink.
 * @param {Logger} params.logger - Structured logger.
 * @returns {Promise<SunbizCorporateZipExtractionSummary>} Extraction summary.
 */
export async function extractCorporateDataS3ObjectByZip({
  s3,
  sourceDataS3Uri,
  sourceFormat,
  zipPrefixes,
  chunkRecordLimit,
  maxRecords,
  onChunk,
  logger,
}) {
  const normalizedZipPrefixes = normalizeZipPrefixes(zipPrefixes);
  if (!Number.isFinite(chunkRecordLimit) || chunkRecordLimit <= 0) {
    throw new Error("chunkRecordLimit must be a positive number");
  }
  const normalizedChunkRecordLimit = Math.floor(chunkRecordLimit);
  const normalizedMaxRecords =
    typeof maxRecords === "number" &&
    Number.isFinite(maxRecords) &&
    maxRecords > 0
      ? Math.floor(maxRecords)
      : null;

  let sourceRecordsRead = 0;
  let invalidRecordCount = 0;
  let matchedRecordCount = 0;
  let chunkIndex = 0;
  let stoppedAfterMaxRecords = false;
  /** @type {SunbizZipExtractedRecord[]} */
  let chunkRecords = [];
  /** @type {SunbizZipExtractionChunkReceipt[]} */
  const chunks = [];
  const filePath = await downloadSourceToTempFile({
    s3,
    sourceDataS3Uri,
    sourceFormat,
  });

  /**
   * Emit the current chunk to the caller-provided chunk sink.
   *
   * @returns {Promise<void>} Resolves after the chunk is written.
   */
  async function flushChunk() {
    if (chunkRecords.length === 0) return;
    const recordCount = chunkRecords.length;
    const receipt = await onChunk({
      chunkIndex,
      recordCount,
      records: chunkRecords,
    });
    chunks.push(receipt);
    chunkRecords = [];
    chunkIndex += 1;
  }

  /**
   * Process one fixed-width line from a text file or ZIP entry.
   *
   * @param {string} line - Raw fixed-width row.
   * @param {string} sourceFileName - Source file name or ZIP entry name.
   * @returns {Promise<boolean>} True to keep scanning, false to stop early.
   */
  const onLine = async (line, sourceFileName) => {
    if (
      normalizedMaxRecords !== null &&
      matchedRecordCount >= normalizedMaxRecords
    ) {
      stoppedAfterMaxRecords = true;
      return false;
    }
    sourceRecordsRead += 1;
    const record = parseCorporateDataRecord(line);
    if (!record) {
      invalidRecordCount += 1;
      return true;
    }
    const matchedAddresses = findZipMatchedAddresses(
      record,
      normalizedZipPrefixes,
    );
    if (matchedAddresses.length === 0) return true;
    matchedRecordCount += 1;
    chunkRecords.push({
      sourceFileName,
      sourceLineNumber: sourceRecordsRead,
      entity: record,
      matchedAddresses,
    });
    if (chunkRecords.length >= normalizedChunkRecordLimit) {
      await flushChunk();
    }
    return true;
  };

  try {
    logger.info("sunbiz_corporate_zip_extract_source_open", {
      sourceDataS3Uri,
      sourceFormat,
      zipPrefixes: normalizedZipPrefixes,
      chunkRecordLimit: normalizedChunkRecordLimit,
      maxRecords: normalizedMaxRecords,
    });
    if (sourceFormat === "zip") {
      await processZipTextLinesAsync(filePath, onLine);
    } else {
      await processTextFileLinesAsync(filePath, onLine);
    }
    await flushChunk();
  } finally {
    await rm(filePath, { force: true }).catch(() => undefined);
  }

  logger.info("sunbiz_corporate_zip_extract_source_complete", {
    sourceDataS3Uri,
    sourceFormat,
    sourceRecordsRead,
    invalidRecordCount,
    matchedRecordCount,
    chunkCount: chunks.length,
    stoppedAfterMaxRecords,
  });

  return {
    schemaVersion: CORPORATE_ZIP_EXTRACTION_SCHEMA_VERSION,
    source: "florida-sunbiz-corporate-bulk",
    sourceDataS3Uri,
    sourceFormat,
    retrievedAt: new Date().toISOString(),
    zipPrefixes: normalizedZipPrefixes,
    sourceRecordsRead,
    invalidRecordCount,
    matchedRecordCount,
    chunkRecordLimit: normalizedChunkRecordLimit,
    maxRecords: normalizedMaxRecords,
    stoppedAfterMaxRecords,
    chunks,
    lexiconStatus: {
      mappedToCurrentLexicon: false,
      notes:
        "Sunbiz entity, registered-agent, officer, and address records are preserved for later lexicon expansion.",
    },
  };
}
