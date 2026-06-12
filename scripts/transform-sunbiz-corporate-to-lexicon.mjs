import {
  GetObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import * as crypto from "crypto";
import { createReadStream, createWriteStream } from "fs";
import { mkdir, mkdtemp, rm, writeFile } from "fs/promises";
import { tmpdir } from "os";
import * as path from "path";
import { pathToFileURL } from "url";
import { once } from "events";
import { parseArgs } from "util";

const SUNBIZ_SOURCE_HTTP_REQUEST = Object.freeze({
  method: "GET",
  url: "https://dos.fl.gov/sunbiz/other-services/data-downloads/",
});

const SOURCE_SYSTEM = "SUNBIZ";
const OUTPUT_SCHEMA_VERSION = "oracle-node.sunbiz-lexicon-transform.v1";
const DEFAULT_PART_RECORD_LIMIT = 50_000;

/**
 * @typedef {object} S3UriParts
 * @property {string} bucket - S3 bucket name parsed from an S3 URI.
 * @property {string} key - S3 object key parsed from an S3 URI.
 */

/**
 * @typedef {object} SunbizAddress
 * @property {string | null} line1 - First street address line from Sunbiz.
 * @property {string | null} line2 - Second street address line from Sunbiz.
 * @property {string | null} city - City text from Sunbiz.
 * @property {string | null} state - State or province code from Sunbiz.
 * @property {string | null} zip - ZIP or postal code from Sunbiz.
 * @property {string | null} country - Country code from Sunbiz.
 * @property {string} singleLine - Collapsed Sunbiz address text.
 * @property {string} normalized - Uppercase punctuation-normalized address text.
 */

/**
 * @typedef {object} SunbizAnnualReport
 * @property {string | null} year - Annual report year from Sunbiz.
 * @property {string | null} date - Annual report filed date from Sunbiz as YYYY-MM-DD.
 */

/**
 * @typedef {object} SunbizOfficer
 * @property {number} ordinal - One-based officer slot from the Sunbiz fixed-width file.
 * @property {string | null} title - Officer title code from Sunbiz.
 * @property {string | null} type - Officer type code from Sunbiz.
 * @property {string | null} name - Officer name from Sunbiz.
 * @property {SunbizAddress} address - Officer address from Sunbiz.
 */

/**
 * @typedef {object} SunbizCorporateRecord
 * @property {string} schemaVersion - Parser schema version that produced this record.
 * @property {string} source - Source system label from the permit-harvest parser.
 * @property {string} documentNumber - Florida corporate document number.
 * @property {string | null} entityName - Legal entity name from Sunbiz.
 * @property {string | null} statusCode - Raw status code from Sunbiz.
 * @property {string | null} status - Human-readable status label derived from Sunbiz statusCode.
 * @property {string | null} filingTypeCode - Raw filing type code from Sunbiz.
 * @property {string | null} filingType - Human-readable filing type derived from Sunbiz filingTypeCode.
 * @property {SunbizAddress} principalAddress - Principal address from Sunbiz.
 * @property {SunbizAddress} mailingAddress - Mailing address from Sunbiz.
 * @property {string | null} filedDate - Initial filed date as YYYY-MM-DD.
 * @property {string | null} feiNumber - Federal Employer Identification number from Sunbiz.
 * @property {boolean} moreThanSixOfficers - True when Sunbiz indicates additional officers exist beyond the fixed-width slots.
 * @property {string | null} lastTransactionDate - Last transaction date as YYYY-MM-DD.
 * @property {string | null} stateCountry - State/country code from Sunbiz.
 * @property {SunbizAnnualReport[]} annualReports - Up to three annual-report entries from Sunbiz.
 * @property {{ name: string | null, type: string | null, address: SunbizAddress }} registeredAgent - Registered-agent details from Sunbiz.
 * @property {SunbizOfficer[]} officers - Up to six officer records from Sunbiz.
 * @property {number} rawRecordLength - Raw fixed-width record length.
 */

/**
 * @typedef {object} SunbizZipMatchedAddress
 * @property {"principalAddress" | "mailingAddress" | "registeredAgentAddress" | "officerAddress"} role - Sunbiz address role that matched the ZIP extraction.
 * @property {string} matchedZipPrefix - ZIP prefix configured for extraction.
 * @property {string} zip - ZIP value found in the source address.
 * @property {number | null} officerOrdinal - Officer slot when role is officerAddress.
 * @property {string | null} officerTitle - Officer title when role is officerAddress.
 * @property {string | null} officerName - Officer name when role is officerAddress.
 * @property {SunbizAddress} address - Address that matched the ZIP extraction.
 */

/**
 * @typedef {object} SunbizZipExtractedRecord
 * @property {string} sourceFileName - Sunbiz source file or ZIP entry name.
 * @property {number} sourceLineNumber - One-based line number within the source file.
 * @property {SunbizCorporateRecord} entity - Parsed Sunbiz corporate record.
 * @property {SunbizZipMatchedAddress[]} matchedAddresses - Address-bearing fields that matched Lee-area ZIP prefixes.
 */

/**
 * @typedef {object} SourceChunk
 * @property {string} uri - JSONL chunk URI emitted by the permit-harvest Sunbiz extractor.
 * @property {string | null} sourceDataUri - Original fixed-width source object URI for provenance.
 * @property {string | null} extractKey - Extraction key from the summary manifest.
 */

/**
 * @typedef {object} LexiconSourceHttpRequest
 * @property {"GET"} method - HTTP method for the public Sunbiz data-download page.
 * @property {string} url - Public Sunbiz data-download page URL.
 */

/**
 * @typedef {object} CompanyRecord
 * @property {LexiconSourceHttpRequest} source_http_request - Source request for the public Sunbiz download landing page.
 * @property {string} request_identifier - Stable source identifier for the company record.
 * @property {string} name - Legal company/entity name.
 */

/**
 * @typedef {object} BusinessRegistrationRecord
 * @property {string} request_identifier - Stable source identifier for this registration.
 * @property {"SUNBIZ"} source_system - Source registry system.
 * @property {string | null} source_data_uri - URI of the fixed-width source object when known.
 * @property {string | null} source_file_name - Source file name from the extraction output.
 * @property {number | null} source_line_number - One-based line number from the source file.
 * @property {string} document_number - Sunbiz document number.
 * @property {string | null} entity_name - Entity legal name.
 * @property {string | null} status_code - Raw Sunbiz status code.
 * @property {string | null} status - Human-readable Sunbiz status.
 * @property {string | null} filing_type_code - Raw Sunbiz filing type code.
 * @property {string | null} filing_type - Human-readable Sunbiz filing type.
 * @property {string | null} filed_date - Filed date as YYYY-MM-DD.
 * @property {string | null} fei_number - FEI number when present.
 * @property {string | null} last_transaction_date - Last transaction date as YYYY-MM-DD.
 * @property {string | null} state_country - State/country code from Sunbiz.
 * @property {string | null} annual_report_1_year - Most recent annual report year.
 * @property {string | null} annual_report_1_date - Most recent annual report date.
 * @property {string | null} annual_report_2_year - Second annual report year.
 * @property {string | null} annual_report_2_date - Second annual report date.
 * @property {string | null} annual_report_3_year - Third annual report year.
 * @property {string | null} annual_report_3_date - Third annual report date.
 * @property {boolean | null} more_than_six_officers - Whether Sunbiz indicates additional officers exist.
 * @property {number | null} raw_record_length - Raw source record length.
 * @property {string[]} matched_address_roles - Address roles selected by ZIP extraction.
 * @property {string[]} matched_zip_prefixes - ZIP prefixes that selected this record.
 */

/**
 * @typedef {object} BusinessRegistrationAddressRecord
 * @property {string} request_identifier - Stable source identifier for this address-role bridge record.
 * @property {"SUNBIZ"} source_system - Source registry system.
 * @property {string} document_number - Sunbiz document number for the owning registration.
 * @property {"PRINCIPAL" | "MAILING"} address_role - Role of this address on the registration.
 * @property {string[]} matched_zip_prefixes - ZIP prefixes that selected this address role.
 */

/**
 * @typedef {object} BusinessRegistrationPartyRecord
 * @property {string} request_identifier - Stable source identifier for this party record.
 * @property {"SUNBIZ"} source_system - Source registry system.
 * @property {string} document_number - Sunbiz document number for the owning registration.
 * @property {"REGISTERED_AGENT" | "OFFICER"} party_role - Party role on the registration.
 * @property {string} name - Party name from Sunbiz.
 * @property {string | null} party_type_code - Raw Sunbiz party type code.
 * @property {string | null} title - Officer or party title.
 * @property {number | null} officer_ordinal - One-based officer slot when party_role is OFFICER.
 * @property {string[]} matched_zip_prefixes - ZIP prefixes that selected this party's address.
 */

/**
 * @typedef {object} AddressRecord
 * @property {LexiconSourceHttpRequest} source_http_request - Source request for the public Sunbiz download landing page.
 * @property {string} request_identifier - Stable identifier derived from normalized address facts.
 * @property {string | null} [city_name] - Uppercase city name adjusted to the address lexicon pattern when possible.
 * @property {string | null} [country_code] - Country code from Sunbiz.
 * @property {string | null} [plus_four_postal_code] - ZIP+4 suffix when present.
 * @property {string | null} [postal_code] - Five-digit ZIP when present.
 * @property {string | null} [state_code] - State code when present.
 * @property {string} unnormalized_address - Single-line address from Sunbiz.
 */

/**
 * @typedef {object} EntityReference
 * @property {string} type - Lexicon class type for the referenced entity.
 * @property {string} request_identifier - Stable request identifier for the referenced entity.
 */

/**
 * @typedef {object} RelationshipRecord
 * @property {string} relationship_type - Lexicon relationship type from the Business Registration data group.
 * @property {EntityReference} from - Source entity reference.
 * @property {EntityReference} to - Target entity reference.
 */

/**
 * @typedef {object} LexiconRecordBundle
 * @property {CompanyRecord[]} companies - Company entities emitted from a source Sunbiz record.
 * @property {BusinessRegistrationRecord[]} businessRegistrations - Business registration entities emitted from a source Sunbiz record.
 * @property {BusinessRegistrationAddressRecord[]} businessRegistrationAddresses - Registration-address bridge entities emitted from a source Sunbiz record.
 * @property {BusinessRegistrationPartyRecord[]} businessRegistrationParties - Registered-agent/officer party entities emitted from a source Sunbiz record.
 * @property {AddressRecord[]} addresses - Address entities emitted from a source Sunbiz record.
 * @property {RelationshipRecord[]} relationships - Relationship records connecting emitted entities by stable identifiers.
 */

/**
 * @typedef {object} JsonlPartReceipt
 * @property {string} dataset - Dataset name under the transform output prefix.
 * @property {number} partIndex - Zero-based part index.
 * @property {number} recordCount - Number of JSONL records in this part.
 * @property {string} uri - Local path or S3 URI for the written part.
 */

/**
 * @typedef {object} JsonlDatasetWriter
 * @property {(value: Record<string, unknown>) => Promise<void>} write - Write one JSONL record.
 * @property {() => Promise<JsonlPartReceipt[]>} close - Close the writer and return emitted part receipts.
 */

/**
 * @typedef {object} S3OutputLocation
 * @property {"s3"} kind - S3 output mode.
 * @property {string} bucket - Destination S3 bucket.
 * @property {string} keyPrefix - Destination S3 key prefix without a trailing slash.
 */

/**
 * @typedef {object} LocalOutputLocation
 * @property {"local"} kind - Local filesystem output mode.
 * @property {string} dir - Local output directory.
 */

/** @typedef {S3OutputLocation | LocalOutputLocation} OutputLocation */

/**
 * @typedef {object} TransformCounters
 * @property {number} sourceChunkCount - Number of source JSONL chunks processed.
 * @property {number} sourceRecordCount - Number of source JSONL records read.
 * @property {number} transformedRecordCount - Number of source records transformed successfully.
 * @property {number} invalidRecordCount - Number of source JSONL records skipped as invalid.
 * @property {number} companyCount - Number of unique company records written.
 * @property {number} businessRegistrationCount - Number of unique business registration records written.
 * @property {number} businessRegistrationAddressCount - Number of unique business registration address-role records written.
 * @property {number} businessRegistrationPartyCount - Number of unique business registration party records written.
 * @property {number} addressCount - Number of unique address records written.
 * @property {number} relationshipCount - Number of unique relationship records written.
 */

/**
 * @typedef {object} TransformState
 * @property {Set<string>} emittedEntities - Dataset/request-identifier keys that need de-duplication before writing.
 * @property {Set<string>} emittedRelationships - Reserved for relationship de-duplication in smaller local runs.
 * @property {TransformCounters} counters - Transform counters.
 */

/**
 * @typedef {object} SunbizLexiconTransformOptions
 * @property {S3Client} s3Client - S3 client used for reading source chunks and writing S3 outputs.
 * @property {string} manifestS3Uri - S3 URI of the Sunbiz ZIP extraction manifest.
 * @property {OutputLocation} outputLocation - Output destination for transformed JSONL datasets.
 * @property {number} partRecordLimit - Maximum records per output JSONL part.
 * @property {number | null} maxChunks - Optional cap on source chunks to process for smoke runs.
 * @property {number | null} maxRecords - Optional cap on source records to transform for smoke runs.
 */

/**
 * Parse an S3 URI into bucket and key parts.
 *
 * @param {string} uri - S3 URI in the form s3://bucket/key.
 * @returns {S3UriParts} Parsed S3 bucket and key.
 */
export function parseS3Uri(uri) {
  const match = uri.match(/^s3:\/\/([^/]+)\/(.+)$/);
  if (!match) {
    throw new Error(`Invalid S3 URI: ${uri}`);
  }
  return { bucket: match[1], key: match[2] };
}

/**
 * Build an S3 URI from bucket and key parts.
 *
 * @param {string} bucket - S3 bucket name.
 * @param {string} key - S3 object key.
 * @returns {string} S3 URI.
 */
function toS3Uri(bucket, key) {
  return `s3://${bucket}/${key}`;
}

/**
 * Normalize text by trimming and collapsing whitespace.
 *
 * @param {string | null | undefined} value - Source text value.
 * @returns {string | null} Normalized text, or null when empty.
 */
function cleanText(value) {
  if (value === null || value === undefined) return null;
  const text = String(value).replace(/\s+/g, " ").trim();
  return text.length > 0 ? text : null;
}

/**
 * Return unique sorted non-empty strings.
 *
 * @param {(string | null | undefined)[]} values - Candidate values.
 * @returns {string[]} Unique sorted values.
 */
function uniqueSortedStrings(values) {
  return Array.from(
    new Set(
      values.map((value) => cleanText(value)).filter((value) => value !== null),
    ),
  ).sort();
}

/**
 * Create a short deterministic hash from string parts.
 *
 * @param {string[]} parts - Values to hash in order.
 * @returns {string} First 24 hex characters of a SHA-256 digest.
 */
function shortHash(parts) {
  return crypto
    .createHash("sha256")
    .update(JSON.stringify(parts))
    .digest("hex")
    .slice(0, 24);
}

/**
 * Convert a Sunbiz city to the current address lexicon's uppercase city-name pattern.
 *
 * @param {string | null | undefined} city - Source city value.
 * @returns {string | null} Pattern-safe uppercase city value.
 */
function normalizeCityName(city) {
  const text = cleanText(city);
  if (!text) return null;
  const normalized = text
    .toUpperCase()
    .replace(/\./g, "")
    .replace(/[^A-Z\s\-']/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return normalized.length > 0 ? normalized : null;
}

/**
 * Split a Sunbiz ZIP-like value into five-digit ZIP and plus-four extension.
 *
 * @param {string | null | undefined} zip - Source ZIP or ZIP+4 value.
 * @returns {{ postalCode: string | null, plusFour: string | null }} Split ZIP components.
 */
function splitZip(zip) {
  const digits = cleanText(zip)?.replace(/\D/g, "") ?? "";
  if (digits.length < 5) return { postalCode: null, plusFour: null };
  return {
    postalCode: digits.slice(0, 5),
    plusFour: digits.length >= 9 ? digits.slice(5, 9) : null,
  };
}

/**
 * Check whether a Sunbiz address has enough information to emit an address entity.
 *
 * @param {SunbizAddress | null | undefined} address - Source Sunbiz address.
 * @returns {boolean} True when the address has at least one meaningful address field.
 */
function hasAddressContent(address) {
  if (!address) return false;
  return Boolean(
    cleanText(address.singleLine) ||
    cleanText(address.line1) ||
    cleanText(address.city) ||
    cleanText(address.state) ||
    cleanText(address.zip),
  );
}

/**
 * Build a single-line address from Sunbiz address components.
 *
 * @param {SunbizAddress} address - Source Sunbiz address.
 * @returns {string} Single-line address suitable for address.unnormalized_address.
 */
function toSingleLineAddress(address) {
  return (
    cleanText(address.singleLine) ??
    [
      address.line1,
      address.line2,
      address.city,
      address.state,
      address.zip,
      address.country,
    ]
      .map((part) => cleanText(part))
      .filter((part) => part !== null)
      .join(" ")
  );
}

/**
 * Build a stable request identifier for an address entity.
 *
 * @param {SunbizAddress} address - Source Sunbiz address.
 * @returns {string} Stable address request identifier.
 */
export function stableAddressIdentifier(address) {
  const normalized =
    cleanText(address.normalized) ?? toSingleLineAddress(address).toUpperCase();
  const state = cleanText(address.state)?.toUpperCase() ?? "";
  const zip = cleanText(address.zip) ?? "";
  const country = cleanText(address.country)?.toUpperCase() ?? "";
  return `sunbiz:address:${shortHash([normalized, state, zip, country])}`;
}

/**
 * Convert a Sunbiz address into the existing lexicon address class shape.
 *
 * @param {SunbizAddress} address - Source Sunbiz address.
 * @returns {AddressRecord | null} Lexicon-shaped address record, or null when empty.
 */
export function toLexiconAddress(address) {
  if (!hasAddressContent(address)) return null;
  const { postalCode, plusFour } = splitZip(address.zip);
  const stateCode = cleanText(address.state)?.toUpperCase() ?? null;
  const countryCode = cleanText(address.country)?.toUpperCase() ?? null;

  /** @type {AddressRecord} */
  const record = {
    source_http_request: SUNBIZ_SOURCE_HTTP_REQUEST,
    request_identifier: stableAddressIdentifier(address),
    unnormalized_address: toSingleLineAddress(address),
  };

  const cityName = normalizeCityName(address.city);
  if (cityName) record.city_name = cityName;
  if (countryCode) record.country_code = countryCode;
  if (plusFour) record.plus_four_postal_code = plusFour;
  if (postalCode) record.postal_code = postalCode;
  if (stateCode) record.state_code = stateCode;

  return record;
}

/**
 * Collect matched ZIP prefixes for a role and optional officer ordinal.
 *
 * @param {SunbizZipMatchedAddress[]} matchedAddresses - Matched address metadata from extraction.
 * @param {SunbizZipMatchedAddress["role"]} role - Role to collect.
 * @param {number | null} officerOrdinal - Officer ordinal to match when role is officerAddress.
 * @returns {string[]} Unique sorted ZIP prefixes.
 */
export function collectMatchedZipPrefixes(
  matchedAddresses,
  role,
  officerOrdinal = null,
) {
  return uniqueSortedStrings(
    matchedAddresses
      .filter((match) => {
        if (match.role !== role) return false;
        if (role !== "officerAddress") return true;
        return match.officerOrdinal === officerOrdinal;
      })
      .map((match) => match.matchedZipPrefix),
  );
}

/**
 * Build an entity reference from type and request identifier.
 *
 * @param {string} type - Lexicon class type.
 * @param {string} requestIdentifier - Stable request identifier.
 * @returns {EntityReference} Relationship endpoint reference.
 */
function entityRef(type, requestIdentifier) {
  return { type, request_identifier: requestIdentifier };
}

/**
 * Build a relationship record.
 *
 * @param {string} relationshipType - Business Registration data group relationship type.
 * @param {EntityReference} from - Source entity reference.
 * @param {EntityReference} to - Target entity reference.
 * @returns {RelationshipRecord} Relationship record.
 */
function relationship(relationshipType, from, to) {
  return { relationship_type: relationshipType, from, to };
}

/**
 * Convert a single Sunbiz extraction record into lexicon-shaped entities and relationships.
 *
 * @param {SunbizZipExtractedRecord} record - Sunbiz JSONL extraction record.
 * @param {{ sourceDataUri?: string | null }} [options] - Optional source provenance.
 * @returns {LexiconRecordBundle} Lexicon-shaped record bundle.
 */
export function transformSunbizRecord(record, options = {}) {
  const entity = record.entity;
  const documentNumber = entity.documentNumber;
  const companyIdentifier = `sunbiz:${documentNumber}:company`;
  const registrationIdentifier = `sunbiz:${documentNumber}:business_registration`;
  const entityName = cleanText(entity.entityName) ?? documentNumber;
  const annualReports = entity.annualReports ?? [];
  const report1 = annualReports[0] ?? { year: null, date: null };
  const report2 = annualReports[1] ?? { year: null, date: null };
  const report3 = annualReports[2] ?? { year: null, date: null };
  const matchedAddressRoles = uniqueSortedStrings(
    record.matchedAddresses.map((match) => match.role),
  );
  const matchedZipPrefixes = uniqueSortedStrings(
    record.matchedAddresses.map((match) => match.matchedZipPrefix),
  );

  /** @type {LexiconRecordBundle} */
  const bundle = {
    companies: [
      {
        source_http_request: SUNBIZ_SOURCE_HTTP_REQUEST,
        request_identifier: companyIdentifier,
        name: entityName,
      },
    ],
    businessRegistrations: [
      {
        request_identifier: registrationIdentifier,
        source_system: SOURCE_SYSTEM,
        source_data_uri: options.sourceDataUri ?? null,
        source_file_name: cleanText(record.sourceFileName),
        source_line_number: Number.isInteger(record.sourceLineNumber)
          ? record.sourceLineNumber
          : null,
        document_number: documentNumber,
        entity_name: cleanText(entity.entityName),
        status_code: cleanText(entity.statusCode),
        status: cleanText(entity.status),
        filing_type_code: cleanText(entity.filingTypeCode),
        filing_type: cleanText(entity.filingType),
        filed_date: cleanText(entity.filedDate),
        fei_number: cleanText(entity.feiNumber),
        last_transaction_date: cleanText(entity.lastTransactionDate),
        state_country: cleanText(entity.stateCountry),
        annual_report_1_year: cleanText(report1.year),
        annual_report_1_date: cleanText(report1.date),
        annual_report_2_year: cleanText(report2.year),
        annual_report_2_date: cleanText(report2.date),
        annual_report_3_year: cleanText(report3.year),
        annual_report_3_date: cleanText(report3.date),
        more_than_six_officers: entity.moreThanSixOfficers,
        raw_record_length: Number.isInteger(entity.rawRecordLength)
          ? entity.rawRecordLength
          : null,
        matched_address_roles: matchedAddressRoles,
        matched_zip_prefixes: matchedZipPrefixes,
      },
    ],
    businessRegistrationAddresses: [],
    businessRegistrationParties: [],
    addresses: [],
    relationships: [
      relationship(
        "company_has_business_registration",
        entityRef("company", companyIdentifier),
        entityRef("business_registration", registrationIdentifier),
      ),
    ],
  };

  addRegistrationAddress(
    bundle,
    record,
    "PRINCIPAL",
    "principalAddress",
    entity.principalAddress,
  );
  addRegistrationAddress(
    bundle,
    record,
    "MAILING",
    "mailingAddress",
    entity.mailingAddress,
  );
  addRegisteredAgent(bundle, record);
  for (const officer of entity.officers ?? []) {
    addOfficer(bundle, record, officer);
  }

  return bundle;
}

/**
 * Add a principal or mailing address bridge and relationships to a bundle.
 *
 * @param {LexiconRecordBundle} bundle - Bundle to mutate.
 * @param {SunbizZipExtractedRecord} record - Source Sunbiz record.
 * @param {"PRINCIPAL" | "MAILING"} addressRole - Lexicon address-role enum value.
 * @param {"principalAddress" | "mailingAddress"} matchedRole - Source matched-address role.
 * @param {SunbizAddress} address - Sunbiz address to emit.
 * @returns {void}
 */
function addRegistrationAddress(
  bundle,
  record,
  addressRole,
  matchedRole,
  address,
) {
  const addressRecord = toLexiconAddress(address);
  if (!addressRecord) return;

  const documentNumber = record.entity.documentNumber;
  const registrationIdentifier = `sunbiz:${documentNumber}:business_registration`;
  const bridgeIdentifier = `sunbiz:${documentNumber}:business_registration_address:${addressRole.toLowerCase()}`;
  const matchedZipPrefixes = collectMatchedZipPrefixes(
    record.matchedAddresses,
    matchedRole,
    null,
  );

  bundle.addresses.push(addressRecord);
  bundle.businessRegistrationAddresses.push({
    request_identifier: bridgeIdentifier,
    source_system: SOURCE_SYSTEM,
    document_number: documentNumber,
    address_role: addressRole,
    matched_zip_prefixes: matchedZipPrefixes,
  });
  bundle.relationships.push(
    relationship(
      "business_registration_has_address",
      entityRef("business_registration", registrationIdentifier),
      entityRef("business_registration_address", bridgeIdentifier),
    ),
    relationship(
      "business_registration_address_has_address",
      entityRef("business_registration_address", bridgeIdentifier),
      entityRef("address", addressRecord.request_identifier),
    ),
  );
}

/**
 * Add a registered-agent party and optional address to a bundle.
 *
 * @param {LexiconRecordBundle} bundle - Bundle to mutate.
 * @param {SunbizZipExtractedRecord} record - Source Sunbiz record.
 * @returns {void}
 */
function addRegisteredAgent(bundle, record) {
  const agent = record.entity.registeredAgent;
  const agentName = cleanText(agent.name);
  if (!agentName) return;

  const documentNumber = record.entity.documentNumber;
  const registrationIdentifier = `sunbiz:${documentNumber}:business_registration`;
  const partyIdentifier = `sunbiz:${documentNumber}:party:registered_agent:${shortHash(
    [agentName, cleanText(agent.type) ?? ""],
  )}`;
  const matchedZipPrefixes = collectMatchedZipPrefixes(
    record.matchedAddresses,
    "registeredAgentAddress",
    null,
  );

  bundle.businessRegistrationParties.push({
    request_identifier: partyIdentifier,
    source_system: SOURCE_SYSTEM,
    document_number: documentNumber,
    party_role: "REGISTERED_AGENT",
    name: agentName,
    party_type_code: cleanText(agent.type),
    title: null,
    officer_ordinal: null,
    matched_zip_prefixes: matchedZipPrefixes,
  });
  bundle.relationships.push(
    relationship(
      "business_registration_has_party",
      entityRef("business_registration", registrationIdentifier),
      entityRef("business_registration_party", partyIdentifier),
    ),
  );

  const addressRecord = toLexiconAddress(agent.address);
  if (!addressRecord) return;
  bundle.addresses.push(addressRecord);
  bundle.relationships.push(
    relationship(
      "business_registration_party_has_address",
      entityRef("business_registration_party", partyIdentifier),
      entityRef("address", addressRecord.request_identifier),
    ),
  );
}

/**
 * Add an officer party and optional address to a bundle.
 *
 * @param {LexiconRecordBundle} bundle - Bundle to mutate.
 * @param {SunbizZipExtractedRecord} record - Source Sunbiz record.
 * @param {SunbizOfficer} officer - Source Sunbiz officer.
 * @returns {void}
 */
function addOfficer(bundle, record, officer) {
  const officerName = cleanText(officer.name);
  if (!officerName) return;

  const documentNumber = record.entity.documentNumber;
  const registrationIdentifier = `sunbiz:${documentNumber}:business_registration`;
  const partyIdentifier = `sunbiz:${documentNumber}:party:officer:${officer.ordinal}:${shortHash(
    [
      officerName,
      cleanText(officer.title) ?? "",
      cleanText(officer.type) ?? "",
    ],
  )}`;
  const matchedZipPrefixes = collectMatchedZipPrefixes(
    record.matchedAddresses,
    "officerAddress",
    officer.ordinal,
  );

  bundle.businessRegistrationParties.push({
    request_identifier: partyIdentifier,
    source_system: SOURCE_SYSTEM,
    document_number: documentNumber,
    party_role: "OFFICER",
    name: officerName,
    party_type_code: cleanText(officer.type),
    title: cleanText(officer.title),
    officer_ordinal: officer.ordinal,
    matched_zip_prefixes: matchedZipPrefixes,
  });
  bundle.relationships.push(
    relationship(
      "business_registration_has_party",
      entityRef("business_registration", registrationIdentifier),
      entityRef("business_registration_party", partyIdentifier),
    ),
  );

  const addressRecord = toLexiconAddress(officer.address);
  if (!addressRecord) return;
  bundle.addresses.push(addressRecord);
  bundle.relationships.push(
    relationship(
      "business_registration_party_has_address",
      entityRef("business_registration_party", partyIdentifier),
      entityRef("address", addressRecord.request_identifier),
    ),
  );
}

/**
 * Create an empty transform state with counters and de-duplication sets.
 *
 * @returns {TransformState} Empty transform state.
 */
function createTransformState() {
  return {
    emittedEntities: new Set(),
    emittedRelationships: new Set(),
    counters: {
      sourceChunkCount: 0,
      sourceRecordCount: 0,
      transformedRecordCount: 0,
      invalidRecordCount: 0,
      companyCount: 0,
      businessRegistrationCount: 0,
      businessRegistrationAddressCount: 0,
      businessRegistrationPartyCount: 0,
      addressCount: 0,
      relationshipCount: 0,
    },
  };
}

/**
 * Increment a dataset counter after a unique entity has been written.
 *
 * @param {TransformCounters} counters - Counters object to mutate.
 * @param {string} dataset - Dataset name that was written.
 * @returns {void}
 */
function incrementEntityCounter(counters, dataset) {
  if (dataset === "classes/company") counters.companyCount += 1;
  else if (dataset === "classes/business_registration")
    counters.businessRegistrationCount += 1;
  else if (dataset === "classes/business_registration_address") {
    counters.businessRegistrationAddressCount += 1;
  } else if (dataset === "classes/business_registration_party") {
    counters.businessRegistrationPartyCount += 1;
  } else if (dataset === "classes/address") counters.addressCount += 1;
}

/**
 * Write a unique entity record to a dataset writer.
 *
 * @param {TransformState} state - Transform state with de-duplication sets.
 * @param {Record<string, JsonlDatasetWriter>} writers - Dataset writers keyed by dataset name.
 * @param {string} dataset - Output dataset name.
 * @param {string} requestIdentifier - Stable entity request identifier.
 * @param {Record<string, unknown>} value - Entity record to write.
 * @returns {Promise<void>}
 */
async function writeUniqueEntity(
  state,
  writers,
  dataset,
  requestIdentifier,
  value,
) {
  const key = `${dataset}:${requestIdentifier}`;
  const shouldDedupe = dataset === "classes/address";
  if (shouldDedupe && state.emittedEntities.has(key)) return;
  const writer = writers[dataset];
  if (!writer) throw new Error(`Missing writer for dataset ${dataset}`);
  if (shouldDedupe) state.emittedEntities.add(key);
  await writer.write(value);
  incrementEntityCounter(state.counters, dataset);
}

/**
 * Write a unique relationship record to its relationship-type dataset.
 *
 * @param {TransformState} state - Transform state with de-duplication sets.
 * @param {Record<string, JsonlDatasetWriter>} writers - Dataset writers keyed by dataset name.
 * @param {RelationshipRecord} value - Relationship record to write.
 * @returns {Promise<void>}
 */
async function writeUniqueRelationship(state, writers, value) {
  const dataset = `relationships/${value.relationship_type}`;
  const writer = writers[dataset];
  if (!writer) throw new Error(`Missing writer for dataset ${dataset}`);
  await writer.write(/** @type {Record<string, unknown>} */ (value));
  state.counters.relationshipCount += 1;
}

/**
 * Write all records from a bundle to output datasets with stable de-duplication.
 *
 * @param {TransformState} state - Transform state with de-duplication sets and counters.
 * @param {Record<string, JsonlDatasetWriter>} writers - Dataset writers keyed by dataset name.
 * @param {LexiconRecordBundle} bundle - Bundle produced from one Sunbiz source record.
 * @returns {Promise<void>}
 */
async function writeBundle(state, writers, bundle) {
  for (const company of bundle.companies) {
    await writeUniqueEntity(
      state,
      writers,
      "classes/company",
      company.request_identifier,
      /** @type {Record<string, unknown>} */ (company),
    );
  }
  for (const registration of bundle.businessRegistrations) {
    await writeUniqueEntity(
      state,
      writers,
      "classes/business_registration",
      registration.request_identifier,
      /** @type {Record<string, unknown>} */ (registration),
    );
  }
  for (const registrationAddress of bundle.businessRegistrationAddresses) {
    await writeUniqueEntity(
      state,
      writers,
      "classes/business_registration_address",
      registrationAddress.request_identifier,
      /** @type {Record<string, unknown>} */ (registrationAddress),
    );
  }
  for (const party of bundle.businessRegistrationParties) {
    await writeUniqueEntity(
      state,
      writers,
      "classes/business_registration_party",
      party.request_identifier,
      /** @type {Record<string, unknown>} */ (party),
    );
  }
  for (const address of bundle.addresses) {
    await writeUniqueEntity(
      state,
      writers,
      "classes/address",
      address.request_identifier,
      /** @type {Record<string, unknown>} */ (address),
    );
  }
  for (const relationshipRecord of bundle.relationships) {
    await writeUniqueRelationship(state, writers, relationshipRecord);
  }
}

/**
 * Check whether an unknown value is a non-array object.
 *
 * @param {unknown} value - Candidate value.
 * @returns {value is Record<string, unknown>} True when value is an object record.
 */
function isObjectRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Validate the minimal JSONL shape needed by the transform.
 *
 * @param {unknown} value - Parsed JSONL value.
 * @returns {value is SunbizZipExtractedRecord} True when value has the expected source record shape.
 */
function isSunbizZipExtractedRecord(value) {
  if (!isObjectRecord(value)) return false;
  if (typeof value.sourceFileName !== "string") return false;
  if (typeof value.sourceLineNumber !== "number") return false;
  if (!Array.isArray(value.matchedAddresses)) return false;
  if (!isObjectRecord(value.entity)) return false;
  return (
    typeof value.entity.documentNumber === "string" &&
    value.entity.documentNumber.length > 0
  );
}

/**
 * Read an S3 object body into a string using the SDK v3 Body helper.
 *
 * @param {S3Client} s3Client - S3 client.
 * @param {string} s3Uri - S3 URI to read.
 * @returns {Promise<string>} Object body as UTF-8 text.
 */
async function readS3ObjectText(s3Client, s3Uri) {
  const { bucket, key } = parseS3Uri(s3Uri);
  const response = await s3Client.send(
    new GetObjectCommand({ Bucket: bucket, Key: key }),
  );
  const body = response.Body;
  if (!body || typeof body.transformToString !== "function") {
    throw new Error(`S3 object body is not readable: ${s3Uri}`);
  }
  return await body.transformToString();
}

/**
 * Read an S3 JSON object and return it as a generic object record.
 *
 * @param {S3Client} s3Client - S3 client.
 * @param {string} s3Uri - S3 URI to read.
 * @returns {Promise<Record<string, unknown>>} Parsed JSON object.
 */
async function readS3JsonObject(s3Client, s3Uri) {
  const text = await readS3ObjectText(s3Client, s3Uri);
  const parsed = /** @type {unknown} */ (JSON.parse(text));
  if (!isObjectRecord(parsed)) {
    throw new Error(`Expected JSON object at ${s3Uri}`);
  }
  return parsed;
}

/**
 * Extract a string property from a generic object.
 *
 * @param {Record<string, unknown>} object - Source object.
 * @param {string} key - Property key.
 * @returns {string | null} String value or null.
 */
function getStringProperty(object, key) {
  const value = object[key];
  return typeof value === "string" ? value : null;
}

/**
 * Collect source JSONL chunk URIs from a Sunbiz extraction manifest and per-entry summaries.
 *
 * @param {S3Client} s3Client - S3 client.
 * @param {string} manifestS3Uri - S3 URI of the extraction manifest.
 * @param {number | null} maxChunks - Optional cap on source JSONL chunks.
 * @returns {Promise<{ manifest: Record<string, unknown>, chunks: SourceChunk[] }>} Manifest and source chunk descriptors.
 */
async function collectSourceChunks(s3Client, manifestS3Uri, maxChunks) {
  const manifest = await readS3JsonObject(s3Client, manifestS3Uri);
  const entriesValue = manifest.entries;
  if (!Array.isArray(entriesValue)) {
    throw new Error(`Manifest is missing entries array: ${manifestS3Uri}`);
  }

  /** @type {SourceChunk[]} */
  const chunks = [];
  for (const entryValue of entriesValue) {
    if (!isObjectRecord(entryValue)) continue;
    const summaryUri = getStringProperty(entryValue, "summaryUri");
    if (!summaryUri) continue;
    const summary = await readS3JsonObject(s3Client, summaryUri);
    const summaryChunks = summary.chunks;
    if (!Array.isArray(summaryChunks)) continue;
    const sourceDataUri = getStringProperty(summary, "sourceDataS3Uri");
    const extractKey = getStringProperty(summary, "extractKey");
    for (const chunkValue of summaryChunks) {
      if (!isObjectRecord(chunkValue)) continue;
      const uri = getStringProperty(chunkValue, "uri");
      if (!uri) continue;
      chunks.push({ uri, sourceDataUri, extractKey });
      if (maxChunks !== null && chunks.length >= maxChunks) {
        return { manifest, chunks };
      }
    }
  }

  return { manifest, chunks };
}

/**
 * Upload a local file to S3 and remove the local copy after success.
 *
 * @param {S3Client} s3Client - S3 client.
 * @param {string} localPath - Local file path to upload.
 * @param {string} bucket - Destination S3 bucket.
 * @param {string} key - Destination S3 key.
 * @returns {Promise<void>}
 */
async function uploadJsonlPartToS3(s3Client, localPath, bucket, key) {
  await s3Client.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: createReadStream(localPath),
      ContentType: "application/x-ndjson",
    }),
  );
  await rm(localPath, { force: true });
}

/**
 * Write a JSON object to the configured output destination.
 *
 * @param {S3Client} s3Client - S3 client.
 * @param {OutputLocation} outputLocation - Output destination.
 * @param {string} name - Output object name below the root output prefix.
 * @param {Record<string, unknown>} value - JSON object to write.
 * @returns {Promise<string>} Local path or S3 URI written.
 */
async function writeOutputJson(s3Client, outputLocation, name, value) {
  const body = `${JSON.stringify(value, null, 2)}\n`;
  if (outputLocation.kind === "s3") {
    const key = `${outputLocation.keyPrefix}/${name}`;
    await s3Client.send(
      new PutObjectCommand({
        Bucket: outputLocation.bucket,
        Key: key,
        Body: body,
        ContentType: "application/json",
      }),
    );
    return toS3Uri(outputLocation.bucket, key);
  }
  const filePath = path.join(outputLocation.dir, name);
  await mkdir(path.dirname(filePath), { recursive: true });
  await writeFile(filePath, body, "utf8");
  return filePath;
}

/**
 * Create a rotating JSONL writer for one output dataset.
 *
 * @param {object} params - Writer configuration.
 * @param {S3Client} params.s3Client - S3 client used when outputLocation is S3.
 * @param {OutputLocation} params.outputLocation - Output destination.
 * @param {string} params.dataset - Dataset name below the transform output prefix.
 * @param {string} params.tempDir - Temporary directory for part files.
 * @param {number} params.partRecordLimit - Maximum records per JSONL part.
 * @returns {JsonlDatasetWriter} Rotating dataset writer.
 */
function createJsonlDatasetWriter({
  s3Client,
  outputLocation,
  dataset,
  tempDir,
  partRecordLimit,
}) {
  /** @type {import("fs").WriteStream | null} */
  let stream = null;
  /** @type {string | null} */
  let currentPath = null;
  let partIndex = 0;
  let currentRecordCount = 0;
  /** @type {JsonlPartReceipt[]} */
  const receipts = [];

  /**
   * Open a new part file.
   *
   * @returns {Promise<void>}
   */
  async function openPart() {
    const partName = `part-${String(partIndex).padStart(5, "0")}.jsonl`;
    const baseDir =
      outputLocation.kind === "local" ? outputLocation.dir : tempDir;
    const nextPath = path.join(baseDir, dataset, partName);
    await mkdir(path.dirname(nextPath), { recursive: true });
    stream = createWriteStream(nextPath, { encoding: "utf8" });
    currentPath = nextPath;
    currentRecordCount = 0;
  }

  /**
   * Close and publish the current part file when it contains records.
   *
   * @returns {Promise<void>}
   */
  async function closeCurrentPart() {
    if (!stream || !currentPath) return;
    const closedPath = currentPath;
    const closedIndex = partIndex;
    const closedRecordCount = currentRecordCount;
    await new Promise((resolve, reject) => {
      if (!stream) {
        resolve(undefined);
        return;
      }
      stream.once("error", reject);
      stream.end(resolve);
    });
    stream = null;
    currentPath = null;

    if (closedRecordCount === 0) {
      await rm(closedPath, { force: true });
      return;
    }

    if (outputLocation.kind === "s3") {
      const key = `${outputLocation.keyPrefix}/${dataset}/part-${String(
        closedIndex,
      ).padStart(5, "0")}.jsonl`;
      await uploadJsonlPartToS3(
        s3Client,
        closedPath,
        outputLocation.bucket,
        key,
      );
      receipts.push({
        dataset,
        partIndex: closedIndex,
        recordCount: closedRecordCount,
        uri: toS3Uri(outputLocation.bucket, key),
      });
    } else {
      receipts.push({
        dataset,
        partIndex: closedIndex,
        recordCount: closedRecordCount,
        uri: closedPath,
      });
    }
    partIndex += 1;
  }

  return {
    async write(value) {
      if (!stream || currentRecordCount >= partRecordLimit) {
        await closeCurrentPart();
        await openPart();
      }
      if (!stream)
        throw new Error(`Writer did not open for dataset ${dataset}`);
      const canContinue = stream.write(`${JSON.stringify(value)}\n`);
      currentRecordCount += 1;
      if (!canContinue) {
        await once(stream, "drain");
      }
    },
    async close() {
      await closeCurrentPart();
      return receipts;
    },
  };
}

/**
 * Create all dataset writers used by the transform.
 *
 * @param {S3Client} s3Client - S3 client used when outputLocation is S3.
 * @param {OutputLocation} outputLocation - Output destination.
 * @param {string} tempDir - Temporary directory for JSONL part files.
 * @param {number} partRecordLimit - Maximum records per JSONL part.
 * @returns {Record<string, JsonlDatasetWriter>} Dataset writers keyed by dataset name.
 */
function createDatasetWriters(
  s3Client,
  outputLocation,
  tempDir,
  partRecordLimit,
) {
  const datasetNames = [
    "classes/company",
    "classes/business_registration",
    "classes/business_registration_address",
    "classes/business_registration_party",
    "classes/address",
    "relationships/company_has_business_registration",
    "relationships/business_registration_has_address",
    "relationships/business_registration_address_has_address",
    "relationships/business_registration_has_party",
    "relationships/business_registration_party_has_address",
  ];
  /** @type {Record<string, JsonlDatasetWriter>} */
  const writers = {};
  for (const dataset of datasetNames) {
    writers[dataset] = createJsonlDatasetWriter({
      s3Client,
      outputLocation,
      dataset,
      tempDir,
      partRecordLimit,
    });
  }
  return writers;
}

/**
 * Close all dataset writers and return all emitted part receipts.
 *
 * @param {Record<string, JsonlDatasetWriter>} writers - Dataset writers keyed by dataset name.
 * @returns {Promise<JsonlPartReceipt[]>} All part receipts.
 */
async function closeDatasetWriters(writers) {
  /** @type {JsonlPartReceipt[]} */
  const receipts = [];
  for (const writer of Object.values(writers)) {
    receipts.push(...(await writer.close()));
  }
  return receipts;
}

/**
 * Process a Sunbiz extraction manifest into lexicon-shaped JSONL datasets.
 *
 * @param {SunbizLexiconTransformOptions} options - Transform options.
 * @returns {Promise<Record<string, unknown>>} Summary object written to the output destination.
 */
export async function processSunbizManifest(options) {
  const tempDir = await mkdtemp(path.join(tmpdir(), "sunbiz-lexicon-"));
  const state = createTransformState();
  const writers = createDatasetWriters(
    options.s3Client,
    options.outputLocation,
    tempDir,
    options.partRecordLimit,
  );

  try {
    const { manifest, chunks } = await collectSourceChunks(
      options.s3Client,
      options.manifestS3Uri,
      options.maxChunks,
    );
    state.counters.sourceChunkCount = chunks.length;

    let stopAfterRecordLimit = false;
    for (const chunk of chunks) {
      if (stopAfterRecordLimit) break;
      const text = await readS3ObjectText(options.s3Client, chunk.uri);
      const lines = text.split(/\r?\n/);
      for (const line of lines) {
        if (!line.trim()) continue;
        if (
          options.maxRecords !== null &&
          state.counters.sourceRecordCount >= options.maxRecords
        ) {
          stopAfterRecordLimit = true;
          break;
        }
        state.counters.sourceRecordCount += 1;
        const parsed = /** @type {unknown} */ (JSON.parse(line));
        if (!isSunbizZipExtractedRecord(parsed)) {
          state.counters.invalidRecordCount += 1;
          continue;
        }
        const bundle = transformSunbizRecord(parsed, {
          sourceDataUri: chunk.sourceDataUri,
        });
        await writeBundle(state, writers, bundle);
        state.counters.transformedRecordCount += 1;
      }
    }

    const parts = await closeDatasetWriters(writers);
    const jobId = getStringProperty(manifest, "jobId");
    const summary = {
      schemaVersion: OUTPUT_SCHEMA_VERSION,
      transformedAt: new Date().toISOString(),
      sourceManifestS3Uri: options.manifestS3Uri,
      sourceJobId: jobId,
      stoppedAfterMaxChunks:
        options.maxChunks !== null &&
        state.counters.sourceChunkCount >= options.maxChunks,
      stoppedAfterMaxRecords: stopAfterRecordLimit,
      partRecordLimit: options.partRecordLimit,
      counters: state.counters,
      outputParts: parts,
      lexiconMapping: {
        dataGroup: "Business Registration",
        classes: [
          "company",
          "business_registration",
          "business_registration_address",
          "business_registration_party",
          "address",
        ],
        relationships: [
          "company_has_business_registration",
          "business_registration_has_address",
          "business_registration_address_has_address",
          "business_registration_has_party",
          "business_registration_party_has_address",
        ],
      },
    };
    await writeOutputJson(
      options.s3Client,
      options.outputLocation,
      "summary.json",
      summary,
    );
    return summary;
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
}

/**
 * Parse a positive integer CLI argument.
 *
 * @param {string | boolean | undefined} value - Raw parseArgs value.
 * @param {string} optionName - Option name for error messages.
 * @returns {number | null} Parsed positive integer or null when absent.
 */
function parseOptionalPositiveInteger(value, optionName) {
  if (value === undefined) return null;
  if (typeof value !== "string")
    throw new Error(`--${optionName} must be a number`);
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`--${optionName} must be a positive integer`);
  }
  return parsed;
}

/**
 * Read a required string CLI argument.
 *
 * @param {Record<string, string | boolean | undefined>} values - parseArgs values.
 * @param {string} optionName - Required option name.
 * @returns {string} Option value.
 */
function requireStringOption(values, optionName) {
  const value = values[optionName];
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`--${optionName} is required`);
  }
  return value;
}

/**
 * Build an output destination from CLI options.
 *
 * @param {Record<string, string | boolean | undefined>} values - parseArgs values.
 * @returns {OutputLocation} Parsed output location.
 */
function parseOutputLocation(values) {
  const outputS3Uri = values["output-s3-uri"];
  const outputDir = values["output-dir"];
  if (typeof outputS3Uri === "string" && typeof outputDir === "string") {
    throw new Error("Use only one of --output-s3-uri or --output-dir");
  }
  if (typeof outputS3Uri === "string") {
    const { bucket, key } = parseS3Uri(
      outputS3Uri.replace(/\/$/, "/_placeholder"),
    );
    const keyPrefix = key.replace(/\/_placeholder$/, "").replace(/\/$/, "");
    return { kind: "s3", bucket, keyPrefix };
  }
  if (typeof outputDir === "string") {
    return { kind: "local", dir: outputDir };
  }
  throw new Error("One of --output-s3-uri or --output-dir is required");
}

/**
 * CLI entrypoint.
 *
 * @returns {Promise<void>}
 */
export async function main() {
  const { values } = parseArgs({
    options: {
      "manifest-s3-uri": { type: "string" },
      "output-s3-uri": { type: "string" },
      "output-dir": { type: "string" },
      "max-chunks": { type: "string" },
      "max-records": { type: "string" },
      "part-record-limit": { type: "string" },
    },
    strict: true,
    allowPositionals: false,
  });

  const manifestS3Uri = requireStringOption(values, "manifest-s3-uri");
  const outputLocation = parseOutputLocation(values);
  const maxChunks = parseOptionalPositiveInteger(
    values["max-chunks"],
    "max-chunks",
  );
  const maxRecords = parseOptionalPositiveInteger(
    values["max-records"],
    "max-records",
  );
  const partRecordLimit =
    parseOptionalPositiveInteger(
      values["part-record-limit"],
      "part-record-limit",
    ) ?? DEFAULT_PART_RECORD_LIMIT;

  const summary = await processSunbizManifest({
    s3Client: new S3Client({}),
    manifestS3Uri,
    outputLocation,
    maxChunks,
    maxRecords,
    partRecordLimit,
  });

  console.log(JSON.stringify(summary, null, 2));
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
