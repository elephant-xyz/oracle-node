/**
 * Match live BCPA `useCode` labels to the published Broward mapping table.
 *
 * The appraiser sometimes returns a family label (`04 - Condominium`) instead
 * of a subtype (`04-01 CONDOMINIUM - RESIDENTIAL`). The published extractor
 * only compares the raw string and then crashes on `undefined`.
 */

/**
 * @typedef {object} BrowardUseCodeMapping
 * @property {string} property_usecode - Mapping table label, usually `NN-NN DESCRIPTION`.
 * @property {string} property_usage_type - Lexicon usage type.
 * @property {string} property_type - Lexicon property type.
 * @property {string | null} ownership_estate_type - Estate type.
 * @property {string | null} structure_form - Structure form.
 * @property {string | null} build_status - VacantLand or Improved.
 */

/**
 * First whitespace-delimited token of a mapping label (`01-01`, `04-01`).
 *
 * @param {string} mappingLabel - `property_usecode` value.
 * @returns {string} Code token.
 */
export function mappingUseCodeToken(mappingLabel) {
  return mappingLabel.trim().split(/\s+/u)[0] ?? "";
}

/**
 * Leading DOR family (`04`) or subtype (`04-01`) from a live use-code string.
 *
 * @param {string} useCode - Live `parcelInfo.useCode`.
 * @returns {{ family: string | undefined, subtype: string | undefined }} Parsed prefix.
 */
export function parseBrowardUseCodePrefix(useCode) {
  const compact = useCode.trim().toUpperCase();
  const subtypeMatch = compact.match(/^(\d{2}-\d{2})\b/u);
  const familyMatch = compact.match(/^(\d{2})(?!\d)/u);
  return {
    family: familyMatch?.[1],
    subtype: subtypeMatch?.[1],
  };
}

/**
 * Find the mapping row for a live BCPA use-code label.
 *
 * Preference: exact subtype token, then case-insensitive full-string includes,
 * then the family's `-01` row, then the first row in that family.
 *
 * @param {string} useCode - Live `parcelInfo.useCode`.
 * @param {readonly BrowardUseCodeMapping[]} mappings - Published mapping table.
 * @returns {BrowardUseCodeMapping | undefined} Matching row, if any.
 */
export function findBrowardPropertyMapping(useCode, mappings) {
  const trimmed = useCode.trim();
  if (trimmed === "") return undefined;
  const { family, subtype } = parseBrowardUseCodePrefix(trimmed);
  if (subtype !== undefined) {
    const exact = mappings.find(
      (mapping) => mappingUseCodeToken(mapping.property_usecode) === subtype,
    );
    if (exact !== undefined) return exact;
  }
  const lower = trimmed.toLowerCase();
  const includesHit = mappings.find((mapping) =>
    mapping.property_usecode.toLowerCase().includes(lower),
  );
  if (includesHit !== undefined) return includesHit;
  if (family === undefined) return undefined;
  const preferred = mappings.find(
    (mapping) =>
      mappingUseCodeToken(mapping.property_usecode) === `${family}-01`,
  );
  if (preferred !== undefined) return preferred;
  return mappings.find((mapping) =>
    mappingUseCodeToken(mapping.property_usecode).startsWith(`${family}-`),
  );
}

/**
 * CommonJS source copied into a packaged Broward scripts ZIP.
 *
 * @type {string}
 */
export const BROWARD_USE_CODE_MATCHER_CJS = `"use strict";

function mappingUseCodeToken(mappingLabel) {
  return mappingLabel.trim().split(/\\s+/u)[0] || "";
}

function parseBrowardUseCodePrefix(useCode) {
  const compact = useCode.trim().toUpperCase();
  const subtypeMatch = compact.match(/^(\\d{2}-\\d{2})\\b/u);
  const familyMatch = compact.match(/^(\\d{2})(?!\\d)/u);
  return {
    family: familyMatch ? familyMatch[1] : undefined,
    subtype: subtypeMatch ? subtypeMatch[1] : undefined,
  };
}

function findBrowardPropertyMapping(useCode, mappings) {
  const trimmed = String(useCode || "").trim();
  if (!trimmed) return undefined;
  const parsed = parseBrowardUseCodePrefix(trimmed);
  if (parsed.subtype) {
    const exact = mappings.find(function (mapping) {
      return mappingUseCodeToken(mapping.property_usecode) === parsed.subtype;
    });
    if (exact) return exact;
  }
  const lower = trimmed.toLowerCase();
  const includesHit = mappings.find(function (mapping) {
    return mapping.property_usecode.toLowerCase().includes(lower);
  });
  if (includesHit) return includesHit;
  if (!parsed.family) return undefined;
  const preferred = mappings.find(function (mapping) {
    return mappingUseCodeToken(mapping.property_usecode) === parsed.family + "-01";
  });
  if (preferred) return preferred;
  return mappings.find(function (mapping) {
    return mappingUseCodeToken(mapping.property_usecode).startsWith(parsed.family + "-");
  });
}

module.exports = {
  findBrowardPropertyMapping: findBrowardPropertyMapping,
  mappingUseCodeToken: mappingUseCodeToken,
  parseBrowardUseCodePrefix: parseBrowardUseCodePrefix,
};
`;

/**
 * Published matching block that crashes when `propertyMapping` is undefined.
 *
 * @type {string}
 */
export const PUBLISHED_BROWARD_USE_CODE_MATCH = `  const useCode = (parcelInfo.useCode || "").trim();
  const propertyMapping = propertyUseCodeMappings.find(mapping => {
    const mappingCode = mapping.property_usecode.split(' ')[0]; // Extract code part (e.g., "01-01")
    return mappingCode === useCode || mapping.property_usecode.startsWith(useCode);
  }) || propertyUseCodeMappings.find(mapping => 
    mapping.property_usecode.toLowerCase().includes(useCode.toLowerCase())
  );
  
  const propertyFields = {
    property_type: propertyMapping.property_type,`;

/**
 * Replacement matching block that family-falls-back and fails loud.
 *
 * @type {string}
 */
export const FIXED_BROWARD_USE_CODE_MATCH = `  const { findBrowardPropertyMapping } = require("./findBrowardPropertyMapping.js");
  const useCode = (parcelInfo.useCode || "").trim();
  const propertyMapping = findBrowardPropertyMapping(useCode, propertyUseCodeMappings);
  if (!propertyMapping) {
    errorOut("Unmapped Broward useCode: " + useCode, "property.property_usage_type");
  }
  const propertyFields = {
    property_type: propertyMapping.property_type,`;
