/**
 * Montgomery County municipal permit harvest helpers & roof intelligence model.
 * Supports Lower Merion, Norristown, Abington, and other PA municipalities.
 *
 * @module scripts/montgomery-discovery/montgomery-permits
 */

export const MONTGOMERY_PERMIT_SOURCE_SYSTEM = "montgomery_permits";

const ROOF_KEYWORDS = [
  "roof",
  "re-roof",
  "reroof",
  "shingle",
  "slate",
  "metal roof",
  "epdm",
  "rubber roof",
  "membrane",
  "standing seam",
  "roof replacement",
  "roof repair",
];

/**
 * @param {string | null | undefined} text
 * @returns {boolean}
 */
export function isRoofPermit(text) {
  if (!text) return false;
  const lower = String(text).toLowerCase();
  return ROOF_KEYWORDS.some((kw) => lower.includes(kw));
}

/**
 * Compute synthetic roof age from built year, remodel year, and permit issue year.
 *
 * @param {Object} params
 * @param {number | null | undefined} params.builtYear
 * @param {number | null | undefined} params.remodelYear
 * @param {number | null | undefined} params.reRoofPermitYear
 * @param {number} [params.currentYear=2026]
 * @returns {{ roofAgeYears: number | null; roofDate: string | null; calculationMethod: string }}
 */
export function calculateRoofAge({
  builtYear,
  remodelYear,
  reRoofPermitYear,
  currentYear = 2026,
}) {
  const bYear =
    builtYear && Number.isFinite(Number(builtYear)) && Number(builtYear) > 1800
      ? Number(builtYear)
      : null;
  const rYear =
    remodelYear &&
    Number.isFinite(Number(remodelYear)) &&
    Number(remodelYear) > 1800
      ? Number(remodelYear)
      : null;
  const pYear =
    reRoofPermitYear &&
    Number.isFinite(Number(reRoofPermitYear)) &&
    Number(reRoofPermitYear) > 1800
      ? Number(reRoofPermitYear)
      : null;

  if (pYear) {
    return {
      roofAgeYears: Math.max(0, currentYear - pYear),
      roofDate: `${pYear}-01-01`,
      calculationMethod: "PermitIssueYear",
    };
  }

  if (rYear && (!bYear || rYear >= bYear)) {
    return {
      roofAgeYears: Math.max(0, currentYear - rYear),
      roofDate: `${rYear}-01-01`,
      calculationMethod: "RemodelYear",
    };
  }

  if (bYear) {
    return {
      roofAgeYears: Math.max(0, currentYear - bYear),
      roofDate: `${bYear}-01-01`,
      calculationMethod: "StructureBuiltYear",
    };
  }

  return {
    roofAgeYears: null,
    roofDate: null,
    calculationMethod: "Unknown",
  };
}

/**
 * @typedef {Object} NormalizedMontgomeryPermit
 * @property {string} source_system
 * @property {string} permit_number
 * @property {string} parcel_identifier
 * @property {string} municipality_name
 * @property {string | null} permit_type
 * @property {string | null} work_description
 * @property {string | null} issue_date
 * @property {number | null} valuation_amount
 * @property {boolean} is_roof_permit
 * @property {string | null} contractor_name
 * @property {string} status
 */

/**
 * Build normalized permit row for Montgomery County municipal portals.
 *
 * @param {Object} input
 * @param {string} input.taxpin
 * @param {string} input.permitNumber
 * @param {string} input.muniName
 * @param {string} [input.permitType]
 * @param {string} [input.description]
 * @param {string} [input.issueDate]
 * @param {number} [input.valuation]
 * @param {string} [input.contractor]
 * @param {string} [input.status="ISSUED"]
 * @returns {NormalizedMontgomeryPermit}
 */
export function buildNormalizedMontgomeryPermit({
  taxpin,
  permitNumber,
  muniName,
  permitType = null,
  description = null,
  issueDate = null,
  valuation = null,
  contractor = null,
  status = "ISSUED",
}) {
  const isRoof = isRoofPermit(`${permitType || ""} ${description || ""}`);

  return {
    source_system: MONTGOMERY_PERMIT_SOURCE_SYSTEM,
    permit_number: permitNumber,
    parcel_identifier: taxpin,
    municipality_name: muniName,
    permit_type: permitType,
    work_description: description,
    issue_date: issueDate,
    valuation_amount: valuation,
    is_roof_permit: isRoof,
    contractor_name: contractor,
    status,
  };
}
