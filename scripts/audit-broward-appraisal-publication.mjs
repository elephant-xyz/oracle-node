#!/usr/bin/env node
// @ts-check

/**
 * Fail-closed privacy and publication audit for transformed Broward County
 * Property Appraiser (BCPA) artifacts.
 *
 * The transformed archives are private inputs. This tool inventories denied
 * publication fields without logging their values, then independently checks a
 * proposed public Parquet derivative against a closed property-fact allowlist,
 * approved sidecar names, source parcel identities, and expected row counts.
 * It has no upload or publication capability.
 */

import { readFile, readdir, stat, writeFile } from "node:fs/promises";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

import { ParquetReader } from "@dsnp/parquetjs";
import AdmZip from "adm-zip";

const POLICY_VERSION = "broward-appraisal-publication-v1";
const MAX_FINDING_EXAMPLES = 100;
const ARTIFACT_NAME_PATTERN = /^([0-9A-Z]{12})\.zip$/u;

/**
 * Exact row fields allowed in a proposed Broward appraisal public derivative.
 * The allowlist is intentionally narrower than the internal Donphan query
 * schema. In particular, it excludes owner, company, person, contact, mailing
 * and situs address, legal-description, source-request, linked-content, and
 * relationship fields.
 */
export const APPROVED_PUBLIC_FIELDS = Object.freeze([
  "property_id",
  "parcel_identifier",
  "source_system",
  "county_name",
  "county_fips",
  "state_code",
  "latitude",
  "longitude",
  "parcel_geometry_wkt",
  "lot_size_acre",
  "lot_area_sqft",
  "exterior_wall_material",
  "roof_covering_material",
  "property_type",
  "property_usage_type",
  "built_year",
  "effective_built_year",
  "livable_floor_area",
  "area_under_air",
  "total_area",
  "number_of_units",
  "assessed_value",
  "market_value",
  "building_value",
  "land_value",
  "taxable_value",
  "avm_value",
  "tax_year",
  "last_sale_date",
  "last_sale_price",
]);

const APPROVED_PUBLIC_FIELD_SET = new Set(APPROVED_PUBLIC_FIELDS);
const APPROVED_SIDECAR_NAMES = new Set([
  "coverage.json",
  "manifest.json",
  "privacy-scan.json",
  "query-table-manifest.json",
  "schema.json",
]);
const MANIFEST_NAMES = new Set(["manifest.json", "query-table-manifest.json"]);

const EMAIL_PATTERN = /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/iu;
const PHONE_PATTERN =
  /(?:\+?1[\s.-]?)?(?:\(\d{3}\)|\d{3})[\s.-]\d{3}[\s.-]\d{4}/u;
const SSN_PATTERN = /\b\d{3}-\d{2}-\d{4}\b/u;
const PO_BOX_PATTERN = /\bP(?:OST)?\.?\s*O(?:FFICE)?\.?\s+BOX\s+\d+\b/iu;
const STREET_ADDRESS_PATTERN =
  /\b\d{1,6}\s+[A-Z0-9][A-Z0-9 .'-]{1,60}\s(?:AVE(?:NUE)?|BLVD|BOULEVARD|CIR(?:CLE)?|CT|COURT|DR(?:IVE)?|HWY|HIGHWAY|LN|LANE|PKWY|PARKWAY|PL|PLACE|RD|ROAD|ST|STREET|TER(?:RACE)?|TRL|TRAIL|WAY)\b/iu;

/**
 * @typedef {"audit_error"|"contact"|"count_mismatch"|"free_text"|"linked_content"|"mailing_address"|"owner_identity"|"sensitive_person_attribute"|"site_address"|"source_payload"|"unapproved_field"|"unsafe_sidecar"|"value_pattern"} FindingCategory
 *
 * @typedef {"public_derivative"|"source_artifact"|"source_directory"} FindingScope
 *
 * @typedef {object} FindingExample
 * @property {FindingScope} scope Audit surface where the denied condition was found.
 * @property {FindingCategory} category Stable policy classification.
 * @property {string} location File, ZIP entry, column, or JSON key path; never a field value.
 * @property {string} reason Human-readable policy reason without private data.
 *
 * @typedef {object} FindingSummary
 * @property {number} total Total findings, including examples omitted by the report cap.
 * @property {Record<string, number>} byCategory Complete counts grouped by stable category.
 * @property {FindingExample[]} examples Bounded, value-free finding locations.
 * @property {boolean} examplesTruncated Whether the bounded examples omit findings.
 *
 * @typedef {object} ValidationSet
 * @property {string | null} path Validation-summary path, when supplied.
 * @property {Set<string> | null} parcelIdentifiers Expected canonical folios, when supplied.
 * @property {number} expectedCount Count established by the summary or explicit CLI argument.
 * @property {FindingSummary} findings Validation-summary reconciliation findings.
 *
 * @typedef {object} SourceAudit
 * @property {string} directory Absolute transformed-artifact directory.
 * @property {number} expectedArtifactCount Bounded count required by policy.
 * @property {number} artifactCount Canonical transformed ZIP count found.
 * @property {number} distinctParcelCount Distinct parcel identifiers read from property records.
 * @property {string[]} artifactParcelIdentifiers Sorted canonical parcel identifiers.
 * @property {number} unsafeSidecarCount Files or directories that must not accompany a public derivative.
 * @property {Record<string, boolean>} countChecks Source count and identity checks.
 * @property {FindingSummary} deniedFindings Denied publication content in raw transformed artifacts.
 * @property {FindingSummary} auditFindings Malformed input and count reconciliation failures.
 * @property {boolean} rawPublicationAllowed Always false when raw content or audit findings exist.
 * @property {boolean} reconciled Whether the bounded private source inventory completed exactly.
 *
 * @typedef {object} PublicDerivativeAudit
 * @property {string} directory Absolute proposed public-derivative directory.
 * @property {string[]} dataFiles Public Parquet files inspected.
 * @property {string[]} sidecarFiles Approved-name JSON sidecars inspected.
 * @property {string[]} fields Physical Parquet fields.
 * @property {number} rowCount Physical Parquet row count.
 * @property {number} distinctParcelCount Distinct non-empty public parcel identifiers.
 * @property {number | null} manifestRowCount Declared row count, or null when unavailable.
 * @property {Record<string, boolean>} countChecks Derivative/source/manifest reconciliation checks.
 * @property {FindingSummary} deniedFindings Public field, value, and sidecar findings.
 * @property {boolean} passed Whether every automated derivative gate passed.
 *
 * @typedef {object} PublicationAuditReport
 * @property {string} generatedAt ISO-8601 audit timestamp.
 * @property {string} policyVersion Closed policy identifier.
 * @property {{county:string,source:string,approvedFactClasses:string[],deniedClasses:string[],humanApprovalRequired:boolean}} policy Policy summary.
 * @property {SourceAudit} source Raw transformed-artifact classification.
 * @property {PublicDerivativeAudit | null} publicDerivative Proposed public derivative, when supplied.
 * @property {{passed:boolean,decision:"AUDIT_PASS_HUMAN_APPROVAL_REQUIRED"|"REFUSE_PUBLICATION",publicationAuthorized:false,humanApprovalRequired:true,reasons:string[]}} publicationGate Technical release gate.
 *
 * @typedef {object} AuditOptions
 * @property {string} transformedDirectory Directory containing canonical transformed ZIP artifacts.
 * @property {string | null} validationSummaryPath Optional bounded validation summary.
 * @property {string | null} publicDirectory Optional proposed public derivative.
 * @property {number | null} expectedCount Explicit expected artifact/row count.
 *
 * @typedef {object} CliOptions
 * @property {AuditOptions} audit Audit-library options.
 * @property {string | null} reportPath Optional private local report path.
 * @property {boolean} help Whether help was requested.
 */

/**
 * Collect complete category counts while retaining only bounded, value-free
 * examples. Private values must never be copied into an audit report.
 */
class FindingCollector {
  /** @type {number} */
  total = 0;

  /** @type {Map<FindingCategory, number>} */
  counts = new Map();

  /** @type {FindingExample[]} */
  examples = [];

  /**
   * Record one policy finding.
   *
   * @param {FindingScope} scope Audit surface.
   * @param {FindingCategory} category Stable finding category.
   * @param {string} location Value-free file, field, or key path.
   * @param {string} reason Value-free explanation.
   * @returns {void}
   */
  add(scope, category, location, reason) {
    this.total += 1;
    this.counts.set(category, (this.counts.get(category) ?? 0) + 1);
    if (this.examples.length < MAX_FINDING_EXAMPLES) {
      this.examples.push({ scope, category, location, reason });
    }
  }

  /**
   * Return a stable serializable summary.
   *
   * @returns {FindingSummary} Complete counts and bounded examples.
   */
  summary() {
    return {
      total: this.total,
      byCategory: Object.fromEntries(
        [...this.counts.entries()].sort(([left], [right]) =>
          left < right ? -1 : left > right ? 1 : 0,
        ),
      ),
      examples: this.examples,
      examplesTruncated: this.examples.length < this.total,
    };
  }
}

/**
 * Return true only for a non-array JSON object.
 *
 * @param {unknown} value Candidate parsed value.
 * @returns {value is Record<string, unknown>} Whether the value is an object.
 */
function isJsonObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Normalize a field or path segment for policy matching.
 *
 * @param {string} value Field or path text.
 * @returns {string} Lowercase underscore-delimited text.
 */
function normalizedName(value) {
  return value
    .replace(/([a-z0-9])([A-Z])/gu, "$1_$2")
    .replace(/[^A-Z0-9]+/giu, "_")
    .replace(/^_+|_+$/gu, "")
    .toLowerCase();
}

/**
 * Classify a key, column, file, or relationship name into every applicable
 * denied publication class. Matching is semantic and does not inspect values.
 *
 * @param {string} value Name to classify.
 * @returns {FindingCategory[]} Zero or more denied categories.
 */
export function classifyDeniedName(value) {
  const name = normalizedName(value);
  /** @type {FindingCategory[]} */
  const categories = [];
  const add = /** @param {FindingCategory} category */ (category) => {
    if (!categories.includes(category)) categories.push(category);
  };

  if (
    /(?:^|_)(mail|mailing|postal)(?:_|$)/u.test(name) &&
    /(?:^|_)(address|street|city|state|zip|postal|location)(?:_|$)/u.test(name)
  ) {
    add("mailing_address");
  }
  if (
    /(?:^|_)(owner|ownership|person|company|corporation|business|taxpayer|trustee|grantor|grantee|buyer|seller)(?:_|$)/u.test(
      name,
    ) ||
    /(?:^|_)(first|middle|last|full|prefix|suffix)_name(?:_|$)/u.test(name)
  ) {
    add("owner_identity");
  }
  if (/(?:^|_)(email|e_mail|phone|telephone|fax|contact)(?:_|$)/u.test(name)) {
    add("contact");
  }
  if (
    /(?:^|_)(birth|citizenship|veteran|ssn|social_security)(?:_|$)/u.test(name)
  ) {
    add("sensitive_person_attribute");
  }
  if (
    categories.includes("mailing_address") === false &&
    /(?:^|_)(address|street|city|zip|postal_code|subdivision)(?:_|$)/u.test(
      name,
    ) &&
    !/^(county_name|state_code)$/u.test(name)
  ) {
    add("site_address");
  }
  if (
    /(?:^|_)(legal_description|description_text|notes|remarks|free_text)(?:_|$)/u.test(
      name,
    )
  ) {
    add("free_text");
  }
  if (
    /(?:^|_)(source_http_request|raw|payload|prepared_input|request_body|response_body)(?:_|$)/u.test(
      name,
    )
  ) {
    add("source_payload");
  }
  if (
    /(?:^|_)(ipfs|cid|url|image|photo|picture|document|file|fact_sheet|index_html)(?:_|$)/u.test(
      name,
    )
  ) {
    add("linked_content");
  }
  return categories;
}

/**
 * Detect recognizable sensitive value formats without retaining or returning
 * the source value itself.
 *
 * @param {string} value Candidate string value.
 * @returns {FindingCategory[]} Value-level finding categories.
 */
function classifyDeniedValue(value) {
  /** @type {FindingCategory[]} */
  const categories = [];
  if (EMAIL_PATTERN.test(value) || PHONE_PATTERN.test(value)) {
    categories.push("contact");
  }
  if (SSN_PATTERN.test(value)) categories.push("sensitive_person_attribute");
  if (PO_BOX_PATTERN.test(value)) categories.push("mailing_address");
  if (STREET_ADDRESS_PATTERN.test(value)) categories.push("site_address");
  return categories.length === 0 ? [] : [...new Set(categories)];
}

/**
 * Recursively scan JSON names and recognizable sensitive value formats. Paths
 * are reported, but values are deliberately discarded.
 *
 * @param {unknown} value Parsed JSON value.
 * @param {string} location Current value-free path.
 * @param {FindingScope} scope Audit surface.
 * @param {FindingCollector} findings Destination collector.
 * @returns {void}
 */
function scanJson(value, location, scope, findings) {
  if (Array.isArray(value)) {
    for (let index = 0; index < value.length; index += 1) {
      scanJson(value[index], `${location}[${index}]`, scope, findings);
    }
    return;
  }
  if (isJsonObject(value)) {
    for (const [key, child] of Object.entries(value)) {
      const childLocation = `${location}.${key}`;
      for (const category of classifyDeniedName(key)) {
        findings.add(
          scope,
          category,
          childLocation,
          "JSON key belongs to a denied publication class",
        );
      }
      if (
        key === "name" &&
        /\.fields\[\d+\]$/u.test(location) &&
        typeof child === "string" &&
        !APPROVED_PUBLIC_FIELD_SET.has(child)
      ) {
        const categories = classifyDeniedName(child);
        for (const category of categories.length > 0
          ? categories
          : /** @type {FindingCategory[]} */ (["unapproved_field"])) {
          findings.add(
            scope,
            category,
            childLocation,
            "Schema sidecar names a field outside the closed public allowlist",
          );
        }
      }
      scanJson(child, childLocation, scope, findings);
    }
    return;
  }
  if (typeof value === "string") {
    for (const category of classifyDeniedValue(value)) {
      findings.add(
        scope,
        category,
        location,
        "String matches a denied sensitive-value pattern",
      );
    }
  }
}

/**
 * Read and reconcile an optional validation summary. If no summary is given,
 * an explicit expected count is mandatory so a partial directory cannot
 * silently define its own denominator.
 *
 * @param {string | null} summaryPath Optional summary path.
 * @param {number | null} explicitExpectedCount Optional operator denominator.
 * @returns {Promise<ValidationSet>} Bounded validation set.
 */
async function readValidationSet(summaryPath, explicitExpectedCount) {
  const findings = new FindingCollector();
  if (summaryPath === null) {
    if (
      explicitExpectedCount === null ||
      !Number.isSafeInteger(explicitExpectedCount) ||
      explicitExpectedCount <= 0
    ) {
      throw new Error(
        "Supply --validation-summary or a positive --expected-count; the audit will not infer its denominator",
      );
    }
    return {
      path: null,
      parcelIdentifiers: null,
      expectedCount: explicitExpectedCount,
      findings: findings.summary(),
    };
  }

  const absolutePath = path.resolve(summaryPath);
  const parsed = /** @type {unknown} */ (
    JSON.parse(await readFile(absolutePath, "utf8"))
  );
  if (!isJsonObject(parsed) || !Array.isArray(parsed.results)) {
    throw new Error(
      "Validation summary must be an object with a results array",
    );
  }

  const identifiers = new Set();
  for (let index = 0; index < parsed.results.length; index += 1) {
    const item = parsed.results[index];
    if (!isJsonObject(item)) {
      findings.add(
        "source_directory",
        "audit_error",
        `validation-summary.results[${index}]`,
        "Validation result is not an object",
      );
      continue;
    }
    if (
      typeof item.requestIdentifier !== "string" ||
      !/^[0-9A-Z]{12}$/u.test(item.requestIdentifier)
    ) {
      findings.add(
        "source_directory",
        "audit_error",
        `validation-summary.results[${index}].requestIdentifier`,
        "Validation result has no canonical Broward folio",
      );
      continue;
    }
    if (item.validationSuccess !== true) {
      findings.add(
        "source_directory",
        "count_mismatch",
        `validation-summary.results[${index}].validationSuccess`,
        "Bounded validation result did not pass",
      );
    }
    if (identifiers.has(item.requestIdentifier)) {
      findings.add(
        "source_directory",
        "count_mismatch",
        `validation-summary.results[${index}].requestIdentifier`,
        "Bounded validation summary repeats a folio",
      );
    }
    identifiers.add(item.requestIdentifier);
  }

  const expectedCount = identifiers.size;
  for (const [field, value] of [
    ["total", parsed.total],
    ["capturesPassed", parsed.capturesPassed],
    ["transformsPassed", parsed.transformsPassed],
    ["validationsPassed", parsed.validationsPassed],
  ]) {
    if (value !== expectedCount) {
      findings.add(
        "source_directory",
        "count_mismatch",
        `validation-summary.${field}`,
        `Validation-summary ${field} does not equal its distinct bounded folio count`,
      );
    }
  }
  if (
    explicitExpectedCount !== null &&
    explicitExpectedCount !== expectedCount
  ) {
    findings.add(
      "source_directory",
      "count_mismatch",
      "validation-summary vs --expected-count",
      "Explicit expected count differs from the bounded validation summary",
    );
  }
  return {
    path: absolutePath,
    parcelIdentifiers: identifiers,
    expectedCount,
    findings: findings.summary(),
  };
}

/**
 * Merge a serializable finding summary into a mutable collector.
 *
 * @param {FindingCollector} destination Destination collector.
 * @param {FindingSummary} source Source summary.
 * @returns {void}
 */
function mergeFindings(destination, source) {
  for (const example of source.examples) {
    destination.add(
      example.scope,
      example.category,
      example.location,
      example.reason,
    );
  }
  const representedCounts = new Map();
  for (const example of source.examples) {
    representedCounts.set(
      example.category,
      (representedCounts.get(example.category) ?? 0) + 1,
    );
  }
  for (const [categoryText, count] of Object.entries(source.byCategory)) {
    const category = /** @type {FindingCategory} */ (categoryText);
    const missing = count - (representedCounts.get(category) ?? 0);
    for (let index = 0; index < missing; index += 1) {
      destination.total += 1;
      destination.counts.set(
        category,
        (destination.counts.get(category) ?? 0) + 1,
      );
    }
  }
}

/**
 * Audit one canonical transformed ZIP and return its property parcel identity.
 *
 * @param {string} artifactPath Canonical transformed ZIP path.
 * @param {FindingCollector} denied Denied-content collector.
 * @param {FindingCollector} audit Malformed-artifact collector.
 * @returns {string | null} Canonical parcel identity, or null on failure.
 */
function auditTransformedZip(artifactPath, denied, audit) {
  const artifactName = path.basename(artifactPath);
  let zip;
  try {
    zip = new AdmZip(artifactPath);
  } catch {
    audit.add(
      "source_artifact",
      "audit_error",
      artifactName,
      "Transformed artifact is not a readable ZIP",
    );
    return null;
  }

  /** @type {string | null} */
  let parcelIdentifier = null;
  for (const entry of zip.getEntries()) {
    const entryLocation = `${artifactName}!${entry.entryName}`;
    for (const category of classifyDeniedName(entry.entryName)) {
      denied.add(
        "source_artifact",
        category,
        entryLocation,
        "Transformed ZIP entry belongs to a denied publication class",
      );
    }
    if (entry.isDirectory) continue;
    if (!entry.entryName.endsWith(".json")) {
      denied.add(
        "source_artifact",
        "linked_content",
        entryLocation,
        "Non-JSON transformed content cannot accompany a public derivative",
      );
      continue;
    }
    try {
      const parsed = /** @type {unknown} */ (
        JSON.parse(entry.getData().toString("utf8"))
      );
      scanJson(parsed, entryLocation, "source_artifact", denied);
      if (entry.entryName === "data/property.json") {
        if (
          isJsonObject(parsed) &&
          typeof parsed.parcel_identifier === "string" &&
          /^[0-9A-Z]{12}$/u.test(parsed.parcel_identifier)
        ) {
          parcelIdentifier = parsed.parcel_identifier;
        } else {
          audit.add(
            "source_artifact",
            "audit_error",
            `${entryLocation}.parcel_identifier`,
            "Property record has no canonical Broward folio",
          );
        }
      }
    } catch {
      audit.add(
        "source_artifact",
        "audit_error",
        entryLocation,
        "JSON entry could not be parsed",
      );
    }
  }
  if (parcelIdentifier === null) {
    audit.add(
      "source_artifact",
      "audit_error",
      `${artifactName}!data/property.json`,
      "Required property identity was not found",
    );
  }
  return parcelIdentifier;
}

/**
 * Audit a bounded transformed-artifact directory. Canonical `<folio>.zip`
 * files are data artifacts; every other sibling is classified as an unsafe
 * publication sidecar but is not deleted or modified.
 *
 * @param {string} transformedDirectory Private artifact directory.
 * @param {ValidationSet} validationSet Bounded expected source set.
 * @returns {Promise<SourceAudit>} Private-source classification and reconciliation.
 */
async function auditSourceDirectory(transformedDirectory, validationSet) {
  const absoluteDirectory = path.resolve(transformedDirectory);
  const entries = await readdir(absoluteDirectory, { withFileTypes: true });
  const denied = new FindingCollector();
  const audit = new FindingCollector();
  mergeFindings(audit, validationSet.findings);
  /** @type {{fileName:string,folio:string}[]} */
  const artifacts = [];
  let unsafeSidecarCount = 0;

  for (const entry of entries) {
    const match = entry.isFile()
      ? ARTIFACT_NAME_PATTERN.exec(entry.name)
      : null;
    if (match !== null && match[1] !== undefined) {
      artifacts.push({ fileName: entry.name, folio: match[1] });
      continue;
    }
    unsafeSidecarCount += 1;
    audit.add(
      "source_directory",
      "unsafe_sidecar",
      entry.name,
      "Noncanonical sibling must never be copied into a public derivative",
    );
  }

  artifacts.sort((left, right) =>
    left.fileName < right.fileName
      ? -1
      : left.fileName > right.fileName
        ? 1
        : 0,
  );
  const parcelIdentifiers = new Set();
  for (const artifact of artifacts) {
    const parcelIdentifier = auditTransformedZip(
      path.join(absoluteDirectory, artifact.fileName),
      denied,
      audit,
    );
    if (parcelIdentifier === null) continue;
    if (parcelIdentifier !== artifact.folio) {
      audit.add(
        "source_artifact",
        "count_mismatch",
        artifact.fileName,
        "Artifact filename folio does not match property parcel identifier",
      );
    }
    if (parcelIdentifiers.has(parcelIdentifier)) {
      audit.add(
        "source_artifact",
        "count_mismatch",
        artifact.fileName,
        "Multiple transformed artifacts resolve to the same parcel identifier",
      );
    }
    parcelIdentifiers.add(parcelIdentifier);
  }

  const expectedIdentifiers = validationSet.parcelIdentifiers;
  const artifactIdentifiers = new Set(
    artifacts.map((artifact) => artifact.folio),
  );
  const expectedIdentitySetMatched =
    expectedIdentifiers === null ||
    (expectedIdentifiers.size === artifactIdentifiers.size &&
      [...expectedIdentifiers].every((folio) =>
        artifactIdentifiers.has(folio),
      ));
  const countChecks = {
    artifactCountMatchesExpected:
      artifacts.length === validationSet.expectedCount,
    distinctPropertyCountMatchesExpected:
      parcelIdentifiers.size === validationSet.expectedCount,
    artifactNamesMatchValidationSet: expectedIdentitySetMatched,
    validationSummaryClean: validationSet.findings.total === 0,
  };
  for (const [name, passed] of Object.entries(countChecks)) {
    if (!passed) {
      audit.add(
        "source_directory",
        "count_mismatch",
        name,
        "Private source denominator, identities, or artifact counts do not reconcile",
      );
    }
  }

  const deniedSummary = denied.summary();
  const auditSummary = audit.summary();
  return {
    directory: absoluteDirectory,
    expectedArtifactCount: validationSet.expectedCount,
    artifactCount: artifacts.length,
    distinctParcelCount: parcelIdentifiers.size,
    artifactParcelIdentifiers: [...parcelIdentifiers].sort(),
    unsafeSidecarCount,
    countChecks,
    deniedFindings: deniedSummary,
    auditFindings: auditSummary,
    rawPublicationAllowed:
      deniedSummary.total === 0 && auditSummary.total === 0,
    reconciled:
      Object.values(countChecks).every(Boolean) &&
      auditSummary.byCategory.audit_error === undefined &&
      auditSummary.byCategory.count_mismatch === undefined,
  };
}

/**
 * Recursively list public-derivative directory entries without following
 * symlinks. Nested paths and symlinks remain visible so they can fail closed.
 *
 * @param {string} root Absolute derivative root.
 * @param {string} [relativeDirectory] Current relative directory.
 * @returns {Promise<{relativePath:string,type:"directory"|"file"|"symlink"|"other"}[]>} Entries.
 */
async function listDerivativeEntries(root, relativeDirectory = "") {
  const directory = path.join(root, relativeDirectory);
  const entries = await readdir(directory, { withFileTypes: true });
  /** @type {{relativePath:string,type:"directory"|"file"|"symlink"|"other"}[]} */
  const listed = [];
  for (const entry of entries) {
    const relativePath = path.join(relativeDirectory, entry.name);
    if (entry.isSymbolicLink()) {
      listed.push({ relativePath, type: "symlink" });
    } else if (entry.isDirectory()) {
      listed.push({ relativePath, type: "directory" });
      listed.push(...(await listDerivativeEntries(root, relativePath)));
    } else if (entry.isFile()) {
      listed.push({ relativePath, type: "file" });
    } else {
      listed.push({ relativePath, type: "other" });
    }
  }
  return listed;
}

/**
 * Scan one approved-name JSON sidecar and return a declared row count when it
 * is a recognized manifest. Sidecar values are never copied to findings.
 *
 * @param {string} root Public derivative root.
 * @param {string} relativePath Sidecar relative path.
 * @param {FindingCollector} findings Public finding collector.
 * @returns {Promise<number | null>} Declared manifest row count.
 */
async function auditJsonSidecar(root, relativePath, findings) {
  const fileName = path.basename(relativePath);
  let parsed;
  try {
    parsed = /** @type {unknown} */ (
      JSON.parse(await readFile(path.join(root, relativePath), "utf8"))
    );
  } catch {
    findings.add(
      "public_derivative",
      "audit_error",
      relativePath,
      "Approved-name JSON sidecar is unreadable or malformed",
    );
    return null;
  }
  scanJson(parsed, relativePath, "public_derivative", findings);
  if (!MANIFEST_NAMES.has(fileName)) return null;
  if (
    !isJsonObject(parsed) ||
    !Number.isSafeInteger(parsed.rowCount) ||
    Number(parsed.rowCount) < 0
  ) {
    findings.add(
      "public_derivative",
      "count_mismatch",
      `${relativePath}.rowCount`,
      "Manifest must declare a non-negative integer rowCount",
    );
    return null;
  }
  return Number(parsed.rowCount);
}

/**
 * Validate physical Parquet schema, rows, value patterns, parcel uniqueness,
 * and exact source parcel identity. Denied values are reported only by field
 * location and category.
 *
 * @param {string} parquetPath Public Parquet path.
 * @param {Set<string>} sourceParcelIdentifiers Reconciled private source identities.
 * @param {FindingCollector} findings Public finding collector.
 * @returns {Promise<{fields:string[],rowCount:number,parcelIdentifiers:Set<string>}>} Physical inspection.
 */
async function auditParquet(parquetPath, sourceParcelIdentifiers, findings) {
  const relativeName = path.basename(parquetPath);
  let reader;
  try {
    reader = await ParquetReader.openFile(parquetPath);
  } catch {
    findings.add(
      "public_derivative",
      "audit_error",
      relativeName,
      "Parquet data file could not be opened",
    );
    return { fields: [], rowCount: 0, parcelIdentifiers: new Set() };
  }

  const fields = Object.keys(reader.schema.fields);
  for (const field of fields) {
    if (APPROVED_PUBLIC_FIELD_SET.has(field)) continue;
    const categories = classifyDeniedName(field);
    for (const category of categories.length > 0
      ? categories
      : /** @type {FindingCategory[]} */ (["unapproved_field"])) {
      findings.add(
        "public_derivative",
        category,
        `${relativeName}:${field}`,
        "Physical Parquet column is outside the closed public allowlist",
      );
    }
  }
  if (!fields.includes("parcel_identifier")) {
    findings.add(
      "public_derivative",
      "count_mismatch",
      `${relativeName}:parcel_identifier`,
      "Public derivative must include parcel_identifier for exact reconciliation",
    );
  }

  let rowCount = 0;
  const parcelIdentifiers = new Set();
  try {
    const cursor = reader.getCursor();
    for (
      let row = await cursor.next();
      row !== null;
      row = await cursor.next()
    ) {
      rowCount += 1;
      if (!isJsonObject(row)) {
        findings.add(
          "public_derivative",
          "audit_error",
          `${relativeName}:row[${rowCount - 1}]`,
          "Parquet cursor returned a non-object row",
        );
        continue;
      }
      for (const [field, value] of Object.entries(row)) {
        if (typeof value !== "string") continue;
        for (const category of classifyDeniedValue(value)) {
          findings.add(
            "public_derivative",
            category,
            `${relativeName}:row[${rowCount - 1}].${field}`,
            "Public value matches a denied sensitive-value pattern",
          );
        }
      }
      const parcelIdentifier = row.parcel_identifier;
      if (
        typeof parcelIdentifier !== "string" ||
        !/^[0-9A-Z]{12}$/u.test(parcelIdentifier)
      ) {
        findings.add(
          "public_derivative",
          "count_mismatch",
          `${relativeName}:row[${rowCount - 1}].parcel_identifier`,
          "Public row has no canonical Broward parcel identifier",
        );
        continue;
      }
      if (parcelIdentifiers.has(parcelIdentifier)) {
        findings.add(
          "public_derivative",
          "count_mismatch",
          `${relativeName}:row[${rowCount - 1}].parcel_identifier`,
          "Public derivative repeats a parcel identifier",
        );
      }
      if (!sourceParcelIdentifiers.has(parcelIdentifier)) {
        findings.add(
          "public_derivative",
          "count_mismatch",
          `${relativeName}:row[${rowCount - 1}].parcel_identifier`,
          "Public parcel identifier is absent from the bounded private source set",
        );
      }
      parcelIdentifiers.add(parcelIdentifier);
    }
  } catch {
    findings.add(
      "public_derivative",
      "audit_error",
      relativeName,
      "Parquet rows could not be fully read",
    );
  } finally {
    await reader.close();
  }
  return { fields, rowCount, parcelIdentifiers };
}

/**
 * Audit a proposed public derivative directory. Exactly one root-level
 * Parquet file and exactly one recognized manifest are required. Unknown,
 * nested, executable, linked, or raw sidecars fail closed.
 *
 * @param {string} publicDirectory Proposed derivative directory.
 * @param {SourceAudit} source Reconciled private source audit.
 * @returns {Promise<PublicDerivativeAudit>} Closed-schema derivative audit.
 */
async function auditPublicDerivative(publicDirectory, source) {
  const absoluteDirectory = path.resolve(publicDirectory);
  const entries = await listDerivativeEntries(absoluteDirectory);
  const findings = new FindingCollector();
  /** @type {string[]} */
  const dataFiles = [];
  /** @type {string[]} */
  const sidecarFiles = [];
  /** @type {number[]} */
  const manifestCounts = [];

  for (const entry of entries) {
    const nested = path.dirname(entry.relativePath) !== ".";
    if (
      entry.type !== "file" ||
      nested ||
      (entry.relativePath.endsWith(".parquet") === false &&
        !APPROVED_SIDECAR_NAMES.has(entry.relativePath))
    ) {
      findings.add(
        "public_derivative",
        "unsafe_sidecar",
        entry.relativePath,
        "Unknown, nested, linked, or non-file derivative entry is not publication-approved",
      );
      continue;
    }
    if (entry.relativePath.endsWith(".parquet")) {
      dataFiles.push(entry.relativePath);
    } else {
      sidecarFiles.push(entry.relativePath);
      const manifestCount = await auditJsonSidecar(
        absoluteDirectory,
        entry.relativePath,
        findings,
      );
      if (manifestCount !== null) manifestCounts.push(manifestCount);
    }
  }

  if (dataFiles.length !== 1) {
    findings.add(
      "public_derivative",
      "count_mismatch",
      "public-derivative:*.parquet",
      "Public derivative must contain exactly one root-level Parquet data file",
    );
  }
  if (manifestCounts.length !== 1) {
    findings.add(
      "public_derivative",
      "count_mismatch",
      "public-derivative:manifest",
      "Public derivative must contain exactly one readable manifest row count",
    );
  }

  let fields = /** @type {string[]} */ ([]);
  let rowCount = 0;
  let parcelIdentifiers = new Set();
  if (dataFiles.length === 1 && dataFiles[0] !== undefined) {
    const inspected = await auditParquet(
      path.join(absoluteDirectory, dataFiles[0]),
      new Set(source.artifactParcelIdentifiers),
      findings,
    );
    fields = inspected.fields;
    rowCount = inspected.rowCount;
    parcelIdentifiers = inspected.parcelIdentifiers;
  }
  const manifestRowCount =
    manifestCounts.length === 1 && manifestCounts[0] !== undefined
      ? manifestCounts[0]
      : null;
  const sourceIdentifiers = new Set(source.artifactParcelIdentifiers);
  const exactParcelSetMatched =
    parcelIdentifiers.size === sourceIdentifiers.size &&
    [...sourceIdentifiers].every((folio) => parcelIdentifiers.has(folio));
  const countChecks = {
    exactlyOneParquet: dataFiles.length === 1,
    exactlyOneManifestCount: manifestCounts.length === 1,
    rowCountMatchesSource: rowCount === source.artifactCount,
    rowCountMatchesExpected: rowCount === source.expectedArtifactCount,
    manifestRowCountMatchesPhysical:
      manifestRowCount !== null && manifestRowCount === rowCount,
    distinctParcelsMatchRows: parcelIdentifiers.size === rowCount,
    parcelIdentitySetMatchesSource: exactParcelSetMatched,
  };
  for (const [name, passed] of Object.entries(countChecks)) {
    if (!passed) {
      findings.add(
        "public_derivative",
        "count_mismatch",
        name,
        "Public derivative count or identity reconciliation failed",
      );
    }
  }
  const summary = findings.summary();
  return {
    directory: absoluteDirectory,
    dataFiles: dataFiles.sort(),
    sidecarFiles: sidecarFiles.sort(),
    fields,
    rowCount,
    distinctParcelCount: parcelIdentifiers.size,
    manifestRowCount,
    countChecks,
    deniedFindings: summary,
    passed:
      source.reconciled &&
      summary.total === 0 &&
      Object.values(countChecks).every(Boolean),
  };
}

/**
 * Run the complete fail-closed Broward appraisal publication audit.
 *
 * A clean automated result is not publication authorization. It means only
 * that the proposed derivative passed the coded policy and may proceed to the
 * documented legal/privacy/data-owner human review.
 *
 * @param {AuditOptions} options Paths and bounded denominator.
 * @returns {Promise<PublicationAuditReport>} Value-free audit report.
 */
export async function auditBrowardAppraisalPublication(options) {
  const validationSet = await readValidationSet(
    options.validationSummaryPath,
    options.expectedCount,
  );
  const source = await auditSourceDirectory(
    options.transformedDirectory,
    validationSet,
  );
  const publicDerivative =
    options.publicDirectory === null
      ? null
      : await auditPublicDerivative(options.publicDirectory, source);

  const reasons = [];
  if (!source.reconciled) {
    reasons.push("Private transformed source inventory did not reconcile.");
  }
  if (publicDerivative === null) {
    reasons.push("No proposed public derivative was supplied.");
  } else if (!publicDerivative.passed) {
    reasons.push(
      "Proposed public derivative has denied findings or count mismatches.",
    );
  }
  const passed =
    source.reconciled && publicDerivative !== null && publicDerivative.passed;
  if (passed) {
    reasons.push(
      "Automated checks passed; legal/privacy/data-owner human approval is still required.",
    );
  }
  return {
    generatedAt: new Date().toISOString(),
    policyVersion: POLICY_VERSION,
    policy: {
      county: "Broward County, Florida",
      source: "Broward County Property Appraiser transformed outputs",
      approvedFactClasses: [
        "property and parcel identity",
        "parcel geometry",
        "building and lot characteristics",
        "assessment and sale value facts",
        "property type and use",
      ],
      deniedClasses: [
        "owners, people, companies, trusts, buyers, sellers, and taxpayers",
        "phone, email, fax, and other contact data",
        "mailing and situs addresses",
        "sensitive person attributes",
        "legal descriptions and unreviewed free text",
        "raw requests, source payloads, linked files, images, CIDs, and fact sheets",
        "unknown or unreviewed fields and sidecars",
      ],
      humanApprovalRequired: true,
    },
    source,
    publicDerivative,
    publicationGate: {
      passed,
      decision: passed
        ? "AUDIT_PASS_HUMAN_APPROVAL_REQUIRED"
        : "REFUSE_PUBLICATION",
      publicationAuthorized: false,
      humanApprovalRequired: true,
      reasons,
    },
  };
}

/**
 * Parse one positive integer CLI option.
 *
 * @param {string | undefined} value Raw option value.
 * @param {string} name CLI option name.
 * @returns {number | null} Positive integer or null when omitted.
 */
function optionalPositiveInteger(value, name) {
  if (value === undefined) return null;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}

/**
 * Parse fail-closed CLI options.
 *
 * @param {readonly string[]} argv Arguments after the executable path.
 * @returns {CliOptions} Parsed CLI options.
 */
export function parseCli(argv) {
  const { values } = parseArgs({
    args: [...argv],
    options: {
      "transformed-dir": { type: "string" },
      "validation-summary": { type: "string" },
      "public-dir": { type: "string" },
      "expected-count": { type: "string" },
      report: { type: "string" },
      help: { type: "boolean", short: "h" },
    },
    strict: true,
    allowPositionals: false,
  });
  if (values.help === true) {
    return {
      audit: {
        transformedDirectory: "",
        validationSummaryPath: null,
        publicDirectory: null,
        expectedCount: null,
      },
      reportPath: null,
      help: true,
    };
  }
  if (
    typeof values["transformed-dir"] !== "string" ||
    values["transformed-dir"].trim() === ""
  ) {
    throw new Error("--transformed-dir is required");
  }
  return {
    audit: {
      transformedDirectory: values["transformed-dir"],
      validationSummaryPath: values["validation-summary"] ?? null,
      publicDirectory: values["public-dir"] ?? null,
      expectedCount: optionalPositiveInteger(
        values["expected-count"],
        "--expected-count",
      ),
    },
    reportPath: values.report ?? null,
    help: false,
  };
}

/**
 * Render CLI usage.
 *
 * @returns {string} Usage text.
 */
function usage() {
  return `Usage:
  node scripts/audit-broward-appraisal-publication.mjs \\
    --transformed-dir <private-artifact-directory> \\
    (--validation-summary <summary.json> | --expected-count <count>) \\
    [--public-dir <proposed-public-derivative>] \\
    [--report <private-local-report.json>]

The command never publishes or deletes data. It exits nonzero unless a proposed
derivative passes every coded field, sidecar, parcel-identity, and count check.
Even a zero exit requires human approval before release.
`;
}

/**
 * Ensure a local audit report cannot be accidentally written inside the
 * proposed public derivative directory.
 *
 * @param {string} reportPath Proposed report path.
 * @param {string | null} publicDirectory Proposed public root.
 * @returns {Promise<void>}
 */
async function assertPrivateReportPath(reportPath, publicDirectory) {
  if (publicDirectory === null) return;
  const report = path.resolve(reportPath);
  const publicRoot = path.resolve(publicDirectory);
  const relative = path.relative(publicRoot, report);
  if (
    relative === "" ||
    (!relative.startsWith("..") && !path.isAbsolute(relative))
  ) {
    throw new Error(
      "--report must be outside --public-dir because the report describes private source findings",
    );
  }
  try {
    const publicStats = await stat(publicRoot);
    if (!publicStats.isDirectory()) {
      throw new Error("--public-dir is not a directory");
    }
  } catch (caught) {
    if (
      caught instanceof Error &&
      caught.message === "--public-dir is not a directory"
    ) {
      throw caught;
    }
  }
}

/**
 * Execute the local CLI and set a fail-closed process result.
 *
 * @returns {Promise<void>}
 */
async function main() {
  const options = parseCli(process.argv.slice(2));
  if (options.help) {
    process.stdout.write(usage());
    return;
  }
  if (options.reportPath !== null) {
    await assertPrivateReportPath(
      options.reportPath,
      options.audit.publicDirectory,
    );
  }
  const report = await auditBrowardAppraisalPublication(options.audit);
  if (options.reportPath !== null) {
    await writeFile(
      path.resolve(options.reportPath),
      `${JSON.stringify(report, null, 2)}\n`,
      { encoding: "utf8", mode: 0o600 },
    );
  }
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
  if (!report.publicationGate.passed) process.exitCode = 1;
}

const invokedPath =
  process.argv[1] === undefined ? null : path.resolve(process.argv[1]);
if (invokedPath !== null && fileURLToPath(import.meta.url) === invokedPath) {
  main().catch((caught) => {
    const message = caught instanceof Error ? caught.message : String(caught);
    process.stderr.write(
      `${JSON.stringify({
        event: "broward_appraisal_publication_audit_failed",
        decision: "REFUSE_PUBLICATION",
        publicationAuthorized: false,
        error: message,
      })}\n`,
    );
    process.exitCode = 1;
  });
}
