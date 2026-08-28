/**
 * Safety contract for deferred Broward query-data-only transform artifacts.
 *
 * These archives contain the Lexicon JSON emitted before Elephant CLI's
 * expensive fact-sheet phase. They are suitable for schema validation and the
 * query-db appraisal loader, but they are deliberately not publication
 * artifacts.
 */

import { readFile } from "fs/promises";
import AdmZip from "adm-zip";

export const QUERY_DATA_ONLY_MODE = "query-data-only";
export const PUBLISHABLE_MODE = "publishable";
export const QUERY_DATA_ONLY_SCHEMA_VERSION =
  "oracle-node.broward-query-data-only.v1";
export const QUERY_DATA_ONLY_MANIFEST_ENTRY =
  "BROWARD_QUERY_DATA_ONLY_DO_NOT_PUBLISH.json";
export const QUERY_DATA_ONLY_SUFFIX = ".query-data-only.zip";

/**
 * @typedef {"publishable" | "query-data-only"} BrowardArtifactMode
 *
 * @typedef {object} QueryDataOnlyManifest
 * @property {string} schemaVersion - Stable local artifact contract.
 * @property {"query-data-only"} artifactMode - Explicit non-publication mode.
 * @property {false} publishable - Machine-readable publication prohibition.
 * @property {string} folio - Canonical Broward parcel identifier.
 * @property {string} generatedAt - ISO timestamp for this derived archive.
 * @property {readonly string[]} deferredOutputs - Outputs that require a later full transform.
 * @property {string} regeneration - Safe regeneration instruction.
 *
 * @typedef {object} QueryDataOnlyInspection
 * @property {QueryDataOnlyManifest} manifest - Parsed root safety marker.
 * @property {readonly string[]} dataEntries - Sorted data entry names.
 * @property {number} jsonEntryCount - Number of retained Lexicon JSON entries.
 */

/**
 * Determine whether a value is a non-array JSON object.
 *
 * @param {unknown} value - Candidate JSON value.
 * @returns {value is Record<string, unknown>} Whether the value is an object.
 */
function isJsonObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Return every relative IPLD file reference in a JSON value.
 *
 * Only objects shaped as `{ "/": "./file.json" }` are treated as links, so
 * ordinary source strings cannot be mistaken for archive references.
 *
 * @param {unknown} value - Parsed JSON value.
 * @returns {string[]} Referenced file names without the leading `./`.
 */
function collectRelativeLinks(value) {
  if (Array.isArray(value)) {
    return value.flatMap((item) => collectRelativeLinks(item));
  }
  if (!isJsonObject(value)) return [];
  const directLink = value["/"];
  const links =
    typeof directLink === "string" && directLink.startsWith("./")
      ? [directLink.slice(2)]
      : [];
  for (const nested of Object.values(value)) {
    links.push(...collectRelativeLinks(nested));
  }
  return links;
}

/**
 * Find a fact-sheet relationship key or deferred file reference in JSON.
 *
 * @param {unknown} value - Parsed JSON value.
 * @param {string} location - Human-readable path for an actionable failure.
 * @returns {string | null} First forbidden location, or null.
 */
function findDeferredReference(value, location) {
  if (Array.isArray(value)) {
    for (let index = 0; index < value.length; index += 1) {
      const found = findDeferredReference(
        value[index],
        `${location}[${index}]`,
      );
      if (found !== null) return found;
    }
    return null;
  }
  if (!isJsonObject(value)) {
    if (
      typeof value === "string" &&
      /(?:^|\/)(?:fact_sheet\.json|index\.html)$/iu.test(value)
    ) {
      return location;
    }
    return null;
  }
  for (const [key, nested] of Object.entries(value)) {
    if (/fact[_-]?sheet/iu.test(key)) return `${location}.${key}`;
    const found = findDeferredReference(nested, `${location}.${key}`);
    if (found !== null) return found;
  }
  return null;
}

/**
 * Parse and validate the root non-publication marker.
 *
 * @param {Buffer} bytes - Marker JSON bytes.
 * @returns {QueryDataOnlyManifest} Valid marker.
 */
function parseManifest(bytes) {
  const parsed = /** @type {unknown} */ (JSON.parse(bytes.toString("utf8")));
  if (
    !isJsonObject(parsed) ||
    parsed.schemaVersion !== QUERY_DATA_ONLY_SCHEMA_VERSION ||
    parsed.artifactMode !== QUERY_DATA_ONLY_MODE ||
    parsed.publishable !== false ||
    typeof parsed.folio !== "string" ||
    typeof parsed.generatedAt !== "string" ||
    !Array.isArray(parsed.deferredOutputs) ||
    !parsed.deferredOutputs.every((value) => typeof value === "string") ||
    typeof parsed.regeneration !== "string"
  ) {
    throw new Error("Invalid Broward query-data-only safety marker");
  }
  return /** @type {QueryDataOnlyManifest} */ (parsed);
}

/**
 * Validate that an in-memory ZIP is non-publishable, internally linked, and
 * free of stale fact-sheet references.
 *
 * @param {AdmZip} zip - Query-data-only archive.
 * @returns {QueryDataOnlyInspection} Validated archive metadata.
 */
export function inspectQueryDataOnlyZip(zip) {
  const marker = zip.getEntry(QUERY_DATA_ONLY_MANIFEST_ENTRY);
  if (marker === null) {
    throw new Error(
      `Missing non-publication marker ${QUERY_DATA_ONLY_MANIFEST_ENTRY}`,
    );
  }
  const manifest = parseManifest(marker.getData());
  const dataEntries = zip
    .getEntries()
    .filter(
      (entry) => !entry.isDirectory && entry.entryName.startsWith("data/"),
    )
    .map((entry) => entry.entryName)
    .sort();
  const dataNames = new Set(
    dataEntries.map((name) => name.slice("data/".length)),
  );
  if (!dataNames.has("property.json")) {
    throw new Error("Query-data-only artifact is missing data/property.json");
  }
  const forbiddenEntry = dataEntries.find(
    (name) =>
      /fact[_-]?sheet/iu.test(name) || /\.(?:html?|css|js)$/iu.test(name),
  );
  if (forbiddenEntry !== undefined) {
    throw new Error(
      `Query-data-only artifact contains deferred output ${forbiddenEntry}`,
    );
  }
  let jsonEntryCount = 0;
  for (const entryName of dataEntries) {
    if (!entryName.endsWith(".json")) continue;
    jsonEntryCount += 1;
    const entry = zip.getEntry(entryName);
    if (entry === null) {
      throw new Error(
        `Archive entry disappeared during inspection: ${entryName}`,
      );
    }
    const parsed = /** @type {unknown} */ (
      JSON.parse(entry.getData().toString("utf8"))
    );
    const deferredReference = findDeferredReference(parsed, entryName);
    if (deferredReference !== null) {
      throw new Error(
        `Query-data-only artifact contains a deferred reference at ${deferredReference}`,
      );
    }
    for (const target of collectRelativeLinks(parsed)) {
      if (!dataNames.has(target)) {
        throw new Error(`${entryName} has broken relative link ./${target}`);
      }
    }
  }
  return { manifest, dataEntries, jsonEntryCount };
}

/**
 * Add the mandatory root marker and verify a newly transformed data-only ZIP.
 *
 * The archive still has a `.zip` payload for existing validators, but its
 * filename suffix, ZIP comment, and root marker all fail closed against being
 * mistaken for a normal publication artifact.
 *
 * @param {string} artifactPath - Temporary transform output path.
 * @param {string} folio - Canonical Broward folio.
 * @returns {QueryDataOnlyInspection} Final validated archive metadata.
 */
export function markQueryDataOnlyArtifact(artifactPath, folio) {
  const zip = new AdmZip(artifactPath);
  /** @type {QueryDataOnlyManifest} */
  const manifest = {
    schemaVersion: QUERY_DATA_ONLY_SCHEMA_VERSION,
    artifactMode: QUERY_DATA_ONLY_MODE,
    publishable: false,
    folio,
    generatedAt: new Date().toISOString(),
    deferredOutputs: [
      "data/index.html",
      "data/fact_sheet.json",
      "data/relationship_*_to_fact_sheet.json",
      "data-group *_has_fact_sheet relationships",
    ],
    regeneration:
      "Re-run the preserved seed and compressed capture through publishable mode; never upload this archive.",
  };
  zip.addFile(
    QUERY_DATA_ONLY_MANIFEST_ENTRY,
    Buffer.from(`${JSON.stringify(manifest, null, 2)}\n`),
  );
  zip.addZipComment(
    "BROWARD QUERY-DATA-ONLY: NOT PUBLISHABLE; FACT SHEET DEFERRED",
  );
  zip.writeZip(artifactPath);
  return inspectQueryDataOnlyZip(new AdmZip(artifactPath));
}

/**
 * Read and validate a query-data-only artifact from disk.
 *
 * @param {string} artifactPath - Archive path.
 * @returns {Promise<QueryDataOnlyInspection>} Validated archive metadata.
 */
export async function inspectQueryDataOnlyArtifact(artifactPath) {
  return inspectQueryDataOnlyZip(new AdmZip(await readFile(artifactPath)));
}
