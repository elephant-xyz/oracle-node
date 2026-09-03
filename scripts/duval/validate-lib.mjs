/**
 * Duval Task 7 validation helpers (geometry bbox, completeness, reconciliation).
 * @module scripts/duval/validate-lib
 */

import * as cheerio from "cheerio";

import { parsePilotArgs } from "../hillsborough/lib.mjs";
import { assertManifestReconciled } from "./pilot-lib.mjs";

/** Task 7 geometry gate from duval-pilot-plan.md (tighter than PIN_BBOX). */
export const DUVAL_VALIDATION_BBOX = Object.freeze({
  minLat: 30.103,
  maxLat: 30.586,
  minLng: -82.05,
  maxLng: -81.318,
});

/**
 * @param {string[]} argv
 * @returns {{
 *   limit: number | null;
 *   pilotRoot: string;
 *   reportPath: string;
 *   staticPartsPath: string;
 *   seedPath: string | null;
 * }}
 */
export function parseDuvalValidateArgs(argv) {
  const shared = parsePilotArgs(argv);
  const reportPath =
    argv.find((arg) => arg.startsWith("--report="))?.split("=")[1] ??
    "docs/duval-appraisal-transform-validation.md";
  const staticPartsPath =
    argv.find((arg) => arg.startsWith("--static-parts="))?.split("=")[1] ??
    "source-html-static-parts/duval.csv";
  if (
    shared.limit !== null &&
    (!Number.isInteger(shared.limit) || shared.limit <= 0)
  ) {
    throw new Error("--limit must be a positive integer");
  }
  return {
    limit: shared.limit,
    pilotRoot: shared.outputRoot ?? "downloads/duval/pilot-run",
    reportPath,
    staticPartsPath,
    seedPath: shared.seedPath,
  };
}

/**
 * @param {string} csvText
 * @returns {string[]}
 */
export function parseStaticPartSelectors(csvText) {
  const selectors = [];
  for (const rawLine of csvText.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || /^cssselector$/i.test(line.replaceAll('"', ""))) continue;
    const unquoted = line.replace(/^"|"$/g, "").trim();
    if (unquoted) selectors.push(unquoted);
  }
  return selectors;
}

/**
 * @param {unknown} record
 * @returns {Array<{ latitude: number; longitude: number }>}
 */
export function collectGeometryPoints(record) {
  /** @type {Array<{ latitude: number; longitude: number }>} */
  const points = [];
  if (!record || typeof record !== "object") return points;
  const body = /** @type {Record<string, unknown>} */ (record);
  const lat = Number(body.latitude);
  const lon = Number(body.longitude);
  if (Number.isFinite(lat) && Number.isFinite(lon)) {
    points.push({ latitude: lat, longitude: lon });
  }
  if (Array.isArray(body.polygon)) {
    for (const vertex of body.polygon) {
      if (!vertex || typeof vertex !== "object") continue;
      const vertexLat = Number(
        /** @type {Record<string, unknown>} */ (vertex).latitude,
      );
      const vertexLon = Number(
        /** @type {Record<string, unknown>} */ (vertex).longitude,
      );
      if (Number.isFinite(vertexLat) && Number.isFinite(vertexLon)) {
        points.push({ latitude: vertexLat, longitude: vertexLon });
      }
    }
  }
  return points;
}

/**
 * @param {Array<{ latitude: number; longitude: number }>} points
 * @returns {void}
 */
export function assertGeometryInCounty(points) {
  if (!points.length) {
    throw new Error("geometry is missing a centroid or polygon");
  }
  const { minLat, maxLat, minLng, maxLng } = DUVAL_VALIDATION_BBOX;
  for (const point of points) {
    if (
      point.latitude < minLat ||
      point.latitude > maxLat ||
      point.longitude < minLng ||
      point.longitude > maxLng
    ) {
      throw new Error(
        `coordinate ${point.latitude},${point.longitude} is outside the Duval bbox`,
      );
    }
  }
}

/**
 * Completeness stand-in: labeled COJ body fields that are not chrome listed in
 * `source-html-static-parts/duval.csv`, measured against transform JSON text.
 * Used because `@elephant-xyz/cli@1.58.1` does not export `mirrorValidate`.
 *
 * @param {string} html
 * @param {readonly string[]} staticSelectors
 * @param {string} transformJsonText
 * @returns {{ onPage: number; inTransform: number; ratio: number; missing: string[] }}
 */
export function scoreLabeledFieldCoverage(
  html,
  staticSelectors,
  transformJsonText,
) {
  const $ = cheerio.load(html);
  const staticSet = new Set(staticSelectors);
  const blob = transformJsonText.toLowerCase();
  let onPage = 0;
  let inTransform = 0;
  /** @type {string[]} */
  const missing = [];

  $("[id]").each((_, element) => {
    const id = $(element).attr("id") ?? "";
    if (!id.startsWith("ctl00_cphBody_lbl")) return;
    const selector = `#${id}`;
    if (staticSet.has(selector)) return;
    const text = $(element).text().replace(/\s+/g, " ").trim();
    if (!text) return;
    onPage += 1;
    const needle = text.toLowerCase();
    const compact = needle.replace(/[^a-z0-9]+/g, "");
    if (
      blob.includes(needle) ||
      (compact.length >= 6 && blob.includes(compact))
    ) {
      inTransform += 1;
    } else {
      missing.push(text);
    }
  });

  return {
    onPage,
    inTransform,
    ratio: onPage === 0 ? 1 : inTransform / onPage,
    missing,
  };
}

/**
 * @param {string} message
 * @returns {"extractor" | "capture" | "lexicon"}
 */
export function classifyValidationGap(message) {
  const lowered = message.toLowerCase();
  if (
    /enum|additionalproperties|unexpected property|missing required property|schema|lexicon|must match|must be equal|normalized version/.test(
      lowered,
    )
  ) {
    return "lexicon";
  }
  if (/labeled field|absent from json|not captured|static part/.test(lowered)) {
    return "capture";
  }
  return "extractor";
}

/**
 * @param {{
 *   reconciled?: {
 *     seedRows?: number;
 *     attempted?: number;
 *     success?: number;
 *     failures?: number;
 *     skipped?: number;
 *   };
 *   results?: Array<{ folio?: string }>;
 * }} manifest
 * @param {number} [expectedParcels]
 * @returns {void}
 */
export function reconcileIngestManifest(manifest, expectedParcels = 50) {
  const reconciled = manifest.reconciled ?? {};
  assertManifestReconciled({
    seedRows: Number(reconciled.seedRows ?? 0),
    attempted: Number(reconciled.attempted ?? 0),
    success: Number(reconciled.success ?? 0),
    failures: Number(reconciled.failures ?? 0),
    skipped: Number(reconciled.skipped ?? 0),
  });
  const folios = (manifest.results ?? [])
    .map((row) => row.folio)
    .filter(Boolean);
  const distinct = new Set(folios);
  if (distinct.size !== expectedParcels) {
    throw new Error(
      `distinct parcel_id ${distinct.size} != ${expectedParcels}`,
    );
  }
}

/**
 * @param {Array<{
 *   folio?: string;
 *   error?: string;
 *   class?: string;
 *   issues?: Array<{ issue: string; class?: string }>;
 * }>} failures
 * @param {number} selectedCount
 * @returns {string}
 */
export function formatValidationIssueLines(failures, selectedCount) {
  /** @type {Map<string, number>} */
  const uniqueIssues = new Map();
  /** @type {string[]} */
  const ungrouped = [];
  for (const failure of failures) {
    const items = failure.issues ?? [];
    if (items.length === 0) {
      const message = failure.error ?? "unknown validation failure";
      const gapClass = failure.class ?? classifyValidationGap(message);
      ungrouped.push(`- \`${failure.folio ?? "?"}\` (${gapClass}): ${message}`);
      continue;
    }
    for (const item of items) {
      uniqueIssues.set(item.issue, (uniqueIssues.get(item.issue) ?? 0) + 1);
    }
  }
  const grouped = [...uniqueIssues.entries()]
    .sort((left, right) => right[1] - left[1])
    .map(
      ([issue, count]) =>
        `- **${count}/${selectedCount}** (${classifyValidationGap(issue)}): ${issue}`,
    );
  const lines = [...grouped, ...ungrouped];
  return lines.length ? lines.join("\n") : "- None.";
}

/**
 * @param {{
 *   lexiconPassed: number;
 *   selectedCount: number;
 *   meanCompleteness: number;
 * }} stats
 * @returns {string}
 */
export function lexiconFailureNarrative(stats) {
  if (stats.selectedCount > 0 && stats.lexiconPassed === stats.selectedCount) {
    return "";
  }
  const completenessPct = `${(stats.meanCompleteness * 100).toFixed(1)}%`;
  if (stats.lexiconPassed === 0) {
    return `**Lexicon did not pass.** Every parcel fails the same address-schema cluster after
\`@elephant-xyz/cli\` wraps the Duval extractor output. The extractor writes
\`address.json\` with \`unnormalized_address\` plus township/range/section. After
wrapping, relationship objects reject \`unnormalized_address\` and require the
normalized field set (\`city_name\`, \`street_*\`, \`source_http_request\`, …). That
is an extractor/lexicon-wrap gap in \`Counties-trasform-scripts\`, not a missing
capture. Completeness at ~${completenessPct} is pessimistic: \`scoreLabeledFieldCoverage\`
treats table chrome (\`RE #\`, \`CAMA\`, money strings) as labeled fields.
`;
  }
  return `**Lexicon passed ${stats.lexiconPassed}/${stats.selectedCount}.** Remaining failures are listed below. Mean labeled-field completeness: ${completenessPct}.
`;
}
