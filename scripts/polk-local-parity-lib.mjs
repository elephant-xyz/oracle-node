import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { access, readFile, stat } from "node:fs/promises";
import { createRequire } from "node:module";
import * as path from "node:path";
import { parseArgs } from "node:util";

const require = createRequire(import.meta.url);
const duckdb = require("duckdb");

/** Version of the evidence-only Polk lifecycle status contract. */
export const POLK_LOCAL_PARITY_SCHEMA_VERSION =
  "oracle-node.polk-local-parity.v1";

/** Stable county metadata shared by local Polk stages. */
export const POLK_COUNTY = Object.freeze({
  key: "polk",
  name: "Polk",
  stateCode: "FL",
  countyFips: "12105",
});

/**
 * @typedef {Record<string, unknown>} JsonObject
 */

/**
 * @typedef {"roofing" | "hvac" | "solar" | "pool" | "electrical" | "plumbing"} PolkPermitTrade
 */

/**
 * @typedef {object} PolkPermitTradeRule
 * @property {PolkPermitTrade} key Stable trade key.
 * @property {string} expression Case-insensitive regular-expression body.
 * @property {string} description Human-readable classification rule.
 */

/**
 * Permit classification rules applied to official Polk bulk permit type,
 * description, and number fields. A permit may belong to multiple trades.
 *
 * These classifications are analytical enrichment only. They do not imply that
 * Polk supplied a contractor, licence, inspection, or portal-detail record.
 *
 * @type {readonly PolkPermitTradeRule[]}
 */
export const POLK_PERMIT_TRADE_RULES = Object.freeze([
  {
    key: "roofing",
    expression:
      "\\b(roof|reroof|re-roof|shingle|membrane|tpo|built[ -]?up|tile roof)\\b",
    description: "Roof, reroof, membrane, shingle, TPO, or roof-tile terms.",
  },
  {
    key: "hvac",
    expression:
      "\\b(hvac|air condition|a/c|heat pump|furnace|condenser|mechanical|duct)\\b",
    description:
      "HVAC, air-conditioning, heat-pump, furnace, condenser, mechanical, or duct terms.",
  },
  {
    key: "solar",
    expression:
      "\\b(solar|photovoltaic|pv system|pv array|rooftop pv|inverter)\\b",
    description: "Solar, photovoltaic, PV-system, or inverter terms.",
  },
  {
    key: "pool",
    expression: "\\b(pool|spa|swimming pool|pool cage|pool enclosure)\\b",
    description: "Pool, spa, pool-cage, or pool-enclosure terms.",
  },
  {
    key: "electrical",
    expression:
      "\\b(electric|electrical|panel upgrade|service change|generator|rewir)\\b",
    description:
      "Electrical, panel-upgrade, service-change, generator, or rewiring terms.",
  },
  {
    key: "plumbing",
    expression: "\\b(plumb|water heater|repipe|sewer|backflow)\\b",
    description: "Plumbing, water-heater, repipe, sewer, or backflow terms.",
  },
]);

/**
 * @typedef {object} PolkStatusCliOptions
 * @property {string} sourceDirectory Completed local appraisal export root.
 * @property {string} workDatabase Persistent DuckDB cache built from Polk bulk files.
 * @property {string} permitSummaryPath Generated permit classification summary.
 * @property {string} overtureSummaryPath Optional Overture extract/probe summary.
 * @property {string} sunbizManifestPath Optional Polk Sunbiz extraction/match manifest.
 * @property {string} bbbSummaryPath Optional Polk BBB harvest/match summary.
 * @property {string} publicationIndexPath Optional staged open-data publication index.
 * @property {string} catalogPath Tracked canonical published-county catalog.
 * @property {string} outputPath Generated lifecycle status JSON.
 * @property {boolean} writeOutput Whether the caller should persist outputPath.
 */

/**
 * @typedef {object} PermitDimensionCount
 * @property {string} value Dimension value.
 * @property {number} count Row count.
 */

/**
 * @typedef {object} PolkPermitSummary
 * @property {string} schemaVersion Summary schema.
 * @property {string} sourceSystem Source-system label.
 * @property {string} generatedAt Generation timestamp.
 * @property {string} workDatabase Local database path.
 * @property {number} permitCount Total official bulk permit rows.
 * @property {number} classifiedPermitCount Permits matching at least one trade.
 * @property {number} unclassifiedPermitCount Permits matching no configured trade.
 * @property {Record<PolkPermitTrade, number>} tradeCounts Overlapping trade counts.
 * @property {number} withPermitNumber Rows with a permit number.
 * @property {number} withAgency Rows with an issuing agency.
 * @property {number} withDescription Rows with project description text.
 * @property {number} withEstimatedValue Rows with estimated-value text.
 * @property {string | null} earliestIssueDate Earliest non-sentinel issue date.
 * @property {string | null} latestIssueDate Latest non-sentinel issue date.
 * @property {readonly PermitDimensionCount[]} agencies Rows by source agency.
 * @property {readonly PermitDimensionCount[]} statuses Rows by source status.
 * @property {readonly PermitDimensionCount[]} zipPrefixes Distinct parcels by situs ZIP prefix.
 * @property {{available:false,reason:string,requiredEvidence:string[]}} contractorEnrichment Explicit contractor-source blocker.
 * @property {readonly PolkPermitTradeRule[]} rules Applied analytical rules.
 */

/**
 * @typedef {object} PolkLifecycleStage
 * @property {string} name Human-readable stage name.
 * @property {"complete" | "probed" | "ready" | "awaiting_human" | "blocked" | "pending"} status Evidence-backed state.
 * @property {string} evidence Concise evidence or blocker.
 * @property {number | null} count Primary count when applicable.
 */

/**
 * @typedef {object} PolkLocalParityStatus
 * @property {string} schemaVersion Status schema.
 * @property {string} generatedAt Generation timestamp.
 * @property {typeof POLK_COUNTY} county County metadata.
 * @property {Record<string, PolkLifecycleStage>} stages Lifecycle stages.
 * @property {JsonObject} localArtifacts Reconciled local artifact metadata.
 * @property {string[]} blockers Exact remaining blockers.
 * @property {boolean} pr200FunctionalParity Whether every Hillsborough outcome is evidenced.
 */

/**
 * Return true only for a non-array JSON object.
 *
 * @param {unknown} value Candidate value.
 * @returns {value is JsonObject} Whether value is a JSON object.
 */
export function isJsonObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Classify one permit into zero or more analytical trade categories.
 *
 * @param {{description?:unknown,permitType?:unknown,permitNumber?:unknown}} permit Official Polk permit fields.
 * @returns {PolkPermitTrade[]} Matching categories in stable rule order.
 */
export function classifyPolkPermitTrades(permit) {
  const text = [permit.description, permit.permitType, permit.permitNumber]
    .filter(
      /** @type {(value: unknown) => value is string | number} */ (
        (value) => typeof value === "string" || typeof value === "number"
      ),
    )
    .map(String)
    .join(" ");
  return POLK_PERMIT_TRADE_RULES.flatMap((rule) =>
    new RegExp(rule.expression, "i").test(text) ? [rule.key] : [],
  );
}

/**
 * Escape a trusted static string as a DuckDB SQL literal.
 *
 * @param {string} value Static text.
 * @returns {string} Single-quoted SQL literal.
 */
function duckdbStringLiteral(value) {
  return `'${value.replaceAll("'", "''")}'`;
}

/**
 * Build the aggregate SQL used for repeatable Polk permit classification.
 *
 * @returns {string} Read-only aggregate query.
 */
export function buildPolkPermitAggregateSql() {
  const flags = POLK_PERMIT_TRADE_RULES.map(
    (rule) =>
      `regexp_matches(search_text, ${duckdbStringLiteral(rule.expression)}, 'i') AS is_${rule.key}`,
  );
  const tradeCounts = POLK_PERMIT_TRADE_RULES.map(
    (rule) => `count(*) FILTER (WHERE is_${rule.key})::INTEGER AS ${rule.key}`,
  );
  const anyTrade = POLK_PERMIT_TRADE_RULES.map((rule) => `is_${rule.key}`).join(
    " OR ",
  );
  return `
    WITH source AS (
      SELECT
        *,
        concat_ws(
          ' ',
          coalesce(description, ''),
          coalesce(permit_type, ''),
          coalesce(permit_number, '')
        ) AS search_text
      FROM polk_permits
    ),
    classified AS (
      SELECT
        *,
        ${flags.join(",\n        ")}
      FROM source
    )
    SELECT
      count(*)::INTEGER AS permit_count,
      count(*) FILTER (WHERE ${anyTrade})::INTEGER AS classified_permit_count,
      count(*) FILTER (WHERE NOT (${anyTrade}))::INTEGER AS unclassified_permit_count,
      ${tradeCounts.join(",\n      ")},
      count(*) FILTER (
        WHERE permit_number IS NOT NULL AND trim(permit_number) <> ''
      )::INTEGER AS with_permit_number,
      count(*) FILTER (
        WHERE agency_name IS NOT NULL AND trim(agency_name) <> ''
      )::INTEGER AS with_agency,
      count(*) FILTER (
        WHERE description IS NOT NULL AND trim(description) <> ''
      )::INTEGER AS with_description,
      count(*) FILTER (
        WHERE estimated_value IS NOT NULL AND trim(estimated_value) <> ''
      )::INTEGER AS with_estimated_value,
      min(
        CASE
          WHEN substr(issue_date, 1, 10) > '1900-01-01'
            THEN substr(issue_date, 1, 10)
          ELSE NULL
        END
      ) AS earliest_issue_date,
      max(
        CASE
          WHEN substr(issue_date, 1, 10) > '1900-01-01'
            THEN substr(issue_date, 1, 10)
          ELSE NULL
        END
      ) AS latest_issue_date
    FROM classified
  `;
}

/**
 * Convert an aggregate count returned by DuckDB into a safe number.
 *
 * @param {unknown} value DuckDB scalar.
 * @param {string} name Field name for diagnostics.
 * @returns {number} Non-negative safe integer.
 */
function readCount(value, name) {
  const count = typeof value === "bigint" ? Number(value) : Number(value);
  if (!Number.isSafeInteger(count) || count < 0) {
    throw new Error(`Invalid ${name} returned by Polk evidence query`);
  }
  return count;
}

/**
 * Read nullable scalar text.
 *
 * @param {unknown} value Candidate scalar.
 * @returns {string | null} Trimmed text or null.
 */
function readText(value) {
  if (typeof value !== "string" && typeof value !== "number") return null;
  const text = String(value).trim();
  return text.length > 0 ? text : null;
}

/**
 * Execute one read-only query against an open DuckDB connection.
 *
 * @param {import("duckdb").Connection} connection DuckDB connection.
 * @param {string} sql Read-only SQL.
 * @returns {Promise<JsonObject[]>} Result rows.
 */
function queryDuckDb(connection, sql) {
  return new Promise((resolve, reject) => {
    connection.all(sql, (error, rows) => {
      if (error !== null) {
        reject(error instanceof Error ? error : new Error(String(error)));
        return;
      }
      resolve(Array.isArray(rows) ? rows.filter(isJsonObject) : []);
    });
  });
}

/**
 * Close a DuckDB connection without masking the primary operation result.
 *
 * @param {import("duckdb").Connection} connection Open connection.
 * @returns {Promise<void>} Resolves after close.
 */
function closeDuckDbConnection(connection) {
  return new Promise((resolve) => {
    connection.close(() => resolve());
  });
}

/**
 * Normalize a DuckDB grouped result into stable value/count rows.
 *
 * @param {readonly JsonObject[]} rows Query rows.
 * @param {string} valueKey Dimension field.
 * @returns {PermitDimensionCount[]} Normalized rows.
 */
function dimensionCounts(rows, valueKey) {
  return rows.map((row) => ({
    value: readText(row[valueKey]) ?? "(missing)",
    count: readCount(row.count, `${valueKey} count`),
  }));
}

/**
 * Build exact, source-backed permit and ZIP enrichment counters.
 *
 * This reads only the closed Polk projections already created by the appraisal
 * exporter. It does not call a permit portal and therefore explicitly reports
 * contractor enrichment as unavailable.
 *
 * @param {string} workDatabase Path to the completed Polk DuckDB cache.
 * @returns {Promise<PolkPermitSummary>} Evidence-backed permit summary.
 */
export async function buildPolkPermitSummary(workDatabase) {
  await access(workDatabase);
  const database = new duckdb.Database(workDatabase);
  const connection = database.connect();
  try {
    const [aggregateRows, agencyRows, statusRows, zipRows] = await Promise.all([
      queryDuckDb(connection, buildPolkPermitAggregateSql()),
      queryDuckDb(
        connection,
        `
          SELECT
            coalesce(nullif(trim(agency_name), ''), '(missing)') AS agency,
            count(*)::INTEGER AS count
          FROM polk_permits
          GROUP BY 1
          ORDER BY count DESC, agency
        `,
      ),
      queryDuckDb(
        connection,
        `
          SELECT
            coalesce(
              nullif(trim(status_description), ''),
              nullif(trim(status), ''),
              '(missing)'
            ) AS status,
            count(*)::INTEGER AS count
          FROM polk_permits
          GROUP BY 1
          ORDER BY count DESC, status
        `,
      ),
      queryDuckDb(
        connection,
        `
          SELECT
            substr(regexp_replace(postal_code, '[^0-9]', '', 'g'), 1, 3)
              AS zip_prefix,
            count(DISTINCT parcel_id)::INTEGER AS count
          FROM polk_sites
          WHERE length(regexp_replace(postal_code, '[^0-9]', '', 'g')) >= 5
          GROUP BY 1
          ORDER BY count DESC, zip_prefix
        `,
      ),
    ]);
    const aggregate = aggregateRows[0];
    if (aggregate === undefined) {
      throw new Error("Polk permit aggregate returned no row");
    }
    /** @type {Record<PolkPermitTrade, number>} */
    const tradeCounts = {
      roofing: readCount(aggregate.roofing, "roofing"),
      hvac: readCount(aggregate.hvac, "hvac"),
      solar: readCount(aggregate.solar, "solar"),
      pool: readCount(aggregate.pool, "pool"),
      electrical: readCount(aggregate.electrical, "electrical"),
      plumbing: readCount(aggregate.plumbing, "plumbing"),
    };
    return {
      schemaVersion: "oracle-node.polk-permit-enrichment.v1",
      sourceSystem:
        "Polk County Property Appraiser official bulk CAMA permit file",
      generatedAt: new Date().toISOString(),
      workDatabase,
      permitCount: readCount(aggregate.permit_count, "permit count"),
      classifiedPermitCount: readCount(
        aggregate.classified_permit_count,
        "classified permit count",
      ),
      unclassifiedPermitCount: readCount(
        aggregate.unclassified_permit_count,
        "unclassified permit count",
      ),
      tradeCounts,
      withPermitNumber: readCount(
        aggregate.with_permit_number,
        "permit-number count",
      ),
      withAgency: readCount(aggregate.with_agency, "agency count"),
      withDescription: readCount(
        aggregate.with_description,
        "description count",
      ),
      withEstimatedValue: readCount(
        aggregate.with_estimated_value,
        "estimated-value count",
      ),
      earliestIssueDate: readText(aggregate.earliest_issue_date),
      latestIssueDate: readText(aggregate.latest_issue_date),
      agencies: dimensionCounts(agencyRows, "agency"),
      statuses: dimensionCounts(statusRows, "status"),
      zipPrefixes: dimensionCounts(zipRows, "zip_prefix"),
      contractorEnrichment: {
        available: false,
        reason:
          "The official Polk ftp_permit projection contains no contractor company, licence, phone, email, inspection, or detail-page URL fields.",
        requiredEvidence: [
          "A certified Polk County and municipal permit-detail endpoint or bulk contractor feed.",
          "Contractor company or licence identifiers joined to each permit.",
          "A staged BBB profile harvest before any BBB-to-contractor match can be claimed.",
        ],
      },
      rules: POLK_PERMIT_TRADE_RULES,
    };
  } finally {
    await closeDuckDbConnection(connection);
    database.close();
  }
}

/**
 * Read a JSON object, returning null only when the file does not exist.
 *
 * @param {string} filePath JSON path.
 * @returns {Promise<JsonObject | null>} Parsed object or null.
 */
export async function readOptionalJsonObject(filePath) {
  let text;
  try {
    text = await readFile(filePath, "utf8");
  } catch (caught) {
    if (
      caught instanceof Error &&
      "code" in caught &&
      /** @type {NodeJS.ErrnoException} */ (caught).code === "ENOENT"
    ) {
      return null;
    }
    throw caught;
  }
  const value = /** @type {unknown} */ (JSON.parse(text));
  if (!isJsonObject(value)) {
    throw new Error(`Expected JSON object at ${filePath}`);
  }
  return value;
}

/**
 * Hash a file without buffering it in memory.
 *
 * @param {string} filePath Input file.
 * @returns {Promise<string>} SHA-256 hex digest.
 */
export function sha256File(filePath) {
  return new Promise((resolve, reject) => {
    const hash = createHash("sha256");
    const stream = createReadStream(filePath);
    stream.on("error", reject);
    stream.on("data", (chunk) => hash.update(chunk));
    stream.on("end", () => resolve(hash.digest("hex")));
  });
}

/**
 * Parse local-only status CLI options.
 *
 * @param {readonly string[]} argv CLI arguments after the script path.
 * @param {string} [rootDirectory] Repository root for relative defaults.
 * @returns {PolkStatusCliOptions} Resolved options.
 */
export function parsePolkStatusCliOptions(argv, rootDirectory = process.cwd()) {
  const { values } = parseArgs({
    args: [...argv],
    options: {
      "source-dir": { type: "string" },
      "work-db": { type: "string" },
      "permit-summary": { type: "string" },
      "overture-summary": { type: "string" },
      "sunbiz-manifest": { type: "string" },
      "bbb-summary": { type: "string" },
      "publication-index": { type: "string" },
      catalog: { type: "string" },
      out: { type: "string" },
      "no-write": { type: "boolean" },
    },
    strict: true,
    allowPositionals: false,
  });
  const sourceDirectory = path.resolve(
    rootDirectory,
    typeof values["source-dir"] === "string"
      ? values["source-dir"]
      : "tmp/polk/full",
  );
  return {
    sourceDirectory,
    workDatabase: path.resolve(
      rootDirectory,
      typeof values["work-db"] === "string"
        ? values["work-db"]
        : "tmp/polk/bulk/extracted/polk-appraisal.duckdb",
    ),
    permitSummaryPath: path.resolve(
      rootDirectory,
      typeof values["permit-summary"] === "string"
        ? values["permit-summary"]
        : "tmp/polk/parity/permit-enrichment.json",
    ),
    overtureSummaryPath: path.resolve(
      rootDirectory,
      typeof values["overture-summary"] === "string"
        ? values["overture-summary"]
        : "tmp/polk/overture/2026-08-19.0/extract/manifest/summary.json",
    ),
    sunbizManifestPath: path.resolve(
      rootDirectory,
      typeof values["sunbiz-manifest"] === "string"
        ? values["sunbiz-manifest"]
        : "tmp/polk/sunbiz/manifest.json",
    ),
    bbbSummaryPath: path.resolve(
      rootDirectory,
      typeof values["bbb-summary"] === "string"
        ? values["bbb-summary"]
        : "tmp/polk/bbb/manifest/summary.json",
    ),
    publicationIndexPath: path.resolve(
      rootDirectory,
      typeof values["publication-index"] === "string"
        ? values["publication-index"]
        : "tmp/polk/filebase-publication/index.json",
    ),
    catalogPath: path.resolve(
      rootDirectory,
      typeof values.catalog === "string"
        ? values.catalog
        : "catalog/published-counties.json",
    ),
    outputPath: path.resolve(
      rootDirectory,
      typeof values.out === "string"
        ? values.out
        : "tmp/polk/parity/status.json",
    ),
    writeOutput: values["no-write"] !== true,
  };
}

/**
 * Read a nested numeric field without accepting numeric-looking strings.
 *
 * @param {JsonObject | null} object Root object.
 * @param {readonly string[]} keys Nested keys.
 * @returns {number | null} Finite number or null.
 */
function nestedNumber(object, keys) {
  let current = /** @type {unknown} */ (object);
  for (const key of keys) {
    if (!isJsonObject(current)) return null;
    current = current[key];
  }
  return typeof current === "number" && Number.isFinite(current)
    ? current
    : null;
}

/**
 * Read a nested boolean field.
 *
 * @param {JsonObject | null} object Root object.
 * @param {readonly string[]} keys Nested keys.
 * @returns {boolean | null} Boolean or null.
 */
function nestedBoolean(object, keys) {
  let current = /** @type {unknown} */ (object);
  for (const key of keys) {
    if (!isJsonObject(current)) return null;
    current = current[key];
  }
  return typeof current === "boolean" ? current : null;
}

/**
 * Find a county entry in the canonical published catalog.
 *
 * @param {JsonObject | null} catalog Parsed catalog.
 * @returns {JsonObject | null} Polk entry or null.
 */
function findPolkCatalogEntry(catalog) {
  if (catalog === null || !Array.isArray(catalog.counties)) return null;
  return (
    catalog.counties.find(
      (entry) => isJsonObject(entry) && entry.countyKey === POLK_COUNTY.key,
    ) ?? null
  );
}

/**
 * Prefer a full Overture extract summary, falling back to the sibling probe
 * only when the configured path uses the standard local `extract` layout.
 *
 * @param {string} summaryPath Preferred full-extract summary path.
 * @returns {Promise<JsonObject | null>} Best available Overture evidence.
 */
async function readOvertureEvidence(summaryPath) {
  const preferred = await readOptionalJsonObject(summaryPath);
  if (
    preferred !== null ||
    !summaryPath.includes(`${path.sep}extract${path.sep}`)
  ) {
    return preferred;
  }
  return readOptionalJsonObject(
    summaryPath.replace(
      `${path.sep}extract${path.sep}`,
      `${path.sep}probe${path.sep}`,
    ),
  );
}

/**
 * Build a truthful, evidence-only Polk lifecycle report.
 *
 * Missing artifacts remain blocked or pending. No stage becomes complete from
 * a configured target, a documentation file, or a hard-coded fallback count.
 *
 * @param {PolkStatusCliOptions} options Local artifact paths.
 * @returns {Promise<{status:PolkLocalParityStatus,permitSummary:PolkPermitSummary}>} Status and permit evidence.
 */
export async function buildPolkLocalParityStatus(options) {
  const [
    manifest,
    coverage,
    checkpoint,
    overture,
    sunbiz,
    bbb,
    publicationIndex,
    catalog,
    permitSummary,
  ] = await Promise.all([
    readOptionalJsonObject(path.join(options.sourceDirectory, "manifest.json")),
    readOptionalJsonObject(path.join(options.sourceDirectory, "coverage.json")),
    readOptionalJsonObject(
      path.join(options.sourceDirectory, ".state", "checkpoint.json"),
    ),
    readOvertureEvidence(options.overtureSummaryPath),
    readOptionalJsonObject(options.sunbizManifestPath),
    readOptionalJsonObject(options.bbbSummaryPath),
    readOptionalJsonObject(options.publicationIndexPath),
    readOptionalJsonObject(options.catalogPath),
    buildPolkPermitSummary(options.workDatabase),
  ]);
  if (manifest === null || coverage === null || checkpoint === null) {
    throw new Error(
      `Completed Polk manifest, coverage, and checkpoint are required under ${options.sourceDirectory}`,
    );
  }
  const propertyCount = nestedNumber(manifest, ["output", "propertyCount"]);
  const queryTableRows = nestedNumber(manifest, [
    "output",
    "queryTable",
    "rowCount",
  ]);
  const queryTableExpectedBytes = nestedNumber(manifest, [
    "output",
    "queryTable",
    "sizeBytes",
  ]);
  const queryTableExpectedHash = readText(
    isJsonObject(manifest.output) && isJsonObject(manifest.output.queryTable)
      ? manifest.output.queryTable.sha256
      : null,
  );
  const queryTablePath = path.join(
    options.sourceDirectory,
    readText(
      isJsonObject(manifest.output) && isJsonObject(manifest.output.queryTable)
        ? manifest.output.queryTable.file
        : null,
    ) ?? "query-table.parquet",
  );
  const queryTableInfo = await stat(queryTablePath);
  const queryTableHash = await sha256File(queryTablePath);
  const validationRows = nestedNumber(manifest, [
    "output",
    "validation",
    "rowCount",
  ]);
  const distinctParcels = nestedNumber(manifest, [
    "output",
    "validation",
    "distinctParcels",
  ]);
  const nullCids = nestedNumber(manifest, ["output", "validation", "nullCids"]);
  const ownerViolations = nestedNumber(manifest, [
    "output",
    "validation",
    "ownerFieldViolations",
  ]);
  const checkpointComplete = checkpoint.complete === true;
  const appraisalComplete =
    propertyCount !== null &&
    propertyCount > 0 &&
    queryTableRows === propertyCount &&
    validationRows === propertyCount &&
    distinctParcels === propertyCount &&
    nullCids === 0 &&
    ownerViolations === 0 &&
    checkpointComplete &&
    queryTableExpectedBytes === queryTableInfo.size &&
    queryTableExpectedHash === queryTableHash;

  const permitSourceCount = nestedNumber(coverage, ["childRows", "permits"]);
  const permitComplete =
    permitSourceCount !== null &&
    permitSourceCount === permitSummary.permitCount;
  const overtureMode = readText(overture?.mode);
  const overtureClipCount = nestedNumber(overture, ["clipCount"]);
  const overtureLicencePassed = nestedBoolean(overture, [
    "licenceGate",
    "passed",
  ]);
  const overtureExtractReady =
    overture !== null &&
    overtureMode !== "counts-only" &&
    overtureClipCount !== null &&
    overtureClipCount > 0 &&
    overtureLicencePassed === true;
  const sunbizMatchedCount =
    nestedNumber(sunbiz, ["matchedPropertyCount"]) ??
    nestedNumber(sunbiz, ["matchedRecordCount"]);
  const bbbMatchedCount =
    nestedNumber(bbb, ["matchedContractorCount"]) ??
    nestedNumber(bbb, ["matchedInBbbCrm"]);
  const stagedPropertyCount = nestedNumber(publicationIndex, ["propertyCount"]);
  const publicationPrepared =
    propertyCount !== null && stagedPropertyCount === propertyCount;
  const polkCatalogEntry = findPolkCatalogEntry(catalog);

  /** @type {Record<string, PolkLifecycleStage>} */
  const stages = {
    appraisal: {
      name: "Local bulk appraisal export",
      status: appraisalComplete ? "complete" : "blocked",
      evidence: appraisalComplete
        ? `${propertyCount} reconciled properties; query-table size/hash and privacy gates match the manifest.`
        : "Local manifest, checkpoint, query-table digest, or reconciliation gate does not match.",
      count: propertyCount,
    },
    permits: {
      name: "Permit classification",
      status: permitComplete ? "complete" : "blocked",
      evidence: permitComplete
        ? `${permitSummary.permitCount} official bulk permits classified across six overlapping trades; contractor-detail enrichment remains unavailable.`
        : "Permit summary does not reconcile to the official bulk permit count.",
      count: permitSummary.permitCount,
    },
    sunbiz: {
      name: "Sunbiz property matching",
      status:
        sunbiz !== null && sunbizMatchedCount !== null ? "ready" : "blocked",
      evidence:
        sunbiz !== null && sunbizMatchedCount !== null
          ? `${sunbizMatchedCount} locally evidenced Polk property matches.`
          : "No local Polk Sunbiz source slice and property-match manifest exists; statewide cordata text is required.",
      count: sunbizMatchedCount,
    },
    bbb: {
      name: "BBB contractor matching",
      status:
        bbb !== null &&
        bbbMatchedCount !== null &&
        permitSummary.contractorEnrichment.available
          ? "ready"
          : "blocked",
      evidence:
        "Polk bulk permits contain no contractor company or licence identifiers, so a BBB-to-permit match cannot be claimed.",
      count: bbbMatchedCount,
    },
    overture: {
      name: "Overture places",
      status: overtureExtractReady
        ? "ready"
        : overtureMode === "counts-only"
          ? "probed"
          : "blocked",
      evidence: overtureExtractReady
        ? `${overtureClipCount} boundary-clipped places passed the local source/licence gate and are ready for load reconciliation.`
        : overtureMode === "counts-only"
          ? `${overtureClipCount ?? 0} places were counted with the Polk TIGER boundary; full extraction, licence gating, and load are still required.`
          : "No Polk TIGER-boundary Overture probe or extract summary exists.",
      count: overtureClipCount,
    },
    queryDatabase: {
      name: "Elephant query DB load",
      status: "blocked",
      evidence:
        "No local load receipt or oracle_dataset_coverage row proves Polk appraisal, permits, Sunbiz, BBB, or places were loaded and reconciled in Neon.",
      count: 0,
    },
    publication: {
      name: "Publication preparation",
      status: publicationPrepared ? "awaiting_human" : "pending",
      evidence: publicationPrepared
        ? `${stagedPropertyCount} property files are locally staged; external upload and IPNS mutation require human approval.`
        : "The completed export has not been reconciled into the standard local open-data staging layout.",
      count: stagedPropertyCount,
    },
    catalog: {
      name: "Catalog and MCP wiring",
      status: polkCatalogEntry === null ? "blocked" : "complete",
      evidence:
        polkCatalogEntry === null
          ? "The canonical catalog has no Polk entry; add it only after stable public query-table and coverage URLs pass gateway verification."
          : "A canonical Polk catalog entry exists; external gateway and MCP smoke verification are still required independently.",
      count: polkCatalogEntry === null ? 0 : 1,
    },
  };

  const blockers = Object.values(stages)
    .filter((stage) => stage.status !== "complete")
    .map((stage) => `${stage.name}: ${stage.evidence}`);
  return {
    status: {
      schemaVersion: POLK_LOCAL_PARITY_SCHEMA_VERSION,
      generatedAt: new Date().toISOString(),
      county: POLK_COUNTY,
      stages,
      localArtifacts: {
        propertyCount,
        propertyBytes: nestedNumber(manifest, ["output", "propertyBytes"]),
        appraisalShardCount: nestedNumber(manifest, ["output", "shardCount"]),
        queryTable: {
          path: queryTablePath,
          rowCount: queryTableRows,
          sizeBytes: queryTableInfo.size,
          sha256: queryTableHash,
          nullCids,
          ownerFieldViolations: ownerViolations,
        },
        permits: {
          sourceCount: permitSourceCount,
          summaryCount: permitSummary.permitCount,
          classifiedPermitCount: permitSummary.classifiedPermitCount,
          tradeCounts: permitSummary.tradeCounts,
          zipPrefixes: permitSummary.zipPrefixes,
        },
        overture: overture ?? {
          status: "missing",
          expectedPath: options.overtureSummaryPath,
        },
        publication:
          publicationIndex === null
            ? {
                status: "missing",
                expectedPath: options.publicationIndexPath,
              }
            : {
                propertyCount: stagedPropertyCount,
                totalBytes: nestedNumber(publicationIndex, ["totalBytes"]),
                shardSize: nestedNumber(publicationIndex, ["shardSize"]),
                shardCount: Array.isArray(publicationIndex.shards)
                  ? publicationIndex.shards.length
                  : null,
              },
      },
      blockers,
      pr200FunctionalParity: Object.values(stages).every(
        (stage) => stage.status === "complete",
      ),
    },
    permitSummary,
  };
}
