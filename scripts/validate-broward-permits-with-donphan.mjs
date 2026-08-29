#!/usr/bin/env node
// @ts-check

import { mkdir, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { pathToFileURL } from "node:url";

/**
 * @typedef {object} ToolTextContent
 * @property {string} type - MCP content type.
 * @property {string} text - JSON text returned by Donphan's handler.
 */

/**
 * @typedef {object} ToolResult
 * @property {readonly ToolTextContent[]} content - Donphan MCP text content.
 */

/**
 * @callback QueryPermitsHandler
 * @param {{county:string,sql:string,limit?:number}} args - Actual Donphan queryPermits arguments.
 * @returns {Promise<ToolResult>} Donphan MCP result.
 */

/**
 * @callback CountyPermitHandler
 * @param {{county:string}} args - Actual Donphan county arguments.
 * @returns {Promise<ToolResult>} Donphan MCP result.
 */

/**
 * @typedef {object} DonphanPermitHandlers
 * @property {QueryPermitsHandler} queryPermitsHandler - Actual Donphan arbitrary permit query handler.
 * @property {CountyPermitHandler} getPermitQuerySchemaHandler - Actual Donphan permit schema handler.
 * @property {CountyPermitHandler} getPermitCoverageHandler - Actual Donphan permit coverage handler.
 */

/**
 * @typedef {object} DonphanValidationOptions
 * @property {string} parquetPath - Local Broward permit-table Parquet.
 * @property {string} modulePath - elephant-mcp `src/tools/permitQuery.ts` path.
 * @property {string} outputPath - Private evidence JSON path.
 * @property {string} county - County argument passed to the real handlers.
 */

/**
 * @typedef {Record<string, unknown>} JsonObject
 */

const USAGE = `Usage:
  npm exec tsx -- /path/to/oracle-node/scripts/validate-broward-permits-with-donphan.mjs \\
    --parquet /path/to/permit-table.parquet \\
    --module /path/to/elephant-mcp/src/tools/permitQuery.ts \\
    --output /path/to/donphan-evidence.json

This command imports and invokes Donphan's actual getPermitQuerySchema,
getPermitCoverage, and queryPermits handlers against the local Parquet. It is
read-only and performs no database, AWS, IPFS, catalog, or publication writes.
`;

/**
 * Return true only for a non-array JSON object.
 *
 * @param {unknown} value - Candidate parsed value.
 * @returns {value is JsonObject} Whether the value is a JSON object.
 */
function isJsonObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Read a required option value from either `--flag value` or `--flag=value`.
 *
 * @param {readonly string[]} args - Raw CLI arguments.
 * @param {number} index - Current argument index.
 * @returns {{flag:string,value:string,nextIndex:number}} Parsed option.
 */
function readOption(args, index) {
  const argument = args[index];
  if (argument === undefined || !argument.startsWith("--")) {
    throw new Error(`Unexpected argument: ${String(argument)}`);
  }
  const equalsIndex = argument.indexOf("=");
  if (equalsIndex >= 0) {
    const flag = argument.slice(0, equalsIndex);
    const value = argument.slice(equalsIndex + 1).trim();
    if (value.length === 0) throw new Error(`${flag} requires a value`);
    return { flag, value, nextIndex: index };
  }
  const value = args[index + 1];
  if (value === undefined || value.startsWith("--")) {
    throw new Error(`${argument} requires a value`);
  }
  return { flag: argument, value: value.trim(), nextIndex: index + 1 };
}

/**
 * Parse local Donphan permit validation options.
 *
 * @param {readonly string[]} args - Arguments after the script path.
 * @returns {DonphanValidationOptions | null} Options, or null for help.
 */
export function parseDonphanValidationOptions(args) {
  let parquetPath = null;
  let modulePath = null;
  let outputPath = null;
  let county = "Broward";
  for (let index = 0; index < args.length; index += 1) {
    if (args[index] === "--help" || args[index] === "-h") return null;
    const option = readOption(args, index);
    index = option.nextIndex;
    if (option.flag === "--parquet") parquetPath = option.value;
    else if (option.flag === "--module") modulePath = option.value;
    else if (option.flag === "--output") outputPath = option.value;
    else if (option.flag === "--county") county = option.value;
    else throw new Error(`Unknown option: ${option.flag}`);
  }
  if (
    parquetPath === null ||
    modulePath === null ||
    outputPath === null ||
    county.length === 0
  ) {
    throw new Error("--parquet, --module, and --output are required");
  }
  return {
    parquetPath: resolve(parquetPath),
    modulePath: resolve(modulePath),
    outputPath: resolve(outputPath),
    county,
  };
}

/**
 * Parse one JSON text response from an actual Donphan handler.
 *
 * @param {ToolResult} result - MCP text result.
 * @param {string} handlerName - Handler name used in validation errors.
 * @returns {JsonObject} Parsed handler payload.
 */
export function parseDonphanToolResult(result, handlerName) {
  if (
    !Array.isArray(result.content) ||
    result.content.length !== 1 ||
    result.content[0]?.type !== "text"
  ) {
    throw new Error(`${handlerName} returned an unexpected MCP content shape`);
  }
  const parsed = /** @type {unknown} */ (JSON.parse(result.content[0].text));
  if (!isJsonObject(parsed)) {
    throw new Error(`${handlerName} did not return a JSON object`);
  }
  if (typeof parsed.error === "string") {
    throw new Error(
      `${handlerName} failed: ${parsed.error}${typeof parsed.details === "string" ? ` (${parsed.details})` : ""}`,
    );
  }
  return parsed;
}

/**
 * Validate and narrow the dynamically imported Donphan permit-query module.
 *
 * @param {unknown} moduleValue - Dynamic import namespace.
 * @returns {DonphanPermitHandlers} Actual callable handlers.
 */
function requireDonphanPermitHandlers(moduleValue) {
  if (!isJsonObject(moduleValue)) {
    throw new Error("Donphan permit query module did not export an object");
  }
  const query = moduleValue.queryPermitsHandler;
  const schema = moduleValue.getPermitQuerySchemaHandler;
  const coverage = moduleValue.getPermitCoverageHandler;
  if (
    typeof query !== "function" ||
    typeof schema !== "function" ||
    typeof coverage !== "function"
  ) {
    throw new Error(
      "Donphan module must export queryPermitsHandler, getPermitQuerySchemaHandler, and getPermitCoverageHandler",
    );
  }
  return /** @type {DonphanPermitHandlers} */ ({
    queryPermitsHandler: query,
    getPermitQuerySchemaHandler: schema,
    getPermitCoverageHandler: coverage,
  });
}

/**
 * Invoke Donphan's actual permit query surface against one local Broward
 * Parquet and persist raw parsed evidence from all three tool families.
 *
 * @param {DonphanValidationOptions} options - Local Parquet, module, county, and evidence paths.
 * @returns {Promise<JsonObject>} Complete validation evidence.
 */
export async function validateBrowardPermitsWithDonphan(options) {
  process.env.PERMIT_QUERY_TABLE = options.parquetPath;
  process.env.PERMIT_QUERY_TABLE_DEFAULT_COUNTY = options.county;
  delete process.env.PERMIT_QUERY_TABLE_MAP;
  const imported = /** @type {unknown} */ (
    await import(pathToFileURL(options.modulePath).href)
  );
  const handlers = requireDonphanPermitHandlers(imported);
  const schema = parseDonphanToolResult(
    await handlers.getPermitQuerySchemaHandler({ county: options.county }),
    "getPermitQuerySchema",
  );
  const coverage = parseDonphanToolResult(
    await handlers.getPermitCoverageHandler({ county: options.county }),
    "getPermitCoverage",
  );
  const reconciliation = parseDonphanToolResult(
    await handlers.queryPermitsHandler({
      county: options.county,
      sql: `SELECT
              count(*) AS query_rows,
              count(DISTINCT property_improvement_id) AS distinct_permit_ids,
              count(DISTINCT parcel_identifier) AS distinct_parcels,
              count(*) FILTER (WHERE permit_number IS NOT NULL) AS rows_with_permit_number
            FROM permits`,
      limit: 10,
    }),
    "queryPermits",
  );
  const byRecordKind = parseDonphanToolResult(
    await handlers.queryPermitsHandler({
      county: options.county,
      sql: `SELECT improvement_action, count(*) AS permit_rows
            FROM permits
            GROUP BY improvement_action
            ORDER BY improvement_action`,
      limit: 20,
    }),
    "queryPermits",
  );
  const knownParcel = parseDonphanToolResult(
    await handlers.queryPermitsHandler({
      county: options.county,
      sql: `SELECT parcel_identifier, permit_number, improvement_type,
                   improvement_status, permit_issue_date, source_system
            FROM permits
            WHERE parcel_identifier = '494318013550'
            ORDER BY permit_issue_date NULLS LAST, permit_number
            LIMIT 5`,
      limit: 5,
    }),
    "queryPermits",
  );
  const evidence = {
    generatedAt: new Date().toISOString(),
    county: options.county,
    parquetPath: options.parquetPath,
    donphanModulePath: options.modulePath,
    handlersInvoked: [
      "getPermitQuerySchema",
      "getPermitCoverage",
      "queryPermits",
    ],
    schema,
    coverage,
    reconciliation,
    byRecordKind,
    knownParcel,
  };
  await mkdir(dirname(options.outputPath), { recursive: true, mode: 0o700 });
  await writeFile(
    options.outputPath,
    `${JSON.stringify(evidence, null, 2)}\n`,
    { encoding: "utf8", mode: 0o600 },
  );
  return evidence;
}

/**
 * Run the standalone validator under elephant-mcp's TypeScript runtime.
 *
 * @returns {Promise<void>} Resolves after one concise evidence summary.
 */
async function main() {
  const options = parseDonphanValidationOptions(process.argv.slice(2));
  if (options === null) {
    process.stdout.write(USAGE);
    return;
  }
  const evidence = await validateBrowardPermitsWithDonphan(options);
  process.stdout.write(
    `${JSON.stringify({
      event: "broward_donphan_permit_validation_completed",
      outputPath: options.outputPath,
      handlersInvoked: evidence.handlersInvoked,
      coverage: evidence.coverage,
      reconciliation: evidence.reconciliation,
    })}\n`,
  );
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main().catch((caught) => {
    process.stderr.write(
      `${JSON.stringify({
        event: "broward_donphan_permit_validation_failed",
        error: caught instanceof Error ? caught.message : String(caught),
      })}\n`,
    );
    process.exitCode = 1;
  });
}
