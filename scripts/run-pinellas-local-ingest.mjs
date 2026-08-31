#!/usr/bin/env node

import { spawn } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

const DEFAULT_SEED_PATH = "data/seeds/pinellas-pilot.csv";
const DEFAULT_FLOW_PATH = "multi-request-flows/Pinellas.json";
const DEFAULT_SCRIPTS_DIRECTORY =
  "../Counties-trasform-scripts/pinellas/scripts";
const DEFAULT_OUTPUT_DIRECTORY = "downloads/pinellas/local-ingest";
const CLI_PACKAGE = "@elephant-xyz/cli@1.58.1";
const LOCAL_IPFS_SHIM_PATH = "scripts/local-ipfs-fetch-shim.cjs";
const LOCAL_IPFS_GATEWAY = "http://127.0.0.1:8080";

/**
 * @typedef {Record<string, string>} SeedRow
 *
 * @typedef {object} LocalIngestCliOptions
 * @property {string} seedPath - Pinellas pilot seed CSV.
 * @property {string} flowPath - Multi-request flow JSON.
 * @property {string} scriptsDirectory - Existing Pinellas transform scripts.
 * @property {string} outputDirectory - Durable local output directory.
 * @property {number | null} limit - Optional row cap after mixed selection.
 * @property {boolean} allRows - When true, ingest every seed row instead of one per use group.
 * @property {boolean} skipValidate - When true, skip `elephant-cli validate`.
 *
 * @typedef {object} ParcelIngestResult
 * @property {string} parcelId - 18-digit STRAP.
 * @property {string} useGroup - Seed use-group label.
 * @property {boolean} prepareSuccess - Whether county-prep prepare completed.
 * @property {boolean} transformSuccess - Whether the Pinellas scripts transform completed.
 * @property {boolean | null} validationSuccess - Lexicon validate result, or null when skipped.
 * @property {string | null} propertyUsageType - Transformed `property.json` usage type.
 * @property {string | null} error - First failure message.
 */

/**
 * Quote one CSV cell using RFC 4180.
 *
 * @param {string} value - Cell value.
 * @returns {string} Encoded cell.
 */
export function encodeCsvCell(value) {
  if (!/[",\r\n]/.test(value)) return value;
  return `"${value.replaceAll('"', '""')}"`;
}

/**
 * Parse RFC 4180 CSV text into row objects.
 *
 * @param {string} text - Complete CSV document.
 * @returns {SeedRow[]} Parsed records.
 */
export function parseCsvRecords(text) {
  /** @type {string[][]} */
  const table = [];
  /** @type {string[]} */
  let row = [];
  let cell = "";
  let inQuotes = false;
  const source = text.endsWith("\n") ? text : `${text}\n`;
  for (let index = 0; index < source.length; index += 1) {
    const character = source[index];
    if (inQuotes) {
      if (character === '"') {
        if (source[index + 1] === '"') {
          cell += '"';
          index += 1;
        } else {
          inQuotes = false;
        }
      } else {
        cell += character;
      }
      continue;
    }
    if (character === '"') {
      inQuotes = true;
      continue;
    }
    if (character === ",") {
      row.push(cell);
      cell = "";
      continue;
    }
    if (character === "\n") {
      row.push(cell);
      table.push(row);
      row = [];
      cell = "";
      continue;
    }
    if (character !== "\r") cell += character;
  }
  if (table.length === 0) return [];
  const [header, ...body] = table;
  return body
    .filter((values) => values.some((value) => value.length > 0))
    .map((values) => {
      /** @type {SeedRow} */
      const record = {};
      for (let index = 0; index < header.length; index += 1) {
        record[header[index]] = values[index] ?? "";
      }
      return record;
    });
}

/**
 * Select one seed row per `use_group`, preserving first-seen order.
 *
 * @param {readonly SeedRow[]} rows - Complete seed rows.
 * @returns {SeedRow[]} Mixed-type subset.
 */
export function selectMixedRows(rows) {
  /** @type {SeedRow[]} */
  const selected = [];
  const seen = new Set();
  for (const row of rows) {
    const useGroup = row.use_group ?? "";
    if (useGroup.length === 0 || seen.has(useGroup)) continue;
    seen.add(useGroup);
    selected.push(row);
  }
  return selected;
}

/**
 * Extract print-page HTML from a multi-request prepare capture.
 *
 * @param {unknown} capture - Parsed `{STRAP}.json` prepare artifact.
 * @returns {string} HTML document.
 */
export function unwrapPropertyPrintHtml(capture) {
  if (capture === null || typeof capture !== "object") {
    throw new Error("Prepare capture is not an object");
  }
  const print = /** @type {{ PropertyPrint?: { response?: unknown } }} */ (
    capture
  ).PropertyPrint;
  const html = print?.response;
  if (typeof html !== "string" || !html.toLowerCase().includes("<html")) {
    throw new Error("PropertyPrint response is not HTML");
  }
  return html;
}

/**
 * Parse local-ingest CLI flags.
 *
 * @param {readonly string[]} argv - Arguments after the script path.
 * @returns {LocalIngestCliOptions} Validated options.
 */
export function parseCliOptions(argv) {
  /** @type {LocalIngestCliOptions} */
  const options = {
    seedPath: DEFAULT_SEED_PATH,
    flowPath: DEFAULT_FLOW_PATH,
    scriptsDirectory: DEFAULT_SCRIPTS_DIRECTORY,
    outputDirectory: DEFAULT_OUTPUT_DIRECTORY,
    limit: null,
    allRows: false,
    skipValidate: false,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const flag = argv[index];
    if (flag === "--all") {
      options.allRows = true;
      continue;
    }
    if (flag === "--skip-validate") {
      options.skipValidate = true;
      continue;
    }
    const value = argv[index + 1];
    if (typeof value !== "string" || value.startsWith("--")) {
      throw new Error(`Missing value for ${flag}`);
    }
    index += 1;
    if (flag === "--seed") options.seedPath = value;
    else if (flag === "--flow") options.flowPath = value;
    else if (flag === "--scripts") options.scriptsDirectory = value;
    else if (flag === "--output") options.outputDirectory = value;
    else if (flag === "--limit") options.limit = Number.parseInt(value, 10);
    else throw new Error(`Unknown option: ${flag}`);
  }
  if (
    options.limit !== null &&
    (!Number.isInteger(options.limit) || options.limit <= 0)
  ) {
    throw new Error("--limit must be a positive integer");
  }
  return options;
}

/**
 * Render one seed row as a complete CSV document.
 *
 * @param {SeedRow} row - Seed record.
 * @returns {string} Header plus one data row.
 */
export function renderSeedCsv(row) {
  const columns = Object.keys(row);
  const header = columns.map(encodeCsvCell).join(",");
  const line = columns.map((column) => encodeCsvCell(row[column] ?? "")).join(
    ",",
  );
  return `${header}\n${line}\n`;
}

/**
 * Keep zip entries that lexicon `validate` is allowed to see.
 *
 * @param {string} entryName - Archive member path.
 * @returns {boolean} False for leftover `fact_sheet.json`.
 */
export function shouldKeepValidationEntry(entryName) {
  const base = entryName.split("/").pop() ?? entryName;
  return base !== "fact_sheet.json";
}

/**
 * Strip `?query` from lexicon `source_http_request.url` values.
 *
 * @param {unknown} value - JSON tree.
 * @returns {unknown} Tree with path-only request URLs.
 */
export function stripQueryFromSourceHttpRequestTree(value) {
  if (Array.isArray(value)) {
    return value.map((item) => stripQueryFromSourceHttpRequestTree(item));
  }
  if (value === null || typeof value !== "object") return value;
  const record = /** @type {Record<string, unknown>} */ (value);
  /** @type {Record<string, unknown>} */
  const next = {};
  for (const [key, child] of Object.entries(record)) {
    next[key] = stripQueryFromSourceHttpRequestTree(child);
  }
  if (typeof next.url === "string" && next.url.includes("?")) {
    const [base, query] = next.url.split("?");
    const params = new URLSearchParams(query);
    const existing = next.multiValueQueryString;
    /** @type {Record<string, string[]>} */
    const multi =
      existing && typeof existing === "object"
        ? { .../** @type {Record<string, string[]>} */ (existing) }
        : {};
    for (const [paramKey, paramValue] of params.entries()) {
      if (!multi[paramKey]) multi[paramKey] = [paramValue];
    }
    next.url = base;
    next.multiValueQueryString = multi;
  }
  return next;
}

/**
 * @param {string} command - Process to spawn.
 * @param {readonly string[]} args - Process arguments.
 * @param {string} cwd - Working directory.
 * @param {NodeJS.ProcessEnv} [extraEnv] - Extra environment variables.
 * @returns {Promise<void>} Resolves when the process exits 0.
 */
function runCommand(command, args, cwd, extraEnv) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, [...args], {
      cwd,
      stdio: "inherit",
      env: extraEnv ? { ...process.env, ...extraEnv } : process.env,
    });
    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`${command} ${args.join(" ")} exited ${code}`));
    });
  });
}

/**
 * @param {string} zipPath - Archive to write.
 * @param {Record<string, string>} files - Arcname to filesystem path.
 * @returns {Promise<void>} Resolves when the archive exists.
 */
async function writeZip(zipPath, files) {
  const script = `
import json, sys, zipfile
spec = json.load(sys.stdin)
with zipfile.ZipFile(spec["zipPath"], "w") as archive:
    for arcname, source in spec["files"].items():
        archive.write(source, arcname)
`;
  const child = spawn("python3", ["-c", script], { stdio: ["pipe", "inherit", "inherit"] });
  await new Promise((resolve, reject) => {
    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`python zip writer exited ${code}`));
    });
    child.stdin.write(JSON.stringify({ zipPath, files }));
    child.stdin.end();
  });
}

/**
 * @param {string} zipPath - Archive to read.
 * @param {string} entryName - Entry path.
 * @returns {Promise<Buffer>} Entry bytes.
 */
async function readZipEntry(zipPath, entryName) {
  const script = `
import json, sys, zipfile
spec = json.load(sys.stdin)
with zipfile.ZipFile(spec["zipPath"]) as archive:
    sys.stdout.buffer.write(archive.read(spec["entryName"]))
`;
  return new Promise((resolve, reject) => {
    /** @type {Buffer[]} */
    const chunks = [];
    const child = spawn("python3", ["-c", script], {
      stdio: ["pipe", "pipe", "inherit"],
    });
    child.stdout.on("data", (chunk) => chunks.push(chunk));
    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0) resolve(Buffer.concat(chunks));
      else reject(new Error(`python zip reader exited ${code}`));
    });
    child.stdin.write(JSON.stringify({ zipPath, entryName }));
    child.stdin.end();
  });
}

/**
 * Rewrite JSON members in a transformed zip so `source_http_request.url` has no query string.
 *
 * @param {string} zipPath - Transformed archive.
 * @returns {Promise<void>} Resolves when the archive is rewritten.
 */
export async function stripQueryFromTransformedZip(zipPath) {
  const script = `
import json, sys, zipfile, io
zip_path = sys.argv[1]
buf = io.BytesIO()
with zipfile.ZipFile(zip_path, "r") as source, zipfile.ZipFile(buf, "w") as dest:
    for info in source.infolist():
        data = source.read(info.filename)
        if info.filename.endswith(".json"):
            try:
                parsed = json.loads(data.decode("utf-8"))
            except Exception:
                dest.writestr(info, data)
                continue
            def walk(value):
                if isinstance(value, list):
                    return [walk(item) for item in value]
                if not isinstance(value, dict):
                    return value
                out = {key: walk(child) for key, child in value.items()}
                url = out.get("url")
                if isinstance(url, str) and "?" in url:
                    base, query = url.split("?", 1)
                    multi = dict(out.get("multiValueQueryString") or {})
                    for part in query.split("&"):
                        if not part:
                            continue
                        key, _, val = part.partition("=")
                        if key and key not in multi:
                            multi[key] = [val]
                    out["url"] = base
                    out["multiValueQueryString"] = multi
                return out
            data = json.dumps(walk(parsed), indent=2).encode("utf-8")
        dest.writestr(info, data)
with open(zip_path, "wb") as handle:
    handle.write(buf.getvalue())
`;
  await runCommand("python3", ["-c", script, zipPath], process.cwd());
}

/**
 * Ingest one STRAP locally: seed transform → prepare → unwrap HTML → county transform.
 *
 * @param {object} params - Parcel parameters.
 * @param {SeedRow} params.row - Seed row.
 * @param {LocalIngestCliOptions} params.options - Run options.
 * @param {string} params.scriptsZipPath - Packaged Pinellas scripts.
 * @param {string} params.repoRoot - oracle-node root.
 * @returns {Promise<ParcelIngestResult>} Per-parcel outcome.
 */
async function ingestParcel({ row, options, scriptsZipPath, repoRoot }) {
  const parcelId = row.parcel_id;
  const useGroup = row.use_group ?? "";
  const parcelDir = path.join(options.outputDirectory, parcelId);
  await mkdir(parcelDir, { recursive: true });
  const workDir = await mkdtemp(path.join(os.tmpdir(), `pinellas-${parcelId}-`));
  try {
    const seedCsvPath = path.join(workDir, "seed.csv");
    const seedInputZip = path.join(workDir, "seed-input.zip");
    const seedOutputZip = path.join(parcelDir, "seed-output.zip");
    const countyPrepZip = path.join(workDir, "county-prep-input.zip");
    const preparedZip = path.join(parcelDir, "prepared.zip");
    const preparedWithHtmlZip = path.join(parcelDir, "prepared-with-html.zip");
    const transformedZip = path.join(parcelDir, "transformed.zip");
    const validationCsv = path.join(parcelDir, "validation.csv");
    await writeFile(seedCsvPath, renderSeedCsv(row), "utf8");
    await writeZip(seedInputZip, { "seed.csv": seedCsvPath });
    await runCommand(
      "npx",
      [
        "--yes",
        CLI_PACKAGE,
        "transform",
        "--input-zip",
        seedInputZip,
        "--output-zip",
        seedOutputZip,
      ],
      repoRoot,
    );
    const extractDir = path.join(workDir, "seed-extract");
    await mkdir(extractDir, { recursive: true });
    await runCommand(
      "python3",
      [
        "-c",
        "import sys, zipfile; zipfile.ZipFile(sys.argv[1]).extractall(sys.argv[2])",
        seedOutputZip,
        extractDir,
      ],
      repoRoot,
    );
    await writeZip(countyPrepZip, {
      "unnormalized_address.json": path.join(
        extractDir,
        "data",
        "unnormalized_address.json",
      ),
      "property_seed.json": path.join(extractDir, "data", "property_seed.json"),
      "input.csv": seedCsvPath,
    });
    await runCommand(
      "npx",
      [
        "--yes",
        CLI_PACKAGE,
        "prepare",
        countyPrepZip,
        "--multi-request-flow-file",
        options.flowPath,
        "--output-zip",
        preparedZip,
      ],
      repoRoot,
    );
    const captureBytes = await readZipEntry(preparedZip, `${parcelId}.json`);
    const html = unwrapPropertyPrintHtml(JSON.parse(captureBytes.toString("utf8")));
    const htmlPath = path.join(workDir, "input.html");
    await writeFile(htmlPath, html, "utf8");
    const capturePath = path.join(workDir, `${parcelId}.json`);
    await writeFile(capturePath, captureBytes);
    await writeZip(preparedWithHtmlZip, {
      "unnormalized_address.json": path.join(
        extractDir,
        "data",
        "unnormalized_address.json",
      ),
      "property_seed.json": path.join(extractDir, "data", "property_seed.json"),
      "input.csv": seedCsvPath,
      [`${parcelId}.json`]: capturePath,
      "input.html": htmlPath,
    });
    await runCommand(
      "npx",
      [
        "--yes",
        CLI_PACKAGE,
        "transform",
        "--input-zip",
        preparedWithHtmlZip,
        "--scripts-zip",
        scriptsZipPath,
        "--output-zip",
        transformedZip,
      ],
      repoRoot,
    );
    await stripQueryFromTransformedZip(transformedZip);
    const propertyJson = JSON.parse(
      (await readZipEntry(transformedZip, "data/property.json")).toString("utf8"),
    );
    const propertyUsageType =
      typeof propertyJson.property_usage_type === "string"
        ? propertyJson.property_usage_type
        : null;
    /** @type {boolean | null} */
    let validationSuccess = null;
    if (!options.skipValidate) {
      try {
        const shimPath = path.join(repoRoot, LOCAL_IPFS_SHIM_PATH);
        await runCommand(
          "npx",
          [
            "--yes",
            "--package",
            CLI_PACKAGE,
            "--",
            "env",
            `PINELLAS_IPFS_GATEWAY=${process.env.PINELLAS_IPFS_GATEWAY ?? LOCAL_IPFS_GATEWAY}`,
            `NODE_OPTIONS=--require ${shimPath}`,
            "elephant-cli",
            "validate",
            transformedZip,
            "--output-csv",
            validationCsv,
          ],
          repoRoot,
        );
        validationSuccess = true;
      } catch (error) {
        validationSuccess = false;
        return {
          parcelId,
          useGroup,
          prepareSuccess: true,
          transformSuccess: true,
          validationSuccess,
          propertyUsageType,
          error: error instanceof Error ? error.message : String(error),
        };
      }
    }
    return {
      parcelId,
      useGroup,
      prepareSuccess: true,
      transformSuccess: true,
      validationSuccess,
      propertyUsageType,
      error: null,
    };
  } catch (error) {
    return {
      parcelId,
      useGroup,
      prepareSuccess: false,
      transformSuccess: false,
      validationSuccess: null,
      propertyUsageType: null,
      error: error instanceof Error ? error.message : String(error),
    };
  } finally {
    await rm(workDir, { recursive: true, force: true });
  }
}

/**
 * Package Pinellas transform scripts, excluding backups.
 *
 * @param {string} scriptsDirectory - Scripts folder.
 * @param {string} destination - Zip path.
 * @returns {Promise<void>} Resolves when packaged.
 */
async function packageScripts(scriptsDirectory, destination) {
  const script = `
import json, sys, zipfile
from pathlib import Path
spec = json.load(sys.stdin)
root = Path(spec["scriptsDirectory"])
with zipfile.ZipFile(spec["destination"], "w") as archive:
    for path in root.iterdir():
        if path.is_file() and path.suffix == ".js":
            archive.write(path, path.name)
`;
  const child = spawn("python3", ["-c", script], { stdio: ["pipe", "inherit", "inherit"] });
  await new Promise((resolve, reject) => {
    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`python scripts packager exited ${code}`));
    });
    child.stdin.write(JSON.stringify({ scriptsDirectory, destination }));
    child.stdin.end();
  });
}

/**
 * Run the local Pinellas prepare → transform ingest.
 *
 * @param {LocalIngestCliOptions} options - Validated CLI options.
 * @returns {Promise<ParcelIngestResult[]>} Per-parcel results.
 */
export async function runLocalIngest(options) {
  const repoRoot = process.cwd();
  const outputDirectory = path.resolve(options.outputDirectory);
  await mkdir(outputDirectory, { recursive: true });
  const seedText = await readFile(options.seedPath, "utf8");
  const allRows = parseCsvRecords(seedText);
  const selected = options.allRows ? allRows : selectMixedRows(allRows);
  const rows =
    options.limit === null ? selected : selected.slice(0, options.limit);
  const scriptsZipPath = path.join(outputDirectory, "pinellas-scripts.zip");
  await packageScripts(path.resolve(options.scriptsDirectory), scriptsZipPath);
  /** @type {ParcelIngestResult[]} */
  const results = [];
  for (const row of rows) {
    results.push(
      await ingestParcel({
        row,
        options: { ...options, outputDirectory },
        scriptsZipPath,
        repoRoot,
      }),
    );
  }
  await writeFile(
    path.join(outputDirectory, "summary.json"),
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        total: results.length,
        transformsPassed: results.filter((result) => result.transformSuccess)
          .length,
        validationsPassed: results.filter(
          (result) => result.validationSuccess === true,
        ).length,
        results,
      },
      null,
      2,
    ),
    "utf8",
  );
  return results;
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const options = parseCliOptions(process.argv.slice(2));
  runLocalIngest(options)
    .then((results) => {
      console.log(
        JSON.stringify(
          {
            total: results.length,
            transformsPassed: results.filter((result) => result.transformSuccess)
              .length,
            validationsPassed: results.filter(
              (result) => result.validationSuccess === true,
            ).length,
            outputDirectory: path.resolve(options.outputDirectory),
          },
          null,
          2,
        ),
      );
      if (results.some((result) => !result.transformSuccess)) {
        process.exitCode = 1;
      }
    })
    .catch((error) => {
      console.error(error instanceof Error ? error.message : String(error));
      process.exitCode = 1;
    });
}
