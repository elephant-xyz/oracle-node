#!/usr/bin/env node
/**
 * Hillsborough County local pilot (no AWS).
 *
 * Fetches HCPA ParcelData JSON, renders HTML for the existing cheerio transform,
 * runs owner/structure/utility/layout mappings + data_extractor, validates
 * required outputs, and optionally loads into Neon elephant-query-db.
 *
 * Usage:
 *   node scripts/hillsborough-local-pilot.mjs
 *   node scripts/hillsborough-local-pilot.mjs --limit=5
 *   node scripts/hillsborough-local-pilot.mjs --limit=all --resume --job-id=hillsborough-full-2026-08-27
 *   node scripts/hillsborough-local-pilot.mjs --retry-failures --job-id=hillsborough-full-2026-08-27
 *   node scripts/hillsborough-local-pilot.mjs --load
 */

import { spawn } from "node:child_process";
import {
  access,
  mkdir,
  readFile,
  readdir,
  rm,
  writeFile,
} from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import AdmZip from "adm-zip";

import {
  COUNTY_NAME,
  HCPA_PARCEL_DATA_URL,
  JURISDICTION_KEY,
  STATE_CODE,
  buildInputHtmlFromParcelData,
  countSeedCsvRows,
  parsePilotArgs,
  parseSeedCsvText,
  readSeedCsvFile,
  streamSeedCsvRows,
} from "./hillsborough/lib.mjs";
import { TransformPool } from "./hillsborough/transform-pool.mjs";
import {
  appendFailure,
  classifyFailure,
  initRunProgress,
  loadRetryableFailures,
  runStatePaths,
  withTransientRetry,
  writeRunProgress,
} from "./hillsborough/run-state.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(__dirname, "..");
const TRANSFORMS_ROOT = resolve(ROOT, "../Counties-trasform-scripts");
const QUERY_DB_ROOT = resolve(ROOT, "../elephant-query-db");
const SCRIPT_DIR = resolve(TRANSFORMS_ROOT, "hillsborough/scripts");
const DEFAULT_SEED = resolve(ROOT, "downloads/hillsborough/pilot-seed-50.csv");
const DEFAULT_OUTPUT = resolve(ROOT, "downloads/hillsborough/pilot-run");

const TRANSFORM_SCRIPTS = [
  "ownerMapping.js",
  "structureMapping.js",
  "utilityMapping.js",
  "layoutMapping.js",
  "data_extractor.js",
];

/** Lexicon JSON files expected under each parcel's `data/` directory. */
const REQUIRED_DATA_ARTIFACTS = ["property.json", "address.json"];

/**
 * @param {string} path
 * @returns {Promise<boolean>}
 */
async function pathExists(path) {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

/**
 * @param {string} command
 * @param {string[]} args
 * @param {import("node:child_process").SpawnOptionsWithoutStdio} [options]
 * @returns {Promise<{ stdout: string; stderr: string }>}
 */
function runCommand(command, args, options = {}) {
  return new Promise((resolvePromise, reject) => {
    const child = spawn(command, args, {
      stdio: ["ignore", "pipe", "pipe"],
      ...options,
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) resolvePromise({ stdout, stderr });
      else {
        reject(
          new Error(
            `${command} ${args.join(" ")} failed (${code})\n${stderr || stdout}`,
          ),
        );
      }
    });
  });
}

/**
 * @param {string} pin
 * @returns {Promise<Record<string, unknown>>}
 */
async function fetchParcelData(pin) {
  const url = new URL(HCPA_PARCEL_DATA_URL);
  url.searchParams.set("pin", pin);
  const response = await fetch(url, {
    headers: {
      Accept: "application/json",
      Referer: "https://gis.hcpafl.org/PropertySearch/",
    },
  });
  if (!response.ok) {
    throw new Error(`ParcelData HTTP ${response.status} for pin=${pin}`);
  }
  const body = /** @type {Record<string, unknown>} */ (await response.json());
  if (!body.pin && !body.propertyCard) {
    throw new Error(`ParcelData empty for pin=${pin}`);
  }
  return body;
}

/**
 * @param {import("./hillsborough/lib.mjs").HillsboroughSeedRow} seed
 * @param {Record<string, unknown>} parcel
 * @returns {{ property_seed: Record<string, unknown>; unnormalized_address: Record<string, unknown> }}
 */
function buildSeedArtifacts(seed, parcel) {
  const pc = /** @type {Record<string, unknown>} */ (parcel.propertyCard || {});
  const pin = String(parcel.pin || seed.source_identifier || seed.pin || "");
  const displayStrap = String(pc.displayStrap || seed.display_pin || pin);
  const folio = String(pc.folio || seed.parcel_id || seed.folio || "");
  const siteAddress = String(parcel.siteAddress || seed.address || "");
  const lat = seed.latitude ? Number(seed.latitude) : null;
  const lon = seed.longitude ? Number(seed.longitude) : null;

  return {
    property_seed: {
      parcel_id: displayStrap || folio,
      folio,
      source_http_request: {
        method: "GET",
        url: `${HCPA_PARCEL_DATA_URL}?pin=${encodeURIComponent(pin)}`,
      },
      request_identifier: pin,
    },
    unnormalized_address: {
      full_address: siteAddress,
      unnormalized_address: siteAddress,
      county_jurisdiction: COUNTY_NAME,
      latitude: Number.isFinite(lat) ? lat : null,
      longitude: Number.isFinite(lon) ? lon : null,
    },
  };
}

/**
 * @param {string} dataDir
 * @returns {Promise<void>}
 */
async function runTransformScripts(dataDir) {
  for (const scriptName of TRANSFORM_SCRIPTS) {
    const scriptPath = join(SCRIPT_DIR, scriptName);
    await runCommand(process.execPath, [scriptPath], {
      cwd: dataDir,
      env: {
        ...process.env,
        NODE_PATH: resolve(ROOT, "node_modules"),
      },
    });
  }
}

/**
 * Package lexicon JSON from `parcelDir/data` under zip `data/` plus `property_seed.json`.
 * @param {string} parcelDir
 * @param {string} zipPath
 * @returns {Promise<number>}
 */
async function packageTransformedZip(parcelDir, zipPath) {
  const zip = new AdmZip();
  const dataDir = join(parcelDir, "data");
  const entries = await readdir(dataDir, { withFileTypes: true });
  let count = 0;
  for (const entry of entries) {
    if (!entry.isFile() || !entry.name.endsWith(".json")) continue;
    zip.addLocalFile(join(dataDir, entry.name), "data");
    count += 1;
  }
  const seedPath = join(parcelDir, "property_seed.json");
  if (await pathExists(seedPath)) {
    zip.addLocalFile(seedPath, "data");
    count += 1;
  }
  zip.writeZip(zipPath);
  return count;
}

/**
 * @param {string} parcelDir
 * @returns {Promise<{ ok: boolean; missing: string[]; present: string[]; hasGeometry: boolean; hasTax: boolean; hasOwner: boolean; permitCount: number; artifactCount: number }>}
 */
async function validateParcelOutputs(parcelDir) {
  const dataDir = join(parcelDir, "data");
  const files = (await pathExists(dataDir)) ? await readdir(dataDir) : [];
  const missing = REQUIRED_DATA_ARTIFACTS.filter(
    (name) => !files.includes(name),
  );
  if (!(await pathExists(join(parcelDir, "property_seed.json")))) {
    missing.push("property_seed.json");
  }
  const present = REQUIRED_DATA_ARTIFACTS.filter((name) =>
    files.includes(name),
  );
  const hasGeometry =
    files.includes("geometry.json") ||
    files.some((f) => f.startsWith("geometry_parcel_"));
  const hasTax = files.some((f) => /^tax_\d+\.json$/.test(f));
  const hasOwner =
    files.some((f) => f.startsWith("person_")) ||
    files.some((f) => f.startsWith("company_"));
  let permitCount = 0;
  try {
    const raw = await readFile(join(parcelDir, "parcel-data.json"), "utf8");
    const parcel = JSON.parse(raw);
    permitCount = Array.isArray(parcel.permitInfo)
      ? parcel.permitInfo.length
      : 0;
  } catch {
    permitCount = 0;
  }
  return {
    ok: missing.length === 0,
    missing,
    present,
    hasGeometry,
    hasTax,
    hasOwner,
    permitCount,
    artifactCount: files.filter((f) => f.endsWith(".json")).length,
  };
}
/**
 * @param {string} databaseUrl
 * @returns {Promise<{
 *   query: (text: string, values?: unknown[]) => Promise<{ rows: Array<Record<string, unknown>> }>;
 *   end: () => Promise<void>;
 * }>}
 */
async function createLoaderClient(databaseUrl) {
  const { Pool } = await import("pg");
  const pool = new Pool({ connectionString: databaseUrl });
  return {
    query: (text, values) => pool.query(text, values),
    end: () => pool.end(),
  };
}

/**
 * Load DATABASE_URL from elephant-query-db/.env.local when unset.
 * @returns {Promise<void>}
 */
async function loadDatabaseUrlFromEnvFile() {
  if (process.env.DATABASE_URL?.trim()) return;
  const envPath = join(QUERY_DB_ROOT, ".env.local");
  if (!(await pathExists(envPath))) return;
  const envText = await readFile(envPath, "utf8");
  for (const line of envText.split("\n")) {
    if (!line.startsWith("DATABASE_URL=") || line.startsWith("#")) continue;
    const value = line
      .slice("DATABASE_URL=".length)
      .trim()
      .replace(/^['"]|['"]$/g, "");
    if (value) process.env.DATABASE_URL = value;
  }
}

/**
 * @param {string[]} artifactPaths
 * @returns {Promise<{ loadedParcels: number; preparedRows: number }>}
 */
async function loadTransformedArtifacts(artifactPaths) {
  await loadDatabaseUrlFromEnvFile();
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error(
      "DATABASE_URL is required for --load (set env or elephant-query-db/.env.local)",
    );
  }

  const distLoader = join(QUERY_DB_ROOT, "dist/loader/index.js");
  const srcLoader = join(QUERY_DB_ROOT, "src/loader/index.ts");
  const loaderPath = (await pathExists(distLoader)) ? distLoader : srcLoader;
  if (!(await pathExists(loaderPath))) {
    throw new Error(
      `elephant-query-db loader missing at ${distLoader} or ${srcLoader}`,
    );
  }

  const loaderModule = await import(loaderPath);
  const { mapAppraisalTransformedFile, upsertPreparedRows } = loaderModule;

  const APPRAISAL_TABLE_ORDER = [
    "unnormalized_addresses",
    "addresses",
    "parcels",
    "properties",
    "property_improvements",
    "people",
    "companies",
    "deeds",
    "fact_sheets",
    "geometries",
    "sales_histories",
    "taxes",
    "property_valuations",
    "structures",
    "utilities",
    "layouts",
    "lots",
    "flood_storm_information",
    "files",
    "ownerships",
  ];

  /**
   * @param {Array<{ tableName: string }>} rows
   */
  function sortRows(rows) {
    const order = new Map(
      APPRAISAL_TABLE_ORDER.map((name, index) => [name, index]),
    );
    return [...rows].sort(
      (left, right) =>
        (order.get(left.tableName) ?? 999) -
        (order.get(right.tableName) ?? 999),
    );
  }

  const client = await createLoaderClient(databaseUrl);
  let loadedParcels = 0;
  let preparedRows = 0;

  try {
    for (const artifactPath of artifactPaths) {
      const zip = new AdmZip(artifactPath);
      const entries = zip
        .getEntries()
        .filter(
          (entry) =>
            entry.entryName.endsWith(".json") &&
            !entry.entryName.includes("relationship_"),
        );

      /** @type {Array<{ tableName: string }>} */
      const rows = [];
      for (const entry of entries) {
        const record = JSON.parse(entry.getData().toString("utf8"));
        const requestIdentifier =
          record.request_identifier ?? record.parcel_identifier;
        const filePath = entry.entryName.replace(/^data\//, "");
        const bundle = mapAppraisalTransformedFile({
          artifactUri: `file://${artifactPath}`,
          filePath,
          record,
          requestIdentifier,
          sourceSystem: JURISDICTION_KEY,
          countyName: COUNTY_NAME,
          stateCode: STATE_CODE,
        });
        rows.push(...bundle.rows);
      }

      const counters = await upsertPreparedRows(client, sortRows(rows));
      loadedParcels += 1;
      preparedRows += counters.attemptedRows;
      console.log(
        JSON.stringify({
          event: "hillsborough_pilot_loaded",
          artifactPath,
          attemptedRows: counters.attemptedRows,
          changedRows: counters.changedRows,
          unchangedRows: counters.unchangedRows,
        }),
      );
    }
  } finally {
    await client.end();
  }

  return { loadedParcels, preparedRows };
}

/**
 * @returns {Promise<number | null>}
 */
async function countNeonParcels() {
  await loadDatabaseUrlFromEnvFile();
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) return null;
  const client = await createLoaderClient(databaseUrl);
  try {
    const result = await client.query(
      "select count(*)::int as count from parcels where jurisdiction_key = $1",
      [JURISDICTION_KEY],
    );
    return result.rows[0]?.count ?? 0;
  } finally {
    await client.end();
  }
}

/**
 * Run a pool of concurrent workers over an array or async iterable.
 * @template T
 * @param {T[] | AsyncIterable<T>} items
 * @param {number} concurrency
 * @param {(item: T, index: number) => Promise<void>} worker
 * @returns {Promise<void>}
 */
async function mapPool(items, concurrency, worker) {
  if (Array.isArray(items)) {
    let next = 0;
    const runners = Array.from(
      { length: Math.min(concurrency, items.length) },
      async () => {
        while (next < items.length) {
          const index = next;
          next += 1;
          await worker(items[index], index);
        }
      },
    );
    await Promise.all(runners);
    return;
  }

  const iterator = items[Symbol.asyncIterator]();
  let index = 0;
  const runners = Array.from(
    { length: Math.max(1, concurrency) },
    async () => {
      while (true) {
        const { value, done } = await iterator.next();
        if (done) break;
        const i = index++;
        await worker(value, i);
      }
    },
  );
  await Promise.all(runners);
}

/**
 * @param {ReturnType<typeof parsePilotArgs>} options
 * @returns {Promise<{
 *   manifestPath: string;
 *   outputRoot: string;
 *   successCount: number;
 *   failureCount: number;
 *   totalCount: number;
 * }>}
 */
async function runHillsboroughLocalPilot(options) {
  const seedPath = resolve(ROOT, options.seedPath || DEFAULT_SEED);
  const outputRoot = resolve(ROOT, options.outputRoot || DEFAULT_OUTPUT);
  const offset = Math.max(options.offset || 0, 0);

  const isStreaming =
    !options.retryFailures &&
    (options.limit === null || options.limit > 1000);
  let seedTotal = 0;
  /** @type {import("./hillsborough/lib.mjs").HillsboroughSeedRow[] | AsyncIterable<import("./hillsborough/lib.mjs").HillsboroughSeedRow>} */
  let rowsSource;

  if (options.retryFailures) {
    const allRows = await readSeedCsvFile(seedPath);
    const failures = await loadRetryableFailures(outputRoot, options.jobId);
    const byFolio = new Map(allRows.map((row) => [row.parcel_id, row]));
    const matchedRows = failures
      .map((f) => byFolio.get(f.folio))
      .filter(/** @returns {row is import("./hillsborough/lib.mjs").HillsboroughSeedRow} */ (row) => Boolean(row));
    console.log(
      JSON.stringify({
        event: "retry_failures_loaded",
        jobId: options.jobId,
        retryable: failures.length,
        matchedSeedRows: matchedRows.length,
      }),
    );
    rowsSource = matchedRows;
    seedTotal = matchedRows.length;
  } else if (isStreaming) {
    const totalInCsv = await countSeedCsvRows(seedPath);
    const available = Math.max(totalInCsv - offset, 0);
    seedTotal =
      options.limit !== null ? Math.min(options.limit, available) : available;
    rowsSource = streamSeedCsvRows(seedPath, {
      limit: options.limit ?? null,
      offset,
    });
  } else {
    const loadedRows = await readSeedCsvFile(seedPath, {
      limit: options.limit ?? null,
      offset,
    });
    rowsSource = loadedRows;
    seedTotal = loadedRows.length;
  }

  if (seedTotal === 0 && !isStreaming) {
    throw new Error(
      options.retryFailures
        ? `No retryable failures for job ${options.jobId}`
        : `No seed rows in ${seedPath}`,
    );
  }

  await mkdir(outputRoot, { recursive: true });
  const progress = await initRunProgress(outputRoot, options.jobId, {
    seedPath,
    seedTotal,
  });
  // Reset runtime stream counters so scanning from row 0 accurately reflects exact CSV line position without double-counting
  if (!options.retryFailures) {
    progress.attempted = 0;
    progress.succeeded = 0;
    progress.failed = 0;
    progress.skipped = 0;
    progress.retried = 0;
  }

  let withGeometryCount = 0;
  let withTaxCount = 0;
  let withOwnerCount = 0;
  let embeddedPermitsCount = 0;
  let successCount = 0;
  let failureCount = 0;
  let skippedCount = 0;
  /** @type {Array<Record<string, unknown>>} */
  const sampleResults = [];
  /** @type {Array<Record<string, unknown>>} */
  const recentFailures = [];
  /** @type {string[]} */
  const zipPaths = [];
  /** @type {Map<string, number>} */
  const attemptCounts = new Map();

  /** Serialize progress file updates under concurrent workers. */
  let progressChain = Promise.resolve();
  /**
   * @param {() => Promise<void>} fn
   * @returns {Promise<void>}
   */
  function withProgressLock(fn) {
    const next = progressChain.then(fn, fn);
    progressChain = next.catch(() => {});
    return next;
  }

  /** Debounced progress flusher to avoid locking on every single parcel */
  let lastProgressFlush = 0;
  let progressFlushPending = false;
  let lastMeta = { lastFolio: /** @type {string | null} */ (null), lastEvent: "init" };

  /**
   * @param {{ lastFolio?: string | null; lastEvent?: string }} [meta]
   * @param {boolean} [force]
   * @returns {Promise<void>}
   */
  async function triggerProgressFlush(meta = {}, force = false) {
    if (meta.lastFolio) lastMeta.lastFolio = meta.lastFolio;
    if (meta.lastEvent) lastMeta.lastEvent = meta.lastEvent;

    const now = Date.now();
    if (!force && now - lastProgressFlush < 500) {
      if (!progressFlushPending) {
        progressFlushPending = true;
        setTimeout(() => {
          progressFlushPending = false;
          withProgressLock(() =>
            writeRunProgress(outputRoot, options.jobId, progress, lastMeta),
          );
          lastProgressFlush = Date.now();
        }, 500);
      }
      return;
    }

    lastProgressFlush = now;
    await withProgressLock(() =>
      writeRunProgress(outputRoot, options.jobId, progress, lastMeta),
    );
  }

  const transformPool = new TransformPool();

  try {
    await mapPool(rowsSource, options.concurrency, async (seed) => {
    const folio = seed.parcel_id;
    const pin = seed.source_identifier || seed.pin;
    if (!pin) {
      const classification = "permanent";
      failureCount += 1;
      if (recentFailures.length < 200) {
        recentFailures.push({
          folio,
          pin,
          error: "missing source_identifier/pin",
          classification,
        });
      }
      progress.attempted += 1;
      progress.failed += 1;
      await appendFailure(outputRoot, options.jobId, {
        folio,
        error: "missing source_identifier/pin",
        classification,
        attempts: 1,
        at: new Date().toISOString(),
        jobId: options.jobId,
      });
      await triggerProgressFlush({
        lastFolio: folio,
        lastEvent: "permanent_failure",
      });
      return;
    }

    const parcelDir = join(outputRoot, folio);
    const zipPath = join(parcelDir, "transformed_output.zip");
    if (options.skipExisting && (await pathExists(zipPath))) {
      skippedCount += 1;
      if (sampleResults.length < 200) {
        sampleResults.push({ folio, pin, ok: true, skipped: true, zipPath });
      }
      if (options.load) zipPaths.push(zipPath);
      progress.attempted += 1;
      progress.skipped += 1;
      await triggerProgressFlush({
        lastFolio: folio,
        lastEvent: "skipped",
      });
      return;
    }

    try {
      await withTransientRetry(
        async () => {
          await rm(parcelDir, { recursive: true, force: true });
          await mkdir(parcelDir, { recursive: true });

          const parcel = await fetchParcelData(pin);
          await writeFile(
            join(parcelDir, "parcel-data.json"),
            JSON.stringify(parcel, null, 2),
            "utf8",
          );

          const html = buildInputHtmlFromParcelData(parcel);
          await writeFile(join(parcelDir, "input.html"), html, "utf8");

          const { property_seed, unnormalized_address } = buildSeedArtifacts(
            seed,
            parcel,
          );
          await writeFile(
            join(parcelDir, "property_seed.json"),
            JSON.stringify(property_seed, null, 2),
            "utf8",
          );
          await writeFile(
            join(parcelDir, "unnormalized_address.json"),
            JSON.stringify(unnormalized_address, null, 2),
            "utf8",
          );

          if (seed.parcel_polygon) {
            const csv = [
              "parcel_id,parcel_polygon,longitude,latitude",
              `"${seed.parcel_id}","${String(seed.parcel_polygon).replace(/"/g, '""')}",${seed.longitude || ""},${seed.latitude || ""}`,
            ].join("\n");
            await writeFile(join(parcelDir, "seed.csv"), csv, "utf8");
          }

          await mkdir(join(parcelDir, "owners"), { recursive: true });
          await transformPool.run(parcelDir);
          const zipEntryCount = await packageTransformedZip(parcelDir, zipPath);

          const validation = await validateParcelOutputs(parcelDir);
          validation.zipEntryCount = zipEntryCount;
          await writeFile(
            join(parcelDir, "summary.json"),
            JSON.stringify(
              {
                folio,
                pin,
                siteAddress: parcel.siteAddress,
                landUse: parcel.landUse,
                validation,
                embeddedPermitCount: validation.permitCount,
              },
              null,
              2,
            ),
            "utf8",
          );

          if (!validation.ok) {
            throw new Error(
              `validation failed missing=${(validation.missing || []).join(",")}`,
            );
          }

          successCount += 1;
          if (validation.hasGeometry) withGeometryCount += 1;
          if (validation.hasTax) withTaxCount += 1;
          if (validation.hasOwner) withOwnerCount += 1;
          embeddedPermitsCount += validation.permitCount || 0;

          if (sampleResults.length < 200) {
            sampleResults.push({
              folio,
              pin,
              ok: true,
              validation,
              zipPath,
              siteAddress: parcel.siteAddress,
              landUse: parcel.landUse,
            });
          }
          if (options.load) zipPaths.push(zipPath);
          console.log(
            JSON.stringify({
              event: "parcel_complete",
              jobId: options.jobId,
              folio,
              ok: true,
              hasGeometry: validation.hasGeometry,
              hasTax: validation.hasTax,
              hasOwner: validation.hasOwner,
              permitCount: validation.permitCount,
            }),
          );
        },
        {
          maxAttempts: options.maxAttempts,
          onRetry: ({ attempt, error, classification }) => {
            progress.retried += 1;
            console.warn(
              JSON.stringify({
                event: "parcel_retry",
                jobId: options.jobId,
                folio,
                attempt,
                classification,
                error:
                  error instanceof Error ? error.message : String(error),
              }),
            );
          },
        },
      );
      progress.attempted += 1;
      progress.succeeded += 1;
      await triggerProgressFlush({
        lastFolio: folio,
        lastEvent: "success",
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const classification = classifyFailure(error);
      const attempts = (attemptCounts.get(folio) || 0) + options.maxAttempts;
      attemptCounts.set(folio, attempts);

      failureCount += 1;
      if (recentFailures.length < 200) {
        recentFailures.push({
          folio,
          pin,
          error: message,
          classification,
        });
      }

      progress.attempted += 1;
      progress.failed += 1;

      await appendFailure(outputRoot, options.jobId, {
        folio,
        pin,
        error: message,
        classification,
        attempts,
        at: new Date().toISOString(),
        jobId: options.jobId,
      });

      await triggerProgressFlush({
        lastFolio: folio,
        lastEvent: "failure",
      });

      console.error(
        JSON.stringify({
          event: "parcel_failed",
          jobId: options.jobId,
          folio,
          classification,
          error: message,
        }),
      );
    }
  });
  } finally {
    await triggerProgressFlush({ lastEvent: "final" }, true);
    transformPool.close();
  }

  progress.status = "completed";
  await writeRunProgress(outputRoot, options.jobId, progress, {
    lastEvent: "completed",
  });
  const runPaths = runStatePaths(outputRoot, options.jobId);

  const manifest = {
    county: COUNTY_NAME,
    state: STATE_CODE,
    jurisdictionKey: JURISDICTION_KEY,
    jobId: options.jobId,
    generatedAt: new Date().toISOString(),
    seedPath,
    outputRoot,
    progressPath: runPaths.progressPath,
    failuresPath: runPaths.failuresPath,
    reconciled: {
      seedRows: seedTotal,
      seedTotal,
      attempted: progress.attempted,
      success: successCount,
      failures: failureCount,
      skipped: skippedCount,
      withGeometry: withGeometryCount,
      withTax: withTaxCount,
      withOwner: withOwnerCount,
      embeddedPermitRows: embeddedPermitsCount,
    },
    schemaChecks: {
      requiredDataArtifacts: REQUIRED_DATA_ARTIFACTS,
      requiredRootArtifacts: ["property_seed.json"],
      allSuccessHaveRequired: failureCount === 0,
    },
    failures: recentFailures,
    results: sampleResults,
  };

  const manifestPath = join(outputRoot, "pilot-manifest.json");
  await writeFile(manifestPath, JSON.stringify(manifest, null, 2), "utf8");
  await writeFile(
    join(runPaths.jobDir, "manifest-snapshot.json"),
    JSON.stringify(
      {
        jobId: options.jobId,
        generatedAt: manifest.generatedAt,
        reconciled: manifest.reconciled,
        failures: manifest.failures,
        progress,
      },
      null,
      2,
    ),
    "utf8",
  );

  if (options.load) {
    const okZips = zipPaths.filter(Boolean);
    if (okZips.length === 0) {
      throw new Error("No transformed zips available for --load");
    }
    const before = await countNeonParcels();
    const loadResult = await loadTransformedArtifacts(okZips);
    const after = await countNeonParcels();
    manifest.neonLoad = {
      loadedZips: loadResult.loadedParcels,
      preparedRows: loadResult.preparedRows,
      parcelsBefore: before,
      parcelsAfter: after,
    };
    await writeFile(manifestPath, JSON.stringify(manifest, null, 2), "utf8");
  }

  return {
    manifestPath,
    outputRoot,
    jobId: options.jobId,
    progressPath: runPaths.progressPath,
    failuresPath: runPaths.failuresPath,
    successCount,
    failureCount,
    skippedCount,
    totalCount: progress.attempted,
    parcelsPerMinute: progress.parcelsPerMinute,
    etaIso: progress.etaIso,
  };
}

async function main() {
  const options = parsePilotArgs(process.argv.slice(2));
  if (!(await pathExists(SCRIPT_DIR))) {
    throw new Error(`Transform scripts missing at ${SCRIPT_DIR}`);
  }
  const result = await runHillsboroughLocalPilot(options);
  console.log(
    JSON.stringify({
      event: "hillsborough_pilot_complete",
      ...result,
    }),
  );
  if (result.failureCount > 0 && result.successCount === 0) {
    process.exitCode = 1;
  }
}

main().catch((error) => {
  console.error(
    error instanceof Error ? (error.stack ?? error.message) : error,
  );
  process.exitCode = 1;
});
