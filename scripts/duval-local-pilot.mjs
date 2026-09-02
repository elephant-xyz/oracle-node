#!/usr/bin/env node
/**
 * Duval County local pilot (no AWS, no Railway-specific APIs).
 *
 * Fetches native COJ HTML, writes capture inputs, runs the Duval transform
 * scripts, packages transformed_output.zip, and writes a reconciled manifest.
 *
 * Usage:
 *   node scripts/duval-local-pilot.mjs --limit=50 --concurrency=2
 *   node scripts/duval-local-pilot.mjs --limit=1 --seed=downloads/duval/pilot-seed-50.csv
 *   node scripts/duval-local-pilot.mjs --retry-failures --job-id=duval-local-2026-09-01
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
  countSeedCsvRows,
  readSeedCsvFile,
  streamSeedCsvRows,
} from "./hillsborough/lib.mjs";
import {
  appendFailure,
  initRunProgress,
  loadRetryableFailures,
  runStatePaths,
  sleep,
  writeRunProgress,
} from "./hillsborough/run-state.mjs";
import {
  COUNTY_FIPS,
  COUNTY_NAME,
  toCanonicalReDisplay,
} from "./duval/lib.mjs";
import {
  assertHtmlMatchesRequestedRe,
  assertManifestReconciled,
  assertTransformedCounty,
  buildPropertySeed,
  buildUnnormalizedAddress,
  classifyDuvalFailure,
  parseDuvalPilotArgs,
  seedRowToCsv,
  toCojCaptureUrl,
} from "./duval/pilot-lib.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(__dirname, "..");
const TRANSFORMS_ROOT = resolve(ROOT, "../Counties-trasform-scripts");
const SCRIPT_DIR = resolve(TRANSFORMS_ROOT, "duval/scripts");
const DEFAULT_SEED = resolve(ROOT, "downloads/duval/pilot-seed-50.csv");
const DEFAULT_OUTPUT = resolve(ROOT, "downloads/duval/pilot-run");
const STATE_CODE = "FL";
const JURISDICTION_KEY = "duval_appraiser";

const TRANSFORM_SCRIPTS = [
  "ownerMapping.js",
  "structureMapping.js",
  "utilityMapping.js",
  "layoutMapping.js",
  "data_extractor.js",
];

const REQUIRED_DATA_ARTIFACTS = ["property.json", "address.json"];

const COJ_FETCH_HEADERS = {
  Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.9",
  "User-Agent":
    "Mozilla/5.0 (compatible; ElephantDuvalPilot/1.0; +https://github.com/elephant-xyz/oracle-node)",
};

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
 * @param {string} url
 * @returns {Promise<string>}
 */
async function fetchCojDetailHtml(url) {
  const response = await fetch(url, {
    headers: COJ_FETCH_HEADERS,
    redirect: "follow",
    signal: AbortSignal.timeout(30_000),
  });
  if (!response.ok) {
    throw new Error(`COJ detail HTTP ${response.status} for ${url}`);
  }
  return response.text();
}

/**
 * @param {string} dataDir
 * @returns {Promise<void>}
 */
async function runTransformScripts(dataDir) {
  const nodePath = [
    resolve(SCRIPT_DIR, "node_modules"),
    resolve(ROOT, "node_modules"),
    process.env.NODE_PATH,
  ]
    .filter(Boolean)
    .join(process.platform === "win32" ? ";" : ":");

  for (const scriptName of TRANSFORM_SCRIPTS) {
    const scriptPath = join(SCRIPT_DIR, scriptName);
    await runCommand(process.execPath, [scriptPath], {
      cwd: dataDir,
      env: {
        ...process.env,
        NODE_PATH: nodePath,
      },
    });
  }
}

/**
 * @param {string} parcelDir
 * @param {string} zipPath
 * @returns {Promise<number>}
 */
async function packageTransformedZip(parcelDir, zipPath) {
  const zip = new AdmZip();
  const dataDir = join(parcelDir, "data");
  const entries = (await pathExists(dataDir))
    ? await readdir(dataDir, { withFileTypes: true })
    : [];
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
 * @returns {Promise<{
 *   ok: boolean;
 *   missing: string[];
 *   present: string[];
 *   hasGeometry: boolean;
 *   hasTax: boolean;
 *   hasOwner: boolean;
 *   artifactCount: number;
 * }>}
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
    files.some((name) => name.startsWith("geometry_parcel_"));
  const hasTax = files.some((name) => /^tax_\d+\.json$/.test(name));
  const hasOwner =
    files.some((name) => name.startsWith("person_")) ||
    files.some((name) => name.startsWith("company_"));
  return {
    ok: missing.length === 0,
    missing,
    present,
    hasGeometry,
    hasTax,
    hasOwner,
    artifactCount: files.filter((name) => name.endsWith(".json")).length,
  };
}

/**
 * @template T
 * @param {T[] | AsyncIterable<T>} items
 * @param {number} concurrency
 * @param {(item: T, index: number) => Promise<void>} worker
 * @returns {Promise<void>}
 */
async function mapPool(items, concurrency, worker) {
  if (Array.isArray(items)) {
    if (items.length === 0) return;
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
  const runners = Array.from({ length: Math.max(1, concurrency) }, async () => {
    while (true) {
      const { value, done } = await iterator.next();
      if (done) break;
      const i = index++;
      await worker(value, i);
    }
  });
  await Promise.all(runners);
}

/**
 * @param {() => Promise<void>} fn
 * @param {{
 *   maxAttempts?: number;
 *   onRetry?: (info: {
 *     attempt: number;
 *     error: unknown;
 *     classification: ReturnType<typeof classifyDuvalFailure>;
 *   }) => void;
 * }} [options]
 * @returns {Promise<void>}
 */
async function withDuvalRetry(fn, options = {}) {
  const maxAttempts = options.maxAttempts ?? 3;
  let lastError = /** @type {unknown} */ (undefined);
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      await fn();
      return;
    } catch (error) {
      lastError = error;
      const classification = classifyDuvalFailure(error);
      if (classification !== "transient" || attempt >= maxAttempts) {
        throw error;
      }
      options.onRetry?.({ attempt, error, classification });
      await sleep(1000 * 2 ** (attempt - 1));
    }
  }
  throw lastError instanceof Error
    ? lastError
    : new Error(String(lastError ?? "retry failed"));
}

/**
 * @param {ReturnType<typeof parseDuvalPilotArgs>} options
 */
async function runDuvalLocalPilot(options) {
  const seedPath = resolve(ROOT, options.seedPath || DEFAULT_SEED);
  const outputRoot = resolve(ROOT, options.outputRoot || DEFAULT_OUTPUT);
  const offset = Math.max(options.offset || 0, 0);
  const concurrency = Math.max(1, options.concurrency || 2);

  const isStreaming =
    !options.retryFailures && (options.limit === null || options.limit > 1000);
  let seedTotal = 0;
  /** @type {import("./hillsborough/lib.mjs").HillsboroughSeedRow[] | AsyncIterable<import("./hillsborough/lib.mjs").HillsboroughSeedRow>} */
  let rowsSource;

  if (options.retryFailures) {
    const allRows = await readSeedCsvFile(seedPath);
    const failures = await loadRetryableFailures(outputRoot, options.jobId);
    const byFolio = new Map(allRows.map((row) => [row.parcel_id, row]));
    const matchedRows = failures
      .map((failure) => byFolio.get(failure.folio))
      .filter(Boolean);
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
  let successCount = 0;
  let failureCount = 0;
  let skippedCount = 0;
  // progress.attempted is cumulative across resumed/retried runs so the saved
  // progress file stays meaningful; the manifest reconciles this run only.
  let attemptedThisRun = 0;
  /** @type {Array<Record<string, unknown>>} */
  const sampleResults = [];
  /** @type {Array<Record<string, unknown>>} */
  const recentFailures = [];
  /** @type {Map<string, number>} */
  const attemptCounts = new Map();

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

  let lastProgressFlush = 0;
  let progressFlushPending = false;
  let lastMeta = {
    lastFolio: /** @type {string | null} */ (null),
    lastEvent: "init",
  };

  /**
   * @param {{ lastFolio?: string | null; lastEvent?: string }} [meta]
   * @param {boolean} [force]
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

  try {
    await mapPool(rowsSource, concurrency, async (seed) => {
      const folio = seed.parcel_id;
      const pin = seed.source_identifier;
      if (!pin) {
        failureCount += 1;
        if (recentFailures.length < 200) {
          recentFailures.push({
            folio,
            pin,
            error: "missing source_identifier",
            classification: "permanent",
          });
        }
        attemptedThisRun += 1;
        progress.attempted += 1;
        progress.failed += 1;
        await appendFailure(outputRoot, options.jobId, {
          folio,
          error: "missing source_identifier",
          classification: "permanent",
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
        attemptedThisRun += 1;
        progress.attempted += 1;
        progress.skipped += 1;
        await triggerProgressFlush({
          lastFolio: folio,
          lastEvent: "skipped",
        });
        return;
      }

      try {
        await withDuvalRetry(
          async () => {
            await rm(parcelDir, { recursive: true, force: true });
            await mkdir(parcelDir, { recursive: true });
            await mkdir(join(parcelDir, "owners"), { recursive: true });

            const url = toCojCaptureUrl(seed);
            const html = await fetchCojDetailHtml(url);
            const canonicalRe = assertHtmlMatchesRequestedRe(html, pin);
            await writeFile(join(parcelDir, "input.html"), html, "utf8");

            const propertySeed = buildPropertySeed(seed);
            const unnormalizedAddress = buildUnnormalizedAddress(seed);
            await writeFile(
              join(parcelDir, "property_seed.json"),
              JSON.stringify(propertySeed, null, 2),
              "utf8",
            );
            await writeFile(
              join(parcelDir, "unnormalized_address.json"),
              JSON.stringify(unnormalizedAddress, null, 2),
              "utf8",
            );
            await writeFile(
              join(parcelDir, "seed.csv"),
              seedRowToCsv(seed),
              "utf8",
            );

            await runTransformScripts(parcelDir);

            const addressPath = join(parcelDir, "data", "address.json");
            const address = JSON.parse(await readFile(addressPath, "utf8"));
            assertTransformedCounty(address);

            const validation = await validateParcelOutputs(parcelDir);

            /**
             * @param {number | null} zipEntryCount
             * @returns {Promise<void>}
             */
            const writeSummary = (zipEntryCount) =>
              writeFile(
                join(parcelDir, "summary.json"),
                JSON.stringify(
                  {
                    folio,
                    pin,
                    canonicalRe,
                    displayRe: toCanonicalReDisplay(pin),
                    captureUrl: url,
                    siteAddress: seed.address,
                    validation,
                    zipEntryCount,
                  },
                  null,
                  2,
                ),
                "utf8",
              );

            // Zip only after validation passes: --skip-existing treats the zip
            // as proof of a completed parcel, so a zip beside a failed parcel
            // would be silently skipped on the next run instead of repaired.
            if (!validation.ok) {
              await writeSummary(null);
              throw new Error(
                `validation failed missing=${(validation.missing || []).join(",")}`,
              );
            }

            const zipEntryCount = await packageTransformedZip(
              parcelDir,
              zipPath,
            );
            await writeSummary(zipEntryCount);

            successCount += 1;
            if (validation.hasGeometry) withGeometryCount += 1;
            if (validation.hasTax) withTaxCount += 1;
            if (validation.hasOwner) withOwnerCount += 1;

            if (sampleResults.length < 200) {
              sampleResults.push({
                folio,
                pin,
                ok: true,
                validation,
                zipPath,
                siteAddress: seed.address,
              });
            }
            console.log(
              JSON.stringify({
                event: "parcel_complete",
                jobId: options.jobId,
                folio,
                ok: true,
                hasGeometry: validation.hasGeometry,
                hasTax: validation.hasTax,
                hasOwner: validation.hasOwner,
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
                  error: error instanceof Error ? error.message : String(error),
                }),
              );
            },
          },
        );
        attemptedThisRun += 1;
        progress.attempted += 1;
        progress.succeeded += 1;
        await triggerProgressFlush({
          lastFolio: folio,
          lastEvent: "success",
        });
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        const classification = classifyDuvalFailure(error);
        const attempts = (attemptCounts.get(folio) || 0) + options.maxAttempts;
        attemptCounts.set(folio, attempts);
        failureCount += 1;
        if (recentFailures.length < 200) {
          recentFailures.push({ folio, pin, error: message, classification });
        }
        attemptedThisRun += 1;
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
  }

  progress.status = "completed";
  await writeRunProgress(outputRoot, options.jobId, progress, {
    lastEvent: "completed",
  });
  const runPaths = runStatePaths(outputRoot, options.jobId);
  const reconciled = {
    seedRows: seedTotal,
    seedTotal,
    attempted: attemptedThisRun,
    attemptedCumulative: progress.attempted,
    success: successCount,
    failures: failureCount,
    skipped: skippedCount,
    withGeometry: withGeometryCount,
    withTax: withTaxCount,
    withOwner: withOwnerCount,
    countyFips: COUNTY_FIPS,
  };
  assertManifestReconciled(reconciled);

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
    reconciled,
    schemaChecks: {
      requiredDataArtifacts: REQUIRED_DATA_ARTIFACTS,
      requiredRootArtifacts: ["property_seed.json", "input.html"],
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
  const options = parseDuvalPilotArgs(process.argv.slice(2));
  if (!(await pathExists(SCRIPT_DIR))) {
    throw new Error(`Transform scripts missing at ${SCRIPT_DIR}`);
  }
  const result = await runDuvalLocalPilot(options);
  console.log(
    JSON.stringify({
      event: "duval_pilot_complete",
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
