import { spawn } from "child_process";
import { createHash } from "crypto";
import { createReadStream } from "fs";
import {
  mkdir,
  open,
  readFile,
  readdir,
  rename,
  stat,
  writeFile,
} from "fs/promises";
import path from "path";
import readline from "readline";
import { pathToFileURL, fileURLToPath } from "url";
import { parseArgs } from "util";
import { parse as parseYaml } from "yaml";

import {
  findZipMatchedAddresses,
  normalizeAddressForMatch,
  normalizeZipPrefixes,
  parseCorporateDataRecord,
} from "../workflow/lambdas/permit-harvest-worker/sunbiz-corporate.mjs";
import { transformSunbizRecord } from "./transform-sunbiz-corporate-to-lexicon.mjs";

const PREPARATION_SCHEMA_VERSION = "oracle-node.broward-sunbiz-local.v1";
const CHECKPOINT_SCHEMA_VERSION =
  "oracle-node.broward-sunbiz-local-checkpoint.v1";
const CANDIDATE_SCHEMA_VERSION =
  "oracle-node.broward-sunbiz-address-candidate.v1";
const DEFAULT_CHECKPOINT_INTERVAL = 10_000;
const SCRIPT_DIRECTORY = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_SOURCE_CATALOG_PATH = path.resolve(
  SCRIPT_DIRECTORY,
  "../docs/broward-sources.yaml",
);

/**
 * @typedef {NonNullable<ReturnType<typeof parseCorporateDataRecord>>} SunbizCorporateRecord
 */

/**
 * @typedef {ReturnType<typeof findZipMatchedAddresses>[number]} SunbizZipMatchedAddress
 */

/**
 * @typedef {"auto" | "text" | "zip"} RequestedSourceFormat
 */

/**
 * @typedef {"text" | "zip"} SourceFormat
 */

/**
 * @typedef {"inside" | "outside" | "unresolved"} AddressValidationStatus
 */

/**
 * @typedef {"records" | "lexicon" | "unresolved" | "outside"} OutputKind
 */

/**
 * @typedef {object} BrowardSunbizConfiguration
 * @property {string} county - Lowercase county name read from the source catalog.
 * @property {string} state - Two-letter state code read from the source catalog.
 * @property {string} countyFips - Five-digit county FIPS code read from the source catalog.
 * @property {string[]} zipCandidates - Exact, unique five-digit ZIP candidates listed under `sunbiz.zip_candidates`; no ranges are expanded.
 * @property {string} sourceCatalogPath - Absolute path to the source catalog.
 * @property {string} sourceCatalogSha256 - SHA-256 digest of the complete source catalog text.
 */

/**
 * @typedef {object} AddressValidationEntry
 * @property {string} validationKey - Stable key emitted in unresolved/outside candidate files.
 * @property {AddressValidationStatus} status - County-boundary decision for the exact normalized address.
 * @property {string | null} countyFips - County FIPS established by the local boundary/address evidence.
 * @property {string | null} evidence - Non-empty local evidence description required for inside/outside decisions.
 */

/**
 * @typedef {object} LocalSourceLine
 * @property {string} line - One raw Sunbiz fixed-width corporate row.
 * @property {string} sourceFileName - Text file basename or ZIP entry name.
 * @property {number} sourceLineNumber - One-based line number within that text file or ZIP entry.
 */

/**
 * @typedef {object} InputFingerprint
 * @property {string} path - Absolute, real local input path used by this run.
 * @property {number} size - Input size in bytes.
 * @property {number} mtimeMs - Input modification time in epoch milliseconds.
 * @property {SourceFormat} format - Effective input format.
 */

/**
 * @typedef {object} ReconciliationCounts
 * @property {number} sourceRecordsRead - Total fixed-width rows scanned.
 * @property {number} invalidRecordCount - Rows rejected by the shared Sunbiz parser because no document number was present.
 * @property {number} validNonCandidateRecordCount - Parsed rows without an address in an exact catalog ZIP candidate.
 * @property {number} candidateRecordCount - Parsed rows with at least one address in an exact catalog ZIP candidate.
 * @property {number} candidateAddressMatchCount - Candidate address-role matches across candidate records.
 * @property {number} verifiedInsideAddressMatchCount - Candidate address-role matches verified inside Broward by the validation manifest.
 * @property {number} verifiedOutsideAddressMatchCount - Candidate address-role matches verified outside Broward by the validation manifest.
 * @property {number} unresolvedAddressMatchCount - Candidate address-role matches without sufficient county evidence.
 * @property {number} emittedBrowardRecordCount - Candidate records emitted after at least one address was verified inside Broward.
 * @property {number} outsideOnlyRecordCount - Candidate records excluded because every candidate address was verified outside Broward.
 * @property {number} unresolvedWithoutInsideRecordCount - Candidate records excluded because no candidate address was verified inside and at least one remained unresolved.
 * @property {number} lexiconBundleCount - Existing-transform bundles emitted for Broward-scoped records.
 */

/**
 * @typedef {object} CheckpointCursor
 * @property {number} inputSequence - One-based row sequence across all input text files or ZIP entries.
 * @property {string | null} sourceFileName - Last durably checkpointed text file or ZIP entry.
 * @property {number} sourceLineNumber - Last durably checkpointed one-based line number within the source file.
 */

/**
 * @typedef {object} BrowardSunbizCheckpoint
 * @property {string} schemaVersion - Checkpoint schema version.
 * @property {"running" | "paused" | "complete"} status - Durable run status.
 * @property {InputFingerprint} input - Input identity used to reject unsafe resumes.
 * @property {string} configurationSha256 - Digest of county catalog, validation manifest, schema, and transform-output mode.
 * @property {CheckpointCursor} cursor - Last input row included in durable output offsets and counts.
 * @property {ReconciliationCounts} counts - Durable reconciliation counts at the cursor.
 * @property {Record<OutputKind, number>} outputOffsets - Durable byte lengths used to roll back uncheckpointed output on resume.
 * @property {string} updatedAt - ISO timestamp of the checkpoint write.
 */

/**
 * @typedef {object} PreparationOutputPaths
 * @property {string} records - Broward-scoped shared-parser extraction records as JSONL.
 * @property {string} lexicon - Existing-transform bundles for Broward-scoped records as JSONL.
 * @property {string} unresolved - Candidate address occurrences that remain unresolved as JSONL.
 * @property {string} outside - Candidate address occurrences verified outside Broward as JSONL.
 * @property {string} checkpoint - Resumable checkpoint JSON.
 * @property {string} reconciliation - Reconciliation summary JSON.
 */

/**
 * @typedef {object} PreparationSummary
 * @property {string} schemaVersion - Preparation output schema version.
 * @property {"paused" | "complete"} status - Whether the input was fully scanned.
 * @property {string} completedAt - ISO timestamp for this invocation's durable summary.
 * @property {{ name: string, state: string, fips: string }} county - Target county identity.
 * @property {InputFingerprint} input - Local source identity.
 * @property {string} sourceCatalogPath - Catalog supplying exact ZIP candidates.
 * @property {string | null} validationManifestPath - Local validation manifest path, or null when all candidates fail closed.
 * @property {string[]} exactZipCandidates - Exact candidate list read from the catalog.
 * @property {ReconciliationCounts} counts - Reconciliation counts.
 * @property {{ sourceRowsBalanced: boolean, candidateRecordsBalanced: boolean, candidateAddressesBalanced: boolean, allBalanced: boolean }} reconciliation - Arithmetic invariant results.
 * @property {{ records: string, lexiconBundles: string | null, unresolvedCandidates: string, outsideCandidates: string, checkpoint: string }} outputs - Local output paths.
 */

/**
 * @typedef {object} PrepareBrowardSunbizOptions
 * @property {string} inputPath - Local daily/quarterly fixed-width text file or ZIP archive.
 * @property {string} outputDir - Local output directory.
 * @property {string | undefined} [sourceCatalogPath] - Optional path override for the checked-in Broward source catalog.
 * @property {string | undefined} [validationManifestPath] - Optional JSONL/JSON array containing exact-address county decisions.
 * @property {RequestedSourceFormat | undefined} [format] - Input format override; defaults to extension-based auto detection.
 * @property {boolean | undefined} [resume] - Resume from the durable checkpoint and truncate uncheckpointed output.
 * @property {number | undefined} [checkpointInterval] - Rows processed between durable checkpoints.
 * @property {number | null | undefined} [maxSourceRecords] - Optional per-invocation row cap used for smoke runs and resumability checks.
 * @property {boolean | undefined} [emitLexiconBundles] - Run the existing Sunbiz transform for each Broward-scoped record; defaults to true.
 */

/**
 * @typedef {object} OutputWriter
 * @property {Record<OutputKind, number>} offsets - Current UTF-8 byte offsets for every append-only output.
 * @property {(kind: OutputKind, value: unknown) => Promise<void>} append - Append one JSON value and newline to an output.
 * @property {() => Promise<void>} sync - Flush every output file to durable local storage.
 * @property {() => Promise<void>} close - Close every output file.
 */

/**
 * Determine whether an unknown value is a non-array object.
 *
 * @param {unknown} value - Candidate object value.
 * @returns {value is Record<string, unknown>} True when the value can be inspected by string property name.
 */
function isObjectRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Determine whether an unknown error is a Node error with a specific code.
 *
 * @param {unknown} error - Thrown value.
 * @param {string} code - Expected Node error code.
 * @returns {boolean} True when the error has the requested code.
 */
function hasErrorCode(error, code) {
  return (
    isObjectRecord(error) &&
    typeof error.code === "string" &&
    error.code === code
  );
}

/**
 * Compute a lowercase SHA-256 digest for text.
 *
 * @param {string} text - UTF-8 text to hash.
 * @returns {string} Sixty-four-character hexadecimal SHA-256 digest.
 */
function sha256Text(text) {
  return createHash("sha256").update(text, "utf8").digest("hex");
}

/**
 * Read and strictly validate the Broward Sunbiz configuration.
 *
 * The function intentionally consumes only the exact strings listed in
 * `docs/broward-sources.yaml`. It rejects malformed values instead of expanding
 * ranges or inferring neighboring ZIPs.
 *
 * @param {string} [sourceCatalogPath] - Broward source catalog path.
 * @returns {Promise<BrowardSunbizConfiguration>} Validated county identity and exact ZIP candidates.
 */
export async function loadBrowardSunbizConfiguration(
  sourceCatalogPath = DEFAULT_SOURCE_CATALOG_PATH,
) {
  const absolutePath = path.resolve(sourceCatalogPath);
  const sourceText = await readFile(absolutePath, "utf8");
  const parsed = /** @type {unknown} */ (parseYaml(sourceText));
  if (!isObjectRecord(parsed) || !isObjectRecord(parsed.sunbiz)) {
    throw new Error(`Missing sunbiz configuration in ${absolutePath}`);
  }
  if (
    parsed.county !== "broward" ||
    parsed.state !== "FL" ||
    parsed.fips !== "12011"
  ) {
    throw new Error(
      `Expected Broward, FL (FIPS 12011) in ${absolutePath}; refusing a different county`,
    );
  }
  const rawCandidates = parsed.sunbiz.zip_candidates;
  if (!Array.isArray(rawCandidates) || rawCandidates.length === 0) {
    throw new Error(`Missing sunbiz.zip_candidates in ${absolutePath}`);
  }
  /** @type {string[]} */
  const zipCandidates = [];
  for (const candidate of rawCandidates) {
    if (typeof candidate !== "string" || !/^\d{5}$/.test(candidate)) {
      throw new Error(
        `Every Broward Sunbiz ZIP candidate must be an exact quoted five-digit string; received ${JSON.stringify(candidate)}`,
      );
    }
    zipCandidates.push(candidate);
  }
  if (new Set(zipCandidates).size !== zipCandidates.length) {
    throw new Error("Broward Sunbiz ZIP candidates must be unique");
  }
  const normalized = normalizeZipPrefixes(zipCandidates);
  if (
    normalized.length !== zipCandidates.length ||
    normalized.some((candidate, index) => candidate !== zipCandidates[index])
  ) {
    throw new Error(
      "Broward ZIP candidates changed during normalization; ranges and partial prefixes are not allowed",
    );
  }
  return {
    county: "broward",
    state: "FL",
    countyFips: "12011",
    zipCandidates,
    sourceCatalogPath: absolutePath,
    sourceCatalogSha256: sha256Text(sourceText),
  };
}

/**
 * Build a stable validation key for an exact normalized Sunbiz address.
 *
 * The key excludes entity/role identifiers so one local boundary decision can
 * be reused when the same physical address appears on multiple registrations.
 *
 * @param {SunbizZipMatchedAddress["address"]} address - Parsed Sunbiz address.
 * @returns {string} Versioned SHA-256 address key.
 */
export function createAddressValidationKey(address) {
  const components = [
    normalizeAddressForMatch(address.line1),
    normalizeAddressForMatch(address.line2),
    normalizeAddressForMatch(address.city),
    normalizeAddressForMatch(address.state),
    String(address.zip ?? "")
      .replace(/\D/g, "")
      .slice(0, 9),
    normalizeAddressForMatch(address.country),
  ];
  return `broward-address-v1:${sha256Text(JSON.stringify(components))}`;
}

/**
 * Check whether an address has enough detail for reusable county validation.
 *
 * ZIP-only or city/ZIP-only rows are always unresolved because validating one
 * such row could incorrectly approve unrelated addresses in a cross-county ZIP.
 *
 * @param {SunbizZipMatchedAddress["address"]} address - Parsed Sunbiz address.
 * @returns {boolean} True when street, city, state, and five ZIP digits are present.
 */
function isAddressSpecificEnough(address) {
  return Boolean(
    normalizeAddressForMatch(address.line1) &&
    normalizeAddressForMatch(address.city) &&
    normalizeAddressForMatch(address.state) &&
    /^\d{5}/.test(String(address.zip ?? "").replace(/\D/g, "")),
  );
}

/**
 * Convert JSONL or a JSON array into unknown manifest values.
 *
 * @param {string} text - Validation manifest contents.
 * @param {string} manifestPath - Path used in parse errors.
 * @returns {unknown[]} Parsed candidate values.
 */
function parseManifestValues(text, manifestPath) {
  const trimmed = text.trim();
  if (!trimmed) return [];
  if (trimmed.startsWith("[")) {
    const parsed = /** @type {unknown} */ (JSON.parse(trimmed));
    if (!Array.isArray(parsed)) {
      throw new Error(`Expected a JSON array in ${manifestPath}`);
    }
    return parsed;
  }
  return trimmed
    .split(/\r?\n/)
    .filter((line) => line.trim().length > 0)
    .map((line, index) => {
      try {
        return /** @type {unknown} */ (JSON.parse(line));
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        throw new Error(
          `Invalid JSONL at ${manifestPath}:${index + 1}: ${message}`,
        );
      }
    });
}

/**
 * Parse one fail-closed county validation entry.
 *
 * `inside` decisions require Broward FIPS 12011 and evidence. `outside`
 * decisions require a different county FIPS and evidence. Every other usable
 * candidate remains unresolved.
 *
 * @param {unknown} value - Parsed JSON/JSONL value.
 * @param {string} countyFips - Target Broward FIPS.
 * @param {string} manifestPath - Path used in validation errors.
 * @param {number} ordinal - One-based entry ordinal used in validation errors.
 * @returns {AddressValidationEntry} Strict validation decision.
 */
function parseAddressValidationEntry(value, countyFips, manifestPath, ordinal) {
  if (!isObjectRecord(value)) {
    throw new Error(
      `Validation entry ${ordinal} in ${manifestPath} must be an object`,
    );
  }
  const validationKey = value.validationKey;
  const status = value.status;
  const entryCountyFips = value.countyFips;
  const evidence = value.evidence;
  if (
    typeof validationKey !== "string" ||
    !/^broward-address-v1:[a-f0-9]{64}$/.test(validationKey)
  ) {
    throw new Error(
      `Validation entry ${ordinal} in ${manifestPath} has an invalid validationKey`,
    );
  }
  if (status !== "inside" && status !== "outside" && status !== "unresolved") {
    throw new Error(
      `Validation entry ${ordinal} in ${manifestPath} must use status inside, outside, or unresolved`,
    );
  }
  const normalizedCountyFips =
    typeof entryCountyFips === "string" && /^\d{5}$/.test(entryCountyFips)
      ? entryCountyFips
      : null;
  const normalizedEvidence =
    typeof evidence === "string" && evidence.trim() ? evidence.trim() : null;
  if (status === "inside") {
    if (normalizedCountyFips !== countyFips || !normalizedEvidence) {
      throw new Error(
        `Inside validation ${validationKey} must include countyFips ${countyFips} and non-empty evidence`,
      );
    }
  }
  if (status === "outside") {
    if (
      normalizedCountyFips === null ||
      normalizedCountyFips === countyFips ||
      !normalizedEvidence
    ) {
      throw new Error(
        `Outside validation ${validationKey} must include a non-Broward countyFips and non-empty evidence`,
      );
    }
  }
  return {
    validationKey,
    status,
    countyFips: normalizedCountyFips,
    evidence: normalizedEvidence,
  };
}

/**
 * Load exact-address county decisions from local JSONL or a JSON array.
 *
 * @param {string | undefined} validationManifestPath - Optional local manifest.
 * @param {string} countyFips - Target Broward FIPS.
 * @returns {Promise<{ entries: Map<string, AddressValidationEntry>, absolutePath: string | null, sha256: string }>} Decisions and provenance digest.
 */
export async function loadAddressValidationManifest(
  validationManifestPath,
  countyFips,
) {
  if (!validationManifestPath) {
    return {
      entries: new Map(),
      absolutePath: null,
      sha256: sha256Text("no-validation-manifest"),
    };
  }
  const absolutePath = path.resolve(validationManifestPath);
  const text = await readFile(absolutePath, "utf8");
  const values = parseManifestValues(text, absolutePath);
  /** @type {Map<string, AddressValidationEntry>} */
  const entries = new Map();
  for (const [index, value] of values.entries()) {
    const entry = parseAddressValidationEntry(
      value,
      countyFips,
      absolutePath,
      index + 1,
    );
    const existing = entries.get(entry.validationKey);
    if (existing && JSON.stringify(existing) !== JSON.stringify(entry)) {
      throw new Error(
        `Conflicting validation decisions for ${entry.validationKey}`,
      );
    }
    entries.set(entry.validationKey, entry);
  }
  return {
    entries,
    absolutePath,
    sha256: sha256Text(text),
  };
}

/**
 * Resolve the effective input format.
 *
 * @param {string} inputPath - Local input path.
 * @param {RequestedSourceFormat} requestedFormat - CLI/programmatic format selection.
 * @returns {SourceFormat} Effective text or ZIP format.
 */
function resolveSourceFormat(inputPath, requestedFormat) {
  if (requestedFormat === "text" || requestedFormat === "zip") {
    return requestedFormat;
  }
  return /\.zip$/i.test(inputPath) ? "zip" : "text";
}

/**
 * Read an entire child-process stream as UTF-8 text.
 *
 * @param {import("stream").Readable} stream - Child stdout or stderr stream.
 * @returns {Promise<string>} Complete UTF-8 stream content.
 */
async function streamToText(stream) {
  /** @type {Buffer[]} */
  const chunks = [];
  for await (const chunk of /** @type {AsyncIterable<unknown>} */ (stream)) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk)));
  }
  return Buffer.concat(chunks).toString("utf8");
}

/**
 * Create a completion promise immediately after spawning a child.
 *
 * @param {import("child_process").ChildProcess} child - Spawned local process.
 * @returns {Promise<{ code: number | null, signal: NodeJS.Signals | null }>} Exit status.
 */
function childCompletion(child) {
  return new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("close", (code, signal) => resolve({ code, signal }));
  });
}

/**
 * List text entries in a local ZIP using the system unzip implementation.
 *
 * The Sunbiz quarterly ZIP has historically used Deflate64 (method 9), which
 * the worker's yauzl reader does not support. The local path therefore uses
 * `unzip`, matching the established Lee runbook without requiring AWS.
 *
 * @param {string} inputPath - Local ZIP path.
 * @returns {Promise<string[]>} Archive-order `.txt` entry names.
 */
async function listZipTextEntries(inputPath) {
  const child = spawn("unzip", ["-Z1", inputPath], {
    stdio: ["ignore", "pipe", "pipe"],
  });
  const completion = childCompletion(child);
  const [stdout, stderr, result] = await Promise.all([
    streamToText(child.stdout),
    streamToText(child.stderr),
    completion,
  ]);
  if (result.code !== 0) {
    throw new Error(
      `Unable to list ZIP entries (${result.code ?? result.signal}): ${stderr.trim()}`,
    );
  }
  const entries = stdout
    .split(/\r?\n/)
    .filter((entry) => entry.length > 0 && !entry.endsWith("/"))
    .filter((entry) => /\.txt$/i.test(entry));
  if (entries.length === 0) {
    throw new Error(`ZIP contains no .txt entries: ${inputPath}`);
  }
  if (entries.some((entry) => /[\r\n]/.test(entry))) {
    throw new Error(`ZIP contains an unsupported newline in an entry name`);
  }
  return entries;
}

/**
 * Yield fixed-width rows from one text entry in a local ZIP.
 *
 * @param {string} inputPath - Local ZIP path.
 * @param {string} entryName - Exact archive entry name.
 * @returns {AsyncGenerator<LocalSourceLine, void, void>} Entry rows with per-entry line numbers.
 */
async function* iterateZipEntryLines(inputPath, entryName) {
  const child = spawn("unzip", ["-p", inputPath, entryName], {
    stdio: ["ignore", "pipe", "pipe"],
  });
  const completion = childCompletion(child);
  const stderrPromise = streamToText(child.stderr);
  const reader = readline.createInterface({
    input: child.stdout,
    crlfDelay: Infinity,
  });
  let sourceLineNumber = 0;
  let exhausted = false;
  try {
    for await (const line of reader) {
      sourceLineNumber += 1;
      yield {
        line,
        sourceFileName: entryName,
        sourceLineNumber,
      };
    }
    exhausted = true;
  } finally {
    reader.close();
    if (!exhausted && child.exitCode === null && child.signalCode === null) {
      child.kill();
    }
    const [stderr, result] = await Promise.all([stderrPromise, completion]);
    if (exhausted && result.code !== 0) {
      throw new Error(
        `Unable to read ZIP entry ${entryName} (${result.code ?? result.signal}): ${stderr.trim()}`,
      );
    }
  }
}

/**
 * Yield fixed-width rows from a local text file or every text entry in a ZIP.
 *
 * @param {string} inputPath - Absolute local input path.
 * @param {SourceFormat} format - Effective local input format.
 * @returns {AsyncGenerator<LocalSourceLine, void, void>} Source rows in deterministic file/archive order.
 */
async function* iterateLocalSourceLines(inputPath, format) {
  if (format === "text") {
    const reader = readline.createInterface({
      input: createReadStream(inputPath),
      crlfDelay: Infinity,
    });
    let sourceLineNumber = 0;
    for await (const line of reader) {
      sourceLineNumber += 1;
      yield {
        line,
        sourceFileName: path.basename(inputPath),
        sourceLineNumber,
      };
    }
    return;
  }
  const entries = await listZipTextEntries(inputPath);
  for (const entryName of entries) {
    yield* iterateZipEntryLines(inputPath, entryName);
  }
}

/**
 * Create zeroed reconciliation counters.
 *
 * @returns {ReconciliationCounts} Fresh counter set.
 */
function createEmptyCounts() {
  return {
    sourceRecordsRead: 0,
    invalidRecordCount: 0,
    validNonCandidateRecordCount: 0,
    candidateRecordCount: 0,
    candidateAddressMatchCount: 0,
    verifiedInsideAddressMatchCount: 0,
    verifiedOutsideAddressMatchCount: 0,
    unresolvedAddressMatchCount: 0,
    emittedBrowardRecordCount: 0,
    outsideOnlyRecordCount: 0,
    unresolvedWithoutInsideRecordCount: 0,
    lexiconBundleCount: 0,
  };
}

/**
 * Build all managed output paths below one local directory.
 *
 * @param {string} outputDir - Absolute local output directory.
 * @returns {PreparationOutputPaths} Managed file paths.
 */
function buildOutputPaths(outputDir) {
  return {
    records: path.join(outputDir, "broward-records.jsonl"),
    lexicon: path.join(outputDir, "broward-lexicon-bundles.jsonl"),
    unresolved: path.join(outputDir, "unresolved-candidates.jsonl"),
    outside: path.join(outputDir, "outside-candidates.jsonl"),
    checkpoint: path.join(outputDir, "checkpoint.json"),
    reconciliation: path.join(outputDir, "reconciliation.json"),
  };
}

/**
 * Ensure a fresh run cannot overwrite output and a resume has a checkpoint.
 *
 * @param {string} outputDir - Absolute output directory.
 * @param {boolean} resume - Whether this invocation is a resume.
 * @param {PreparationOutputPaths} outputPaths - Managed paths.
 * @returns {Promise<void>} Resolves after output preconditions are enforced.
 */
async function prepareOutputDirectory(outputDir, resume, outputPaths) {
  if (resume) {
    try {
      await stat(outputPaths.checkpoint);
    } catch (error) {
      if (hasErrorCode(error, "ENOENT")) {
        throw new Error(
          `Cannot resume without checkpoint: ${outputPaths.checkpoint}`,
        );
      }
      throw error;
    }
    return;
  }
  await mkdir(outputDir, { recursive: true });
  const entries = await readdir(outputDir);
  if (entries.length > 0) {
    throw new Error(
      `Fresh output directory must be empty: ${outputDir}; use --resume only for a matching checkpoint`,
    );
  }
}

/**
 * Atomically write one pretty JSON value with a final newline.
 *
 * @param {string} destinationPath - Final JSON path.
 * @param {unknown} value - JSON-serializable value.
 * @returns {Promise<void>} Resolves after atomic replacement.
 */
async function writeJsonAtomic(destinationPath, value) {
  const temporaryPath = `${destinationPath}.tmp-${process.pid}`;
  await writeFile(temporaryPath, `${JSON.stringify(value, null, 2)}\n`, "utf8");
  await rename(temporaryPath, destinationPath);
}

/**
 * Read a JSON file and require an object at its root.
 *
 * @param {string} filePath - JSON file path.
 * @returns {Promise<Record<string, unknown>>} Parsed object.
 */
async function readJsonObject(filePath) {
  const parsed = /** @type {unknown} */ (
    JSON.parse(await readFile(filePath, "utf8"))
  );
  if (!isObjectRecord(parsed)) {
    throw new Error(`Expected a JSON object in ${filePath}`);
  }
  return parsed;
}

/**
 * Check that every reconciliation counter property is a non-negative integer.
 *
 * @param {unknown} value - Candidate counters.
 * @returns {value is ReconciliationCounts} True when all expected counters are valid.
 */
function isReconciliationCounts(value) {
  if (!isObjectRecord(value)) return false;
  const expected = Object.keys(createEmptyCounts());
  return expected.every(
    (key) =>
      typeof value[key] === "number" &&
      Number.isInteger(value[key]) &&
      value[key] >= 0,
  );
}

/**
 * Parse and validate the durable checkpoint shape.
 *
 * @param {Record<string, unknown>} value - Parsed checkpoint object.
 * @param {string} checkpointPath - Path used in validation errors.
 * @returns {BrowardSunbizCheckpoint} Valid checkpoint.
 */
function parseCheckpoint(value, checkpointPath) {
  const input = value.input;
  const cursor = value.cursor;
  const offsets = value.outputOffsets;
  const status = value.status;
  if (
    value.schemaVersion !== CHECKPOINT_SCHEMA_VERSION ||
    (status !== "running" && status !== "paused" && status !== "complete") ||
    typeof value.configurationSha256 !== "string" ||
    typeof value.updatedAt !== "string" ||
    !isObjectRecord(input) ||
    typeof input.path !== "string" ||
    typeof input.size !== "number" ||
    typeof input.mtimeMs !== "number" ||
    (input.format !== "text" && input.format !== "zip") ||
    !isObjectRecord(cursor) ||
    typeof cursor.inputSequence !== "number" ||
    (typeof cursor.sourceFileName !== "string" &&
      cursor.sourceFileName !== null) ||
    typeof cursor.sourceLineNumber !== "number" ||
    !isReconciliationCounts(value.counts) ||
    !isObjectRecord(offsets) ||
    !["records", "lexicon", "unresolved", "outside"].every(
      (key) =>
        typeof offsets[key] === "number" &&
        Number.isInteger(offsets[key]) &&
        offsets[key] >= 0,
    )
  ) {
    throw new Error(`Invalid Broward Sunbiz checkpoint: ${checkpointPath}`);
  }
  return /** @type {BrowardSunbizCheckpoint} */ (value);
}

/**
 * Compare input fingerprints exactly for safe resume.
 *
 * @param {InputFingerprint} left - Current input identity.
 * @param {InputFingerprint} right - Checkpoint input identity.
 * @returns {boolean} True when path, size, mtime, and format are identical.
 */
function fingerprintsEqual(left, right) {
  return (
    left.path === right.path &&
    left.size === right.size &&
    left.mtimeMs === right.mtimeMs &&
    left.format === right.format
  );
}

/**
 * Open managed JSONL files and restore durable offsets on resume.
 *
 * @param {PreparationOutputPaths} paths - Managed output paths.
 * @param {Record<OutputKind, number>} durableOffsets - Last checkpointed UTF-8 offsets.
 * @param {boolean} resume - Whether existing files should be truncated to durable offsets.
 * @returns {Promise<OutputWriter>} Append-only writer with explicit byte positions.
 */
async function createOutputWriter(paths, durableOffsets, resume) {
  const pathByKind = {
    records: paths.records,
    lexicon: paths.lexicon,
    unresolved: paths.unresolved,
    outside: paths.outside,
  };
  const handles = {
    records: await open(pathByKind.records, resume ? "a+" : "w+"),
    lexicon: await open(pathByKind.lexicon, resume ? "a+" : "w+"),
    unresolved: await open(pathByKind.unresolved, resume ? "a+" : "w+"),
    outside: await open(pathByKind.outside, resume ? "a+" : "w+"),
  };
  /** @type {Record<OutputKind, number>} */
  const offsets = { ...durableOffsets };
  if (resume) {
    for (const kind of /** @type {OutputKind[]} */ (Object.keys(pathByKind))) {
      const fileStats = await handles[kind].stat();
      if (fileStats.size < offsets[kind]) {
        throw new Error(
          `Output ${pathByKind[kind]} is shorter than checkpoint offset ${offsets[kind]}`,
        );
      }
      await handles[kind].truncate(offsets[kind]);
    }
  }
  return {
    offsets,
    async append(kind, value) {
      const line = `${JSON.stringify(value)}\n`;
      const byteLength = Buffer.byteLength(line, "utf8");
      await handles[kind].write(line, offsets[kind], "utf8");
      offsets[kind] += byteLength;
    },
    async sync() {
      await Promise.all(
        Object.values(handles).map(async (handle) => handle.sync()),
      );
    },
    async close() {
      await Promise.all(
        Object.values(handles).map(async (handle) => handle.close()),
      );
    },
  };
}

/**
 * Build arithmetic reconciliation invariants from counters.
 *
 * @param {ReconciliationCounts} counts - Final or partial run counters.
 * @returns {{ sourceRowsBalanced: boolean, candidateRecordsBalanced: boolean, candidateAddressesBalanced: boolean, allBalanced: boolean }} Invariant results.
 */
function buildReconciliationInvariants(counts) {
  const sourceRowsBalanced =
    counts.sourceRecordsRead ===
    counts.invalidRecordCount +
      counts.validNonCandidateRecordCount +
      counts.candidateRecordCount;
  const candidateRecordsBalanced =
    counts.candidateRecordCount ===
    counts.emittedBrowardRecordCount +
      counts.outsideOnlyRecordCount +
      counts.unresolvedWithoutInsideRecordCount;
  const candidateAddressesBalanced =
    counts.candidateAddressMatchCount ===
    counts.verifiedInsideAddressMatchCount +
      counts.verifiedOutsideAddressMatchCount +
      counts.unresolvedAddressMatchCount;
  return {
    sourceRowsBalanced,
    candidateRecordsBalanced,
    candidateAddressesBalanced,
    allBalanced:
      sourceRowsBalanced &&
      candidateRecordsBalanced &&
      candidateAddressesBalanced,
  };
}

/**
 * Create one audit occurrence for an unresolved or outside candidate address.
 *
 * @param {object} params - Candidate occurrence fields.
 * @param {SunbizZipMatchedAddress} params.match - Shared-extractor ZIP address match.
 * @param {string} params.validationKey - Exact-address validation key.
 * @param {AddressValidationStatus} params.status - Fail-closed decision.
 * @param {string} params.reason - Machine-readable reason for the decision.
 * @param {AddressValidationEntry | null} params.validation - Applied validation entry when present.
 * @param {string} params.sourceFileName - Text file or ZIP entry provenance.
 * @param {number} params.sourceLineNumber - One-based source line provenance.
 * @param {string} params.documentNumber - Sunbiz document number.
 * @returns {Record<string, unknown>} JSONL audit occurrence.
 */
function buildCandidateOccurrence({
  match,
  validationKey,
  status,
  reason,
  validation,
  sourceFileName,
  sourceLineNumber,
  documentNumber,
}) {
  return {
    schemaVersion: CANDIDATE_SCHEMA_VERSION,
    validationKey,
    status,
    reason,
    countyFips: validation?.countyFips ?? null,
    evidence: validation?.evidence ?? null,
    sourceFileName,
    sourceLineNumber,
    documentNumber,
    role: match.role,
    officerOrdinal: match.officerOrdinal,
    matchedZipPrefix: match.matchedZipPrefix,
    address: match.address,
  };
}

/**
 * Build a durable checkpoint from current mutable state.
 *
 * @param {object} params - Current progress values.
 * @param {"running" | "paused" | "complete"} params.status - Durable run status.
 * @param {InputFingerprint} params.input - Current input identity.
 * @param {string} params.configurationSha256 - Resume-compatibility digest.
 * @param {CheckpointCursor} params.cursor - Last processed source cursor.
 * @param {ReconciliationCounts} params.counts - Current counters.
 * @param {Record<OutputKind, number>} params.outputOffsets - Current JSONL byte offsets.
 * @returns {BrowardSunbizCheckpoint} Serializable durable checkpoint.
 */
function buildCheckpoint({
  status,
  input,
  configurationSha256,
  cursor,
  counts,
  outputOffsets,
}) {
  return {
    schemaVersion: CHECKPOINT_SCHEMA_VERSION,
    status,
    input,
    configurationSha256,
    cursor: { ...cursor },
    counts: { ...counts },
    outputOffsets: { ...outputOffsets },
    updatedAt: new Date().toISOString(),
  };
}

/**
 * Process a local Sunbiz daily/quarterly text file or ZIP into fail-closed,
 * Broward-scoped records with resumable output and reconciliation.
 *
 * Candidate ZIP is only a screening signal. A record is emitted only when at
 * least one candidate address has an `inside` validation entry backed by FIPS
 * 12011 evidence. Missing/weak decisions are written as unresolved and never
 * silently treated as Broward.
 *
 * @param {PrepareBrowardSunbizOptions} options - Local preparation options.
 * @returns {Promise<PreparationSummary>} Durable partial or complete summary.
 */
export async function prepareBrowardSunbizLocal(options) {
  const inputPath = path.resolve(options.inputPath);
  const outputDir = path.resolve(options.outputDir);
  const requestedFormat = options.format ?? "auto";
  const format = resolveSourceFormat(inputPath, requestedFormat);
  const resume = options.resume ?? false;
  const emitLexiconBundles = options.emitLexiconBundles ?? true;
  const checkpointInterval =
    options.checkpointInterval ?? DEFAULT_CHECKPOINT_INTERVAL;
  if (!Number.isInteger(checkpointInterval) || checkpointInterval <= 0) {
    throw new Error("checkpointInterval must be a positive integer");
  }
  if (
    options.maxSourceRecords !== undefined &&
    options.maxSourceRecords !== null &&
    (!Number.isInteger(options.maxSourceRecords) ||
      options.maxSourceRecords <= 0)
  ) {
    throw new Error(
      "maxSourceRecords must be a positive integer when provided",
    );
  }
  const configuration = await loadBrowardSunbizConfiguration(
    options.sourceCatalogPath,
  );
  const validation = await loadAddressValidationManifest(
    options.validationManifestPath,
    configuration.countyFips,
  );
  const inputStats = await stat(inputPath);
  if (!inputStats.isFile()) {
    throw new Error(`Sunbiz input must be a local file: ${inputPath}`);
  }
  /** @type {InputFingerprint} */
  const input = {
    path: inputPath,
    size: inputStats.size,
    mtimeMs: inputStats.mtimeMs,
    format,
  };
  const configurationSha256 = sha256Text(
    [
      PREPARATION_SCHEMA_VERSION,
      configuration.sourceCatalogSha256,
      validation.sha256,
      emitLexiconBundles ? "lexicon:on" : "lexicon:off",
    ].join("\0"),
  );
  const outputPaths = buildOutputPaths(outputDir);
  await prepareOutputDirectory(outputDir, resume, outputPaths);

  /** @type {BrowardSunbizCheckpoint | null} */
  let priorCheckpoint = null;
  if (resume) {
    priorCheckpoint = parseCheckpoint(
      await readJsonObject(outputPaths.checkpoint),
      outputPaths.checkpoint,
    );
    if (!fingerprintsEqual(input, priorCheckpoint.input)) {
      throw new Error(
        "Input path, size, modification time, or format changed; refusing unsafe resume",
      );
    }
    if (priorCheckpoint.configurationSha256 !== configurationSha256) {
      throw new Error(
        "Broward source ZIP candidates, validation manifest, schema, or transform mode changed; use a fresh output directory",
      );
    }
    if (priorCheckpoint.status === "complete") {
      return /** @type {PreparationSummary} */ (
        await readJsonObject(outputPaths.reconciliation)
      );
    }
  }

  const counts = priorCheckpoint
    ? { ...priorCheckpoint.counts }
    : createEmptyCounts();
  /** @type {CheckpointCursor} */
  const cursor = priorCheckpoint
    ? { ...priorCheckpoint.cursor }
    : {
        inputSequence: 0,
        sourceFileName: null,
        sourceLineNumber: 0,
      };
  /** @type {Record<OutputKind, number>} */
  const durableOffsets = priorCheckpoint
    ? { ...priorCheckpoint.outputOffsets }
    : { records: 0, lexicon: 0, unresolved: 0, outside: 0 };
  const writer = await createOutputWriter(
    outputPaths,
    durableOffsets,
    priorCheckpoint !== null,
  );

  /**
   * Flush JSONL outputs and atomically update the resumable checkpoint.
   *
   * @param {"running" | "paused" | "complete"} status - Durable progress status.
   * @returns {Promise<void>} Resolves after outputs and checkpoint are durable.
   */
  async function saveCheckpoint(status) {
    await writer.sync();
    await writeJsonAtomic(
      outputPaths.checkpoint,
      buildCheckpoint({
        status,
        input,
        configurationSha256,
        cursor,
        counts,
        outputOffsets: writer.offsets,
      }),
    );
  }

  try {
    if (!priorCheckpoint) {
      await saveCheckpoint("running");
    }
    let inputSequence = 0;
    let processedThisInvocation = 0;
    let paused = false;
    for await (const source of iterateLocalSourceLines(inputPath, format)) {
      inputSequence += 1;
      if (inputSequence <= cursor.inputSequence) continue;
      if (
        options.maxSourceRecords !== undefined &&
        options.maxSourceRecords !== null &&
        processedThisInvocation >= options.maxSourceRecords
      ) {
        paused = true;
        break;
      }

      processedThisInvocation += 1;
      counts.sourceRecordsRead += 1;
      cursor.inputSequence = inputSequence;
      cursor.sourceFileName = source.sourceFileName;
      cursor.sourceLineNumber = source.sourceLineNumber;
      const entity = parseCorporateDataRecord(source.line);
      if (!entity) {
        counts.invalidRecordCount += 1;
      } else {
        const candidateMatches = findZipMatchedAddresses(
          entity,
          configuration.zipCandidates,
        );
        if (candidateMatches.length === 0) {
          counts.validNonCandidateRecordCount += 1;
        } else {
          counts.candidateRecordCount += 1;
          counts.candidateAddressMatchCount += candidateMatches.length;
          /** @type {SunbizZipMatchedAddress[]} */
          const insideMatches = [];
          let outsideMatchCount = 0;
          let unresolvedMatchCount = 0;

          for (const match of candidateMatches) {
            const validationKey = createAddressValidationKey(match.address);
            const decision = validation.entries.get(validationKey) ?? null;
            if (
              isAddressSpecificEnough(match.address) &&
              decision?.status === "inside" &&
              decision.countyFips === configuration.countyFips
            ) {
              insideMatches.push(match);
              counts.verifiedInsideAddressMatchCount += 1;
              continue;
            }
            if (
              isAddressSpecificEnough(match.address) &&
              decision?.status === "outside"
            ) {
              outsideMatchCount += 1;
              counts.verifiedOutsideAddressMatchCount += 1;
              await writer.append(
                "outside",
                buildCandidateOccurrence({
                  match,
                  validationKey,
                  status: "outside",
                  reason: "validated_outside_broward",
                  validation: decision,
                  sourceFileName: source.sourceFileName,
                  sourceLineNumber: source.sourceLineNumber,
                  documentNumber: entity.documentNumber,
                }),
              );
              continue;
            }
            unresolvedMatchCount += 1;
            counts.unresolvedAddressMatchCount += 1;
            const reason = !isAddressSpecificEnough(match.address)
              ? "insufficient_exact_address"
              : decision?.status === "unresolved"
                ? "validation_manifest_unresolved"
                : "missing_validation_manifest_entry";
            await writer.append(
              "unresolved",
              buildCandidateOccurrence({
                match,
                validationKey,
                status: "unresolved",
                reason,
                validation: decision,
                sourceFileName: source.sourceFileName,
                sourceLineNumber: source.sourceLineNumber,
                documentNumber: entity.documentNumber,
              }),
            );
          }

          if (insideMatches.length > 0) {
            counts.emittedBrowardRecordCount += 1;
            const extractedRecord = {
              sourceFileName: source.sourceFileName,
              sourceLineNumber: source.sourceLineNumber,
              entity,
              matchedAddresses: insideMatches,
              countyScope: {
                county: configuration.county,
                state: configuration.state,
                fips: configuration.countyFips,
                method: "exact-address-validation-manifest",
                validationKeys: [
                  ...new Set(
                    insideMatches.map((match) =>
                      createAddressValidationKey(match.address),
                    ),
                  ),
                ],
              },
            };
            await writer.append("records", extractedRecord);
            if (emitLexiconBundles) {
              const bundle = transformSunbizRecord(extractedRecord, {
                sourceDataUri: pathToFileURL(inputPath).href,
              });
              await writer.append("lexicon", {
                schemaVersion: PREPARATION_SCHEMA_VERSION,
                countyScope: extractedRecord.countyScope,
                sourceFileName: source.sourceFileName,
                sourceLineNumber: source.sourceLineNumber,
                documentNumber: entity.documentNumber,
                bundle,
              });
              counts.lexiconBundleCount += 1;
            }
          } else if (unresolvedMatchCount > 0) {
            counts.unresolvedWithoutInsideRecordCount += 1;
          } else if (outsideMatchCount > 0) {
            counts.outsideOnlyRecordCount += 1;
          }
        }
      }

      if (processedThisInvocation % checkpointInterval === 0) {
        await saveCheckpoint("running");
      }
    }

    const status = paused ? "paused" : "complete";
    const invariants = buildReconciliationInvariants(counts);
    /** @type {PreparationSummary} */
    const summary = {
      schemaVersion: PREPARATION_SCHEMA_VERSION,
      status,
      completedAt: new Date().toISOString(),
      county: {
        name: configuration.county,
        state: configuration.state,
        fips: configuration.countyFips,
      },
      input,
      sourceCatalogPath: configuration.sourceCatalogPath,
      validationManifestPath: validation.absolutePath,
      exactZipCandidates: configuration.zipCandidates,
      counts: { ...counts },
      reconciliation: invariants,
      outputs: {
        records: outputPaths.records,
        lexiconBundles: emitLexiconBundles ? outputPaths.lexicon : null,
        unresolvedCandidates: outputPaths.unresolved,
        outsideCandidates: outputPaths.outside,
        checkpoint: outputPaths.checkpoint,
      },
    };
    await writer.sync();
    await writeJsonAtomic(outputPaths.reconciliation, summary);
    await saveCheckpoint(status);
    return summary;
  } finally {
    await writer.close();
  }
}

/**
 * Read a required non-empty string from parseArgs values.
 *
 * @param {Record<string, string | boolean | undefined>} values - Parsed CLI values.
 * @param {string} name - Required option name without leading dashes.
 * @returns {string} Non-empty option value.
 */
function requireStringOption(values, name) {
  const value = values[name];
  if (typeof value !== "string" || !value.trim()) {
    throw new Error(`--${name} is required`);
  }
  return value;
}

/**
 * Parse an optional positive integer CLI value.
 *
 * @param {string | boolean | undefined} value - Raw CLI option value.
 * @param {string} name - Option name used in errors.
 * @returns {number | null} Positive integer or null when absent.
 */
function parseOptionalPositiveInteger(value, name) {
  if (value === undefined) return null;
  if (typeof value !== "string" || !/^\d+$/.test(value)) {
    throw new Error(`--${name} must be a positive integer`);
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`--${name} must be a positive integer`);
  }
  return parsed;
}

/**
 * Parse and validate a source-format CLI value.
 *
 * @param {string | boolean | undefined} value - Raw `--format` value.
 * @returns {RequestedSourceFormat} Validated format.
 */
function parseRequestedFormat(value) {
  if (value === undefined) return "auto";
  if (value === "auto" || value === "text" || value === "zip") return value;
  throw new Error("--format must be auto, text, or zip");
}

/**
 * Local-only Broward Sunbiz CLI entrypoint.
 *
 * @returns {Promise<void>} Resolves after printing the durable reconciliation summary.
 */
export async function main() {
  const { values } = parseArgs({
    options: {
      input: { type: "string" },
      "output-dir": { type: "string" },
      sources: { type: "string" },
      "validation-manifest": { type: "string" },
      format: { type: "string" },
      resume: { type: "boolean", default: false },
      "checkpoint-interval": { type: "string" },
      "max-source-records": { type: "string" },
      "skip-lexicon": { type: "boolean", default: false },
    },
    strict: true,
    allowPositionals: false,
  });
  const checkpointInterval =
    parseOptionalPositiveInteger(
      values["checkpoint-interval"],
      "checkpoint-interval",
    ) ?? DEFAULT_CHECKPOINT_INTERVAL;
  const maxSourceRecords = parseOptionalPositiveInteger(
    values["max-source-records"],
    "max-source-records",
  );
  const summary = await prepareBrowardSunbizLocal({
    inputPath: requireStringOption(values, "input"),
    outputDir: requireStringOption(values, "output-dir"),
    sourceCatalogPath:
      typeof values.sources === "string" ? values.sources : undefined,
    validationManifestPath:
      typeof values["validation-manifest"] === "string"
        ? values["validation-manifest"]
        : undefined,
    format: parseRequestedFormat(values.format),
    resume: values.resume === true,
    checkpointInterval,
    maxSourceRecords,
    emitLexiconBundles: values["skip-lexicon"] !== true,
  });
  console.log(JSON.stringify(summary, null, 2));
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
