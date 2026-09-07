import { createHash } from "node:crypto";
import { once } from "node:events";
import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import * as path from "node:path";
import { createInterface } from "node:readline";
import { pathToFileURL } from "node:url";
import { parseArgs } from "node:util";

/**
 * @typedef {Record<string, unknown>} JsonObject
 */

/**
 * Query-DB normalized city-permit JSONL contract.
 *
 * @typedef {object} NormalizedPolkPermit
 * @property {string} source_system Portal-specific source identity.
 * @property {string} source_url Stable source record URL.
 * @property {string | undefined} [city] Issuing city when explicitly known.
 * @property {string} permit_number Published permit or application number.
 * @property {null} parcel_identifier No parcel match was published by these sources.
 * @property {string | null} work_location Published location text.
 * @property {string | null} permit_issue_date Source-evidenced issue date only.
 * @property {string | null} record_status Published status.
 * @property {string | null} record_type Published permit type.
 * @property {string | null} project_description Published description.
 * @property {JsonObject} raw Complete immutable harvest record.
 */

/**
 * @typedef {object} PolkPermitNormalizationOptions
 * @property {string} accelaInput Verified Accela list JSONL.
 * @property {string} accelaReceipt Accela harvest receipt.
 * @property {string} lakelandInput Verified Lakeland ArcGIS JSONL.
 * @property {string} lakelandReceipt Lakeland harvest receipt.
 * @property {string} outputDirectory Load-ready output directory.
 * @property {string} manifest Normalization manifest.
 * @property {number | null} limit Optional loadable-record pilot cap per source.
 * @property {number} recordsPerPart Maximum records in each loader part.
 */

/**
 * @typedef {object} NormalizationResult
 * @property {NormalizedPolkPermit | null} record Load-ready record, if applicable.
 * @property {string | null} excludedReason Auditable exclusion category.
 */

/**
 * @typedef {object} VerifiedHarvest
 * @property {string} input Absolute source JSONL path.
 * @property {string} receipt Absolute source receipt path.
 * @property {number} inputBytes Verified source bytes.
 * @property {string} inputSha256 Verified source digest.
 * @property {number} expectedRecordCount Verified source record count.
 * @property {number} expectedLoadableRecordCount Expected permit rows.
 */

/**
 * @typedef {object} NormalizedSourceSummary
 * @property {string} source Source name.
 * @property {string} input Absolute source JSONL path.
 * @property {string} receipt Absolute source receipt path.
 * @property {number} inputBytes Verified source bytes.
 * @property {string} inputSha256 Verified source digest.
 * @property {number} observedRecordCount Parsed source records.
 * @property {number} loadReadyRecordCount Emitted permit records.
 * @property {Record<string, number>} excludedRecordCounts Auditable exclusions.
 * @property {string} output Absolute normalized JSONL path.
 * @property {number} outputBytes Normalized output bytes.
 * @property {string} outputSha256 Normalized output digest.
 * @property {string} partDirectory Loader part directory.
 * @property {number} partCount Number of loader parts.
 * @property {NormalizedPartSummary[]} parts Ordered loader part receipts.
 */

/**
 * @typedef {object} NormalizedPartSummary
 * @property {number} partIndex Zero-based part index.
 * @property {number} recordCount Records in the part.
 * @property {number} bytes Part bytes.
 * @property {string} sha256 Part digest.
 * @property {string} path Absolute content-addressed part path.
 */

const NORMALIZATION_SCHEMA = "oracle-node.polk-permit-source-normalization.v1";
const ACCELA_RECORD_SCHEMA = "oracle-node.polk-county-accela-list-record.v1";
const ACCELA_RECEIPT_SCHEMA = "oracle-node.polk-county-accela-list-harvest.v1";
const LAKELAND_RECORD_SCHEMA = "oracle-node.polk-lakeland-arcgis-permit.v1";
const LAKELAND_RECEIPT_SCHEMA = "oracle-node.polk-lakeland-arcgis-harvest.v1";

/**
 * Parse the load-ready normalization CLI.
 *
 * @param {readonly string[]} argv Arguments excluding node and script.
 * @returns {PolkPermitNormalizationOptions} Validated options.
 */
export function parsePolkPermitNormalizationOptions(argv) {
  const { values } = parseArgs({
    args: [...argv],
    options: {
      "accela-input": {
        type: "string",
        default: "tmp/polk/permits/polk-county-accela-list.jsonl",
      },
      "accela-receipt": {
        type: "string",
        default: "tmp/polk/permits/polk-county-accela-list-receipt.json",
      },
      "lakeland-input": {
        type: "string",
        default: "tmp/polk/permits/lakeland-arcgis-permits.jsonl",
      },
      "lakeland-receipt": {
        type: "string",
        default: "tmp/polk/permits/lakeland-arcgis-permits-receipt.json",
      },
      "output-dir": {
        type: "string",
        default: "tmp/polk/permits/load-ready",
      },
      manifest: { type: "string" },
      limit: { type: "string" },
      "records-per-part": { type: "string" },
    },
    strict: true,
  });
  const limit =
    values.limit === undefined
      ? null
      : readPositiveInteger(values.limit, "limit");
  return {
    accelaInput: values["accela-input"],
    accelaReceipt: values["accela-receipt"],
    lakelandInput: values["lakeland-input"],
    lakelandReceipt: values["lakeland-receipt"],
    outputDirectory: values["output-dir"],
    manifest:
      values.manifest ??
      path.join(values["output-dir"], "normalization-manifest.json"),
    limit,
    recordsPerPart:
      values["records-per-part"] === undefined
        ? 25_000
        : readPositiveInteger(values["records-per-part"], "records-per-part"),
  };
}

/**
 * Normalize one verified Polk County Accela list record.
 *
 * Contractor-licensing rows remain in the immutable harvest but are excluded
 * from `property_improvements`; they are not permits.
 *
 * @param {unknown} value Candidate source record.
 * @returns {NormalizationResult} Load-ready permit or explicit exclusion.
 */
export function normalizePolkAccelaPermit(value) {
  const record = requireObject(value, "Polk Accela record");
  if (record.schemaVersion !== ACCELA_RECORD_SCHEMA) {
    throw new Error("Polk Accela record schema changed");
  }
  if (record.sourceSystem !== "polk_county_accela_csv") {
    throw new Error("Polk Accela record source system changed");
  }
  const permitNumber = requireString(
    record.permitNumber,
    "Polk Accela permit number",
  );
  const recordClass = requireString(
    record.recordClass,
    "Polk Accela record class",
  );
  if (recordClass === "license") {
    return { record: null, excludedReason: "contractor_license" };
  }
  if (recordClass !== "permit") {
    throw new Error(
      `Unsupported Polk Accela record class ${JSON.stringify(recordClass)}`,
    );
  }
  return {
    record: compactObject({
      source_system: "polk_county_accela_csv",
      source_url: requireString(record.sourceUrl, "Polk Accela source URL"),
      permit_number: permitNumber,
      parcel_identifier: null,
      work_location: readString(record.address),
      permit_issue_date: null,
      record_status: readString(record.status),
      record_type: readString(record.recordType),
      project_description: readString(record.description),
      raw: record,
    }),
    excludedReason: null,
  };
}

/**
 * Normalize one verified Lakeland ArcGIS permit record.
 *
 * `issuedAt` is the only field mapped to `permit_issue_date`; `appliedAt` and
 * `approvedAt` remain in `raw` because they do not prove issuance.
 *
 * @param {unknown} value Candidate source record.
 * @returns {NormalizationResult} Load-ready Lakeland permit.
 */
export function normalizeLakelandArcgisPermit(value) {
  const record = requireObject(value, "Lakeland ArcGIS permit record");
  if (record.schemaVersion !== LAKELAND_RECORD_SCHEMA) {
    throw new Error("Lakeland ArcGIS permit record schema changed");
  }
  if (record.sourceSystem !== "lakeland_arcgis_permit_layer") {
    throw new Error("Lakeland ArcGIS permit source system changed");
  }
  return {
    record: compactObject({
      source_system: "lakeland_arcgis_permit_layer",
      source_url: requireString(record.sourceUrl, "Lakeland ArcGIS source URL"),
      city: "Lakeland",
      permit_number: requireString(
        record.permitNumber,
        "Lakeland ArcGIS permit number",
      ),
      parcel_identifier: null,
      work_location: buildLakelandWorkLocation(record.siteAddress),
      permit_issue_date: readIsoDate(record.issuedAt),
      record_status: null,
      record_type: readString(record.permitType),
      project_description: readString(record.description),
      raw: record,
    }),
    excludedReason: null,
  };
}

/**
 * Flatten the published Lakeland site-address object without inferring missing
 * components.
 *
 * @param {unknown} value Candidate site-address object.
 * @returns {string | null} Source-evidenced work-location text.
 */
export function buildLakelandWorkLocation(value) {
  if (value === null || value === undefined) return null;
  const address = requireObject(value, "Lakeland site address");
  const statePostal = [
    readString(address.stateCode),
    readString(address.postalCode),
  ]
    .filter((part) => part !== null)
    .join(" ");
  const parts = [
    readString(address.line1),
    readString(address.city),
    statePostal.length === 0 ? null : statePostal,
  ].filter((part) => part !== null);
  return parts.length === 0 ? null : parts.join(", ");
}

/**
 * Verify both harvests and write load-ready JSONL plus a reconciliation
 * manifest. Existing destination files are replaced atomically.
 *
 * @param {PolkPermitNormalizationOptions} options Runtime options.
 * @returns {Promise<JsonObject>} Persisted normalization manifest.
 */
export async function normalizePolkPermitSources(options) {
  const absoluteOutputDirectory = path.resolve(options.outputDirectory);
  const absoluteManifest = path.resolve(options.manifest);
  const [accelaHarvest, lakelandHarvest] = await Promise.all([
    verifyAccelaHarvest(options),
    verifyLakelandHarvest(options),
  ]);
  await mkdir(absoluteOutputDirectory, { recursive: true });
  const accelaOutput = path.join(
    absoluteOutputDirectory,
    "polk-county-accela-permits.normalized.jsonl",
  );
  const lakelandOutput = path.join(
    absoluteOutputDirectory,
    "lakeland-arcgis-permits.normalized.jsonl",
  );
  const accelaNormalized = await normalizeSourceFile({
    source: "polk_county_accela_csv",
    harvest: accelaHarvest,
    output: accelaOutput,
    limit: options.limit,
    normalize: normalizePolkAccelaPermit,
  });
  const lakelandNormalized = await normalizeSourceFile({
    source: "lakeland_arcgis_permit_layer",
    harvest: lakelandHarvest,
    output: lakelandOutput,
    limit: options.limit,
    normalize: normalizeLakelandArcgisPermit,
  });
  const [accelaParts, lakelandParts] = await Promise.all([
    splitNormalizedSourceFile({
      input: accelaNormalized.output,
      expectedRecordCount: accelaNormalized.loadReadyRecordCount,
      partDirectory: path.join(absoluteOutputDirectory, "accela-parts"),
      recordsPerPart: options.recordsPerPart,
    }),
    splitNormalizedSourceFile({
      input: lakelandNormalized.output,
      expectedRecordCount: lakelandNormalized.loadReadyRecordCount,
      partDirectory: path.join(absoluteOutputDirectory, "lakeland-parts"),
      recordsPerPart: options.recordsPerPart,
    }),
  ]);
  const accela = { ...accelaNormalized, ...accelaParts };
  const lakeland = { ...lakelandNormalized, ...lakelandParts };
  const manifest = {
    schemaVersion: NORMALIZATION_SCHEMA,
    generatedAt: new Date().toISOString(),
    county: "polk",
    stateCode: "FL",
    queryDbPermitSourceSystem: "polk_permits",
    recordsPerPart: options.recordsPerPart,
    sources: [accela, lakeland],
    sourceRecordCount:
      accela.observedRecordCount + lakeland.observedRecordCount,
    loadReadyPermitCount:
      accela.loadReadyRecordCount + lakeland.loadReadyRecordCount,
    excludedRecordCounts: sumExclusionCounts([
      accela.excludedRecordCounts,
      lakeland.excludedRecordCounts,
    ]),
    unmatchedPermitCount:
      accela.loadReadyRecordCount + lakeland.loadReadyRecordCount,
    matchPolicy:
      "No parcel or property relation is inferred; every load-ready permit remains unmatched.",
    pilot: options.limit !== null,
    complete: options.limit === null,
  };
  await writeAtomicText(
    absoluteManifest,
    `${JSON.stringify(manifest, null, 2)}\n`,
  );
  return manifest;
}

/**
 * Verify the Accela input against its complete harvest receipt.
 *
 * @param {PolkPermitNormalizationOptions} options Runtime options.
 * @returns {Promise<VerifiedHarvest>} Verified source metadata.
 */
async function verifyAccelaHarvest(options) {
  const input = path.resolve(options.accelaInput);
  const receiptPath = path.resolve(options.accelaReceipt);
  const receipt = await readJsonObject(receiptPath);
  if (
    receipt.schemaVersion !== ACCELA_RECEIPT_SCHEMA ||
    receipt.complete !== true
  ) {
    throw new Error("Polk Accela harvest receipt is not complete");
  }
  const classCounts = requireObject(
    receipt.classCounts,
    "Polk Accela class counts",
  );
  const expectedRecordCount = requireSafeInteger(
    receipt.accessibleRecordCount,
    "Polk Accela accessible record count",
  );
  const expectedLoadableRecordCount = requireSafeInteger(
    classCounts.permit,
    "Polk Accela permit record count",
  );
  const expectedLicenseCount = requireSafeInteger(
    classCounts.license,
    "Polk Accela license record count",
  );
  if (
    expectedLoadableRecordCount + expectedLicenseCount !==
    expectedRecordCount
  ) {
    throw new Error("Polk Accela class counts do not reconcile");
  }
  return verifyHarvestFile({
    input,
    receipt: receiptPath,
    expectedBytes: requireSafeInteger(
      receipt.outputBytes,
      "Polk Accela output bytes",
    ),
    expectedSha256: requireSha256(
      receipt.outputSha256,
      "Polk Accela output digest",
    ),
    expectedRecordCount,
    expectedLoadableRecordCount,
  });
}

/**
 * Verify the Lakeland input against its complete harvest receipt.
 *
 * @param {PolkPermitNormalizationOptions} options Runtime options.
 * @returns {Promise<VerifiedHarvest>} Verified source metadata.
 */
async function verifyLakelandHarvest(options) {
  const input = path.resolve(options.lakelandInput);
  const receiptPath = path.resolve(options.lakelandReceipt);
  const receipt = await readJsonObject(receiptPath);
  if (
    receipt.schemaVersion !== LAKELAND_RECEIPT_SCHEMA ||
    receipt.complete !== true
  ) {
    throw new Error("Lakeland ArcGIS harvest receipt is not complete");
  }
  const expectedRecordCount = requireSafeInteger(
    receipt.harvestedRecordCount,
    "Lakeland harvested record count",
  );
  return verifyHarvestFile({
    input,
    receipt: receiptPath,
    expectedBytes: requireSafeInteger(
      receipt.outputBytes,
      "Lakeland output bytes",
    ),
    expectedSha256: requireSha256(
      receipt.outputSha256,
      "Lakeland output digest",
    ),
    expectedRecordCount,
    expectedLoadableRecordCount: expectedRecordCount,
  });
}

/**
 * Verify source bytes and digest before normalization.
 *
 * @param {{input:string,receipt:string,expectedBytes:number,expectedSha256:string,expectedRecordCount:number,expectedLoadableRecordCount:number}} params Expected source contract.
 * @returns {Promise<VerifiedHarvest>} Verified source metadata.
 */
async function verifyHarvestFile(params) {
  const sourceStat = await stat(params.input);
  if (sourceStat.size !== params.expectedBytes) {
    throw new Error(
      `Harvest byte count changed for ${params.input}: expected ${params.expectedBytes}, received ${sourceStat.size}`,
    );
  }
  const inputSha256 = await hashFile(params.input);
  if (inputSha256 !== params.expectedSha256) {
    throw new Error(`Harvest digest changed for ${params.input}`);
  }
  return {
    input: params.input,
    receipt: params.receipt,
    inputBytes: sourceStat.size,
    inputSha256,
    expectedRecordCount: params.expectedRecordCount,
    expectedLoadableRecordCount: params.expectedLoadableRecordCount,
  };
}

/**
 * Stream and normalize one verified source.
 *
 * @param {{source:string,harvest:VerifiedHarvest,output:string,limit:number | null,normalize:(value:unknown)=>NormalizationResult}} params Source contract.
 * @returns {Promise<NormalizedSourceSummary>} Output reconciliation.
 */
async function normalizeSourceFile(params) {
  if (path.resolve(params.output) === params.harvest.input) {
    throw new Error("Normalized output must not replace its harvest input");
  }
  const temporary = `${params.output}.tmp-${process.pid}-${Date.now()}`;
  const input = createReadStream(params.harvest.input, { encoding: "utf8" });
  const lines = createInterface({ input, crlfDelay: Infinity });
  const output = createWriteStream(temporary, {
    encoding: "utf8",
    flags: "wx",
  });
  const outputHash = createHash("sha256");
  let observedRecordCount = 0;
  let loadReadyRecordCount = 0;
  /** @type {Record<string, number>} */
  const excludedRecordCounts = {};
  try {
    for await (const line of lines) {
      if (line.trim().length === 0) {
        throw new Error(
          `${params.source} source contains an empty JSONL record`,
        );
      }
      observedRecordCount += 1;
      /** @type {unknown} */
      let parsed;
      try {
        parsed = JSON.parse(line);
      } catch (caught) {
        const message =
          caught instanceof Error ? caught.message : String(caught);
        throw new Error(
          `${params.source} record ${observedRecordCount} is invalid JSON: ${message}`,
        );
      }
      const normalized = params.normalize(parsed);
      if (normalized.excludedReason !== null) {
        excludedRecordCounts[normalized.excludedReason] =
          (excludedRecordCounts[normalized.excludedReason] ?? 0) + 1;
      }
      if (
        normalized.record === null ||
        (params.limit !== null && loadReadyRecordCount >= params.limit)
      ) {
        continue;
      }
      const text = `${JSON.stringify(normalized.record)}\n`;
      outputHash.update(text);
      if (!output.write(text)) await once(output, "drain");
      loadReadyRecordCount += 1;
    }
    output.end();
    await once(output, "finish");
    if (observedRecordCount !== params.harvest.expectedRecordCount) {
      throw new Error(
        `${params.source} source count changed: expected ${params.harvest.expectedRecordCount}, received ${observedRecordCount}`,
      );
    }
    const expectedOutputCount =
      params.limit === null
        ? params.harvest.expectedLoadableRecordCount
        : Math.min(params.limit, params.harvest.expectedLoadableRecordCount);
    if (loadReadyRecordCount !== expectedOutputCount) {
      throw new Error(
        `${params.source} load-ready count changed: expected ${expectedOutputCount}, received ${loadReadyRecordCount}`,
      );
    }
    await rename(temporary, params.output);
  } catch (caught) {
    lines.close();
    input.destroy();
    output.destroy();
    await rm(temporary, { force: true });
    throw caught;
  }
  const outputStat = await stat(params.output);
  return {
    source: params.source,
    input: params.harvest.input,
    receipt: params.harvest.receipt,
    inputBytes: params.harvest.inputBytes,
    inputSha256: params.harvest.inputSha256,
    observedRecordCount,
    loadReadyRecordCount,
    excludedRecordCounts,
    output: params.output,
    outputBytes: outputStat.size,
    outputSha256: outputHash.digest("hex"),
  };
}

/**
 * Split one verified normalized aggregate into bounded, content-addressed
 * loader parts so the query-DB loader never parses a multi-hundred-megabyte
 * JSONL object in one allocation.
 *
 * @param {{input:string,expectedRecordCount:number,partDirectory:string,recordsPerPart:number}} params Split contract.
 * @returns {Promise<{partDirectory:string,partCount:number,parts:NormalizedPartSummary[]}>} Part receipts.
 */
async function splitNormalizedSourceFile(params) {
  const temporaryDirectory = `${params.partDirectory}.tmp-${process.pid}-${Date.now()}`;
  await mkdir(temporaryDirectory, { recursive: true });
  const input = createReadStream(params.input, { encoding: "utf8" });
  const lines = createInterface({ input, crlfDelay: Infinity });
  /** @type {NormalizedPartSummary[]} */
  const parts = [];
  let observedRecordCount = 0;
  let partIndex = 0;
  /** @type {{path:string,stream:import("node:fs").WriteStream,hash:import("node:crypto").Hash,recordCount:number,bytes:number} | null} */
  let activePart = null;

  /**
   * Open the next temporary loader part.
   *
   * @returns {{path:string,stream:import("node:fs").WriteStream,hash:import("node:crypto").Hash,recordCount:number,bytes:number}} Open part.
   */
  const openPart = () => {
    const temporaryPath = path.join(
      temporaryDirectory,
      `part-${String(partIndex).padStart(6, "0")}.tmp`,
    );
    return {
      path: temporaryPath,
      stream: createWriteStream(temporaryPath, {
        encoding: "utf8",
        flags: "wx",
      }),
      hash: createHash("sha256"),
      recordCount: 0,
      bytes: 0,
    };
  };

  /**
   * Commit the active part under its content-addressed filename.
   *
   * @returns {Promise<void>} Resolves after the part rename.
   */
  const commitPart = async () => {
    if (activePart === null) return;
    activePart.stream.end();
    await once(activePart.stream, "finish");
    const digest = activePart.hash.digest("hex");
    const finalPath = path.join(
      temporaryDirectory,
      `part-${String(partIndex).padStart(6, "0")}.${digest}.jsonl`,
    );
    await rename(activePart.path, finalPath);
    parts.push({
      partIndex,
      recordCount: activePart.recordCount,
      bytes: activePart.bytes,
      sha256: digest,
      path: finalPath,
    });
    activePart = null;
    partIndex += 1;
  };

  try {
    for await (const line of lines) {
      if (line.trim().length === 0) {
        throw new Error(
          `Normalized source ${params.input} contains an empty line`,
        );
      }
      if (activePart === null) activePart = openPart();
      const text = `${line}\n`;
      const bytes = Buffer.byteLength(text);
      activePart.hash.update(text);
      activePart.bytes += bytes;
      activePart.recordCount += 1;
      observedRecordCount += 1;
      if (!activePart.stream.write(text)) {
        await once(activePart.stream, "drain");
      }
      if (activePart.recordCount === params.recordsPerPart) {
        await commitPart();
      }
    }
    await commitPart();
    if (observedRecordCount !== params.expectedRecordCount) {
      throw new Error(
        `Normalized part count changed for ${params.input}: expected ${params.expectedRecordCount}, received ${observedRecordCount}`,
      );
    }
    await rm(params.partDirectory, { recursive: true, force: true });
    await rename(temporaryDirectory, params.partDirectory);
  } catch (caught) {
    lines.close();
    input.destroy();
    activePart?.stream.destroy();
    await rm(temporaryDirectory, { recursive: true, force: true });
    throw caught;
  }
  return {
    partDirectory: params.partDirectory,
    partCount: parts.length,
    parts: parts.map((part) => ({
      ...part,
      path: path.join(params.partDirectory, path.basename(part.path)),
    })),
  };
}

/**
 * Read a JSON object from disk.
 *
 * @param {string} filePath JSON file path.
 * @returns {Promise<JsonObject>} Parsed object.
 */
async function readJsonObject(filePath) {
  const parsed = JSON.parse(await readFile(filePath, "utf8"));
  return requireObject(parsed, `JSON document ${filePath}`);
}

/**
 * Hash a file without loading it into memory.
 *
 * @param {string} filePath File path.
 * @returns {Promise<string>} Lowercase SHA-256 digest.
 */
async function hashFile(filePath) {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(filePath)) hash.update(chunk);
  return hash.digest("hex");
}

/**
 * Atomically replace a UTF-8 text file.
 *
 * @param {string} destination Destination path.
 * @param {string} text File contents.
 * @returns {Promise<void>} Resolves after rename.
 */
async function writeAtomicText(destination, text) {
  await mkdir(path.dirname(destination), { recursive: true });
  const temporary = `${destination}.tmp-${process.pid}-${Date.now()}`;
  try {
    await writeFile(temporary, text, { encoding: "utf8", flag: "wx" });
    await rename(temporary, destination);
  } catch (caught) {
    await rm(temporary, { force: true });
    throw caught;
  }
}

/**
 * Require a non-array JSON object.
 *
 * @param {unknown} value Candidate value.
 * @param {string} label Error label.
 * @returns {JsonObject} Validated object.
 */
function requireObject(value, label) {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${label} must be a JSON object`);
  }
  return /** @type {JsonObject} */ (value);
}

/**
 * Read optional non-empty source text.
 *
 * @param {unknown} value Candidate value.
 * @returns {string | null} Trimmed text or null.
 */
function readString(value) {
  if (typeof value !== "string") return null;
  const text = value.trim();
  return text.length === 0 ? null : text;
}

/**
 * Require non-empty source text.
 *
 * @param {unknown} value Candidate value.
 * @param {string} label Error label.
 * @returns {string} Validated text.
 */
function requireString(value, label) {
  const text = readString(value);
  if (text === null) throw new Error(`${label} is required`);
  return text;
}

/**
 * Read an ISO timestamp/date as a calendar date.
 *
 * @param {unknown} value Candidate value.
 * @returns {string | null} YYYY-MM-DD or null.
 */
function readIsoDate(value) {
  const text = readString(value);
  if (text === null) return null;
  const match = /^(\d{4}-\d{2}-\d{2})(?:T.*)?$/.exec(text);
  if (match === null || match[1] === undefined) {
    throw new Error(
      `Expected an ISO source date, received ${JSON.stringify(text)}`,
    );
  }
  const date = new Date(`${match[1]}T00:00:00.000Z`);
  if (
    Number.isNaN(date.getTime()) ||
    date.toISOString().slice(0, 10) !== match[1]
  ) {
    throw new Error(`Invalid ISO source date ${JSON.stringify(text)}`);
  }
  return match[1];
}

/**
 * Require a non-negative safe integer.
 *
 * @param {unknown} value Candidate count.
 * @param {string} label Error label.
 * @returns {number} Validated count.
 */
function requireSafeInteger(value, label) {
  if (!Number.isSafeInteger(value) || Number(value) < 0) {
    throw new Error(`${label} must be a non-negative safe integer`);
  }
  return Number(value);
}

/**
 * Require a lowercase SHA-256 digest.
 *
 * @param {unknown} value Candidate digest.
 * @param {string} label Error label.
 * @returns {string} Validated digest.
 */
function requireSha256(value, label) {
  if (typeof value !== "string" || !/^[a-f0-9]{64}$/.test(value)) {
    throw new Error(`${label} must be a lowercase SHA-256 digest`);
  }
  return value;
}

/**
 * Parse a positive integer CLI option.
 *
 * @param {string} value Raw CLI value.
 * @param {string} name Option name.
 * @returns {number} Validated integer.
 */
function readPositiveInteger(value, name) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isSafeInteger(parsed) || parsed < 1 || String(parsed) !== value) {
    throw new Error(`--${name} must be a positive integer`);
  }
  return parsed;
}

/**
 * Remove undefined properties while preserving explicit nulls.
 *
 * @template {Record<string, unknown>} Value
 * @param {Value} value Source object.
 * @returns {Value} Compact object.
 */
function compactObject(value) {
  return /** @type {Value} */ (
    Object.fromEntries(
      Object.entries(value).filter(([, entry]) => entry !== undefined),
    )
  );
}

/**
 * Add exclusion counts from independent source summaries.
 *
 * @param {readonly Record<string, number>[]} counts Source count maps.
 * @returns {Record<string, number>} Combined counts.
 */
function sumExclusionCounts(counts) {
  /** @type {Record<string, number>} */
  const total = {};
  for (const source of counts) {
    for (const [key, value] of Object.entries(source)) {
      total[key] = (total[key] ?? 0) + value;
    }
  }
  return total;
}

if (
  process.argv[1] !== undefined &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  normalizePolkPermitSources(
    parsePolkPermitNormalizationOptions(process.argv.slice(2)),
  )
    .then((manifest) => {
      process.stdout.write(`${JSON.stringify(manifest, null, 2)}\n`);
    })
    .catch((caught) => {
      const message = caught instanceof Error ? caught.message : String(caught);
      process.stderr.write(
        `${JSON.stringify({ event: "polk_permit_normalization_failed", error: message })}\n`,
      );
      process.exitCode = 1;
    });
}
