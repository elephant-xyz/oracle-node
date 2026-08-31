#!/usr/bin/env node

import { createHash } from "node:crypto";
import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, readFile, readdir, stat, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import * as path from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

import { transformSunbizRecord } from "../transform-sunbiz-corporate-to-lexicon.mjs";
import {
  extractCorporateDataLinesByZip,
  normalizeZipPrefixes,
} from "../../workflow/lambdas/permit-harvest-worker/sunbiz-corporate.mjs";
import { isJsonObject, sha256File } from "../polk-local-parity-lib.mjs";

const require = createRequire(import.meta.url);
const duckdb = require("duckdb");

/**
 * @typedef {Record<string, unknown>} JsonObject
 */

/**
 * @typedef {object} PolkSiteAddress
 * @property {string} parcelId Polk parcel identifier.
 * @property {string} street Structured street line.
 * @property {string} city Situs city.
 * @property {string} zip Five-digit situs ZIP.
 */

/**
 * @typedef {object} PolkSunbizAddressCandidate
 * @property {string} documentNumber Sunbiz document number.
 * @property {"PRINCIPAL" | "MAILING" | "REGISTERED_AGENT" | "OFFICER"} role Address role.
 * @property {number | null} officerOrdinal Officer slot when applicable.
 * @property {string} street Street address.
 * @property {string} city City.
 * @property {string} state State code.
 * @property {string} zip ZIP code.
 */

/**
 * @typedef {object} PolkSunbizFilterOptions
 * @property {string[]} sources Local fixed-width cordata text paths.
 * @property {string} workDatabase Completed Polk DuckDB cache.
 * @property {string} outputDirectory Filter output root.
 * @property {number} chunkRecordLimit Maximum records per JSONL chunk.
 * @property {number | null} maxRecords Optional smoke cap.
 * @property {string} jobId Stable local job id.
 */

/**
 * @typedef {object} PolkSunbizTransformOptions
 * @property {string} inputDirectory Filter output root.
 * @property {string} workDatabase Completed Polk DuckDB cache.
 * @property {string} outputDirectory Transform and match output root.
 * @property {number | null} maxRecords Optional smoke cap.
 */

/**
 * Execute a read-only query against DuckDB.
 *
 * @param {import("duckdb").Connection} connection Open connection.
 * @param {string} sql Read-only SQL.
 * @returns {Promise<JsonObject[]>} Object rows.
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
 * Close a DuckDB connection.
 *
 * @param {import("duckdb").Connection} connection Open connection.
 * @returns {Promise<void>} Resolves after close.
 */
function closeDuckDbConnection(connection) {
  return new Promise((resolve) => connection.close(() => resolve()));
}

/**
 * Run a callback with a DuckDB connection and guaranteed cleanup.
 *
 * @template Result
 * @param {string} databasePath Database path.
 * @param {(connection: import("duckdb").Connection) => Promise<Result>} callback Read-only callback.
 * @returns {Promise<Result>} Callback result.
 */
async function withDuckDb(databasePath, callback) {
  const database = new duckdb.Database(databasePath);
  const connection = database.connect();
  try {
    return await callback(connection);
  } finally {
    await closeDuckDbConnection(connection);
    database.close();
  }
}

/**
 * Read the exact five-digit ZIP set represented by Polk situs rows.
 *
 * Exact ZIPs avoid the neighboring-county over-selection caused by broad 338,
 * 347, or 335 prefixes.
 *
 * @param {string} workDatabase Completed Polk DuckDB cache.
 * @returns {Promise<string[]>} Sorted unique five-digit ZIPs.
 */
export async function derivePolkSunbizZips(workDatabase) {
  return withDuckDb(workDatabase, async (connection) => {
    const rows = await queryDuckDb(
      connection,
      `
        SELECT DISTINCT substr(regexp_replace(postal_code, '[^0-9]', '', 'g'), 1, 5) AS zip
        FROM polk_sites
        WHERE length(regexp_replace(postal_code, '[^0-9]', '', 'g')) >= 5
        ORDER BY zip
      `,
    );
    return rows.flatMap((row) =>
      typeof row.zip === "string" && /^\d{5}$/.test(row.zip) ? [row.zip] : [],
    );
  });
}

/**
 * Normalize a street address while preserving unit identifiers.
 *
 * @param {unknown} value Street text.
 * @returns {string | null} Canonical uppercase street text.
 */
export function normalizePolkStreetAddress(value) {
  if (typeof value !== "string") return null;
  const normalized = value
    .toUpperCase()
    .replace(/\bSTREET\b/g, "ST")
    .replace(/\bROAD\b/g, "RD")
    .replace(/\bAVENUE\b/g, "AVE")
    .replace(/\bBOULEVARD\b/g, "BLVD")
    .replace(/\bDRIVE\b/g, "DR")
    .replace(/\bLANE\b/g, "LN")
    .replace(/\bCOURT\b/g, "CT")
    .replace(/\bCIRCLE\b/g, "CIR")
    .replace(/\bPARKWAY\b/g, "PKWY")
    .replace(/\bHIGHWAY\b/g, "HWY")
    .replace(/\bTRAIL\b/g, "TRL")
    .replace(/\bTERRACE\b/g, "TER")
    .replace(/\bPLACE\b/g, "PL")
    .replace(/\bNORTH\b/g, "N")
    .replace(/\bSOUTH\b/g, "S")
    .replace(/\bEAST\b/g, "E")
    .replace(/\bWEST\b/g, "W")
    .replace(/[^A-Z0-9#]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return normalized.length > 0 ? normalized : null;
}

/**
 * Build an exact street/city/ZIP key for local property matching.
 *
 * @param {{street:unknown,city:unknown,zip:unknown}} address Address components.
 * @returns {string | null} Exact key or null when a required component is absent.
 */
export function buildPolkPropertyAddressKey(address) {
  const street = normalizePolkStreetAddress(address.street);
  const city =
    typeof address.city === "string"
      ? address.city
          .toUpperCase()
          .replace(/[^A-Z0-9]+/g, " ")
          .replace(/\s+/g, " ")
          .trim()
      : "";
  const zip =
    typeof address.zip === "string"
      ? address.zip.replace(/\D/g, "").slice(0, 5)
      : "";
  if (street === null || city.length === 0 || !/^\d{5}$/.test(zip)) {
    return null;
  }
  return `${street}|${city}|${zip}`;
}

/**
 * Hash an address key so match artifacts can prove identity without duplicating
 * the raw address in every row.
 *
 * @param {string} key Exact address key.
 * @returns {string} SHA-256 hex digest.
 */
function hashAddressKey(key) {
  return createHash("sha256").update(key).digest("hex");
}

/**
 * Construct the structured street line from one Polk `polk_sites` row.
 *
 * @param {JsonObject} row Site row.
 * @returns {string} Joined street and optional unit.
 */
function polkSiteStreet(row) {
  return [
    row.street_number,
    row.street_number_suffix,
    row.street_prefix,
    row.street,
    row.street_suffix,
    row.street_suffix_direction,
    row.unit,
  ]
    .flatMap((value) =>
      typeof value === "string" && value.trim().length > 0
        ? [value.trim()]
        : [],
    )
    .join(" ");
}

/**
 * Load all exact Polk situs address keys and parcel ids.
 *
 * @param {string} workDatabase Completed Polk DuckDB cache.
 * @returns {Promise<{addressIndex:Map<string,string[]>,siteAddressCount:number,duplicateAddressKeyCount:number}>} Match index and evidence counts.
 */
export async function buildPolkPropertyAddressIndex(workDatabase) {
  return withDuckDb(workDatabase, async (connection) => {
    const rows = await queryDuckDb(
      connection,
      `
        SELECT
          parcel_id,
          street_number,
          street_number_suffix,
          street_prefix,
          street,
          street_suffix,
          street_suffix_direction,
          unit,
          city,
          postal_code
        FROM polk_sites
        WHERE parcel_id IS NOT NULL
          AND trim(parcel_id) <> ''
          AND street_number IS NOT NULL
          AND street IS NOT NULL
          AND city IS NOT NULL
          AND postal_code IS NOT NULL
      `,
    );
    /** @type {Map<string, string[]>} */
    const addressIndex = new Map();
    let siteAddressCount = 0;
    for (const row of rows) {
      const key = buildPolkPropertyAddressKey({
        street: polkSiteStreet(row),
        city: row.city,
        zip: row.postal_code,
      });
      const parcelId =
        typeof row.parcel_id === "string" ? row.parcel_id.trim() : "";
      if (key === null || parcelId.length === 0) continue;
      const parcels = addressIndex.get(key) ?? [];
      if (!parcels.includes(parcelId)) parcels.push(parcelId);
      addressIndex.set(key, parcels);
      siteAddressCount += 1;
    }
    return {
      addressIndex,
      siteAddressCount,
      duplicateAddressKeyCount: [...addressIndex.values()].filter(
        (parcels) => parcels.length > 1,
      ).length,
    };
  });
}

/**
 * Convert a Sunbiz address object into a candidate only when all exact-match
 * components are present.
 *
 * @param {string} documentNumber Sunbiz document number.
 * @param {PolkSunbizAddressCandidate["role"]} role Address role.
 * @param {number | null} officerOrdinal Officer slot.
 * @param {unknown} value Candidate address.
 * @returns {PolkSunbizAddressCandidate | null} Complete candidate or null.
 */
function sunbizAddressCandidate(documentNumber, role, officerOrdinal, value) {
  if (!isJsonObject(value)) return null;
  const street = [value.line1, value.line2]
    .flatMap((part) =>
      typeof part === "string" && part.trim().length > 0 ? [part.trim()] : [],
    )
    .join(" ");
  const city = typeof value.city === "string" ? value.city.trim() : "";
  const state = typeof value.state === "string" ? value.state.trim() : "";
  const zip = typeof value.zip === "string" ? value.zip.trim() : "";
  if (
    street.length === 0 ||
    city.length === 0 ||
    zip.length === 0 ||
    state.toUpperCase() !== "FL"
  ) {
    return null;
  }
  return {
    documentNumber,
    role,
    officerOrdinal,
    street,
    city,
    state: state.toUpperCase(),
    zip,
  };
}

/**
 * Collect every property-matchable address from one Sunbiz extraction record.
 *
 * @param {unknown} value Parsed extraction record.
 * @returns {PolkSunbizAddressCandidate[]} Complete address candidates.
 */
export function collectPolkSunbizAddressCandidates(value) {
  if (!isJsonObject(value) || !isJsonObject(value.entity)) return [];
  const entity = value.entity;
  const documentNumber =
    typeof entity.documentNumber === "string"
      ? entity.documentNumber.trim()
      : "";
  if (documentNumber.length === 0) return [];
  /** @type {(PolkSunbizAddressCandidate | null)[]} */
  const candidates = [
    sunbizAddressCandidate(
      documentNumber,
      "PRINCIPAL",
      null,
      entity.principalAddress,
    ),
    sunbizAddressCandidate(
      documentNumber,
      "MAILING",
      null,
      entity.mailingAddress,
    ),
  ];
  if (isJsonObject(entity.registeredAgent)) {
    candidates.push(
      sunbizAddressCandidate(
        documentNumber,
        "REGISTERED_AGENT",
        null,
        entity.registeredAgent.address,
      ),
    );
  }
  if (Array.isArray(entity.officers)) {
    for (const officer of entity.officers) {
      if (!isJsonObject(officer)) continue;
      candidates.push(
        sunbizAddressCandidate(
          documentNumber,
          "OFFICER",
          typeof officer.ordinal === "number" &&
            Number.isSafeInteger(officer.ordinal)
            ? officer.ordinal
            : null,
          officer.address,
        ),
      );
    }
  }
  return candidates.filter(
    /** @type {(candidate: PolkSunbizAddressCandidate | null) => candidate is PolkSunbizAddressCandidate} */ (
      (candidate) => candidate !== null
    ),
  );
}

/**
 * Open a local fixed-width text source as lines.
 *
 * @param {string} source Local source path.
 * @returns {AsyncIterable<string>} Source lines.
 */
function openLocalLines(source) {
  return createInterface({
    input: createReadStream(source, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });
}

/**
 * Write one JSONL chunk with stream backpressure.
 *
 * @param {string} filePath Destination path.
 * @param {readonly JsonObject[]} records Records.
 * @returns {Promise<void>} Resolves after close.
 */
async function writeJsonlChunk(filePath, records) {
  const writer = createWriteStream(filePath, { encoding: "utf8" });
  for (const record of records) {
    if (!writer.write(`${JSON.stringify(record)}\n`)) {
      await new Promise((resolve) => writer.once("drain", resolve));
    }
  }
  await new Promise((resolve, reject) => {
    writer.once("error", reject);
    writer.end(resolve);
  });
}

/**
 * Filter local statewide Sunbiz fixed-width files to exact Polk situs ZIPs.
 *
 * @param {PolkSunbizFilterOptions} options Filter options.
 * @returns {Promise<JsonObject>} Evidence manifest.
 */
export async function filterPolkSunbizByZip(options) {
  const exactZips = normalizeZipPrefixes(
    await derivePolkSunbizZips(options.workDatabase),
  );
  if (exactZips.some((zip) => zip.length !== 5)) {
    throw new Error("Polk Sunbiz filter requires exact five-digit ZIPs");
  }
  const chunksDirectory = path.join(options.outputDirectory, "chunks");
  await mkdir(chunksDirectory, { recursive: true });
  /** @type {JsonObject[]} */
  const entries = [];
  let matchedRecordCount = 0;
  let sourceRecordsRead = 0;
  let invalidRecordCount = 0;
  let remaining = options.maxRecords;
  for (const sourcePath of options.sources) {
    if (remaining !== null && remaining <= 0) break;
    if (sourcePath.startsWith("s3://")) {
      throw new Error(`Local Polk Sunbiz filter rejects S3 URI: ${sourcePath}`);
    }
    const absolutePath = path.resolve(sourcePath);
    const sourceInfo = await stat(absolutePath);
    const sourceSha256 = await sha256File(absolutePath);
    const sourceFileName = path.basename(absolutePath);
    /** @type {JsonObject[]} */
    const chunks = [];
    const summary = await extractCorporateDataLinesByZip({
      lines: openLocalLines(absolutePath),
      zipPrefixes: exactZips,
      chunkRecordLimit: options.chunkRecordLimit,
      maxRecords: remaining,
      sourceFileName,
      sourceDataS3Uri: `file://${absolutePath}`,
      sourceFormat: "text",
      onChunk: async (chunk) => {
        const fileName = `${path.parse(sourceFileName).name}-chunk-${String(
          chunk.chunkIndex,
        ).padStart(5, "0")}.jsonl`;
        const filePath = path.join(chunksDirectory, fileName);
        await writeJsonlChunk(
          filePath,
          /** @type {JsonObject[]} */ (chunk.records),
        );
        const receipt = {
          file: path.relative(options.outputDirectory, filePath),
          recordCount: chunk.records.length,
          sizeBytes: (await stat(filePath)).size,
          sha256: await sha256File(filePath),
        };
        chunks.push(receipt);
        return receipt;
      },
    });
    const entryMatched = Number(summary.matchedRecordCount ?? 0);
    const entryRead = Number(summary.sourceRecordsRead ?? 0);
    const entryInvalid = Number(summary.invalidRecordCount ?? 0);
    matchedRecordCount += entryMatched;
    sourceRecordsRead += entryRead;
    invalidRecordCount += entryInvalid;
    if (remaining !== null) remaining = Math.max(0, remaining - entryMatched);
    entries.push({
      sourceFileName,
      sourcePath: absolutePath,
      sizeBytes: sourceInfo.size,
      sha256: sourceSha256,
      sourceRecordsRead: entryRead,
      matchedRecordCount: entryMatched,
      invalidRecordCount: entryInvalid,
      chunks,
    });
  }
  const chunkRecordCount = entries.reduce(
    (total, entry) =>
      total +
      (Array.isArray(entry.chunks)
        ? entry.chunks.reduce(
            (subtotal, chunk) =>
              subtotal +
              (isJsonObject(chunk) && typeof chunk.recordCount === "number"
                ? chunk.recordCount
                : 0),
            0,
          )
        : 0),
    0,
  );
  const manifest = {
    schemaVersion: "oracle-node.polk-sunbiz-filter.v1",
    generatedAt: new Date().toISOString(),
    county: "polk",
    jobId: options.jobId,
    workDatabase: path.resolve(options.workDatabase),
    exactZips,
    exactZipCount: exactZips.length,
    sourceRecordsRead,
    matchedRecordCount,
    invalidRecordCount,
    chunkRecordCount,
    entries,
    complete:
      options.sources.length > 0 &&
      sourceRecordsRead > 0 &&
      matchedRecordCount === chunkRecordCount &&
      invalidRecordCount === 0 &&
      (options.maxRecords === null || matchedRecordCount <= options.maxRecords),
  };
  await writeFile(
    path.join(options.outputDirectory, "manifest.json"),
    `${JSON.stringify(manifest, null, 2)}\n`,
    "utf8",
  );
  return manifest;
}

/**
 * Create all class/relationship output streams.
 *
 * @param {string} outputDirectory Transform root.
 * @returns {Promise<Record<string, import("node:fs").WriteStream>>} Writers keyed by dataset.
 */
async function createTransformWriters(outputDirectory) {
  const datasets = [
    "classes/company",
    "classes/address",
    "classes/business_registration",
    "classes/business_registration_address",
    "classes/business_registration_party",
    "relationships/company_has_business_registration",
    "relationships/business_registration_has_address",
    "relationships/business_registration_address_has_address",
    "relationships/business_registration_has_party",
    "relationships/business_registration_party_has_address",
  ];
  /** @type {Record<string, import("node:fs").WriteStream>} */
  const writers = {};
  for (const dataset of datasets) {
    const directory = path.join(outputDirectory, dataset);
    await mkdir(directory, { recursive: true });
    writers[dataset] = createWriteStream(
      path.join(directory, "part-00000.jsonl"),
      { encoding: "utf8" },
    );
  }
  return writers;
}

/**
 * Write one record to a named transform stream.
 *
 * @param {Record<string, import("node:fs").WriteStream>} writers Writers.
 * @param {string} dataset Dataset key.
 * @param {JsonObject} record Record.
 * @returns {Promise<void>} Resolves after acceptance.
 */
async function writeTransformRecord(writers, dataset, record) {
  const writer = writers[dataset];
  if (writer === undefined) {
    throw new Error(`Missing Polk Sunbiz writer for ${dataset}`);
  }
  if (!writer.write(`${JSON.stringify(record)}\n`)) {
    await new Promise((resolve) => writer.once("drain", resolve));
  }
}

/**
 * Close every transform stream.
 *
 * @param {Record<string, import("node:fs").WriteStream>} writers Writers.
 * @returns {Promise<void>} Resolves after all close.
 */
async function closeTransformWriters(writers) {
  await Promise.all(
    Object.values(writers).map(
      (writer) =>
        new Promise((resolve, reject) => {
          writer.once("error", reject);
          writer.end(resolve);
        }),
    ),
  );
}

/**
 * Validate a minimal Sunbiz extraction record.
 *
 * @param {unknown} value Candidate record.
 * @returns {boolean} Whether the transform can consume it.
 */
function isSunbizExtractionRecord(value) {
  return (
    isJsonObject(value) &&
    isJsonObject(value.entity) &&
    typeof value.entity.documentNumber === "string" &&
    value.entity.documentNumber.length > 0 &&
    Array.isArray(value.matchedAddresses)
  );
}

/**
 * Write a transformed record bundle and return emitted dataset counts.
 *
 * @param {Record<string, import("node:fs").WriteStream>} writers Writers.
 * @param {ReturnType<typeof transformSunbizRecord>} bundle Transform bundle.
 * @param {Set<string>} emittedIdentifiers Cross-record entity dedupe set.
 * @returns {Promise<Record<string, number>>} Counts emitted by dataset.
 */
async function writeTransformBundle(writers, bundle, emittedIdentifiers) {
  /** @type {Record<string, number>} */
  const counts = {};
  const classGroups = [
    ["classes/company", bundle.companies],
    ["classes/address", bundle.addresses],
    ["classes/business_registration", bundle.businessRegistrations],
    [
      "classes/business_registration_address",
      bundle.businessRegistrationAddresses,
    ],
    ["classes/business_registration_party", bundle.businessRegistrationParties],
  ];
  for (const [dataset, records] of classGroups) {
    if (typeof dataset !== "string" || !Array.isArray(records)) continue;
    for (const record of records) {
      if (!isJsonObject(record)) continue;
      const identifier =
        typeof record.request_identifier === "string"
          ? record.request_identifier
          : JSON.stringify(record);
      const dedupeKey = `${dataset}:${identifier}`;
      if (emittedIdentifiers.has(dedupeKey)) continue;
      emittedIdentifiers.add(dedupeKey);
      await writeTransformRecord(writers, dataset, record);
      counts[dataset] = (counts[dataset] ?? 0) + 1;
    }
  }
  for (const relationship of bundle.relationships) {
    if (!isJsonObject(relationship)) continue;
    const relationshipType =
      typeof relationship.relationship_type === "string"
        ? relationship.relationship_type
        : "";
    if (relationshipType.length === 0) continue;
    const dataset = `relationships/${relationshipType}`;
    const dedupeKey = `${dataset}:${JSON.stringify(relationship)}`;
    if (emittedIdentifiers.has(dedupeKey)) continue;
    emittedIdentifiers.add(dedupeKey);
    await writeTransformRecord(writers, dataset, relationship);
    counts[dataset] = (counts[dataset] ?? 0) + 1;
  }
  return counts;
}

/**
 * Add per-dataset counts.
 *
 * @param {Record<string, number>} target Mutable totals.
 * @param {Record<string, number>} addition New counts.
 * @returns {void}
 */
function addDatasetCounts(target, addition) {
  for (const [dataset, count] of Object.entries(addition)) {
    target[dataset] = (target[dataset] ?? 0) + count;
  }
}

/**
 * Transform a Polk ZIP slice and exact-match its addresses to local Polk situs
 * rows. Every output family gets a count, byte size, and SHA-256 receipt.
 *
 * @param {PolkSunbizTransformOptions} options Transform options.
 * @returns {Promise<JsonObject>} Transform and property-match manifest.
 */
export async function transformAndMatchPolkSunbiz(options) {
  const filterManifestPath = path.join(options.inputDirectory, "manifest.json");
  const filterManifest = /** @type {unknown} */ (
    JSON.parse(await readFile(filterManifestPath, "utf8"))
  );
  if (
    !isJsonObject(filterManifest) ||
    filterManifest.county !== "polk" ||
    filterManifest.complete !== true
  ) {
    throw new Error("A complete Polk Sunbiz filter manifest is required");
  }
  const sourceUriByFileName = new Map(
    Array.isArray(filterManifest.entries)
      ? filterManifest.entries.flatMap((entry) =>
          isJsonObject(entry) &&
          typeof entry.sourceFileName === "string" &&
          typeof entry.sourcePath === "string"
            ? [[entry.sourceFileName, `file://${entry.sourcePath}`]]
            : [],
        )
      : [],
  );
  const chunksDirectory = path.join(options.inputDirectory, "chunks");
  const chunkNames = (await readdir(chunksDirectory))
    .filter((name) => name.endsWith(".jsonl"))
    .sort();
  if (chunkNames.length === 0) {
    throw new Error("No Polk Sunbiz ZIP-filter chunks were found");
  }
  const propertyIndex = await buildPolkPropertyAddressIndex(
    options.workDatabase,
  );
  const writers = await createTransformWriters(options.outputDirectory);
  const matchPath = path.join(
    options.outputDirectory,
    "matches",
    "property-address-matches.jsonl",
  );
  await mkdir(path.dirname(matchPath), { recursive: true });
  const matchWriter = createWriteStream(matchPath, { encoding: "utf8" });
  const emittedIdentifiers = new Set();
  const matchedDocuments = new Set();
  const matchedProperties = new Set();
  /** @type {Record<string, number>} */
  const datasetCounts = {};
  let sourceRecordCount = 0;
  let transformedRecordCount = 0;
  let invalidRecordCount = 0;
  let addressCandidateCount = 0;
  let matchedAddressCount = 0;
  let ambiguousAddressCount = 0;
  try {
    for (const chunkName of chunkNames) {
      const reader = createInterface({
        input: createReadStream(path.join(chunksDirectory, chunkName), {
          encoding: "utf8",
        }),
        crlfDelay: Infinity,
      });
      for await (const line of reader) {
        if (line.trim().length === 0) continue;
        if (
          options.maxRecords !== null &&
          sourceRecordCount >= options.maxRecords
        ) {
          break;
        }
        sourceRecordCount += 1;
        let parsed;
        try {
          parsed = /** @type {unknown} */ (JSON.parse(line));
        } catch {
          invalidRecordCount += 1;
          continue;
        }
        if (!isSunbizExtractionRecord(parsed)) {
          invalidRecordCount += 1;
          continue;
        }
        const bundle = transformSunbizRecord(
          /** @type {import("../transform-sunbiz-corporate-to-lexicon.mjs").SunbizZipExtractedRecord} */ (
            parsed
          ),
          {
            sourceDataUri:
              isJsonObject(parsed) && typeof parsed.sourceFileName === "string"
                ? (sourceUriByFileName.get(parsed.sourceFileName) ??
                  `file://${path.resolve(filterManifestPath)}`)
                : `file://${path.resolve(filterManifestPath)}`,
          },
        );
        addDatasetCounts(
          datasetCounts,
          await writeTransformBundle(writers, bundle, emittedIdentifiers),
        );
        transformedRecordCount += 1;
        for (const candidate of collectPolkSunbizAddressCandidates(parsed)) {
          addressCandidateCount += 1;
          const key = buildPolkPropertyAddressKey({
            street: candidate.street,
            city: candidate.city,
            zip: candidate.zip,
          });
          if (key === null) continue;
          const parcelIdentifiers = propertyIndex.addressIndex.get(key) ?? [];
          if (parcelIdentifiers.length === 0) continue;
          matchedAddressCount += 1;
          matchedDocuments.add(candidate.documentNumber);
          for (const parcelIdentifier of parcelIdentifiers) {
            matchedProperties.add(parcelIdentifier);
          }
          if (parcelIdentifiers.length > 1) ambiguousAddressCount += 1;
          const match = {
            documentNumber: candidate.documentNumber,
            addressRole: candidate.role,
            officerOrdinal: candidate.officerOrdinal,
            addressKeySha256: hashAddressKey(key),
            parcelIdentifiers,
            matchMethod: "exact_normalized_street_city_zip",
            matchConfidence:
              parcelIdentifiers.length === 1
                ? "exact_unique"
                : "exact_ambiguous",
            sourceChunk: chunkName,
          };
          if (!matchWriter.write(`${JSON.stringify(match)}\n`)) {
            await new Promise((resolve) => matchWriter.once("drain", resolve));
          }
        }
      }
      if (
        options.maxRecords !== null &&
        sourceRecordCount >= options.maxRecords
      ) {
        break;
      }
    }
  } finally {
    await Promise.all([
      closeTransformWriters(writers),
      new Promise((resolve, reject) => {
        matchWriter.once("error", reject);
        matchWriter.end(resolve);
      }),
    ]);
  }
  /** @type {JsonObject[]} */
  const outputArtifacts = [];
  for (const dataset of Object.keys(writers).sort()) {
    const filePath = path.join(
      options.outputDirectory,
      dataset,
      "part-00000.jsonl",
    );
    const info = await stat(filePath);
    outputArtifacts.push({
      dataset,
      file: path.relative(options.outputDirectory, filePath),
      recordCount: datasetCounts[dataset] ?? 0,
      sizeBytes: info.size,
      sha256: await sha256File(filePath),
    });
  }
  const matchInfo = await stat(matchPath);
  const expectedFilteredCount =
    typeof filterManifest.matchedRecordCount === "number"
      ? filterManifest.matchedRecordCount
      : null;
  const isFullRun = options.maxRecords === null;
  const complete =
    sourceRecordCount > 0 &&
    transformedRecordCount === sourceRecordCount &&
    invalidRecordCount === 0 &&
    (!isFullRun || sourceRecordCount === expectedFilteredCount);
  const manifest = {
    schemaVersion: "oracle-node.polk-sunbiz-transform-match.v1",
    generatedAt: new Date().toISOString(),
    county: "polk",
    filterManifest: {
      file: path.resolve(filterManifestPath),
      sha256: await sha256File(filterManifestPath),
      matchedRecordCount: expectedFilteredCount,
    },
    workDatabase: path.resolve(options.workDatabase),
    sourceRecordCount,
    transformedRecordCount,
    invalidRecordCount,
    outputArtifacts,
    propertyMatching: {
      siteAddressCount: propertyIndex.siteAddressCount,
      indexedAddressKeyCount: propertyIndex.addressIndex.size,
      duplicateAddressKeyCount: propertyIndex.duplicateAddressKeyCount,
      addressCandidateCount,
      matchedAddressCount,
      matchedDocumentCount: matchedDocuments.size,
      matchedPropertyCount: matchedProperties.size,
      ambiguousAddressCount,
      matchMethod: "exact_normalized_street_city_zip",
      file: path.relative(options.outputDirectory, matchPath),
      sizeBytes: matchInfo.size,
      sha256: await sha256File(matchPath),
    },
    limited: !isFullRun,
    complete,
  };
  await writeFile(
    path.join(options.outputDirectory, "manifest.json"),
    `${JSON.stringify(manifest, null, 2)}\n`,
    "utf8",
  );
  return manifest;
}

/**
 * Parse and run one local Polk Sunbiz stage.
 *
 * @param {readonly string[]} argv CLI arguments.
 * @returns {Promise<JsonObject>} Stage manifest.
 */
export async function runPolkSunbizCli(argv) {
  const { values } = parseArgs({
    args: [...argv],
    options: {
      stage: { type: "string" },
      source: { type: "string", multiple: true },
      "work-db": { type: "string" },
      "filter-dir": { type: "string" },
      "output-dir": { type: "string" },
      "chunk-record-limit": { type: "string" },
      "max-records": { type: "string" },
      "job-id": { type: "string" },
    },
    strict: true,
    allowPositionals: false,
  });
  const stage = typeof values.stage === "string" ? values.stage : "filter";
  const workDatabase =
    typeof values["work-db"] === "string"
      ? values["work-db"]
      : "tmp/polk/bulk/extracted/polk-appraisal.duckdb";
  const filterDirectory =
    typeof values["filter-dir"] === "string"
      ? values["filter-dir"]
      : "tmp/polk/sunbiz/corporate-by-zip";
  const outputDirectory =
    typeof values["output-dir"] === "string"
      ? values["output-dir"]
      : "tmp/polk/sunbiz/transformed";
  const maxRecords =
    typeof values["max-records"] === "string"
      ? Number.parseInt(values["max-records"], 10)
      : null;
  if (
    maxRecords !== null &&
    (!Number.isSafeInteger(maxRecords) || maxRecords < 1)
  ) {
    throw new Error("--max-records must be a positive integer");
  }
  if (stage === "filter") {
    const sources = Array.isArray(values.source)
      ? values.source.map(String)
      : [];
    if (sources.length === 0) {
      throw new Error("Polk Sunbiz filter requires at least one --source");
    }
    const chunkRecordLimit =
      typeof values["chunk-record-limit"] === "string"
        ? Number.parseInt(values["chunk-record-limit"], 10)
        : 5_000;
    if (!Number.isSafeInteger(chunkRecordLimit) || chunkRecordLimit < 1) {
      throw new Error("--chunk-record-limit must be a positive integer");
    }
    return filterPolkSunbizByZip({
      sources,
      workDatabase,
      outputDirectory: filterDirectory,
      chunkRecordLimit,
      maxRecords,
      jobId:
        typeof values["job-id"] === "string"
          ? values["job-id"]
          : `sunbiz-polk-${new Date().toISOString().slice(0, 10)}`,
    });
  }
  if (stage === "transform-match") {
    return transformAndMatchPolkSunbiz({
      inputDirectory: filterDirectory,
      workDatabase,
      outputDirectory,
      maxRecords,
    });
  }
  throw new Error("--stage must be filter or transform-match");
}

if (
  process.argv[1] !== undefined &&
  fileURLToPath(import.meta.url) === path.resolve(process.argv[1])
) {
  runPolkSunbizCli(process.argv.slice(2))
    .then((manifest) => {
      process.stdout.write(`${JSON.stringify(manifest, null, 2)}\n`);
    })
    .catch((caught) => {
      const message = caught instanceof Error ? caught.message : String(caught);
      process.stderr.write(
        `${JSON.stringify({ event: "polk_sunbiz_failed", error: message })}\n`,
      );
      process.exitCode = 1;
    });
}
