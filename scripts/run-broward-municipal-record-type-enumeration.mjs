#!/usr/bin/env node
// @ts-check

/**
 * Enumerate one Broward municipal portal through its complete official
 * exact-record-type selector. Partitions retain the source option value because
 * historical eSuite labels can be duplicated while their option IDs remain
 * distinct and non-overlapping.
 */

import { createHash } from "node:crypto";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

import { renderMunicipalPermitJsonl } from "./permit-source-adapters/broward-municipal-core.mjs";
import { getBrowardMunicipalPermitConfig } from "./permit-source-adapters/broward-municipal-config.mjs";
import { createBrowardMunicipalTransport } from "./permit-source-adapters/broward-municipal-transport.mjs";

const CHECKPOINT_SCHEMA_VERSION = /** @type {const} */ (
  "oracle-node.broward-municipal-record-type-enumeration.v1"
);
const SUPPORTED_JURISDICTIONS = new Set([
  "dania_beach",
  "davie",
  "lighthouse_point",
]);
const EXPECTED_PAGE_SIZE = 10;
const ESUITE_ANONYMOUS_RESULT_CEILING = 100;

/**
 * @typedef {import("./permit-source-adapters/broward-municipal-core.mjs").NormalizedBrowardMunicipalPermit} NormalizedBrowardMunicipalPermit
 * @typedef {import("./permit-source-adapters/broward-municipal-transport.mjs").BrowardMunicipalRecordTypePartition} BrowardMunicipalRecordTypePartition
 *
 * @typedef {object} MunicipalTypeEnumerationOptions
 * @property {string} jurisdictionKey - Exact supported municipal configuration.
 * @property {string} outputDirectory - Owner-only artifact root.
 * @property {string | null} partitionValue - Optional exact pilot partition.
 * @property {number | null} maxPartitions - Optional invocation pause bound.
 * @property {number} maxPagesPerPartition - Hard source-cap ceiling.
 * @property {number} delayMs - Minimum delay between public source operations.
 * @property {number} requestTimeoutMs - Per-browser-operation deadline.
 *
 * @typedef {object} MunicipalTypePageReceipt
 * @property {number} page - One-based source page.
 * @property {number} referenceCount - Reconciled unique references on the page.
 * @property {string} referenceSha256 - Digest proving replay stability.
 * @property {string} recordsPath - Private normalized page artifact.
 *
 * @typedef {object} MunicipalTypePartitionProgress
 * @property {string} value - Exact source option identity.
 * @property {string} label - Public source option label.
 * @property {number} nextPage - Next one-based source page.
 * @property {number | null} reportedCount - Stable source total when exposed.
 * @property {Record<string, MunicipalTypePageReceipt>} completedPages - Durable page receipts.
 * @property {string[]} seenRecordKeys - Stable identities used to reject page overlap.
 *
 * @typedef {object} CompletedMunicipalTypePartition
 * @property {string} value - Exact source option identity.
 * @property {string} label - Public source option label.
 * @property {number} pageCount - Terminal page count.
 * @property {number} recordCount - Exact partition record count.
 * @property {number | null} reportedCount - Source total when exposed.
 * @property {Record<string, MunicipalTypePageReceipt>} completedPages - Durable page receipts.
 * @property {string} completedAt - ISO terminal timestamp.
 *
 * @typedef {object} MunicipalTypeCheckpoint
 * @property {typeof CHECKPOINT_SCHEMA_VERSION} schemaVersion - Exact schema identity.
 * @property {string} jurisdictionKey - Configured jurisdiction.
 * @property {string} sourceSystem - Stable source identity.
 * @property {string} configurationSha256 - Immutable run configuration digest.
 * @property {string} universeSha256 - Complete official selector digest.
 * @property {"full_official_record_type_universe" | "selected_exact_record_type"} coverageBoundary
 * @property {number} sourcePartitionCount - Full source selector cardinality.
 * @property {string[]} pendingPartitionValues - Exact planned partition identities.
 * @property {Record<string, CompletedMunicipalTypePartition>} completedPartitions
 * @property {string[]} cappedPartitionValues - Exact partitions blocked at a proven source ceiling.
 * @property {MunicipalTypePartitionProgress | null} currentPartition
 * @property {number} recordObservations - Detail records durably captured.
 * @property {number} uniqueRecords - Deduplicated captured records.
 * @property {"running" | "paused" | "cooling" | "complete"} status
 * @property {"source_cap" | "timeout" | "incomplete_pagination" | "source_error" | null} blocker
 * @property {string | null} nextAttemptAt - Earliest safe retry for cooling.
 * @property {string} startedAt - ISO initial start.
 * @property {string} updatedAt - ISO durable update.
 *
 * @typedef {object} MunicipalTypeEnumerationSummary
 * @property {"paused" | "cooling" | "complete"} status - Invocation outcome.
 * @property {string} jurisdictionKey - Stable jurisdiction key.
 * @property {string} sourceSystem - Stable source identity.
 * @property {MunicipalTypeCheckpoint["coverageBoundary"]} coverageBoundary
 * @property {number} sourcePartitionCount - Complete official type count.
 * @property {number} plannedPartitionCount - Full or selected run count.
 * @property {number} completedPartitionCount - Durable terminal partitions.
 * @property {number} pendingPartitionCount - Remaining partitions.
 * @property {number} cappedPartitionCount - Partitions blocked by a source ceiling.
 * @property {number} capturedRecordCount - Unique normalized records.
 * @property {number} duplicateRecordCount - Exact duplicate observations.
 * @property {MunicipalTypeCheckpoint["blocker"]} blocker - Safe pause reason.
 * @property {string | null} nextAttemptAt - Safe retry time when cooling.
 */

/**
 * Parse exact-type enumeration options without accepting unknown flags.
 *
 * @param {readonly string[]} argv - CLI arguments after the script path.
 * @returns {MunicipalTypeEnumerationOptions} Validated local run options.
 */
export function parseMunicipalTypeEnumerationOptions(argv) {
  const allowed = new Set([
    "--jurisdiction",
    "--output-dir",
    "--partition-value",
    "--max-partitions",
    "--max-pages-per-partition",
    "--delay-ms",
    "--request-timeout-ms",
  ]);
  const values = new Map();
  for (let index = 0; index < argv.length; index += 2) {
    const flag = argv[index];
    const value = argv[index + 1];
    if (
      typeof flag !== "string" ||
      !allowed.has(flag) ||
      typeof value !== "string" ||
      value.startsWith("--") ||
      values.has(flag)
    ) {
      throw new Error(
        "Municipal type options must be unique --flag value pairs",
      );
    }
    values.set(flag, value);
  }
  const jurisdictionKey = values.get("--jurisdiction");
  if (
    typeof jurisdictionKey !== "string" ||
    !SUPPORTED_JURISDICTIONS.has(jurisdictionKey)
  ) {
    throw new Error(
      "--jurisdiction must be dania_beach, davie, or lighthouse_point",
    );
  }
  const outputDirectory = values.get("--output-dir");
  if (typeof outputDirectory !== "string" || outputDirectory.trim() === "") {
    throw new Error("--output-dir is required");
  }
  const partitionValue = values.get("--partition-value") ?? null;
  if (partitionValue !== null && partitionValue.trim() === "") {
    throw new Error("--partition-value must be non-empty");
  }
  const maxPartitionsText = values.get("--max-partitions");
  return {
    jurisdictionKey,
    outputDirectory: path.resolve(outputDirectory),
    partitionValue,
    maxPartitions:
      maxPartitionsText === undefined
        ? null
        : boundedInteger(maxPartitionsText, "--max-partitions", 1, 10_000),
    maxPagesPerPartition: boundedInteger(
      values.get("--max-pages-per-partition") ?? "10000",
      "--max-pages-per-partition",
      1,
      10_000,
    ),
    delayMs: boundedInteger(
      values.get("--delay-ms") ?? "1500",
      "--delay-ms",
      1_000,
      60_000,
    ),
    requestTimeoutMs: boundedInteger(
      values.get("--request-timeout-ms") ?? "45000",
      "--request-timeout-ms",
      1_000,
      120_000,
    ),
  };
}

/**
 * Run or resume a complete exact-type source enumeration.
 *
 * Each page artifact becomes durable before the page receipt advances. A
 * resumed browser replays completed pages and verifies their identity digests
 * before continuing, which preserves ASP.NET/SmartGov conversation state
 * without accepting changed pagination.
 *
 * @param {MunicipalTypeEnumerationOptions} options - Validated run options.
 * @param {{
 *   now?:()=>string,
 *   wait?:(milliseconds:number)=>Promise<void>,
 *   createTransport?:typeof createBrowardMunicipalTransport
 * }} [dependencies={}] - Injectable deterministic test dependencies.
 * @returns {Promise<MunicipalTypeEnumerationSummary>} Aggregate-only outcome.
 */
export async function runMunicipalTypeEnumeration(options, dependencies = {}) {
  const now = dependencies.now ?? (() => new Date().toISOString());
  const wait =
    dependencies.wait ??
    ((milliseconds) =>
      new Promise((resolvePromise) => {
        setTimeout(resolvePromise, milliseconds);
      }));
  const config = getBrowardMunicipalPermitConfig(options.jurisdictionKey);
  if (
    !config.capabilities.searchBy.includes("record_type") ||
    !["tyler_esuite", "smartgov"].includes(config.protocol)
  ) {
    throw new Error("Municipal source lacks exact record-type enumeration");
  }
  await mkdir(options.outputDirectory, { recursive: true, mode: 0o700 });
  const pagesDirectory = path.join(options.outputDirectory, "pages-private");
  await mkdir(pagesDirectory, { recursive: true, mode: 0o700 });
  const checkpointPath = path.join(
    options.outputDirectory,
    "checkpoint.private.json",
  );
  const normalizedListPath = path.join(
    options.outputDirectory,
    "normalized-list.private.jsonl",
  );
  const createTransport =
    dependencies.createTransport ?? createBrowardMunicipalTransport;
  const transport = await createTransport(config, {
    requestTimeoutMs: options.requestTimeoutMs,
    rawResultRowLimit: EXPECTED_PAGE_SIZE + 1,
  });
  /** @type {MunicipalTypeCheckpoint} */
  let checkpoint;
  try {
    const sourcePartitions = await transport.listRecordTypePartitions();
    const plan = createPartitionPlan(sourcePartitions, options.partitionValue);
    checkpoint = await readOrCreateCheckpoint(
      checkpointPath,
      config,
      options,
      sourcePartitions,
      plan,
      now(),
    );
    const aggregate = await readCapturedRecords(checkpoint);
    if (config.protocol === "tyler_esuite") {
      const historicalCapValues = Object.values(checkpoint.completedPartitions)
        .filter(isCompletedEsuiteSourceCap)
        .map((partition) => partition.value);
      if (historicalCapValues.length > 0) {
        checkpoint = {
          ...checkpoint,
          cappedPartitionValues: [
            ...new Set([
              ...checkpoint.cappedPartitionValues,
              ...historicalCapValues,
            ]),
          ].sort((left, right) => left.localeCompare(right)),
          status: "paused",
          blocker: "source_cap",
          nextAttemptAt: null,
          updatedAt: now(),
        };
        await writeCheckpoint(checkpointPath, checkpoint);
      }
    }
    let processedPartitions = 0;
    let requestCount = 0;

    try {
      while (
        checkpoint.cappedPartitionValues.length === 0 &&
        checkpoint.pendingPartitionValues.length > 0 &&
        (options.maxPartitions === null ||
          processedPartitions < options.maxPartitions)
      ) {
        const partitionValue = checkpoint.pendingPartitionValues[0];
        if (partitionValue === undefined) break;
        const partition = plan.find(
          (candidate) => candidate.value === partitionValue,
        );
        if (partition === undefined) {
          throw new Error("Checkpoint partition left the immutable plan");
        }
        let progress =
          checkpoint.currentPartition ?? createPartitionProgress(partition);
        if (progress.value !== partition.value) {
          throw new Error("Checkpoint current partition does not match plan");
        }

        for (
          let pageNumber = 1;
          pageNumber <= progress.nextPage;
          pageNumber += 1
        ) {
          if (pageNumber > options.maxPagesPerPartition) {
            throw new Error("Municipal partition exceeds maxPages source cap");
          }
          checkpoint = {
            ...checkpoint,
            currentPartition: progress,
            status: "running",
            blocker: null,
            nextAttemptAt: null,
            updatedAt: now(),
          };
          await writeCheckpoint(checkpointPath, checkpoint);
          if (requestCount > 0) await wait(options.delayMs);
          const page = await transport.fetchSearchPage(
            { kind: "record_type", value: partition.value },
            pageNumber,
          );
          requestCount += 1;
          const referenceKeys = page.references
            .map(
              (reference) =>
                `${config.sourceSystem}:${reference.sourceRecordId}`,
            )
            .sort((left, right) => left.localeCompare(right));
          if (new Set(referenceKeys).size !== referenceKeys.length) {
            throw new Error("Municipal partition page contains duplicate rows");
          }
          const referenceSha256 = sha256(JSON.stringify(referenceKeys));
          const replayReceipt = progress.completedPages[String(pageNumber)];
          if (pageNumber < progress.nextPage) {
            if (
              replayReceipt === undefined ||
              replayReceipt.referenceCount !== referenceKeys.length ||
              replayReceipt.referenceSha256 !== referenceSha256
            ) {
              throw new Error(
                "Municipal resumed pagination no longer matches its receipt",
              );
            }
            continue;
          }
          if (replayReceipt !== undefined) {
            throw new Error("Municipal next page already has a receipt");
          }
          validatePageContract(page, pageNumber, progress, config.protocol);
          const previouslySeen = new Set(progress.seenRecordKeys);
          if (referenceKeys.some((key) => previouslySeen.has(key))) {
            throw new Error("Municipal partition pages overlap");
          }
          /** @type {NormalizedBrowardMunicipalPermit[]} */
          const pageRecords = [];
          for (const reference of page.references) {
            if (requestCount > 0) await wait(options.delayMs);
            const record = await transport.fetchDetail(reference, {
              kind: "record_type",
              value: partition.value,
            });
            requestCount += 1;
            const expectedKey = `${config.sourceSystem}:${reference.sourceRecordId}`;
            if (
              record.record_key !== expectedKey ||
              record.permit_number !== reference.permitNumber
            ) {
              throw new Error("Municipal type detail identity mismatch");
            }
            checkpoint = {
              ...checkpoint,
              currentPartition: progress,
              status: "running",
              blocker: null,
              nextAttemptAt: null,
              updatedAt: now(),
            };
            await writeCheckpoint(checkpointPath, checkpoint);
            const existing = aggregate.byKey.get(record.record_key);
            if (
              existing !== undefined &&
              JSON.stringify(existing) !== JSON.stringify(record)
            ) {
              throw new Error("Municipal type partitions conflict");
            }
            if (existing !== undefined) {
              throw new Error("Municipal exact-type partitions overlap");
            }
            pageRecords.push(record);
          }
          const partitionIndex = plan.findIndex(
            (candidate) => candidate.value === partition.value,
          );
          const recordsPath = path.join(
            pagesDirectory,
            `partition-${String(partitionIndex + 1).padStart(4, "0")}`,
            `page-${String(pageNumber).padStart(5, "0")}.private.jsonl`,
          );
          await writePrivateAtomic(
            recordsPath,
            renderMunicipalPermitJsonl(pageRecords),
          );
          for (const record of pageRecords) {
            aggregate.byKey.set(record.record_key, record);
          }
          const reportedCount =
            progress.reportedCount ?? page.reportedCount ?? null;
          const nextSeenRecordKeys = [
            ...progress.seenRecordKeys,
            ...referenceKeys,
          ].sort((left, right) => left.localeCompare(right));
          const completedPages = {
            ...progress.completedPages,
            [String(pageNumber)]: {
              page: pageNumber,
              referenceCount: referenceKeys.length,
              referenceSha256,
              recordsPath,
            },
          };
          if (page.nextPage === null) {
            if (
              reportedCount !== null &&
              nextSeenRecordKeys.length !== reportedCount
            ) {
              throw new Error(
                "Municipal partition terminal count does not reconcile",
              );
            }
            const completedAt = now();
            checkpoint = {
              ...checkpoint,
              pendingPartitionValues:
                checkpoint.pendingPartitionValues.slice(1),
              completedPartitions: {
                ...checkpoint.completedPartitions,
                [partition.value]: {
                  value: partition.value,
                  label: partition.label,
                  pageCount: Object.keys(completedPages).length,
                  recordCount: nextSeenRecordKeys.length,
                  reportedCount,
                  completedPages,
                  completedAt,
                },
              },
              currentPartition: null,
              recordObservations:
                checkpoint.recordObservations + pageRecords.length,
              uniqueRecords: aggregate.byKey.size,
              status:
                checkpoint.pendingPartitionValues.length === 1
                  ? "complete"
                  : "running",
              blocker: null,
              nextAttemptAt: null,
              updatedAt: completedAt,
            };
            await writeCheckpoint(checkpointPath, checkpoint);
            processedPartitions += 1;
            break;
          }
          progress = {
            ...progress,
            nextPage: pageNumber + 1,
            reportedCount,
            completedPages,
            seenRecordKeys: nextSeenRecordKeys,
          };
          checkpoint = {
            ...checkpoint,
            currentPartition: progress,
            recordObservations:
              checkpoint.recordObservations + pageRecords.length,
            uniqueRecords: aggregate.byKey.size,
            status: "running",
            blocker: null,
            nextAttemptAt: null,
            updatedAt: now(),
          };
          await writeCheckpoint(checkpointPath, checkpoint);
        }
      }
    } catch (error) {
      const blocker = classifyFailure(error);
      const cooling = blocker === "timeout" || blocker === "source_error";
      const cappedPartitionValues =
        blocker === "source_cap" && checkpoint.currentPartition !== null
          ? [
              ...new Set([
                ...checkpoint.cappedPartitionValues,
                checkpoint.currentPartition.value,
              ]),
            ].sort((left, right) => left.localeCompare(right))
          : checkpoint.cappedPartitionValues;
      checkpoint = {
        ...checkpoint,
        cappedPartitionValues,
        status: cooling ? "cooling" : "paused",
        blocker,
        nextAttemptAt: cooling
          ? new Date(Date.parse(now()) + 15 * 60_000).toISOString()
          : null,
        updatedAt: now(),
      };
      await writeCheckpoint(checkpointPath, checkpoint);
    }

    if (
      checkpoint.pendingPartitionValues.length > 0 &&
      checkpoint.status === "running"
    ) {
      checkpoint = {
        ...checkpoint,
        status: "paused",
        blocker: null,
        nextAttemptAt: null,
        updatedAt: now(),
      };
      await writeCheckpoint(checkpointPath, checkpoint);
    }
    await writePrivateAtomic(
      normalizedListPath,
      renderMunicipalPermitJsonl([...aggregate.byKey.values()]),
    );
    const cappedValues = new Set(checkpoint.cappedPartitionValues);
    const completedPartitionValues = Object.keys(
      checkpoint.completedPartitions,
    );
    const cappedCompletedPartitionCount = completedPartitionValues.filter(
      (value) => cappedValues.has(value),
    ).length;
    const completedPartitionCount =
      completedPartitionValues.length - cappedCompletedPartitionCount;
    const pendingPartitionCount =
      checkpoint.pendingPartitionValues.length + cappedCompletedPartitionCount;
    return {
      status:
        checkpoint.status === "complete"
          ? "complete"
          : checkpoint.status === "cooling"
            ? "cooling"
            : "paused",
      jurisdictionKey: config.key,
      sourceSystem: config.sourceSystem,
      coverageBoundary: checkpoint.coverageBoundary,
      sourcePartitionCount: checkpoint.sourcePartitionCount,
      plannedPartitionCount: completedPartitionCount + pendingPartitionCount,
      completedPartitionCount,
      pendingPartitionCount,
      cappedPartitionCount: cappedValues.size,
      capturedRecordCount: aggregate.byKey.size,
      duplicateRecordCount: aggregate.duplicateRecordCount,
      blocker: checkpoint.blocker,
      nextAttemptAt: checkpoint.nextAttemptAt,
    };
  } finally {
    await transport.close();
  }
}

/**
 * Build the full or exactly selected source partition plan.
 *
 * @param {readonly BrowardMunicipalRecordTypePartition[]} sourcePartitions - Complete selector universe.
 * @param {string | null} selectedValue - Optional pilot value.
 * @returns {readonly BrowardMunicipalRecordTypePartition[]} Immutable run plan.
 */
function createPartitionPlan(sourcePartitions, selectedValue) {
  if (selectedValue === null) return sourcePartitions;
  const matches = sourcePartitions.filter(
    (partition) => partition.value === selectedValue,
  );
  if (matches.length !== 1) {
    throw new Error("Selected municipal record-type partition is not unique");
  }
  return matches;
}

/**
 * Initialize one partition before its page-one request.
 *
 * @param {BrowardMunicipalRecordTypePartition} partition - Exact source partition.
 * @returns {MunicipalTypePartitionProgress} Empty page progress.
 */
function createPartitionProgress(partition) {
  return {
    value: partition.value,
    label: partition.label,
    nextPage: 1,
    reportedCount: null,
    completedPages: {},
    seenRecordKeys: [],
  };
}

/**
 * Recognize a historical eSuite partition that stopped exactly at the repeated
 * anonymous ten-page/100-row boundary without exposing a source total.
 *
 * @param {CompletedMunicipalTypePartition} partition - Previously terminal partition.
 * @returns {boolean} True only for the exact observed anonymous source ceiling.
 */
function isCompletedEsuiteSourceCap(partition) {
  if (
    partition.pageCount !==
      ESUITE_ANONYMOUS_RESULT_CEILING / EXPECTED_PAGE_SIZE ||
    partition.recordCount !== ESUITE_ANONYMOUS_RESULT_CEILING ||
    partition.reportedCount !== null
  ) {
    return false;
  }
  return Array.from(
    {
      length: ESUITE_ANONYMOUS_RESULT_CEILING / EXPECTED_PAGE_SIZE,
    },
    (_value, index) => index + 1,
  ).every((pageNumber) => {
    const receipt = partition.completedPages[String(pageNumber)];
    return (
      receipt?.page === pageNumber &&
      receipt.referenceCount === EXPECTED_PAGE_SIZE
    );
  });
}

/**
 * Validate strict sequential paging and stable optional source totals.
 *
 * @param {import("./permit-source-adapters/broward-municipal-core.mjs").BrowardMunicipalSearchPage} page - Parsed source page.
 * @param {number} pageNumber - Current one-based page.
 * @param {MunicipalTypePartitionProgress} progress - Durable partition progress.
 * @param {import("./permit-source-adapters/broward-municipal-core.mjs").BrowardMunicipalProtocol} protocol - Exact source protocol.
 * @returns {void}
 */
function validatePageContract(page, pageNumber, progress, protocol) {
  if (
    page.nextPage !== null &&
    (page.nextPage !== pageNumber + 1 ||
      page.references.length !== EXPECTED_PAGE_SIZE)
  ) {
    throw new Error("Municipal partition pagination is incomplete");
  }
  if (page.nextPage === null && page.references.length > EXPECTED_PAGE_SIZE) {
    throw new Error("Municipal partition terminal page exceeds page size");
  }
  if (
    protocol === "tyler_esuite" &&
    page.nextPage === null &&
    pageNumber * EXPECTED_PAGE_SIZE === ESUITE_ANONYMOUS_RESULT_CEILING &&
    page.references.length === EXPECTED_PAGE_SIZE &&
    (page.reportedCount === undefined || page.reportedCount === null) &&
    progress.reportedCount === null
  ) {
    throw new Error("Municipal partition reached anonymous eSuite source cap");
  }
  if (
    page.reportedCount !== undefined &&
    page.reportedCount !== null &&
    progress.reportedCount !== null &&
    page.reportedCount !== progress.reportedCount
  ) {
    throw new Error("Municipal partition source total changed");
  }
}

/**
 * Read all durable page artifacts and reconcile identities across partitions.
 *
 * @param {MunicipalTypeCheckpoint} checkpoint - Validated checkpoint.
 * @returns {Promise<{byKey:Map<string,NormalizedBrowardMunicipalPermit>,duplicateRecordCount:number}>}
 *   Captured unique records and exact duplicate observations.
 */
async function readCapturedRecords(checkpoint) {
  /** @type {Map<string, NormalizedBrowardMunicipalPermit>} */
  const byKey = new Map();
  let duplicateRecordCount = 0;
  const receipts = [
    ...Object.values(checkpoint.completedPartitions).flatMap((partition) =>
      Object.values(partition.completedPages),
    ),
    ...Object.values(checkpoint.currentPartition?.completedPages ?? {}),
  ];
  for (const receipt of receipts) {
    const text = await readFile(receipt.recordsPath, "utf8");
    for (const line of text.split(/\r?\n/u)) {
      if (line.trim() === "") continue;
      const parsed = /** @type {unknown} */ (JSON.parse(line));
      if (!isRecord(parsed) || typeof parsed.record_key !== "string") {
        throw new Error("Municipal type page artifact is malformed");
      }
      const record = /** @type {NormalizedBrowardMunicipalPermit} */ (parsed);
      const existing = byKey.get(record.record_key);
      if (
        existing !== undefined &&
        JSON.stringify(existing) !== JSON.stringify(record)
      ) {
        throw new Error("Municipal type page artifacts conflict");
      }
      if (existing !== undefined) {
        throw new Error("Municipal exact-type page artifacts overlap");
      }
      byKey.set(record.record_key, record);
    }
  }
  return { byKey, duplicateRecordCount };
}

/**
 * Read or create one checkpoint tied to an immutable source universe.
 *
 * @param {string} checkpointPath - Private checkpoint path.
 * @param {ReturnType<typeof getBrowardMunicipalPermitConfig>} config - Source configuration.
 * @param {MunicipalTypeEnumerationOptions} options - Run options.
 * @param {readonly BrowardMunicipalRecordTypePartition[]} sourcePartitions - Full source universe.
 * @param {readonly BrowardMunicipalRecordTypePartition[]} plan - Selected run plan.
 * @param {string} startedAt - Initial ISO timestamp.
 * @returns {Promise<MunicipalTypeCheckpoint>} Validated durable state.
 */
async function readOrCreateCheckpoint(
  checkpointPath,
  config,
  options,
  sourcePartitions,
  plan,
  startedAt,
) {
  const universeSha256 = sha256(JSON.stringify(sourcePartitions));
  const coverageBoundary =
    options.partitionValue === null
      ? /** @type {const} */ ("full_official_record_type_universe")
      : /** @type {const} */ ("selected_exact_record_type");
  const configurationSha256 = sha256(
    JSON.stringify({
      jurisdictionKey: config.key,
      sourceSystem: config.sourceSystem,
      universeSha256,
      coverageBoundary,
      partitionValues: plan.map((partition) => partition.value),
      expectedPageSize: EXPECTED_PAGE_SIZE,
      maxPagesPerPartition: options.maxPagesPerPartition,
    }),
  );
  try {
    const parsed = /** @type {unknown} */ (
      JSON.parse(await readFile(checkpointPath, "utf8"))
    );
    if (
      !isRecord(parsed) ||
      parsed.schemaVersion !== CHECKPOINT_SCHEMA_VERSION ||
      parsed.jurisdictionKey !== config.key ||
      parsed.sourceSystem !== config.sourceSystem ||
      parsed.configurationSha256 !== configurationSha256 ||
      parsed.universeSha256 !== universeSha256 ||
      parsed.coverageBoundary !== coverageBoundary ||
      parsed.sourcePartitionCount !== sourcePartitions.length ||
      !Array.isArray(parsed.pendingPartitionValues) ||
      !parsed.pendingPartitionValues.every(
        (value) => typeof value === "string",
      ) ||
      !isRecord(parsed.completedPartitions) ||
      (parsed.cappedPartitionValues !== undefined &&
        (!Array.isArray(parsed.cappedPartitionValues) ||
          !parsed.cappedPartitionValues.every(
            (value) => typeof value === "string",
          ) ||
          new Set(parsed.cappedPartitionValues).size !==
            parsed.cappedPartitionValues.length))
    ) {
      throw new Error(
        "Existing municipal type checkpoint does not match source universe",
      );
    }
    return /** @type {MunicipalTypeCheckpoint} */ ({
      ...parsed,
      cappedPartitionValues: parsed.cappedPartitionValues ?? [],
    });
  } catch (error) {
    if (!isNodeError(error) || error.code !== "ENOENT") throw error;
  }
  const checkpoint = {
    schemaVersion: CHECKPOINT_SCHEMA_VERSION,
    jurisdictionKey: config.key,
    sourceSystem: config.sourceSystem,
    configurationSha256,
    universeSha256,
    coverageBoundary,
    sourcePartitionCount: sourcePartitions.length,
    pendingPartitionValues: plan.map((partition) => partition.value),
    completedPartitions: {},
    cappedPartitionValues: [],
    currentPartition: null,
    recordObservations: 0,
    uniqueRecords: 0,
    status: /** @type {const} */ ("running"),
    blocker: null,
    nextAttemptAt: null,
    startedAt,
    updatedAt: startedAt,
  };
  await writeCheckpoint(checkpointPath, checkpoint);
  return checkpoint;
}

/**
 * Convert one source failure to an allowlisted aggregate checkpoint reason.
 *
 * @param {unknown} error - Caught source/transport error.
 * @returns {Exclude<MunicipalTypeCheckpoint["blocker"], null>} Safe reason.
 */
function classifyFailure(error) {
  if (!(error instanceof Error)) return "source_error";
  if (/maxPages|source cap/iu.test(error.message)) return "source_cap";
  if (/timed out|timeout|signal timed out/iu.test(error.message)) {
    return "timeout";
  }
  if (/pagination|terminal count|source total|overlap/iu.test(error.message)) {
    return "incomplete_pagination";
  }
  return "source_error";
}

/**
 * Write one complete private checkpoint atomically.
 *
 * @param {string} checkpointPath - Final private path.
 * @param {MunicipalTypeCheckpoint} checkpoint - Complete state.
 * @returns {Promise<void>} Resolves after durable replacement.
 */
async function writeCheckpoint(checkpointPath, checkpoint) {
  await writePrivateAtomic(
    checkpointPath,
    `${JSON.stringify(checkpoint, null, 2)}\n`,
  );
}

/**
 * Write one owner-only artifact through atomic replacement.
 *
 * @param {string} filePath - Final private path.
 * @param {string} content - Complete UTF-8 content.
 * @returns {Promise<void>} Resolves after replacement.
 */
async function writePrivateAtomic(filePath, content) {
  await mkdir(path.dirname(filePath), { recursive: true, mode: 0o700 });
  const temporaryPath = `${filePath}.${String(process.pid)}.tmp`;
  await writeFile(temporaryPath, content, {
    encoding: "utf8",
    mode: 0o600,
  });
  await rename(temporaryPath, filePath);
}

/**
 * Parse one bounded decimal integer.
 *
 * @param {string} value - Candidate decimal text.
 * @param {string} flag - CLI flag for fixed diagnostics.
 * @param {number} minimum - Inclusive minimum.
 * @param {number} maximum - Inclusive maximum.
 * @returns {number} Validated integer.
 */
function boundedInteger(value, flag, minimum, maximum) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < minimum || parsed > maximum) {
    throw new Error(
      `${flag} must be an integer from ${String(minimum)} through ${String(maximum)}`,
    );
  }
  return parsed;
}

/**
 * Hash deterministic source/run metadata.
 *
 * @param {string} value - Canonical serialized value.
 * @returns {string} Lowercase SHA-256.
 */
function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

/**
 * Narrow parsed JSON to an object record.
 *
 * @param {unknown} value - Candidate value.
 * @returns {value is Record<string, unknown>} True for non-array objects.
 */
function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/**
 * Narrow a caught filesystem error to a Node error code.
 *
 * @param {unknown} value - Caught value.
 * @returns {value is Error & {code:string}} True when a string code exists.
 */
function isNodeError(value) {
  return (
    value instanceof Error && "code" in value && typeof value.code === "string"
  );
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  runMunicipalTypeEnumeration(
    parseMunicipalTypeEnumerationOptions(process.argv.slice(2)),
  )
    .then((summary) => {
      console.log(
        JSON.stringify({
          event: "broward_municipal_type_enumeration_finished",
          ...summary,
        }),
      );
    })
    .catch((error) => {
      console.error(
        JSON.stringify({
          event: "broward_municipal_type_enumeration_failed",
          message: error instanceof Error ? error.message : "Unknown error",
        }),
      );
      process.exitCode = 1;
    });
}
