#!/usr/bin/env node
// @ts-check

/**
 * Enumerate municipal sources without complete broad lists through a verified
 * BCPA property-first seed. Every source query is exact, sequential, durable,
 * and heartbeated through detail traversal. A query becomes terminal only when
 * the client-all result page remains below its exclusive row cap and every
 * selected detail reconciles.
 */

import { createHash } from "node:crypto";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

import { renderMunicipalPermitJsonl } from "./permit-source-adapters/broward-municipal-core.mjs";
import { getBrowardMunicipalPermitConfig } from "./permit-source-adapters/broward-municipal-config.mjs";
import { createBrowardMunicipalTransport } from "./permit-source-adapters/broward-municipal-transport.mjs";

const CHECKPOINT_SCHEMA_VERSION = /** @type {const} */ (
  "oracle-node.broward-municipal-property-enumeration.v1"
);
const DEFERRED_CAP_SCHEMA_VERSION = /** @type {const} */ (
  "oracle-node.broward-municipal-deferred-cap.v1"
);
const MAX_RAW_RESULT_ROW_LIMIT = 1_000;
const MAX_DEFERRED_CAP_ATTEMPTS = 3;
const MAX_DEFERRED_CAP_RETRIES_PER_INVOCATION = 3;
const DEFERRED_CAP_RETRY_DELAY_MS = 24 * 60 * 60_000;
const SUPPORTED_JURISDICTIONS = new Set([
  "coconut_creek",
  "lauderhill",
  "margate",
  "pompano_beach",
  "tamarac",
]);

/**
 * @typedef {import("./permit-source-adapters/broward-municipal-core.mjs").BrowardMunicipalQuery} BrowardMunicipalQuery
 * @typedef {import("./permit-source-adapters/broward-municipal-core.mjs").NormalizedBrowardMunicipalPermit} NormalizedBrowardMunicipalPermit
 *
 * @typedef {object} MunicipalPropertyEnumerationOptions
 * @property {string} jurisdictionKey - Exact supported source key.
 * @property {string} seedPath - Complete private municipal seed CSV.
 * @property {string} outputDirectory - Owner-only artifact root.
 * @property {number | null} maxQueries - Optional invocation pilot/pause bound.
 * @property {number} maxResultsPerQuery - Exclusive client-all source cap.
 * @property {number} delayMs - Minimum delay between source operations.
 * @property {number} requestTimeoutMs - Per-request deadline.
 *
 * @typedef {object} MunicipalPropertySeedQuery
 * @property {string} jurisdictionKey - Hyphenated registry key.
 * @property {"folio" | "address"} queryKind - Certified source field.
 * @property {string} queryValue - Private exact query.
 * @property {number} propertyCount - BCPA properties represented.
 *
 * @typedef {object} MunicipalDeferredCapItem
 * @property {typeof DEFERRED_CAP_SCHEMA_VERSION} schemaVersion - Safe ledger row schema.
 * @property {string} seedSha256 - Immutable private seed content identity.
 * @property {string} queryPlanSha256 - Immutable jurisdiction query-plan identity.
 * @property {number} queryIndex - Zero-based stable position in the seed plan.
 * @property {string} queryDigest - SHA-256 identity derived from plan, index, kind, and private query.
 * @property {number} representedProperties - Aggregate properties represented by the deferred query.
 * @property {"client_all_exclusive_cap"} reason - Fail-closed deferral reason.
 * @property {number} observedResultCount - Source result count, bounded by the parser ceiling.
 * @property {number} exclusiveCap - Configured threshold that cannot be accepted as terminal.
 * @property {number} attemptCount - Bounded source-cap observations for this query.
 * @property {string} firstDeferredAt - First ISO deferral timestamp.
 * @property {string} lastAttemptAt - Latest ISO cap observation timestamp.
 * @property {string | null} nextAttemptAt - Earliest future bounded retry, or null after the attempt ceiling.
 *
 * @typedef {object} MunicipalPropertyCheckpoint
 * @property {typeof CHECKPOINT_SCHEMA_VERSION} schemaVersion - Exact schema.
 * @property {string} jurisdictionKey - Municipal configuration key.
 * @property {string} sourceSystem - Stable source identity.
 * @property {string} seedSha256 - Complete seed bytes digest.
 * @property {string} queryPlanSha256 - Exact jurisdiction query plan digest.
 * @property {"bcpa_property_first_folio" | "bcpa_property_first_address"} coverageBoundary
 * @property {number} totalQueries - Immutable source-query denominator.
 * @property {number} representedProperties - Immutable BCPA property denominator.
 * @property {number} nextQueryIndex - Zero-based resume cursor.
 * @property {number} completedQueries - Durable terminal source queries.
 * @property {number} emptyQueries - Explicit terminal empty source queries.
 * @property {number} recordObservations - Detail records across terminal queries.
 * @property {number} uniqueRecords - Deduplicated captured records.
 * @property {Record<string,MunicipalDeferredCapItem>} deferredCapItems - Unresolved cap items keyed only by safe digest.
 * @property {"running" | "paused" | "cooling" | "complete"} status
 * @property {"source_cap" | "timeout" | "incomplete_pagination" | "source_error" | null} blocker
 * @property {string | null} nextAttemptAt - Earliest safe retry when cooling.
 * @property {string} startedAt - ISO first start.
 * @property {string} updatedAt - ISO durable update.
 *
 * @typedef {object} MunicipalPropertyEnumerationSummary
 * @property {"paused" | "cooling" | "complete"} status - Invocation outcome.
 * @property {string} jurisdictionKey - Stable configuration key.
 * @property {string} sourceSystem - Stable source identity.
 * @property {MunicipalPropertyCheckpoint["coverageBoundary"]} coverageBoundary
 * @property {number} totalQueries - Exact private seed query count.
 * @property {number} completedQueries - Durable terminal query count.
 * @property {number} representedProperties - BCPA property denominator.
 * @property {number} recordObservations - Detail records across source queries.
 * @property {number} uniqueRecordCount - Deduplicated normalized records.
 * @property {number} duplicateRecordCount - Exact repeated record observations.
 * @property {number} deferredCapCount - Unresolved exact-address source caps.
 * @property {MunicipalPropertyCheckpoint["blocker"]} blocker - Safe reason.
 * @property {string | null} nextAttemptAt - Safe retry time when cooling.
 */

/**
 * Parse property-first enumeration options.
 *
 * @param {readonly string[]} argv - CLI arguments after the script path.
 * @returns {MunicipalPropertyEnumerationOptions} Validated options.
 */
export function parseMunicipalPropertyEnumerationOptions(argv) {
  const allowed = new Set([
    "--jurisdiction",
    "--seed",
    "--output-dir",
    "--max-queries",
    "--max-results-per-query",
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
        "Municipal property options must be unique supported --flag value pairs",
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
      "--jurisdiction must be coconut_creek, lauderhill, margate, pompano_beach, or tamarac",
    );
  }
  const seedPath = values.get("--seed");
  const outputDirectory = values.get("--output-dir");
  if (typeof seedPath !== "string" || seedPath.trim() === "") {
    throw new Error("--seed is required");
  }
  if (typeof outputDirectory !== "string" || outputDirectory.trim() === "") {
    throw new Error("--output-dir is required");
  }
  const maxQueriesText = values.get("--max-queries");
  return {
    jurisdictionKey,
    seedPath: path.resolve(seedPath),
    outputDirectory: path.resolve(outputDirectory),
    maxQueries:
      maxQueriesText === undefined
        ? null
        : boundedInteger(maxQueriesText, "--max-queries", 1, 10_000_000),
    maxResultsPerQuery: boundedInteger(
      values.get("--max-results-per-query") ?? "100",
      "--max-results-per-query",
      2,
      1_000,
    ),
    delayMs: boundedInteger(
      values.get("--delay-ms") ?? "1500",
      "--delay-ms",
      1_000,
      60_000,
    ),
    requestTimeoutMs: boundedInteger(
      values.get("--request-timeout-ms") ?? "30000",
      "--request-timeout-ms",
      1_000,
      120_000,
    ),
  };
}

/**
 * Run or resume one jurisdiction's complete private property query plan.
 *
 * @param {MunicipalPropertyEnumerationOptions} options - Validated run options.
 * @param {{
 *   now?:()=>string,
 *   wait?:(milliseconds:number)=>Promise<void>,
 *   createTransport?:typeof createBrowardMunicipalTransport
 * }} [dependencies={}] - Injectable deterministic dependencies.
 * @returns {Promise<MunicipalPropertyEnumerationSummary>} Aggregate-only outcome.
 */
export async function runMunicipalPropertyEnumeration(
  options,
  dependencies = {},
) {
  const now = dependencies.now ?? (() => new Date().toISOString());
  const wait =
    dependencies.wait ??
    ((milliseconds) =>
      new Promise((resolvePromise) => {
        setTimeout(resolvePromise, milliseconds);
      }));
  const seedText = await readFile(options.seedPath, "utf8");
  const seedSha256 = sha256(seedText);
  const queries = readMunicipalSeedQueries(seedText, options.jurisdictionKey);
  if (queries.length === 0) {
    throw new Error("Municipal property seed has no jurisdiction queries");
  }
  const config = getBrowardMunicipalPermitConfig(options.jurisdictionKey);
  if (
    queries.some(
      (query) => !config.capabilities.searchBy.includes(query.queryKind),
    )
  ) {
    throw new Error("Municipal property seed uses an unsupported query kind");
  }
  await mkdir(options.outputDirectory, { recursive: true, mode: 0o700 });
  const queriesDirectory = path.join(
    options.outputDirectory,
    "queries-private",
  );
  await mkdir(queriesDirectory, { recursive: true, mode: 0o700 });
  const checkpointPath = path.join(
    options.outputDirectory,
    "checkpoint.private.json",
  );
  const normalizedListPath = path.join(
    options.outputDirectory,
    "normalized-list.private.jsonl",
  );
  const deferredCapLedgerPath = path.join(
    options.outputDirectory,
    "deferred-cap.private.jsonl",
  );
  let checkpoint = await readOrCreateCheckpoint(
    checkpointPath,
    config,
    queries,
    seedSha256,
    now(),
  );
  const aggregate = await readCompletedQueryArtifacts(
    queriesDirectory,
    checkpoint,
  );
  await writeDeferredCapLedger(deferredCapLedgerPath, checkpoint);
  const createTransport =
    dependencies.createTransport ?? createBrowardMunicipalTransport;
  const transport = await createTransport(config, {
    requestTimeoutMs: options.requestTimeoutMs,
    rawResultRowLimit: MAX_RAW_RESULT_ROW_LIMIT,
  });
  try {
    let processed = 0;
    let operationCount = 0;
    try {
      while (
        checkpoint.nextQueryIndex < queries.length &&
        (options.maxQueries === null || processed < options.maxQueries)
      ) {
        const seedQuery = queries[checkpoint.nextQueryIndex];
        if (seedQuery === undefined) {
          throw new Error("Municipal property checkpoint exceeds query plan");
        }
        const query = /** @type {BrowardMunicipalQuery} */ ({
          kind: seedQuery.queryKind,
          value: seedQuery.queryValue,
        });
        checkpoint = {
          ...checkpoint,
          status: "running",
          blocker: null,
          nextAttemptAt: null,
          updatedAt: now(),
        };
        await writeCheckpoint(checkpointPath, checkpoint);
        if (operationCount > 0) await wait(options.delayMs);
        let page;
        try {
          page = await transport.fetchSearchPage(query, 1);
          operationCount += 1;
        } catch (error) {
          if (classifyFailure(error) !== "source_cap") throw error;
          const deferredAt = now();
          checkpoint = deferCappedPropertyQuery(
            checkpoint,
            seedQuery,
            checkpoint.nextQueryIndex,
            options.maxResultsPerQuery,
            options.maxResultsPerQuery,
            deferredAt,
          );
          await writeDeferredCapLedger(deferredCapLedgerPath, checkpoint);
          await writeCheckpoint(checkpointPath, checkpoint);
          processed += 1;
          continue;
        }
        if (page.nextPage !== null) {
          throw new Error(
            "Municipal property query returned incomplete pagination",
          );
        }
        if (page.references.length >= options.maxResultsPerQuery) {
          const deferredAt = now();
          checkpoint = deferCappedPropertyQuery(
            checkpoint,
            seedQuery,
            checkpoint.nextQueryIndex,
            page.references.length,
            options.maxResultsPerQuery,
            deferredAt,
          );
          await writeDeferredCapLedger(deferredCapLedgerPath, checkpoint);
          await writeCheckpoint(checkpointPath, checkpoint);
          processed += 1;
          continue;
        }
        if (
          page.reportedCount !== undefined &&
          page.reportedCount !== null &&
          page.reportedCount !== page.references.length
        ) {
          throw new Error("Municipal property query total does not reconcile");
        }
        /** @type {NormalizedBrowardMunicipalPermit[]} */
        const records = [];
        for (const reference of page.references) {
          if (operationCount > 0) await wait(options.delayMs);
          const record = await transport.fetchDetail(reference, query);
          operationCount += 1;
          const expectedKey = `${config.sourceSystem}:${reference.sourceRecordId}`;
          if (
            record.record_key !== expectedKey ||
            record.permit_number !== reference.permitNumber
          ) {
            throw new Error("Municipal property detail identity mismatch");
          }
          records.push(record);
          checkpoint = {
            ...checkpoint,
            status: "running",
            blocker: null,
            nextAttemptAt: null,
            updatedAt: now(),
          };
          await writeCheckpoint(checkpointPath, checkpoint);
        }
        const queryPath = path.join(
          queriesDirectory,
          `query-${String(checkpoint.nextQueryIndex + 1).padStart(8, "0")}.private.jsonl`,
        );
        await writePrivateAtomic(
          queryPath,
          renderMunicipalPermitJsonl(records),
        );
        for (const record of records) {
          const existing = aggregate.byKey.get(record.record_key);
          if (
            existing !== undefined &&
            JSON.stringify(existing) !== JSON.stringify(record)
          ) {
            throw new Error("Municipal property query artifacts conflict");
          }
          if (existing !== undefined) aggregate.duplicateRecordCount += 1;
          else aggregate.byKey.set(record.record_key, record);
        }
        const completedAt = now();
        const nextQueryIndex = checkpoint.nextQueryIndex + 1;
        const deferredCapCount = Object.keys(
          checkpoint.deferredCapItems,
        ).length;
        checkpoint = {
          ...checkpoint,
          nextQueryIndex,
          completedQueries: checkpoint.completedQueries + 1,
          emptyQueries:
            checkpoint.emptyQueries + (records.length === 0 ? 1 : 0),
          recordObservations: checkpoint.recordObservations + records.length,
          uniqueRecords: aggregate.byKey.size,
          status:
            nextQueryIndex === queries.length
              ? deferredCapCount === 0
                ? "complete"
                : "cooling"
              : "running",
          blocker:
            nextQueryIndex === queries.length && deferredCapCount > 0
              ? "source_cap"
              : null,
          nextAttemptAt:
            nextQueryIndex === queries.length && deferredCapCount > 0
              ? earliestDeferredCapRetry(checkpoint.deferredCapItems)
              : null,
          updatedAt: completedAt,
        };
        await writeCheckpoint(checkpointPath, checkpoint);
        processed += 1;
      }
      if (
        checkpoint.nextQueryIndex === queries.length &&
        processed === 0 &&
        Object.keys(checkpoint.deferredCapItems).length > 0
      ) {
        const retryEvaluationAt = now();
        const retryEvaluationMs = Date.parse(retryEvaluationAt);
        const eligibleItems = Object.values(checkpoint.deferredCapItems)
          .filter(
            (item) =>
              item.nextAttemptAt !== null &&
              Date.parse(item.nextAttemptAt) <= retryEvaluationMs,
          )
          .sort(
            (left, right) =>
              left.queryIndex - right.queryIndex ||
              left.queryDigest.localeCompare(right.queryDigest),
          )
          .slice(0, MAX_DEFERRED_CAP_RETRIES_PER_INVOCATION);
        for (const item of eligibleItems) {
          const seedQuery = queries[item.queryIndex];
          if (seedQuery === undefined) {
            throw new Error("Deferred municipal query left the seed plan");
          }
          const query = /** @type {BrowardMunicipalQuery} */ ({
            kind: seedQuery.queryKind,
            value: seedQuery.queryValue,
          });
          checkpoint = {
            ...checkpoint,
            status: "running",
            blocker: null,
            nextAttemptAt: null,
            updatedAt: now(),
          };
          await writeCheckpoint(checkpointPath, checkpoint);
          if (operationCount > 0) await wait(options.delayMs);
          let page;
          try {
            page = await transport.fetchSearchPage(query, 1);
            operationCount += 1;
          } catch (error) {
            if (classifyFailure(error) !== "source_cap") throw error;
            checkpoint = refreshDeferredCapItem(
              checkpoint,
              item,
              options.maxResultsPerQuery,
              now(),
            );
            await writeDeferredCapLedger(deferredCapLedgerPath, checkpoint);
            await writeCheckpoint(checkpointPath, checkpoint);
            continue;
          }
          if (page.nextPage !== null) {
            throw new Error(
              "Municipal deferred query returned incomplete pagination",
            );
          }
          if (page.references.length >= options.maxResultsPerQuery) {
            checkpoint = refreshDeferredCapItem(
              checkpoint,
              item,
              page.references.length,
              now(),
            );
            await writeDeferredCapLedger(deferredCapLedgerPath, checkpoint);
            await writeCheckpoint(checkpointPath, checkpoint);
            continue;
          }
          if (
            page.reportedCount !== undefined &&
            page.reportedCount !== null &&
            page.reportedCount !== page.references.length
          ) {
            throw new Error(
              "Municipal deferred query total does not reconcile",
            );
          }
          /** @type {NormalizedBrowardMunicipalPermit[]} */
          const records = [];
          for (const reference of page.references) {
            if (operationCount > 0) await wait(options.delayMs);
            const record = await transport.fetchDetail(reference, query);
            operationCount += 1;
            const expectedKey = `${config.sourceSystem}:${reference.sourceRecordId}`;
            if (
              record.record_key !== expectedKey ||
              record.permit_number !== reference.permitNumber
            ) {
              throw new Error("Municipal deferred detail identity mismatch");
            }
            records.push(record);
            checkpoint = {
              ...checkpoint,
              status: "running",
              blocker: null,
              nextAttemptAt: null,
              updatedAt: now(),
            };
            await writeCheckpoint(checkpointPath, checkpoint);
          }
          const queryPath = path.join(
            queriesDirectory,
            `query-${String(item.queryIndex + 1).padStart(8, "0")}.private.jsonl`,
          );
          await writePrivateAtomic(
            queryPath,
            renderMunicipalPermitJsonl(records),
          );
          for (const record of records) {
            const existing = aggregate.byKey.get(record.record_key);
            if (
              existing !== undefined &&
              JSON.stringify(existing) !== JSON.stringify(record)
            ) {
              throw new Error("Municipal deferred query artifacts conflict");
            }
            if (existing !== undefined) aggregate.duplicateRecordCount += 1;
            else aggregate.byKey.set(record.record_key, record);
          }
          const deferredCapItems = { ...checkpoint.deferredCapItems };
          delete deferredCapItems[item.queryDigest];
          const completedAt = now();
          checkpoint = {
            ...checkpoint,
            completedQueries: checkpoint.completedQueries + 1,
            emptyQueries:
              checkpoint.emptyQueries + (records.length === 0 ? 1 : 0),
            recordObservations: checkpoint.recordObservations + records.length,
            uniqueRecords: aggregate.byKey.size,
            deferredCapItems,
            status:
              Object.keys(deferredCapItems).length === 0
                ? "complete"
                : earliestDeferredCapRetry(deferredCapItems) === null
                  ? "paused"
                  : "cooling",
            blocker:
              Object.keys(deferredCapItems).length === 0 ? null : "source_cap",
            nextAttemptAt: earliestDeferredCapRetry(deferredCapItems),
            updatedAt: completedAt,
          };
          await writeDeferredCapLedger(deferredCapLedgerPath, checkpoint);
          await writeCheckpoint(checkpointPath, checkpoint);
        }
      }
    } catch (error) {
      const blocker = classifyFailure(error);
      const cooling = blocker === "timeout" || blocker === "source_error";
      checkpoint = {
        ...checkpoint,
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
      checkpoint.nextQueryIndex < queries.length &&
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
      totalQueries: checkpoint.totalQueries,
      completedQueries: checkpoint.completedQueries,
      representedProperties: checkpoint.representedProperties,
      recordObservations: checkpoint.recordObservations,
      uniqueRecordCount: aggregate.byKey.size,
      duplicateRecordCount: aggregate.duplicateRecordCount,
      deferredCapCount: Object.keys(checkpoint.deferredCapItems).length,
      blocker: checkpoint.blocker,
      nextAttemptAt: checkpoint.nextAttemptAt,
    };
  } finally {
    await transport.close();
  }
}

/**
 * Record another bounded observation of an already deferred cap item.
 *
 * @param {MunicipalPropertyCheckpoint} checkpoint - End-of-seed checkpoint.
 * @param {MunicipalDeferredCapItem} item - Existing unresolved item.
 * @param {number} observedResultCount - Bounded source rows observed.
 * @param {string} attemptedAt - ISO retry timestamp.
 * @returns {MunicipalPropertyCheckpoint} Updated unresolved checkpoint.
 */
function refreshDeferredCapItem(
  checkpoint,
  item,
  observedResultCount,
  attemptedAt,
) {
  const attemptCount = item.attemptCount + 1;
  const nextAttemptAt =
    attemptCount >= MAX_DEFERRED_CAP_ATTEMPTS
      ? null
      : new Date(
          Date.parse(attemptedAt) + DEFERRED_CAP_RETRY_DELAY_MS,
        ).toISOString();
  const deferredCapItems = {
    ...checkpoint.deferredCapItems,
    [item.queryDigest]: {
      ...item,
      observedResultCount,
      attemptCount,
      lastAttemptAt: attemptedAt,
      nextAttemptAt,
    },
  };
  return {
    ...checkpoint,
    deferredCapItems,
    status:
      earliestDeferredCapRetry(deferredCapItems) === null
        ? "paused"
        : "cooling",
    blocker: "source_cap",
    nextAttemptAt: earliestDeferredCapRetry(deferredCapItems),
    updatedAt: attemptedAt,
  };
}

/**
 * Parse and filter the deterministic four-column seed.
 *
 * @param {string} text - Complete private seed CSV.
 * @param {string} jurisdictionKey - Underscored municipal configuration key.
 * @returns {MunicipalPropertySeedQuery[]} Exact ordered jurisdiction queries.
 */
export function readMunicipalSeedQueries(text, jurisdictionKey) {
  const lines = text.split(/\r?\n/u).filter((line) => line.length > 0);
  if (lines[0] !== "jurisdiction_key,query_kind,query_value,property_count") {
    throw new Error("Municipal property seed header is invalid");
  }
  const registryKey = jurisdictionKey.replaceAll("_", "-");
  /** @type {MunicipalPropertySeedQuery[]} */
  const queries = [];
  for (const line of lines.slice(1)) {
    const cells = parseCsvLine(line);
    if (cells.length !== 4) {
      throw new Error("Municipal property seed row is malformed");
    }
    const [rowJurisdiction, queryKind, queryValue, rawPropertyCount] = cells;
    if (rowJurisdiction !== registryKey) continue;
    const propertyCount = Number(rawPropertyCount);
    if (
      (queryKind !== "folio" && queryKind !== "address") ||
      queryValue === undefined ||
      queryValue.trim() === "" ||
      !Number.isSafeInteger(propertyCount) ||
      propertyCount < 1
    ) {
      throw new Error("Municipal property seed query is invalid");
    }
    queries.push({
      jurisdictionKey: rowJurisdiction,
      queryKind,
      queryValue,
      propertyCount,
    });
  }
  const identities = queries.map(
    (query) => `${query.queryKind}\u0000${query.queryValue.toUpperCase()}`,
  );
  if (new Set(identities).size !== identities.length) {
    throw new Error("Municipal property seed queries are duplicated");
  }
  return queries;
}

/**
 * Parse one RFC 4180-compatible single-line CSV row.
 *
 * @param {string} line - One line without a newline.
 * @returns {string[]} Decoded cells.
 */
function parseCsvLine(line) {
  /** @type {string[]} */
  const cells = [];
  let value = "";
  let quoted = false;
  for (let index = 0; index < line.length; index += 1) {
    const character = line[index];
    if (quoted) {
      if (character === '"' && line[index + 1] === '"') {
        value += '"';
        index += 1;
      } else if (character === '"') quoted = false;
      else value += character;
    } else if (character === '"') quoted = true;
    else if (character === ",") {
      cells.push(value);
      value = "";
    } else value += character;
  }
  if (quoted) throw new Error("Municipal property seed has an open quote");
  cells.push(value);
  return cells;
}

/**
 * Read all terminal query artifacts by deterministic query index.
 *
 * @param {string} queriesDirectory - Private artifact directory.
 * @param {MunicipalPropertyCheckpoint} checkpoint - Durable processed cursor and unresolved query indexes.
 * @returns {Promise<{byKey:Map<string,NormalizedBrowardMunicipalPermit>,duplicateRecordCount:number}>}
 *   Reconciled unique records and duplicate observations.
 */
async function readCompletedQueryArtifacts(queriesDirectory, checkpoint) {
  /** @type {Map<string, NormalizedBrowardMunicipalPermit>} */
  const byKey = new Map();
  let duplicateRecordCount = 0;
  const deferredIndexes = new Set(
    Object.values(checkpoint.deferredCapItems).map((item) => item.queryIndex),
  );
  for (let index = 0; index < checkpoint.nextQueryIndex; index += 1) {
    if (deferredIndexes.has(index)) continue;
    const queryPath = path.join(
      queriesDirectory,
      `query-${String(index + 1).padStart(8, "0")}.private.jsonl`,
    );
    const text = await readFile(queryPath, "utf8");
    for (const line of text.split(/\r?\n/u)) {
      if (line.trim() === "") continue;
      const parsed = /** @type {unknown} */ (JSON.parse(line));
      if (!isRecord(parsed) || typeof parsed.record_key !== "string") {
        throw new Error("Municipal property query artifact is malformed");
      }
      const record = /** @type {NormalizedBrowardMunicipalPermit} */ (parsed);
      const existing = byKey.get(record.record_key);
      if (
        existing !== undefined &&
        JSON.stringify(existing) !== JSON.stringify(record)
      ) {
        throw new Error("Municipal property query artifacts conflict");
      }
      if (existing !== undefined) duplicateRecordCount += 1;
      else byKey.set(record.record_key, record);
    }
  }
  return { byKey, duplicateRecordCount };
}

/**
 * Read or initialize one immutable seed-bound checkpoint.
 *
 * @param {string} checkpointPath - Private checkpoint path.
 * @param {ReturnType<typeof getBrowardMunicipalPermitConfig>} config - Source.
 * @param {readonly MunicipalPropertySeedQuery[]} queries - Exact plan.
 * @param {string} seedSha256 - Complete seed bytes digest.
 * @param {string} startedAt - Initial ISO timestamp.
 * @returns {Promise<MunicipalPropertyCheckpoint>} Validated checkpoint.
 */
async function readOrCreateCheckpoint(
  checkpointPath,
  config,
  queries,
  seedSha256,
  startedAt,
) {
  const queryPlanSha256 = sha256(
    JSON.stringify(
      queries.map((query) => ({
        kind: query.queryKind,
        value: query.queryValue,
        propertyCount: query.propertyCount,
      })),
    ),
  );
  const representedProperties = queries.reduce(
    (sum, query) => sum + query.propertyCount,
    0,
  );
  const coverageBoundary =
    queries[0]?.queryKind === "folio"
      ? /** @type {const} */ ("bcpa_property_first_folio")
      : /** @type {const} */ ("bcpa_property_first_address");
  try {
    const parsed = /** @type {unknown} */ (
      JSON.parse(await readFile(checkpointPath, "utf8"))
    );
    if (
      !isRecord(parsed) ||
      parsed.schemaVersion !== CHECKPOINT_SCHEMA_VERSION ||
      parsed.jurisdictionKey !== config.key ||
      parsed.sourceSystem !== config.sourceSystem ||
      parsed.seedSha256 !== seedSha256 ||
      parsed.queryPlanSha256 !== queryPlanSha256 ||
      parsed.totalQueries !== queries.length ||
      parsed.representedProperties !== representedProperties ||
      !Number.isInteger(parsed.nextQueryIndex) ||
      /** @type {number} */ (parsed.nextQueryIndex) < 0 ||
      /** @type {number} */ (parsed.nextQueryIndex) > queries.length
    ) {
      throw new Error(
        "Existing municipal property checkpoint does not match seed",
      );
    }
    const deferredCapItems =
      parsed.deferredCapItems === undefined ? {} : parsed.deferredCapItems;
    if (
      !isRecord(deferredCapItems) ||
      !Number.isInteger(parsed.completedQueries) ||
      /** @type {number} */ (parsed.completedQueries) < 0
    ) {
      throw new Error("Existing municipal property deferrals are malformed");
    }
    const deferredIndexes = new Set();
    for (const [digest, rawItem] of Object.entries(deferredCapItems)) {
      if (
        !/^[a-f0-9]{64}$/u.test(digest) ||
        !isValidDeferredCapItem(
          rawItem,
          digest,
          queries,
          seedSha256,
          queryPlanSha256,
          /** @type {number} */ (parsed.nextQueryIndex),
        )
      ) {
        throw new Error("Existing municipal property deferral is invalid");
      }
      deferredIndexes.add(rawItem.queryIndex);
    }
    if (
      deferredIndexes.size !== Object.keys(deferredCapItems).length ||
      /** @type {number} */ (parsed.completedQueries) + deferredIndexes.size !==
        /** @type {number} */ (parsed.nextQueryIndex)
    ) {
      throw new Error("Municipal property processed queries do not reconcile");
    }
    return /** @type {MunicipalPropertyCheckpoint} */ ({
      ...parsed,
      deferredCapItems,
    });
  } catch (error) {
    if (!isNodeError(error) || error.code !== "ENOENT") throw error;
  }
  const checkpoint = {
    schemaVersion: CHECKPOINT_SCHEMA_VERSION,
    jurisdictionKey: config.key,
    sourceSystem: config.sourceSystem,
    seedSha256,
    queryPlanSha256,
    coverageBoundary,
    totalQueries: queries.length,
    representedProperties,
    nextQueryIndex: 0,
    completedQueries: 0,
    emptyQueries: 0,
    recordObservations: 0,
    uniqueRecords: 0,
    deferredCapItems: {},
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
 * Advance past one capped source item without marking it terminal.
 *
 * The checkpoint and companion ledger retain only hashes, aggregate counts,
 * and bounded retry metadata. Query text never enters either artifact.
 *
 * @param {MunicipalPropertyCheckpoint} checkpoint - Current immutable-plan checkpoint.
 * @param {MunicipalPropertySeedQuery} query - Private query used only to derive its digest.
 * @param {number} queryIndex - Stable zero-based seed position.
 * @param {number} observedResultCount - Bounded source rows observed.
 * @param {number} exclusiveCap - Threshold that cannot establish completeness.
 * @param {string} deferredAt - ISO source observation time.
 * @returns {MunicipalPropertyCheckpoint} Advanced partial-coverage checkpoint.
 */
function deferCappedPropertyQuery(
  checkpoint,
  query,
  queryIndex,
  observedResultCount,
  exclusiveCap,
  deferredAt,
) {
  const queryDigest = municipalPropertyQueryDigest(
    checkpoint.queryPlanSha256,
    query,
    queryIndex,
  );
  const previous = checkpoint.deferredCapItems[queryDigest];
  const attemptCount = (previous?.attemptCount ?? 0) + 1;
  const nextAttemptAt =
    attemptCount >= MAX_DEFERRED_CAP_ATTEMPTS
      ? null
      : new Date(
          Date.parse(deferredAt) + DEFERRED_CAP_RETRY_DELAY_MS,
        ).toISOString();
  /** @type {MunicipalDeferredCapItem} */
  const item = {
    schemaVersion: DEFERRED_CAP_SCHEMA_VERSION,
    seedSha256: checkpoint.seedSha256,
    queryPlanSha256: checkpoint.queryPlanSha256,
    queryIndex,
    queryDigest,
    representedProperties: query.propertyCount,
    reason: "client_all_exclusive_cap",
    observedResultCount,
    exclusiveCap,
    attemptCount,
    firstDeferredAt: previous?.firstDeferredAt ?? deferredAt,
    lastAttemptAt: deferredAt,
    nextAttemptAt,
  };
  const deferredCapItems = {
    ...checkpoint.deferredCapItems,
    [queryDigest]: item,
  };
  const nextQueryIndex = queryIndex + 1;
  const exhausted = nextQueryIndex === checkpoint.totalQueries;
  return {
    ...checkpoint,
    deferredCapItems,
    nextQueryIndex,
    status: exhausted ? "cooling" : "running",
    blocker: exhausted ? "source_cap" : null,
    nextAttemptAt: exhausted
      ? earliestDeferredCapRetry(deferredCapItems)
      : null,
    updatedAt: deferredAt,
  };
}

/**
 * Derive a stable opaque identity for one private seed item.
 *
 * @param {string} queryPlanSha256 - Immutable plan identity.
 * @param {MunicipalPropertySeedQuery} query - Private seed query.
 * @param {number} queryIndex - Stable zero-based plan position.
 * @returns {string} Lowercase SHA-256 digest.
 */
function municipalPropertyQueryDigest(queryPlanSha256, query, queryIndex) {
  return sha256(
    JSON.stringify({
      queryPlanSha256,
      queryIndex,
      queryKind: query.queryKind,
      queryValue: query.queryValue.toUpperCase(),
    }),
  );
}

/**
 * Validate one persisted safe deferral against the immutable private plan.
 *
 * @param {unknown} value - Candidate ledger item.
 * @param {string} digest - Object key expected to match the item digest.
 * @param {readonly MunicipalPropertySeedQuery[]} queries - Immutable plan.
 * @param {string} seedSha256 - Expected seed hash.
 * @param {string} queryPlanSha256 - Expected plan hash.
 * @param {number} nextQueryIndex - Exclusive processed cursor.
 * @returns {value is MunicipalDeferredCapItem} True for a reconciled safe item.
 */
function isValidDeferredCapItem(
  value,
  digest,
  queries,
  seedSha256,
  queryPlanSha256,
  nextQueryIndex,
) {
  if (!isRecord(value)) return false;
  const queryIndex = value.queryIndex;
  if (
    value.schemaVersion !== DEFERRED_CAP_SCHEMA_VERSION ||
    value.seedSha256 !== seedSha256 ||
    value.queryPlanSha256 !== queryPlanSha256 ||
    value.queryDigest !== digest ||
    !Number.isInteger(queryIndex) ||
    /** @type {number} */ (queryIndex) < 0 ||
    /** @type {number} */ (queryIndex) >= nextQueryIndex ||
    !Number.isSafeInteger(value.representedProperties) ||
    /** @type {number} */ (value.representedProperties) < 1 ||
    value.reason !== "client_all_exclusive_cap" ||
    !Number.isSafeInteger(value.observedResultCount) ||
    /** @type {number} */ (value.observedResultCount) < 2 ||
    !Number.isSafeInteger(value.exclusiveCap) ||
    /** @type {number} */ (value.exclusiveCap) < 2 ||
    /** @type {number} */ (value.observedResultCount) <
      /** @type {number} */ (value.exclusiveCap) ||
    !Number.isInteger(value.attemptCount) ||
    /** @type {number} */ (value.attemptCount) < 1 ||
    /** @type {number} */ (value.attemptCount) > MAX_DEFERRED_CAP_ATTEMPTS ||
    typeof value.firstDeferredAt !== "string" ||
    !Number.isFinite(Date.parse(value.firstDeferredAt)) ||
    typeof value.lastAttemptAt !== "string" ||
    !Number.isFinite(Date.parse(value.lastAttemptAt)) ||
    (value.nextAttemptAt !== null &&
      (typeof value.nextAttemptAt !== "string" ||
        !Number.isFinite(Date.parse(value.nextAttemptAt))))
  ) {
    return false;
  }
  const query = queries[/** @type {number} */ (queryIndex)];
  return (
    query !== undefined &&
    value.representedProperties === query.propertyCount &&
    municipalPropertyQueryDigest(
      queryPlanSha256,
      query,
      /** @type {number} */ (queryIndex),
    ) === digest
  );
}

/**
 * Return the earliest scheduled retry across unresolved cap items.
 *
 * @param {Readonly<Record<string,MunicipalDeferredCapItem>>} items - Safe unresolved items.
 * @returns {string | null} Earliest ISO retry time, or null when exhausted.
 */
function earliestDeferredCapRetry(items) {
  const scheduled = Object.values(items)
    .map((item) => item.nextAttemptAt)
    .filter((value) => value !== null)
    .sort((left, right) => left.localeCompare(right));
  return scheduled[0] ?? null;
}

/**
 * Persist the safe deferred-cap ledger as deterministic owner-only JSONL.
 *
 * @param {string} ledgerPath - Private ledger destination.
 * @param {MunicipalPropertyCheckpoint} checkpoint - Source checkpoint.
 * @returns {Promise<void>} Resolves after atomic replacement.
 */
async function writeDeferredCapLedger(ledgerPath, checkpoint) {
  const rows = Object.values(checkpoint.deferredCapItems).sort(
    (left, right) =>
      left.queryIndex - right.queryIndex ||
      left.queryDigest.localeCompare(right.queryDigest),
  );
  await writePrivateAtomic(
    ledgerPath,
    rows.length === 0
      ? ""
      : `${rows.map((row) => JSON.stringify(row)).join("\n")}\n`,
  );
}

/**
 * Convert a source failure to an allowlisted aggregate reason.
 *
 * @param {unknown} error - Caught source/transport failure.
 * @returns {Exclude<MunicipalPropertyCheckpoint["blocker"], null>} Safe reason.
 */
function classifyFailure(error) {
  if (!(error instanceof Error)) return "source_error";
  if (/row limit|result limit|source cap/iu.test(error.message)) {
    return "source_cap";
  }
  if (/timed out|timeout|signal timed out/iu.test(error.message)) {
    return "timeout";
  }
  if (/pagination|total does not reconcile/iu.test(error.message)) {
    return "incomplete_pagination";
  }
  return "source_error";
}

/**
 * Write one complete checkpoint atomically.
 *
 * @param {string} checkpointPath - Final private path.
 * @param {MunicipalPropertyCheckpoint} checkpoint - Complete state.
 * @returns {Promise<void>} Resolves after replacement.
 */
async function writeCheckpoint(checkpointPath, checkpoint) {
  await writePrivateAtomic(
    checkpointPath,
    `${JSON.stringify(checkpoint, null, 2)}\n`,
  );
}

/**
 * Write one owner-only UTF-8 artifact.
 *
 * @param {string} filePath - Final private path.
 * @param {string} content - Complete content.
 * @returns {Promise<void>} Resolves after atomic replacement.
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
 * @param {string} flag - Fixed option label.
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
 * Hash deterministic content.
 *
 * @param {string} value - Complete content.
 * @returns {string} Lowercase SHA-256.
 */
function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

/**
 * Narrow parsed JSON to a non-array object.
 *
 * @param {unknown} value - Candidate value.
 * @returns {value is Record<string, unknown>} True for plain records.
 */
function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/**
 * Narrow a caught filesystem error to a Node error code.
 *
 * @param {unknown} value - Caught value.
 * @returns {value is Error & {code:string}} True when a code exists.
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
  runMunicipalPropertyEnumeration(
    parseMunicipalPropertyEnumerationOptions(process.argv.slice(2)),
  )
    .then((summary) => {
      console.log(
        JSON.stringify({
          event: "broward_municipal_property_enumeration_finished",
          ...summary,
        }),
      );
    })
    .catch((error) => {
      console.error(
        JSON.stringify({
          event: "broward_municipal_property_enumeration_failed",
          message: error instanceof Error ? error.message : "Unknown error",
        }),
      );
      process.exitCode = 1;
    });
}
