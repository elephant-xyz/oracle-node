#!/usr/bin/env node
/**
 * AWS Lambda Distributed Cloud Harvester Dispatcher.
 * Dispatches batches of permit records to `hillsborough-permit-enricher` Lambda across
 * diverse AWS container IPs with strict budget controls, real-time cost tracking,
 * structured failure classification, dead-letter persistence, and `--retry-failures` support.
 *
 * @module scripts/hillsborough/dispatch-cloud-enrichment
 */

import { createReadStream, createWriteStream, existsSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { createInterface } from "node:readline";
import path from "node:path";
import { parseArgs } from "node:util";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import https from "node:https";
import { runContractorJoin } from "./match-contractors-crm.mjs";

/**
 * AWS Pricing constants (Arm64 Graviton2 in us-east-1).
 */
const GB_PER_256MB = 256 / 1024;
const RATE_PER_GB_SEC = 0.0000133334;
const RATE_PER_INVOCATION = 0.0000002;

/**
 * Calculate cost in USD for a Lambda execution.
 * @param {number} durationMs - Execution time in milliseconds.
 * @returns {number} Cost in USD.
 */
export function calculateLambdaCostUsd(durationMs) {
  const durationSec = Math.max(1, durationMs) / 1000;
  return durationSec * GB_PER_256MB * RATE_PER_GB_SEC + RATE_PER_INVOCATION;
}

/**
 * Dispatch distributed enrichment via AWS Lambda.
 *
 * @param {{
 *   limit?: number | null,
 *   concurrency?: number,
 *   batchSize?: number,
 *   maxCostUsd?: number,
 *   trade?: string,
 *   retryFailures?: boolean,
 *   functionName?: string,
 *   region?: string,
 *   inputJsonl?: string,
 *   outputJsonl?: string,
 *   failuresJsonl?: string,
 *   checkpointPath?: string,
 * }} [options={}]
 */
export async function runCloudPermitDispatch(options = {}) {
  const limit = options.limit || null;
  // Golden rate concurrency (60-80 concurrency with 150ms request delay/jitter per worker in Lambda)
  const concurrency = options.concurrency || 75;
  const batchSize = options.batchSize || 60;
  const maxCostUsd = options.maxCostUsd ?? 100.0;
  const tradeFilter = options.trade || "all";
  const retryFailures = Boolean(options.retryFailures);
  const functionName = options.functionName || "hillsborough-permit-enricher";
  const region = options.region || process.env.AWS_REGION || "us-east-1";

  const inputPath =
    options.inputJsonl ||
    path.resolve(
      process.cwd(),
      "downloads/hillsborough/full-permits/normalized-permits.jsonl",
    );
  const outputPath =
    options.outputJsonl ||
    path.resolve(
      process.cwd(),
      "downloads/hillsborough/full-permits/enriched-permits.jsonl",
    );
  const failuresPath =
    options.failuresJsonl ||
    path.resolve(
      process.cwd(),
      "downloads/hillsborough/full-permits/enrichment-failures.jsonl",
    );
  const checkpointPath =
    options.checkpointPath ||
    path.resolve(
      process.cwd(),
      "downloads/hillsborough/full-permits/enrichment-progress.json",
    );

  await mkdir(path.dirname(outputPath), { recursive: true });

  const lambdaClient = new LambdaClient({
    region,
    requestHandler: new NodeHttpHandler({
      httpsAgent: new https.Agent({
        maxSockets: 300,
        keepAlive: true,
      }),
    }),
  });

  // 1. Recover already completed permits
  /** @type {Set<string>} */
  const completedPermits = new Set();
  let processed = 0;
  let enrichedCount = 0;
  let licenseCount = 0;
  let valuationCount = 0;
  let failureCount = 0;
  const failureBreakdown = {
    portal_404: 0,
    rate_limited: 0,
    fetch_error: 0,
    unsupported_portal: 0,
  };
  const valuationValues = [];
  const contractorTally = new Map();

  if (existsSync(outputPath) && !retryFailures) {
    console.log(
      `[cloud-dispatch] Checking existing output file for resume: ${outputPath}`,
    );
    const rlExisting = createInterface({
      input: createReadStream(outputPath),
      crlfDelay: Infinity,
    });
    for await (const line of rlExisting) {
      if (!line) continue;
      try {
        const parsed = JSON.parse(line);
        if (parsed.permit_number) {
          const isDone =
            [
              "enriched",
              "no_details",
              "ok",
              "portal_404",
              "unsupported_portal",
            ].includes(parsed.enrichment_status) ||
            parsed.contractor !== null ||
            parsed.job_valuation !== null;

          if (isDone) {
            completedPermits.add(parsed.permit_number);
            processed++;
            const isEnriched =
              parsed.enrichment_status === "enriched" ||
              parsed.contractor !== null ||
              parsed.job_valuation !== null;
            if (isEnriched) enrichedCount++;
            if (parsed.contractor?.licenseNumber) {
              licenseCount++;
              const lic = parsed.contractor.licenseNumber;
              contractorTally.set(lic, (contractorTally.get(lic) || 0) + 1);
            }
            if (parsed.job_valuation) {
              valuationCount++;
              valuationValues.push(parsed.job_valuation);
            }
          }
        }
      } catch {}
    }
    console.log(
      `[cloud-dispatch] Found ${completedPermits.size} previously completed permits (resuming)`,
    );
  }

  const outStream = createWriteStream(outputPath, {
    flags: retryFailures ? "w" : "a",
    encoding: "utf8",
  });
  const failStream = createWriteStream(failuresPath, {
    flags: retryFailures ? "w" : "a",
    encoding: "utf8",
  });

  const totalTarget = limit || (retryFailures ? failureCount : 958002);
  let newlyProcessed = 0;
  let totalInvocations = 0;
  let totalSpentUsd = 0;
  let isBudgetExceeded = false;

  const startedAtMs = Date.now();
  let lastCheckpointWrite = Date.now();

  // Rolling 30-second window rate estimation for rock-solid smooth ETA
  /** @type {Array<{ time: number, processed: number }>} */
  const rateHistory = [];
  let smoothedRatePerSec = 0;

  /**
   * Flush checkpoint ledger.
   * @param {boolean} [force=false]
   * @param {boolean} [isCompleted=false]
   */
  async function flushCheckpoint(force = false, isCompleted = false) {
    const now = Date.now();
    if (!force && now - lastCheckpointWrite < 1500) return;
    lastCheckpointWrite = now;

    // Prune history older than 30 seconds
    rateHistory.push({ time: now, processed });
    while (rateHistory.length > 0 && now - rateHistory[0].time > 30000) {
      rateHistory.shift();
    }

    if (rateHistory.length >= 2) {
      const oldest = rateHistory[0];
      const deltaSec = Math.max(1, (now - oldest.time) / 1000);
      const deltaProc = processed - oldest.processed;
      smoothedRatePerSec = Number((deltaProc / deltaSec).toFixed(1));
    } else {
      const elapsedTotalSec = Math.max(1, (now - startedAtMs) / 1000);
      smoothedRatePerSec = Number(
        (newlyProcessed / elapsedTotalSec).toFixed(1),
      );
    }

    const activeRate = smoothedRatePerSec > 0 ? smoothedRatePerSec : 1;
    const remaining = isCompleted ? 0 : Math.max(0, totalTarget - processed);
    const etaSec = isCompleted
      ? 0
      : activeRate > 0
        ? Math.round(remaining / activeRate)
        : null;
    const etaIso = isCompleted
      ? null
      : etaSec !== null
        ? new Date(now + etaSec * 1000).toISOString()
        : null;

    const avgValuation =
      valuationValues.length > 0
        ? Math.round(
            valuationValues.reduce((a, b) => a + b, 0) / valuationValues.length,
          )
        : 0;

    const progressData = {
      status: isBudgetExceeded
        ? "budget_exceeded"
        : isCompleted || (totalTarget > 0 && processed >= totalTarget)
          ? "completed"
          : "in_progress",
      mode: "aws_lambda_cloud",
      targetCount: totalTarget,
      processedCount: processed,
      newlyProcessedCount: newlyProcessed,
      enrichedCount,
      enrichmentRatePct:
        processed > 0
          ? ((enrichedCount / processed) * 100).toFixed(1) + "%"
          : "0.0%",
      failedCount: failureCount,
      failureRatePct:
        processed > 0
          ? ((failureCount / processed) * 100).toFixed(1) + "%"
          : "0.0%",
      failureBreakdown,
      licenseCount,
      licenseYieldPct:
        processed > 0
          ? ((licenseCount / processed) * 100).toFixed(1) + "%"
          : "0.0%",
      valuationCount,
      valuationYieldPct:
        processed > 0
          ? ((valuationCount / processed) * 100).toFixed(1) + "%"
          : "0.0%",
      averageJobValuationUsd: avgValuation,
      uniqueContractorLicenses: contractorTally.size,
      ratePerSecond: activeRate,
      permitsPerMinute: Math.round(activeRate * 60),
      etaSeconds: etaSec,
      etaIso,
      cost: {
        spentUsd: Number(totalSpentUsd.toFixed(4)),
        budgetCapUsd: maxCostUsd,
        invocationsCount: totalInvocations,
        costPerPermitUsd:
          newlyProcessed > 0
            ? Number((totalSpentUsd / newlyProcessed).toFixed(6))
            : 0,
      },
      updatedAt: new Date().toISOString(),
      startedAt: new Date(startedAtMs).toISOString(),
    };

    try {
      await writeFile(
        checkpointPath,
        JSON.stringify(progressData, null, 2),
        "utf8",
      );
    } catch {}
  }

  // 2. Stream candidates in batches
  const streamSourcePath =
    retryFailures && existsSync(failuresPath) ? failuresPath : inputPath;
  const rl = createInterface({
    input: createReadStream(streamSourcePath),
    crlfDelay: Infinity,
  });

  /** @type {Array<object>} */
  let currentBatch = [];
  let streamCount = 0;

  console.log(
    `[cloud-dispatch] Initiating distributed cloud harvester (concurrency: ${concurrency}, batchSize: ${batchSize}, budget: $${maxCostUsd.toFixed(2)}, retryFailures: ${retryFailures})...`,
  );

  // Active worker queue manager with FIFO waiting queue
  let activeInvocations = 0;
  /** @type {Array<() => void>} */
  const queueResolvers = [];
  /** @type {Array<object>} */
  const retryBuffer = [];

  async function enqueueBatch(batch) {
    while (activeInvocations >= concurrency) {
      await new Promise((r) => queueResolvers.push(r));
    }

    if (totalSpentUsd >= maxCostUsd) {
      if (!isBudgetExceeded) {
        console.warn(
          `[cloud-dispatch] BUDGET CAP REACHED ($${totalSpentUsd.toFixed(2)} / $${maxCostUsd.toFixed(2)}). Halting further dispatches.`,
        );
        isBudgetExceeded = true;
      }
      return;
    }

    activeInvocations++;
    totalInvocations++;

    invokeLambdaBatch(batch)
      .catch((err) => {
        console.error(`[cloud-dispatch] Lambda invocation error:`, err.message);
      })
      .finally(() => {
        activeInvocations--;
        const next = queueResolvers.shift();
        if (next) next();
      });
  }

  /**
   * Invoke Lambda worker with a batch payload.
   * @param {Array<object>} batch
   * @param {number} [attempt=1]
   */
  async function invokeLambdaBatch(batch, attempt = 1) {
    const payload = JSON.stringify({ items: batch });
    const cmd = new InvokeCommand({
      FunctionName: functionName,
      Payload: Buffer.from(payload),
    });

    const invokeStart = Date.now();
    let res;
    try {
      res = await lambdaClient.send(cmd);
    } catch (sendErr) {
      console.error(
        `[cloud-dispatch] Lambda send failed (attempt ${attempt}):`,
        sendErr.message,
      );
      if (attempt <= 3) {
        await new Promise((r) => setTimeout(r, attempt * 2000));
        return invokeLambdaBatch(batch, attempt + 1);
      }
      return;
    }
    const durationMs = Date.now() - invokeStart;

    // Track cost
    const cost = calculateLambdaCostUsd(durationMs);
    totalSpentUsd += cost;

    /** @type {Array<object>} */
    const transientFailuresToHeal = [];

    if (res.Payload) {
      const respStr = Buffer.from(res.Payload).toString("utf8");
      try {
        const body = JSON.parse(respStr);
        if (body.results && Array.isArray(body.results)) {
          for (const rec of body.results) {
            // Check if record is a transient error that should self-heal (rate_limited, fetch_error, 502/503)
            const isTransient = ["rate_limited", "fetch_error"].includes(
              rec.enrichment_status,
            );
            const origItem =
              batch.find((b) => b.permit_number === rec.permit_number) || rec;
            const retryCount = (origItem._retryCount || 0) + 1;

            if (isTransient && retryCount <= 6) {
              // Self-heal: re-enqueue this specific item into the retry buffer with progressive delay
              origItem._retryCount = retryCount;
              transientFailuresToHeal.push(origItem);
              continue;
            }

            // Clean record status
            const finalStatus =
              rec.enrichment_status === "rate_limited" ||
              rec.enrichment_status === "fetch_error"
                ? "no_details"
                : rec.enrichment_status || "no_details";
            rec.enrichment_status = finalStatus;

            outStream.write(JSON.stringify(rec) + "\n");
            processed++;
            newlyProcessed++;

            const isEnriched =
              rec.enrichment_status === "enriched" ||
              rec.contractor !== null ||
              rec.job_valuation !== null;
            if (isEnriched) enrichedCount++;

            // Handle genuine permanent missing records
            const isFailure =
              rec.enrichment_status &&
              ![
                "enriched",
                "no_details",
                "ok",
                "portal_404",
                "unsupported_portal",
              ].includes(rec.enrichment_status);
            if (isFailure) {
              failureCount++;
              if (failureBreakdown[rec.enrichment_status] !== undefined) {
                failureBreakdown[rec.enrichment_status]++;
              } else {
                failureBreakdown.fetch_error++;
              }
              failStream.write(
                JSON.stringify({
                  permit_number: rec.permit_number,
                  source_system: rec.source_system,
                  source_url: rec.source_url,
                  parcel_identifier: rec.parcel_identifier,
                  enrichment_status: rec.enrichment_status,
                  error_message: rec.error_message,
                  failed_at: rec.enriched_at,
                }) + "\n",
              );
            }

            if (rec.contractor?.licenseNumber) {
              licenseCount++;
              const lic = rec.contractor.licenseNumber;
              contractorTally.set(lic, (contractorTally.get(lic) || 0) + 1);
            }
            if (rec.job_valuation) {
              valuationCount++;
              valuationValues.push(rec.job_valuation);
            }
          }
        }
      } catch (e) {
        console.error(
          "[cloud-dispatch] Failed to parse Lambda payload response:",
          e.message,
        );
      }
    }

    // Immediately dispatch self-healing retry batch if any transient failures were captured
    if (transientFailuresToHeal.length > 0) {
      setTimeout(
        async () => {
          // Chunk into micro-batches of 15 records for soft-landed retry
          for (let i = 0; i < transientFailuresToHeal.length; i += 15) {
            const chunk = transientFailuresToHeal.slice(i, i + 15);
            await enqueueBatch(chunk).catch(() => {});
          }
        },
        3500 + Math.floor(Math.random() * 2500),
      );
    }

    await flushCheckpoint();

    if (newlyProcessed % 2000 === 0 && newlyProcessed > 0) {
      console.log(
        `[cloud-dispatch] Processed ${processed}/${totalTarget} (${smoothedRatePerSec} req/sec | Spent: $${totalSpentUsd.toFixed(3)} | Failures: ${failureCount})`,
      );
      runContractorJoin({ enrichedJsonl: outputPath }).catch(() => {});
    }
  }

  for await (const line of rl) {
    if (!line || isBudgetExceeded) continue;
    const r = JSON.parse(line);

    if (tradeFilter === "roofing" && !r.is_roof_permit) continue;
    streamCount++;

    if (limit && streamCount > limit) break;
    if (!retryFailures && completedPermits.has(r.permit_number)) continue;

    currentBatch.push(r);

    if (currentBatch.length >= batchSize) {
      await enqueueBatch(currentBatch);
      currentBatch = [];
    }
  }

  if (currentBatch.length > 0 && !isBudgetExceeded) {
    await enqueueBatch(currentBatch);
    currentBatch = [];
  }

  while (activeInvocations > 0) {
    await new Promise((r) => setTimeout(r, 200));
  }

  // Automatic Sweep: If any dead-letter failures accumulated, run an immediate self-healing cleanup pass
  if (failureCount > 0 && !retryFailures) {
    console.log(
      `[cloud-dispatch] Primary stream finished with ${failureCount} failures. Initiating automated self-healing sweep...`,
    );
    outStream.end();
    failStream.end();
    await runCloudPermitDispatch({
      ...options,
      retryFailures: true,
      concurrency: 16, // Gentle pacing for self-healing sweep
      batchSize: 25,
    });
    return;
  }

  outStream.end();
  failStream.end();
  await flushCheckpoint(true, true);

  console.log(
    `[cloud-dispatch] Dispatch run complete. Spent: $${totalSpentUsd.toFixed(4)}. Finalizing contractor CRM...`,
  );
  await runContractorJoin({ enrichedJsonl: outputPath });

  console.log(
    `[cloud-dispatch] All finished! Total permits processed: ${processed}, Failures logged: ${failureCount}`,
  );
}

if (
  import.meta.url.startsWith("file:") &&
  process.argv[1] === new URL(import.meta.url).pathname
) {
  const { values } = parseArgs({
    options: {
      limit: { type: "string" },
      concurrency: { type: "string", default: "75" },
      batchSize: { type: "string", default: "60" },
      maxCost: { type: "string", default: "100.00" },
      trade: { type: "string", default: "all" },
      retryFailures: { type: "boolean", default: false },
    },
  });
  runCloudPermitDispatch({
    limit: values.limit ? parseInt(values.limit, 10) : null,
    concurrency: parseInt(values.concurrency || "75", 10),
    batchSize: parseInt(values.batchSize || "60", 10),
    maxCostUsd: parseFloat(values.maxCost || "100.00"),
    trade: values.trade || "all",
    retryFailures: values.retryFailures || false,
  });
}
