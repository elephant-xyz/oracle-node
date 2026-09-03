// @ts-check

/**
 * Aggregate-only observation and ETA logic for the ten actively enumerating
 * Broward permit routes. This module never reads source artifacts or Neon
 * record tables; callers provide reconciled checkpoint aggregates and a
 * bounded process snapshot.
 */

import { execFile } from "node:child_process";

const MINIMUM_ACTIVITY_WINDOW_MS = 60_000;
const MINIMUM_ETA_WINDOW_MS = 5 * 60_000;
const MAXIMUM_HISTORY_WINDOW_MS = 60 * 60_000;
const CHECKPOINT_STALE_AFTER_MS = 5 * 60_000;
const MAXIMUM_DETAIL_PROCESS_AGE_SECONDS = 15 * 60;
const RATE_SEGMENT_COUNT = 3;
const MAXIMUM_RATE_VARIABILITY_RATIO = 3;

/**
 * @typedef {"full" | "property_first"} ActiveEnumerationMethod
 * @typedef {"municipal_property" | "municipal_type" | "bcs_posse" | "citizenserve"} ActiveEnumerationFamily
 * @typedef {"local_checkpoint" | "durable_route_checkpoint"} ActiveEnumerationCountSource
 * @typedef {"running" | "cooling" | "paused" | "complete" | "stalled"} ActiveEnumerationState
 * @typedef {"warming_up" | "work_units_advanced" | "checkpoint_updated" | "stationary"} ActiveEnumerationCheckpointActivity
 * @typedef {"complete" | "rate_stable" | "dashboard_snapshot_stale" | "worker_not_running" | "checkpoint_stale" | "detail_activity" | "variable_detail_loop" | "observation_window_short" | "no_checkpoint_movement" | "rate_variability_high" | "work_unit_total_changed"} ActiveEnumerationEtaReason
 *
 * @typedef {object} ActiveEnumerationRouteDefinition
 * @property {string} key - Stable public route key.
 * @property {string} jurisdiction - Public jurisdiction label.
 * @property {ActiveEnumerationMethod} method - Enumeration scope.
 * @property {ActiveEnumerationFamily} family - Executable source family.
 * @property {ActiveEnumerationCountSource} countSource
 *   Whether record counts come from local capture or a durable route row.
 * @property {string} processScript - Exact runner script basename.
 * @property {string} processJurisdictionKey
 *   Exact jurisdiction token expected in the runner arguments.
 *
 * @typedef {object} EnumerationWorkerAggregate
 * @property {string} source - Public jurisdiction label.
 * @property {string} family - Existing dashboard family.
 * @property {"not_started" | "running" | "cooling_down" | "paused" | "complete"} status
 * @property {number} completedWindows - Durable completed work units.
 * @property {number} pendingWindows - Reconciled remaining work units.
 * @property {number} totalWindows - Immutable work-unit denominator.
 * @property {number} accessibleRecords - Reconciled record aggregate.
 * @property {number} sourceMissingRecords - Explicit inaccessible source rows.
 * @property {number} deferredCapCount - Durable unresolved cap ledger size.
 * @property {string | null} updatedAt - Last durable checkpoint timestamp.
 *
 * @typedef {object} ActiveEnumerationProcessSnapshot
 * @property {boolean} available - Whether the bounded process read succeeded.
 * @property {ReadonlySet<string>} routeKeys - Definitions with a live runner.
 * @property {ReadonlySet<string>} detailRouteKeys
 *   Definitions with a live bounded probe or browser descendant no older than
 *   fifteen minutes.
 * @property {ReadonlyMap<string,string>} supervisorNotBeforeByKey
 *   Valid operator retry boundaries parsed from live municipal supervisors.
 *
 * @typedef {object} ActiveEnumerationProcessEntry
 * @property {number} pid - Operating-system process identifier used in-memory only.
 * @property {number} parentPid - Parent identifier used only for bounded ancestry.
 * @property {number} elapsedSeconds - Current process age from the OS snapshot.
 * @property {string} command - Private command line retained only in-memory.
 *
 * @typedef {object} ActiveEnumerationObservation
 * @property {number} observedUnits - Completed-unit delta in this window.
 * @property {number} windowSeconds - Wall-clock observation duration.
 * @property {number | null} unitsPerHour - Observed completed units per hour.
 * @property {number | null} variabilityRatio
 *   Maximum divided by minimum segment rate when derivable.
 *
 * @typedef {object} ActiveEnumerationEta
 * @property {"estimate" | "unknown" | "complete"} kind - ETA validity.
 * @property {number | null} estimatedHours - Point estimate when valid.
 * @property {number | null} lowHours - Optimistic observed-rate bound.
 * @property {number | null} highHours - Conservative observed-rate bound.
 * @property {ActiveEnumerationEtaReason} reason - Validity or refusal reason.
 *
 * @typedef {object} ActiveEnumerationWorker
 * @property {string} jurisdiction - Public jurisdiction label.
 * @property {ActiveEnumerationMethod} method - Enumeration scope.
 * @property {ActiveEnumerationFamily} family - Executable source family.
 * @property {ActiveEnumerationState} state - Reconciled operational state.
 * @property {boolean | null} processAlive
 *   Live runner evidence, or null when process inspection failed.
 * @property {boolean | null} detailActive
 *   Live bounded child/probe/browser evidence, or null when inspection failed.
 * @property {ActiveEnumerationCheckpointActivity} checkpointActivity
 *   Whether completed units or only the checkpoint timestamp moved.
 * @property {number} completedUnits - Durable completed work units.
 * @property {number} totalUnits - Immutable work-unit denominator.
 * @property {number} remainingUnits - Reconciled unfinished work units.
 * @property {number} completionPercent - Completed percentage.
 * @property {number | null} locallyCapturedRecords
 *   Locally reconciled unique records when this route owns local artifacts.
 * @property {number | null} durableLoadedRecords
 *   Route-checkpointed Neon-loaded records when directly derivable.
 * @property {number} deferredCapCount - Unresolved source caps.
 * @property {number} sourceMissingCount - Explicit inaccessible source rows.
 * @property {string | null} lastCheckpointAt - Last checkpoint timestamp.
 * @property {number | null} checkpointAgeSeconds - Snapshot-relative age.
 * @property {boolean} checkpointStale - Whether checkpoint activity is stale.
 * @property {ActiveEnumerationObservation} throughput - Recent observed rate.
 * @property {ActiveEnumerationEta} eta - Conditional ETA or refusal reason.
 *
 * @typedef {object} ActivePermitEnumerationStatus
 * @property {string} generatedAt - Observation timestamp.
 * @property {boolean} snapshotStale - Whether the whole dashboard snapshot is stale.
 * @property {number} observationWindowSeconds - Longest worker observation window.
 * @property {ActiveEnumerationWorker[]} workers - Exactly the configured active routes.
 *
 * @typedef {object} WorkerSample
 * @property {number} observedAtMs - Aggregate observation epoch.
 * @property {number} completedUnits - Completed work units.
 * @property {number} totalUnits - Work-unit denominator.
 * @property {number | null} checkpointAtMs - Durable checkpoint epoch.
 */

/**
 * Read a bounded process list and return only route-presence booleans.
 *
 * Raw command lines can contain private worker arguments, so they remain
 * inside this function and are never returned. The operating-system read has
 * a short timeout and fixed output ceiling.
 *
 * @param {readonly ActiveEnumerationRouteDefinition[]} definitions
 *   Executable route/process definitions.
 * @returns {Promise<ActiveEnumerationProcessSnapshot>} Sanitized liveness map.
 */
export function readActiveEnumerationProcessSnapshot(definitions) {
  return new Promise((resolvePromise) => {
    execFile(
      "ps",
      ["-eo", "pid=,ppid=,etimes=,args="],
      {
        encoding: "utf8",
        timeout: 3_000,
        maxBuffer: 512 * 1_024,
      },
      (error, stdout) => {
        if (error !== null) {
          resolvePromise({
            available: false,
            routeKeys: new Set(),
            detailRouteKeys: new Set(),
            supervisorNotBeforeByKey: new Map(),
          });
          return;
        }
        const entries = stdout
          .split("\n")
          .map(parseProcessEntry)
          .filter((entry) => entry !== null);
        const detected = detectActiveEnumerationProcessDetails(
          definitions,
          entries,
        );
        resolvePromise({
          available: true,
          ...detected,
        });
      },
    );
  });
}

/**
 * Parse one fixed-column `ps` row without returning it outside process
 * inspection. Invalid or truncated rows are ignored.
 *
 * @param {string} line - One `ps -eo pid,ppid,etimes,args` output row.
 * @returns {ActiveEnumerationProcessEntry | null} Parsed bounded entry.
 */
function parseProcessEntry(line) {
  const match = /^\s*(\d+)\s+(\d+)\s+(\d+)\s+(.+)$/u.exec(line);
  if (match === null) return null;
  const pid = Number(match[1]);
  const parentPid = Number(match[2]);
  const elapsedSeconds = Number(match[3]);
  const command = match[4];
  if (
    !Number.isSafeInteger(pid) ||
    !Number.isSafeInteger(parentPid) ||
    !Number.isSafeInteger(elapsedSeconds) ||
    typeof command !== "string"
  ) {
    return null;
  }
  return { pid, parentPid, elapsedSeconds, command };
}

/**
 * Match sanitized route definitions against command lines. Exposed for tests;
 * production callers must not serialize or log the supplied command strings.
 *
 * @param {readonly ActiveEnumerationRouteDefinition[]} definitions
 *   Exact executable route definitions.
 * @param {readonly string[]} commands - Process command lines.
 * @returns {ReadonlySet<string>} Stable route keys with live runner evidence.
 */
export function detectActiveEnumerationProcesses(definitions, commands) {
  const liveKeys = new Set();
  for (const definition of definitions) {
    const escapedKey = escapeRegExp(definition.processJurisdictionKey);
    const flag =
      definition.method === "full" ? "--jurisdiction" : "--jurisdictions";
    const matcher = new RegExp(
      `(?:^|\\s)${flag}(?:=|\\s+)[^\\s]*${escapedKey}(?:,|\\s|$)`,
      "u",
    );
    if (
      commands.some(
        (command) =>
          command.includes(definition.processScript) && matcher.test(command),
      )
    ) {
      liveKeys.add(definition.key);
    }
  }
  return liveKeys;
}

/**
 * Reduce a bounded process tree to aggregate route liveness, recent detail
 * activity, and operator retry boundaries. Raw commands and PIDs never leave
 * this result.
 *
 * @param {readonly ActiveEnumerationRouteDefinition[]} definitions
 *   Exact executable route definitions.
 * @param {readonly ActiveEnumerationProcessEntry[]} entries
 *   Parsed private process rows.
 * @returns {Omit<ActiveEnumerationProcessSnapshot,"available">}
 *   Aggregate-only process evidence.
 */
export function detectActiveEnumerationProcessDetails(definitions, entries) {
  const commands = entries.map((entry) => entry.command);
  const routeKeys = detectActiveEnumerationProcesses(definitions, commands);
  const detailRouteKeys = new Set();
  /** @type {Map<string,string>} */
  const supervisorNotBeforeByKey = new Map();

  for (const definition of definitions) {
    const rootEntries = entries.filter((entry) =>
      commandMatchesDefinition(definition, entry.command),
    );
    if (rootEntries.length === 0) continue;
    const rootPids = new Set(rootEntries.map((entry) => entry.pid));
    const descendantPids = collectDescendantPids(entries, rootPids);
    if (
      entries.some(
        (entry) =>
          descendantPids.has(entry.pid) &&
          entry.elapsedSeconds <= MAXIMUM_DETAIL_PROCESS_AGE_SECONDS &&
          isBoundedDetailCommand(entry.command),
      )
    ) {
      detailRouteKeys.add(definition.key);
    }
    if (definition.method !== "full") continue;
    for (const entry of rootEntries) {
      const boundary = readNotBeforeBoundary(entry.command);
      if (boundary === null) continue;
      const prior = supervisorNotBeforeByKey.get(definition.key);
      if (prior === undefined || Date.parse(boundary) > Date.parse(prior)) {
        supervisorNotBeforeByKey.set(definition.key, boundary);
      }
    }
  }
  return { routeKeys, detailRouteKeys, supervisorNotBeforeByKey };
}

/**
 * Test whether one process command is the configured route runner.
 *
 * @param {ActiveEnumerationRouteDefinition} definition - Exact route definition.
 * @param {string} command - Private process command.
 * @returns {boolean} True when script and jurisdiction arguments both match.
 */
function commandMatchesDefinition(definition, command) {
  const escapedKey = escapeRegExp(definition.processJurisdictionKey);
  const flag =
    definition.method === "full" ? "--jurisdiction" : "--jurisdictions";
  const matcher = new RegExp(
    `(?:^|\\s)${flag}(?:=|\\s+)[^\\s]*${escapedKey}(?:,|\\s|$)`,
    "u",
  );
  return command.includes(definition.processScript) && matcher.test(command);
}

/**
 * Traverse one bounded process snapshot without retaining external state.
 *
 * @param {readonly ActiveEnumerationProcessEntry[]} entries - Process rows.
 * @param {ReadonlySet<number>} rootPids - Route runner process identifiers.
 * @returns {ReadonlySet<number>} All transitive child identifiers.
 */
function collectDescendantPids(entries, rootPids) {
  const descendants = new Set();
  let changed = true;
  while (changed) {
    changed = false;
    for (const entry of entries) {
      if (
        !descendants.has(entry.pid) &&
        (rootPids.has(entry.parentPid) || descendants.has(entry.parentPid))
      ) {
        descendants.add(entry.pid);
        changed = true;
      }
    }
  }
  return descendants;
}

/**
 * Identify only known bounded detail processes. Supervisor shells and worker
 * parents do not count as detail activity.
 *
 * @param {string} command - Private process command.
 * @returns {boolean} Whether the command is a probe/browser detail child.
 */
function isBoundedDetailCommand(command) {
  return (
    command.includes("probe-broward-") ||
    /(?:^|[/\s-])(?:chrome|chromium|playwright)(?:[/\s-]|$)/iu.test(command)
  );
}

/**
 * Extract a valid UTC operator boundary from a live supervisor command.
 *
 * @param {string} command - Private supervisor command.
 * @returns {string | null} Valid ISO boundary or null.
 */
function readNotBeforeBoundary(command) {
  if (
    !command.includes("run-broward-municipal-enumeration-supervisor.mjs")
  ) {
    return null;
  }
  const value = /(?:^|\s)--not-before(?:=|\s+)(\S+)/u.exec(command)?.[1];
  return typeof value === "string" &&
    value.endsWith("Z") &&
    Number.isFinite(Date.parse(value))
    ? value
    : null;
}

/**
 * Create a bounded in-memory observation tracker.
 *
 * The tracker keeps at most one hour of small aggregate samples. It never
 * persists route records, process arguments, source identifiers, or errors.
 *
 * @param {readonly ActiveEnumerationRouteDefinition[]} definitions
 *   Ten executable active-route definitions.
 * @returns {(
 *   workers:readonly EnumerationWorkerAggregate[],
 *   processes:ActiveEnumerationProcessSnapshot,
 *   nowMs?:number
 * )=>ActivePermitEnumerationStatus} Aggregate status observer.
 */
export function createActivePermitEnumerationTracker(definitions) {
  validateDefinitions(definitions);
  /** @type {Map<string, WorkerSample[]>} */
  const historyByKey = new Map(
    definitions.map((definition) => [definition.key, []]),
  );
  return (workers, processes, nowMs = Date.now()) => {
    if (!Number.isFinite(nowMs)) {
      throw new Error("Active enumeration observation time is invalid");
    }
    const workerBySource = new Map(
      workers.map((worker) => [worker.source, worker]),
    );
    let observationWindowSeconds = 0;
    const activeWorkers = definitions.map((definition) => {
      const worker = workerBySource.get(definition.jurisdiction);
      if (worker === undefined) {
        throw new Error("Active enumeration worker aggregate is missing");
      }
      validateWorkerAggregate(worker);
      const samples = historyByKey.get(definition.key);
      if (samples === undefined) {
        throw new Error("Active enumeration history is missing");
      }
      const checkpointAtMs =
        worker.updatedAt === null ? null : Date.parse(worker.updatedAt);
      if (
        checkpointAtMs !== null &&
        (!Number.isFinite(checkpointAtMs) || checkpointAtMs > nowMs + 60_000)
      ) {
        throw new Error("Active enumeration checkpoint timestamp is invalid");
      }
      const prior = samples.at(-1);
      if (
        prior !== undefined &&
        (worker.completedWindows < prior.completedUnits ||
          (prior.totalUnits > 0 &&
            worker.totalWindows > 0 &&
            worker.totalWindows !== prior.totalUnits))
      ) {
        throw new Error("Active enumeration work units regressed");
      }
      samples.push({
        observedAtMs: nowMs,
        completedUnits: worker.completedWindows,
        totalUnits: worker.totalWindows,
        checkpointAtMs,
      });
      trimHistory(samples, nowMs);
      const processAlive = processes.available
        ? processes.routeKeys.has(definition.key)
        : null;
      const detailActive = processes.available
        ? processes.detailRouteKeys.has(definition.key)
        : null;
      const projected = projectActiveWorker(
        definition,
        worker,
        samples,
        processAlive,
        detailActive,
        nowMs,
      );
      observationWindowSeconds = Math.max(
        observationWindowSeconds,
        projected.throughput.windowSeconds,
      );
      return projected;
    });
    return {
      generatedAt: new Date(nowMs).toISOString(),
      snapshotStale: false,
      observationWindowSeconds,
      workers: activeWorkers,
    };
  };
}

/**
 * Refuse live ETAs and process claims when the resilient dashboard is serving
 * its last successful aggregate after a refresh failure.
 *
 * @param {ActivePermitEnumerationStatus} status - Last successful active status.
 * @param {number} [nowMs=Date.now()] - Stale response epoch.
 * @returns {ActivePermitEnumerationStatus} Safe stale clone.
 */
export function markActivePermitEnumerationSnapshotStale(
  status,
  nowMs = Date.now(),
) {
  return {
    ...structuredClone(status),
    generatedAt: new Date(nowMs).toISOString(),
    snapshotStale: true,
    workers: status.workers.map((worker) => {
      const checkpointMs =
        worker.lastCheckpointAt === null
          ? Number.NaN
          : Date.parse(worker.lastCheckpointAt);
      return {
        ...structuredClone(worker),
        processAlive: null,
        detailActive: null,
        checkpointAgeSeconds: Number.isFinite(checkpointMs)
          ? Math.max(0, Math.round((nowMs - checkpointMs) / 1_000))
          : null,
        checkpointStale:
          !Number.isFinite(checkpointMs) ||
          nowMs - checkpointMs > CHECKPOINT_STALE_AFTER_MS,
        eta:
          worker.state === "complete"
            ? completeEta()
            : unknownEta("dashboard_snapshot_stale"),
      };
    }),
  };
}

/**
 * Project one aggregate worker from bounded history.
 *
 * @param {ActiveEnumerationRouteDefinition} definition - Route metadata.
 * @param {EnumerationWorkerAggregate} worker - Current aggregate checkpoint.
 * @param {readonly WorkerSample[]} samples - Bounded chronological samples.
 * @param {boolean | null} processAlive - Current process evidence.
 * @param {boolean | null} detailActive - Current bounded detail evidence.
 * @param {number} nowMs - Observation epoch.
 * @returns {ActiveEnumerationWorker} Public active-worker projection.
 */
function projectActiveWorker(
  definition,
  worker,
  samples,
  processAlive,
  detailActive,
  nowMs,
) {
  const completedUnits = worker.completedWindows;
  const totalUnits = worker.totalWindows;
  const remainingUnits = Math.max(0, totalUnits - completedUnits);
  if (
    completedUnits + remainingUnits !== totalUnits ||
    worker.pendingWindows !== remainingUnits
  ) {
    throw new Error("Active enumeration counts do not reconcile");
  }
  const checkpointAtMs =
    worker.updatedAt === null ? null : Date.parse(worker.updatedAt);
  const checkpointAgeSeconds =
    checkpointAtMs === null
      ? null
      : Math.max(0, Math.round((nowMs - checkpointAtMs) / 1_000));
  const checkpointStale =
    checkpointAtMs === null ||
    nowMs - checkpointAtMs > CHECKPOINT_STALE_AFTER_MS;
  const state = classifyState(
    worker,
    processAlive,
    detailActive,
    checkpointStale,
  );
  const metrics = calculateObservation(samples);
  const checkpointActivity = classifyCheckpointActivity(samples, metrics);
  const eta = calculateEta(
    definition,
    state,
    checkpointStale,
    detailActive,
    remainingUnits,
    samples,
    metrics,
  );
  return {
    jurisdiction: definition.jurisdiction,
    method: definition.method,
    family: definition.family,
    state,
    processAlive,
    detailActive,
    checkpointActivity,
    completedUnits,
    totalUnits,
    remainingUnits,
    completionPercent:
      totalUnits === 0 ? 0 : round((completedUnits / totalUnits) * 100, 3),
    locallyCapturedRecords:
      definition.countSource === "local_checkpoint"
        ? worker.accessibleRecords
        : null,
    durableLoadedRecords:
      definition.countSource === "durable_route_checkpoint"
        ? worker.accessibleRecords
        : null,
    deferredCapCount: worker.deferredCapCount,
    sourceMissingCount: worker.sourceMissingRecords,
    lastCheckpointAt: worker.updatedAt,
    checkpointAgeSeconds,
    checkpointStale,
    throughput: metrics,
    eta,
  };
}

/**
 * Classify current operational state while keeping process presence separate
 * from durable checkpoint freshness.
 *
 * @param {EnumerationWorkerAggregate} worker - Current worker aggregate.
 * @param {boolean | null} processAlive - Process evidence.
 * @param {boolean | null} detailActive - Bounded detail-process evidence.
 * @param {boolean} checkpointStale - Checkpoint staleness.
 * @returns {ActiveEnumerationState} Reconciled public state.
 */
function classifyState(worker, processAlive, detailActive, checkpointStale) {
  if (
    worker.status === "complete" &&
    worker.completedWindows === worker.totalWindows
  ) {
    return "complete";
  }
  if (worker.status === "cooling_down" && processAlive === true) {
    return "cooling";
  }
  if (processAlive === true && detailActive === true) return "running";
  if (processAlive === true && checkpointStale) return "stalled";
  if (processAlive === true) return "running";
  if (
    processAlive === null &&
    worker.status === "running" &&
    !checkpointStale
  ) {
    return "running";
  }
  return "paused";
}

/**
 * Calculate overall observed work-unit throughput.
 *
 * @param {readonly WorkerSample[]} samples - Chronological aggregate samples.
 * @returns {ActiveEnumerationObservation} Observation metrics.
 */
function calculateObservation(samples) {
  const current = samples.at(-1);
  const first = samples[0];
  if (current === undefined || first === undefined) {
    return {
      observedUnits: 0,
      windowSeconds: 0,
      unitsPerHour: null,
      variabilityRatio: null,
    };
  }
  const windowMs = Math.max(0, current.observedAtMs - first.observedAtMs);
  const observedUnits = Math.max(
    0,
    current.completedUnits - first.completedUnits,
  );
  const segmentRates = calculateSegmentRates(samples);
  const variabilityRatio =
    segmentRates.length === RATE_SEGMENT_COUNT &&
    segmentRates.every((rate) => rate > 0)
      ? Math.max(...segmentRates) / Math.min(...segmentRates)
      : null;
  return {
    observedUnits,
    windowSeconds: Math.round(windowMs / 1_000),
    unitsPerHour:
      windowMs >= MINIMUM_ACTIVITY_WINDOW_MS
        ? round((observedUnits * 3_600_000) / windowMs, 3)
        : null,
    variabilityRatio:
      variabilityRatio === null ? null : round(variabilityRatio, 3),
  };
}

/**
 * Identify whether completed units or only a detail-loop checkpoint moved.
 *
 * @param {readonly WorkerSample[]} samples - Chronological samples.
 * @param {ActiveEnumerationObservation} observation - Calculated throughput.
 * @returns {ActiveEnumerationCheckpointActivity} Activity classification.
 */
function classifyCheckpointActivity(samples, observation) {
  if (observation.windowSeconds * 1_000 < MINIMUM_ACTIVITY_WINDOW_MS) {
    return "warming_up";
  }
  if (observation.observedUnits > 0) return "work_units_advanced";
  const first = samples[0];
  const current = samples.at(-1);
  if (
    first !== undefined &&
    current !== undefined &&
    first.checkpointAtMs !== null &&
    current.checkpointAtMs !== null &&
    current.checkpointAtMs > first.checkpointAtMs
  ) {
    return "checkpoint_updated";
  }
  return "stationary";
}

/**
 * Calculate ETA only from a sufficiently long, advancing, low-variability
 * observation window with an unchanged denominator.
 *
 * @param {ActiveEnumerationRouteDefinition} definition - Route metadata.
 * @param {ActiveEnumerationState} state - Current state.
 * @param {boolean} checkpointStale - Checkpoint staleness.
 * @param {boolean | null} detailActive - Bounded detail-process evidence.
 * @param {number} remainingUnits - Unfinished units.
 * @param {readonly WorkerSample[]} samples - Chronological observations.
 * @param {ActiveEnumerationObservation} observation - Overall observed rate.
 * @returns {ActiveEnumerationEta} Estimate or explicit refusal reason.
 */
function calculateEta(
  definition,
  state,
  checkpointStale,
  detailActive,
  remainingUnits,
  samples,
  observation,
) {
  if (remainingUnits === 0) return completeEta();
  if (state === "stalled") return unknownEta("checkpoint_stale");
  if (state !== "running") return unknownEta("worker_not_running");
  if (detailActive === true && checkpointStale) {
    return unknownEta("detail_activity");
  }
  if (checkpointStale) return unknownEta("checkpoint_stale");
  if (definition.family === "municipal_type") {
    return unknownEta("variable_detail_loop");
  }
  if (observation.windowSeconds * 1_000 < MINIMUM_ETA_WINDOW_MS) {
    return unknownEta("observation_window_short");
  }
  if (new Set(samples.map((sample) => sample.totalUnits)).size !== 1) {
    return unknownEta("work_unit_total_changed");
  }
  if (
    observation.observedUnits === 0 ||
    observation.unitsPerHour === null ||
    observation.unitsPerHour <= 0
  ) {
    return unknownEta("no_checkpoint_movement");
  }
  const segmentRates = calculateSegmentRates(samples);
  if (
    segmentRates.length !== RATE_SEGMENT_COUNT ||
    segmentRates.some((rate) => rate <= 0)
  ) {
    return unknownEta("rate_variability_high");
  }
  const minimumRate = Math.min(...segmentRates);
  const maximumRate = Math.max(...segmentRates);
  if (maximumRate / minimumRate > MAXIMUM_RATE_VARIABILITY_RATIO) {
    return unknownEta("rate_variability_high");
  }
  return {
    kind: "estimate",
    estimatedHours: round(remainingUnits / observation.unitsPerHour, 3),
    lowHours: round(remainingUnits / maximumRate, 3),
    highHours: round(remainingUnits / minimumRate, 3),
    reason: "rate_stable",
  };
}

/**
 * Build three equal-duration segment rates from the nearest observed samples.
 *
 * @param {readonly WorkerSample[]} samples - Chronological observations.
 * @returns {number[]} Completed work units per hour for each segment.
 */
function calculateSegmentRates(samples) {
  const first = samples[0];
  const last = samples.at(-1);
  if (
    first === undefined ||
    last === undefined ||
    last.observedAtMs - first.observedAtMs < MINIMUM_ETA_WINDOW_MS
  ) {
    return [];
  }
  /** @type {WorkerSample[]} */
  const boundaries = [first];
  for (let segment = 1; segment < RATE_SEGMENT_COUNT; segment += 1) {
    const target =
      first.observedAtMs +
      ((last.observedAtMs - first.observedAtMs) * segment) / RATE_SEGMENT_COUNT;
    const nearest = samples.reduce((selected, sample) =>
      Math.abs(sample.observedAtMs - target) <
      Math.abs(selected.observedAtMs - target)
        ? sample
        : selected,
    );
    boundaries.push(nearest);
  }
  boundaries.push(last);
  const rates = [];
  for (let index = 1; index < boundaries.length; index += 1) {
    const prior = boundaries[index - 1];
    const current = boundaries[index];
    if (prior === undefined || current === undefined) return [];
    const durationMs = current.observedAtMs - prior.observedAtMs;
    const completed = current.completedUnits - prior.completedUnits;
    if (durationMs <= 0 || completed < 0) return [];
    rates.push((completed * 3_600_000) / durationMs);
  }
  return rates;
}

/**
 * Drop samples older than the fixed observation horizon.
 *
 * @param {WorkerSample[]} samples - Mutable route history.
 * @param {number} nowMs - Current observation epoch.
 * @returns {void}
 */
function trimHistory(samples, nowMs) {
  while (
    samples.length > 1 &&
    (samples[1]?.observedAtMs ?? nowMs) < nowMs - MAXIMUM_HISTORY_WINDOW_MS
  ) {
    samples.shift();
  }
}

/**
 * Validate immutable route definitions before accepting observations.
 *
 * @param {readonly ActiveEnumerationRouteDefinition[]} definitions - Routes.
 * @returns {void}
 */
function validateDefinitions(definitions) {
  if (
    definitions.length !== 10 ||
    new Set(definitions.map((definition) => definition.key)).size !==
      definitions.length ||
    new Set(definitions.map((definition) => definition.jurisdiction)).size !==
      definitions.length
  ) {
    throw new Error("Active enumeration definitions do not reconcile");
  }
}

/**
 * Validate non-negative integer aggregate counters.
 *
 * @param {EnumerationWorkerAggregate} worker - Existing worker aggregate.
 * @returns {void}
 */
function validateWorkerAggregate(worker) {
  const counters = [
    worker.completedWindows,
    worker.pendingWindows,
    worker.totalWindows,
    worker.accessibleRecords,
    worker.sourceMissingRecords,
    worker.deferredCapCount,
  ];
  if (
    counters.some(
      (value) => !Number.isSafeInteger(value) || Number(value) < 0,
    ) ||
    worker.completedWindows + worker.pendingWindows !== worker.totalWindows
  ) {
    throw new Error("Active enumeration aggregate counters are invalid");
  }
}

/**
 * Return a complete terminal ETA result.
 *
 * @returns {ActiveEnumerationEta} Complete result.
 */
function completeEta() {
  return {
    kind: "complete",
    estimatedHours: 0,
    lowHours: 0,
    highHours: 0,
    reason: "complete",
  };
}

/**
 * Return an explicit unknown ETA result.
 *
 * @param {Exclude<ActiveEnumerationEtaReason, "complete" | "rate_stable">} reason - Refusal reason.
 * @returns {ActiveEnumerationEta} Unknown result.
 */
function unknownEta(reason) {
  return {
    kind: "unknown",
    estimatedHours: null,
    lowHours: null,
    highHours: null,
    reason,
  };
}

/**
 * Round one finite number to fixed decimal precision.
 *
 * @param {number} value - Finite number.
 * @param {number} decimals - Non-negative decimal count.
 * @returns {number} Rounded number.
 */
function round(value, decimals) {
  const scale = 10 ** decimals;
  return Math.round(value * scale) / scale;
}

/**
 * Escape one literal string for use in a regular expression.
 *
 * @param {string} value - Literal value.
 * @returns {string} Escaped regular-expression fragment.
 */
function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
}
