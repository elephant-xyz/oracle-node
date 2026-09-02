import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import {
  createBrowardMunicipalPropertySeedRows,
  normalizeMunicipalPropertyAddress,
  parseMunicipalPropertySeedOptions,
  renderMunicipalPropertySeedGapRow,
} from "../../scripts/build-broward-municipal-property-seed.mjs";
import { getBrowardMunicipalPermitConfig } from "../../scripts/permit-source-adapters/broward-municipal-config.mjs";
import {
  parseMunicipalPropertyEnumerationOptions,
  readMunicipalSeedQueries,
  runMunicipalPropertyEnumeration,
} from "../../scripts/run-broward-municipal-property-enumeration.mjs";
import {
  parseMunicipalTypeEnumerationOptions,
  runMunicipalTypeEnumeration,
} from "../../scripts/run-broward-municipal-record-type-enumeration.mjs";

/** @type {string[]} */
const temporaryDirectories = [];

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { force: true, recursive: true })),
  );
});

/**
 * Create one registered private test directory.
 *
 * @returns {Promise<string>} Temporary root.
 */
async function createTemporaryDirectory() {
  const directory = await mkdtemp(
    path.join(tmpdir(), "broward-municipal-enumeration-"),
  );
  temporaryDirectories.push(directory);
  return directory;
}

/**
 * Build one complete normalized municipal record.
 *
 * @param {ReturnType<typeof getBrowardMunicipalPermitConfig>} config - Source.
 * @param {string} sourceRecordId - Stable fixture identity.
 * @param {string} permitNumber - Stable fixture number.
 * @param {import("../../scripts/permit-source-adapters/broward-municipal-core.mjs").BrowardMunicipalQuery} query - Exact discovery query.
 * @returns {import("../../scripts/permit-source-adapters/broward-municipal-core.mjs").NormalizedBrowardMunicipalPermit}
 *   Complete normalized fixture record.
 */
function normalizedRecord(config, sourceRecordId, permitNumber, query) {
  return {
    source_system: config.sourceSystem,
    source_protocol: config.protocol,
    source_url: `${new URL(config.searchUrl).origin}/detail/${sourceRecordId}`,
    source_search_url: config.searchUrl,
    source_record_id: sourceRecordId,
    record_key: `${config.sourceSystem}:${sourceRecordId}`,
    jurisdiction: config.jurisdiction,
    permit_number: permitNumber,
    parcel_identifier: null,
    query_folio: query.kind === "folio" ? query.value : null,
    work_location: null,
    application_date: null,
    permit_issue_date: null,
    expiration_date: null,
    record_status: "Issued",
    record_type: "ROOF",
    project_description: null,
    job_value: null,
    inspections: [],
    is_roof_permit: true,
    raw: { source_page: 1, query_kind: query.kind },
  };
}

describe("Broward municipal property seed", () => {
  it("routes exact BCPA city tails and deduplicates normalized address queries", () => {
    expect(parseMunicipalPropertySeedOptions([])).toMatchObject({
      outputPath: expect.stringMatching(
        /broward-municipal-property-seed\.private\.csv$/u,
      ),
      gapOutputPath: expect.stringMatching(
        /broward-municipal-property-seed-gaps\.private\.csv$/u,
      ),
    });
    expect(
      parseMunicipalPropertySeedOptions(["--output", "custom.private.csv"])
        .gapOutputPath,
    ).toMatch(/custom-gaps\.private\.csv$/u);
    expect(
      normalizeMunicipalPropertyAddress(
        "100 PRIVATE STREET, MARGATE, FL 33063",
        ["MARGATE"],
      ),
    ).toBe("100 PRIVATE ST");
    const result = createBrowardMunicipalPropertySeedRows([
      {
        request_identifier: "484200000001",
        unnormalized_address: "100 PRIVATE STREET MARGATE FL 33063",
      },
      {
        request_identifier: "484200000002",
        unnormalized_address: "100 PRIVATE ST MARGATE FL 33063",
      },
      {
        request_identifier: "484200000003",
        unnormalized_address: "101 PRIVATE ROAD COCONUT CREEK FL 33063",
      },
      {
        request_identifier: "484200000004",
        unnormalized_address: "NO NUMBER POMPANO BEACH FL 33060",
      },
    ]);
    expect(result.rows).toEqual([
      {
        jurisdiction_key: "coconut-creek",
        query_kind: "folio",
        query_value: "484200000003",
        property_count: 1,
      },
      {
        jurisdiction_key: "margate",
        query_kind: "address",
        query_value: "100 PRIVATE ST",
        property_count: 2,
      },
    ]);
    expect(result.propertyCounts).toMatchObject({
      "coconut-creek": 1,
      margate: 2,
      "pompano-beach": 1,
    });
    expect(result.unqueryableCounts["pompano-beach"]).toBe(1);
    expect(result.gapRows).toEqual([
      {
        jurisdiction_key: "pompano-beach",
        property_identifier: "484200000004",
        reason: "unrepresentable_normalized_address",
      },
    ]);
    const gapRow = result.gapRows[0];
    expect(gapRow).toBeDefined();
    if (gapRow === undefined) throw new Error("Expected one fixture gap row");
    expect(renderMunicipalPropertySeedGapRow(gapRow)).toBe(
      "pompano-beach,484200000004,unrepresentable_normalized_address",
    );
  });
});

describe("Broward municipal property enumeration", () => {
  it("checkpoints one terminal client-all query before completing", async () => {
    const root = await createTemporaryDirectory();
    const seedPath = path.join(root, "seed.private.csv");
    const outputDirectory = path.join(root, "capture");
    const seed =
      "jurisdiction_key,query_kind,query_value,property_count\n" +
      "coconut-creek,folio,484200000001,1\n";
    await writeFile(seedPath, seed, { mode: 0o600 });
    expect(readMunicipalSeedQueries(seed, "coconut_creek")).toEqual([
      {
        jurisdictionKey: "coconut-creek",
        queryKind: "folio",
        queryValue: "484200000001",
        propertyCount: 1,
      },
    ]);
    const config = getBrowardMunicipalPermitConfig("coconut_creek");
    const close = vi.fn(async () => {});
    const summary = await runMunicipalPropertyEnumeration(
      {
        jurisdictionKey: "coconut_creek",
        seedPath,
        outputDirectory,
        maxQueries: null,
        maxResultsPerQuery: 100,
        delayMs: 1_000,
        requestTimeoutMs: 30_000,
      },
      {
        now: () => "2026-09-02T17:00:00.000Z",
        wait: async () => {},
        createTransport: async () => ({
          fetchSearchPage: async () => {
            const activeCheckpoint = JSON.parse(
              await readFile(
                path.join(outputDirectory, "checkpoint.private.json"),
                "utf8",
              ),
            );
            expect(activeCheckpoint).toMatchObject({
              nextQueryIndex: 0,
              status: "running",
              blocker: null,
            });
            return {
              references: [
                {
                  sourceRecordId: "fixture-1",
                  permitNumber: "26000001",
                  detailUrl: "https://example.test/detail/fixture-1",
                  sourcePage: 1,
                  listData: {},
                },
              ],
              nextPage: null,
            };
          },
          fetchDetail: async (reference, query) =>
            normalizedRecord(
              config,
              reference.sourceRecordId,
              reference.permitNumber,
              query,
            ),
          listRecordTypePartitions: async () => {
            throw new Error("unsupported");
          },
          close,
        }),
      },
    );
    expect(summary).toMatchObject({
      status: "complete",
      totalQueries: 1,
      completedQueries: 1,
      representedProperties: 1,
      recordObservations: 1,
      uniqueRecordCount: 1,
      duplicateRecordCount: 0,
      deferredCapCount: 0,
      blocker: null,
    });
    expect(close).toHaveBeenCalledOnce();
    const checkpoint = JSON.parse(
      await readFile(
        path.join(outputDirectory, "checkpoint.private.json"),
        "utf8",
      ),
    );
    expect(checkpoint).toMatchObject({
      sourceSystem: config.sourceSystem,
      coverageBoundary: "bcpa_property_first_folio",
      completedQueries: 1,
      uniqueRecords: 1,
      deferredCapItems: {},
      status: "complete",
    });
  });

  it("defers a capped item without marking it complete and continues the seed", async () => {
    const root = await createTemporaryDirectory();
    const seedPath = path.join(root, "seed.private.csv");
    const outputDirectory = path.join(root, "capture");
    await writeFile(
      seedPath,
      "jurisdiction_key,query_kind,query_value,property_count\n" +
        "margate,address,100 PRIVATE ST,1\n" +
        "margate,address,101 PRIVATE ST,2\n",
      { mode: 0o600 },
    );
    const fetchDetail = vi.fn();
    /** @type {string[]} */
    const searchedQueries = [];
    const close = vi.fn(async () => {});

    const summary = await runMunicipalPropertyEnumeration(
      {
        jurisdictionKey: "margate",
        seedPath,
        outputDirectory,
        maxQueries: null,
        maxResultsPerQuery: 2,
        delayMs: 1_000,
        requestTimeoutMs: 30_000,
      },
      {
        now: () => "2026-09-02T17:00:00.000Z",
        wait: async () => {},
        createTransport: async () => ({
          fetchSearchPage: async (query) => {
            searchedQueries.push(query.value);
            return {
              references:
                query.value === "100 PRIVATE ST"
                  ? [
                      {
                        sourceRecordId: "fixture-1",
                        permitNumber: "PERMIT-1",
                        detailUrl: "https://example.test/detail/fixture-1",
                        sourcePage: 1,
                        listData: {},
                      },
                      {
                        sourceRecordId: "fixture-2",
                        permitNumber: "PERMIT-2",
                        detailUrl: "https://example.test/detail/fixture-2",
                        sourcePage: 1,
                        listData: {},
                      },
                    ]
                  : [],
              nextPage: null,
            };
          },
          fetchDetail,
          listRecordTypePartitions: async () => {
            throw new Error("unsupported");
          },
          close,
        }),
      },
    );

    expect(summary).toMatchObject({
      status: "cooling",
      totalQueries: 2,
      completedQueries: 1,
      deferredCapCount: 1,
      blocker: "source_cap",
    });
    expect(searchedQueries).toEqual(["100 PRIVATE ST", "101 PRIVATE ST"]);
    expect(fetchDetail).not.toHaveBeenCalled();
    expect(close).toHaveBeenCalledOnce();
    const checkpoint = JSON.parse(
      await readFile(
        path.join(outputDirectory, "checkpoint.private.json"),
        "utf8",
      ),
    );
    expect(checkpoint).toMatchObject({
      nextQueryIndex: 2,
      completedQueries: 1,
      status: "cooling",
      blocker: "source_cap",
    });
    const deferredItems = Object.values(checkpoint.deferredCapItems);
    expect(deferredItems).toHaveLength(1);
    expect(deferredItems[0]).toMatchObject({
      queryIndex: 0,
      representedProperties: 1,
      reason: "client_all_exclusive_cap",
      observedResultCount: 2,
      exclusiveCap: 2,
      attemptCount: 1,
    });
    const ledgerPath = path.join(outputDirectory, "deferred-cap.private.jsonl");
    const ledger = await readFile(ledgerPath, "utf8");
    expect(ledger).not.toMatch(/100 PRIVATE ST|101 PRIVATE ST/u);
    expect(JSON.parse(ledger)).toMatchObject({
      queryIndex: 0,
      reason: "client_all_exclusive_cap",
    });
    expect((await stat(ledgerPath)).mode & 0o777).toBe(0o600);

    /** @type {string[]} */
    const retriedQueries = [];
    const retryFetchDetail = vi.fn();
    const retrySummary = await runMunicipalPropertyEnumeration(
      {
        jurisdictionKey: "margate",
        seedPath,
        outputDirectory,
        maxQueries: null,
        maxResultsPerQuery: 2,
        delayMs: 1_000,
        requestTimeoutMs: 30_000,
      },
      {
        now: () => "2026-09-04T17:00:00.000Z",
        wait: async () => {},
        createTransport: async () => ({
          fetchSearchPage: async (query) => {
            retriedQueries.push(query.value);
            return { references: [], nextPage: null };
          },
          fetchDetail: retryFetchDetail,
          listRecordTypePartitions: async () => {
            throw new Error("unsupported");
          },
          close: async () => {},
        }),
      },
    );
    expect(retriedQueries).toEqual(["100 PRIVATE ST"]);
    expect(retryFetchDetail).not.toHaveBeenCalled();
    expect(retrySummary).toMatchObject({
      status: "complete",
      completedQueries: 2,
      deferredCapCount: 0,
      blocker: null,
    });
    expect(await readFile(ledgerPath, "utf8")).toBe("");
  });

  it("migrates a legacy source-cap cursor before issuing another query", async () => {
    const root = await createTemporaryDirectory();
    const seedPath = path.join(root, "seed.private.csv");
    const outputDirectory = path.join(root, "capture");
    await writeFile(
      seedPath,
      "jurisdiction_key,query_kind,query_value,property_count\n" +
        "tamarac,address,100 PRIVATE ST,1\n" +
        "tamarac,address,101 PRIVATE ST,1\n",
      { mode: 0o600 },
    );
    const options = {
      jurisdictionKey: "tamarac",
      seedPath,
      outputDirectory,
      maxQueries: 1,
      maxResultsPerQuery: 100,
      delayMs: 1_000,
      requestTimeoutMs: 30_000,
    };
    await runMunicipalPropertyEnumeration(options, {
      now: () => "2026-09-02T17:00:00.000Z",
      wait: async () => {},
      createTransport: async () => ({
        fetchSearchPage: async () => ({ references: [], nextPage: null }),
        fetchDetail: vi.fn(),
        listRecordTypePartitions: async () => {
          throw new Error("unsupported");
        },
        close: async () => {},
      }),
    });
    const checkpointPath = path.join(
      outputDirectory,
      "checkpoint.private.json",
    );
    const legacyCheckpoint = JSON.parse(await readFile(checkpointPath, "utf8"));
    await writeFile(
      checkpointPath,
      `${JSON.stringify(
        {
          ...legacyCheckpoint,
          status: "paused",
          blocker: "source_cap",
        },
        null,
        2,
      )}\n`,
      { mode: 0o600 },
    );
    const fetchSearchPage = vi.fn();
    const summary = await runMunicipalPropertyEnumeration(options, {
      now: () => "2026-09-02T18:00:00.000Z",
      wait: async () => {},
      createTransport: async () => ({
        fetchSearchPage,
        fetchDetail: vi.fn(),
        listRecordTypePartitions: async () => {
          throw new Error("unsupported");
        },
        close: async () => {},
      }),
    });

    expect(fetchSearchPage).not.toHaveBeenCalled();
    expect(summary).toMatchObject({
      status: "cooling",
      completedQueries: 1,
      deferredCapCount: 1,
      blocker: "source_cap",
    });
    const migratedCheckpoint = JSON.parse(
      await readFile(checkpointPath, "utf8"),
    );
    expect(migratedCheckpoint).toMatchObject({
      nextQueryIndex: 2,
      completedQueries: 1,
    });
    expect(Object.values(migratedCheckpoint.deferredCapItems)).toHaveLength(1);
  });

  it("requires conservative production options", () => {
    expect(
      parseMunicipalPropertyEnumerationOptions([
        "--jurisdiction",
        "margate",
        "--seed",
        "seed.csv",
        "--output-dir",
        "capture",
      ]),
    ).toMatchObject({
      jurisdictionKey: "margate",
      maxResultsPerQuery: 100,
      delayMs: 1_500,
    });
    expect(
      parseMunicipalPropertyEnumerationOptions([
        "--jurisdiction",
        "lauderhill",
        "--seed",
        "seed.csv",
        "--output-dir",
        "capture",
        "--max-results-per-query",
        "1000",
      ]).maxResultsPerQuery,
    ).toBe(1_000);
    expect(() =>
      parseMunicipalPropertyEnumerationOptions([
        "--jurisdiction",
        "lauderhill",
        "--seed",
        "seed.csv",
        "--output-dir",
        "capture",
        "--max-results-per-query",
        "1001",
      ]),
    ).toThrow("1000");
    expect(() =>
      parseMunicipalPropertyEnumerationOptions([
        "--jurisdiction",
        "margate",
        "--seed",
        "seed.csv",
        "--output-dir",
        "capture",
        "--delay-ms",
        "999",
      ]),
    ).toThrow("1000");
  });
});

describe("Broward municipal exact-type enumeration", () => {
  it("reconciles sequential pages and the complete option universe", async () => {
    const root = await createTemporaryDirectory();
    const config = getBrowardMunicipalPermitConfig("lighthouse_point");
    const close = vi.fn(async () => {});
    let nowInvocation = 0;
    const summary = await runMunicipalTypeEnumeration(
      {
        jurisdictionKey: "lighthouse_point",
        outputDirectory: root,
        partitionValue: null,
        maxPartitions: null,
        maxPagesPerPartition: 10,
        delayMs: 1_000,
        requestTimeoutMs: 30_000,
      },
      {
        now: () => {
          const timestamp = new Date(
            Date.parse("2026-09-02T17:00:00.000Z") + nowInvocation * 1_000,
          ).toISOString();
          nowInvocation += 1;
          return timestamp;
        },
        wait: async () => {},
        createTransport: async () => ({
          listRecordTypePartitions: async () => [
            { value: "TYPE-1", label: "TYPE ONE" },
            { value: "TYPE-2", label: "TYPE TWO" },
          ],
          fetchSearchPage: async (query, page) => {
            const activeCheckpoint = JSON.parse(
              await readFile(
                path.join(root, "checkpoint.private.json"),
                "utf8",
              ),
            );
            expect(activeCheckpoint).toMatchObject({
              status: "running",
              blocker: null,
              currentPartition: {
                value: query.value,
              },
            });
            expect(activeCheckpoint.updatedAt).not.toBe(
              activeCheckpoint.startedAt,
            );
            const count = query.value === "TYPE-1" ? (page === 1 ? 10 : 1) : 0;
            return {
              references: Array.from({ length: count }, (_value, index) => {
                const ordinal = page === 1 ? index + 1 : 11;
                return {
                  sourceRecordId: `${query.value}-${String(ordinal)}`,
                  permitNumber: `P-${query.value}-${String(ordinal)}`,
                  detailUrl: `https://example.test/${query.value}/${String(ordinal)}`,
                  sourcePage: Number(page),
                  listData: { record_type: query.value },
                };
              }),
              nextPage: query.value === "TYPE-1" && page === 1 ? 2 : null,
              reportedCount: query.value === "TYPE-1" ? 11 : 0,
            };
          },
          fetchDetail: async (reference, query) =>
            normalizedRecord(
              config,
              reference.sourceRecordId,
              reference.permitNumber,
              query,
            ),
          close,
        }),
      },
    );
    expect(summary).toMatchObject({
      status: "complete",
      coverageBoundary: "full_official_record_type_universe",
      sourcePartitionCount: 2,
      plannedPartitionCount: 2,
      completedPartitionCount: 2,
      pendingPartitionCount: 0,
      capturedRecordCount: 11,
      duplicateRecordCount: 0,
      blocker: null,
    });
    expect(close).toHaveBeenCalledOnce();
  });

  it("fails closed at the anonymous eSuite 100-row terminal ceiling", async () => {
    const root = await createTemporaryDirectory();
    const config = getBrowardMunicipalPermitConfig("dania_beach");
    const summary = await runMunicipalTypeEnumeration(
      {
        jurisdictionKey: "dania_beach",
        outputDirectory: root,
        partitionValue: null,
        maxPartitions: null,
        maxPagesPerPartition: 20,
        delayMs: 1_000,
        requestTimeoutMs: 30_000,
      },
      {
        wait: async () => {},
        createTransport: async () => ({
          listRecordTypePartitions: async () => [
            { value: "TYPE-CAPPED", label: "CAPPED TYPE" },
          ],
          fetchSearchPage: async (query, page) => ({
            references: Array.from({ length: 10 }, (_value, index) => {
              const ordinal = (Number(page) - 1) * 10 + index + 1;
              return {
                sourceRecordId: `${query.value}-${String(ordinal)}`,
                permitNumber: `P-${String(ordinal)}`,
                detailUrl: `https://example.test/${String(ordinal)}`,
                sourcePage: Number(page),
                listData: { record_type: query.value },
              };
            }),
            nextPage: Number(page) < 10 ? Number(page) + 1 : null,
          }),
          fetchDetail: async (reference, query) =>
            normalizedRecord(
              config,
              reference.sourceRecordId,
              reference.permitNumber,
              query,
            ),
          close: async () => {},
        }),
      },
    );

    expect(summary).toMatchObject({
      status: "paused",
      completedPartitionCount: 0,
      pendingPartitionCount: 1,
      cappedPartitionCount: 1,
      capturedRecordCount: 90,
      blocker: "source_cap",
    });
    const checkpoint = JSON.parse(
      await readFile(path.join(root, "checkpoint.private.json"), "utf8"),
    );
    expect(checkpoint).toMatchObject({
      completedPartitions: {},
      cappedPartitionValues: ["TYPE-CAPPED"],
      currentPartition: {
        value: "TYPE-CAPPED",
        nextPage: 10,
      },
      status: "paused",
      blocker: "source_cap",
    });
    expect(
      Object.keys(checkpoint.currentPartition.completedPages),
    ).toHaveLength(9);
  });

  it("supports one exact pilot partition without changing the source universe count", () => {
    expect(
      parseMunicipalTypeEnumerationOptions([
        "--jurisdiction",
        "davie",
        "--output-dir",
        "capture",
        "--partition-value",
        "267",
        "--max-partitions",
        "1",
      ]),
    ).toMatchObject({
      jurisdictionKey: "davie",
      partitionValue: "267",
      maxPartitions: 1,
      delayMs: 1_500,
    });
  });
});
