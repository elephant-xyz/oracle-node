import { describe, expect, it } from "vitest";

import {
  buildBrowardSeedEntities,
  buildBrowardSourceRequest,
  parseCliOptions,
  runWorkerHandoffs,
} from "../../scripts/ingest-broward-appraisal-local.mjs";

describe("Broward local appraisal ingestion", () => {
  it("builds the exact fail-closed BCPA request", () => {
    expect(buildBrowardSourceRequest("504108BJ0140")).toMatchObject({
      method: "POST",
      url: expect.stringContaining("getParcelInformation"),
      headers: {
        "content-type": "application/json",
        accept: expect.any(String),
        "x-requested-with": "XMLHttpRequest",
        origin: "https://web.bcpa.net",
        referer: expect.any(String),
      },
      json: {
        folioNumber: "504108BJ0140",
        taxyear: "",
        action: "CURRENT",
        use: "",
      },
    });
  });

  it("builds county-scoped compatibility seed entities", () => {
    const entities = buildBrowardSeedEntities(
      {
        request_identifier: "504108BJ0140",
        address: "",
        latitude: "26.10864266",
        longitude: "-80.27918202",
      },
      "504108BJ0140",
    );
    expect(entities.propertySeed).toMatchObject({
      request_identifier: "504108BJ0140",
      parcel_id: "504108BJ0140",
      source_http_request: {
        method: "POST",
        headers: { "content-type": "application/json" },
        json: { folioNumber: "504108BJ0140", taxyear: "" },
      },
    });
    expect(entities.unnormalizedAddress).toMatchObject({
      request_identifier: "504108BJ0140",
      county_jurisdiction: "Broward",
      latitude: 26.10864266,
      longitude: -80.27918202,
    });
  });

  it("bounds source concurrency and supports resumable run options", () => {
    expect(
      parseCliOptions([
        "--seed",
        "downloads/broward/test.csv",
        "--concurrency",
        "4",
        "--limit",
        "25",
        "--reset-checkpoint",
      ]),
    ).toMatchObject({
      seedPath: "downloads/broward/test.csv",
      concurrency: 4,
      limit: 25,
      resetCheckpoint: true,
    });
    expect(() => parseCliOptions(["--concurrency", "5"])).toThrow(
      /cannot exceed 4/,
    );
  });

  it("classifies query-data-only output and a zero-traffic capture source", () => {
    expect(
      parseCliOptions([
        "--query-data-only",
        "--capture-source",
        "/private/pilot-captures.zip",
        "--start-row",
        "2600",
      ]),
    ).toMatchObject({
      artifactMode: "query-data-only",
      captureSource: "/private/pilot-captures.zip",
      outputDirectory: "downloads/broward/query-data-only-ingestion",
      startRow: 2600,
    });
    expect(() =>
      parseCliOptions([
        "--query-data-only",
        "--output",
        "/tmp/looks-publishable",
      ]),
    ).toThrow(/must include 'query-data-only'/);
    expect(() => parseCliOptions(["--start-row", "2600"])).toThrow(
      /only with --query-data-only/,
    );
  });

  it("hands a free warm worker its next row without moving the ordered checkpoint", async () => {
    /** @type {string[]} */
    const events = [];
    /** @type {number[][]} */
    const committed = [];
    const tasks = [
      { rowIndex: 0, row: {} },
      { rowIndex: 1, row: {} },
      { rowIndex: 2, row: {} },
    ];
    async function* taskSource() {
      for (const task of tasks) yield task;
    }
    const workers = [0, 1].map(() => ({
      run() {
        return Promise.resolve({ success: true, error: null });
      },
      close() {
        return Promise.resolve();
      },
    }));

    await runWorkerHandoffs({
      workers,
      taskIterator: taskSource()[Symbol.asyncIterator](),
      firstRowIndex: 0,
      async runTask(task, worker) {
        const workerIndex = workers.indexOf(worker);
        events.push(`start:${String(task.rowIndex)}:${String(workerIndex)}`);
        await new Promise((resolve) =>
          setTimeout(resolve, task.rowIndex === 1 ? 30 : 1),
        );
        events.push(`finish:${String(task.rowIndex)}:${String(workerIndex)}`);
        return {
          rowIndex: task.rowIndex,
          folio: String(task.rowIndex),
          status: "succeeded",
          durationMs: 1,
          propertyUsageType: "Residential",
          error: null,
        };
      },
      commitResults(results) {
        committed.push(results.map((result) => result.rowIndex));
        return Promise.resolve(false);
      },
    });

    expect(events.indexOf("start:2:0")).toBeLessThan(
      events.indexOf("finish:1:1"),
    );
    expect(committed).toEqual([[0], [1, 2]]);
  });
});
