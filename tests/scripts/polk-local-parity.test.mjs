import { createHash } from "node:crypto";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import * as path from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  buildPolkLocalParityStatus,
  buildPolkPermitAggregateSql,
  buildPolkPermitSummary,
  classifyPolkPermitTrades,
  parsePolkStatusCliOptions,
} from "../../scripts/polk-local-parity-lib.mjs";
import {
  createPolkDashboardServer,
  parsePolkDashboardOptions,
  renderPolkDashboard,
} from "../../scripts/polk-local-dashboard.mjs";

const require = createRequire(import.meta.url);
const duckdb = require("duckdb");

/** @type {string[]} */
const temporaryDirectories = [];

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { recursive: true, force: true })),
  );
});

/**
 * Create a test-owned temporary directory.
 *
 * @returns {Promise<string>} Absolute temporary directory path.
 */
async function createTemporaryDirectory() {
  const directory = await mkdtemp(path.join(tmpdir(), "polk-parity-"));
  temporaryDirectories.push(directory);
  return directory;
}

/**
 * Execute setup SQL against a temporary DuckDB file.
 *
 * @param {string} databasePath Destination database.
 * @param {string} sql Setup statements.
 * @returns {Promise<void>} Resolves after setup and close.
 */
async function executeDuckDb(databasePath, sql) {
  const database = new duckdb.Database(databasePath);
  const connection = database.connect();
  await new Promise((resolve, reject) => {
    connection.exec(sql, (error) => {
      if (error) reject(error);
      else resolve(undefined);
    });
  });
  await new Promise((resolve) => connection.close(() => resolve(undefined)));
  database.close();
}

/**
 * Create the closed Polk tables used by the evidence summarizer.
 *
 * @param {string} databasePath Destination database.
 * @returns {Promise<void>} Resolves after fixture creation.
 */
async function createPermitFixture(databasePath) {
  await executeDuckDb(
    databasePath,
    `
      CREATE TABLE polk_permits (
        description VARCHAR,
        permit_type VARCHAR,
        permit_number VARCHAR,
        agency_name VARCHAR,
        status VARCHAR,
        status_description VARCHAR,
        estimated_value VARCHAR,
        issue_date VARCHAR
      );
      INSERT INTO polk_permits VALUES
        ('REROOF WITH SOLAR PV ARRAY', 'ROOF', 'P-1', 'POLK COUNTY', 'C', 'Complete', '25000', '2026-01-03'),
        ('REPLACE HEAT PUMP', 'MECHANICAL', 'P-2', 'LAKELAND', 'P', 'Pending', NULL, '2025-02-04'),
        ('PAINT EXTERIOR', 'MAINTENANCE', NULL, NULL, 'C', 'Complete', NULL, NULL);
      CREATE TABLE polk_sites (
        parcel_id VARCHAR,
        postal_code VARCHAR
      );
      INSERT INTO polk_sites VALUES
        ('A', '33801'),
        ('B', '34759'),
        ('B', '34759');
    `,
  );
}

/**
 * Write JSON with stable test formatting.
 *
 * @param {string} filePath Destination path.
 * @param {unknown} value JSON-compatible value.
 * @returns {Promise<void>} Resolves after write.
 */
async function writeJson(filePath, value) {
  await mkdir(path.dirname(filePath), { recursive: true });
  await writeFile(filePath, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

describe("Polk permit enrichment evidence", () => {
  it("classifies overlapping permit trades from official text fields", () => {
    expect(
      classifyPolkPermitTrades({
        description: "Reroof and install rooftop photovoltaic array",
        permitType: "Building",
        permitNumber: "R-1",
      }),
    ).toEqual(["roofing", "solar"]);
    expect(
      classifyPolkPermitTrades({
        description: "Replace 3-ton heat pump and condenser",
      }),
    ).toEqual(["hvac"]);
    expect(classifyPolkPermitTrades({ description: "Interior paint" })).toEqual(
      [],
    );
  });

  it("builds a read-only aggregate query for all six trades", () => {
    const sql = buildPolkPermitAggregateSql();
    expect(sql).toContain("FROM polk_permits");
    expect(sql).toContain("AS roofing");
    expect(sql).toContain("AS hvac");
    expect(sql).toContain("AS solar");
    expect(sql).not.toMatch(/\b(?:INSERT|UPDATE|DELETE|COPY|ATTACH)\b/i);
  });

  it("reconciles official permits, trades, agencies, statuses, and ZIPs", async () => {
    const directory = await createTemporaryDirectory();
    const databasePath = path.join(directory, "polk.duckdb");
    await createPermitFixture(databasePath);

    const summary = await buildPolkPermitSummary(databasePath);

    expect(summary.permitCount).toBe(3);
    expect(summary.classifiedPermitCount).toBe(2);
    expect(summary.unclassifiedPermitCount).toBe(1);
    expect(summary.tradeCounts).toMatchObject({
      roofing: 1,
      solar: 1,
      hvac: 1,
    });
    expect(summary.earliestIssueDate).toBe("2025-02-04");
    expect(summary.latestIssueDate).toBe("2026-01-03");
    expect(summary.agencies).toContainEqual({
      value: "POLK COUNTY",
      count: 1,
    });
    expect(summary.zipPrefixes).toEqual([
      { value: "338", count: 1 },
      { value: "347", count: 1 },
    ]);
    expect(summary.contractorEnrichment.available).toBe(false);
    expect(summary.contractorEnrichment.reason).toMatch(
      /no contractor company/i,
    );
  });
});

describe("Polk evidence-only lifecycle status", () => {
  it("uses explicit local defaults and supports no-write status", () => {
    const options = parsePolkStatusCliOptions(
      ["--source-dir", "custom/full", "--no-write"],
      "/repo",
    );
    expect(options.sourceDirectory).toBe("/repo/custom/full");
    expect(options.workDatabase).toBe(
      "/repo/tmp/polk/bulk/extracted/polk-appraisal.duckdb",
    );
    expect(options.writeOutput).toBe(false);
  });

  it("marks only evidenced local stages complete", async () => {
    const root = await createTemporaryDirectory();
    const sourceDirectory = path.join(root, "full");
    const databasePath = path.join(root, "polk.duckdb");
    const queryTablePath = path.join(sourceDirectory, "query-table.parquet");
    await createPermitFixture(databasePath);
    await mkdir(path.join(sourceDirectory, ".state"), { recursive: true });
    const queryTableBody = Buffer.from("PAR1-test-query-table-PAR1");
    const queryTableHash = createHash("sha256")
      .update(queryTableBody)
      .digest("hex");
    await writeFile(queryTablePath, queryTableBody);
    await writeJson(path.join(sourceDirectory, "manifest.json"), {
      county: "polk",
      output: {
        propertyCount: 2,
        propertyBytes: 100,
        shardCount: 1,
        queryTable: {
          file: "query-table.parquet",
          rowCount: 2,
          sizeBytes: queryTableBody.byteLength,
          sha256: queryTableHash,
        },
        validation: {
          rowCount: 2,
          distinctParcels: 2,
          distinctPropertyIds: 2,
          nullCids: 0,
          ownerFieldViolations: 0,
        },
      },
    });
    await writeJson(path.join(sourceDirectory, "coverage.json"), {
      county: "polk",
      childRows: { permits: 3 },
    });
    await writeJson(path.join(sourceDirectory, ".state", "checkpoint.json"), {
      complete: true,
    });
    const overtureSummaryPath = path.join(
      root,
      "overture",
      "manifest",
      "summary.json",
    );
    await writeJson(overtureSummaryPath, {
      mode: "counts-only",
      county: "polk",
      clipCount: 123,
    });
    const publicationIndexPath = path.join(root, "publication", "index.json");
    await writeJson(publicationIndexPath, {
      county: "polk",
      propertyCount: 2,
    });
    const catalogPath = path.join(root, "published-counties.json");
    await writeJson(catalogPath, {
      counties: [],
    });
    const options = {
      sourceDirectory,
      workDatabase: databasePath,
      permitSummaryPath: path.join(root, "permit-summary.json"),
      permitEnrichmentReceiptPath: path.join(
        root,
        "missing-permit-enrichment.json",
      ),
      overtureSummaryPath,
      overturePublicationReceiptPath: path.join(
        root,
        "missing-overture-publication.json",
      ),
      sunbizManifestPath: path.join(root, "missing-sunbiz.json"),
      bbbSummaryPath: path.join(root, "missing-bbb.json"),
      neonReceiptPath: path.join(root, "missing-neon.json"),
      publicationIndexPath,
      catalogPath,
      outputPath: path.join(root, "status.json"),
      writeOutput: false,
    };

    const { status } = await buildPolkLocalParityStatus(options);

    expect(status.stages.appraisal.status).toBe("complete");
    expect(status.stages.permits.status).toBe("complete");
    expect(status.stages.overture).toMatchObject({
      status: "probed",
      count: 123,
    });
    expect(status.stages.publication.status).toBe("awaiting_human");
    expect(status.stages.sunbiz.status).toBe("blocked");
    expect(status.stages.bbb.status).toBe("blocked");
    expect(status.stages.permitEnrichment.status).toBe("blocked");
    expect(status.stages.queryDatabase.status).toBe("blocked");
    expect(status.stages.catalog.status).toBe("blocked");
    expect(status.pr200FunctionalParity).toBe(false);
  });
});

describe("Polk local dashboard", () => {
  const report = {
    schemaVersion: "oracle-node.polk-local-parity.v1",
    generatedAt: "2026-08-30T00:00:00.000Z",
    county: {
      key: "polk",
      name: "Polk",
      stateCode: "FL",
      countyFips: "12105",
    },
    stages: {
      appraisal: {
        name: "Local bulk appraisal export",
        status: "complete",
        evidence: "2 properties",
        count: 2,
      },
      sunbiz: {
        name: "Sunbiz property matching",
        status: "blocked",
        evidence: "Missing <source> & evidence",
        count: null,
      },
    },
    localArtifacts: {},
    blockers: ["Sunbiz: Missing <source> & evidence"],
    pr200FunctionalParity: false,
  };

  it("binds to loopback by default and validates ports", () => {
    expect(parsePolkDashboardOptions([])).toEqual({
      host: "127.0.0.1",
      port: 3889,
    });
    expect(() => parsePolkDashboardOptions(["--port", "70000"])).toThrow(
      /port/,
    );
  });

  it("renders current evidence without unsafe HTML", () => {
    const html = renderPolkDashboard(report);
    expect(html).toContain("Polk County local ingestion");
    expect(html).toContain("PR #200 parity: not yet evidenced");
    expect(html).toContain("2 properties");
    expect(html).toContain("Missing &lt;source&gt; &amp; evidence");
    expect(html).not.toContain("524,196");
  });

  it("serves the dashboard and JSON status from one evidence loader", async () => {
    const server = createPolkDashboardServer(async () => report);
    await new Promise((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", () => resolve(undefined));
    });
    try {
      const address = server.address();
      if (typeof address !== "object" || address === null) {
        throw new Error("Expected TCP test server address");
      }
      const base = `http://127.0.0.1:${address.port}`;
      const [htmlResponse, statusResponse] = await Promise.all([
        fetch(`${base}/`),
        fetch(`${base}/api/status`),
      ]);
      expect(htmlResponse.status).toBe(200);
      expect(await htmlResponse.text()).toContain(
        "Polk County local ingestion",
      );
      expect(await statusResponse.json()).toMatchObject({
        county: { key: "polk" },
        pr200FunctionalParity: false,
      });
    } finally {
      await new Promise((resolve) => server.close(() => resolve(undefined)));
    }
  });
});
