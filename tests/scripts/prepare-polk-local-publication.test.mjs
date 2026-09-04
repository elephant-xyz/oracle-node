import { createHash } from "node:crypto";
import {
  mkdtemp,
  mkdir,
  readFile,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  buildPolkPublicCoverageSnapshot,
  buildPolkPublicationPlan,
  inventoryPolkPublicationSource,
  parsePolkPublicationCliOptions,
  parsePolkPublicationEntry,
  runPreparePolkLocalPublication,
} from "../../scripts/prepare-polk-local-publication.mjs";

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
 * Write JSON fixture data.
 *
 * @param {string} filePath Destination path.
 * @param {unknown} value JSON-compatible fixture.
 * @returns {Promise<void>} Resolves after write.
 */
async function writeJson(filePath, value) {
  await mkdir(path.dirname(filePath), { recursive: true });
  await writeFile(filePath, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

/**
 * Create one complete, reconciled local Polk export fixture.
 *
 * @returns {Promise<{root:string,sourceDirectory:string,outputDirectory:string,propertyPath:string,queryTableBody:Buffer}>} Fixture paths.
 */
async function createPublicationFixture() {
  const root = await mkdtemp(path.join(tmpdir(), "polk-publication-"));
  temporaryDirectories.push(root);
  const sourceDirectory = path.join(root, "full");
  const outputDirectory = path.join(root, "prepared");
  const propertyRelativePath = "properties/aa/253006000000023190.json";
  const propertyPath = path.join(sourceDirectory, propertyRelativePath);
  const propertyBody = Buffer.from('{"county":"polk"}\n');
  const propertyHash = createHash("sha256").update(propertyBody).digest("hex");
  const queryTableBody = Buffer.from("PAR1-polk-fixture-PAR1");
  const queryTableHash = createHash("sha256")
    .update(queryTableBody)
    .digest("hex");
  await mkdir(path.dirname(propertyPath), { recursive: true });
  await writeFile(propertyPath, propertyBody);
  await writeFile(
    path.join(sourceDirectory, "query-table.parquet"),
    queryTableBody,
  );
  await writeJson(
    path.join(sourceDirectory, "manifests", "shard-000000.json"),
    {
      entries: [
        {
          propertyId: "11111111-1111-5111-8111-111111111111",
          parcelIdentifier: "253006000000023190",
          file: propertyRelativePath,
          fileSizeBytes: propertyBody.byteLength,
          sha256: propertyHash,
          cid: "QmFixturePropertyCid",
        },
      ],
    },
  );
  await writeJson(path.join(sourceDirectory, "manifest.json"), {
    county: "polk",
    run: {
      startedAt: "2026-08-28T10:00:00.000Z",
      completedAt: "2026-08-28T10:01:00.000Z",
    },
    output: {
      propertyCount: 1,
      propertyBytes: propertyBody.byteLength,
      queryTable: {
        file: "query-table.parquet",
        rowCount: 1,
        sizeBytes: queryTableBody.byteLength,
        sha256: queryTableHash,
      },
      validation: {
        rowCount: 1,
        distinctParcels: 1,
        distinctPropertyIds: 1,
        nullCids: 0,
        ownerFieldViolations: 0,
      },
    },
  });
  await writeJson(path.join(sourceDirectory, "coverage.json"), {
    county: "polk",
    childRows: { permits: 7 },
    privacy: { passed: true },
  });
  await writeJson(path.join(sourceDirectory, ".state", "checkpoint.json"), {
    complete: true,
  });
  await writeJson(
    path.join(root, "permits", "load-ready", "normalization-manifest.json"),
    {
      schemaVersion: "oracle-node.polk-permit-source-normalization.v1",
      county: "polk",
      queryDbPermitSourceSystem: "polk_permits",
      loadReadyPermitCount: 2,
      unmatchedPermitCount: 2,
      excludedRecordCounts: { contractor_license: 1 },
      pilot: false,
      complete: true,
    },
  );
  return {
    root,
    sourceDirectory,
    outputDirectory,
    propertyPath,
    queryTableBody,
  };
}

describe("Polk publication preparation", () => {
  it("defaults to a no-write dry run and requires explicit materialization", () => {
    expect(parsePolkPublicationCliOptions([], "/repo")).toEqual({
      sourceDirectory: "/repo/tmp/polk/full",
      outputDirectory: "/repo/tmp/polk/publication-prepared",
      permitEnrichmentReceiptPath:
        "/repo/tmp/polk/permits/enrichment-receipt.json",
      permitNormalizationManifestPath:
        "/repo/tmp/polk/permits/load-ready/normalization-manifest.json",
      permitPublicationReceiptPath:
        "/repo/tmp/polk/permits/permit-table-publication-receipt.json",
      sunbizManifestPath: "/repo/tmp/polk/sunbiz/transformed/manifest.json",
      bbbManifestPath: "/repo/tmp/polk/bbb/manifest/contractor-crm.json",
      neonReceiptPath: "/repo/tmp/polk/neon/reconciliation-receipt.json",
      overturePublicationReceiptPath:
        "/repo/tmp/polk/overture/2026-08-19.0/publication-receipt.json",
      shardSize: 10000,
      materialize: false,
    });
    expect(
      parsePolkPublicationCliOptions(
        ["--materialize", "--shard-size", "250"],
        "/repo",
      ),
    ).toMatchObject({
      shardSize: 250,
      materialize: true,
    });
    expect(() =>
      parsePolkPublicationCliOptions(["--materialize", "--dry-run"], "/repo"),
    ).toThrow(/only one/i);
  });

  it("rejects unsafe or private-looking publication entries", () => {
    expect(() =>
      parsePolkPublicationEntry(
        {
          propertyId: "11111111-1111-5111-8111-111111111111",
          parcelIdentifier: "253006000000023190",
          file: "../owner.json",
          fileSizeBytes: 10,
          sha256: "a".repeat(64),
          cid: "QmExample",
        },
        "shard.json",
      ),
    ).toThrow(/unsafe/i);
  });

  it("reconciles local properties, query table, checkpoint, and privacy evidence", async () => {
    const fixture = await createPublicationFixture();
    const options = {
      sourceDirectory: fixture.sourceDirectory,
      outputDirectory: fixture.outputDirectory,
      shardSize: 10000,
      materialize: false,
    };

    const inventory = await inventoryPolkPublicationSource(
      fixture.sourceDirectory,
    );
    const { plan } = await buildPolkPublicationPlan(options);

    expect(inventory).toMatchObject({
      propertyCount: 1,
      propertyBytes: 18,
    });
    expect(plan.validation).toMatchObject({
      passed: true,
      propertyCount: 1,
      permitCount: 9,
      propertyPermitCount: 7,
      portalPermitCount: 2,
      privacyPassed: true,
      checkpointComplete: true,
    });
    expect(plan.families).toMatchObject({
      openData: {
        requiredBucket: "elephant-oracle-open-data-polk",
        requiredIpnsLabel: "oracle-open-data-polk",
        externalStatus: "awaiting_human_approval",
      },
      queryTable: {
        requiredIpnsLabel: "oracle-query-table-polk",
      },
      permitQueryTable: {
        requiredIpnsLabel: "oracle-permit-table-polk",
        expectedRowCount: 9,
      },
      places: {
        externalStatus:
          "blocked_until_neon_load_reconciliation_and_publication_review",
        localExtractStatus: "missing_or_unvalidated",
      },
    });
    expect(plan.catalogRegistration).toMatchObject({
      status: "blocked_until_stable_public_urls_are_gateway_verified",
      countyEntryTemplate: {
        countyKey: "polk",
        countyFips: "12105",
        queryTableUrl: "<verified-query-table-ipns-url>",
        permitQueryTableUrl: "<verified-permit-table-ipns-url>",
      },
    });
    expect(plan.status).toBe("dry_run");
    expect(plan.parityEvidenceReady).toBe(false);
  });

  it("builds canonical coverage without claiming unpublished enrichments", () => {
    expect(
      buildPolkPublicCoverageSnapshot({
        propertyCount: 438612,
        permitCount: 1128608,
        exportedAt: "2026-08-28T22:31:53.851Z",
        completedAt: "2026-08-28T22:44:54.220Z",
        openDataCid: "QmPolkOpenDataIndex",
      }),
    ).toEqual({
      county: "polk",
      exportedAt: "2026-08-28T22:44:54.220Z",
      datasets: [
        {
          county: "polk",
          source: "appraisal",
          ingested_count: 438612,
          expected_count: 438612,
          first_loaded_at: "2026-08-28T22:31:53.851Z",
          last_loaded_at: "2026-08-28T22:44:54.220Z",
          cid: "QmPolkOpenDataIndex",
          ipns_label: "oracle-open-data-polk",
        },
        {
          county: "polk",
          source: "permits",
          ingested_count: 1128608,
          expected_count: 1128608,
          first_loaded_at: "2026-08-28T22:31:53.851Z",
          last_loaded_at: "2026-08-28T22:44:54.220Z",
          cid: null,
          ipns_label: null,
        },
        {
          county: "polk",
          source: "corporate",
          ingested_count: 0,
          expected_count: null,
          first_loaded_at: null,
          last_loaded_at: null,
          cid: null,
          ipns_label: null,
        },
        {
          county: "polk",
          source: "bbb",
          ingested_count: 0,
          expected_count: null,
          first_loaded_at: null,
          last_loaded_at: null,
          cid: null,
          ipns_label: null,
        },
        {
          county: "polk",
          source: "overture_places",
          ingested_count: 0,
          expected_count: null,
          first_loaded_at: null,
          last_loaded_at: null,
          cid: null,
          ipns_label: null,
        },
      ],
    });
  });

  it("promotes enrichment coverage only from passed Neon track receipts", () => {
    const snapshot = buildPolkPublicCoverageSnapshot({
      propertyCount: 10,
      permitCount: 20,
      exportedAt: "2026-08-28T22:31:53.851Z",
      completedAt: "2026-08-28T22:44:54.220Z",
      openDataCid: "QmPolkOpenDataIndex",
      permitTableCid: "QmPolkPermitTable",
      permitTableIpnsLabel: "oracle-permit-table-polk",
      reconciledTracks: [
        {
          source: "permits",
          localCount: 20,
          neonCoverageCount: 20,
          firstLoadedAt: "2026-08-28T22:39:00.000Z",
          lastLoadedAt: "2026-08-28T22:42:00.000Z",
          passed: true,
        },
        {
          source: "sunbiz",
          localCount: 5,
          neonCoverageCount: 5,
          firstLoadedAt: "2026-08-28T22:40:00.000Z",
          lastLoadedAt: "2026-08-28T22:41:00.000Z",
          passed: true,
        },
        {
          source: "bbb",
          localCount: 3,
          neonCoverageCount: 2,
          firstLoadedAt: "2026-08-28T22:40:00.000Z",
          lastLoadedAt: "2026-08-28T22:41:00.000Z",
          passed: true,
        },
      ],
    });

    expect(snapshot.datasets[1]).toMatchObject({
      source: "permits",
      ingested_count: 20,
      expected_count: 20,
      first_loaded_at: "2026-08-28T22:39:00.000Z",
      last_loaded_at: "2026-08-28T22:42:00.000Z",
      cid: "QmPolkPermitTable",
      ipns_label: "oracle-permit-table-polk",
    });
    expect(snapshot.datasets[2]).toMatchObject({
      source: "corporate",
      ingested_count: 5,
      expected_count: 5,
    });
    expect(snapshot.datasets[3]).toMatchObject({
      source: "bbb",
      ingested_count: 0,
      expected_count: null,
    });
  });

  it("recognizes a full licence-gated Polk Overture extract as ready for load", async () => {
    const fixture = await createPublicationFixture();
    await writeJson(
      path.join(
        fixture.root,
        "overture",
        "2026-08-19.0",
        "extract",
        "manifest",
        "summary.json",
      ),
      {
        county: "polk",
        mode: "extract",
        clipCount: 30079,
        licenceGate: {
          passed: true,
          osmPresent: false,
          unknownDatasets: [],
        },
      },
    );

    const { plan } = await buildPolkPublicationPlan({
      sourceDirectory: fixture.sourceDirectory,
      outputDirectory: fixture.outputDirectory,
      shardSize: 10000,
      materialize: false,
    });

    expect(plan.families.places).toMatchObject({
      localExtractStatus: "ready_for_neon_load",
      localPlaceCount: 30079,
      externalStatus:
        "blocked_until_neon_load_reconciliation_and_publication_review",
    });
    expect(plan.externalActions.at(-1)).toMatch(
      /load and reconcile the completed Polk Overture extract/i,
    );
  });

  it("materializes separate open-data, query-table, and coverage families locally", async () => {
    const fixture = await createPublicationFixture();
    await writeJson(
      path.join(
        fixture.root,
        "permits",
        "permit-table-publication-receipt.json",
      ),
      {
        schemaVersion: "oracle-node.polk-permit-table-publication.v1",
        county: "polk",
        rowCount: 9,
        distinctPermitIds: 9,
        cid: "QmPolkPermitTable",
        ipnsLabel: "oracle-permit-table-polk",
        ipnsName: "k51-polk-permit-table",
        gatewayUrl: "https://ipfs.filebase.io/ipns/k51-polk-permit-table",
        gatewayVerified: true,
        firstFourBytes: "PAR1",
      },
    );

    const plan = await runPreparePolkLocalPublication([
      "--source-dir",
      fixture.sourceDirectory,
      "--output-dir",
      fixture.outputDirectory,
      "--permit-normalization-manifest",
      path.join(
        fixture.root,
        "permits",
        "load-ready",
        "normalization-manifest.json",
      ),
      "--permit-publication-receipt",
      path.join(
        fixture.root,
        "permits",
        "permit-table-publication-receipt.json",
      ),
      "--materialize",
      "--shard-size",
      "1",
    ]);

    const openDataIndex = JSON.parse(
      await readFile(
        path.join(fixture.outputDirectory, "open-data", "index.json"),
        "utf8",
      ),
    );
    const stagedProperty = path.join(
      fixture.outputDirectory,
      "open-data",
      "properties",
      "11111111-1111-5111-8111-111111111111.json",
    );
    const [sourceInfo, stagedInfo] = await Promise.all([
      stat(fixture.propertyPath),
      stat(stagedProperty),
    ]);
    expect(openDataIndex).toMatchObject({
      county: "polk",
      propertyCount: 1,
      shardSize: 1,
    });
    expect(sourceInfo.ino).toBe(stagedInfo.ino);
    expect(
      await readFile(
        path.join(
          fixture.outputDirectory,
          "query-table",
          "query-table.parquet",
        ),
      ),
    ).toEqual(fixture.queryTableBody);
    const publicCoverage = JSON.parse(
      await readFile(
        path.join(
          fixture.outputDirectory,
          "dataset-coverage",
          "dataset-coverage.json",
        ),
        "utf8",
      ),
    );
    expect(publicCoverage.datasets[1]).toMatchObject({
      source: "permits",
      ingested_count: 9,
      expected_count: 9,
      cid: "QmPolkPermitTable",
      ipns_label: "oracle-permit-table-polk",
    });
    expect(plan.families.permitQueryTable.externalStatus).toBe(
      "published_and_gateway_verified",
    );
    expect(plan.status).toBe("materialized");
  });

  it("fails closed when query-table bytes differ from the manifest", async () => {
    const fixture = await createPublicationFixture();
    await writeFile(
      path.join(fixture.sourceDirectory, "query-table.parquet"),
      "changed",
    );

    await expect(
      buildPolkPublicationPlan({
        sourceDirectory: fixture.sourceDirectory,
        outputDirectory: fixture.outputDirectory,
        shardSize: 10000,
        materialize: false,
      }),
    ).rejects.toThrow(/publication gate failed/i);
  });
});
