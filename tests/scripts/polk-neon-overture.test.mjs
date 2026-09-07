import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  buildPolkLocalLoadManifest,
  parsePolkNeonObservations,
  reconcilePolkNeon,
} from "../../scripts/polk/neon-reconciliation.mjs";
import {
  POLK_OVERTURE_PII_POLICY,
  buildPolkOverturePublicationPlan,
  parsePolkOverturePublicationOptions,
} from "../../scripts/polk/overture-publication.mjs";

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
 * Create a temporary directory.
 *
 * @returns {Promise<string>} Temporary directory.
 */
async function createTemporaryDirectory() {
  const directory = await mkdtemp(path.join(tmpdir(), "polk-neon-"));
  temporaryDirectories.push(directory);
  return directory;
}

/**
 * Write JSON fixture data.
 *
 * @param {string} filePath Destination path.
 * @param {unknown} value JSON-compatible value.
 * @returns {Promise<void>} Resolves after write.
 */
async function writeJson(filePath, value) {
  await mkdir(path.dirname(filePath), { recursive: true });
  await writeFile(filePath, `${JSON.stringify(value)}\n`, "utf8");
}

/**
 * Build a fully ready local manifest without filesystem dependencies.
 *
 * @returns {object} Local manifest.
 */
function readyLocalManifest() {
  return {
    schemaVersion: "oracle-node.polk-local-load-manifest.v1",
    generatedAt: "2026-08-31T00:00:00.000Z",
    county: "polk",
    tracks: [
      { source: "appraisal", localCount: 10, ready: true },
      { source: "permits", localCount: 20, ready: true },
      { source: "sunbiz", localCount: 5, ready: true },
      { source: "bbb", localCount: 3, ready: true },
      { source: "overture_places", localCount: 7, ready: true },
    ],
    loaderHandoff: {},
    ready: true,
  };
}

describe("Polk local-to-Neon receipts", () => {
  it("does not mark a local load ready when evidence manifests are absent", async () => {
    const root = await createTemporaryDirectory();
    const manifest = await buildPolkLocalLoadManifest({
      sourceDirectory: path.join(root, "full"),
      permitSummaryPath: path.join(root, "permit.json"),
      sunbizManifestPath: path.join(root, "sunbiz.json"),
      bbbManifestPath: path.join(root, "bbb.json"),
      overtureSummaryPath: path.join(root, "overture.json"),
    });

    expect(manifest.ready).toBe(false);
    expect(manifest.tracks).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          source: "sunbiz",
          localCount: null,
          ready: false,
          manifestSha256: null,
        }),
      ]),
    );
    expect(manifest.loaderHandoff).toMatchObject({
      executed: false,
      requiredJurisdictionKey: "polk_appraiser",
    });
  });

  it("combines CAMA and verified portal permits without counting licenses", async () => {
    const root = await createTemporaryDirectory();
    const sourceDirectory = path.join(root, "full");
    const permitSummaryPath = path.join(root, "permit.json");
    const permitNormalizationManifestPath = path.join(
      root,
      "permit-normalization.json",
    );
    await Promise.all([
      writeJson(path.join(sourceDirectory, "manifest.json"), {
        output: {
          propertyCount: 1,
          queryTable: { rowCount: 1 },
          validation: { distinctParcels: 1 },
        },
      }),
      writeJson(path.join(sourceDirectory, ".state", "checkpoint.json"), {
        complete: true,
      }),
      writeJson(permitSummaryPath, {
        schemaVersion: "oracle-node.polk-permit-enrichment.v1",
        permitCount: 7,
      }),
      writeJson(permitNormalizationManifestPath, {
        schemaVersion: "oracle-node.polk-permit-source-normalization.v1",
        county: "polk",
        queryDbPermitSourceSystem: "polk_permits",
        loadReadyPermitCount: 2,
        unmatchedPermitCount: 2,
        excludedRecordCounts: { contractor_license: 1 },
        pilot: false,
        complete: true,
      }),
    ]);

    const manifest = await buildPolkLocalLoadManifest({
      sourceDirectory,
      permitSummaryPath,
      permitNormalizationManifestPath,
      sunbizManifestPath: path.join(root, "sunbiz.json"),
      bbbManifestPath: path.join(root, "bbb.json"),
      overtureSummaryPath: path.join(root, "overture.json"),
    });

    expect(
      manifest.tracks.find((track) => track.source === "permits"),
    ).toMatchObject({
      localCount: 9,
      ready: true,
    });
  });

  it("requires exact coverage counts, timestamps, and direct places evidence", () => {
    const observations = parsePolkNeonObservations({
      schemaVersion: "oracle-node.polk-neon-observations.v1",
      observedAt: "2026-08-31T01:00:00.000Z",
      county: "polk",
      coverageRows: [
        {
          source: "appraisal",
          ingested_count: "10",
          expected_count: null,
          first_loaded_at: "2026-08-31T00:00:00Z",
          last_loaded_at: "2026-08-31T00:10:00Z",
        },
        {
          source: "permits",
          ingested_count: "20",
          expected_count: null,
          first_loaded_at: "2026-08-31T00:00:00Z",
          last_loaded_at: "2026-08-31T00:10:00Z",
        },
        {
          source: "sunbiz",
          ingested_count: "5",
          expected_count: null,
          first_loaded_at: "2026-08-31T00:00:00Z",
          last_loaded_at: "2026-08-31T00:10:00Z",
        },
        {
          source: "bbb",
          ingested_count: "3",
          expected_count: null,
          first_loaded_at: "2026-08-31T00:00:00Z",
          last_loaded_at: "2026-08-31T00:10:00Z",
        },
        {
          source: "overture_places",
          ingested_count: "7",
          expected_count: null,
          first_loaded_at: "2026-08-31T00:00:00Z",
          last_loaded_at: "2026-08-31T00:10:00Z",
        },
      ],
      places: {
        release: "2026-08-19.0",
        rowCount: "7",
        distinctGersIds: "7",
        extractionClipCount: 7,
        licenceGatePassed: true,
      },
    });

    const receipt = reconcilePolkNeon(readyLocalManifest(), observations);

    expect(receipt.complete).toBe(true);
    expect(receipt.tracks).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          source: "overture_places",
          countMatches: true,
          directPlacesMatch: true,
          passed: true,
        }),
      ]),
    );

    observations.coverageRows.find(
      (row) => row.source === "sunbiz",
    ).ingestedCount = 4;
    const failed = reconcilePolkNeon(readyLocalManifest(), observations);
    expect(failed.complete).toBe(false);
    expect(
      failed.tracks.find((track) => track.source === "sunbiz"),
    ).toMatchObject({ countMatches: false, passed: false });

    observations.coverageRows = observations.coverageRows.filter(
      (row) => row.source !== "permits",
    );
    const missing = reconcilePolkNeon(readyLocalManifest(), observations);
    expect(
      missing.tracks.find((track) => track.source === "permits"),
    ).toMatchObject({
      neonCoverageCount: null,
      timestampEvidenced: false,
      passed: false,
    });
  });
});

describe("Polk Overture publication orchestration", () => {
  it("requires a reconciled Neon places track before enabling export", () => {
    const options = {
      extractSummaryPath: "/tmp/extract.json",
      neonReceiptPath: "/tmp/neon.json",
      outputDirectory: "/tmp/publication",
      envFile: "/tmp/.env",
      release: "2026-08-19.0",
      executeExport: false,
      receiptPath: "/tmp/receipt.json",
    };
    const extract = {
      schemaVersion: "oracle-node.overture-places-extract.v1",
      county: "polk",
      overtureRelease: "2026-08-19.0",
      mode: "extract",
      clipCount: 7,
      jsonl: { recordCount: 7 },
      licenceGate: { passed: true, osmPresent: false },
    };
    const blocked = buildPolkOverturePublicationPlan(options, extract, null);
    expect(blocked.ready).toBe(false);
    expect(blocked.export.status).toBe("blocked");

    const ready = buildPolkOverturePublicationPlan(options, extract, {
      schemaVersion: "oracle-node.polk-neon-reconciliation.v1",
      county: "polk",
      tracks: [
        {
          source: "overture_places",
          localCount: 7,
          neonCoverageCount: 7,
          passed: true,
        },
      ],
    });
    expect(ready.ready).toBe(true);
    expect(ready.export.status).toBe("ready_for_read_only_neon_export");
    expect(ready.externalPublication).toMatchObject({
      status: "blocked_until_validated_neon_export",
      piiPolicy: {
        approved: true,
        decision: "publish_public_business_contacts",
      },
    });
    expect(ready.externalPublication.uploadCommand).toContain("--dry-run true");
    expect(POLK_OVERTURE_PII_POLICY.fields).toEqual(["phones", "emails"]);
    expect(ready.catalogHandoff.placesTableUrl).toContain(
      "verified-polk-places",
    );
  });

  it("defaults to planning and never executes an export implicitly", () => {
    expect(parsePolkOverturePublicationOptions([])).toMatchObject({
      release: "2026-08-19.0",
      executeExport: false,
      neonReceiptPath: "tmp/polk/neon/reconciliation-receipt.json",
    });
  });
});
