import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  buildLakelandWorkLocation,
  normalizeLakelandArcgisPermit,
  normalizePolkAccelaPermit,
  normalizePolkPermitSources,
  parsePolkPermitNormalizationOptions,
} from "../../scripts/polk/normalize-permit-sources.mjs";

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
 * Build a representative verified Accela list record.
 *
 * @param {"permit" | "license"} recordClass Published record class.
 * @returns {Record<string, unknown>} Source record.
 */
function accelaRecord(recordClass) {
  return {
    schemaVersion: "oracle-node.polk-county-accela-list-record.v1",
    sourceSystem: "polk_county_accela_csv",
    sourceRecordKey: `polk_accela:${recordClass}-1`,
    sourceUrl: `https://example.test/accela/${recordClass}-1`,
    retrievedAt: "2026-09-03T00:00:00.000Z",
    sourceWindow: {
      startDate: "2026-09-01",
      endDate: "2026-09-03",
    },
    permitNumber: recordClass === "permit" ? "BLD-H-1" : "LIC-H-1",
    recordType:
      recordClass === "permit" ? "Building Permit" : "Contractor Licensing",
    recordClass,
    address: "1 MAIN ST, BARTOW FL 33830",
    status: "Issued",
    sourceDate: "2026-09-01",
    projectName: "SOURCE PROJECT",
    description: "SOURCE DESCRIPTION",
    expirationDate: null,
    shortNotes: null,
    parcelIdentifier: null,
    propertyMatch: null,
  };
}

/**
 * Build a representative verified Lakeland ArcGIS permit record.
 *
 * @returns {Record<string, unknown>} Source record.
 */
function lakelandRecord() {
  return {
    schemaVersion: "oracle-node.polk-lakeland-arcgis-permit.v1",
    sourceSystem: "lakeland_arcgis_permit_layer",
    sourceRecordKey: "lakeland_arcgis:global-1",
    sourceObjectId: 1,
    sourceGlobalId: "global-1",
    sourceUrl: "https://example.test/arcgis/1",
    retrievedAt: "2026-09-03T00:00:00.000Z",
    permitNumber: "BP-1",
    description: "SCREEN ROOM",
    permitType: "Building",
    applicantName: "PUBLIC APPLICANT",
    appliedAt: "2026-01-01T00:00:00.000Z",
    approvedAt: "2026-01-02T00:00:00.000Z",
    issuedAt: "2026-01-03T00:00:00.000Z",
    jobValueUsd: 5000,
    siteAddress: {
      line1: "1609 PINEBERRY ST",
      city: "Lakeland",
      stateCode: "FL",
      postalCode: "33803",
      sourceAddressId: "53083",
    },
    sourceCoordinates: { x: 1, y: 2, wkid: 2237 },
    appliedFiscalYear: 2026,
    icon: "PERMIT",
    createdBy: "LakelandFLGIS",
    createdAt: "2026-09-02T00:00:00.000Z",
    updatedBy: "LakelandFLGIS",
    updatedAt: "2026-09-02T00:00:00.000Z",
    parcelIdentifier: null,
    propertyMatch: null,
  };
}

/**
 * Serialize records as canonical newline-terminated JSONL.
 *
 * @param {readonly Record<string, unknown>[]} records Source records.
 * @returns {string} JSONL text.
 */
function jsonl(records) {
  return records
    .map((record) => JSON.stringify(record))
    .join("\n")
    .concat("\n");
}

/**
 * Compute a lowercase SHA-256 digest.
 *
 * @param {string} text Source text.
 * @returns {string} Digest.
 */
function sha256(text) {
  return createHash("sha256").update(text).digest("hex");
}

/**
 * Write a source JSONL file and a matching complete harvest receipt.
 *
 * @param {object} params Fixture settings.
 * @param {string} params.directory Temporary directory.
 * @param {"accela" | "lakeland"} params.source Source contract.
 * @param {string} params.text JSONL text.
 * @returns {Promise<{input:string,receipt:string}>} Fixture paths.
 */
async function writeHarvestFixture(params) {
  const input = path.join(params.directory, `${params.source}.jsonl`);
  const receipt = path.join(params.directory, `${params.source}-receipt.json`);
  await writeFile(input, params.text, "utf8");
  const file = await stat(input);
  const recordCount = params.text.trim().split("\n").length;
  const payload =
    params.source === "accela"
      ? {
          schemaVersion: "oracle-node.polk-county-accela-list-harvest.v1",
          complete: true,
          outputBytes: file.size,
          outputSha256: sha256(params.text),
          accessibleRecordCount: recordCount,
          classCounts: { permit: 1, license: 1, other: 0 },
        }
      : {
          schemaVersion: "oracle-node.polk-lakeland-arcgis-harvest.v1",
          complete: true,
          outputBytes: file.size,
          outputSha256: sha256(params.text),
          harvestedRecordCount: recordCount,
        };
  await writeFile(receipt, `${JSON.stringify(payload)}\n`, "utf8");
  return { input, receipt };
}

describe("Polk permit source normalization", () => {
  it("parses deterministic defaults and a pilot limit", () => {
    expect(
      parsePolkPermitNormalizationOptions(["--limit", "25"]),
    ).toMatchObject({
      outputDirectory: "tmp/polk/permits/load-ready",
      manifest: "tmp/polk/permits/load-ready/normalization-manifest.json",
      limit: 25,
      recordsPerPart: 25_000,
    });
  });

  it("excludes contractor licenses without misclassifying the generic list date", () => {
    const permit = normalizePolkAccelaPermit(accelaRecord("permit"));
    const license = normalizePolkAccelaPermit(accelaRecord("license"));

    expect(permit.record).toMatchObject({
      source_system: "polk_county_accela_csv",
      permit_number: "BLD-H-1",
      parcel_identifier: null,
      permit_issue_date: null,
      work_location: "1 MAIN ST, BARTOW FL 33830",
    });
    expect(permit.record?.raw).toMatchObject({ sourceDate: "2026-09-01" });
    expect(license).toEqual({
      record: null,
      excludedReason: "contractor_license",
    });
  });

  it("maps only Lakeland source-evidenced issue and address fields", () => {
    const normalized = normalizeLakelandArcgisPermit(lakelandRecord());

    expect(buildLakelandWorkLocation(lakelandRecord().siteAddress)).toBe(
      "1609 PINEBERRY ST, Lakeland, FL 33803",
    );
    expect(normalized.record).toMatchObject({
      source_system: "lakeland_arcgis_permit_layer",
      city: "Lakeland",
      permit_number: "BP-1",
      permit_issue_date: "2026-01-03",
      parcel_identifier: null,
    });
    expect(normalized.record?.raw).toMatchObject({
      appliedAt: "2026-01-01T00:00:00.000Z",
      jobValueUsd: 5000,
    });
  });

  it("writes reconciled load-ready files and a manifest", async () => {
    const directory = await mkdtemp(
      path.join(tmpdir(), "polk-permit-normalization-"),
    );
    temporaryDirectories.push(directory);
    const accela = await writeHarvestFixture({
      directory,
      source: "accela",
      text: jsonl([accelaRecord("permit"), accelaRecord("license")]),
    });
    const lakeland = await writeHarvestFixture({
      directory,
      source: "lakeland",
      text: jsonl([lakelandRecord()]),
    });
    const outputDirectory = path.join(directory, "load-ready");
    const manifest = await normalizePolkPermitSources({
      accelaInput: accela.input,
      accelaReceipt: accela.receipt,
      lakelandInput: lakeland.input,
      lakelandReceipt: lakeland.receipt,
      outputDirectory,
      manifest: path.join(outputDirectory, "manifest.json"),
      limit: null,
      recordsPerPart: 1,
    });
    const accelaOutput = await readFile(
      path.join(outputDirectory, "polk-county-accela-permits.normalized.jsonl"),
      "utf8",
    );

    expect(accelaOutput.trim().split("\n")).toHaveLength(1);
    expect(manifest).toMatchObject({
      queryDbPermitSourceSystem: "polk_permits",
      sourceRecordCount: 3,
      loadReadyPermitCount: 2,
      excludedRecordCounts: { contractor_license: 1 },
      unmatchedPermitCount: 2,
      recordsPerPart: 1,
      pilot: false,
      complete: true,
    });
    expect(manifest.sources).toEqual(
      expect.arrayContaining([expect.objectContaining({ partCount: 1 })]),
    );
  });

  it("fails closed when a verified harvest changes", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "polk-permit-tamper-"));
    temporaryDirectories.push(directory);
    const accela = await writeHarvestFixture({
      directory,
      source: "accela",
      text: jsonl([accelaRecord("permit"), accelaRecord("license")]),
    });
    const lakeland = await writeHarvestFixture({
      directory,
      source: "lakeland",
      text: jsonl([lakelandRecord()]),
    });
    await writeFile(accela.input, `${await readFile(accela.input, "utf8")} `);

    await expect(
      normalizePolkPermitSources({
        accelaInput: accela.input,
        accelaReceipt: accela.receipt,
        lakelandInput: lakeland.input,
        lakelandReceipt: lakeland.receipt,
        outputDirectory: path.join(directory, "load-ready"),
        manifest: path.join(directory, "load-ready", "manifest.json"),
        limit: null,
        recordsPerPart: 1,
      }),
    ).rejects.toThrow(/byte count changed/);
  });
});
