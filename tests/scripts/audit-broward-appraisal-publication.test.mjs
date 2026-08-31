import { spawnSync } from "node:child_process";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { fileURLToPath } from "node:url";

import { ParquetSchema, ParquetWriter } from "@dsnp/parquetjs";
import AdmZip from "adm-zip";
import { afterEach, describe, expect, it } from "vitest";

import {
  APPROVED_PUBLIC_FIELDS,
  auditBrowardAppraisalPublication,
  classifyDeniedName,
  parseCli,
} from "../../scripts/audit-broward-appraisal-publication.mjs";

const FIXTURE_DIRECTORY = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../fixtures/broward-privacy",
);

/** @type {string[]} */
const temporaryDirectories = [];

/**
 * @typedef {Record<string, unknown>} JsonObject
 *
 * @typedef {object} PrivateArtifactFixture
 * @property {string} folio Canonical fixture folio.
 * @property {Record<string, JsonObject>} entries ZIP entry payloads by path.
 *
 * @typedef {object} FixtureWorkspace
 * @property {string} root Temporary root.
 * @property {string} sourceDirectory Private transformed-artifact directory.
 * @property {string} publicDirectory Proposed public derivative directory.
 */

/**
 * Read a checked JSON fixture.
 *
 * @param {string} fileName Fixture file name.
 * @returns {Promise<unknown>} Parsed fixture value.
 */
async function readJsonFixture(fileName) {
  return JSON.parse(
    await readFile(path.join(FIXTURE_DIRECTORY, fileName), "utf8"),
  );
}

/**
 * Create canonical transformed ZIP fixtures without writing any real owner or
 * address data into test output or audit reports.
 *
 * @param {string} sourceDirectory Destination source directory.
 * @returns {Promise<string[]>} Fixture folios.
 */
async function writePrivateArtifacts(sourceDirectory) {
  const parsed = /** @type {{artifacts: PrivateArtifactFixture[]}} */ (
    await readJsonFixture("private-transformed-artifacts.json")
  );
  const folios = [];
  for (const artifact of parsed.artifacts) {
    const zip = new AdmZip();
    for (const [entryName, value] of Object.entries(artifact.entries)) {
      zip.addFile(entryName, Buffer.from(JSON.stringify(value), "utf8"));
    }
    zip.writeZip(path.join(sourceDirectory, `${artifact.folio}.zip`));
    folios.push(artifact.folio);
  }
  return folios;
}

/**
 * Return the physical public fixture schema.
 *
 * @param {boolean} unsafe Whether to append denied owner and address columns.
 * @returns {ParquetSchema} Fixture Parquet schema.
 */
function publicSchema(unsafe) {
  const fields = {
    property_id: { type: "UTF8" },
    parcel_identifier: { type: "UTF8" },
    source_system: { type: "UTF8" },
    county_name: { type: "UTF8" },
    county_fips: { type: "UTF8" },
    state_code: { type: "UTF8" },
    latitude: { type: "DOUBLE" },
    longitude: { type: "DOUBLE" },
    property_type: { type: "UTF8" },
    property_usage_type: { type: "UTF8" },
    built_year: { type: "INT64", optional: true },
    market_value: { type: "DOUBLE" },
  };
  if (unsafe) {
    return new ParquetSchema({
      ...fields,
      owner_name: { type: "UTF8" },
      address_street: { type: "UTF8" },
    });
  }
  return new ParquetSchema(fields);
}

/**
 * Write a proposed public Parquet and its required row-count manifest.
 *
 * @param {string} publicDirectory Destination directory.
 * @param {{unsafe?:boolean,rowLimit?:number,manifestRowCount?:number}} [options] Fixture mutations.
 * @returns {Promise<void>}
 */
async function writePublicDerivative(publicDirectory, options = {}) {
  const rows = /** @type {JsonObject[]} */ (
    await readJsonFixture("public-safe-rows.json")
  );
  const selectedRows = rows.slice(0, options.rowLimit ?? rows.length);
  const unsafe = options.unsafe ?? false;
  const writer = await ParquetWriter.openFile(
    publicSchema(unsafe),
    path.join(publicDirectory, "query-table.parquet"),
  );
  try {
    for (const row of selectedRows) {
      await writer.appendRow(
        unsafe
          ? {
              ...row,
              owner_name: "PRIVATE FIXTURE OWNER",
              address_street: "123 FIXTURE STREET",
            }
          : row,
      );
    }
  } finally {
    await writer.close();
  }
  await writeFile(
    path.join(publicDirectory, "manifest.json"),
    `${JSON.stringify({
      datasetId: "broward-appraisal-public-fixture",
      rowCount: options.manifestRowCount ?? selectedRows.length,
    })}\n`,
    "utf8",
  );
}

/**
 * Build an isolated source/public fixture pair.
 *
 * @returns {Promise<FixtureWorkspace>} Temporary fixture workspace.
 */
async function createFixtureWorkspace() {
  const root = await mkdtemp(
    path.join(tmpdir(), "broward-publication-audit-test-"),
  );
  temporaryDirectories.push(root);
  const sourceDirectory = path.join(root, "private");
  const publicDirectory = path.join(root, "public");
  await mkdir(sourceDirectory);
  await mkdir(publicDirectory);
  await writePrivateArtifacts(sourceDirectory);
  return { root, sourceDirectory, publicDirectory };
}

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { recursive: true, force: true })),
  );
});

describe("Broward appraisal privacy and publication audit", () => {
  it("classifies BCPA owner, contact, mailing, and source-link fields", () => {
    expect(classifyDeniedName("person_1.json")).toContain("owner_identity");
    expect(classifyDeniedName("ownerEmail")).toEqual(
      expect.arrayContaining(["owner_identity", "contact"]),
    );
    expect(
      classifyDeniedName("relationship_person_has_mailing_address"),
    ).toEqual(expect.arrayContaining(["owner_identity", "mailing_address"]));
    expect(classifyDeniedName("source_http_request")).toContain(
      "source_payload",
    );
    expect(classifyDeniedName("property_market_value_amount")).toEqual([]);
  });

  it("classifies raw artifacts as private but passes a stripped exact derivative", async () => {
    const workspace = await createFixtureWorkspace();
    await writePublicDerivative(workspace.publicDirectory);

    const report = await auditBrowardAppraisalPublication({
      transformedDirectory: workspace.sourceDirectory,
      validationSummaryPath: null,
      publicDirectory: workspace.publicDirectory,
      expectedCount: 2,
    });

    expect(report.source.reconciled).toBe(true);
    expect(report.source.rawPublicationAllowed).toBe(false);
    expect(report.source.deniedFindings.byCategory).toMatchObject({
      mailing_address: expect.any(Number),
      owner_identity: expect.any(Number),
    });
    const serializedReport = JSON.stringify(report);
    expect(serializedReport).not.toContain("PRIVATE FIXTURE");
    expect(serializedReport).not.toContain("Privacy Fixture");
    expect(serializedReport).not.toContain("123 FIXTURE STREET");
    expect(
      report.publicDerivative?.fields.every((field) =>
        APPROVED_PUBLIC_FIELDS.includes(field),
      ),
    ).toBe(true);
    expect(report.publicDerivative?.passed).toBe(true);
    expect(report.publicationGate).toMatchObject({
      passed: true,
      decision: "AUDIT_PASS_HUMAN_APPROVAL_REQUIRED",
      publicationAuthorized: false,
      humanApprovalRequired: true,
    });
  });

  it("refuses denied Parquet fields and unknown sidecars", async () => {
    const workspace = await createFixtureWorkspace();
    await writePublicDerivative(workspace.publicDirectory, { unsafe: true });
    await writeFile(
      path.join(workspace.publicDirectory, "donphan-verification.json"),
      '{"result":"pass"}\n',
      "utf8",
    );

    const report = await auditBrowardAppraisalPublication({
      transformedDirectory: workspace.sourceDirectory,
      validationSummaryPath: null,
      publicDirectory: workspace.publicDirectory,
      expectedCount: 2,
    });

    expect(report.publicDerivative?.passed).toBe(false);
    expect(report.publicDerivative?.deniedFindings.byCategory).toMatchObject({
      owner_identity: expect.any(Number),
      site_address: expect.any(Number),
      unsafe_sidecar: 1,
    });
    expect(report.publicationGate.decision).toBe("REFUSE_PUBLICATION");
  });

  it("refuses physical, manifest, and source count mismatches", async () => {
    const workspace = await createFixtureWorkspace();
    await writePublicDerivative(workspace.publicDirectory, {
      rowLimit: 1,
      manifestRowCount: 2,
    });

    const report = await auditBrowardAppraisalPublication({
      transformedDirectory: workspace.sourceDirectory,
      validationSummaryPath: null,
      publicDirectory: workspace.publicDirectory,
      expectedCount: 2,
    });

    expect(report.publicDerivative?.countChecks).toMatchObject({
      rowCountMatchesSource: false,
      rowCountMatchesExpected: false,
      manifestRowCountMatchesPhysical: false,
      parcelIdentitySetMatchesSource: false,
    });
    expect(
      report.publicDerivative?.deniedFindings.byCategory.count_mismatch,
    ).toBeGreaterThanOrEqual(4);
    expect(report.publicationGate.passed).toBe(false);
  });

  it("requires an explicit bounded denominator and keeps reports private", async () => {
    const workspace = await createFixtureWorkspace();
    await expect(
      auditBrowardAppraisalPublication({
        transformedDirectory: workspace.sourceDirectory,
        validationSummaryPath: null,
        publicDirectory: null,
        expectedCount: null,
      }),
    ).rejects.toThrow(/will not infer its denominator/u);

    expect(() =>
      parseCli([
        "--transformed-dir",
        workspace.sourceDirectory,
        "--expected-count",
        "2",
        "--public-dir",
        workspace.publicDirectory,
      ]),
    ).not.toThrow();
    expect(() =>
      parseCli([
        "--transformed-dir",
        workspace.sourceDirectory,
        "--expected-count",
        "0",
      ]),
    ).toThrow(/positive integer/u);

    const cliPath = path.resolve(
      path.dirname(fileURLToPath(import.meta.url)),
      "../../scripts/audit-broward-appraisal-publication.mjs",
    );
    const cliResult = spawnSync(
      process.execPath,
      [
        cliPath,
        "--transformed-dir",
        workspace.sourceDirectory,
        "--expected-count",
        "2",
        "--public-dir",
        workspace.publicDirectory,
        "--report",
        path.join(workspace.publicDirectory, "private-audit.json"),
      ],
      { encoding: "utf8" },
    );
    expect(cliResult.status).toBe(1);
    expect(cliResult.stderr).toMatch(/report must be outside --public-dir/u);
  });
});
