import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import * as path from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  buildPolkPropertyAddressKey,
  collectPolkSunbizAddressCandidates,
  derivePolkSunbizZips,
  normalizePolkStreetAddress,
  transformAndMatchPolkSunbiz,
} from "../../scripts/polk/sunbiz-local.mjs";

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
 * Create a temporary directory owned by one test.
 *
 * @returns {Promise<string>} Temporary directory.
 */
async function createTemporaryDirectory() {
  const directory = await mkdtemp(path.join(tmpdir(), "polk-sunbiz-"));
  temporaryDirectories.push(directory);
  return directory;
}

/**
 * Execute fixture SQL against a DuckDB file.
 *
 * @param {string} databasePath Database path.
 * @param {string} sql Fixture SQL.
 * @returns {Promise<void>} Resolves after close.
 */
async function executeDuckDb(databasePath, sql) {
  const database = new duckdb.Database(databasePath);
  const connection = database.connect();
  await new Promise((resolve, reject) => {
    connection.exec(sql, (error) => {
      if (error !== null) reject(error);
      else resolve(undefined);
    });
  });
  await new Promise((resolve) => connection.close(() => resolve(undefined)));
  database.close();
}

/**
 * Build a complete address object used by the Sunbiz transform.
 *
 * @param {string | null} line1 Street line.
 * @param {string | null} city City.
 * @param {string | null} zip ZIP.
 * @returns {object} Sunbiz address fixture.
 */
function sunbizAddress(line1, city, zip) {
  const singleLine = [line1, city, "FL", zip].filter(Boolean).join(" ");
  return {
    line1,
    line2: null,
    city,
    state: "FL",
    zip,
    country: "US",
    singleLine,
    normalized: singleLine.toUpperCase(),
  };
}

/**
 * Build one minimal valid ZIP-extracted Sunbiz record.
 *
 * @returns {object} Extraction record.
 */
function sunbizRecord() {
  return {
    sourceFileName: "cordata1.txt",
    sourceLineNumber: 1,
    entity: {
      schemaVersion: "fixture",
      source: "sunbiz",
      documentNumber: "L26000000001",
      entityName: "POLK FIXTURE LLC",
      statusCode: "A",
      status: "ACTIVE",
      filingTypeCode: "LC",
      filingType: "FLORIDA LIMITED LIABILITY COMPANY",
      principalAddress: sunbizAddress("123 Main Street", "Lakeland", "33801"),
      mailingAddress: sunbizAddress("PO BOX 1", "Lakeland", "33802"),
      filedDate: "2026-01-01",
      feiNumber: null,
      moreThanSixOfficers: false,
      lastTransactionDate: null,
      stateCountry: "FL",
      annualReports: [],
      registeredAgent: {
        name: null,
        type: null,
        address: sunbizAddress(null, null, null),
      },
      officers: [],
      rawRecordLength: 100,
    },
    matchedAddresses: [
      {
        role: "principalAddress",
        matchedZipPrefix: "33801",
        zip: "33801",
        officerOrdinal: null,
        officerTitle: null,
        officerName: null,
        address: sunbizAddress("123 Main Street", "Lakeland", "33801"),
      },
    ],
  };
}

describe("Polk Sunbiz ZIP and address evidence", () => {
  it("derives exact five-digit ZIPs instead of broad prefixes", async () => {
    const root = await createTemporaryDirectory();
    const databasePath = path.join(root, "polk.duckdb");
    await executeDuckDb(
      databasePath,
      `
        CREATE TABLE polk_sites (
          parcel_id VARCHAR,
          street_number VARCHAR,
          street_number_suffix VARCHAR,
          street_prefix VARCHAR,
          street VARCHAR,
          street_suffix VARCHAR,
          street_suffix_direction VARCHAR,
          unit VARCHAR,
          city VARCHAR,
          postal_code VARCHAR
        );
        INSERT INTO polk_sites VALUES
          ('1', '123', NULL, NULL, 'MAIN', 'ST', NULL, NULL, 'LAKELAND', '33801'),
          ('2', '1', NULL, NULL, 'BROADWAY', 'AVE', NULL, NULL, 'DAVENPORT', '33837-0001'),
          ('3', '8', NULL, NULL, 'LAKE', 'RD', NULL, NULL, 'DAVENPORT', '33837');
      `,
    );

    expect(await derivePolkSunbizZips(databasePath)).toEqual([
      "33801",
      "33837",
    ]);
  });

  it("normalizes equivalent street suffixes while preserving exact city and ZIP", () => {
    expect(normalizePolkStreetAddress("123 Main Street")).toBe("123 MAIN ST");
    expect(
      buildPolkPropertyAddressKey({
        street: "123 Main Street",
        city: "Lakeland",
        zip: "33801-1234",
      }),
    ).toBe("123 MAIN ST|LAKELAND|33801");
  });

  it("collects only complete Florida address candidates", () => {
    expect(collectPolkSunbizAddressCandidates(sunbizRecord())).toEqual([
      expect.objectContaining({
        documentNumber: "L26000000001",
        role: "PRINCIPAL",
        street: "123 Main Street",
        city: "Lakeland",
        zip: "33801",
      }),
      expect.objectContaining({
        role: "MAILING",
        street: "PO BOX 1",
      }),
    ]);
  });
});

describe("Polk Sunbiz local transform and property matching", () => {
  it("writes lexicon artifacts and a digest-backed exact property match receipt", async () => {
    const root = await createTemporaryDirectory();
    const databasePath = path.join(root, "polk.duckdb");
    const inputDirectory = path.join(root, "filter");
    const outputDirectory = path.join(root, "transformed");
    await executeDuckDb(
      databasePath,
      `
        CREATE TABLE polk_sites (
          parcel_id VARCHAR,
          street_number VARCHAR,
          street_number_suffix VARCHAR,
          street_prefix VARCHAR,
          street VARCHAR,
          street_suffix VARCHAR,
          street_suffix_direction VARCHAR,
          unit VARCHAR,
          city VARCHAR,
          postal_code VARCHAR
        );
        INSERT INTO polk_sites VALUES
          ('252801000000000010', '123', NULL, NULL, 'MAIN', 'ST', NULL, NULL, 'LAKELAND', '33801');
      `,
    );
    await mkdir(path.join(inputDirectory, "chunks"), { recursive: true });
    await writeFile(
      path.join(inputDirectory, "chunks", "cordata1-chunk-00000.jsonl"),
      `${JSON.stringify(sunbizRecord())}\n`,
      "utf8",
    );
    await writeFile(
      path.join(inputDirectory, "manifest.json"),
      `${JSON.stringify({
        schemaVersion: "oracle-node.polk-sunbiz-filter.v1",
        county: "polk",
        matchedRecordCount: 1,
        complete: true,
      })}\n`,
      "utf8",
    );

    const manifest = await transformAndMatchPolkSunbiz({
      inputDirectory,
      workDatabase: databasePath,
      outputDirectory,
      maxRecords: null,
    });

    expect(manifest).toMatchObject({
      county: "polk",
      sourceRecordCount: 1,
      transformedRecordCount: 1,
      invalidRecordCount: 0,
      complete: true,
      propertyMatching: {
        matchedAddressCount: 1,
        matchedDocumentCount: 1,
        matchedPropertyCount: 1,
        ambiguousAddressCount: 0,
        matchMethod: "exact_normalized_street_city_zip",
      },
    });
    const matchText = await readFile(
      path.join(outputDirectory, "matches", "property-address-matches.jsonl"),
      "utf8",
    );
    expect(JSON.parse(matchText)).toMatchObject({
      documentNumber: "L26000000001",
      parcelIdentifiers: ["252801000000000010"],
      matchConfidence: "exact_unique",
    });
    expect(manifest.outputArtifacts).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          dataset: "classes/business_registration",
          recordCount: 1,
        }),
        expect.objectContaining({
          dataset: "relationships/company_has_business_registration",
          recordCount: 1,
        }),
      ]),
    );
  });
});
