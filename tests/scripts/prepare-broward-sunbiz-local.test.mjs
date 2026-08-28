import AdmZip from "adm-zip";
import { mkdtemp, readFile, rm, writeFile } from "fs/promises";
import { tmpdir } from "os";
import path from "path";
import { describe, expect, it } from "vitest";

import {
  createAddressValidationKey,
  loadAddressValidationManifest,
  loadBrowardSunbizConfiguration,
  prepareBrowardSunbizLocal,
} from "../../scripts/prepare-broward-sunbiz-local.mjs";
import { parseCorporateDataRecord } from "../../workflow/lambdas/permit-harvest-worker/sunbiz-corporate.mjs";
import {
  buildBrowardReconciliationSampleLines,
  buildCorporateFixtureLine,
} from "../fixtures/sunbiz/corporate-fixture.mjs";

const EXPECTED_BROWARD_ZIPS = [
  "33004",
  "33009",
  "33019",
  "33020",
  "33021",
  "33023",
  "33024",
  "33025",
  "33026",
  "33027",
  "33028",
  "33029",
  "33060",
  "33062",
  "33063",
  "33064",
  "33065",
  "33066",
  "33067",
  "33068",
  "33069",
  "33071",
  "33073",
  "33076",
  "33301",
  "33304",
  "33305",
  "33306",
  "33308",
  "33309",
  "33311",
  "33312",
  "33313",
  "33314",
  "33315",
  "33316",
  "33317",
  "33319",
  "33321",
  "33322",
  "33323",
  "33324",
  "33325",
  "33326",
  "33327",
  "33328",
  "33330",
  "33331",
  "33332",
  "33334",
  "33351",
  "33388",
  "33394",
  "33441",
  "33442",
];

/**
 * Read non-empty JSONL rows as unknown values.
 *
 * @param {string} filePath - JSONL file path.
 * @returns {Promise<unknown[]>} Parsed JSONL values.
 */
async function readJsonl(filePath) {
  const text = await readFile(filePath, "utf8");
  return text
    .split(/\r?\n/)
    .filter((line) => line.trim())
    .map((line) => /** @type {unknown} */ (JSON.parse(line)));
}

/**
 * Require a fixture row to parse with the shared production parser.
 *
 * @param {string} line - Sunbiz fixed-width fixture row.
 * @returns {NonNullable<ReturnType<typeof parseCorporateDataRecord>>} Parsed corporate record.
 */
function parseFixtureRecord(line) {
  const record = parseCorporateDataRecord(line);
  if (!record) throw new Error("Expected fixture to have a document number");
  return record;
}

/**
 * Build local inside/outside decisions for the bundled reconciliation sample.
 *
 * @param {string[]} lines - Bundled fixed-width sample lines.
 * @returns {Array<{ validationKey: string, status: "inside" | "outside", countyFips: string, evidence: string }>} Strict local validation decisions.
 */
function buildSampleValidationEntries(lines) {
  const inside = parseFixtureRecord(lines[0] ?? "").principalAddress;
  const outside = parseFixtureRecord(lines[1] ?? "").principalAddress;
  return [
    {
      validationKey: createAddressValidationKey(inside),
      status: "inside",
      countyFips: "12011",
      evidence:
        "Bundled test boundary fixture: point intersects Broward FIPS 12011",
    },
    {
      validationKey: createAddressValidationKey(outside),
      status: "outside",
      countyFips: "12086",
      evidence:
        "Bundled test boundary fixture: point intersects Miami-Dade FIPS 12086",
    },
  ];
}

describe("local Broward Sunbiz preparation", () => {
  it("uses exactly the ZIP candidates listed in the Broward source catalog", async () => {
    const configuration = await loadBrowardSunbizConfiguration();

    expect(configuration.countyFips).toBe("12011");
    expect(configuration.zipCandidates).toEqual(EXPECTED_BROWARD_ZIPS);
    expect(configuration.zipCandidates).not.toContain("33070");
    expect(
      configuration.zipCandidates.every((zip) => /^\d{5}$/.test(zip)),
    ).toBe(true);
  });

  it("rejects an inside decision without Broward FIPS evidence", async () => {
    const directory = await mkdtemp(
      path.join(tmpdir(), "broward-sunbiz-invalid-validation-"),
    );
    const manifestPath = path.join(directory, "validation.jsonl");
    try {
      await writeFile(
        manifestPath,
        `${JSON.stringify({
          validationKey: `broward-address-v1:${"a".repeat(64)}`,
          status: "inside",
          countyFips: "12011",
          evidence: "",
        })}\n`,
      );

      await expect(
        loadAddressValidationManifest(manifestPath, "12011"),
      ).rejects.toThrow("non-empty evidence");
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("resumes text extraction without duplicates and reconciles every candidate", async () => {
    const directory = await mkdtemp(
      path.join(tmpdir(), "broward-sunbiz-text-"),
    );
    const inputPath = path.join(directory, "sample-corporate.txt");
    const validationPath = path.join(directory, "validation.jsonl");
    const outputDir = path.join(directory, "output");
    const lines = buildBrowardReconciliationSampleLines();
    try {
      await writeFile(inputPath, `${lines.join("\n")}\n`, "utf8");
      await writeFile(
        validationPath,
        `${buildSampleValidationEntries(lines)
          .map((entry) => JSON.stringify(entry))
          .join("\n")}\n`,
        "utf8",
      );

      const partial = await prepareBrowardSunbizLocal({
        inputPath,
        outputDir,
        validationManifestPath: validationPath,
        checkpointInterval: 1,
        maxSourceRecords: 2,
      });
      expect(partial.status).toBe("paused");
      expect(partial.counts).toMatchObject({
        sourceRecordsRead: 2,
        candidateRecordCount: 2,
        emittedBrowardRecordCount: 1,
        outsideOnlyRecordCount: 1,
      });

      const complete = await prepareBrowardSunbizLocal({
        inputPath,
        outputDir,
        validationManifestPath: validationPath,
        checkpointInterval: 1,
        resume: true,
      });
      expect(complete.status).toBe("complete");
      expect(complete.counts).toEqual({
        sourceRecordsRead: 5,
        invalidRecordCount: 1,
        validNonCandidateRecordCount: 1,
        candidateRecordCount: 3,
        candidateAddressMatchCount: 4,
        verifiedInsideAddressMatchCount: 1,
        verifiedOutsideAddressMatchCount: 1,
        unresolvedAddressMatchCount: 2,
        emittedBrowardRecordCount: 1,
        outsideOnlyRecordCount: 1,
        unresolvedWithoutInsideRecordCount: 1,
        lexiconBundleCount: 1,
      });
      expect(complete.reconciliation).toEqual({
        sourceRowsBalanced: true,
        candidateRecordsBalanced: true,
        candidateAddressesBalanced: true,
        allBalanced: true,
      });

      const records = await readJsonl(
        path.join(outputDir, "broward-records.jsonl"),
      );
      const bundles = await readJsonl(
        path.join(outputDir, "broward-lexicon-bundles.jsonl"),
      );
      const unresolved = await readJsonl(
        path.join(outputDir, "unresolved-candidates.jsonl"),
      );
      const outside = await readJsonl(
        path.join(outputDir, "outside-candidates.jsonl"),
      );
      expect(records).toHaveLength(1);
      expect(records[0]).toMatchObject({
        entity: { documentNumber: "P26000000001" },
        matchedAddresses: [{ role: "principalAddress" }],
        countyScope: { fips: "12011" },
      });
      expect(bundles).toHaveLength(1);
      expect(unresolved).toHaveLength(2);
      expect(outside).toHaveLength(1);

      const idempotent = await prepareBrowardSunbizLocal({
        inputPath,
        outputDir,
        validationManifestPath: validationPath,
        resume: true,
      });
      expect(idempotent.counts).toEqual(complete.counts);
      expect(
        await readJsonl(path.join(outputDir, "broward-records.jsonl")),
      ).toHaveLength(1);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("fails closed without a county validation manifest", async () => {
    const directory = await mkdtemp(
      path.join(tmpdir(), "broward-sunbiz-fail-closed-"),
    );
    const inputPath = path.join(directory, "candidate.txt");
    const outputDir = path.join(directory, "output");
    const line = buildCorporateFixtureLine({
      documentNumber: "P26000000005",
      entityName: "UNVALIDATED BROWARD ZIP SAMPLE INC.",
      principalAddress: "600 FEDERAL HIGHWAY",
      principalCity: "FORT LAUDERDALE",
      principalState: "FL",
      principalZip: "33301",
    });
    try {
      await writeFile(inputPath, `${line}\n`, "utf8");
      const summary = await prepareBrowardSunbizLocal({
        inputPath,
        outputDir,
        checkpointInterval: 1,
      });

      expect(summary.counts).toMatchObject({
        candidateRecordCount: 1,
        unresolvedAddressMatchCount: 1,
        emittedBrowardRecordCount: 0,
        unresolvedWithoutInsideRecordCount: 1,
      });
      expect(
        await readJsonl(path.join(outputDir, "broward-records.jsonl")),
      ).toHaveLength(0);
      expect(
        await readJsonl(path.join(outputDir, "unresolved-candidates.jsonl")),
      ).toHaveLength(1);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("reads a local ZIP and applies the same shared parser and validation gate", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "broward-sunbiz-zip-"));
    const inputPath = path.join(directory, "daily-corporate.zip");
    const validationPath = path.join(directory, "validation.json");
    const outputDir = path.join(directory, "output");
    const line = buildCorporateFixtureLine({
      documentNumber: "P26000000006",
      entityName: "ZIP BROWARD SAMPLE INC.",
      principalAddress: "700 LAS OLAS BOULEVARD",
      principalCity: "FORT LAUDERDALE",
      principalState: "FL",
      principalZip: "33301",
    });
    const record = parseFixtureRecord(line);
    try {
      const zip = new AdmZip();
      zip.addFile("daily/20260828c.txt", Buffer.from(`${line}\n`, "utf8"));
      zip.writeZip(inputPath);
      await writeFile(
        validationPath,
        JSON.stringify([
          {
            validationKey: createAddressValidationKey(record.principalAddress),
            status: "inside",
            countyFips: "12011",
            evidence:
              "Bundled ZIP test boundary fixture intersects Broward FIPS 12011",
          },
        ]),
        "utf8",
      );

      const summary = await prepareBrowardSunbizLocal({
        inputPath,
        outputDir,
        validationManifestPath: validationPath,
        checkpointInterval: 1,
      });

      expect(summary.input.format).toBe("zip");
      expect(summary.counts).toMatchObject({
        sourceRecordsRead: 1,
        candidateRecordCount: 1,
        verifiedInsideAddressMatchCount: 1,
        emittedBrowardRecordCount: 1,
      });
      const records = await readJsonl(
        path.join(outputDir, "broward-records.jsonl"),
      );
      expect(records[0]).toMatchObject({
        sourceFileName: "daily/20260828c.txt",
        sourceLineNumber: 1,
        entity: { documentNumber: "P26000000006" },
      });
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });
});
