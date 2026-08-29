import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  classifyDorUseCode,
  normalizeBrowardTaxRollParcelId,
  parseCliOptions,
  profileBrowardTaxRoll,
} from "../../scripts/profile-broward-tax-roll.mjs";

/** @type {string[]} */
const temporaryDirectories = [];

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { recursive: true, force: true })),
  );
});

describe("Broward tax-roll profile", () => {
  it("preserves leading zeros and rejects malformed folios", () => {
    expect(normalizeBrowardTaxRollParcelId("004108BJ0140")).toBe(
      "004108BJ0140",
    );
    expect(normalizeBrowardTaxRollParcelId(" 504108bj0140 ")).toBe(
      "504108BJ0140",
    );
    expect(normalizeBrowardTaxRollParcelId("504108-0140")).toBeNull();
    expect(normalizeBrowardTaxRollParcelId(5041080140)).toBeNull();
  });

  it("uses official broad DOR code ranges", () => {
    expect(classifyDorUseCode("004")).toBe("residential");
    expect(classifyDorUseCode("048")).toBe("commercial_industrial");
    expect(classifyDorUseCode("063")).toBe("agricultural");
    expect(classifyDorUseCode("094")).toBe(
      "institutional_government_utility",
    );
    expect(classifyDorUseCode("099")).toBe("nonagricultural_acreage");
    expect(classifyDorUseCode("NAP")).toBe("invalid");
  });

  it("requires explicit roll provenance", () => {
    expect(() => parseCliOptions(["--roll-year", "2026"])).toThrow(
      /certification-status/u,
    );
    expect(
      parseCliOptions([
        "--nal-csv",
        "/tmp/NAL.csv",
        "--gis-seed",
        "/tmp/gis.csv",
        "--source-zip",
        "/tmp/NAL.zip",
        "--source-url",
        "https://example.test/NAL.zip",
        "--roll-year",
        "2026",
        "--certification-status",
        "preliminary",
        "--retrieved-at",
        "2026-08-29T22:49:05Z",
        "--output",
        "/tmp/profile.json",
      ]),
    ).toMatchObject({
      rollYear: 2026,
      certificationStatus: "preliminary",
    });
  });

  it("reconciles tax-roll, GIS, condominium, duplicate, and invalid counts", async () => {
    const directory = await mkdtemp(
      path.join(tmpdir(), "broward-tax-roll-profile-"),
    );
    temporaryDirectories.push(directory);
    const nalCsvPath = path.join(directory, "NAL.csv");
    const gisSeedPath = path.join(directory, "gis.csv");
    const sourceZipPath = path.join(directory, "NAL.zip");
    const outputPath = path.join(directory, "profile.json");
    const pilotCsvPath = path.join(directory, "pilot.csv");
    const pilotManifestPath = path.join(directory, "pilot.json");
    const header = [
      "CO_NO",
      "PARCEL_ID",
      "FILE_T",
      "ASMNT_YR",
      "BAS_STRT",
      "DOR_UC",
    ].join(",");
    await writeFile(
      nalCsvPath,
      [
        header,
        "16,504108BJ0140,R,2026,01,004",
        "16,111111111111,R,2026,01,001",
        "16,222222222222,R,2026,06,048",
        "16,333333333333,R,2026,04,000",
        "16,111111111111,R,2026,01,001",
        "16,BAD-ID,R,2026,01,001",
        "16,,R,2026,01,001",
      ].join("\n"),
    );
    await writeFile(
      gisSeedPath,
      [
        "parcel_id,request_identifier",
        "111111111111,111111111111",
        "222222222222,222222222222",
        "444444444444,444444444444",
      ].join("\n"),
    );
    await writeFile(sourceZipPath, "official-source-fixture");

    const report = await profileBrowardTaxRoll({
      nalCsvPath,
      gisSeedPath,
      sourceZipPath,
      sourceUrl: "https://example.test/Broward-NAL.zip",
      rollYear: 2026,
      certificationStatus: "preliminary",
      retrievedAt: "2026-08-29T22:49:05Z",
      outputPath,
      pilotCsvPath,
      pilotManifestPath,
    });
    expect(report.profile).toMatchObject({
      sourceRows: 7,
      uniqueValidParcelIds: 4,
      duplicateParcelIdCount: 1,
      duplicateRowsBeyondFirst: 1,
      missingParcelIds: 1,
      malformedParcelIds: 1,
      condominiumRows: 1,
    });
    expect(report.gisJoin).toMatchObject({
      addressUsedAsKey: false,
      gisSeedRows: 3,
      gisUniqueFolios: 3,
      matchedTaxRollToGis: 2,
      taxRollOnly: 2,
      gisOnly: 1,
      condominiumMatchedToGis: 0,
      condominiumTaxRollOnly: 1,
      unexplainedTaxRollDifference: 0,
    });
    const persisted = JSON.parse(await readFile(outputPath, "utf8"));
    expect(persisted.source).toMatchObject({
      certificationStatus: "preliminary",
      fileType: "NAL real property",
      napIncluded: false,
    });
    const manifest = JSON.parse(
      await readFile(pilotManifestPath, "utf8"),
    );
    expect(manifest.private).toBe(true);
    expect(manifest.publishable).toBe(false);
    expect(manifest.gisOnlyControls).toEqual(["444444444444"]);
    expect(await readFile(pilotCsvPath, "utf8")).not.toContain("BAD-ID");
  });
});
