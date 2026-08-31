import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  POLK_BBB_TRADE_SOURCES,
  buildPolkBbbHarvestPlan,
  extractBbbLicenseNumbers,
  matchPolkPermitContractorsToBbb,
  readPermitContractorEvidence,
} from "../../scripts/polk/bbb-contractor-crm.mjs";

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
 * Create a test temporary directory.
 *
 * @returns {Promise<string>} Temporary directory.
 */
async function createTemporaryDirectory() {
  const directory = await mkdtemp(path.join(tmpdir(), "polk-bbb-"));
  temporaryDirectories.push(directory);
  return directory;
}

describe("Polk multi-trade BBB plan", () => {
  it("uses three verified Lakeland category sources without claiming completion", () => {
    const plan = buildPolkBbbHarvestPlan("/tmp/polk-bbb");
    expect(POLK_BBB_TRADE_SOURCES.map((trade) => trade.key)).toEqual([
      "roofing",
      "hvac",
      "solar",
    ]);
    expect(plan.trades).toHaveLength(3);
    expect(plan.trades).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          key: "roofing",
          categoryUrl:
            "https://www.bbb.org/us/fl/lakeland/category/roofing-contractors",
        }),
      ]),
    );
    expect(plan.complete).toBe(false);
  });

  it("extracts real licence text and rejects bulk-only contractor assumptions", () => {
    expect(
      extractBbbLicenseNumbers([
        { rawText: "State of Florida license CCC 1234567 active" },
        { licenseNumber: "CAC1812345" },
        { rawText: "Polk County license CRC:11516516" },
      ]),
    ).toEqual(["CAC1812345", "CCC1234567", "CRC11516516"]);
    expect(
      readPermitContractorEvidence({
        permit_number: "P-1",
        agency_name: "POLK COUNTY",
        description: "Reroof by Example LLC",
      }),
    ).toBeNull();
  });
});

describe("Polk contractor CRM matching", () => {
  it("matches only exact licences backed by certified permit detail", async () => {
    const root = await createTemporaryDirectory();
    const permitEvidencePath = path.join(root, "permits.jsonl");
    const bbbRoot = path.join(root, "bbb");
    const outputPath = path.join(bbbRoot, "manifest", "contractor-crm.json");
    await writeFile(
      permitEvidencePath,
      `${JSON.stringify({
        permitNumber: "BR-2026-1",
        sourceKey: "polk_county_accela",
        sourceUrl:
          "https://aca-prod.accela.com/POLKCO/Cap/CapDetail.aspx?altId=BR-2026-1",
        status: "enriched",
        detail: {
          contractor: {
            businessName: "POLK ROOF LLC",
            licenseNumber: "CCC1234567",
          },
        },
      })}\n`,
      "utf8",
    );
    for (const trade of POLK_BBB_TRADE_SOURCES) {
      const tradeDirectory = path.join(bbbRoot, trade.key);
      await mkdir(path.join(tradeDirectory, "manifest"), { recursive: true });
      await mkdir(path.join(tradeDirectory, "profiles"), { recursive: true });
      await writeFile(
        path.join(tradeDirectory, "manifest", "summary.json"),
        `${JSON.stringify({
          categoryUrl: trade.categoryUrl,
          profilesHarvested: 1,
          profilesFailed: 0,
        })}\n`,
        "utf8",
      );
      await writeFile(
        path.join(tradeDirectory, "profiles", "profiles-part-0001.jsonl"),
        `${JSON.stringify({
          providerProfileId: `${trade.key}:profile`,
          profileUrl: `https://www.bbb.org/profile/${trade.key}`,
          name: `${trade.name} Fixture`,
          bbbRating: "A",
          accredited: true,
          licenses:
            trade.key === "roofing"
              ? [{ rawText: "Florida Roofing CCC1234567" }]
              : [],
        })}\n`,
        "utf8",
      );
    }
    await mkdir(path.join(bbbRoot, "manifest"), { recursive: true });
    await writeFile(
      path.join(bbbRoot, "manifest", "summary.json"),
      `${JSON.stringify({
        schemaVersion: "oracle-node.polk-bbb-multi-trade-harvest.v1",
        county: "polk",
        complete: true,
      })}\n`,
      "utf8",
    );

    const summary = await matchPolkPermitContractorsToBbb({
      permitEvidencePath,
      bbbRoot,
      outputPath,
    });

    expect(summary).toMatchObject({
      gate: {
        actualPermitContractorLicenseEvidence: true,
        permitEvidenceRecordCount: 1,
        uniquePermitLicenseCount: 1,
        matchMethodsAllowed: ["permit_license_exact"],
      },
      harvestedProfileCount: 3,
      profilesWithLicenseEvidence: 1,
      matchedContractorCount: 1,
      matchedPermitCount: 1,
      complete: true,
    });
    expect(summary.matches).toEqual([
      expect.objectContaining({
        matchMethod: "permit_license_exact",
        licenseNumber: "CCC1234567",
        permitNumbers: ["BR-2026-1"],
      }),
    ]);
  });

  it("keeps the match stage blocked without permit licence evidence", async () => {
    const root = await createTemporaryDirectory();
    const permitEvidencePath = path.join(root, "permits.jsonl");
    const outputPath = path.join(root, "bbb", "manifest", "crm.json");
    await writeFile(
      permitEvidencePath,
      `${JSON.stringify({
        permitNumber: "P-1",
        sourceKey: "polk_property_appraiser_bulk",
        status: "unsupported_source",
        detail: null,
      })}\n`,
      "utf8",
    );

    const summary = await matchPolkPermitContractorsToBbb({
      permitEvidencePath,
      bbbRoot: path.join(root, "bbb"),
      outputPath,
    });

    expect(summary.complete).toBe(false);
    expect(summary.gate.actualPermitContractorLicenseEvidence).toBe(false);
    expect(summary.blocker).toMatch(/no certified permit detail/i);
  });
});
