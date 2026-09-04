import { mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  assertLakelandArcgisPermitPage,
  buildLakelandArcgisPageUrl,
  normalizeLakelandArcgisPermit,
  parseLakelandArcgisHarvestOptions,
  runLakelandArcgisPermits,
} from "../../scripts/polk/lakeland-arcgis-permits.mjs";

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
 * Build one representative official source feature.
 *
 * @param {number} objectId Source object ID.
 * @returns {{attributes:Record<string, unknown>}} ArcGIS feature.
 */
function sourceFeature(objectId) {
  return {
    attributes: {
      OBJECTID: objectId,
      GLOBALID: `{GLOBAL-${objectId}}`,
      PERMIT_NO: `BLD24-${String(objectId).padStart(5, "0")}`,
      DESCRIPTION: "TEST WORK",
      SITE_ADDR: `${objectId} MAIN ST`,
      SITE_CITY: "Lakeland",
      SITE_STATE: "FL",
      SITE_ZIP: "33801",
      ADDRESSID: `A-${objectId}`,
      XCOORD: 680000 + objectId,
      YCOORD: 1300000 + objectId,
      PERMITORPROJECTTYPE: "Building",
      APPLICANT_NAME: "TEST APPLICANT",
      APPLIED: 1_704_067_200_000 + objectId,
      APPROVED: null,
      ISSUED: null,
      JOBVALUE: 1234.56,
      ICON: "PERMIT",
      APPLIEDFY: 2024,
      CREATED_USER: "publisher",
      CREATED_DATE: 1_704_067_200_000,
      LAST_EDITED_USER: "publisher",
      LAST_EDITED_DATE: 1_704_067_200_000,
    },
  };
}

/**
 * Create a deterministic in-memory ArcGIS endpoint.
 *
 * @param {readonly {attributes:Record<string, unknown>}[]} features Features.
 * @returns {typeof fetch} Fetch implementation.
 */
function createArcgisFetch(features) {
  return /** @type {typeof fetch} */ (
    async (input) => {
      const url = new URL(String(input));
      if (url.searchParams.get("returnCountOnly") === "true") {
        return new Response(JSON.stringify({ count: features.length }), {
          status: 200,
        });
      }
      if (url.searchParams.has("outStatistics")) {
        return new Response(
          JSON.stringify({
            features: [
              {
                attributes: {
                  min_oid: features[0]?.attributes.OBJECTID ?? null,
                  max_oid: features.at(-1)?.attributes.OBJECTID ?? null,
                  min_applied: features[0]?.attributes.APPLIED ?? null,
                  max_applied: features.at(-1)?.attributes.APPLIED ?? null,
                },
              },
            ],
          }),
          { status: 200 },
        );
      }
      const where = url.searchParams.get("where") ?? "";
      const after = Number.parseInt(
        /OBJECTID > (\d+)/.exec(where)?.[1] ?? "0",
        10,
      );
      const maximum = Number.parseInt(
        /OBJECTID <= (\d+)/.exec(where)?.[1] ?? String(Number.MAX_SAFE_INTEGER),
        10,
      );
      const pageSize = Number.parseInt(
        url.searchParams.get("resultRecordCount") ?? "2000",
        10,
      );
      const page = features
        .filter((feature) => {
          const objectId = Number(feature.attributes.OBJECTID);
          return objectId > after && objectId <= maximum;
        })
        .slice(0, pageSize);
      return new Response(JSON.stringify({ features: page }), { status: 200 });
    }
  );
}

describe("Polk Lakeland ArcGIS permit harvest", () => {
  it("uses frozen OBJECTID keyset pagination", () => {
    const url = new URL(
      buildLakelandArcgisPageUrl({
        afterObjectId: 100,
        maxObjectId: 500,
        pageSize: 25,
      }),
    );

    expect(url.searchParams.get("where")).toBe(
      "TYPE = 'Permit' AND OBJECTID > 100 AND OBJECTID <= 500",
    );
    expect(url.searchParams.get("orderByFields")).toBe("OBJECTID ASC");
    expect(url.searchParams.get("resultOffset")).toBeNull();
    expect(url.searchParams.get("resultRecordCount")).toBe("25");
  });

  it("preserves published permits without guessing a parcel match", () => {
    const record = normalizeLakelandArcgisPermit(
      sourceFeature(42),
      "2026-09-03T00:00:00.000Z",
    );

    expect(record).toMatchObject({
      sourceRecordKey: "lakeland_arcgis:{global-42}",
      sourceObjectId: 42,
      permitNumber: "BLD24-00042",
      siteAddress: {
        line1: "42 MAIN ST",
        sourceAddressId: "A-42",
      },
      parcelIdentifier: null,
      propertyMatch: null,
    });
  });

  it("rejects duplicate or out-of-order source identities", () => {
    const records = [1, 2].map((objectId) =>
      normalizeLakelandArcgisPermit(
        sourceFeature(objectId),
        "2026-09-03T00:00:00.000Z",
      ),
    );
    expect(() =>
      assertLakelandArcgisPermitPage(records, 0, 2, 2),
    ).not.toThrow();
    expect(() =>
      assertLakelandArcgisPermitPage([records[1], records[0]], 0, 2, 2),
    ).toThrow(/strictly ordered/);
    expect(() =>
      assertLakelandArcgisPermitPage(
        [records[0], { ...records[1], sourceGlobalId: "{GLOBAL-1}" }],
        0,
        2,
        2,
      ),
    ).toThrow(/duplicate GLOBALID/);
  });

  it("requires explicit approval above the bounded pilot limit", async () => {
    const directory = await mkdtemp(
      path.join(tmpdir(), "polk-lakeland-arcgis-scale-"),
    );
    temporaryDirectories.push(directory);
    const options = parseLakelandArcgisHarvestOptions([
      "--stage",
      "harvest",
      "--output",
      path.join(directory, "permits.jsonl"),
    ]);

    await expect(
      runLakelandArcgisPermits(options, createArcgisFetch([sourceFeature(1)])),
    ).rejects.toThrow(/requires --approve-scale/);
  });

  it("harvests, resumes, and strictly verifies content-addressed parts", async () => {
    const directory = await mkdtemp(
      path.join(tmpdir(), "polk-lakeland-arcgis-harvest-"),
    );
    temporaryDirectories.push(directory);
    const output = path.join(directory, "permits.jsonl");
    const features = [sourceFeature(1), sourceFeature(5), sourceFeature(9)];
    const fetchImpl = createArcgisFetch(features);
    const harvestOptions = parseLakelandArcgisHarvestOptions([
      "--stage",
      "harvest",
      "--output",
      output,
      "--page-size",
      "2",
      "--limit",
      "3",
    ]);

    const first = await runLakelandArcgisPermits(harvestOptions, fetchImpl);
    const resumed = await runLakelandArcgisPermits(harvestOptions, fetchImpl);
    const lines = (await readFile(output, "utf8"))
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line));

    expect(first).toMatchObject({
      snapshotRecordCount: 3,
      harvestedRecordCount: 3,
      uniqueGlobalIdCount: 3,
      completedPartCount: 2,
      pilot: true,
      countyCoverageComplete: false,
      complete: true,
    });
    expect(resumed.outputSha256).toBe(first.outputSha256);
    expect(lines.map((line) => line.sourceObjectId)).toEqual([1, 5, 9]);
    expect(lines.every((line) => line.parcelIdentifier === null)).toBe(true);

    const verification = await runLakelandArcgisPermits(
      { ...harvestOptions, stage: "verify" },
      fetchImpl,
    );
    expect(verification).toMatchObject({
      verifiedPartCount: 2,
      verifiedRecordCount: 3,
      uniqueGlobalIdCount: 3,
      complete: true,
    });
  });

  it("detects committed part tampering by filename digest", async () => {
    const directory = await mkdtemp(
      path.join(tmpdir(), "polk-lakeland-arcgis-corrupt-"),
    );
    temporaryDirectories.push(directory);
    const output = path.join(directory, "permits.jsonl");
    const options = parseLakelandArcgisHarvestOptions([
      "--stage",
      "harvest",
      "--output",
      output,
      "--limit",
      "1",
    ]);
    const fetchImpl = createArcgisFetch([sourceFeature(1)]);
    await runLakelandArcgisPermits(options, fetchImpl);
    const partDirectory = `${output}.parts`;
    const partName = (await readdir(partDirectory)).find((name) =>
      name.startsWith("part-"),
    );
    expect(partName).toBeDefined();
    await writeFile(
      path.join(partDirectory, String(partName)),
      `${JSON.stringify({ corrupted: true })}\n`,
      "utf8",
    );

    await expect(
      runLakelandArcgisPermits({ ...options, stage: "verify" }, fetchImpl),
    ).rejects.toThrow(/digest mismatch/);
  });
});
