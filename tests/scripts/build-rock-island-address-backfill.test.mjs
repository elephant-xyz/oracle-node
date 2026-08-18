import { mkdtemp, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  TARGET_FOLIOS,
  buildAddressBackfillPackage,
  parseOutputPath,
  renderAddressBackfillPackage,
  writeAddressBackfillPackage,
} from "../../scripts/build-rock-island-address-backfill.mjs";

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
 * Create a disposable test directory.
 *
 * @returns {Promise<string>} Temporary directory path.
 */
async function temporaryDirectory() {
  const directory = await mkdtemp(join(tmpdir(), "ri-address-backfill-"));
  temporaryDirectories.push(directory);
  return directory;
}

describe("Rock Island supplemental address backfill", () => {
  it("covers the exact 25-folio public null-address scope", () => {
    const packageValue = buildAddressBackfillPackage();

    expect(packageValue.summary).toEqual({
      folioCount: 25,
      found: 19,
      notFound: 6,
      conflicting: 0,
    });
    expect(Object.keys(packageValue.recordsByFolio).sort()).toEqual(
      [...TARGET_FOLIOS].sort(),
    );
    expect(packageValue.apply).toBe(false);
    expect(packageValue.classification).toBe("private_review_only");
    expect(packageValue.correctedQueryTableCid).toBe(
      "QmQnm6W2Ye9GH3oD6SUswHrQCMegnpGbhRFgipitYW6zCc",
    );
  });

  it("stages only exact-key E911 site addresses with official provenance", () => {
    const packageValue = buildAddressBackfillPackage();

    expect(packageValue.recordsByFolio["0436100005"]).toMatchObject({
      status: "found",
      conflicting: false,
      siteAddress: {
        streetLine: "1107 S HIGH ST",
        city: "PORT BYRON",
        stateCode: "IL",
        postalCode: "61275",
      },
      provenance: {
        addressRole: "site",
        parcel: {
          pin: "0436100005",
          ricoParcelId: "05159-1",
          objectId: 3805,
        },
        e911AddressPoint: {
          objectId: 6610,
          raw: {
            address: "1107 S HIGH ST",
            propertyAddress: "1107 S HIGH ST",
          },
        },
      },
    });
    expect(packageValue.recordsByFolio["1602429005"]).toMatchObject({
      status: "found",
      siteAddress: {
        streetLine: "2019 17TH ST",
        city: "ROCK ISLAND",
      },
      provenance: {
        parcel: {
          ricoParcelId: "102168",
          keyEvidence: "validated_2026_08_03_parcel_snapshot",
          validatedSnapshot: {
            sourceRevision: "2026-07-14T12:08:19.189Z",
            sourceSnapshotAt: "2026-08-03T18:45:08.716Z",
          },
        },
      },
    });

    for (const record of Object.values(packageValue.recordsByFolio)) {
      expect(record.provenance.prohibitedSourcesExcluded).toEqual([
        "owner",
        "mailing",
        "tax_bill",
      ]);
      if (record.status === "found") {
        expect(record.siteAddress).not.toBeNull();
        expect(record.provenance.e911AddressPoint.layerUrl).toBe(
          "https://gis.rockislandcountyil.gov/arcgis/rest/services/Hosted/AddressPoints/FeatureServer/0",
        );
      }
    }
  });

  it("retains exact not-found evidence without inventing nearby addresses", () => {
    const packageValue = buildAddressBackfillPackage();

    expect(packageValue.recordsByFolio["0831449003"]).toMatchObject({
      status: "not_found",
      siteAddress: null,
      conflicting: false,
      provenance: {
        parcel: { ricoParcelId: "089419" },
        e911AddressPoint: { matchingRecordCount: 0 },
      },
    });
    expect(packageValue.recordsByFolio["1602429006"]).toMatchObject({
      status: "not_found",
      siteAddress: null,
      provenance: {
        parcel: {
          ricoParcelId: "102169-1",
          keyEvidence: "validated_2026_08_03_parcel_snapshot",
          validatedSnapshot: {
            evidencePath: "downloads/rock-island/rock-island.csv",
          },
        },
      },
    });
  });

  it("renders byte-identical packages for idempotent review and apply planning", () => {
    const first = buildAddressBackfillPackage();
    const second = buildAddressBackfillPackage();

    expect(first.recordsSha256).toMatch(/^[a-f0-9]{64}$/u);
    expect(first.recordsSha256).toBe(second.recordsSha256);
    expect(renderAddressBackfillPackage(first)).toBe(
      renderAddressBackfillPackage(second),
    );
    expect(first.applyPolicy).toMatchObject({
      targetKey: "request_identifier/folio",
      operation: "upsert_site_address_only_when_current_site_address_is_null",
      databaseMutationPerformedByThisPackage: false,
    });
  });

  it("writes the private artifact with owner-only permissions", async () => {
    const directory = await temporaryDirectory();
    const outputPath = join(directory, "package.json");

    const result = await writeAddressBackfillPackage(outputPath);
    const fileStats = await stat(outputPath);

    expect(result.packageValue.summary.found).toBe(19);
    expect(fileStats.mode & 0o777).toBe(0o600);
  });

  it("parses only the bounded output option", () => {
    expect(parseOutputPath([])).toContain(
      "rock-island-site-address-backfill-v1.json",
    );
    expect(parseOutputPath(["--out", "/tmp/package.json"])).toBe(
      "/tmp/package.json",
    );
    expect(() => parseOutputPath(["--apply"])).toThrow(/Usage/u);
  });
});
