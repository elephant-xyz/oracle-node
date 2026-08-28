import { mkdtemp, rm } from "fs/promises";
import os from "os";
import path from "path";
import AdmZip from "adm-zip";
import { describe, expect, it } from "vitest";

import { load } from "../../scripts/broward-query-data-only-loader.mjs";
import {
  inspectQueryDataOnlyArtifact,
  markQueryDataOnlyArtifact,
  QUERY_DATA_ONLY_MANIFEST_ENTRY,
} from "../../scripts/broward-query-data-only.mjs";
import { assertRetainedJsonParity } from "../../scripts/validate-broward-query-data-only.mjs";

/**
 * Add one JSON value to the data directory of an in-memory archive.
 *
 * @param {AdmZip} zip - Archive to update.
 * @param {string} name - Filename relative to `data/`.
 * @param {Record<string, unknown>} value - Serializable JSON object.
 * @returns {void}
 */
function addDataJson(zip, name, value) {
  zip.addFile(`data/${name}`, Buffer.from(JSON.stringify(value)));
}

/**
 * Build the smallest internally linked County data group used by safety tests.
 *
 * @returns {AdmZip} Unclassified pre-fact-sheet transform output.
 */
function buildDataOnlyFixture() {
  const zip = new AdmZip();
  addDataJson(zip, "property.json", { parcel_identifier: "504108BJ0140" });
  addDataJson(zip, "address.json", { county_name: "Broward" });
  addDataJson(zip, "relationship_property_address.json", {
    from: { "/": "./property.json" },
    to: { "/": "./address.json" },
  });
  addDataJson(zip, "bafkreicounty.json", {
    label: "County",
    relationships: {
      property_has_address: {
        "/": "./relationship_property_address.json",
      },
    },
  });
  return zip;
}

describe("Broward query-data-only safety contract", () => {
  it("marks valid JSON output as non-publishable and keeps links intact", async () => {
    const temporaryDirectory = await mkdtemp(
      path.join(os.tmpdir(), "broward-data-only-test-"),
    );
    const artifactPath = path.join(
      temporaryDirectory,
      "504108BJ0140.query-data-only.zip",
    );
    try {
      buildDataOnlyFixture().writeZip(artifactPath);
      const marked = markQueryDataOnlyArtifact(artifactPath, "504108BJ0140");
      expect(marked.manifest).toMatchObject({
        artifactMode: "query-data-only",
        publishable: false,
        folio: "504108BJ0140",
      });
      expect(marked.dataEntries).toContain("data/property.json");
      const inspected = await inspectQueryDataOnlyArtifact(artifactPath);
      expect(inspected.jsonEntryCount).toBe(4);
      expect(
        new AdmZip(artifactPath).getEntry(QUERY_DATA_ONLY_MANIFEST_ENTRY),
      ).not.toBeNull();
    } finally {
      await rm(temporaryDirectory, { recursive: true, force: true });
    }
  });

  it("rejects a stale fact-sheet reference and a broken relative link", async () => {
    const temporaryDirectory = await mkdtemp(
      path.join(os.tmpdir(), "broward-data-only-invalid-"),
    );
    try {
      const factSheetPath = path.join(
        temporaryDirectory,
        "fact.query-data-only.zip",
      );
      const factSheetZip = buildDataOnlyFixture();
      addDataJson(factSheetZip, "fact_sheet.json", {
        ipfs_url: "./index.html",
      });
      factSheetZip.writeZip(factSheetPath);
      expect(() =>
        markQueryDataOnlyArtifact(factSheetPath, "504108BJ0140"),
      ).toThrow(/deferred output/);

      const brokenPath = path.join(
        temporaryDirectory,
        "broken.query-data-only.zip",
      );
      const brokenZip = buildDataOnlyFixture();
      addDataJson(brokenZip, "relationship_property_lot.json", {
        from: { "/": "./property.json" },
        to: { "/": "./missing-lot.json" },
      });
      addDataJson(brokenZip, "bafkreisecond.json", {
        label: "County",
        relationships: {
          property_has_lot: {
            "/": "./relationship_property_lot.json",
          },
        },
      });
      brokenZip.writeZip(brokenPath);
      expect(() =>
        markQueryDataOnlyArtifact(brokenPath, "504108BJ0140"),
      ).toThrow(/broken relative link/);
    } finally {
      await rm(temporaryDirectory, { recursive: true, force: true });
    }
  });

  it("retains every non-fact-sheet JSON filename from a full artifact", () => {
    const dataOnly = buildDataOnlyFixture();
    const full = buildDataOnlyFixture();
    addDataJson(full, "fact_sheet.json", { ipfs_url: "./index.html" });
    addDataJson(full, "relationship_property_to_fact_sheet.json", {
      from: { "/": "./property.json" },
      to: { "/": "./fact_sheet.json" },
    });
    expect(() => assertRetainedJsonParity(dataOnly, full)).not.toThrow();
    addDataJson(full, "tax_1.json", { tax_year: 2026 });
    expect(() => assertRetainedJsonParity(dataOnly, full)).toThrow(/tax_1/);
  });

  it("applies the exact upstream no-fact-sheet call omission in memory", async () => {
    const source = [
      "async function transform() {",
      "                await generateFactSheet(tempRoot);",
      "}",
    ].join("\n");
    const loaded = await load(
      "file:///repo/node_modules/@elephant-xyz/cli/dist/commands/transform/index.js",
      {},
      () => Promise.resolve({ format: "module", source }),
    );
    expect(String(loaded.source)).toContain(
      "Deferring HTML and fact-sheet generation",
    );
    await expect(
      load(
        "file:///repo/node_modules/@elephant-xyz/cli/dist/commands/transform/index.js",
        {},
        () => Promise.resolve({ format: "module", source: "upstream changed" }),
      ),
    ).rejects.toThrow(/expected one fact-sheet call/);
  });
});
