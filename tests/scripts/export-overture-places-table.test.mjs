import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";

import { ParquetWriter } from "@dsnp/parquetjs";
import { describe, expect, it } from "vitest";

import {
  assertApprovedPlaceDatasets,
  isValidTaxonomyHierarchyScalar,
  validatePlacesTable,
} from "../../scripts/overture-places-lib.mjs";
import {
  buildPlacesTableParquetSchema,
  inspectPlacesParquet,
  parseExportCli,
  resolvePlacesPublicationPaths,
  resolveUnpooledDatabaseUrl,
  toPlacesParquetRow,
} from "../../scripts/export-overture-places-table.mjs";

describe("places publication layout", () => {
  it("puts NOTICE at the artifact root and index.json beside the parquet", () => {
    const paths = resolvePlacesPublicationPaths(
      "/tmp/publish",
      "lee",
    );
    expect(paths.publicationRoot).toBe("/tmp/publish");
    expect(paths.parquetPath).toBe("/tmp/publish/lee/places-table.parquet");
    expect(paths.indexPath).toBe("/tmp/publish/lee/index.json");
    expect(paths.noticePath).toBe("/tmp/publish/NOTICE.txt");
  });

  it("treats a parquet path under <county>/ as having NOTICE one level up", () => {
    const paths = resolvePlacesPublicationPaths(
      "/tmp/publish/lee/places-table.parquet",
      "lee",
    );
    expect(paths.noticePath).toBe("/tmp/publish/NOTICE.txt");
    expect(paths.indexPath).toBe("/tmp/publish/lee/index.json");
  });
});

describe("places Neon export CLI", () => {
  it("does not require --input-dir when --from-neon is set", () => {
    const options = parseExportCli([
      "--from-neon",
      "--county",
      "lee",
      "--release",
      "2026-07-22.0",
      "--out",
      "/tmp/publish",
      "--env-file",
      "/tmp/.env.local",
    ]);
    expect(options.fromNeon).toBe(true);
    expect(options.writeNotice).toBe(true);
    expect(options.inputDir).toBe("");
    expect(options.release).toBe("2026-07-22.0");
  });

  it("prefers DATABASE_URL_UNPOOLED over DATABASE_URL", () => {
    const previousUnpooled = process.env.DATABASE_URL_UNPOOLED;
    const previousUrl = process.env.DATABASE_URL;
    process.env.DATABASE_URL_UNPOOLED = "postgresql://unpooled.example/db";
    process.env.DATABASE_URL = "postgresql://pooled.example/db";
    try {
      expect(resolveUnpooledDatabaseUrl()).toBe("postgresql://unpooled.example/db");
    } finally {
      if (previousUnpooled === undefined) delete process.env.DATABASE_URL_UNPOOLED;
      else process.env.DATABASE_URL_UNPOOLED = previousUnpooled;
      if (previousUrl === undefined) delete process.env.DATABASE_URL;
      else process.env.DATABASE_URL = previousUrl;
    }
  });
});

describe("places parquet row mapping", () => {
  it("serializes taxonomy.hierarchy as a /-delimited string and coerces numeric strings", () => {
    const row = toPlacesParquetRow({
      gers_id: "abc",
      taxonomy_hierarchy: ["beauty_and_spa", "nail_salon"],
      confidence: "0.910000",
      longitude: "-81.87000000",
      latitude: "26.64000000",
      websites: ["https://example.com"],
    });
    expect(row.taxonomy_hierarchy).toBe("beauty_and_spa/nail_salon");
    expect(row.confidence).toBe(0.91);
    expect(row.longitude).toBe(-81.87);
    expect(row.latitude).toBe(26.64);
    expect(row.websites).toBe("https://example.com");
  });
});

describe("taxonomy hierarchy scalar gate", () => {
  it("accepts /-delimited paths and rejects JSON or Postgres array literals", () => {
    expect(isValidTaxonomyHierarchyScalar("beauty_and_spa/nail_salon")).toBe(true);
    expect(isValidTaxonomyHierarchyScalar("atm")).toBe(true);
    expect(isValidTaxonomyHierarchyScalar('["beauty_and_spa","nail_salon"]')).toBe(false);
    expect(isValidTaxonomyHierarchyScalar("{beauty_and_spa,nail_salon}")).toBe(false);
  });

  it("fails the publish gate when hierarchy serialization is absent or invalid", () => {
    const licenceGate = assertApprovedPlaceDatasets(["meta"]);
    const invalid = validatePlacesTable({
      parquetRowCount: 1,
      businessLocationRowCount: 1,
      gersIds: ["a"],
      nullGeometryCount: 0,
      licenceGate,
      invalidHierarchyCount: 1,
      hierarchyPresentCount: 1,
    });
    expect(invalid.passed).toBe(false);
    const absent = validatePlacesTable({
      parquetRowCount: 1,
      businessLocationRowCount: 1,
      gersIds: ["a"],
      nullGeometryCount: 0,
      licenceGate,
      invalidHierarchyCount: 0,
      hierarchyPresentCount: 0,
    });
    expect(absent.passed).toBe(false);
  });
});

describe("places parquet inspection", () => {
  it("counts rows, unique GERS ids, geometries, and hierarchy scalars", async () => {
    const dir = await mkdtemp(path.join(tmpdir(), "places-export-"));
    const parquetPath = path.join(dir, "places-table.parquet");
    try {
      const writer = await ParquetWriter.openFile(
        buildPlacesTableParquetSchema(),
        parquetPath,
      );
      await writer.appendRow(
        toPlacesParquetRow({
          gers_id: "id-1",
          taxonomy_hierarchy: ["a", "b"],
          longitude: -81.8,
          latitude: 26.6,
        }),
      );
      await writer.appendRow(
        toPlacesParquetRow({
          gers_id: "id-2",
          taxonomy_hierarchy: ["a", "c"],
          longitude: -81.9,
          latitude: 26.7,
        }),
      );
      await writer.close();
      const inspection = await inspectPlacesParquet(parquetPath);
      expect(inspection.rowCount).toBe(2);
      expect(inspection.gersIds).toEqual(["id-1", "id-2"]);
      expect(inspection.nullGeometryCount).toBe(0);
      expect(inspection.invalidHierarchyCount).toBe(0);
      expect(inspection.hierarchyPresentCount).toBe(2);
      const notice = await readFile(
        new URL("../../docs/overture-places-NOTICE.txt", import.meta.url),
        "utf8",
      );
      expect(notice).toContain("Copyright 2024 Foursquare Labs, Inc.");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });
});
