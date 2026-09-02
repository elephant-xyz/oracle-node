import { describe, expect, it } from "vitest";

import { APPROVED_PUBLIC_FIELDS } from "../../scripts/audit-broward-appraisal-publication.mjs";
import {
  PERMIT_FIELDS,
  PROPERTY_FIELDS,
  browardSnapshotPrefix,
  buildCoverageSnapshot,
  parseSitusAddress,
  parseSnapshotOptions,
  snapshotVersion,
  validateReusablePropertyManifest,
} from "../../scripts/stage-broward-donphan-snapshot.mjs";

const loadedAt = "2026-09-02T17:45:01.234Z";

/**
 * Build a complete aggregate fixture whose arithmetic reconciles.
 *
 * @returns {import("../../scripts/stage-broward-donphan-snapshot.mjs").SnapshotCounts}
 *   Transactionally consistent Broward counts.
 */
function counts() {
  return {
    propertyRows: 526_068,
    distinctPropertyIds: 526_068,
    distinctFolios: 526_068,
    nullFolios: 0,
    permitRows: 494_751,
    distinctPermitIds: 494_751,
    linkedPermits: 359_079,
    unlinkedPermits: 135_672,
    foreignLinkedPermits: 0,
    linkedProperties: 67_180,
    roofingPermits: 48_719,
    permitSourceSystemCount: 2,
    sunbizAddressMatches: 14_865,
    sunbizRegistrations: 12_432,
    sunbizProperties: 9_023,
    bbbProfiles: 2_823,
    bbbMatchedProperties: 0,
    appraisalFirstLoadedAt: loadedAt,
    appraisalLastLoadedAt: loadedAt,
    permitFirstLoadedAt: loadedAt,
    permitLastLoadedAt: loadedAt,
    sunbizFirstLoadedAt: loadedAt,
    sunbizLastLoadedAt: loadedAt,
    bbbFirstLoadedAt: loadedAt,
    bbbLastLoadedAt: loadedAt,
  };
}

describe("Broward Donphan snapshot staging", () => {
  it("builds a path-safe immutable Broward-only version prefix", () => {
    const version = snapshotVersion(loadedAt);

    expect(version).toBe("20260902T174501234Z");
    expect(browardSnapshotPrefix(version)).toBe(
      "publication-staging/broward/donphan/snapshots/20260902T174501234Z",
    );
    expect(() => browardSnapshotPrefix("../lee")).toThrow(
      "Snapshot version must be a basic UTC timestamp",
    );
  });

  it("parses only bounded local staging options", () => {
    expect(
      parseSnapshotOptions([
        "--output-root",
        "downloads/broward/snapshots",
        "--batch-size",
        "5000",
        "--reuse-property-version",
        "20260902T205931937Z",
        "--upload",
      ]),
    ).toMatchObject({
      batchSize: 5_000,
      upload: true,
      reusePropertyVersion: "20260902T205931937Z",
    });
    expect(() => parseSnapshotOptions(["--bucket", "other-county"])).toThrow(
      "Unknown option --bucket",
    );
    expect(() =>
      parseSnapshotOptions(["--reuse-property-version", "../lee"]),
    ).toThrow("Snapshot version must be a basic UTC timestamp");
  });

  it("parses situs text without deriving or retaining the state token", () => {
    expect(parseSitusAddress("123 MAIN ST, HOLLYWOOD, FL 33020")).toEqual({
      street: "123 MAIN ST",
      city: "HOLLYWOOD",
      postalCode: "33020",
    });
    expect(parseSitusAddress(null)).toEqual({
      street: null,
      city: null,
      postalCode: null,
    });
  });

  it("locks public-safe Donphan scalar schemas", () => {
    const propertyNames = PROPERTY_FIELDS.map(({ name }) => name);
    const permitNames = PERMIT_FIELDS.map(({ name }) => name);

    expect(PROPERTY_FIELDS).toHaveLength(11);
    expect(PERMIT_FIELDS).toHaveLength(17);
    expect(
      propertyNames.every((name) => APPROVED_PUBLIC_FIELDS.includes(name)),
    ).toBe(true);
    expect(propertyNames).toEqual(
      expect.arrayContaining([
        "property_id",
        "parcel_identifier",
        "county_fips",
        "property_usage_type",
        "livable_floor_area",
      ]),
    );
    expect(propertyNames).not.toEqual(
      expect.arrayContaining([
        "owner_name",
        "request_identifier",
        "address_street",
        "has_sunbiz_tenant",
      ]),
    );
    expect(permitNames).not.toEqual(
      expect.arrayContaining(["project_description", "description", "fee"]),
    );
  });

  it("reuses only a manifest with the same frozen appraisal state", () => {
    const currentCounts = counts();
    const priorVersion = "20260902T174501234Z";
    const manifest = {
      schemaVersion: "oracle-node.broward-donphan-snapshot.v2",
      snapshotVersion: priorVersion,
      snapshotTimestamp: loadedAt,
      counts: currentCounts,
      artifactSchemas: { property: PROPERTY_FIELDS },
      physicalExports: {
        property: {
          rowCount: currentCounts.propertyRows,
          nonNullCounts: Object.fromEntries(
            PROPERTY_FIELDS.map(({ name }) => [
              name,
              currentCounts.propertyRows,
            ]),
          ),
        },
      },
      artifacts: [
        {
          name: "property-query-table",
          fileName: "query-table.parquet",
          contentType: "application/vnd.apache.parquet",
          s3Key: `${browardSnapshotPrefix(priorVersion)}/query-table.parquet`,
          sizeBytes: 100,
          sha256: "a".repeat(64),
          checksumSha256: "YQ==",
          cid: `Qm${"a".repeat(44)}`,
        },
      ],
    };

    expect(
      validateReusablePropertyManifest(manifest, currentCounts, priorVersion),
    ).toMatchObject({
      artifact: {
        sha256: "a".repeat(64),
        cid: `Qm${"a".repeat(44)}`,
      },
      physicalExport: { rowCount: currentCounts.propertyRows },
    });
    expect(() =>
      validateReusablePropertyManifest(
        {
          ...manifest,
          counts: { ...currentCounts, propertyRows: 526_069 },
        },
        currentCounts,
        priorVersion,
      ),
    ).toThrow("Current appraisal state differs");
  });

  it("marks reconciled current data as supported partial", () => {
    const coverage = buildCoverageSnapshot({
      snapshotTimestamp: loadedAt,
      counts: counts(),
      permitSources: [
        {
          sourceSystem: "broward_source_a_permits",
          rowCount: 250_000,
          linkedCount: 200_000,
          unlinkedCount: 50_000,
          roofingCount: 25_000,
        },
        {
          sourceSystem: "broward_source_b_permits",
          rowCount: 244_751,
          linkedCount: 159_079,
          unlinkedCount: 85_672,
          roofingCount: 23_719,
        },
      ],
      routes: {
        registryVersion: "2026-09-01.2",
        totalCurrentRoutes: 32,
        implementedCurrentRoutes: 24,
        manualCaptchaCurrentRoutes: 3,
        hardBlockedCurrentRoutes: 5,
        unattendedUnavailableCurrentRoutes: 8,
        implementedJurisdictions: [],
        manualCaptchaJurisdictions: [],
        hardBlockCategories: [],
      },
    });

    expect(coverage).toMatchObject({
      snapshotTimestamp: loadedAt,
      coverage_status: "supported_partial",
      county_complete: false,
      publicationScope: {
        level: "partial",
        denominatorBasis: "county_total",
      },
      denominator_semantics: {
        appraisal: {
          expectedCount: 534_309,
          ingestedCount: 526_068,
        },
        permits: {
          rowExpectedCount: null,
          routeCount: 32,
          supportedRouteCount: 24,
          unattendedUnavailableRouteCount: 8,
        },
      },
      reconciliation: {
        allBalanced: true,
      },
      acceptedTerminalExceptions: [
        {
          jurisdiction: "Pembroke Pines",
          kind: "source_missing_record",
          count: 2,
          treatment: "accepted_terminal_exclusion",
        },
      ],
    });
  });
});
