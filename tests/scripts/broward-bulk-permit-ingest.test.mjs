import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  FORT_LAUDERDALE_PERMIT_ARCGIS_SOURCE,
  arcgisEpochToIsoDate,
  buildFortLauderdalePermitUrl,
  fetchArcgisPermitFeatures,
  fetchArcgisPermitObjectIds,
  hashArcgisObjectIds,
  normalizeArcgisBrowardFolio,
  normalizeFortLauderdaleArcgisPermit,
} from "../../scripts/permit-source-adapters/broward-arcgis-bulk.mjs";
import {
  mapBulkPermitLoadRow,
  parseBulkPermitOptions,
  runBrowardBulkPermitIngest,
} from "../../scripts/run-broward-bulk-permit-ingest.mjs";

const temporaryDirectories = [];

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { force: true, recursive: true })),
  );
});

/**
 * Create one complete Fort Lauderdale ArcGIS feature.
 *
 * @param {Partial<Record<string, unknown>>} [overrides] - Attribute overrides.
 * @returns {{attributes:Record<string, unknown>}} Source feature.
 */
function fortLauderdaleFeature(overrides = {}) {
  return {
    attributes: {
      OBJECTID: 2,
      PERMITID: "bld-roof-25010001",
      CASEKEY: "25CAP-00000-00ABC",
      PERMITTYPE: "Roofing",
      PERMITSTAT: "Issued",
      PERMITDESC: "Remove and replace tile roof",
      SUBMITDT: Date.UTC(2025, 0, 2),
      APPROVEDT: Date.UTC(2025, 0, 8),
      PARCELID: "5042-16-10-0030",
      FULLADDR: "100 TEST AVE",
      LOCDESC: "Main structure",
      OWNERNAME: "OMITTED OWNER",
      OWNERADDR: "OMITTED OWNER ADDRESS",
      APPLICANT: "PUBLIC APPLICANT",
      CONTRACTOR: "PUBLIC ROOFING LLC",
      CONTRACTPH: "954-555-0100",
      CONTRACTID: "CCC1234567",
      ESTCOST: 25_000,
      COID: "CO-1",
      COSTATUS: "Complete",
      COISSUE: Date.UTC(2025, 2, 1),
      USECLASS: "Residential",
      LASTUPDATEDATE: Date.UTC(2025, 2, 2),
      SYNCDATE: Date.UTC(2025, 2, 3),
      GlobalID: "{00000000-0000-4000-8000-000000000002}",
      ...overrides,
    },
  };
}

describe("Fort Lauderdale official bulk permit normalization", () => {
  it("preserves folios and maps the portal-compatible permit identity", () => {
    expect(normalizeArcgisBrowardFolio("504108BJ0140")).toBe("504108BJ0140");
    expect(normalizeArcgisBrowardFolio("5042-16-10-0030")).toBe(
      "504216100030",
    );
    expect(normalizeArcgisBrowardFolio(504216100030)).toBeNull();
    expect(arcgisEpochToIsoDate(Date.UTC(2025, 0, 2))).toBe("2025-01-02");

    const normalized = normalizeFortLauderdaleArcgisPermit(
      fortLauderdaleFeature(),
      "2026-08-31T17:00:00.000Z",
    );
    expect(normalized.invalidReason).toBeNull();
    expect(normalized.record).toMatchObject({
      source_system: "broward_fort_lauderdale_lauderbuild_permits",
      source_vendor: "arcgis_feature_service",
      source_record_id: "2",
      record_key:
        "broward_fort_lauderdale_lauderbuild_permits:permit:BLD-ROOF-25010001",
      permit_number: "BLD-ROOF-25010001",
      parcel_identifier: "504216100030",
      application_date: "2025-01-02",
      approved_date: "2025-01-08",
      contractor_license: "CCC1234567",
      job_value: 25_000,
      is_roof_permit: true,
    });
    expect(normalized.record?.source_url).toContain("capID1=25CAP");
    expect(normalized.record?.source_payload).not.toHaveProperty("owner_name");
    expect(normalized.record?.source_payload).not.toHaveProperty(
      "contractor_phone",
    );
  });

  it("fails individual rows closed without failing the source chunk", () => {
    expect(
      normalizeFortLauderdaleArcgisPermit(
        fortLauderdaleFeature({ PERMITID: null }),
        "2026-08-31T17:00:00.000Z",
      ),
    ).toEqual({
      record: null,
      invalidReason: "missing_permit_number",
    });
    expect(
      normalizeFortLauderdaleArcgisPermit(
        fortLauderdaleFeature({ OBJECTID: "2" }),
        "2026-08-31T17:00:00.000Z",
      ),
    ).toEqual({
      record: null,
      invalidReason: "invalid_object_id",
    });
  });

  it("builds exact Accela and fallback FeatureServer URLs", () => {
    expect(
      buildFortLauderdalePermitUrl(
        "25CAP-00000-00ABC",
        "BLD-ROOF-25010001",
      ),
    ).toContain("capID3=00ABC");
    expect(
      buildFortLauderdalePermitUrl(null, "BLD-ROOF-25010001"),
    ).toContain("PERMITID%3D%27BLD-ROOF-25010001%27");
  });
});

describe("Fort Lauderdale ArcGIS snapshot transport", () => {
  it("captures an uncapped, sorted, unique object-ID snapshot", async () => {
    const requests = [];
    const fetchImpl = async (url, init) => {
      requests.push({ url: String(url), body: String(init?.body ?? "") });
      return new Response(JSON.stringify({ objectIds: [3, 1, 2] }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    };
    await expect(
      fetchArcgisPermitObjectIds(
        FORT_LAUDERDALE_PERMIT_ARCGIS_SOURCE,
        fetchImpl,
      ),
    ).resolves.toEqual([1, 2, 3]);
    expect(requests[0]?.body).toContain("returnIdsOnly=true");
    expect(hashArcgisObjectIds([1, 2, 3])).toMatch(/^[a-f0-9]{64}$/u);
  });

  it("requires every requested feature exactly once", async () => {
    const fetchImpl = async () =>
      new Response(
        JSON.stringify({
          features: [
            fortLauderdaleFeature({ OBJECTID: 2 }),
            fortLauderdaleFeature({ OBJECTID: 1 }),
          ],
        }),
        { status: 200 },
      );
    await expect(
      fetchArcgisPermitFeatures(
        FORT_LAUDERDALE_PERMIT_ARCGIS_SOURCE,
        [1, 2],
        fetchImpl,
      ),
    ).resolves.toMatchObject({
      features: [
        { attributes: { OBJECTID: 1 } },
        { attributes: { OBJECTID: 2 } },
      ],
    });
    await expect(
      fetchArcgisPermitFeatures(
        FORT_LAUDERDALE_PERMIT_ARCGIS_SOURCE,
        [1, 2, 3],
        fetchImpl,
      ),
    ).rejects.toThrow(/omitted requested feature IDs/u);
  });
});

describe("durable Broward bulk permit runner", () => {
  it("parses bounded jobs and rejects unsupported sources", () => {
    expect(
      parseBulkPermitOptions([
        "--job-id",
        "broward-permits-ftl-bulk-pilot-20260831",
        "--source",
        "fort-lauderdale",
        "--limit",
        "100",
        "--chunk-size",
        "50",
        "--load",
        "false",
      ]),
    ).toEqual({
      jobId: "broward-permits-ftl-bulk-pilot-20260831",
      sourceKey: "fort-lauderdale",
      outputDirectory:
        "downloads/broward/permit-bulk/fort-lauderdale",
      chunkSize: 50,
      limit: 100,
      load: false,
    });
    expect(() =>
      parseBulkPermitOptions([
        "--job-id",
        "broward-permits-unsupported",
        "--source",
        "countywide",
      ]),
    ).toThrow(/fort-lauderdale/u);
  });

  it("writes reconciled private chunks and resumes without refetching them", async () => {
    const outputDirectory = await mkdtemp(
      join(tmpdir(), "broward-permit-bulk-"),
    );
    temporaryDirectories.push(outputDirectory);
    let idCalls = 0;
    let featureCalls = 0;
    const fetchImpl = async (_url, init) => {
      const body = String(init?.body ?? "");
      if (body.includes("returnIdsOnly=true")) {
        idCalls += 1;
        return new Response(JSON.stringify({ objectIds: [1, 2] }), {
          status: 200,
        });
      }
      featureCalls += 1;
      return new Response(
        JSON.stringify({
          features: [
            fortLauderdaleFeature({ OBJECTID: 1 }),
            fortLauderdaleFeature({
              OBJECTID: 2,
              PERMITID: null,
            }),
          ],
        }),
        { status: 200 },
      );
    };
    const options = {
      jobId: "broward-permits-ftl-bulk-test",
      sourceKey: "fort-lauderdale",
      outputDirectory,
      chunkSize: 2,
      limit: null,
      load: false,
    };
    const first = await runBrowardBulkPermitIngest(options, {
      fetchImpl,
      now: () => "2026-08-31T17:00:00.000Z",
    });
    expect(first.reconciliation).toEqual({
      sourceRecords: 2,
      normalizedRecords: 1,
      uniquePermitRecords: 1,
      duplicatePermitRecords: 0,
      invalidRecords: 1,
      roofingRecords: 1,
      matchedProperties: 0,
      unmatchedProperties: 0,
      allSourceRowsAccountedFor: true,
      completedChunks: 1,
      expectedChunks: 1,
    });
    const resumed = await runBrowardBulkPermitIngest(options, {
      fetchImpl,
      now: () => "2026-08-31T17:01:00.000Z",
    });
    expect(resumed.reconciliation).toEqual(first.reconciliation);
    expect(idCalls).toBe(2);
    expect(featureCalls).toBe(1);
    expect(
      await readFile(
        join(outputDirectory, "normalized-private", "000000.jsonl"),
        "utf8",
      ),
    ).toContain("BLD-ROOF-25010001");
  });

  it("maps exact and unmatched properties without changing source identity", () => {
    const normalized = normalizeFortLauderdaleArcgisPermit(
      fortLauderdaleFeature(),
      "2026-08-31T17:00:00.000Z",
    ).record;
    expect(normalized).not.toBeNull();
    const exact = mapBulkPermitLoadRow(normalized, {
      propertyId: "11111111-1111-4111-8111-111111111111",
      parcelId: "22222222-2222-4222-8222-222222222222",
    });
    expect(exact).toMatchObject({
      source_record_key:
        "broward_fort_lauderdale_lauderbuild_permits:permit:BLD-ROOF-25010001",
      property_match_method: "exact_folio",
      property_match_confidence: "exact",
      licensed_professional:
        "PUBLIC ROOFING LLC (CCC1234567)",
    });
    expect(mapBulkPermitLoadRow(normalized, undefined)).toMatchObject({
      property_id: null,
      parcel_id: null,
      property_match_method: "unmatched",
      property_match_confidence: "unmatched",
    });
  });
});
