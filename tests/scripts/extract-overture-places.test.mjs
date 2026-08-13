import { describe, expect, it } from "vitest";

import {
  APPROVED_PLACE_DATASETS,
  HOSTED_SERVICE_SEED_LEAVES,
  SCOPING_BASELINE_CLIP_COUNT,
  SCOPING_BASELINE_RELEASE,
  assertApprovedPlaceDatasets,
  buildCountyAssignment,
  buildExtractCopySql,
  collectDatasetsFromSources,
  countySlugFromPlacesArtifactUri,
  duckdbStringLiteral,
  hostedServiceRuleId,
  matchHostedService,
  parseExtractCli,
  parseHostedServiceCategoryList,
  parseOvertureStacCatalog,
  parseTigerBoundarySource,
  rebuildHostedServicePaths,
  renderPlacesNotice,
  taxonomyHierarchyToPath,
  validatePlacesTable,
} from "../../scripts/overture-places-lib.mjs";

describe("Overture places extract CLI", () => {
  it("parses the skill interface and defaults", () => {
    const options = parseExtractCli([
      "--county",
      "lee",
      "--county-fips",
      "12071",
      "--release",
      "2026-07-22.0",
      "--boundary-source",
      "tiger/tl_2024_us_county",
      "--output-dir",
      "/tmp/overture-lee",
    ]);
    expect(options).toMatchObject({
      county: "lee",
      countyFips: "12071",
      release: "2026-07-22.0",
      boundarySource: "tiger/tl_2024_us_county",
      countsOnly: false,
      keepParquet: true,
      partRecordLimit: 5000,
    });
    expect(options.outputLocation).toEqual({
      kind: "local",
      dir: "/tmp/overture-lee",
    });
  });

  it("discovers the release via STAC when --release is omitted", () => {
    const options = parseExtractCli([
      "--county",
      "lee",
      "--county-fips",
      "12071",
      "--counts-only",
    ]);
    expect(options.release).toBeNull();
    expect(options.countsOnly).toBe(true);
  });

  it("rejects a bbox-only FIPS and mixed output destinations", () => {
    expect(() =>
      parseExtractCli(["--county", "lee", "--county-fips", "1207"]),
    ).toThrow("five-digit");
    expect(() =>
      parseExtractCli([
        "--county",
        "lee",
        "--county-fips",
        "12071",
        "--output-dir",
        "/tmp/a",
        "--output-s3-uri",
        "s3://bucket/prefix",
      ]),
    ).toThrow("only one of");
  });
});

describe("licence gate", () => {
  it("passes the approved providers and fails osm or other unknowns", () => {
    expect(APPROVED_PLACE_DATASETS).toEqual([
      "meta",
      "microsoft",
      "foursquare",
      "pinmeto",
      "krick",
      "rendersEO",
      "dac",
      "brightquery",
      "alltheplaces",
      "Overture",
      "Overture-signals",
    ]);
    const passing = assertApprovedPlaceDatasets(["meta", "foursquare", "microsoft"]);
    expect(passing.passed).toBe(true);
    expect(passing.osmPresent).toBe(false);

    const osm = assertApprovedPlaceDatasets(["meta", "osm"]);
    expect(osm.passed).toBe(false);
    expect(osm.osmPresent).toBe(true);

    const unknown = assertApprovedPlaceDatasets(["meta", "not-a-provider"]);
    expect(unknown.passed).toBe(false);
    expect(unknown.unknownDatasets).toEqual(["not-a-provider"]);

    const titleCase = assertApprovedPlaceDatasets([
      "Microsoft",
      "Foursquare",
      "AllThePlaces",
      "RenderSEO",
    ]);
    expect(titleCase.passed).toBe(true);

    const overtureSelf = assertApprovedPlaceDatasets(["meta", "Overture", "Overture-signals"]);
    expect(overtureSelf.passed).toBe(true);
    expect(overtureSelf.unknownDatasets).toEqual([]);
  });

  it("collects dataset names from Overture sources arrays", () => {
    expect(
      collectDatasetsFromSources([
        { dataset: "meta", record_id: "1" },
        { dataset: "foursquare" },
      ]),
    ).toEqual(["meta", "foursquare"]);
  });
});

describe("hosted-service matching", () => {
  it("matches only the full taxonomy path", () => {
    const list = parseHostedServiceCategoryList(`
# comment
financial/atm/atms
retail/rental_kiosks
`);
    expect(list).toEqual(["financial/atm/atms", "retail/rental_kiosks"]);
    expect(
      matchHostedService("financial/atm/atms", list, "hosted-service-categories@2026-07-22.0"),
    ).toEqual({
      isHostedService: true,
      hostedServiceRule: "hosted-service-categories@2026-07-22.0",
    });
    expect(matchHostedService("atms", list, "rule")).toEqual({
      isHostedService: false,
      hostedServiceRule: null,
    });
    expect(HOSTED_SERVICE_SEED_LEAVES).toContain("atms");
  });

  it("rebuilds full paths from seed leaves observed in an extract", () => {
    const rebuilt = rebuildHostedServicePaths({
      observedPaths: [
        "financial/atm/atms",
        "retail/kiosk/rental_kiosks",
        "food_and_drink/restaurant",
        "amenity/vending_machine",
      ],
      seedLeaves: ["atms", "rental_kiosks", "trusts"],
    });
    expect(rebuilt.resolved).toEqual([
      "financial/atm/atms",
      "retail/kiosk/rental_kiosks",
    ]);
    expect(rebuilt.unresolvedLeaves).toEqual(["trusts"]);
    expect(rebuilt.reviewCandidates).toEqual(["amenity/vending_machine"]);
  });

  it("resolves singular taxonomy leaves onto plural seed labels", () => {
    const rebuilt = rebuildHostedServicePaths({
      observedPaths: [
        "services_and_business/financial_service/atm",
        "services_and_business/financial_service/money_transfer_service",
        "services_and_business/real_estate/real_estate_service/rental_service/rental_kiosk",
      ],
      seedLeaves: ["atms", "rental_kiosks", "money_transfer_services"],
    });
    expect(rebuilt.resolved).toEqual([
      "services_and_business/financial_service/atm",
      "services_and_business/financial_service/money_transfer_service",
      "services_and_business/real_estate/real_estate_service/rental_service/rental_kiosk",
    ]);
    expect(rebuilt.unresolvedLeaves).toEqual([]);
  });

  it("joins hierarchy arrays with slashes, L0 first", () => {
    expect(
      taxonomyHierarchyToPath(["food_and_drink", "restaurant", "casual_eatery"]),
    ).toBe("food_and_drink/restaurant/casual_eatery");
    expect(hostedServiceRuleId("2026-07-22.0")).toBe(
      "hosted-service-categories@2026-07-22.0",
    );
  });
});

describe("STAC, TIGER, and extract SQL", () => {
  it("parses the Overture STAC catalog latest field", () => {
    const discovery = parseOvertureStacCatalog(
      {
        latest: "2026-08-20.0",
        links: [
          { rel: "child", href: "./2026-08-20.0/catalog.json" },
          { rel: "child", href: "./2026-07-22.0/catalog.json" },
        ],
      },
      "https://stac.overturemaps.org/catalog.json",
      "2026-08-12T00:00:00.000Z",
    );
    expect(discovery.latest).toBe("2026-08-20.0");
    expect(discovery.releases).toEqual(["2026-07-22.0", "2026-08-20.0"]);
  });

  it("parses a TIGER boundary source into a Census zip URL", () => {
    expect(parseTigerBoundarySource("tiger/tl_2024_us_county")).toEqual({
      year: "2024",
      stem: "tl_2024_us_county",
      zipUrl:
        "https://www2.census.gov/geo/tiger/TIGER2024/COUNTY/tl_2024_us_county.zip",
    });
  });

  it("builds two-stage extract SQL that bbox-prunes then ST_Within", () => {
    const sql = buildExtractCopySql({
      releaseLiteral: duckdbStringLiteral("2026-07-22.0"),
      countyFipsLiteral: duckdbStringLiteral("12071"),
      boundaryPathLiteral: duckdbStringLiteral("/tmp/tl_2024_us_county.shp"),
      outLiteral: duckdbStringLiteral("/tmp/places.parquet"),
      placesGlobLiteral: duckdbStringLiteral(
        "s3://overturemaps-us-west-2/release/2026-07-22.0/theme=places/type=place/*",
      ),
      limit: null,
    });
    expect(sql).toContain("LOAD spatial");
    expect(sql).toContain("SET s3_region = 'us-west-2'");
    expect(sql).toContain("p.bbox.xmin >= b.xmin");
    expect(sql).toContain("ST_Within(p.geometry, c.geometry)");
    expect(sql).toContain("p.taxonomy.hierarchy");
    expect(sql).toContain("p.taxonomy.alternates");
    expect(sql).toContain("p.categories.primary");
    expect(sql).not.toContain("confidence >");
  });
});

describe("summary counters and publish gate", () => {
  it("records a geometry-vs-address county discrepancy in source_payload shape", () => {
    const assignment = buildCountyAssignment({
      countyFips: "12071",
      countyKey: "lee",
      countyName: "Lee",
      address0: { county: "Hendry", locality: "LaBelle" },
    });
    expect(assignment).toEqual({
      assignedBy: "geometry",
      assignedCountyFips: "12071",
      assignedCountyKey: "lee",
      addressCounty: "Hendry",
      discrepancy: true,
    });
  });

  it("fails the publish gate on duplicate GERS ids, null geometry, or osm", () => {
    const licenceGate = assertApprovedPlaceDatasets(["osm"]);
    const result = validatePlacesTable({
      parquetRowCount: 2,
      businessLocationRowCount: 3,
      gersIds: ["a", "a"],
      nullGeometryCount: 1,
      licenceGate,
    });
    expect(result.passed).toBe(false);
    expect(result.errors.length).toBeGreaterThanOrEqual(3);
  });

  it("renders NOTICE.txt with Elephant's change date, not Overture's", () => {
    const notice = renderPlacesNotice({
      overtureRelease: "2026-07-22.0",
      accessedDate: "2026-08-12",
      elephantChangedDate: "2026-08-12",
      distinctDatasets: ["meta", "foursquare"],
    });
    expect(notice).toContain("Copyright 2024 Foursquare Labs, Inc.");
    expect(notice).toContain("Changed by Elephant: 2026-08-12");
    expect(notice).not.toContain("Changed: 2026-03-18");
    expect(notice).toContain(SCOPING_BASELINE_RELEASE);
  });

  it("parses the county slug from a places artifact URI", () => {
    expect(
      countySlugFromPlacesArtifactUri(
        "s3://bucket/overture-places/lee/2026-07-22.0/places/places-part-0001.jsonl",
      ),
    ).toBe("lee");
    expect(SCOPING_BASELINE_CLIP_COUNT).toBe(40_190);
  });
});
