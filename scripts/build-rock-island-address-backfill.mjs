#!/usr/bin/env node

import { createHash } from "node:crypto";
import { chmod, mkdir, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { pathToFileURL } from "node:url";

const DEFAULT_OUTPUT_PATH =
  "downloads/rock-island/private/rock-island-site-address-backfill-v1.json";
const PACKAGE_ID = "rock-island-site-address-backfill-v1";
const EVIDENCE_CAPTURED_ON = "2026-08-14";
const PARCEL_LAYER_URL =
  "https://services9.arcgis.com/6FnscPPlUa9DXXOk/ArcGIS/rest/services/Parcels/FeatureServer/0";
const PARCEL_ITEM_ID = "9cae8a64ab0e4cea99758f741ca43b3c";
const ADDRESS_LAYER_URL =
  "https://gis.rockislandcountyil.gov/arcgis/rest/services/Hosted/AddressPoints/FeatureServer/0";
const ADDRESS_ITEM_GUID = "02E4F14A-8124-4B89-B883-29F94A8EDD9E";
const CORRECTED_QUERY_TABLE_CID =
  "QmQnm6W2Ye9GH3oD6SUswHrQCMegnpGbhRFgipitYW6zCc";
const VALIDATED_PARCEL_SNAPSHOT = Object.freeze({
  evidencePath: "downloads/rock-island/rock-island.csv",
  sourceRevision: "2026-07-14T12:08:19.189Z",
  sourceSnapshotAt: "2026-08-03T18:45:08.716Z",
});

/**
 * @typedef {object} FoundAddressEvidence
 * @property {string} folio - Exact ten-digit Rock Island PIN.
 * @property {number} parcelObjectId - Parcel FeatureServer object ID.
 * @property {string} ricoParcelId - County cross-layer parcel key.
 * @property {number} addressObjectId - E911 AddressPoints object ID.
 * @property {string} address - E911 display address.
 * @property {string} propertyAddress - AddressPoints `PRP_ADDR` value.
 * @property {string} cityCode - AddressPoints city abbreviation.
 * @property {string} cityState - AddressPoints `PRP_CTYST` value.
 * @property {string} postalCode - AddressPoints `PRP_ZIP` value.
 * @property {number} latitude - WGS84 address-point latitude.
 * @property {number} longitude - WGS84 address-point longitude.
 * @property {"current_parcel_layer" | "validated_2026_08_03_parcel_snapshot"} parcelKeyEvidence - How PIN-to-RICO_PARCE was proven.
 *
 * @typedef {object} NotFoundEvidence
 * @property {string} folio - Exact ten-digit Rock Island PIN.
 * @property {number} parcelObjectId - Current or validated historical parcel object ID.
 * @property {string} ricoParcelId - County cross-layer parcel key.
 * @property {"current_parcel_layer" | "validated_2026_08_03_parcel_snapshot"} parcelKeyEvidence - How PIN-to-RICO_PARCE was proven.
 * @property {string} reason - Evidence-backed reason no address is staged.
 *
 * @typedef {object} SiteAddress
 * @property {string} streetLine - Official E911 address line.
 * @property {string} city - Official city name from `PRP_CTYST`.
 * @property {"IL"} stateCode - Official state code.
 * @property {string} postalCode - Official five-digit ZIP code.
 * @property {string} unnormalizedAddress - Complete reviewable site address.
 *
 * @typedef {object} BackfillRecord
 * @property {string} folio - Idempotency key.
 * @property {"found" | "not_found"} status - Investigation result.
 * @property {SiteAddress | null} siteAddress - Staged site address, never a mailing address.
 * @property {string | null} reason - Why no address is staged.
 * @property {false} conflicting - Whether authoritative source fields conflict.
 * @property {Record<string, unknown>} provenance - Official parcel and E911 source evidence.
 *
 * @typedef {object} BackfillPackage
 * @property {"1.0"} schemaVersion - Package schema version.
 * @property {string} packageId - Stable package identifier.
 * @property {"private_review_only"} classification - Review boundary.
 * @property {false} apply - Production mutation guard.
 * @property {string} evidenceCapturedOn - Evidence review date.
 * @property {string} correctedQueryTableCid - Immutable public scope evidence.
 * @property {{folioCount:number, found:number, notFound:number, conflicting:number}} summary - Scope counts.
 * @property {Record<string, BackfillRecord>} recordsByFolio - Deterministic folio-keyed records.
 * @property {string} recordsSha256 - Stable digest of the folio-keyed record object.
 * @property {Record<string, unknown>} applyPolicy - Explicit future-apply contract.
 */

/**
 * Exact null-address scope from the immutable corrected public query table.
 *
 * @type {readonly string[]}
 */
export const TARGET_FOLIOS = Object.freeze([
  "0436100005",
  "0831449003",
  "0831449018",
  "0834120022",
  "0919106035",
  "1532102015",
  "1601301027",
  "1602412002",
  "1602429005",
  "1602429006",
  "1612122002",
  "1614114001",
  "1614201026",
  "1701111005",
  "1702125007",
  "1703114018",
  "1707301004",
  "1707301009",
  "1707301014",
  "1708107010",
  "1709203010",
  "1712409027",
  "1723424030",
  "1726300042",
  "2326201005",
]);

/**
 * Official E911 records joined through exact county `RICO_PARCE` values.
 *
 * @type {readonly Readonly<FoundAddressEvidence>[]}
 */
export const FOUND_ADDRESS_EVIDENCE = Object.freeze([
  {
    folio: "0436100005",
    parcelObjectId: 3805,
    ricoParcelId: "05159-1",
    addressObjectId: 6610,
    address: "1107 S HIGH ST",
    propertyAddress: "1107 S HIGH ST",
    cityCode: "PB",
    cityState: "PORT BYRON IL",
    postalCode: "61275",
    latitude: 41.59247692512323,
    longitude: -90.33598075729404,
    parcelKeyEvidence: "current_parcel_layer",
  },
  {
    folio: "0834120022",
    parcelObjectId: 25853,
    ricoParcelId: "081645",
    addressObjectId: 28492,
    address: "530 37TH ST",
    propertyAddress: "530 37 ST",
    cityCode: "MO",
    cityState: "MOLINE IL",
    postalCode: "61265",
    latitude: 41.50986273187581,
    longitude: -90.48475011914756,
    parcelKeyEvidence: "current_parcel_layer",
  },
  {
    folio: "0919106035",
    parcelObjectId: 65932,
    ricoParcelId: "065553",
    addressObjectId: 8230,
    address: "298 ISLAND AV",
    propertyAddress: "298 ISLAND AVE",
    cityCode: "EM",
    cityState: "EAST MOLINE IL",
    postalCode: "61244",
    latitude: 41.54240097984008,
    longitude: -90.43281440401708,
    parcelKeyEvidence: "current_parcel_layer",
  },
  {
    folio: "1532102015",
    parcelObjectId: 44890,
    ricoParcelId: "161034",
    addressObjectId: 845,
    address: "9101 141ST ST W",
    propertyAddress: "9101 141ST ST W",
    cityCode: "TR",
    cityState: "TAYLOR RIDGE IL",
    postalCode: "61284",
    latitude: 41.42876323111389,
    longitude: -90.7592805722963,
    parcelKeyEvidence: "current_parcel_layer",
  },
  {
    folio: "1601301027",
    parcelObjectId: 65933,
    ricoParcelId: "1052-A",
    addressObjectId: 50055,
    address: "1800 9TH 1/2 ST",
    propertyAddress: "1800 9 1/2 ST",
    cityCode: "RI",
    cityState: "ROCK ISLAND IL",
    postalCode: "61201",
    latitude: 41.49399109934365,
    longitude: -90.58519833045294,
    parcelKeyEvidence: "current_parcel_layer",
  },
  {
    folio: "1602412002",
    parcelObjectId: 47897,
    ricoParcelId: "102183",
    addressObjectId: 43192,
    address: "1824 22ND ST",
    propertyAddress: "1824 22 ST",
    cityCode: "RI",
    cityState: "ROCK ISLAND IL",
    postalCode: "61201",
    latitude: 41.4928826948934,
    longitude: -90.57011465569916,
    parcelKeyEvidence: "current_parcel_layer",
  },
  {
    folio: "1602429005",
    parcelObjectId: 53407,
    ricoParcelId: "102168",
    addressObjectId: 49775,
    address: "2019 17TH ST",
    propertyAddress: "2019 17 ST",
    cityCode: "RI",
    cityState: "ROCK ISLAND IL",
    postalCode: "61201",
    latitude: 41.49055452642107,
    longitude: -90.57219147887803,
    parcelKeyEvidence: "validated_2026_08_03_parcel_snapshot",
  },
  {
    folio: "1612122002",
    parcelObjectId: 55913,
    ricoParcelId: "104742",
    addressObjectId: 47183,
    address: "2618 29TH 1/2 ST CT",
    propertyAddress: "2618 29 1/2 ST CT",
    cityCode: "RI",
    cityState: "ROCK ISLAND IL",
    postalCode: "61201",
    latitude: 41.483443081088154,
    longitude: -90.55900128742897,
    parcelKeyEvidence: "current_parcel_layer",
  },
  {
    folio: "1614114001",
    parcelObjectId: 54910,
    ricoParcelId: "102595-B",
    addressObjectId: 48051,
    address: "3902 14TH ST",
    propertyAddress: "3902 14 ST",
    cityCode: "RI",
    cityState: "ROCK ISLAND IL",
    postalCode: "61201",
    latitude: 41.47225532124675,
    longitude: -90.57990334594255,
    parcelKeyEvidence: "current_parcel_layer",
  },
  {
    folio: "1614201026",
    parcelObjectId: 51905,
    ricoParcelId: "103891-84",
    addressObjectId: 48055,
    address: "10 HAWTHORNE RD",
    propertyAddress: "10 HAWTHORNE RD",
    cityCode: "RI",
    cityState: "ROCK ISLAND IL",
    postalCode: "61201",
    latitude: 41.47211592733757,
    longitude: -90.5753066072105,
    parcelKeyEvidence: "current_parcel_layer",
  },
  {
    folio: "1701111005",
    parcelObjectId: 57416,
    ricoParcelId: "0714381",
    addressObjectId: 18322,
    address: "443 35TH AV",
    propertyAddress: "443 35 AVE",
    cityCode: "EM",
    cityState: "EAST MOLINE IL",
    postalCode: "61244",
    latitude: 41.49664337871596,
    longitude: -90.44967885083317,
    parcelKeyEvidence: "current_parcel_layer",
  },
  {
    folio: "1702125007",
    parcelObjectId: 12324,
    ricoParcelId: "0714206",
    addressObjectId: 18804,
    address: "5409 19TH AV",
    propertyAddress: "5409 19 AVE",
    cityCode: "MO",
    cityState: "MOLINE IL",
    postalCode: "61265",
    latitude: 41.49543198047309,
    longitude: -90.46273612039062,
    parcelKeyEvidence: "current_parcel_layer",
  },
  {
    folio: "1703114018",
    parcelObjectId: 10322,
    ricoParcelId: "0712961",
    addressObjectId: 18129,
    address: "1511 37TH ST",
    propertyAddress: "1511 37 ST",
    cityCode: "MO",
    cityState: "MOLINE IL",
    postalCode: "61265",
    latitude: 41.49725599786262,
    longitude: -90.48435595124532,
    parcelKeyEvidence: "current_parcel_layer",
  },
  {
    folio: "1707301004",
    parcelObjectId: 54409,
    ricoParcelId: "104280",
    addressObjectId: 50872,
    address: "4417 37TH AV",
    propertyAddress: "4417 37 AVE",
    cityCode: "RI",
    cityState: "ROCK ISLAND IL",
    postalCode: "61201",
    latitude: 41.475167567256825,
    longitude: -90.54121516568927,
    parcelKeyEvidence: "current_parcel_layer",
  },
  {
    folio: "1707301009",
    parcelObjectId: 54408,
    ricoParcelId: "104274",
    addressObjectId: 50805,
    address: "3714 44TH ST",
    propertyAddress: "3714 44 ST",
    cityCode: "RI",
    cityState: "ROCK ISLAND IL",
    postalCode: "61201",
    latitude: 41.47425085346571,
    longitude: -90.54247442478268,
    parcelKeyEvidence: "current_parcel_layer",
  },
  {
    folio: "1707301014",
    parcelObjectId: 54410,
    ricoParcelId: "104284",
    addressObjectId: 50880,
    address: "4420 37TH AV",
    propertyAddress: "4420 37 AVE",
    cityCode: "RI",
    cityState: "ROCK ISLAND IL",
    postalCode: "61201",
    latitude: 41.47462667123159,
    longitude: -90.54112999649128,
    parcelKeyEvidence: "current_parcel_layer",
  },
  {
    folio: "1708107010",
    parcelObjectId: 20843,
    ricoParcelId: "089236",
    addressObjectId: 35683,
    address: "2704 11TH ST CT",
    propertyAddress: "2704 11TH ST CT",
    cityCode: "MO",
    cityState: "MOLINE IL",
    postalCode: "61265",
    latitude: 41.48372129940305,
    longitude: -90.52110913557914,
    parcelKeyEvidence: "current_parcel_layer",
  },
  {
    folio: "1709203010",
    parcelObjectId: 17336,
    ricoParcelId: "0712994",
    addressObjectId: 23402,
    address: "2913 28TH AV A",
    propertyAddress: "2913 28 AVE A",
    cityCode: "MO",
    cityState: "MOLINE IL",
    postalCode: "61265",
    latitude: 41.479060963115764,
    longitude: -90.49875779834436,
    parcelKeyEvidence: "current_parcel_layer",
  },
  {
    folio: "1712409027",
    parcelObjectId: 14831,
    ricoParcelId: "0714517",
    addressObjectId: 23930,
    address: "3410 78TH ST CT",
    propertyAddress: "3410 78 ST CT",
    cityCode: "MO",
    cityState: "MOLINE IL",
    postalCode: "61265",
    latitude: 41.477564210224,
    longitude: -90.43373587502525,
    parcelKeyEvidence: "current_parcel_layer",
  },
]);

/**
 * Exact-key searches that returned no E911 AddressPoints record.
 *
 * @type {readonly Readonly<NotFoundEvidence>[]}
 */
export const NOT_FOUND_EVIDENCE = Object.freeze([
  {
    folio: "0831449003",
    parcelObjectId: 27355,
    ricoParcelId: "089419",
    parcelKeyEvidence: "current_parcel_layer",
    reason:
      "The official E911 AddressPoints layer returned zero records for exact RICO_PARCE 089419.",
  },
  {
    folio: "0831449018",
    parcelObjectId: 27356,
    ricoParcelId: "089424",
    parcelKeyEvidence: "current_parcel_layer",
    reason:
      "The official E911 AddressPoints layer returned zero records for exact RICO_PARCE 089424.",
  },
  {
    folio: "1602429006",
    parcelObjectId: 53408,
    ricoParcelId: "102169-1",
    parcelKeyEvidence: "validated_2026_08_03_parcel_snapshot",
    reason:
      "The official E911 AddressPoints layer returned zero records for exact historical RICO_PARCE 102169-1; the current parcel layer also no longer returns this PIN.",
  },
  {
    folio: "1723424030",
    parcelObjectId: 64931,
    ricoParcelId: "121988",
    parcelKeyEvidence: "current_parcel_layer",
    reason:
      "The official E911 AddressPoints layer returned zero records for exact RICO_PARCE 121988.",
  },
  {
    folio: "1726300042",
    parcelObjectId: 39881,
    ricoParcelId: "1274-7",
    parcelKeyEvidence: "current_parcel_layer",
    reason:
      "The official E911 AddressPoints layer returned zero records for exact RICO_PARCE 1274-7.",
  },
  {
    folio: "2326201005",
    parcelObjectId: 42385,
    ricoParcelId: "141155",
    parcelKeyEvidence: "current_parcel_layer",
    reason:
      "The official E911 AddressPoints layer returned zero records for exact RICO_PARCE 141155.",
  },
]);

/**
 * Build an exact official ArcGIS query URL for one field value.
 *
 * @param {string} layerUrl - ArcGIS FeatureServer layer URL.
 * @param {string} fieldName - Exact source field name.
 * @param {string | number} fieldValue - Exact source key.
 * @returns {string} Reviewable official query URL.
 */
function exactQueryUrl(layerUrl, fieldName, fieldValue) {
  const url = new URL(`${layerUrl}/query`);
  const encodedValue =
    typeof fieldValue === "number"
      ? String(fieldValue)
      : `'${fieldValue.replaceAll("'", "''")}'`;
  url.searchParams.set("where", `${fieldName}=${encodedValue}`);
  url.searchParams.set("outFields", "*");
  url.searchParams.set("returnGeometry", "true");
  url.searchParams.set("outSR", "4326");
  url.searchParams.set("f", "pjson");
  return url.href;
}

/**
 * Read a city name from the official `PRP_CTYST` value.
 *
 * @param {string} cityState - Official city/state text.
 * @returns {string} City name without the Illinois suffix.
 */
function cityName(cityState) {
  const city = cityState.replace(/\s+IL$/u, "").trim();
  if (city.length === 0)
    throw new Error(`Invalid PRP_CTYST value: ${cityState}`);
  return city;
}

/**
 * Convert one official E911 record into a staged, site-only backfill record.
 *
 * @param {Readonly<FoundAddressEvidence>} evidence - Exact official join evidence.
 * @returns {BackfillRecord} Review-only found record.
 */
function foundRecord(evidence) {
  const city = cityName(evidence.cityState);
  return {
    folio: evidence.folio,
    status: "found",
    siteAddress: {
      streetLine: evidence.address,
      city,
      stateCode: "IL",
      postalCode: evidence.postalCode,
      unnormalizedAddress: `${evidence.address}, ${city} IL ${evidence.postalCode}`,
    },
    reason: null,
    conflicting: false,
    provenance: {
      addressRole: "site",
      prohibitedSourcesExcluded: ["owner", "mailing", "tax_bill"],
      parcel: {
        layerUrl: PARCEL_LAYER_URL,
        itemId: PARCEL_ITEM_ID,
        objectId: evidence.parcelObjectId,
        pin: evidence.folio,
        ricoParcelId: evidence.ricoParcelId,
        keyEvidence: evidence.parcelKeyEvidence,
        validatedSnapshot:
          evidence.parcelKeyEvidence === "validated_2026_08_03_parcel_snapshot"
            ? VALIDATED_PARCEL_SNAPSHOT
            : null,
        exactQueryUrl: exactQueryUrl(PARCEL_LAYER_URL, "PIN", evidence.folio),
      },
      e911AddressPoint: {
        layerUrl: ADDRESS_LAYER_URL,
        itemGuid: ADDRESS_ITEM_GUID,
        objectId: evidence.addressObjectId,
        exactQueryUrl: exactQueryUrl(
          ADDRESS_LAYER_URL,
          "objectid",
          evidence.addressObjectId,
        ),
        raw: {
          address: evidence.address,
          propertyAddress: evidence.propertyAddress,
          cityCode: evidence.cityCode,
          propertyCityState: evidence.cityState,
          propertyZip: evidence.postalCode,
          ricoParcelId: evidence.ricoParcelId,
        },
        geometry: {
          latitude: evidence.latitude,
          longitude: evidence.longitude,
          spatialReference: "EPSG:4326",
        },
      },
      sourceAgreement:
        evidence.address === evidence.propertyAddress
          ? "exact"
          : "equivalent_county_normalization",
    },
  };
}

/**
 * Convert an exact-key miss into a reviewable non-backfill record.
 *
 * @param {Readonly<NotFoundEvidence>} evidence - Exact official join evidence.
 * @returns {BackfillRecord} Review-only not-found record.
 */
function notFoundRecord(evidence) {
  return {
    folio: evidence.folio,
    status: "not_found",
    siteAddress: null,
    reason: evidence.reason,
    conflicting: false,
    provenance: {
      addressRole: "site",
      prohibitedSourcesExcluded: ["owner", "mailing", "tax_bill"],
      parcel: {
        layerUrl: PARCEL_LAYER_URL,
        itemId: PARCEL_ITEM_ID,
        objectId: evidence.parcelObjectId,
        pin: evidence.folio,
        ricoParcelId: evidence.ricoParcelId,
        keyEvidence: evidence.parcelKeyEvidence,
        validatedSnapshot:
          evidence.parcelKeyEvidence === "validated_2026_08_03_parcel_snapshot"
            ? VALIDATED_PARCEL_SNAPSHOT
            : null,
        exactQueryUrl: exactQueryUrl(PARCEL_LAYER_URL, "PIN", evidence.folio),
      },
      e911AddressPoint: {
        layerUrl: ADDRESS_LAYER_URL,
        itemGuid: ADDRESS_ITEM_GUID,
        exactQueryUrl: exactQueryUrl(
          ADDRESS_LAYER_URL,
          "rico_parce",
          evidence.ricoParcelId,
        ),
        matchingRecordCount: 0,
      },
    },
  };
}

/**
 * Build the deterministic, non-applying address backfill package.
 *
 * @returns {BackfillPackage} Exact 25-folio review package.
 */
export function buildAddressBackfillPackage() {
  /** @type {Map<string, BackfillRecord>} */
  const records = new Map();
  for (const evidence of FOUND_ADDRESS_EVIDENCE) {
    records.set(evidence.folio, foundRecord(evidence));
  }
  for (const evidence of NOT_FOUND_EVIDENCE) {
    if (records.has(evidence.folio)) {
      throw new Error(
        `Conflicting address evidence for folio ${evidence.folio}`,
      );
    }
    records.set(evidence.folio, notFoundRecord(evidence));
  }

  const expected = [...TARGET_FOLIOS].sort();
  const actual = [...records.keys()].sort();
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(
      `Backfill scope mismatch: expected ${expected.length} folios, found ${actual.length}`,
    );
  }
  const recordsByFolio = Object.fromEntries(
    actual.map((folio) => [folio, records.get(folio)]),
  );
  const recordsJson = JSON.stringify(recordsByFolio);
  const found = actual.filter(
    (folio) => recordsByFolio[folio]?.status === "found",
  ).length;
  const notFound = actual.length - found;
  const conflicting = actual.filter(
    (folio) => recordsByFolio[folio]?.conflicting === true,
  ).length;
  return {
    schemaVersion: "1.0",
    packageId: PACKAGE_ID,
    classification: "private_review_only",
    apply: false,
    evidenceCapturedOn: EVIDENCE_CAPTURED_ON,
    correctedQueryTableCid: CORRECTED_QUERY_TABLE_CID,
    summary: {
      folioCount: actual.length,
      found,
      notFound,
      conflicting,
    },
    recordsByFolio,
    recordsSha256: createHash("sha256").update(recordsJson).digest("hex"),
    applyPolicy: {
      targetKey: "request_identifier/folio",
      operation: "upsert_site_address_only_when_current_site_address_is_null",
      conflictPolicy:
        "fail_if_target_has_a_different_non_null_site_address_or_source_key",
      rerunPolicy:
        "skip_when_the_same_folio_and_recordsSha256_have_already_been_applied",
      databaseMutationPerformedByThisPackage: false,
    },
  };
}

/**
 * Serialize a package with stable formatting.
 *
 * @param {BackfillPackage} packageValue - Validated backfill package.
 * @returns {string} Stable JSON document.
 */
export function renderAddressBackfillPackage(packageValue) {
  return `${JSON.stringify(packageValue, null, 2)}\n`;
}

/**
 * Write the review package with owner-only filesystem permissions.
 *
 * @param {string} outputPath - Private package destination.
 * @returns {Promise<{outputPath:string, packageValue:BackfillPackage}>} Written package details.
 */
export async function writeAddressBackfillPackage(outputPath) {
  const absoluteOutputPath = resolve(outputPath);
  const packageValue = buildAddressBackfillPackage();
  await mkdir(dirname(absoluteOutputPath), { recursive: true, mode: 0o700 });
  await writeFile(
    absoluteOutputPath,
    renderAddressBackfillPackage(packageValue),
    { encoding: "utf8", mode: 0o600 },
  );
  await chmod(absoluteOutputPath, 0o600);
  return { outputPath: absoluteOutputPath, packageValue };
}

/**
 * Parse the optional output path.
 *
 * @param {readonly string[]} argv - Arguments after the script path.
 * @returns {string} Output path.
 */
export function parseOutputPath(argv) {
  if (argv.length === 0) return DEFAULT_OUTPUT_PATH;
  if (argv.length === 2 && argv[0] === "--out" && argv[1].length > 0) {
    return argv[1];
  }
  throw new Error(
    "Usage: build-rock-island-address-backfill.mjs [--out <path>]",
  );
}

if (
  typeof process.argv[1] === "string" &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  writeAddressBackfillPackage(parseOutputPath(process.argv.slice(2)))
    .then(({ outputPath, packageValue }) => {
      console.log(
        JSON.stringify(
          {
            outputPath,
            ...packageValue.summary,
            recordsSha256: packageValue.recordsSha256,
            apply: packageValue.apply,
          },
          null,
          2,
        ),
      );
    })
    .catch((error) => {
      console.error(error instanceof Error ? error.message : String(error));
      process.exitCode = 1;
    });
}
