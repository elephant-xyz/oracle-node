import { describe, expect, it } from "vitest";

import {
  parseAddress,
  parseOptions,
  propertyId,
  rowFromArtifacts,
} from "../../scripts/opendoor/export_query_table.mjs";

describe("OpenDoor targeted query-table export", () => {
  it("parses county defaults and explicit flags", () => {
    expect(parseOptions(["--county=pasco"])).toMatchObject({
      county: "pasco",
      countyName: "Pasco",
      seedPath: "data/seeds/pasco.csv",
      ingestDirectory: "downloads/pasco/local-ingest",
      outputDirectory: "data/artifacts/publish/pasco",
      allowMissing: false,
    });
    expect(
      parseOptions(["--county", "st-johns", "--allow-missing"]),
    ).toMatchObject({
      county: "st-johns",
      countyName: "St Johns",
      allowMissing: true,
    });
  });

  it("parses the transformed situs address", () => {
    expect(parseAddress("14741 11TH STREET, DADE CITY, FL 33523")).toEqual({
      street: "14741 11TH STREET",
      city: "DADE CITY",
      zip: "33523",
    });
  });

  it("uses a stable county-scoped property id", () => {
    const first = propertyId("pasco_appraiser", "27-24-21-0570-00000-0260");
    expect(first).toBe(
      propertyId("pasco_appraiser", "27-24-21-0570-00000-0260"),
    );
    expect(first).not.toBe(
      propertyId("lake_appraiser", "27-24-21-0570-00000-0260"),
    );
  });

  it("preserves OpenDoor identity instead of reminting unit rows", () => {
    const files = new Map([
      [
        "property.json",
        {
          parcel_identifier: "31-25-27-5678-0022-2202",
          property_type: "Building",
          subdivision: "CHAMPIONS CLUB CONDO PH 22",
        },
      ],
      [
        "address.json",
        {
          unnormalized_address: "1207 LONG COVE LOOP, DAVENPORT, FL 33896",
        },
      ],
    ]);
    const row = rowFromArtifacts({
      county: "osceola",
      countyName: "Osceola",
      seedRow: {
        parcel_id: "31-25-27-5678-0022-2202",
        latitude: "28.254",
        longitude: "-81.618",
        elephant_uuid: "b9f97069-135a-5094-8706-94c37ca35e35",
        elephant_token:
          "a71e797946e7e60d0bba18e4e044b656e5b55e15a58c0609cf3bdf2455f7430d",
      },
      files,
      identity: null,
      captureAddress: null,
    });

    expect(row.elephant_uuid).toBe("b9f97069-135a-5094-8706-94c37ca35e35");
    expect(row.elephant_token).toBe(
      "a71e797946e7e60d0bba18e4e044b656e5b55e15a58c0609cf3bdf2455f7430d",
    );
    expect(row.address_street).toBe("1207 LONG COVE LOOP");
    expect(row.latitude).toBe(28.254);
    expect(row.longitude).toBe(-81.618);
    expect(row.subdivision).toBe("CHAMPIONS CLUB CONDO PH 22");
  });
});
