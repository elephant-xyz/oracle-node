import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

import {
  dedupeAndSortNormalizedPermits,
  normalizeTylerSearchResponse,
  renderNormalizedPermitJsonl,
  validateProbeQueries,
} from "../../scripts/permit-source-adapters/tyler-civic-access.mjs";
import { parseOptions } from "../../scripts/probe-rock-island-permits.mjs";

const CONFIG = {
  portalBaseUrl:
    "https://cityofrockislandil-energovweb.tylerhost.net/apps/selfservice",
  city: "Rock Island",
  sourceSystem: "rock_island_city_tyler_permits",
};

const fixture = JSON.parse(
  await readFile(
    new URL(
      "../fixtures/rock-island-permits/tyler-search-response.json",
      import.meta.url,
    ),
    "utf8",
  ),
);

describe("Rock Island Tyler Civic Access pilot", () => {
  it("normalizes only permit entities into the city-permit JSONL contract", () => {
    const records = normalizeTylerSearchResponse(fixture, CONFIG);

    expect(records).toHaveLength(2);
    expect(records[0]).toEqual({
      source_system: "rock_island_city_tyler_permits",
      source_url:
        "https://cityofrockislandil-energovweb.tylerhost.net/apps/selfservice#/permit/22222222-2222-4222-8222-222222222222",
      city: "Rock Island",
      permit_number: "BLDG-2026-00002",
      parcel_identifier: "1700000002",
      work_location: "200 TEST AVE ROCK ISLAND IL 61201",
      permit_issue_date: "2026-03-04",
      record_status: "Finaled",
      record_type: "Building - Residential",
      project_description: "REPLACE EXISTING ROOF",
      is_roof_permit: true,
      raw: {
        case_id: "22222222-2222-4222-8222-222222222222",
        work_class: "Roofing",
        applied_date: "2026-03-01",
        expiration_date: "2026-09-01",
        finalized_date: "2026-03-20",
      },
    });
    expect(records[1]).toMatchObject({
      permit_number: "MECH-2026-00001",
      work_location: "100 TEST AVE ROCK ISLAND IL 61201",
      is_roof_permit: false,
    });
    expect(
      records.some((record) => record.permit_number.startsWith("INSP")),
    ).toBe(false);
  });

  it("deduplicates and sorts deterministic JSONL without contact fields", () => {
    const records = normalizeTylerSearchResponse(fixture, CONFIG);
    const duplicatedAndReversed = [records[1], records[0], records[1]];

    expect(
      dedupeAndSortNormalizedPermits(duplicatedAndReversed).map(
        (record) => record.permit_number,
      ),
    ).toEqual(["BLDG-2026-00002", "MECH-2026-00001"]);

    const jsonl = renderNormalizedPermitJsonl(duplicatedAndReversed);
    expect(jsonl.endsWith("\n")).toBe(true);
    expect(jsonl.trim().split("\n")).toHaveLength(2);
    expect(jsonl).not.toMatch(
      /applicant|assignedTo|email|contact|CompanyName|HolderFirstName/i,
    );
  });

  it("enforces the ten-lookup and low-rate CLI guardrails", () => {
    expect(validateProbeQueries(["  PERMIT-1  "], 10)).toEqual(["PERMIT-1"]);
    expect(() =>
      validateProbeQueries(
        Array.from({ length: 11 }, (_, index) => `PERMIT-${String(index)}`),
        10,
      ),
    ).toThrow("approved maximum is 10");

    expect(
      parseOptions([
        "--query",
        "PERMIT-1",
        "--output",
        "downloads/rock-island/permit-pilot.jsonl",
        "--delay-ms",
        "1250",
      ]),
    ).toEqual({
      queries: ["PERMIT-1"],
      outputPath: "downloads/rock-island/permit-pilot.jsonl",
      delayMs: 1250,
    });
    expect(() =>
      parseOptions(["--query", "PERMIT-1", "--delay-ms", "999"]),
    ).toThrow("at least 1000");
  });

  it("fails closed when the public API envelope is unsuccessful", () => {
    expect(() =>
      normalizeTylerSearchResponse(
        {
          Success: false,
          ErrorMessage: "Cannot find tenant information.",
          Result: null,
        },
        CONFIG,
      ),
    ).toThrow("Cannot find tenant information.");
  });
});
