import { describe, expect, it } from "vitest";

import {
  hashSunbizMatchCandidates,
  parseSunbizMatchOptions,
} from "../../scripts/match-broward-sunbiz-to-properties.mjs";

/**
 * Build one complete exact-hash match candidate.
 *
 * @param {Record<string, string>} [overrides] - Field overrides.
 * @returns {{
 *   businessRegistrationAddressId:string,
 *   businessRegistrationId:string,
 *   originalAddressId:string,
 *   matchedAddressId:string,
 *   propertyId:string,
 *   normalizedAddressHash:string,
 *   addressRole:string
 * }} Candidate.
 */
function candidate(overrides = {}) {
  return {
    businessRegistrationAddressId: "11111111-1111-4111-8111-111111111111",
    businessRegistrationId: "22222222-2222-4222-8222-222222222222",
    originalAddressId: "33333333-3333-4333-8333-333333333333",
    matchedAddressId: "44444444-4444-4444-8444-444444444444",
    propertyId: "55555555-5555-4555-8555-555555555555",
    normalizedAddressHash: "a".repeat(64),
    addressRole: "principalAddress",
    ...overrides,
  };
}

describe("Broward Sunbiz exact property matching", () => {
  it("parses dry-run and durable chunk options", () => {
    expect(
      parseSunbizMatchOptions([
        "--job-id",
        "broward-sunbiz-property-pilot-20260831",
        "--source-catalog",
        "docs/broward-sources.yaml",
        "--chunk-size",
        "1000",
        "--limit",
        "100",
        "--apply",
        "true",
      ]),
    ).toEqual({
      jobId: "broward-sunbiz-property-pilot-20260831",
      sourceCatalogPath: "docs/broward-sources.yaml",
      chunkSize: 1000,
      limit: 100,
      apply: true,
    });
    expect(() =>
      parseSunbizMatchOptions(["--job-id", "unscoped", "--apply", "false"]),
    ).toThrow(/broward-sunbiz-/u);
  });

  it("keeps candidate identity stable when the current source address is repointed", () => {
    const before = candidate();
    const after = candidate({
      originalAddressId: before.matchedAddressId,
    });
    expect(hashSunbizMatchCandidates([before])).toBe(
      hashSunbizMatchCandidates([after]),
    );
    expect(
      hashSunbizMatchCandidates([
        candidate({
          propertyId: "66666666-6666-4666-8666-666666666666",
        }),
      ]),
    ).not.toBe(hashSunbizMatchCandidates([before]));
    expect(hashSunbizMatchCandidates([before])).toMatch(/^[a-f0-9]{64}$/u);
  });
});
