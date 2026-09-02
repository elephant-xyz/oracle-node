import { describe, expect, it } from "vitest";

import {
  DUVAL_VALIDATION_BBOX,
  assertGeometryInCounty,
  classifyValidationGap,
  collectGeometryPoints,
  formatValidationIssueLines,
  lexiconFailureNarrative,
  parseDuvalValidateArgs,
  parseStaticPartSelectors,
  reconcileIngestManifest,
  scoreLabeledFieldCoverage,
} from "../../scripts/duval/validate-lib.mjs";

describe("Duval appraisal validation helpers", () => {
  it("defaults the output report path and rejects a non-positive limit", () => {
    const options = parseDuvalValidateArgs(["--limit=2"]);
    expect(options.limit).toBe(2);
    expect(options.pilotRoot).toMatch(/downloads\/duval\/pilot-run$/);
    expect(options.reportPath).toMatch(
      /docs\/duval-appraisal-transform-validation\.md$/,
    );
    expect(() => parseDuvalValidateArgs(["--limit=0"])).toThrow(
      /positive integer/,
    );
  });

  it("keeps geometry inside the Task 7 Duval bbox and rejects a point outside it", () => {
    expect(DUVAL_VALIDATION_BBOX.minLat).toBe(30.103);
    expect(
      collectGeometryPoints({
        latitude: 30.245,
        longitude: -81.773,
        polygon: [{ latitude: 30.245, longitude: -81.773 }],
      }),
    ).toEqual([
      { latitude: 30.245, longitude: -81.773 },
      { latitude: 30.245, longitude: -81.773 },
    ]);
    expect(() =>
      assertGeometryInCounty([{ latitude: 30.245, longitude: -81.773 }]),
    ).not.toThrow();
    expect(() =>
      assertGeometryInCounty([{ latitude: 25.7, longitude: -80.2 }]),
    ).toThrow(/Duval bbox/);
  });

  it("reconciles ingest counts and requires 50 distinct parcel ids", () => {
    expect(() =>
      reconcileIngestManifest({
        reconciled: {
          seedRows: 50,
          attempted: 50,
          success: 50,
          failures: 0,
        },
        results: Array.from({ length: 50 }, (_, index) => ({
          folio: String(index).padStart(10, "0"),
        })),
      }),
    ).not.toThrow();
    expect(() =>
      reconcileIngestManifest({
        reconciled: {
          seedRows: 50,
          attempted: 49,
          success: 48,
          failures: 1,
        },
        results: [{ folio: "0969250000" }],
      }),
    ).toThrow(/seedRows/);
  });

  it("scores labeled COJ fields against transform JSON, ignoring static chrome", () => {
    const html = `
      <span id="ctl00_cphBody_lblRealEstateNumber">096925-0000</span>
      <span id="ctl00_cphBody_lblNote">working tax roll</span>
      <div id="nav">Property Appraiser</div>
    `;
    const selectors = parseStaticPartSelectors(
      ["cssSelector", '"#nav"', '"#ctl00_cphBody_lblNote"'].join("\n"),
    );
    expect(selectors).toContain("#nav");
    const score = scoreLabeledFieldCoverage(
      html,
      selectors,
      JSON.stringify({ parcel_identifier: "096925-0000" }),
    );
    expect(score.onPage).toBe(1);
    expect(score.inTransform).toBe(1);
    expect(score.ratio).toBe(1);
    expect(scoreLabeledFieldCoverage(html, selectors, "{}").inTransform).toBe(
      0,
    );
  });

  it("classifies gaps as extractor, capture, or lexicon", () => {
    expect(classifyValidationGap("must match schema enum")).toBe("lexicon");
    expect(classifyValidationGap("missing required property 'city_name'")).toBe(
      "lexicon",
    );
    expect(classifyValidationGap("missing property.json")).toBe("extractor");
    expect(
      classifyValidationGap("labeled field Roof Covering is absent from JSON"),
    ).toBe("capture");
    expect(
      classifyValidationGap("unexpected property 'unnormalized_address'"),
    ).toBe("lexicon");
  });

  it("groups lexicon issues and still lists catch-path failures", () => {
    const lines = formatValidationIssueLines(
      [
        {
          folio: "0000010005",
          issues: [
            {
              issue: "missing required property 'city_name'",
              class: "lexicon",
            },
          ],
        },
        {
          folio: "0000010005",
          issues: [
            {
              issue: "missing required property 'city_name'",
              class: "lexicon",
            },
          ],
        },
        {
          folio: "0000020010",
          error: "coordinate 25.7,-80.2 is outside the Duval bbox",
          class: "extractor",
        },
      ],
      50,
    );
    expect(lines).toContain(
      "**2/50** (lexicon): missing required property 'city_name'",
    );
    expect(lines).toContain("`0000020010`");
    expect(lines).toContain("outside the Duval bbox");
  });

  it("omits the address-wrap narrative when lexicon fully passes", () => {
    expect(
      lexiconFailureNarrative({
        lexiconPassed: 50,
        selectedCount: 50,
        meanCompleteness: 0.5,
      }),
    ).toBe("");
    expect(
      lexiconFailureNarrative({
        lexiconPassed: 0,
        selectedCount: 50,
        meanCompleteness: 0.108,
      }),
    ).toMatch(/Lexicon did not pass/);
  });
});
