import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

import {
  unwrapBrowardPrepareCapture,
  requireParcelRecords,
} from "../../scripts/capture-broward-parcel.mjs";
import {
  parseCliOptions,
  parseValidationIssues,
  readCaptureParcel,
  renderSeedRecord,
} from "../../scripts/validate-broward-appraisal.mjs";

const BROWARD_TRANSFORM_PATCH_URL = new URL(
  "../../docs/patches/counties-transform-scripts-broward-live-capture.patch",
  import.meta.url,
);
const BROWARD_TRANSFORM_PATCH_SHA256 =
  "0a5cddfc53ce778317694c8968964ec42586e6acb1588362941bb8f66c666582";

describe("Broward prepare capture unwrap", () => {
  it("unwraps the elephant-cli multi-request wrapper", () => {
    const envelope = unwrapBrowardPrepareCapture({
      input: {
        source_http_request: { method: "POST" },
        response: {
          d: {
            parcelInfok__BackingField: [
              { folioNumber: "504108BJ0140", useCode: "04 - Condominium" },
            ],
          },
        },
      },
    });
    expect(envelope.d?.parcelInfok__BackingField).toHaveLength(1);
    const { record } = readCaptureParcel(
      /** @type {import("adm-zip").IZipEntry} */ ({
        getData: () =>
          Buffer.from(
            JSON.stringify({
              input: {
                response: {
                  d: {
                    parcelInfok__BackingField: [
                      {
                        folioNumber: "504108BJ0140",
                        useCode: "04 - Condominium",
                        situsCity: "PLANTATION",
                      },
                    ],
                  },
                },
              },
            }),
          ),
      }),
      "504108BJ0140",
    );
    expect(record.useCode).toBe("04 - Condominium");
    expect(record.situsCity).toBe("PLANTATION");
  });

  it("passes a raw ASP.NET envelope through", () => {
    const envelope = unwrapBrowardPrepareCapture({
      d: { parcelInfok__BackingField: [{ folioNumber: "474135010090" }] },
    });
    expect(requireParcelRecords(envelope, "474135010090")).toHaveLength(1);
  });

  it("fails closed on a wrapped empty parcel list", () => {
    expect(() =>
      requireParcelRecords(
        unwrapBrowardPrepareCapture({
          input: {
            response: { d: { parcelInfok__BackingField: null } },
          },
        }),
        "474131010000",
      ),
    ).toThrow(/no parcelInfok__BackingField/);
  });
});

describe("Broward appraisal validation harness", () => {
  it("preserves the complete accepted live-capture transform patch", async () => {
    const patch = await readFile(BROWARD_TRANSFORM_PATCH_URL);
    expect(createHash("sha256").update(patch).digest("hex")).toBe(
      BROWARD_TRANSFORM_PATCH_SHA256,
    );
    const source = patch.toString("utf8");
    expect(source).toContain("findBrowardPropertyMapping");
    expect(source).toContain("capturedInput?.input?.response");
    expect(source).toContain("relationship_property_has_structure.json");
    expect(source).toContain("relationship_property_has_utility.json");
  });

  it("renders a complete one-row seed without corrupting JSON or geometry", () => {
    const csv = renderSeedRecord(["request_identifier", "parcel_polygon"], {
      request_identifier: "504108BJ0140",
      parcel_polygon: `{"type":"Polygon","coordinates":[[[-80,26],[-80.1,26],[-80,26]]]}`,
    });

    expect(csv).toContain("504108BJ0140");
    expect(csv.split("\n")).toHaveLength(3);
  });

  it("parses bounded local runs", () => {
    expect(
      parseCliOptions([
        "--limit",
        "1",
        "--skip-prepare",
        "--skip-validate",
        "--apply-use-code-fix",
        "--output",
        "downloads/broward/test-output",
      ]),
    ).toMatchObject({
      limit: 1,
      prepareCaptures: false,
      skipValidate: true,
      applyUseCodeFix: true,
      outputDirectory: "downloads/broward/test-output",
    });
    expect(() => parseCliOptions(["--limit", "-1"])).toThrow(
      /positive integer/,
    );
  });

  it("preserves distinct validator issues from the error CSV", () => {
    const csv = [
      "error_message,file_path",
      '"Schema CID fetch failed","property.json"',
      '"Unused fact_sheet.json","fact_sheet.json"',
      '"Schema CID fetch failed","lot.json"',
      "",
    ].join("\n");

    expect(parseValidationIssues(csv)).toEqual([
      "Schema CID fetch failed",
      "Unused fact_sheet.json",
    ]);
  });
});
