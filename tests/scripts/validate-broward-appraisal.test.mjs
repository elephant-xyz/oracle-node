import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";

import { afterEach, describe, expect, it, vi } from "vitest";

import {
  BROWARD_SOURCE_TIMEOUT_MS,
  fetchBrowardParcelEnvelope,
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
  "ef1e201c20503ae00c2107db035801861f5baf6256245d7f64c558e4517d191a";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("Broward prepare capture unwrap", () => {
  it("bounds every live BCPA request with an abort timeout", async () => {
    /** @type {RequestInit | undefined} */
    let observedOptions;
    vi.stubGlobal(
      "fetch",
      vi.fn(
        /**
         * Capture the request options without making a network request.
         *
         * @param {string | URL | Request} _input - Ignored BCPA URL.
         * @param {RequestInit | undefined} options - Fetch request options.
         * @returns {Promise<Pick<Response, "ok" | "json">>} Minimal successful response.
         */
        (_input, options) => {
          observedOptions = options;
          return Promise.resolve({
            ok: true,
            json: () =>
              Promise.resolve({
                d: {
                  parcelInfok__BackingField: [
                    { folioNumber: "504108BJ0140" },
                  ],
                },
              }),
          });
        },
      ),
    );

    await expect(
      fetchBrowardParcelEnvelope("504108BJ0140"),
    ).resolves.toMatchObject({
      d: { parcelInfok__BackingField: [{ folioNumber: "504108BJ0140" }] },
    });
    expect(BROWARD_SOURCE_TIMEOUT_MS).toBe(15_000);
    expect(observedOptions?.signal).toBeInstanceOf(AbortSignal);
    expect(observedOptions?.signal?.aborted).toBe(false);
  });

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
    expect(source).toContain("parsed = JSON.parse(rawHtml)");
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
