import { describe, expect, it } from "vitest";

import {
  DATASET_VERSION,
  PERMIT_BUCKET,
  PERMIT_IPNS_LABEL,
  assertDedicatedPublicationTarget,
  buildSafePermitUploadPlan,
  deriveFilebaseApiToken,
  parseIpfsRoots,
} from "../../scripts/publish-rock-island-safe-permits.mjs";

describe("Rock Island safe permit publisher", () => {
  it("uses a dedicated versioned bucket plan", () => {
    const plan = buildSafePermitUploadPlan();
    expect(plan).toHaveLength(5);
    expect(plan.filter((artifact) => artifact.ipnsTarget)).toEqual([
      expect.objectContaining({
        name: "parquet",
        key: `versions/${DATASET_VERSION}/permit-query.parquet`,
      }),
    ]);
    expect(plan.map((artifact) => artifact.fileName)).toEqual([
      "permit-query.parquet",
      "schema.json",
      "manifest.json",
      "coverage.json",
      "privacy-scan.json",
    ]);
  });

  it("refuses property and corporate publication targets", () => {
    expect(() =>
      assertDedicatedPublicationTarget(
        "elephant-oracle-open-data-rock-island",
        PERMIT_IPNS_LABEL,
      ),
    ).toThrow(/non-dedicated permit bucket/);
    expect(() =>
      assertDedicatedPublicationTarget(
        PERMIT_BUCKET,
        "oracle-corporate-registration-rock-island",
      ),
    ).toThrow(/non-dedicated permit IPNS label/);
    expect(() =>
      assertDedicatedPublicationTarget(PERMIT_BUCKET, PERMIT_IPNS_LABEL),
    ).not.toThrow();
  });

  it("derives the documented Filebase bearer token without exposing inputs", () => {
    expect(
      deriveFilebaseApiToken({
        S3_ACCESS_KEY_ID: "access",
        S3_SECRET_ACCESS_KEY: "secret",
      }),
    ).toBe(Buffer.from("access:secret").toString("base64"));
    expect(
      deriveFilebaseApiToken({
        S3_ACCESS_KEY_ID: "access",
        S3_SECRET_ACCESS_KEY: "secret",
        FILEBASE_API_TOKEN: " explicit-token ",
      }),
    ).toBe("explicit-token");
  });

  it("parses gateway CID roots", () => {
    expect(parseIpfsRoots("/ipfs/QmOne, /ipfs/QmTwo")).toEqual([
      "QmOne",
      "QmTwo",
    ]);
    expect(parseIpfsRoots(null)).toEqual([]);
  });
});
