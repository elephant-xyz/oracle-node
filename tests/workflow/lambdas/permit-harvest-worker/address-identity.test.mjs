import { describe, expect, it } from "vitest";

import {
  buildAppraisalSitusAddressContext,
  mapAppraisalTransformedFile,
  mintSitusAddressIdentity,
} from "../../../../workflow/lambdas/permit-harvest-worker/query-db-loader/index.js";

const GOLDEN_UUID = "c3a982a7-1102-50b8-b2cd-6cb3fca2060f";
const GOLDEN_TOKEN =
  "da5b90e067f162ea35eb482befaea835b32df7861adb282c6fb3983f17fa325e";

function addressRow(bundle) {
  return bundle.rows.find((row) => row.tableName === "addresses");
}

describe("AWS appraisal address identity parity", () => {
  it("matches the canonical address:v1 golden vector", () => {
    expect(
      mintSitusAddressIdentity({
        state: "FL",
        postalCode: "32225",
        street: "11659 JONATHAN RD",
      }),
    ).toMatchObject({
      elephantUuid: GOLDEN_UUID,
      elephantToken: GOLDEN_TOKEN,
    });
  });

  it("mints from the authoritative sibling situs record", () => {
    const situsAddressContext = buildAppraisalSitusAddressContext([
      {
        filePath: "data/unnormalized_address.json",
        record: {
          full_address: "11659 JONATHAN RD, JACKSONVILLE, FL 32225",
        },
      },
    ]);
    const bundle = mapAppraisalTransformedFile({
      artifactUri: "s3://bucket/transformed.zip",
      filePath: "data/address.json",
      record: {
        request_identifier: "11659-jonathan",
        unnormalized_address: "1 MAILING ST, NEW YORK, NY 10001",
      },
      situsAddressContext,
    });

    expect(addressRow(bundle)?.values).toMatchObject({
      elephant_uuid: GOLDEN_UUID,
      elephant_token: GOLDEN_TOKEN,
    });
  });

  it("never assigns property identity to a mailing-address row", () => {
    const bundle = mapAppraisalTransformedFile({
      artifactUri: "s3://bucket/transformed.zip",
      filePath: "data/mailing_address_1.json",
      record: {
        request_identifier: "11659-jonathan",
        unnormalized_address: "1 MAILING ST, NEW YORK, NY 10001",
      },
    });

    expect(addressRow(bundle)?.values.elephant_uuid).toBeNull();
    expect(addressRow(bundle)?.values.elephant_token).toBeNull();
  });
});
