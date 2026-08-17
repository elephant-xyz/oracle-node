import { CID } from "multiformats/cid";
import { sha256 } from "multiformats/hashes/sha2";
import { describe, expect, it } from "vitest";

import {
  collectReferencedSchemaCids,
  parseCliOptions,
  verifyCidBytes,
} from "../../scripts/prefetch-elephant-schema-cache.mjs";

/**
 * Build a raw CID for deterministic test bytes.
 *
 * @param {Uint8Array} bytes - Content bytes.
 * @returns {Promise<string>} CIDv1 string.
 */
async function cidFor(bytes) {
  return CID.createV1(0x55, await sha256.digest(bytes)).toString();
}

describe("Elephant schema cache prefetch", () => {
  it("accepts only bytes matching the expected CID", async () => {
    const bytes = new TextEncoder().encode('{"title":"County"}');
    const cid = await cidFor(bytes);

    await expect(verifyCidBytes(cid, bytes)).resolves.toBeUndefined();
    await expect(
      verifyCidBytes(cid, new TextEncoder().encode('{"title":"Other"}')),
    ).rejects.toThrow(/do not match/);
  });

  it("collects distinct nested schema CID references", async () => {
    const first = await cidFor(new TextEncoder().encode("first"));
    const second = await cidFor(new TextEncoder().encode("second"));

    expect(
      collectReferencedSchemaCids({
        properties: {
          one: { cid: first },
          two: { items: { cid: second } },
          duplicate: { cid: first },
        },
      }),
    ).toEqual([first, second]);
  });

  it("validates bounded concurrency", () => {
    expect(parseCliOptions(["--concurrency", "4"]).concurrency).toBe(4);
    expect(() => parseCliOptions(["--concurrency", "0"])).toThrow(
      /1 through 32/,
    );
  });
});
