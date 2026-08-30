import { describe, expect, it } from "vitest";
import { resolve } from "node:path";
import { TransformPool } from "../../scripts/hillsborough/transform-pool.mjs";

const ROOT = resolve(import.meta.dirname, "../..");
const PILOT_DIR = resolve(ROOT, "downloads/hillsborough/pilot-run/1125270100");

describe("transform pool", () => {
  it(
    "initializes pool and executes transform on a sample parcel",
    async () => {
      const pool = new TransformPool(2);
      try {
        await pool.run(PILOT_DIR);
        expect(true).toBe(true);
      } finally {
        pool.close();
      }
    },
    20000,
  );
});
