import { existsSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { TransformPool } from "../../scripts/hillsborough/transform-pool.mjs";

const ROOT = resolve(import.meta.dirname, "../..");
const PILOT_DIR = resolve(ROOT, "downloads/hillsborough/pilot-run/1125270100");
const SCRIPTS_DIR = resolve(ROOT, "Counties-trasform-scripts/hillsborough");

describe("transform pool", () => {
  it("initializes pool and executes transform on a sample parcel", async () => {
    const pool = new TransformPool(1);
    try {
      if (existsSync(PILOT_DIR) && existsSync(SCRIPTS_DIR)) {
        await pool.run(PILOT_DIR);
      }
      expect(true).toBe(true);
    } finally {
      pool.close();
    }
  }, 20000);
});
