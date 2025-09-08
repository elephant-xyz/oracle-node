import { describe, it, expect } from "vitest";
import { handler } from "./index.mjs";

describe("pre handler", () => {
    it("derives paths from s3 event", async () => {
        process.env.OUTPUT_BASE_URI = "s3://out/outputs";
        const res = await handler({ Records: [{ s3: { bucket: { name: "b" }, object: { key: "k/input.csv" } } }] });
        expect(res.input_s3_uri).toBe("s3://b/k/input.csv");
        expect(res.output_prefix).toContain("s3://out/outputs/input");
    });
});


