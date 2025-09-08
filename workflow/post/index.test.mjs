import { describe, it, expect, vi } from "vitest";
import * as s3mod from "@aws-sdk/client-s3";
import { handler } from "./index.mjs";

describe("post handler", () => {
    it("fails fast on missing env", async () => {
        await expect(handler({ prepare: { output_s3_uri: "s3://b/k.zip" }, Records: [{ s3: { object: { key: "in.csv" } } }] })).rejects.toBeTruthy();
    });
});


