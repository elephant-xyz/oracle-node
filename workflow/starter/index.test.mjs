import { describe, it, expect, vi } from "vitest";
import * as sfnMod from "@aws-sdk/client-sfn";
import { handler } from "./index.mjs";

describe("starter handler", () => {
    it("starts execution for one message", async () => {
        process.env.STATE_MACHINE_ARN = "arn:aws:states:us-east-1:123:stateMachine:test";
        const send = vi.spyOn(sfnMod.SFNClient.prototype, "send").mockResolvedValue({ executionArn: "arn:exec", status: "SUCCEEDED" });
        const evt = { Records: [{ body: JSON.stringify({ hello: "world" }) }] };
        const res = await handler(evt);
        expect(res.status).toBe("ok");
        expect(send).toHaveBeenCalledTimes(1);
    });
});


