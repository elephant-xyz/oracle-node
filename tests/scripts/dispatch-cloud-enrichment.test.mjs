import { describe, expect, it } from "vitest";
import { calculateLambdaCostUsd } from "../../scripts/hillsborough/dispatch-cloud-enrichment.mjs";

describe("dispatch-cloud-enrichment", () => {
  it("calculates precise AWS Lambda invocation cost based on memory and duration", () => {
    // 1000ms duration on 256MB Arm64
    const cost1s = calculateLambdaCostUsd(1000);
    // 1s * (256/1024) * 0.0000133334 + 0.0000002 = 0.00000333335 + 0.0000002 = ~0.00000353 USD
    expect(cost1s).toBeGreaterThan(0.000003);
    expect(cost1s).toBeLessThan(0.000004);

    // 1 million 1-second invocations = ~$3.53
    const cost1M = cost1s * 1000000;
    expect(cost1M).toBeGreaterThan(3.5);
    expect(cost1M).toBeLessThan(3.6);
  });
});
