import { describe, expect, it } from "vitest";

import {
  COUNTY,
  DEFAULT_SOURCE_CSV_S3_URI,
  buildFeederMessage,
  parseArgs,
  suggestedJobId,
} from "../../scripts/send-pinellas-seed-feeder.mjs";

describe("Pinellas seed feeder sender", () => {
  it("targets the Pinellas prepare queue and appraiser source system", () => {
    expect(COUNTY.feederType).toBe("pinellas-property-first-seed-feeder");
    expect(COUNTY.sourceSystem).toBe("pinellas_appraiser");
    expect(COUNTY.slug).toBe("pinellas-property-first-seed");
    expect(COUNTY.prepareQueueName).toBe(
      "elephant-oracle-node-prepare-queue-pinellas",
    );
    expect(DEFAULT_SOURCE_CSV_S3_URI).toBe("s3://counties-seeds/pinellas.csv");
  });

  it("requires a fixed --job-id", () => {
    expect(() => parseArgs([])).toThrow(/--job-id is REQUIRED/);
    expect(parseArgs(["--job-id", "pinellas-property-first-seed-all-20260831"]))
      .toMatchObject({
        jobId: "pinellas-property-first-seed-all-20260831",
        dryRun: false,
        sourceCsvS3Uri: "s3://counties-seeds/pinellas.csv",
      });
  });

  it("builds a backpressured feeder message under the pinellas job prefix", () => {
    const cli = parseArgs([
      "--job-id",
      "pinellas-property-first-seed-all-20260831",
      "--dry-run",
    ]);
    const message = buildFeederMessage({
      cli,
      environmentBucketName: "elephant-oracle-node-environmentbucket-test",
      workflowQueueUrl: "https://sqs.us-east-1.amazonaws.com/123/workflow",
      propertyFirstPermitQueueUrl:
        "https://sqs.us-east-1.amazonaws.com/123/property-first",
      feederQueueUrl: "https://sqs.us-east-1.amazonaws.com/123/permit-harvest",
    });

    expect(message.type).toBe("pinellas-property-first-seed-feeder");
    expect(message.sourceSystem).toBe("pinellas_appraiser");
    expect(message.batchSize).toBe(500);
    expect(message.generatedSeedPrefix).toContain(
      "seed-inputs/pinellas-property-first-seed/pinellas-property-first-seed-all-20260831",
    );
    expect(message.backpressureQueues).toEqual([
      {
        name: "workflow",
        queueUrl: "https://sqs.us-east-1.amazonaws.com/123/workflow",
        maxMessages: 400,
      },
    ]);
    expect(suggestedJobId()).toMatch(/^pinellas-property-first-seed-all-\d{8}$/);
  });
});
