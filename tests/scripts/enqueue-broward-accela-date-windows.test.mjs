import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

import {
  buildBrowardAccelaWindowMessage,
  createEnqueueDateWindows,
  enqueueBrowardAccelaDateWindows,
  parseEnqueueBrowardAccelaOptions,
} from "../../scripts/enqueue-broward-accela-date-windows.mjs";

describe("Broward Accela FIFO window enqueue", () => {
  it("defines a disabled-by-default FIFO worker lane in CloudFormation", async () => {
    const template = await readFile(
      new URL("../../permit-harvest/template.yaml", import.meta.url),
      "utf8",
    );
    expect(template).toContain("BrowardPermitEnumerationQueue:");
    expect(template).toContain("FifoQueue: true");
    expect(template).toContain(
      "BROWARD_PERMIT_ENUMERATION_QUEUE_URL: !Ref BrowardPermitEnumerationQueue",
    );
    expect(template).toMatch(
      /BrowardPermitEnumerationWorkerEventSourceMapping:[\s\S]*?Enabled: false/u,
    );
  });

  it("requires explicit source/date identity and builds exhaustive windows", () => {
    const options = parseEnqueueBrowardAccelaOptions([
      "--source",
      "weston",
      "--job-id",
      "broward-permits-weston-all-20260831",
      "--start-date",
      "1997-01-01",
      "--end-date",
      "1997-01-05",
      "--window-days",
      "2",
      "--queue-url",
      "https://sqs.us-east-1.amazonaws.com/123/broward.fifo",
      "--dry-run",
    ]);
    expect(options).toMatchObject({
      sourceKey: "weston",
      jobId: "broward-permits-weston-all-20260831",
      startDate: "1997-01-01",
      endDate: "1997-01-05",
      windowDays: 2,
      dryRun: true,
    });
    expect(
      createEnqueueDateWindows(
        options.startDate,
        options.endDate,
        options.windowDays,
      ),
    ).toEqual([
      { startDate: "1997-01-01", endDate: "1997-01-02" },
      { startDate: "1997-01-03", endDate: "1997-01-04" },
      { startDate: "1997-01-05", endDate: "1997-01-05" },
    ]);
    expect(() =>
      parseEnqueueBrowardAccelaOptions([
        "--source",
        "fort-lauderdale",
        "--job-id",
        "broward-permits-invalid",
        "--start-date",
        "2026-01-01",
        "--end-date",
        "2026-01-02",
      ]),
    ).toThrow(/hollywood, plantation, cooper-city, or weston/u);
  });

  it("uses the jurisdiction as FIFO group for every initial window", async () => {
    const sent = [];
    const logs = [];
    const options = {
      sourceKey: "hollywood",
      jobId: "broward-permits-hollywood-all-20260831",
      startDate: "2026-08-28",
      endDate: "2026-08-31",
      windowDays: 2,
      splitThreshold: 100,
      maxPages: 200,
      queueUrl:
        "https://sqs.us-east-1.amazonaws.com/123/broward-enumeration.fifo",
      stackName: "unused",
      outputPrefix: "s3://example-bucket/permit-harvest",
      dryRun: false,
    };
    const result = await enqueueBrowardAccelaDateWindows(options, {
      sqs: {
        send: async (command) => {
          sent.push(command.input);
          return {};
        },
      },
      log: (value) => logs.push(value),
    });
    expect(result).toEqual({
      queueUrl: options.queueUrl,
      windowCount: 2,
      dryRun: false,
    });
    expect(sent).toHaveLength(2);
    expect(sent).toEqual([
      expect.objectContaining({
        QueueUrl: options.queueUrl,
        MessageGroupId: "hollywood",
        MessageDeduplicationId: expect.stringMatching(/^[a-f0-9]{64}$/u),
      }),
      expect.objectContaining({
        QueueUrl: options.queueUrl,
        MessageGroupId: "hollywood",
        MessageDeduplicationId: expect.stringMatching(/^[a-f0-9]{64}$/u),
      }),
    ]);
    expect(JSON.parse(sent[0].MessageBody)).toEqual(
      buildBrowardAccelaWindowMessage(options, {
        startDate: "2026-08-28",
        endDate: "2026-08-29",
      }),
    );
    expect(logs).toHaveLength(2);
  });
});
