import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import * as path from "node:path";

import { describe, expect, it } from "vitest";

const REPO_ROOT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../..",
);
const TEMPLATE_PATH = path.join(
  REPO_ROOT,
  "infra/overture-places-refresh/template.yaml",
);

/**
 * Read the embedded Amazon States Language JSON from the CloudFormation file.
 *
 * @returns {Promise<Record<string, unknown>>} Parsed state machine definition.
 */
async function readStateMachineDefinition() {
  const template = await readFile(TEMPLATE_PATH, "utf8");
  const match = template.match(
    /      DefinitionString: \|\n([\s\S]+?)\n\n  SchedulerRole:/,
  );
  if (match?.[1] === undefined) {
    throw new Error("Could not locate Overture refresh DefinitionString");
  }
  const json = match[1]
    .split("\n")
    .map((line) => line.slice(8))
    .join("\n");
  return JSON.parse(json);
}

describe("Overture places refresh infrastructure", () => {
  it("deploys the monthly schedule disabled and unapproved", async () => {
    const template = await readFile(TEMPLATE_PATH, "utf8");
    expect(template).toMatch(/MonthlySchedule:[\s\S]+?State: DISABLED/);
    expect(template).toContain('"publishApproved":false');
    expect(template).toContain("ScheduleState:\n    Value: DISABLED");
  });

  it("contains every explicit workflow gate and county lock", async () => {
    const definition = await readStateMachineDefinition();
    const states = /** @type {Record<string, Record<string, unknown>>} */ (
      definition.States
    );
    expect(Object.keys(states)).toEqual(
      expect.arrayContaining([
        "Plan",
        "AcquireCountyLock",
        "Extract",
        "ValidateIncoming",
        "LoadIncremental",
        "ExportFullCurrent",
        "Publish",
        "VerifyPublishedPointer",
        "Finalize",
        "RollbackPublishedPointer",
      ]),
    );
    expect(states.ValidateIncoming?.Retry).toBeUndefined();
    expect(states.Extract?.Retry).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          MaxAttempts: 2,
          JitterStrategy: "FULL",
        }),
      ]),
    );
  });

  it("scopes artifacts and references secrets without a DynamoDB scan filter", async () => {
    const template = await readFile(TEMPLATE_PATH, "utf8");
    expect(template).toContain(
      "arn:${AWS::Partition}:s3:::${WorkBucketName}/overture-places-refresh/*",
    );
    expect(template).toContain("ValueFrom: !Ref DatabaseSecretArn");
    expect(template).toContain(
      'ValueFrom: !Sub "${FilebaseSecretArn}:FILEBASE_API_TOKEN::"',
    );
    expect(template).not.toContain("FilterExpression");
    expect(template).toContain(
      "ConditionExpression\": \"attribute_not_exists(lock_key) OR expires_at < :now",
    );
  });
});
