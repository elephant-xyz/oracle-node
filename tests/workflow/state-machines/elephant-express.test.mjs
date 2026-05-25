import { describe, expect, it } from "vitest";
import { readFile } from "fs/promises";
import { parse } from "yaml";

/**
 * Load the Elephant Express ASL definition through the same YAML document that
 * deployment consumes.
 *
 * @returns {Promise<{
 *   StartAt: string,
 *   States: Record<string, {
 *     Type: string,
 *     Next?: string,
 *     Default?: string,
 *     Choices?: Array<Record<string, unknown>>,
 *     Parameters?: Record<string, unknown>,
 *   }>,
 * }>} Parsed state machine definition.
 */
async function loadStateMachine() {
  const source = await readFile(
    new URL(
      "../../../workflow/state-machines/elephant-express.asl.yaml",
      import.meta.url,
    ),
    "utf8",
  );

  return parse(source);
}

describe("Elephant Express workflow branch selection", () => {
  it("validates and normalizes the Workflow Branch before other workflow work", async () => {
    const definition = await loadStateMachine();

    expect(definition.StartAt).toBe("ValidateWorkflowBranch");
    expect(definition.States.ValidateWorkflowBranch).toMatchObject({
      Type: "Choice",
      Default: "InvalidWorkflowBranch",
    });
    expect(definition.States.ValidateWorkflowBranch.Choices).toEqual(
      expect.arrayContaining([
        {
          Variable: "$.workflowBranch",
          IsPresent: false,
          Next: "DefaultWorkflowBranch",
        },
        {
          Variable: "$.workflowBranch",
          StringEquals: "structured_archive",
          Next: "CheckDirectSubmit",
        },
        {
          Variable: "$.workflowBranch",
          StringEquals: "minting",
          Next: "CheckDirectSubmit",
        },
      ]),
    );
    expect(definition.States.DefaultWorkflowBranch).toMatchObject({
      Type: "Pass",
      Next: "CheckDirectSubmit",
    });
    expect(definition.States.InvalidWorkflowBranch).toMatchObject({
      Type: "Fail",
      Error: "InvalidWorkflowBranch",
    });
  });

  it("routes the default post-transform branch to Structured Archive", async () => {
    const definition = await loadStateMachine();

    expect(definition.States.EmitTransformSucceeded.Next).toBe(
      "ChoosePostTransformBranch",
    );
    expect(definition.States.ChoosePostTransformBranch).toMatchObject({
      Type: "Choice",
      Default: "EmitStructuredArchiveSucceeded",
    });
  });

  it("falls through to the post-transform branch when EmitTransformSucceeded errors", async () => {
    const definition = await loadStateMachine();

    expect(definition.States.EmitTransformSucceeded.Catch).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          ErrorEquals: ["States.ALL"],
          Next: "ChoosePostTransformBranch",
        }),
      ]),
    );
  });

  it("routes explicit Minting executions to the existing SVL path", async () => {
    const definition = await loadStateMachine();

    expect(definition.States.ChoosePostTransformBranch.Choices).toContainEqual({
      Variable: "$.workflowBranch",
      StringEquals: "minting",
      Next: "EmitSvlScheduled",
    });
  });

  it("preserves the Workflow Branch when rebuilding state for prepare-skip checks", async () => {
    const definition = await loadStateMachine();

    expect(definition.States.BuildHeadParams.Output).toContain(
      '"workflowBranch": $states.input.workflowBranch',
    );
  });

  it("emits a Structured Archive success event before completing default executions", async () => {
    const definition = await loadStateMachine();

    expect(definition.States.EmitStructuredArchiveSucceeded).toMatchObject({
      Type: "Task",
      Resource: "arn:aws:states:::events:putEvents",
      Parameters: {
        Entries: [
          {
            Detail: {
              status: "SUCCEEDED",
              phase: "StructuredArchive",
              step: "StructuredArchive",
            },
          },
        ],
      },
      Next: "WorkflowComplete",
    });
  });
});
