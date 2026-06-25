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

  it("routes the default post-transform branch through the SVL gate to Structured Archive", async () => {
    const definition = await loadStateMachine();

    expect(definition.States.EmitTransformSucceeded.Next).toBe(
      "ChoosePostTransformBranch",
    );
    // Non-minting (default) branches now pass through the archive SVL gate first,
    // then resume to Structured Archive once SVL passes.
    expect(definition.States.ChoosePostTransformBranch).toMatchObject({
      Type: "Choice",
      Default: "EmitArchiveSvlScheduled",
    });
    expect(definition.States.ChooseArchivePostSvl).toMatchObject({
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

  it("routes seeded property-first appraisal executions to the permit queue after transform", async () => {
    const definition = await loadStateMachine();

    // Post-transform, non-minting branches now pass through the archive SVL gate first;
    // the property-first permit routing resumes in ChooseArchivePostSvl (after SVL passes).
    expect(definition.States.ChooseArchivePostSvl.Choices).toContainEqual({
      Variable: "$.message.propertyFirstPermitQueueUrl",
      IsPresent: true,
      Next: "ChoosePropertyFirstPermitEligibility",
    });
    expect(definition.States.Transform.ResultSelector).toMatchObject({
      "propertyFirstPermitEligibility.$": "$.propertyFirstPermitEligibility",
    });
    expect(
      definition.States.ChoosePropertyFirstPermitEligibility,
    ).toMatchObject({
      Type: "Choice",
      Choices: [
        {
          Variable: "$.transform.propertyFirstPermitEligibility.shouldEnqueue",
          BooleanEquals: true,
          Next: "EmitPropertyFirstPermitScheduled",
        },
      ],
      Default: "EmitPropertyFirstPermitSkipped",
    });
    expect(definition.States.EmitPropertyFirstPermitSkipped).toMatchObject({
      Type: "Task",
      Resource: "arn:aws:states:::events:putEvents",
      Next: "EmitStructuredArchiveSucceeded",
    });
    expect(definition.States.Preprocess.ResultSelector).toMatchObject({
      "parcel_identifier.$": "$.Payload.parcel_identifier",
      "request_identifier.$": "$.Payload.request_identifier",
      "best_permit_address.$": "$.Payload.best_permit_address",
    });
    expect(definition.States.EnqueuePropertyFirstPermit).toMatchObject({
      Type: "Task",
      Resource: "arn:aws:states:::sqs:sendMessage",
      Parameters: {
        "QueueUrl.$": "$.message.propertyFirstPermitQueueUrl",
        MessageBody: {
          type: "lee-property-first-permit-parcel",
          version: 1,
          "jobId.$": "$.message.propertyFirstPermitJobId",
          "parcelIdentifier.$": "$.pre.parcel_identifier",
          "requestIdentifier.$": "$.pre.request_identifier",
          "appraisalOutputS3Uri.$": "$.transform.transformedOutputS3Uri",
          "appraisalPreparedOutputS3Uri.$": "$.prepare.output_s3_uri",
          "outputPrefix.$": "$.message.propertyFirstPermitOutputPrefix",
          loadToNeon: true,
          loadAppraisalToNeon: true,
        },
      },
      Next: "EmitPropertyFirstPermitEnqueued",
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
