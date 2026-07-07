import { describe, expect, it } from "vitest";
import { _private } from "../../../../workflow/lambdas/permit-harvest-worker/index.mjs";

describe("permit harvest worker message helpers", () => {
  it("builds Lee detail batches even for windows that may continue splitting", () => {
    const sourceMessage = {
      type: "lee-permit-list-window",
      version: 1,
      jobId: "lee-permit-backfill-test",
      startDate: "1990-01-01",
      endDate: "1990-01-30",
      outputPrefix: "s3://example-bucket/permit-harvest",
      maxPages: 200,
      detailBatchSize: 2,
      splitThreshold: 100,
    };

    const permits = [
      {
        recordNumber: "RES1990-00001",
        url: "https://aca-prod.accela.com/LEECO/Cap/CapDetail.aspx?id=1",
        address: "1 MAIN ST",
        description: "FIRST PERMIT",
        status: "Closed",
        submittalType: "ePlan",
        relatedRecords: "0",
        action: null,
        sourceWindowKey: "19900101_19900130",
        sourcePage: 1,
      },
      {
        recordNumber: "RES1990-00002",
        url: "https://aca-prod.accela.com/LEECO/Cap/CapDetail.aspx?id=2",
        address: "2 MAIN ST",
        description: "SECOND PERMIT",
        status: "Issued",
        submittalType: "ePlan",
        relatedRecords: "0",
        action: null,
        sourceWindowKey: "19900101_19900130",
        sourcePage: 1,
      },
      {
        recordNumber: "RES1990-00003",
        url: "https://aca-prod.accela.com/LEECO/Cap/CapDetail.aspx?id=3",
        address: "3 MAIN ST",
        description: "THIRD PERMIT",
        status: "Issued",
        submittalType: "ePlan",
        relatedRecords: "0",
        action: null,
        sourceWindowKey: "19900101_19900130",
        sourcePage: 1,
      },
    ];

    expect(
      _private.buildLeePermitDetailBatchMessages({
        sourceMessage,
        windowKey: "19900101_19900130",
        permits,
        batchSize: 2,
      }),
    ).toEqual([
      {
        type: "lee-permit-detail-batch",
        version: 1,
        jobId: "lee-permit-backfill-test",
        windowKey: "19900101_19900130",
        batchIndex: 0,
        permits: permits.slice(0, 2),
        outputPrefix: "s3://example-bucket/permit-harvest",
        skipExisting: true,
      },
      {
        type: "lee-permit-detail-batch",
        version: 1,
        jobId: "lee-permit-backfill-test",
        windowKey: "19900101_19900130",
        batchIndex: 1,
        permits: permits.slice(2),
        outputPrefix: "s3://example-bucket/permit-harvest",
        skipExisting: true,
      },
    ]);
  });

  it("validates Sunbiz ZIP extraction messages and resolves source format", () => {
    const message = _private.validateMessage({
      type: "sunbiz-corporate-zip-extract",
      version: 1,
      jobId: "sunbiz-lee-smoke",
      extractKey: "lee-county-zips",
      sourceDataS3Uri: "s3://example-bucket/sunbiz/cordata.zip",
      zipPrefixes: ["33917", "33907"],
      chunkRecordLimit: 500,
      maxRecords: 25,
      outputPrefix: "s3://example-bucket/permit-harvest",
    });

    expect(message).toMatchObject({
      type: "sunbiz-corporate-zip-extract",
      extractKey: "lee-county-zips",
      zipPrefixes: ["33917", "33907"],
    });
    expect(_private.resolveSunbizSourceFormat(message)).toBe("zip");
  });

  it("validates property-first parcel messages and builds normalized targets", () => {
    const message = _private.validateMessage({
      type: "lee-property-first-permit-parcel",
      version: 1,
      jobId: "lee-property-first-all-20260606",
      parcelIdentifier: "08-46-25-57-00000.0130",
      requestIdentifier: "12345678",
      appraisalOutputS3Uri: "s3://example-bucket/appraisal/folio-12345678.json",
      appraisalPreparedOutputS3Uri:
        "s3://example-bucket/appraisal/folio-12345678-prepared.zip",
      propertyId: "property-1",
      propertyUsageType: "Industrial",
      maxPages: 250,
      outputPrefix: "s3://example-bucket/permit-harvest",
      loadAppraisalToNeon: true,
    });

    expect(message).toMatchObject({
      type: "lee-property-first-permit-parcel",
      parcelIdentifier: "08-46-25-57-00000.0130",
      maxPages: 250,
      loadAppraisalToNeon: true,
    });
    expect(_private.buildPropertyFirstTarget(message)).toEqual({
      parcelIdentifier: "08-46-25-57-00000.0130",
      normalizedParcelIdentifier: "08462557000000130",
      requestIdentifier: "12345678",
      appraisalOutputS3Uri: "s3://example-bucket/appraisal/folio-12345678.json",
      appraisalPreparedOutputS3Uri:
        "s3://example-bucket/appraisal/folio-12345678-prepared.zip",
      propertyId: "property-1",
      propertyUsageType: "Industrial",
      bestPermitAddress: null,
      addressBase: null,
    });
  });

  it("routes only commercial appraiser usage types to property-first permit retrieval", () => {
    const eligibleUsageTypes =
      _private.resolvePropertyFirstPermitEligibleUsageTypes();

    expect(
      _private.buildPropertyFirstPermitEligibility({
        propertyUsageType: "Industrial",
        readError: null,
        eligibleUsageTypes,
      }),
    ).toMatchObject({
      shouldEnqueue: true,
      reason: "eligible_property_usage_type",
      propertyUsageType: "Industrial",
    });

    expect(
      _private.buildPropertyFirstPermitEligibility({
        propertyUsageType: "Residential",
        readError: null,
        eligibleUsageTypes,
      }),
    ).toMatchObject({
      shouldEnqueue: false,
      reason: "non_commercial_property_usage_type",
      propertyUsageType: "Residential",
    });
  });

  it("bypasses the appraiser usage-type gate for on-demand MCP harvests only", () => {
    const eligibleUsageTypes =
      _private.resolvePropertyFirstPermitEligibleUsageTypes();
    const ineligible = _private.buildPropertyFirstPermitEligibility({
      propertyUsageType: "Residential",
      readError: null,
      eligibleUsageTypes,
    });
    const eligible = _private.buildPropertyFirstPermitEligibility({
      propertyUsageType: "Industrial",
      readError: null,
      eligibleUsageTypes,
    });

    // Bulk/seed path (onDemand false) still skips ineligible parcels.
    expect(
      _private.shouldSkipIneligiblePropertyFirstParcel({
        eligibility: ineligible,
        onDemand: false,
      }),
    ).toBe(true);

    // On-demand path harvests the ineligible parcel anyway.
    expect(
      _private.shouldSkipIneligiblePropertyFirstParcel({
        eligibility: ineligible,
        onDemand: true,
      }),
    ).toBe(false);

    // Eligible parcels are never skipped, regardless of onDemand.
    expect(
      _private.shouldSkipIneligiblePropertyFirstParcel({
        eligibility: eligible,
        onDemand: false,
      }),
    ).toBe(false);
    expect(
      _private.shouldSkipIneligiblePropertyFirstParcel({
        eligibility: eligible,
        onDemand: true,
      }),
    ).toBe(false);
  });

  it("validates the optional onDemand flag on property-first parcel messages", () => {
    expect(
      _private.validateMessage({
        type: "lee-property-first-permit-parcel",
        version: 1,
        jobId: "mcp-on-demand-12071-1",
        parcelIdentifier: "294627L40900A1339",
        onDemand: true,
      }),
    ).toMatchObject({ onDemand: true });

    // Absent onDemand preserves current (bulk/seed) behavior.
    expect(
      _private.validateMessage({
        type: "lee-property-first-permit-parcel",
        version: 1,
        jobId: "seed-12071-1",
        parcelIdentifier: "294627L40900A1339",
      }).onDemand,
    ).toBeUndefined();

    expect(() =>
      _private.validateMessage({
        type: "lee-property-first-permit-parcel",
        version: 1,
        jobId: "bad-onDemand",
        parcelIdentifier: "294627L40900A1339",
        onDemand: "yes",
      }),
    ).toThrow("onDemand must be a boolean");
  });

  it("validates seed feeder messages and preserves one-row Lee seed CSVs", () => {
    const message = _private.validateMessage({
      type: "lee-property-first-seed-feeder",
      version: 1,
      jobId: "lee-property-first-seed-all-20260606",
      sourceCsvS3Uri: "s3://counties-seeds/lee.csv",
      workflowQueueUrl:
        "https://sqs.us-east-1.amazonaws.com/123/elephant-workflow-queue",
      propertyFirstPermitQueueUrl:
        "https://sqs.us-east-1.amazonaws.com/123/property-first",
      feederQueueUrl: "https://sqs.us-east-1.amazonaws.com/123/property-first",
      generatedSeedPrefix:
        "s3://example-bucket/seed-inputs/lee-property-first-seed/job",
      workflowOutputBaseUri:
        "s3://example-bucket/outputs/lee-property-first-seed/job",
      propertyFirstPermitOutputPrefix:
        "s3://example-bucket/permit-harvest/lee-property-first-seed",
      stateS3Uri:
        "s3://example-bucket/permit-harvest/lee-property-first-seed/job/state.json",
      batchSize: 25,
      maxPages: 200,
      requeueDelaySeconds: 900,
      backpressureQueues: [
        {
          name: "workflow",
          queueUrl:
            "https://sqs.us-east-1.amazonaws.com/123/elephant-workflow-queue",
          maxMessages: 250,
        },
      ],
    });

    expect(message).toMatchObject({
      type: "lee-property-first-seed-feeder",
      jobId: "lee-property-first-seed-all-20260606",
      batchSize: 25,
    });
    expect(_private.resolveBackpressureQueues(message)).toEqual([
      {
        name: "workflow",
        queueUrl:
          "https://sqs.us-east-1.amazonaws.com/123/elephant-workflow-queue",
        maxMessages: 250,
      },
    ]);
    expect(
      _private.buildOneRowSeedCsv(
        ["parcel_id", "address", "source_identifier"],
        {
          parcel_id: "154527L4000030160",
          address: "680 BELL BOULEVARD SOUTH, LEHIGH ACRES, FL 33974",
          source_identifier: "10635900",
        },
      ),
    ).toBe(
      'parcel_id,address,source_identifier\n154527L4000030160,"680 BELL BOULEVARD SOUTH, LEHIGH ACRES, FL 33974",10635900\n',
    );
  });

  it("creates seed feeder state and detects existing Neon Lee rows", () => {
    const state = _private.createInitialLeePropertyFirstSeedFeederState({
      type: "lee-property-first-seed-feeder",
      version: 1,
      jobId: "lee-property-first-seed-all-20260606",
      sourceCsvS3Uri: "s3://counties-seeds/lee.csv",
    });

    expect(state).toMatchObject({
      schemaVersion: "permit-harvest.property-first-seed-feeder-state.v2",
      jobId: "lee-property-first-seed-all-20260606",
      sourceCsvS3Uri: "s3://counties-seeds/lee.csv",
      nextSourceRowNumber: 1,
      sourceExhausted: false,
    });
    expect(
      _private.isExistingLeeAppraiserSeedRow(
        { parcel_id: "08-46-25-57-00000.0130", source_identifier: "12345678" },
        {
          requestIdentifiers: new Set(["12345678"]),
          normalizedParcelIdentifiers: new Set(),
        },
      ),
    ).toBe(true);
    expect(
      _private.isExistingLeeAppraiserSeedRow(
        { parcel_id: "08-46-25-57-00000.0130", source_identifier: "87654321" },
        {
          requestIdentifiers: new Set(),
          normalizedParcelIdentifiers: new Set(["08462557000000130"]),
        },
      ),
    ).toBe(true);
  });

  it("adds property-first parcel evidence without overwriting existing permit parcel details", () => {
    const target = _private.buildPropertyFirstTarget({
      type: "lee-property-first-permit-parcel",
      version: 1,
      jobId: "lee-property-first-test",
      parcelIdentifier: "08-46-25-57-00000.0130",
    });

    expect(
      _private.withPropertyFirstParcel(
        {
          recordNumber: "COM2025-00001",
          parcelIdentifier: null,
          moreDetails: { Type: "Commercial" },
        },
        target,
      ),
    ).toMatchObject({
      parcelIdentifier: "08462557000000130",
      moreDetails: {
        Type: "Commercial",
        "Parcel Number": "08462557000000130",
      },
    });

    expect(
      _private.withPropertyFirstParcel(
        {
          recordNumber: "COM2025-00002",
          parcelIdentifier: "13452432000000120",
          moreDetails: { "Parcel Number": "13452432000000120" },
        },
        target,
      ),
    ).toMatchObject({
      parcelIdentifier: "13452432000000120",
      moreDetails: { "Parcel Number": "13452432000000120" },
    });
  });

  it("links property-first permits using both alphanumeric and digit-only Lee STRAP forms", async () => {
    const target = _private.buildPropertyFirstTarget({
      type: "lee-property-first-permit-parcel",
      version: 1,
      jobId: "lee-property-first-test",
      parcelIdentifier: "294627L40900A1339",
      requestIdentifier: "10635528",
    });
    const queries = [];
    const client = {
      query: async (sql, params) => {
        queries.push({ sql, params });
        if (sql.includes("from properties")) {
          return {
            rows: [{ property_id: "property-1", parcel_id: "parcel-1" }],
            rowCount: 1,
          };
        }
        if (sql.includes("select count(*)::int as matched_permit_rows")) {
          return { rows: [{ matched_permit_rows: 2 }], rowCount: 1 };
        }
        return {
          rows: [
            { property_improvement_id: "permit-1" },
            { property_improvement_id: "permit-2" },
          ],
          rowCount: 2,
        };
      },
    };

    await expect(
      _private.linkPropertyFirstPermits(client, target),
    ).resolves.toEqual({
      matchedPermitRows: 2,
      linkedPermitRows: 2,
    });
    expect(
      _private.normalizeParcelDigits(target.normalizedParcelIdentifier),
    ).toBe("294627409001339");
    expect(queries.map((query) => query.params)).toEqual([
      [null, "10635528", "294627L40900A1339", "294627409001339"],
      ["294627L40900A1339", "294627409001339"],
      ["294627L40900A1339", "294627409001339", "property-1", "parcel-1"],
    ]);
    expect(queries[1].sql).toContain("[^[:alnum:]]");
    expect(queries[1].sql).toContain("[^0-9]");
  });

  it("commits on-demand permits with zero link counters when no appraiser property matches", async () => {
    const target = _private.buildPropertyFirstTarget({
      type: "lee-property-first-permit-parcel",
      version: 1,
      jobId: "mcp-on-demand-test",
      parcelIdentifier: "294627L40900A1339",
    });
    const buildClient = () => {
      const queries = [];
      return {
        queries,
        query: async (sql, params) => {
          queries.push({ sql, params });
          if (sql.includes("from properties")) {
            // No appraiser property loaded yet for this parcel.
            return { rows: [], rowCount: 0 };
          }
          if (sql.includes("select count(*)::int as matched_permit_rows")) {
            return { rows: [{ matched_permit_rows: 0 }], rowCount: 1 };
          }
          return { rows: [], rowCount: 0 };
        },
      };
    };

    // On-demand: returns zero counters plus propertyMissing:true (so the caller
    // can defer writing completed.json) and never runs the match/update queries.
    const onDemandClient = buildClient();
    await expect(
      _private.linkPropertyFirstPermits(onDemandClient, target, true),
    ).resolves.toEqual({
      matchedPermitRows: 0,
      linkedPermitRows: 0,
      propertyMissing: true,
    });
    expect(onDemandClient.queries).toHaveLength(1);
    expect(onDemandClient.queries[0].sql).toContain("from properties");

    // Non-onDemand: still throws exactly as before, which rolls back the upsert.
    const bulkClient = buildClient();
    await expect(
      _private.linkPropertyFirstPermits(bulkClient, target, false),
    ).rejects.toThrow("No loaded Lee Appraiser property found");
    await expect(
      _private.linkPropertyFirstPermits(bulkClient, target),
    ).rejects.toThrow("No loaded Lee Appraiser property found");
  });

  it("defers completed-state only for on-demand harvests where the appraiser property is missing", () => {
    // Deferred: on-demand harvest linked no property yet -> do NOT mark
    // completed, so a future run links the permits once the property loads.
    expect(
      _private.shouldWritePropertyFirstCompletedState({
        onDemand: true,
        propertyMissing: true,
      }),
    ).toBe(false);

    // On-demand but property present -> completed state written as before.
    expect(
      _private.shouldWritePropertyFirstCompletedState({
        onDemand: true,
        propertyMissing: false,
      }),
    ).toBe(true);

    // Non-onDemand (bulk) always writes completed state, even in the (unreached)
    // propertyMissing case -> bulk behavior is unchanged.
    expect(
      _private.shouldWritePropertyFirstCompletedState({
        onDemand: false,
        propertyMissing: true,
      }),
    ).toBe(true);
    expect(
      _private.shouldWritePropertyFirstCompletedState({
        onDemand: false,
        propertyMissing: false,
      }),
    ).toBe(true);
  });

  it("retries transient async operations before surfacing failure", async () => {
    let attempts = 0;
    const retries = [];

    await expect(
      _private.retryAsyncOperation({
        maxAttempts: 3,
        delayMs: 0,
        operation: async (attempt) => {
          attempts = attempt;
          if (attempt < 3) throw new Error(`transient ${String(attempt)}`);
          return "ok";
        },
        onRetry: (details) => retries.push(details),
      }),
    ).resolves.toBe("ok");
    expect(attempts).toBe(3);
    expect(retries).toEqual([
      { attempt: 1, maxAttempts: 3, errorMessage: "transient 1" },
      { attempt: 2, maxAttempts: 3, errorMessage: "transient 2" },
    ]);

    await expect(
      _private.retryAsyncOperation({
        maxAttempts: 1,
        delayMs: 0,
        operation: async () => {
          throw new Error("terminal");
        },
      }),
    ).rejects.toThrow("terminal");
  });

  it("reads Neon database URLs from raw and JSON secret payloads", () => {
    expect(
      _private.readDatabaseUrlFromSecretString(
        "postgresql://user:pass@example.test/neondb?sslmode=require",
      ),
    ).toBe("postgresql://user:pass@example.test/neondb?sslmode=require");
    expect(
      _private.readDatabaseUrlFromSecretString(
        JSON.stringify({
          DATABASE_URL: "postgres://user:pass@example.test/db",
        }),
      ),
    ).toBe("postgres://user:pass@example.test/db");
  });
});
