import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { mockClient } from "aws-sdk-client-mock";
import { DynamoDBClient, PutItemCommand } from "@aws-sdk/client-dynamodb";

const dynamoMock = mockClient(DynamoDBClient);

// Mock @elephant-xyz/cli/lib
const mockCheckGasPrice = vi.fn();
vi.mock("@elephant-xyz/cli/lib", () => ({
  checkGasPrice: mockCheckGasPrice,
}));

describe("gas-price-updater handler", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    vi.clearAllMocks();
    dynamoMock.reset();
    mockCheckGasPrice.mockClear();

    process.env = {
      ...originalEnv,
      ELEPHANT_RPC_URL: "https://rpc.example.com",
      GAS_PRICE_TABLE_NAME: "test-stack-GasPrice",
    };

    vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    process.env = originalEnv;
    vi.restoreAllMocks();
  });

  describe("Successful gas price updates", () => {
    it("should fetch gas price and update DynamoDB item", async () => {
      // CLI returns values in Gwei directly
      mockCheckGasPrice.mockResolvedValue({
        eip1559: { maxFeePerGas: 25 },
        legacy: { gasPrice: 25 },
      });

      dynamoMock.on(PutItemCommand).resolves({});

      const { handler } =
        await import("../../../../workflow/lambdas/gas-price-updater/index.mjs");

      const result = await handler();

      expect(mockCheckGasPrice).toHaveBeenCalledWith({
        rpcUrl: "https://rpc.example.com",
      });

      expect(dynamoMock.calls()).toHaveLength(1);
      const putCall = dynamoMock.calls()[0];
      expect(putCall.args[0].input.TableName).toBe("test-stack-GasPrice");
      expect(putCall.args[0].input.Item.PK.S).toBe("CURRENT");
      expect(putCall.args[0].input.Item.gasPrice.N).toBe("25");
      expect(putCall.args[0].input.Item.updatedAt.S).toBeDefined();

      expect(result.gasPrice).toBe(25);
      expect(result.updatedAt).toBeDefined();
    });

    it("should use EIP-1559 maxFeePerGas when available", async () => {
      mockCheckGasPrice.mockResolvedValue({
        eip1559: { maxFeePerGas: 30 },
        legacy: { gasPrice: 20 },
      });

      dynamoMock.on(PutItemCommand).resolves({});

      const { handler } =
        await import("../../../../workflow/lambdas/gas-price-updater/index.mjs");

      const result = await handler();

      // Should use EIP-1559 maxFeePerGas (30 Gwei)
      expect(result.gasPrice).toBe(30);
    });

    it("should fall back to legacy gasPrice when EIP-1559 is not available", async () => {
      mockCheckGasPrice.mockResolvedValue({
        legacy: { gasPrice: 20 },
      });

      dynamoMock.on(PutItemCommand).resolves({});

      const { handler } =
        await import("../../../../workflow/lambdas/gas-price-updater/index.mjs");

      const result = await handler();

      // Should use legacy gasPrice (20 Gwei)
      expect(result.gasPrice).toBe(20);
    });

    it("should handle decimal gas prices", async () => {
      mockCheckGasPrice.mockResolvedValue({
        eip1559: { maxFeePerGas: 25.5 },
        legacy: { gasPrice: 25.5 },
      });

      dynamoMock.on(PutItemCommand).resolves({});

      const { handler } =
        await import("../../../../workflow/lambdas/gas-price-updater/index.mjs");

      const result = await handler();

      expect(result.gasPrice).toBe(25.5);
    });

    it("should convert string gas prices to numbers", async () => {
      // CLI may return gas prices as strings per type definitions
      mockCheckGasPrice.mockResolvedValue({
        eip1559: { maxFeePerGas: "30.5" },
        legacy: { gasPrice: "30.5" },
      });

      dynamoMock.on(PutItemCommand).resolves({});

      const { handler } =
        await import("../../../../workflow/lambdas/gas-price-updater/index.mjs");

      const result = await handler();

      expect(result.gasPrice).toBe(30.5);
      expect(typeof result.gasPrice).toBe("number");

      // Verify DynamoDB receives the number as a string (N type)
      const putCall = dynamoMock.calls()[0];
      expect(putCall.args[0].input.Item.gasPrice.N).toBe("30.5");
    });

    it("should handle very low gas prices", async () => {
      mockCheckGasPrice.mockResolvedValue({
        eip1559: { maxFeePerGas: 1 },
        legacy: { gasPrice: 1 },
      });

      dynamoMock.on(PutItemCommand).resolves({});

      const { handler } =
        await import("../../../../workflow/lambdas/gas-price-updater/index.mjs");

      const result = await handler();

      expect(result.gasPrice).toBe(1);
    });

    it("should handle high gas prices", async () => {
      mockCheckGasPrice.mockResolvedValue({
        eip1559: { maxFeePerGas: 500 },
        legacy: { gasPrice: 500 },
      });

      dynamoMock.on(PutItemCommand).resolves({});

      const { handler } =
        await import("../../../../workflow/lambdas/gas-price-updater/index.mjs");

      const result = await handler();

      expect(result.gasPrice).toBe(500);
    });
  });

  describe("Configuration validation", () => {
    it("should fail when RPC URL is missing", async () => {
      delete process.env.ELEPHANT_RPC_URL;

      const { handler } =
        await import("../../../../workflow/lambdas/gas-price-updater/index.mjs");

      await expect(handler()).rejects.toThrow(
        "RPC URL is required (ELEPHANT_RPC_URL env var)",
      );
    });

    it("should fail when DynamoDB table name is missing", async () => {
      delete process.env.GAS_PRICE_TABLE_NAME;

      const { handler } =
        await import("../../../../workflow/lambdas/gas-price-updater/index.mjs");

      await expect(handler()).rejects.toThrow(
        "DynamoDB table name is required (GAS_PRICE_TABLE_NAME env var)",
      );
    });
  });

  describe("Error handling", () => {
    it("should fail when checkGasPrice throws an error", async () => {
      mockCheckGasPrice.mockRejectedValue(new Error("RPC connection failed"));

      const { handler } =
        await import("../../../../workflow/lambdas/gas-price-updater/index.mjs");

      await expect(handler()).rejects.toThrow("RPC connection failed");
    });

    it("should fail when gas price cannot be retrieved", async () => {
      mockCheckGasPrice.mockResolvedValue({});

      const { handler } =
        await import("../../../../workflow/lambdas/gas-price-updater/index.mjs");

      await expect(handler()).rejects.toThrow(
        "Unable to retrieve gas price from RPC",
      );
    });

    it("should fail when DynamoDB PutItem fails", async () => {
      mockCheckGasPrice.mockResolvedValue({
        eip1559: { maxFeePerGas: 25 },
        legacy: { gasPrice: 25 },
      });

      dynamoMock
        .on(PutItemCommand)
        .rejects(new Error("DynamoDB service unavailable"));

      const { handler } =
        await import("../../../../workflow/lambdas/gas-price-updater/index.mjs");

      await expect(handler()).rejects.toThrow("DynamoDB service unavailable");
    });

    it("should fail when eip1559 is null and legacy is null", async () => {
      mockCheckGasPrice.mockResolvedValue({
        eip1559: null,
        legacy: null,
      });

      const { handler } =
        await import("../../../../workflow/lambdas/gas-price-updater/index.mjs");

      await expect(handler()).rejects.toThrow(
        "Unable to retrieve gas price from RPC",
      );
    });

    it("should fail when gas price is not a valid number", async () => {
      mockCheckGasPrice.mockResolvedValue({
        eip1559: { maxFeePerGas: "invalid" },
        legacy: { gasPrice: "invalid" },
      });

      const { handler } =
        await import("../../../../workflow/lambdas/gas-price-updater/index.mjs");

      await expect(handler()).rejects.toThrow(
        "Unable to retrieve gas price from RPC",
      );
    });
  });

  describe("Logging", () => {
    it("should log gas price retrieval", async () => {
      const consoleLogSpy = vi.spyOn(console, "log");

      mockCheckGasPrice.mockResolvedValue({
        eip1559: { maxFeePerGas: 25 },
        legacy: { gasPrice: 25 },
      });

      dynamoMock.on(PutItemCommand).resolves({});

      const { handler } =
        await import("../../../../workflow/lambdas/gas-price-updater/index.mjs");

      await handler();

      // Verify logging calls were made
      expect(consoleLogSpy).toHaveBeenCalled();

      // Find the gas_price_retrieved log entry
      const logCalls = consoleLogSpy.mock.calls;
      const retrievedLog = logCalls.find((call) => {
        try {
          const parsed = JSON.parse(call[0]);
          return parsed.msg === "gas_price_retrieved";
        } catch {
          return false;
        }
      });

      expect(retrievedLog).toBeDefined();
      const parsedLog = JSON.parse(retrievedLog[0]);
      expect(parsedLog.gasPriceGwei).toBe(25);
    });

    it("should log DynamoDB item update", async () => {
      const consoleLogSpy = vi.spyOn(console, "log");

      mockCheckGasPrice.mockResolvedValue({
        eip1559: { maxFeePerGas: 25 },
        legacy: { gasPrice: 25 },
      });

      dynamoMock.on(PutItemCommand).resolves({});

      const { handler } =
        await import("../../../../workflow/lambdas/gas-price-updater/index.mjs");

      await handler();

      const logCalls = consoleLogSpy.mock.calls;
      const updatedLog = logCalls.find((call) => {
        try {
          const parsed = JSON.parse(call[0]);
          return parsed.msg === "dynamodb_item_updated";
        } catch {
          return false;
        }
      });

      expect(updatedLog).toBeDefined();
      const parsedLog = JSON.parse(updatedLog[0]);
      expect(parsedLog.tableName).toBe("test-stack-GasPrice");
      expect(parsedLog.gasPriceGwei).toBe(25);
    });

    it("should log errors on failure", async () => {
      const consoleErrorSpy = vi.spyOn(console, "error");

      mockCheckGasPrice.mockRejectedValue(new Error("RPC error"));

      const { handler } =
        await import("../../../../workflow/lambdas/gas-price-updater/index.mjs");

      await expect(handler()).rejects.toThrow("RPC error");

      const errorCalls = consoleErrorSpy.mock.calls;
      const errorLog = errorCalls.find((call) => {
        try {
          const parsed = JSON.parse(call[0]);
          return parsed.msg === "gas_price_update_failed";
        } catch {
          return false;
        }
      });

      expect(errorLog).toBeDefined();
    });
  });

  describe("DynamoDB item structure", () => {
    it("should store gas price with correct structure", async () => {
      mockCheckGasPrice.mockResolvedValue({
        eip1559: { maxFeePerGas: 25 },
        legacy: { gasPrice: 25 },
      });

      dynamoMock.on(PutItemCommand).resolves({});

      const { handler } =
        await import("../../../../workflow/lambdas/gas-price-updater/index.mjs");

      await handler();

      const putCall = dynamoMock.calls()[0];
      const item = putCall.args[0].input.Item;

      expect(item).toHaveProperty("PK");
      expect(item).toHaveProperty("gasPrice");
      expect(item).toHaveProperty("updatedAt");
      expect(item.PK.S).toBe("CURRENT");
      expect(typeof item.gasPrice.N).toBe("string"); // DynamoDB N type is string
      expect(typeof item.updatedAt.S).toBe("string");

      // Verify updatedAt is a valid ISO date string
      const date = new Date(item.updatedAt.S);
      expect(date.toISOString()).toBe(item.updatedAt.S);
    });

    it("should not include maxGasPrice or isAcceptable in stored value", async () => {
      mockCheckGasPrice.mockResolvedValue({
        eip1559: { maxFeePerGas: 25 },
        legacy: { gasPrice: 25 },
      });

      dynamoMock.on(PutItemCommand).resolves({});

      const { handler } =
        await import("../../../../workflow/lambdas/gas-price-updater/index.mjs");

      await handler();

      const putCall = dynamoMock.calls()[0];
      const item = putCall.args[0].input.Item;

      // DynamoDB should only store PK, gasPrice, and updatedAt
      // Step Function compares against GasPriceMaxGwei threshold directly
      expect(item).not.toHaveProperty("maxGasPrice");
      expect(item).not.toHaveProperty("isAcceptable");
    });
  });
});
