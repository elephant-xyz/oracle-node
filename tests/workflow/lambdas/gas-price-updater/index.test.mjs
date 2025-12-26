import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { mockClient } from "aws-sdk-client-mock";
import { SSMClient, PutParameterCommand } from "@aws-sdk/client-ssm";

const ssmMock = mockClient(SSMClient);

// Mock @elephant-xyz/cli/lib
const mockCheckGasPrice = vi.fn();
vi.mock("@elephant-xyz/cli/lib", () => ({
  checkGasPrice: mockCheckGasPrice,
}));

describe("gas-price-updater handler", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    vi.clearAllMocks();
    ssmMock.reset();
    mockCheckGasPrice.mockClear();

    process.env = {
      ...originalEnv,
      ELEPHANT_RPC_URL: "https://rpc.example.com",
      GAS_PRICE_PARAMETER_NAME: "/test-stack/gas-price/current",
    };

    vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    process.env = originalEnv;
    vi.restoreAllMocks();
  });

  describe("Successful gas price updates", () => {
    it("should fetch gas price and update SSM parameter", async () => {
      mockCheckGasPrice.mockResolvedValue({
        eip1559: { maxFeePerGas: "25000000000" }, // 25 Gwei in Wei
        legacy: { gasPrice: "25000000000" },
      });

      ssmMock.on(PutParameterCommand).resolves({});

      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-updater/index.mjs"
      );

      const result = await handler();

      expect(mockCheckGasPrice).toHaveBeenCalledWith({
        rpcUrl: "https://rpc.example.com",
      });

      expect(ssmMock.calls()).toHaveLength(1);
      const putCall = ssmMock.calls()[0];
      expect(putCall.args[0].input.Name).toBe("/test-stack/gas-price/current");
      expect(putCall.args[0].input.Type).toBe("String");
      expect(putCall.args[0].input.Overwrite).toBe(true);

      // Parse the stored value to verify structure
      const storedValue = JSON.parse(putCall.args[0].input.Value);
      expect(storedValue.gasPrice).toBe(25);
      expect(storedValue.updatedAt).toBeDefined();

      expect(result.gasPrice).toBe(25);
      expect(result.updatedAt).toBeDefined();
    });

    it("should use EIP-1559 maxFeePerGas when available", async () => {
      mockCheckGasPrice.mockResolvedValue({
        eip1559: { maxFeePerGas: "30000000000" }, // 30 Gwei
        legacy: { gasPrice: "20000000000" }, // 20 Gwei
      });

      ssmMock.on(PutParameterCommand).resolves({});

      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-updater/index.mjs"
      );

      const result = await handler();

      // Should use EIP-1559 maxFeePerGas (30 Gwei)
      expect(result.gasPrice).toBe(30);
    });

    it("should fall back to legacy gasPrice when EIP-1559 is not available", async () => {
      mockCheckGasPrice.mockResolvedValue({
        legacy: { gasPrice: "20000000000" }, // 20 Gwei
      });

      ssmMock.on(PutParameterCommand).resolves({});

      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-updater/index.mjs"
      );

      const result = await handler();

      // Should use legacy gasPrice (20 Gwei)
      expect(result.gasPrice).toBe(20);
    });

    it("should handle decimal gas prices", async () => {
      mockCheckGasPrice.mockResolvedValue({
        eip1559: { maxFeePerGas: "25500000000" }, // 25.5 Gwei
        legacy: { gasPrice: "25500000000" },
      });

      ssmMock.on(PutParameterCommand).resolves({});

      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-updater/index.mjs"
      );

      const result = await handler();

      expect(result.gasPrice).toBe(25.5);
    });

    it("should handle very low gas prices", async () => {
      mockCheckGasPrice.mockResolvedValue({
        eip1559: { maxFeePerGas: "1000000000" }, // 1 Gwei
        legacy: { gasPrice: "1000000000" },
      });

      ssmMock.on(PutParameterCommand).resolves({});

      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-updater/index.mjs"
      );

      const result = await handler();

      expect(result.gasPrice).toBe(1);
    });

    it("should handle high gas prices", async () => {
      mockCheckGasPrice.mockResolvedValue({
        eip1559: { maxFeePerGas: "500000000000" }, // 500 Gwei
        legacy: { gasPrice: "500000000000" },
      });

      ssmMock.on(PutParameterCommand).resolves({});

      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-updater/index.mjs"
      );

      const result = await handler();

      expect(result.gasPrice).toBe(500);
    });
  });

  describe("Configuration validation", () => {
    it("should fail when RPC URL is missing", async () => {
      delete process.env.ELEPHANT_RPC_URL;

      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-updater/index.mjs"
      );

      await expect(handler()).rejects.toThrow(
        "RPC URL is required (ELEPHANT_RPC_URL env var)",
      );
    });

    it("should fail when SSM Parameter name is missing", async () => {
      delete process.env.GAS_PRICE_PARAMETER_NAME;

      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-updater/index.mjs"
      );

      await expect(handler()).rejects.toThrow(
        "SSM Parameter name is required (GAS_PRICE_PARAMETER_NAME env var)",
      );
    });
  });

  describe("Error handling", () => {
    it("should fail when checkGasPrice throws an error", async () => {
      mockCheckGasPrice.mockRejectedValue(new Error("RPC connection failed"));

      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-updater/index.mjs"
      );

      await expect(handler()).rejects.toThrow("RPC connection failed");
    });

    it("should fail when gas price cannot be retrieved", async () => {
      mockCheckGasPrice.mockResolvedValue({});

      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-updater/index.mjs"
      );

      await expect(handler()).rejects.toThrow(
        "Unable to retrieve gas price from RPC",
      );
    });

    it("should fail when SSM PutParameter fails", async () => {
      mockCheckGasPrice.mockResolvedValue({
        eip1559: { maxFeePerGas: "25000000000" },
        legacy: { gasPrice: "25000000000" },
      });

      ssmMock
        .on(PutParameterCommand)
        .rejects(new Error("SSM service unavailable"));

      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-updater/index.mjs"
      );

      await expect(handler()).rejects.toThrow("SSM service unavailable");
    });

    it("should fail when eip1559 is null and legacy is null", async () => {
      mockCheckGasPrice.mockResolvedValue({
        eip1559: null,
        legacy: null,
      });

      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-updater/index.mjs"
      );

      await expect(handler()).rejects.toThrow(
        "Unable to retrieve gas price from RPC",
      );
    });
  });

  describe("Logging", () => {
    it("should log gas price retrieval", async () => {
      const consoleLogSpy = vi.spyOn(console, "log");

      mockCheckGasPrice.mockResolvedValue({
        eip1559: { maxFeePerGas: "25000000000" },
        legacy: { gasPrice: "25000000000" },
      });

      ssmMock.on(PutParameterCommand).resolves({});

      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-updater/index.mjs"
      );

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

    it("should log SSM parameter update", async () => {
      const consoleLogSpy = vi.spyOn(console, "log");

      mockCheckGasPrice.mockResolvedValue({
        eip1559: { maxFeePerGas: "25000000000" },
        legacy: { gasPrice: "25000000000" },
      });

      ssmMock.on(PutParameterCommand).resolves({});

      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-updater/index.mjs"
      );

      await handler();

      const logCalls = consoleLogSpy.mock.calls;
      const updatedLog = logCalls.find((call) => {
        try {
          const parsed = JSON.parse(call[0]);
          return parsed.msg === "ssm_parameter_updated";
        } catch {
          return false;
        }
      });

      expect(updatedLog).toBeDefined();
      const parsedLog = JSON.parse(updatedLog[0]);
      expect(parsedLog.parameterName).toBe("/test-stack/gas-price/current");
      expect(parsedLog.gasPriceGwei).toBe(25);
    });

    it("should log errors on failure", async () => {
      const consoleErrorSpy = vi.spyOn(console, "error");

      mockCheckGasPrice.mockRejectedValue(new Error("RPC error"));

      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-updater/index.mjs"
      );

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

  describe("SSM Parameter structure", () => {
    it("should store gas price with correct structure", async () => {
      mockCheckGasPrice.mockResolvedValue({
        eip1559: { maxFeePerGas: "25000000000" },
        legacy: { gasPrice: "25000000000" },
      });

      ssmMock.on(PutParameterCommand).resolves({});

      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-updater/index.mjs"
      );

      await handler();

      const putCall = ssmMock.calls()[0];
      const storedValue = JSON.parse(putCall.args[0].input.Value);

      expect(storedValue).toHaveProperty("gasPrice");
      expect(storedValue).toHaveProperty("updatedAt");
      expect(typeof storedValue.gasPrice).toBe("number");
      expect(typeof storedValue.updatedAt).toBe("string");

      // Verify updatedAt is a valid ISO date string
      const date = new Date(storedValue.updatedAt);
      expect(date.toISOString()).toBe(storedValue.updatedAt);
    });

    it("should not include maxGasPrice or isAcceptable in stored value", async () => {
      mockCheckGasPrice.mockResolvedValue({
        eip1559: { maxFeePerGas: "25000000000" },
        legacy: { gasPrice: "25000000000" },
      });

      ssmMock.on(PutParameterCommand).resolves({});

      const { handler } = await import(
        "../../../../workflow/lambdas/gas-price-updater/index.mjs"
      );

      await handler();

      const putCall = ssmMock.calls()[0];
      const storedValue = JSON.parse(putCall.args[0].input.Value);

      // SSM should only store gas price and updatedAt
      // Step Function compares against GasPriceMaxGwei threshold directly
      expect(storedValue).not.toHaveProperty("maxGasPrice");
      expect(storedValue).not.toHaveProperty("isAcceptable");
    });
  });
});
