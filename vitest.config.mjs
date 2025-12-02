import { defineConfig } from "vitest/config";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
import { resolve } from "path";

export default defineConfig({
  resolve: {
    alias: {
      // Force all AWS SDK imports to use the root node_modules copies
      // This ensures mocking works correctly across workspace packages
      "@aws-sdk/client-s3": path.resolve(
        __dirname,
        "node_modules/@aws-sdk/client-s3",
      ),
      "@aws-sdk/client-eventbridge": path.resolve(
        __dirname,
        "node_modules/@aws-sdk/client-eventbridge",
      ),
      "@aws-sdk/client-sfn": path.resolve(
        __dirname,
        "node_modules/@aws-sdk/client-sfn",
      ),
      "@aws-sdk/client-dynamodb": path.resolve(
        __dirname,
        "node_modules/@aws-sdk/client-dynamodb",
      ),
      "@aws-sdk/lib-dynamodb": path.resolve(
        __dirname,
        "node_modules/@aws-sdk/lib-dynamodb",
      ),
      // Resolve 'shared/*' imports to the shared-layer source for tests
      shared: resolve(__dirname, "workflow-events/layers/shared/src"),
    },
  },
  test: {
    globals: true,
    environment: "node",
    include: [
      "tests/**/*.test.mjs",
      "tests/**/*.test.js",
      "tests/**/*.test.ts",
      "**/__tests__/**/*.test.ts",
    ],
    exclude: ["**/node_modules/**", "**/dist/**"],

    // Setup file for custom matchers and global test configuration
    setupFiles: ["./tests/setup.mjs", "./tests/setup.ts"],

    // Automatically clear and restore mocks between tests
    clearMocks: true,
    restoreMocks: true,

    // Coverage configuration
    coverage: {
      provider: "v8",
      reporter: ["text", "json", "html"],
      reportsDirectory: "./coverage",
      exclude: [
        "**/node_modules/**",
        "**/dist/**",
        "**/tests/**",
        "**/__tests__/**",
      ],
    },
  },
});
