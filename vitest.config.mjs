import { defineConfig } from "vitest/config";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
import { resolve } from "path";

export default defineConfig({
  resolve: {
    alias: [
      // Force all AWS SDK imports to use the root node_modules copies
      // This ensures mocking works correctly across workspace packages
      {
        find: "@aws-sdk/client-s3",
        replacement: path.resolve(__dirname, "node_modules/@aws-sdk/client-s3"),
      },
      {
        find: "@aws-sdk/client-eventbridge",
        replacement: path.resolve(
          __dirname,
          "node_modules/@aws-sdk/client-eventbridge",
        ),
      },
      {
        find: "@aws-sdk/client-sfn",
        replacement: path.resolve(
          __dirname,
          "node_modules/@aws-sdk/client-sfn",
        ),
      },
      {
        find: "@aws-sdk/client-dynamodb",
        replacement: path.resolve(
          __dirname,
          "node_modules/@aws-sdk/client-dynamodb",
        ),
      },
      {
        find: "@aws-sdk/lib-dynamodb",
        replacement: path.resolve(
          __dirname,
          "node_modules/@aws-sdk/lib-dynamodb",
        ),
      },
      // Resolve 'shared/*.js' imports for workflow-events lambdas (import from "shared/types.js" etc.)
      // Maps .js extension to .ts source files for test resolution
      // Must come before exact 'shared' match to handle subpath imports first
      {
        find: /^shared\/(.*)\.js$/,
        replacement: resolve(
          __dirname,
          "workflow-events/layers/shared/src/$1.ts",
        ),
      },
      // Resolve exact 'shared' imports for workflow lambdas (import from "shared")
      {
        find: /^shared$/,
        replacement: resolve(__dirname, "workflow/layers/shared/src/index.mjs"),
      },
    ],
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
