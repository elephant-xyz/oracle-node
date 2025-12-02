import { defineConfig } from "vitest/config";
import { resolve } from "path";

export default defineConfig({
  resolve: {
    alias: {
      // Resolve 'shared/*' imports to the shared-layer source for tests
      shared: resolve(__dirname, "workflow-events/lambdas/shared-layer/src"),
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
