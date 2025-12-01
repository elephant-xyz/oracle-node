import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globals: true,
    environment: "node",
    include: ["**/*.test.mjs", "**/*.test.js"],
    exclude: ["**/node_modules/**", "**/dist/**"],

    // Setup file for custom matchers and global test configuration
    setupFiles: ["./test/setup.mjs"],

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
        "**/*.test.mjs",
        "**/test/**",
      ],
    },
  },
});
